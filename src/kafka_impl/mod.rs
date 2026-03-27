//! Kafka Implementations Module
//!
//! Contains implementations of the public traits in this library, configured for interactions with a Kafka instance.

use crate::{Message, PubSubError, Publisher, Snapshot, Subscriber};
use rdkafka::{
    ClientConfig, Message as RdMessage,
    consumer::{Consumer, StreamConsumer},
    error::{KafkaError, KafkaResult},
    message::BorrowedMessage,
    producer::{FutureProducer, FutureRecord},
    types::RDKafkaErrorCode,
};
use rust_env_var_lib::env_var;
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::LazyLock,
    time::Duration,
};
use tokio::sync::RwLock;
use tokio_stream::{Stream, StreamExt};
use uuid::Uuid;

#[cfg(any(feature = "testing-utils", test))]
pub mod testing_utils;

#[cfg(test)]
mod tests;

/// [`FutureProducer`] is intended to be a one-per-host construct, shared by all parts of the application that need to produce on that host.
/// As such, we build a static map of the producers to be shared by all instances of [`KafkaPublisher`] that get requested for the same host.  
static PRODUCER_MAP: LazyLock<RwLock<HashMap<String, FutureProducer>>> =
    LazyLock::new(RwLock::default);

impl From<KafkaError> for PubSubError {
    fn from(value: KafkaError) -> Self {
        PubSubError::from_display(value)
    }
}

/// Implementation of the [`Publisher`] trait for Kafka connections.
pub struct KafkaPublisher {
    host: String,
    topic: String,
}
impl KafkaPublisher {
    async fn get_connection(&self) -> Result<FutureProducer, PubSubError> {
        let naive_read = PRODUCER_MAP.read().await.get(&self.host).cloned();
        match naive_read {
            Some(producer) => Ok(producer),
            None => {
                let mut lock = PRODUCER_MAP.write().await;
                match lock.get(&self.host).cloned() {
                    Some(producer) => Ok(producer),
                    None => {
                        let default = ClientConfig::new()
                            .set("bootstrap.servers", &self.host)
                            .create()?;
                        let producer = lock
                            .entry(self.host.clone())
                            .insert_entry(default)
                            .get()
                            .clone();
                        Ok(producer)
                    }
                }
            }
        }
    }
}
#[async_trait::async_trait]
impl Publisher for KafkaPublisher {
    fn new(host: String, topic: String) -> Self {
        Self { host, topic }
    }

    async fn publish<T, M: Message<T>>(&self, message: M) -> Result<(), PubSubError> {
        let producer = self.get_connection().await?;
        let bytes = message.into_bytes();
        let mut record = FutureRecord::to(&self.topic).payload(&bytes.value);
        if let Some(key) = &bytes.key {
            record = record.key(key);
        }
        match producer.send(record, get_kafka_timeout_val()).await {
            Ok(_) => Ok(()),
            Err((err, _)) => Err(PubSubError::from(err)),
        }
    }
}
impl Debug for KafkaPublisher {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("KafkaPublisher")
            .field("host", &self.host)
            .field("topic", &self.topic)
            .finish()
    }
}

/// Implementation of the [`Snapshot`] trait for Kafka connections.
#[derive(Debug)]
pub struct KafkaSnapshot;
impl KafkaSnapshot {
    fn configure_consumer(host: &str, topic: &str) -> Result<StreamConsumer, PubSubError> {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", host)
            .set("group.id", Uuid::new_v4().as_hyphenated().to_string())
            .set("auto.offset.reset", "earliest")
            .create::<StreamConsumer>()?;
        consumer.subscribe(&[topic])?;
        Ok(consumer)
    }

    fn determine_max_offsets(
        consumer: &StreamConsumer,
        topic: &str,
    ) -> Result<HashMap<i32, i64>, KafkaError> {
        let timeout = get_kafka_timeout_val();
        let metadata = consumer.fetch_metadata(Some(topic), timeout)?;
        match metadata.topics().first() {
            Some(topic_metadata) => {
                let mut offsets = HashMap::new();
                for partition in topic_metadata.partitions() {
                    let (_, high) = consumer.fetch_watermarks(topic, partition.id(), timeout)?;
                    if high > 0 {
                        // The "high watermark" is the next offset to be assigned. Subtracting 1 ensures we
                        // return the actual max offset for messages in the topic currently.
                        offsets.insert(partition.id(), high - 1);
                    }
                }
                Ok(offsets)
            }
            None => Err(KafkaError::MetadataFetch(
                RDKafkaErrorCode::InvalidPartitions,
            )),
        }
    }
}
#[async_trait::async_trait]
impl Snapshot for KafkaSnapshot {
    async fn get<T, M: Message<T>>(host: String, topic: String) -> Result<Vec<M>, PubSubError> {
        let consumer = Self::configure_consumer(&host, &topic)?;
        let mut offsets = Self::determine_max_offsets(&consumer, &topic)?;

        let mut stream = consumer.stream();
        let mut data: Vec<M> = Vec::new();
        while !offsets.is_empty()
            && let Some(msg_res) = stream.next().await
        {
            let msg = msg_res?;
            offsets.retain(|k, v| *k != msg.partition() || *v > msg.offset());

            data.push(convert_to_message(msg)?);
        }
        Ok(data)
    }
}

/// Implementation of the [`Subscriber`] trait for Kafka connections.
pub struct KafkaSubscriber {
    consumer: Option<StreamConsumer>,
    host: String,
    topic: String,
    uuid: Uuid,
}
impl KafkaSubscriber {
    fn check_connection(&mut self) -> Result<(), PubSubError> {
        if self.consumer.is_none() {
            self.configure_consumer()?;
        }
        Ok(())
    }

    fn configure_consumer(&mut self) -> Result<(), PubSubError> {
        #[cfg(not(any(feature = "testing-utils", test)))]
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.host)
            .set("group.id", self.uuid.as_hyphenated().to_string())
            .create::<StreamConsumer>()?;

        // During testing, the low latency of the mock Kafka cluster means that messages are often produced on the broker before the
        // consumer is registered. Setting the auto offset reset to "earliest" ensures we see all messages during the test.
        #[cfg(any(feature = "testing-utils", test))]
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.host)
            .set("group.id", self.uuid.as_hyphenated().to_string())
            .set("auto.offset.reset", "earliest")
            .create::<StreamConsumer>()?;
        consumer.subscribe(&[&self.topic])?;
        self.consumer = Some(consumer);
        Ok(())
    }

    fn convert_stream<T, M: Message<T>>(
        initial: KafkaResult<BorrowedMessage>,
    ) -> Result<M, PubSubError> {
        convert_to_message(initial?)
    }
}
impl Subscriber for KafkaSubscriber {
    fn new(host: String, topic: String) -> Self {
        Self {
            consumer: None,
            host,
            topic,
            uuid: Uuid::new_v4(),
        }
    }

    fn get_stream<T, M: Message<T>>(
        &mut self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>> + Unpin + Send, PubSubError> {
        self.check_connection()?;
        Ok(self
            .consumer
            .as_ref()
            .unwrap()
            .stream()
            .map(Self::convert_stream))
    }
}
impl Debug for KafkaSubscriber {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let consumer_text = if self.consumer.is_none() {
            "None".to_string()
        } else {
            "Some(StreamConsumer)".to_string()
        };
        f.debug_struct("KafkaSubscriber")
            .field("consumer", &consumer_text)
            .field("host", &self.host)
            .field("topic", &self.topic)
            .field("uuid", &self.uuid.as_hyphenated().to_string())
            .finish()
    }
}

fn convert_to_message<T, M: Message<T>>(incoming: BorrowedMessage) -> Result<M, PubSubError> {
    let value = incoming.payload().ok_or_else(PubSubError::default)?;
    let key = incoming.key();

    Ok(M::from_bytes(key, value))
}

fn get_kafka_timeout_val() -> Duration {
    let secs = env_var::get("KAFKA_CONNECTION_SECONDS").or(1);
    Duration::from_secs(secs)
}
