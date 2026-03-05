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
    time::Duration,
};
use tokio_stream::{Stream, StreamExt};
use uuid::Uuid;

impl From<KafkaError> for PubSubError {
    fn from(value: KafkaError) -> Self {
        PubSubError {
            cause: Some(Box::new(value)),
            ..Default::default()
        }
    }
}

/// Implementation of the [`Publisher`] trait for Kafka connections.
pub struct KafkaPublisher {
    host: String,
    producer: Option<FutureProducer>,
    topic: String,
}
impl KafkaPublisher {
    fn check_connection(&mut self) -> Result<(), PubSubError> {
        if self.producer.is_none() {
            let producer = ClientConfig::new()
                .set("bootstrap.servers", &self.host)
                .create()?;
            self.producer = Some(producer);
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl Publisher for KafkaPublisher {
    fn new(host: String, topic: String) -> Self {
        Self {
            host,
            producer: None,
            topic,
        }
    }

    async fn publish(&mut self, message: Message) -> Result<(), PubSubError> {
        self.check_connection()?;
        let producer = self.producer.as_ref().unwrap();
        let mut record = FutureRecord::to(&self.topic).payload(&message.value);
        if let Some(key) = &message.key {
            record = record.key(key);
        }
        match producer.send(record, get_kafka_timeout_val()).await {
            Ok(_) => Ok(()),
            Err((err, _)) => {
                self.producer = None;
                Err(PubSubError::from(err))
            }
        }
    }
}
impl Debug for KafkaPublisher {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Publisher")
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
    async fn get(host: String, topic: String) -> Result<Vec<Message>, PubSubError> {
        let consumer = Self::configure_consumer(&host, &topic)?;
        let mut offsets = Self::determine_max_offsets(&consumer, &topic)?;

        let mut stream = consumer.stream();
        let mut data: Vec<Message> = Vec::new();
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
    fn configure_consumer(&mut self) -> Result<(), PubSubError> {
        #[cfg(not(test))]
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.host)
            .set("group.id", self.uuid.as_hyphenated().to_string())
            .create::<StreamConsumer>()?;

        // During testing, the low latency of the mock Kafka cluster means that messages are often produced on the broker before the
        // consumer is registered. Setting the auto offset reset to "earliest" ensures we see all messages during the test.
        #[cfg(test)]
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.host)
            .set("group.id", self.uuid.as_hyphenated().to_string())
            .set("auto.offset.reset", "earliest")
            .create::<StreamConsumer>()?;
        consumer.subscribe(&[&self.topic])?;
        self.consumer = Some(consumer);
        Ok(())
    }

    fn convert_stream(initial: KafkaResult<BorrowedMessage>) -> Result<Message, PubSubError> {
        convert_to_message(initial?)
    }

    fn check_connection(&mut self) -> Result<(), PubSubError> {
        if self.consumer.is_none() {
            self.configure_consumer()?;
        }
        Ok(())
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

    fn get_stream(
        &mut self,
    ) -> Result<impl Stream<Item = Result<Message, PubSubError>>, PubSubError> {
        self.check_connection()?;
        let consumer = self.consumer.as_ref().unwrap();
        Ok(consumer.stream().map(Self::convert_stream))
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

fn convert_to_message(incoming: BorrowedMessage) -> Result<Message, PubSubError> {
    let value_bytes = incoming.payload().ok_or_else(PubSubError::default)?;
    let value = String::from_utf8_lossy(value_bytes).to_string();

    let key = incoming
        .key()
        .map(|bytes| String::from_utf8_lossy(bytes).to_string());

    Ok(Message { key, value })
}

fn get_kafka_timeout_val() -> Duration {
    let secs = env_var::get("KAFKA_CONNECTION_SECONDS").or(1);
    Duration::from_secs(secs)
}

#[cfg(test)]
pub mod testing_utils {
    //! Testing Utilities Module
    //!
    //! This module contains useful structures for writing tests against this library.

    use rdkafka::{mocking::MockCluster, producer::DefaultProducerContext};

    /// A set of values for running tests against an instance of [`MockCluster`].
    pub struct Harness<'a> {
        /// The comma-delimited list of addresses generated by the [`MockCluster`].
        pub host: String,

        /// The instance of [`MockCluster`] to target during testing.
        pub mock_cluster: MockCluster<'a, DefaultProducerContext>,

        /// The topic that has been configured on the [`MockCluster`].
        pub topic: String,
    }
    impl<'a> Harness<'a> {
        /// Generates an instance of [`Harness`] with a [`MockCluster`] that has been prepopulated with the specified topic.
        pub fn for_topic(topic: String) -> Self {
            let cluster = MockCluster::new(3).unwrap();
            cluster.create_topic(&topic, 3, 1).unwrap();

            Harness {
                host: cluster.bootstrap_servers(),
                mock_cluster: cluster,
                topic,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{testing_utils::Harness, *};
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn kafka_consumer_and_producer() {
        let test_harness = Harness::for_topic(String::from("test_topic"));

        let mut test_sub =
            KafkaSubscriber::new(test_harness.host.clone(), test_harness.topic.clone());
        let mut stream = test_sub.get_stream().unwrap();

        let message = Message::new(None, "testing".to_string());
        let mut test_pub =
            KafkaPublisher::new(test_harness.host.clone(), test_harness.topic.clone());
        test_pub.publish(message.clone()).await.unwrap();

        assert_eq!(message, stream.next().await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn kafka_snapshot() {
        let test_harness = Harness::for_topic(String::from("test_topic"));

        let message = Message::new(None, "testing".to_string());
        let mut test_pub =
            KafkaPublisher::new(test_harness.host.clone(), test_harness.topic.clone());
        test_pub.publish(message.clone()).await.unwrap();

        let result =
            KafkaSnapshot::get(test_harness.host.clone(), test_harness.topic.clone()).await;
        assert_eq!(vec![message], result.unwrap());
    }
}
