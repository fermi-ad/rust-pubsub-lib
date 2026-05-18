//! Kafka-backed implementations of the crate's core messaging traits.
//!
//! This module provides Kafka implementations for [`Publisher`](crate::Publisher),
//! [`Snapshot`](crate::Snapshot), and [`Subscriber`](crate::Subscriber).
//!
//! The Kafka subscriber path shares a cached stream per host/topic pair within the process so that
//! multiple subscribers can reuse the same background Kafka consumer task. Idle cached producers and
//! streams are eventually cleaned up by an internal reaper task.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::time::Duration;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::FutureRecord;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{ClientConfig, Message as RdMessage};
use rust_env_var_lib::env_var;
use tokio::time::timeout;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use uuid::Uuid;

use crate::{ByteMessage, Message, PubSubError, Publisher, Snapshot, Subscriber};

#[cfg(any(feature = "testing-utils", test))]
pub mod testing_utils;

mod cache;
mod stream;

#[cfg(test)]
mod tests;

const SNAPSHOT_MESSAGE_TIMEOUT: Duration = Duration::from_secs(5);

/// Kafka implementation of [`Publisher`](crate::Publisher).
///
/// Producers are cached per Kafka bootstrap host so repeated publishes can reuse an existing
/// connection instead of constructing a fresh producer every time.
pub struct KafkaPublisher {
    host: String,
    topic: String,
}

#[async_trait::async_trait]
impl Publisher for KafkaPublisher {
    fn new(host: String, topic: String) -> Self {
        Self { host, topic }
    }

    async fn publish<M: Message>(&self, message: M) -> Result<(), PubSubError> {
        let producer = cache::get_kafka_producer(self.host.clone()).await?;
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

impl From<KafkaError> for PubSubError {
    fn from(value: KafkaError) -> Self {
        PubSubError::from_debug(value)
    }
}

/// Kafka implementation of [`Snapshot`](crate::Snapshot).
///
/// A snapshot creates a short-lived consumer, records the current high watermark for each
/// partition, and reads until those offsets have been observed. This means the snapshot includes
/// messages visible up to the discovered per-partition bounds at the time offset discovery
/// completes.
///
/// Messages produced after watermark discovery are not guaranteed to appear. Returned ordering
/// reflects Kafka partition consumption rather than a single global topic order.
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
    async fn get<M: Message>(host: String, topic: String) -> Result<Vec<M>, PubSubError> {
        let consumer = Self::configure_consumer(&host, &topic)?;
        let mut offsets = Self::determine_max_offsets(&consumer, &topic)?;

        let mut stream = consumer.stream();
        let mut data: Vec<M> = Vec::new();
        while !offsets.is_empty() {
            let item = timeout(SNAPSHOT_MESSAGE_TIMEOUT, stream.next())
                .await
                .map_err(PubSubError::from_debug)?;
            let msg_res = item.ok_or_else(|| {
                PubSubError::from(KafkaError::MessageConsumption(
                    RDKafkaErrorCode::PartitionEOF,
                ))
            })?;

            let msg = msg_res?;
            offsets.retain(|k, v| *k != msg.partition() || *v > msg.offset());
            data.push(convert_to_message(msg)?);
        }
        Ok(data)
    }
}

/// Kafka implementation of [`Subscriber`](crate::Subscriber).
///
/// Subscribers reuse a shared [`KafkaStream`](stream::KafkaStream) per host/topic pair. This avoids
/// spinning up duplicate consumers when multiple callers subscribe to the same Kafka topic inside a
/// single process.
///
/// Constructing a subscriber is side-effect free; the shared background consumer runtime is started
/// lazily by the first call to [`Subscriber::get_stream()`](crate::Subscriber::get_stream).
pub struct KafkaSubscriber {
    host: String,
    topic: String,
}

impl KafkaSubscriber {
    fn convert_stream<M: Message>(
        stream: BroadcastStream<ByteMessage>,
    ) -> impl Stream<Item = Result<M, PubSubError>> + Unpin + Send {
        stream.map(|incoming| match incoming {
            Ok(msg) => Ok(M::from(msg)),
            Err(err) => Err(PubSubError::from_debug(err)),
        })
    }
}

#[async_trait::async_trait]
impl Subscriber for KafkaSubscriber {
    fn new(host: String, topic: String) -> Self {
        Self { host, topic }
    }

    async fn get_stream<M: Message>(
        &mut self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>> + Unpin + Send, PubSubError> {
        Ok(Self::convert_stream::<M>(
            cache::get_kafka_stream(self.host.clone(), self.topic.clone()).await,
        ))
    }
}

impl Debug for KafkaSubscriber {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("KafkaSubscriber")
            .field("host", &self.host)
            .field("topic", &self.topic)
            .finish()
    }
}

fn get_kafka_timeout_val() -> Duration {
    let secs = env_var::get("KAFKA_CONNECTION_SECONDS").or(1);
    Duration::from_secs(secs)
}

fn convert_to_message<M: Message>(incoming: BorrowedMessage) -> Result<M, PubSubError> {
    let value = incoming.payload().ok_or_else(PubSubError::default)?;
    let key = incoming.key();

    Ok(M::from_bytes(key, value))
}
