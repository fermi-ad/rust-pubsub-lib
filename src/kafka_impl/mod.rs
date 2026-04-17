//! Kafka-backed implementations of the crate's core messaging traits.
//!
//! This module provides Kafka implementations for [`Publisher`](crate::Publisher),
//! [`Snapshot`](crate::Snapshot), and [`Subscriber`](crate::Subscriber).
//!
//! The Kafka subscriber path shares a cached stream per host/topic pair within the process so that
//! multiple subscribers can reuse the same background Kafka consumer task. Idle cached producers and
//! streams are eventually cleaned up by an internal reaper task.

use crate::{ByteMessage, Message, PubSubError, Publisher, Snapshot, Subscriber};
use rdkafka::{
    ClientConfig, Message as RdMessage,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::BorrowedMessage,
    producer::{FutureProducer, FutureRecord},
    types::RDKafkaErrorCode,
};
use rust_env_var_lib::env_var;
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::LazyLock,
    time::{Duration, Instant},
};
use stream::KafkaStream;
use tokio::{
    sync::RwLock,
    time::{sleep, timeout},
};
use tokio_stream::{Stream, StreamExt, wrappers::BroadcastStream};
use uuid::Uuid;

mod stream;

#[cfg(any(feature = "testing-utils", test))]
pub mod testing_utils;

#[cfg(test)]
mod tests;

/// Implementation of the [`Publisher`](crate::Publisher) trait for Kafka connections.
///
/// Producers are cached per Kafka bootstrap host so repeated publishes can reuse an existing
/// connection instead of constructing a fresh producer every time.
pub struct KafkaPublisher {
    host: String,
    topic: String,
}
impl KafkaPublisher {
    async fn get_connection(&self) -> Result<FutureProducer, PubSubError> {
        let mut lock = PRODUCER_MAP.write().await;
        if let Some(entry) = lock.get_mut(&self.host) {
            entry.last_used = Instant::now();
            Ok(entry.data.clone())
        } else {
            let default: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &self.host)
                .create()?;
            lock.insert(
                self.host.clone(),
                CacheEntry {
                    data: default.clone(),
                    last_used: Instant::now(),
                },
            );
            Ok(default)
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

/// Implementation of the [`Snapshot`](crate::Snapshot) trait for Kafka connections.
///
/// A snapshot creates a short-lived consumer, determines the current high watermark for each
/// partition, and then reads until those last known offsets have been observed. This means the
/// snapshot represents the topic contents visible at the time offset discovery completes.
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
        while !offsets.is_empty() {
            let item = timeout(SNAPSHOT_MESSAGE_TIMEOUT, stream.next())
                .await
                .map_err(PubSubError::from_display)?;
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

/// Implementation of the [`Subscriber`](crate::Subscriber) trait for Kafka connections.
///
/// Subscribers reuse a shared [`KafkaStream`](stream::KafkaStream) per host/topic pair. This avoids
/// spinning up duplicate consumers when multiple callers subscribe to the same Kafka topic inside a
/// single process.
pub struct KafkaSubscriber {
    host: String,
    topic: String,
}
impl KafkaSubscriber {
    async fn cached_stream(&self) -> BroadcastStream<ByteMessage> {
        *REAPER_STARTED;
        let key = (self.host.clone(), self.topic.clone());
        if let Some(stream) = CONSUMER_MAP
            .read()
            .await
            .get(&key)
            .map(|entry| entry.data.get_stream())
        {
            return stream;
        }

        let mut lock = CONSUMER_MAP.write().await;

        if let Some(entry) = lock.get(&key) {
            return entry.data.get_stream();
        }

        let entry = lock.entry(key).or_insert_with(|| CacheEntry {
            data: KafkaStream::new(self.host.clone(), self.topic.clone()),
            last_used: Instant::now(),
        });
        entry.data.get_stream()
    }

    fn convert_stream<T, M: Message<T>>(
        stream: BroadcastStream<ByteMessage>,
    ) -> impl Stream<Item = Result<M, PubSubError>> + Unpin + Send {
        stream.map(|incoming| match incoming {
            Ok(msg) => Ok(M::from_bytes(msg.key.as_deref(), &msg.value)),
            Err(err) => Err(PubSubError::from_display(err)),
        })
    }
}
#[async_trait::async_trait]
impl Subscriber for KafkaSubscriber {
    fn new(host: String, topic: String) -> Self {
        Self { host, topic }
    }

    async fn get_stream<T, M: Message<T>>(
        &mut self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>> + Unpin + Send, PubSubError> {
        Ok(Self::convert_stream(self.cached_stream().await))
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

#[derive(Debug)]
struct CacheEntry<T> {
    data: T,
    last_used: Instant,
}

/// [`FutureProducer`] is intended to be a one-per-host construct, shared by all parts of the application that need to produce on that host.
/// As such, we build a static map of the producers to be shared by all instances of [`KafkaPublisher`] that get requested for the same host.
static PRODUCER_MAP: LazyLock<RwLock<HashMap<String, CacheEntry<FutureProducer>>>> =
    LazyLock::new(RwLock::default);

/// Kafka subscriber streams are shared by host/topic across the process.
static CONSUMER_MAP: LazyLock<RwLock<HashMap<(String, String), CacheEntry<KafkaStream>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

static REAPER_STARTED: LazyLock<()> = LazyLock::new(|| {
    tokio::spawn(reap_unused_streams());
});

const REAPER_INTERVAL: Duration = Duration::from_secs(10);
const EVICT_AFTER_IDLE: Duration = Duration::from_secs(60);
const SNAPSHOT_MESSAGE_TIMEOUT: Duration = Duration::from_secs(5);

impl From<KafkaError> for PubSubError {
    fn from(value: KafkaError) -> Self {
        PubSubError::from_display(value)
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

async fn reap_unused_streams() {
    loop {
        sleep(REAPER_INTERVAL).await;

        let now = Instant::now();
        CONSUMER_MAP.write().await.retain(|_, entry| {
            if entry.data.receiver_count() > 0 {
                entry.last_used = now;
                true
            } else {
                now.duration_since(entry.last_used) < EVICT_AFTER_IDLE
            }
        });
        PRODUCER_MAP
            .write()
            .await
            .retain(|_, entry| now.duration_since(entry.last_used) < EVICT_AFTER_IDLE);
    }
}
