//! Shared Kafka producer and subscriber-stream caches.
//!
//! This module centralizes per-process reuse for Kafka infrastructure:
//! producer instances are cached by bootstrap host, while subscriber fan-out runtimes are cached by
//! `(host, topic)` pairs. A background reaper removes idle entries after a grace period.

use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use rdkafka::{ClientConfig, producer::FutureProducer};
use tokio::{sync::RwLock, time::sleep};
use tokio_stream::wrappers::BroadcastStream;

use crate::{ByteMessage, PubSubError, kafka_impl::stream::KafkaStream};

#[cfg(test)]
mod tests;

/// Interval between idle-cache cleanup passes.
const REAPER_INTERVAL: Duration = Duration::from_secs(10);
/// Idle duration after which cached entries are evicted.
const EVICT_AFTER_IDLE_SECS: u64 = 60;

/// Shared map of one Kafka [`FutureProducer`] per bootstrap host.
type ProducerCache = RwLock<HashMap<String, ProducerCacheEntry>>;
/// Cache key identifying a shared subscriber runtime by host and topic.
type ConsumerCacheKey = (String, String);
/// Shared map of Kafka subscriber runtimes indexed by [`ConsumerCacheKey`].
type ConsumerCache = RwLock<HashMap<ConsumerCacheKey, ConsumerCacheEntry>>;

/// Kafka producers are cached by bootstrap host across the process.
static PRODUCER_MAP: LazyLock<ProducerCache> = LazyLock::new(RwLock::default);
/// Kafka subscriber streams are cached by `(host, topic)` across the process.
static CONSUMER_MAP: LazyLock<ConsumerCache> = LazyLock::new(|| RwLock::new(HashMap::new()));
/// Lazily starts the shared background reaper the first time a cache is used.
static REAPER_STARTED: LazyLock<()> = LazyLock::new(|| {
    tokio::spawn(reap_unused_streams());
});
/// The [`Instant`] that the process first interacted with the cache.
static START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Returns the shared Kafka subscriber stream for a host/topic pair, creating it on first use.
pub async fn get_kafka_stream(host: String, topic: String) -> BroadcastStream<ByteMessage> {
    let key = (host.clone(), topic.clone());
    if let Some(stream) = CONSUMER_MAP
        .read()
        .await
        .get(&key)
        .map(|entry| entry.kafka_stream.get_stream())
    {
        return stream;
    }

    let mut lock = CONSUMER_MAP.write().await;
    lock.entry(key)
        .or_insert_with(|| {
            *REAPER_STARTED;
            ConsumerCacheEntry {
                kafka_stream: KafkaStream::new(host, topic),
                last_used_epoch_secs: now_secs(),
            }
        })
        .kafka_stream
        .get_stream()
}

/// Returns the cached Kafka producer for a bootstrap host, creating it on first use.
pub async fn get_kafka_producer(host: String) -> Result<FutureProducer, PubSubError> {
    let now = now_secs();

    // Clone the entry under the read lock so `last_used_epoch_secs` (an `Arc<AtomicU64>`) can be
    // updated after the lock is dropped. Cloning is cheap: one `Arc` refcount bump plus an
    // rdkafka-internal `Arc` clone for the `FutureProducer`.
    let read_result = PRODUCER_MAP.read().await.get(&host).cloned();
    if let Some(entry) = read_result {
        entry.last_used_epoch_secs.store(now, Ordering::Relaxed);
        return Ok(entry.producer);
    }

    let mut lock = PRODUCER_MAP.write().await;
    if let Some(entry) = lock.get(&host) {
        entry.last_used_epoch_secs.store(now, Ordering::Relaxed);
        Ok(entry.producer.clone())
    } else {
        let default: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &host)
            .create()?;
        lock.insert(
            host,
            ProducerCacheEntry {
                producer: default.clone(),
                last_used_epoch_secs: Arc::new(AtomicU64::new(now)),
            },
        );
        // Force one-time lazy initialization of the shared reaper so producer-only workloads still
        // clean up idle cached producers even if no subscriber stream is ever requested.
        *REAPER_STARTED;
        Ok(default)
    }
}

struct ConsumerCacheEntry {
    kafka_stream: KafkaStream,
    last_used_epoch_secs: u64,
}
#[derive(Clone)]
struct ProducerCacheEntry {
    producer: FutureProducer,
    last_used_epoch_secs: Arc<AtomicU64>,
}

fn now_secs() -> u64 {
    START.elapsed().as_secs()
}

/// Periodically removes idle cached producers and subscriber runtimes.
async fn reap_unused_streams() {
    loop {
        sleep(REAPER_INTERVAL).await;

        let now = now_secs();
        CONSUMER_MAP.write().await.retain(|_, entry| {
            if entry.kafka_stream.receiver_count() > 0 {
                entry.last_used_epoch_secs = now;
                true
            } else {
                now.saturating_sub(entry.last_used_epoch_secs) < EVICT_AFTER_IDLE_SECS
            }
        });
        PRODUCER_MAP.write().await.retain(|_, entry| {
            now.saturating_sub(entry.last_used_epoch_secs.load(Ordering::Relaxed))
                < EVICT_AFTER_IDLE_SECS
        });
    }
}
