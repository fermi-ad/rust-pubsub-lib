//! Shared Kafka producer and subscriber-stream caches.
//!
//! This module centralizes per-process reuse for Kafka infrastructure:
//! producer instances are cached by bootstrap host, while subscriber fan-out runtimes are cached by
//! `(host, topic)` pairs. A background reaper removes idle entries after a grace period.

use std::{
    collections::HashMap,
    sync::LazyLock,
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
const EVICT_AFTER_IDLE: Duration = Duration::from_secs(60);

/// Shared map of one Kafka [`FutureProducer`] per bootstrap host.
type ProducerCache = RwLock<HashMap<String, CacheEntry<FutureProducer>>>;
/// Cache key identifying a shared subscriber runtime by host and topic.
type ConsumerCacheKey = (String, String);
/// Shared map of Kafka subscriber runtimes indexed by [`ConsumerCacheKey`].
type ConsumerCache = RwLock<HashMap<ConsumerCacheKey, CacheEntry<KafkaStream>>>;

/// Kafka producers are cached by bootstrap host across the process.
static PRODUCER_MAP: LazyLock<ProducerCache> = LazyLock::new(RwLock::default);
/// Kafka subscriber streams are cached by `(host, topic)` across the process.
static CONSUMER_MAP: LazyLock<ConsumerCache> = LazyLock::new(|| RwLock::new(HashMap::new()));
/// Lazily starts the shared background reaper the first time a cache is used.
static REAPER_STARTED: LazyLock<()> = LazyLock::new(|| {
    tokio::spawn(reap_unused_streams());
});

/// Returns the shared Kafka subscriber stream for a host/topic pair, creating it on first use.
pub async fn get_kafka_stream(host: String, topic: String) -> BroadcastStream<ByteMessage> {
    let key = (host.clone(), topic.clone());
    if let Some(stream) = CONSUMER_MAP
        .read()
        .await
        .get(&key)
        .map(|entry| entry.data.get_stream())
    {
        return stream;
    }

    let mut lock = CONSUMER_MAP.write().await;
    lock.entry(key)
        .or_insert_with(|| {
            *REAPER_STARTED;
            CacheEntry {
                data: KafkaStream::new(host, topic),
                last_used: Instant::now(),
            }
        })
        .data
        .get_stream()
}

/// Returns the cached Kafka producer for a bootstrap host, creating it on first use.
pub async fn get_kafka_producer(host: String) -> Result<FutureProducer, PubSubError> {
    let mut lock = PRODUCER_MAP.write().await;
    if let Some(entry) = lock.get_mut(&host) {
        entry.last_used = Instant::now();
        Ok(entry.data.clone())
    } else {
        let default: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &host)
            .create()?;
        lock.insert(
            host,
            CacheEntry {
                data: default.clone(),
                last_used: Instant::now(),
            },
        );
        // Force one-time lazy initialization of the shared reaper so producer-only workloads still
        // clean up idle cached producers even if no subscriber stream is ever requested.
        *REAPER_STARTED;
        Ok(default)
    }
}

#[derive(Debug)]
struct CacheEntry<T> {
    data: T,
    last_used: Instant,
}

/// Periodically removes idle cached producers and subscriber runtimes.
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
