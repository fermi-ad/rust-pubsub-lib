//! Shared Kafka producer cache.
//!
//! Producer instances are cached by bootstrap host so repeated publishes reuse an existing
//! connection rather than constructing a fresh producer every time. A background reaper removes
//! idle entries after a grace period.
//!
//! Subscriber fan-out runtimes are managed by the shared [`crate::cache`] module.

use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use rdkafka::{ClientConfig, producer::FutureProducer};
use tokio::{sync::RwLock, time::sleep};

use crate::PubSubError;
use crate::cache::{EVICT_AFTER_IDLE, REAPER_INTERVAL};

#[cfg(test)]
mod tests;

//                              -------- Cache entries --------
//   Producer "last used" values must be updated on each read, as we don't have the same
//   "check how many open streams there are" ability for producers as we do with consumers.
//   Therefore, the field is an Arc<AtomicU64> so the last used time can be updated without
//   write-locking the cache on every "get_producer" call.
#[derive(Clone)]
struct ProducerCacheEntry {
    producer: FutureProducer,
    last_used_epoch_secs: Arc<AtomicU64>,
}

/// Shared map of one Kafka [`FutureProducer`] per bootstrap host.
type ProducerCache = RwLock<HashMap<String, ProducerCacheEntry>>;

/// Kafka producers are cached by bootstrap host across the process.
static PRODUCER_MAP: LazyLock<ProducerCache> = LazyLock::new(RwLock::default);
/// Lazily starts the shared background reaper the first time a cache is used.
static REAPER_STARTED: LazyLock<()> = LazyLock::new(|| {
    tokio::spawn(reap_unused_streams());
});
/// The [`Instant`] that the process first interacted with the cache.
static START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Returns the cached Kafka producer for a bootstrap host, creating it on first use.
pub async fn get_kafka_producer(host: &str) -> Result<FutureProducer, PubSubError> {
    let now = now_secs();

    // Clone the entry under the read lock so `last_used_epoch_secs` (an `Arc<AtomicU64>`) can be
    // updated after the lock is dropped. Cloning is cheap: one `Arc` refcount bump plus an
    // rdkafka-internal `Arc` clone for the `FutureProducer`.
    let read_result = PRODUCER_MAP.read().await.get(host).cloned();
    if let Some(entry) = read_result {
        entry.last_used_epoch_secs.store(now, Ordering::Relaxed);
        return Ok(entry.producer);
    }

    let mut lock = PRODUCER_MAP.write().await;
    if let Some(entry) = lock.get(host) {
        entry.last_used_epoch_secs.store(now, Ordering::Relaxed);
        Ok(entry.producer.clone())
    } else {
        let default: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", host)
            .create()?;
        lock.insert(
            host.to_string(),
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

fn now_secs() -> u64 {
    START.elapsed().as_secs()
}

/// Periodically removes idle cached producers.
async fn reap_unused_streams() {
    let evict_after_secs = EVICT_AFTER_IDLE.as_secs();
    loop {
        sleep(REAPER_INTERVAL).await;

        let now = now_secs();
        PRODUCER_MAP.write().await.retain(|_, entry| {
            now.saturating_sub(entry.last_used_epoch_secs.load(Ordering::Relaxed))
                < evict_after_secs
        });
    }
}
