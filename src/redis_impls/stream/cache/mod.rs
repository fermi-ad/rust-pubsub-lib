//! Shared cache for Redis Stream subscriber runtimes.
//!
//! Each `(host, topic)` pair reuses one background polling runtime within the process. Idle runtimes
//! are evicted after a short grace period once they no longer have listeners.

use std::{
    collections::HashMap,
    sync::LazyLock,
    time::{Duration, Instant},
};

use tokio::{sync::RwLock, time::sleep};
use tokio_stream::wrappers::BroadcastStream;

use super::runtime::RedisStream;

#[cfg(test)]
mod tests;

/// Interval between idle-runtime cleanup passes.
const REAPER_INTERVAL: Duration = Duration::from_secs(1);
/// Idle duration after which an unused runtime is evicted.
const EVICT_AFTER_IDLE: Duration = Duration::from_secs(2);

/// Cache key identifying a shared Redis Stream runtime by host and topic.
type ConsumerCacheKey = (String, String);
/// Shared map of Redis Stream runtimes indexed by [`ConsumerCacheKey`].
type ConsumerCache = RwLock<HashMap<ConsumerCacheKey, CacheEntry<RedisStream>>>;

/// Redis Stream runtimes are cached by `(host, topic)` across the process.
#[cfg(test)]
pub(super) static CONSUMER_MAP: LazyLock<ConsumerCache> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
#[cfg(not(test))]
static CONSUMER_MAP: LazyLock<ConsumerCache> = LazyLock::new(|| RwLock::new(HashMap::new()));
/// Lazily starts the shared background reaper the first time the cache is used.
static REAPER_STARTED: LazyLock<()> = LazyLock::new(|| {
    tokio::spawn(reap_unused_streams());
});

/// Returns the shared Redis Stream runtime for a host/topic pair, creating it on first use.
pub(crate) async fn get_redis_stream(
    host: String,
    topic: String,
) -> BroadcastStream<Result<crate::ByteMessage, crate::PubSubError>> {
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
                data: RedisStream::new(host, topic),
                last_used: Instant::now(),
            }
        })
        .data
        .get_stream()
}

#[derive(Debug)]
pub(super) struct CacheEntry<T> {
    pub(super) data: T,
    last_used: Instant,
}

/// Periodically removes idle cached Redis Stream runtimes.
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
    }
}
