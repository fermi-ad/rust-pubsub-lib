//! Shared cache for Redis Stream subscriber runtimes.
//!
//! Each `(host, topic)` pair reuses one background polling runtime within the process. Idle runtimes
//! are evicted after a short grace period once they no longer have listeners.

use std::{
    borrow::Cow,
    collections::HashMap,
    sync::LazyLock,
    time::{Duration, Instant},
};

use tokio::{sync::RwLock, time::sleep};
use tokio_stream::wrappers::BroadcastStream;

use crate::{ByteMessage, PubSubError};

use super::runtime::RedisStream;

#[cfg(test)]
mod tests;

/// Interval between idle-runtime cleanup passes.
const REAPER_INTERVAL: Duration = Duration::from_secs(1);
/// Idle duration after which an unused runtime is evicted.
const EVICT_AFTER_IDLE: Duration = Duration::from_secs(2);

/// Cache key identifying a shared Redis Stream runtime by host and topic.
/// Used exclusively within this module as an implementation detail of the runtime-wide cache.
///
/// Utilizes [`Cow`] fields so lookups only require borrowing a value, while inserting new
/// Redis Stream runtimes can easily convert to owned data.
#[derive(Hash, PartialEq, Eq)]
struct ConsumerCacheKey<'a> {
    host: Cow<'a, str>,
    topic: Cow<'a, str>,
}

impl<'a> ConsumerCacheKey<'a> {
    fn new(host: &'a str, topic: &'a str) -> Self {
        ConsumerCacheKey {
            host: Cow::Borrowed(host),
            topic: Cow::Borrowed(topic),
        }
    }

    fn into_owned(self) -> ConsumerCacheKey<'static> {
        ConsumerCacheKey {
            host: Cow::Owned(self.host.into_owned()),
            topic: Cow::Owned(self.topic.into_owned()),
        }
    }
}

struct CacheEntry {
    redis_stream: RedisStream,
    last_used: Instant,
}

/// Shared map of Redis Stream runtimes indexed by [`ConsumerCacheKey`].
type ConsumerCache = RwLock<HashMap<ConsumerCacheKey<'static>, CacheEntry>>;

/// Redis Stream runtimes are cached by `(host, topic)` across the process.
static CONSUMER_MAP: LazyLock<ConsumerCache> = LazyLock::new(|| RwLock::new(HashMap::new()));
/// Lazily starts the shared background reaper after the first entry is added to the cache.
static REAPER_STARTED: LazyLock<()> = LazyLock::new(|| {
    tokio::spawn(reap_unused_streams());
});

/// Returns the shared Redis Stream runtime for a host/topic pair, creating it on first use.
pub(crate) async fn get_redis_stream(
    host: &str,
    topic: &str,
) -> BroadcastStream<Result<ByteMessage, PubSubError>> {
    let key = ConsumerCacheKey::new(host, topic);
    if let Some(stream) = CONSUMER_MAP
        .read()
        .await
        .get(&key)
        .map(|entry| entry.redis_stream.get_stream())
    {
        return stream;
    }

    let mut lock = CONSUMER_MAP.write().await;
    if let Some(entry) = lock.get(&key) {
        entry.redis_stream.get_stream()
    } else {
        *REAPER_STARTED;
        let entry = CacheEntry {
            redis_stream: RedisStream::new(host.to_string(), topic.to_string()),
            last_used: Instant::now(),
        };
        let stream = entry.redis_stream.get_stream();
        lock.insert(key.into_owned(), entry);
        stream
    }
}

/// Periodically removes idle cached Redis Stream runtimes.
async fn reap_unused_streams() {
    loop {
        sleep(REAPER_INTERVAL).await;

        let now = Instant::now();
        CONSUMER_MAP.write().await.retain(|_, entry| {
            if entry.redis_stream.receiver_count() > 0 {
                entry.last_used = now;
                true
            } else {
                now.duration_since(entry.last_used) < EVICT_AFTER_IDLE
            }
        });
    }
}
