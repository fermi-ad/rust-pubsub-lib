//! Shared cache for subscriber runtimes across all streaming backends.
//!
//! Each `(host, topic)` pair reuses one background streaming runtime within the process. Idle
//! runtimes are evicted after a short grace period once they no longer have active listeners.
//!
//! This module is backend-agnostic: it is used by the Kafka, Redis pub/sub, and Redis Stream
//! subscriber implementations. Each backend supplies its own `start_stream` function via the
//! `stream_task` parameter of [`get_stream`].

use std::{borrow::Cow, collections::HashMap, sync::LazyLock};

use tokio::{
    sync::{
        RwLock,
        broadcast::{Sender, channel},
    },
    time::{Duration, Instant, sleep},
};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{ByteMessage, Message, MessageStream};

/// Interval between idle-runtime cleanup passes.
pub(crate) const REAPER_INTERVAL: Duration = Duration::from_secs(10);
/// Idle duration after which an unused runtime is evicted.
pub(crate) const EVICT_AFTER_IDLE: Duration = Duration::from_secs(60);
/// Default capacity for the internal message channels used by each backend.
///
/// A subscriber that falls behind by more than this many messages will be considered lagged and
/// the oldest unread messages will be dropped with a warning. Callers that require guaranteed
/// delivery should use the corresponding [`Snapshot`](crate::Snapshot) implementation to
/// re-hydrate missed state.
const DEFAULT_CHANNEL_CAPACITY: usize = 100;

/// Shared map of runtimes indexed by [`CacheKey`].
type RuntimeCache = RwLock<HashMap<CacheKey<'static>, CachedRuntime>>;

/// Subscriber runtimes are cached by `(host, topic)` across the process, shared by all backends.
static RUNTIMES_MAP: LazyLock<RuntimeCache> = LazyLock::new(|| RwLock::new(HashMap::new()));
/// Lazily starts the shared background reaper after the first entry is added to the cache.
/// Must be referenced from within a tokio async runtime.
static REAPER_STARTED: LazyLock<()> = LazyLock::new(|| {
    tokio::spawn(reap_unused_streams());
});

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) enum Source {
    Kafka,
    RedisPubSub,
    RedisStream,
}

/// Returns a typed [`MessageStream<M>`] for a host/topic pair, creating the shared background
/// runtime on first use.
///
/// If the broadcast receiver falls behind by more than [`DEFAULT_CHANNEL_CAPACITY`] messages,
/// the lagged items are dropped and a lag warning is logged at `WARN` level.
pub(crate) async fn get_stream<M, F, Fut>(
    host: &str,
    topic: &str,
    source: Source,
    stream_task: F,
) -> MessageStream<M>
where
    M: Message,
    F: FnOnce(CancellationToken, String, String, Sender<ByteMessage>) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let raw = get_raw_stream(host, topic, source, stream_task).await;
    Box::pin(raw.filter_map(move |incoming| match incoming {
        Ok(msg) => Some(M::from(msg)),
        Err(err) => {
            warn!("{source:?} broadcast receiver lagged, messages were dropped: {err:?}");
            None
        }
    }))
}

/// Returns the raw [`BroadcastStream<ByteMessage>`] for a host/topic pair, creating the shared
/// background runtime on first use. Used internally by [`get_stream`].
async fn get_raw_stream<F, Fut>(
    host: &str,
    topic: &str,
    source: Source,
    stream_task: F,
) -> BroadcastStream<ByteMessage>
where
    F: FnOnce(CancellationToken, String, String, Sender<ByteMessage>) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let key = CacheKey::new(host, topic, source);
    if let Some(stream) = RUNTIMES_MAP
        .read()
        .await
        .get(&key)
        .map(|entry| entry.get_stream())
    {
        return stream;
    }

    let mut lock = RUNTIMES_MAP.write().await;
    if let Some(entry) = lock.get(&key) {
        entry.get_stream()
    } else {
        *REAPER_STARTED;
        let entry = CachedRuntime::new();
        tokio::spawn(stream_task(
            entry.cancel_token.child_token(),
            host.to_string(),
            topic.to_string(),
            entry.sender.clone(),
        ));
        let stream = entry.get_stream();
        lock.insert(key.into_owned(), entry);
        stream
    }
}

/// Cache key identifying a shared streaming runtime by host and topic.
/// Used exclusively within this module as an implementation detail of the process-wide cache.
///
/// Uses [`Cow`] fields so lookups only require borrowing, while inserting a new entry can
/// cheaply convert to owned data via [`CacheKey::into_owned`].
#[derive(Hash, PartialEq, Eq)]
struct CacheKey<'a> {
    host: Cow<'a, str>,
    topic: Cow<'a, str>,
    source: Source,
}

impl<'a> CacheKey<'a> {
    fn new(host: &'a str, topic: &'a str, source: Source) -> Self {
        CacheKey {
            host: Cow::Borrowed(host),
            topic: Cow::Borrowed(topic),
            source,
        }
    }

    fn into_owned(self) -> CacheKey<'static> {
        CacheKey {
            host: Cow::Owned(self.host.into_owned()),
            topic: Cow::Owned(self.topic.into_owned()),
            source: self.source,
        }
    }
}

struct CachedRuntime {
    cancel_token: CancellationToken,
    sender: Sender<ByteMessage>,
    last_used: Instant,
}

impl CachedRuntime {
    /// Creates a new cached runtime handle for a host/topic pair.
    fn new() -> Self {
        let (sender, _) = channel(DEFAULT_CHANNEL_CAPACITY);
        let cancel_token = CancellationToken::new();

        Self {
            cancel_token,
            sender,
            last_used: Instant::now(),
        }
    }

    /// Returns a new broadcast-backed stream subscribed to this runtime's fan-out channel.
    fn get_stream(&self) -> BroadcastStream<ByteMessage> {
        BroadcastStream::new(self.sender.subscribe())
    }

    /// Returns the number of active listeners currently attached to this runtime.
    fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Drop for CachedRuntime {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

/// Periodically removes idle cached runtimes that have had no active listeners for
/// [`EVICT_AFTER_IDLE`].
async fn reap_unused_streams() {
    loop {
        sleep(REAPER_INTERVAL).await;

        let now = Instant::now();
        RUNTIMES_MAP.write().await.retain(|_, entry| {
            if entry.receiver_count() > 0 {
                // Update last used time while we have the write lock
                entry.last_used = now;
                true
            } else {
                now.duration_since(entry.last_used) < EVICT_AFTER_IDLE
            }
        });
    }
}

/// Cache tests.
///
/// These tests verify shared runtime caching and lazy startup across backends.
#[cfg(test)]
mod tests {
    use tokio::task::yield_now;

    use super::*;

    async fn dummy_test_runtime(
        cancel: CancellationToken,
        _host: String,
        _topic: String,
        _sender: Sender<ByteMessage>,
    ) {
        cancel.cancelled().await
    }

    #[tokio::test]
    async fn cached_runtime_starts_when_first_receiver_is_requested() {
        let host = "lazy-runtime-host";
        let topic = "lazy-runtime-topic";

        // Use get_raw_stream so the BroadcastStream receiver stays alive for receiver_count.
        let _runtime = get_raw_stream(host, topic, Source::Kafka, dummy_test_runtime).await;
        // Yield to let the spawned background task be scheduled; no real time delay needed.
        yield_now().await;

        let key = CacheKey::new(host, topic, Source::Kafka);
        let lock = RUNTIMES_MAP.read().await;
        let receiver_count = lock
            .get(&key)
            .map(|entry| entry.receiver_count())
            .expect("missing cached runtime");
        assert_eq!(1, receiver_count);
    }

    #[tokio::test]
    async fn same_host_topic_shares_one_cache_entry() {
        let host = "shared-runtime-host";
        let topic = "shared-runtime-topic";
        let source = Source::RedisPubSub;

        // Use get_raw_stream so the BroadcastStream receivers stay alive for receiver_count.
        let _first = get_raw_stream(host, topic, source, dummy_test_runtime).await;
        let _second = get_raw_stream(host, topic, source, dummy_test_runtime).await;

        let key = CacheKey::new(host, topic, source);
        let lock = RUNTIMES_MAP.read().await;
        let entry = lock
            .get(&key)
            .expect("cache entry should exist for shared (host, topic, source)");

        assert_eq!(
            entry.receiver_count(),
            2,
            "both streams should share one runtime — receiver_count should be 2"
        );
        assert_eq!(
            lock.keys()
                .filter(|k| k.host == host && k.topic == topic && k.source == source)
                .count(),
            1,
            "only one cache entry should exist for the same (host, topic, source) tuple"
        );
    }
}
