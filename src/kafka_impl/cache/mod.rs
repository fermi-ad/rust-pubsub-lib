use crate::{ByteMessage, PubSubError, kafka_impl::stream::KafkaStream};
use rdkafka::{ClientConfig, producer::FutureProducer};
use std::{
    collections::HashMap,
    sync::LazyLock,
    time::{Duration, Instant},
};
use tokio::{sync::RwLock, time::sleep};
use tokio_stream::wrappers::BroadcastStream;

#[cfg(test)]
mod tests;

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
        Ok(default)
    }
}

static PRODUCER_MAP: LazyLock<ProducerCache> = LazyLock::new(RwLock::default);
/// Kafka subscriber streams are shared by host/topic across the process.
static CONSUMER_MAP: LazyLock<ConsumerCache> = LazyLock::new(|| RwLock::new(HashMap::new()));
static REAPER_STARTED: LazyLock<()> = LazyLock::new(|| {
    tokio::spawn(reap_unused_streams());
});
const REAPER_INTERVAL: Duration = Duration::from_secs(10);
const EVICT_AFTER_IDLE: Duration = Duration::from_secs(60);
/// [`FutureProducer`] is intended to be a one-per-host construct, shared by all parts of the application that need to produce on that host.
/// As such, we build a static map of the producers to be shared by all instances of [`KafkaPublisher`] that get requested for the same host.
type ProducerCache = RwLock<HashMap<String, CacheEntry<FutureProducer>>>;
type ConsumerCacheKey = (String, String);
type ConsumerCache = RwLock<HashMap<ConsumerCacheKey, CacheEntry<KafkaStream>>>;

#[derive(Debug)]
struct CacheEntry<T> {
    data: T,
    last_used: Instant,
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
