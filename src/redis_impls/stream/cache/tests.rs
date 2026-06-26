//! Redis Stream cache tests.
//!
//! These tests verify shared runtime caching and lazy startup for the Redis Stream subscriber path.

use std::time::Duration;

use tokio::time::sleep;

use super::*;
use crate::{
    StringMessage, Subscriber, redis_impls::stream::RedisSubscriber as StreamRedisSubscriber,
};

#[tokio::test]
async fn redis_stream_cached_runtime_starts_when_first_receiver_is_requested() {
    let host = "redis://lazy-runtime-host";
    let topic = "lazy-runtime-topic";

    let _runtime = get_redis_stream(host, topic).await;
    sleep(Duration::from_millis(200)).await;

    let key = ConsumerCacheKey::new(host, topic);
    let lock = CONSUMER_MAP.read().await;
    let receiver_count = lock
        .get(&key)
        .map(|entry| entry.redis_stream.receiver_count())
        .expect("missing cached Redis Stream runtime");
    assert_eq!(1, receiver_count);
}

#[tokio::test]
async fn redis_stream_cached_runtime_starts_when_subscriber_requests_a_stream() {
    let host = "not-a-valid-redis-uri";
    let topic = "lazy-runtime-start-topic";
    let subscriber = StreamRedisSubscriber::new(host.to_string(), topic.to_string());

    let _stream = subscriber.get_stream::<StringMessage>().await.unwrap();
    sleep(Duration::from_millis(200)).await;

    let key = ConsumerCacheKey::new(host, topic);
    let lock = CONSUMER_MAP.read().await;
    let receiver_count = lock
        .get(&key)
        .map(|entry| entry.redis_stream.receiver_count())
        .expect("missing cached Redis Stream runtime after subscription");
    assert_eq!(1, receiver_count);
}
