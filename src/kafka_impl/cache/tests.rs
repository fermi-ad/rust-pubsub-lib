//! Kafka cache tests.
//!
//! These tests verify shared-stream caching and lazy runtime startup for Kafka subscribers.

use std::time::Duration;

use tokio::time::sleep;

use super::*;
use crate::{KafkaSubscriber, KafkaTestHarness, StringMessage, Subscriber};

#[tokio::test]
async fn kafka_subscriber_shares_cached_stream_per_host_topic() {
    let topic = String::from("shared_topic");
    let test_harness = KafkaTestHarness::with_topics(vec![topic.clone()]).await;
    let host = test_harness.host().await;

    let mut first = KafkaSubscriber::new(host.clone(), topic.clone());
    let mut second = KafkaSubscriber::new(host.clone(), topic.clone());

    let _stream_a = first.get_stream::<StringMessage>().await.unwrap();
    let _stream_b = second.get_stream::<StringMessage>().await.unwrap();

    let lock = CONSUMER_MAP.read().await;
    let entry = lock
        .get(&(host, topic))
        .expect("missing shared stream cache entry");
    assert_eq!(2, entry.kafka_stream.receiver_count());
}

#[tokio::test]
async fn kafka_cached_stream_starts_when_first_receiver_is_requested() {
    let topic = String::from("lazy_runtime_topic");
    let test_harness = KafkaTestHarness::with_topics(vec![topic.clone()]).await;
    let host = test_harness.host().await;
    let key = (host.clone(), topic.clone());

    let _runtime = get_kafka_stream(host, topic).await;
    sleep(Duration::from_millis(200)).await;

    let lock = CONSUMER_MAP.read().await;
    let entry = lock.get(&key).expect("missing cached Kafka stream runtime");
    assert_eq!(1, entry.kafka_stream.receiver_count());
}

#[tokio::test]
async fn kafka_producer_hot_path_does_not_insert_duplicate_entry() {
    let topic = String::from("producer_hot_path_topic");
    let test_harness = KafkaTestHarness::with_topics(vec![topic.clone()]).await;
    let host = test_harness.host().await;

    // First call: cache miss — inserts the entry.
    let _ = get_kafka_producer(host.clone()).await.unwrap();
    assert!(
        PRODUCER_MAP.read().await.contains_key(&host),
        "entry must exist after first call"
    );

    // Second call: cache hit — must reuse the existing entry. We verify this by checking that
    // the Arc pointer to `last_used_epoch_secs` is the same object, not a freshly allocated one.
    let arc_before = PRODUCER_MAP
        .read()
        .await
        .get(&host)
        .map(|e| Arc::as_ptr(&e.last_used_epoch_secs))
        .expect("entry must exist before second call");

    let _ = get_kafka_producer(host.clone()).await.unwrap();

    let arc_after = PRODUCER_MAP
        .read()
        .await
        .get(&host)
        .map(|e| Arc::as_ptr(&e.last_used_epoch_secs))
        .expect("entry must still exist after second call");

    assert_eq!(
        arc_before, arc_after,
        "second get_kafka_producer call must reuse the cached entry, not replace it"
    );
}
