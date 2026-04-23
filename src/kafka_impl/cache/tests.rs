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

    let _stream_a = first.get_stream::<String, StringMessage>().await.unwrap();
    let _stream_b = second.get_stream::<String, StringMessage>().await.unwrap();

    let lock = CONSUMER_MAP.read().await;
    let entry = lock
        .get(&(host, topic))
        .expect("missing shared stream cache entry");
    assert_eq!(2, entry.data.receiver_count());
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
    assert_eq!(1, entry.data.receiver_count());
}
