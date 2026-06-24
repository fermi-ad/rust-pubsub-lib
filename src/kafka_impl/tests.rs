//! Kafka implementation tests.
//!
//! These tests cover formatting, error conversion, publishing, subscription, and snapshot behavior
//! for the Kafka-backed implementations.

use tokio::time::{Duration, timeout};
use tokio_stream::StreamExt;

use super::*;
use crate::{KafkaPublisher, KafkaTestHarness, StringMessage};

#[test]
fn format_kafka_publisher() {
    let host = String::from("host");
    let topic = String::from("topic");
    let test_pub = KafkaPublisher::new(host.clone(), topic.clone());
    assert_eq!(
        "KafkaPublisher { host: \"host\", topic: \"topic\" }",
        format!("{test_pub:?}")
    );
}

#[test]
fn format_kafka_subscriber() {
    let topic = String::from("topic");
    let test_sub = KafkaSubscriber::new("host".to_string(), topic.clone());
    assert_eq!(
        "KafkaSubscriber { host: \"host\", topic: \"topic\" }",
        format!("{test_sub:?}")
    );
}

#[test]
fn from_kafka_error() {
    let expected = PubSubError::from_debug(KafkaError::Canceled);
    let result = PubSubError::from(KafkaError::Canceled);
    assert_eq!(format!("{expected}"), format!("{result}"));
}

#[tokio::test]
async fn kafka_consumer_and_producer() {
    let topic = String::from("test_topic");
    let test_harness = KafkaTestHarness::with_topics(vec![topic.clone()]).await;
    let host = test_harness.host().await;

    let test_sub = KafkaSubscriber::new(host.clone(), topic.clone());
    let mut stream = test_sub.get_stream().await.unwrap();

    let message = StringMessage::from_value("testing".to_string());
    let test_pub = KafkaPublisher::new(host, topic);
    test_pub.publish(message.clone()).await.unwrap();

    assert_eq!(message, stream.next().await.unwrap().unwrap());
}

#[tokio::test]
async fn kafka_snapshot() {
    let topic = String::from("test_topic");
    let test_harness = KafkaTestHarness::with_topics(vec![topic.clone()]).await;
    let host = test_harness.host().await;

    let message = StringMessage::new(None, "testing".to_string());
    let test_pub = KafkaPublisher::new(host.clone(), topic.clone());
    test_pub.publish(message.clone()).await.unwrap();

    let result = KafkaSnapshot::get(host, topic).await;
    assert!(result.unwrap().contains(&message));
}

#[tokio::test]
async fn kafka_snapshot_empty_topic_returns_empty_without_hanging() {
    let topic = String::from("empty_snapshot_topic");
    let test_harness = KafkaTestHarness::with_topics(vec![topic.clone()]).await;
    let host = test_harness.host().await;

    let result = timeout(
        Duration::from_secs(2),
        KafkaSnapshot::get::<StringMessage>(host, topic),
    )
    .await
    .expect("snapshot call timed out")
    .unwrap();

    assert!(result.is_empty());
}

#[tokio::test]
async fn kafka_producer_creation_starts_reaper_path() {
    let topic = String::from("producer_reaper_topic");
    let test_harness = KafkaTestHarness::with_topics(vec![topic.clone()]).await;
    let host = test_harness.host().await;

    let publisher = KafkaPublisher::new(host, topic);
    publisher
        .publish(StringMessage::from_value("testing".to_string()))
        .await
        .unwrap();
}
