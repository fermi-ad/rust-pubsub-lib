//! The tests for the Kafka Implementation Module

use super::*;
use crate::{StringMessage, kafka_impl::testing_utils::Harness};
use tokio::time::{Duration, timeout};
use tokio_stream::StreamExt;

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
    let expected = PubSubError::from_display(KafkaError::Canceled);
    let result = PubSubError::from(KafkaError::Canceled);
    assert_eq!(format!("{expected}"), format!("{result}"));
}

#[tokio::test]
async fn kafka_consumer_and_producer() {
    let topic = String::from("test_topic");
    let test_harness = Harness::with_topics(vec![topic.clone()]).await;

    let mut test_sub = KafkaSubscriber::new(test_harness.host(), topic.clone());
    let mut stream = test_sub.get_stream().await.unwrap();

    let message = StringMessage::from_value("testing".to_string());
    let test_pub = KafkaPublisher::new(test_harness.host(), topic);
    test_pub.publish(message.clone()).await.unwrap();

    assert_eq!(message, stream.next().await.unwrap().unwrap());
}

#[tokio::test]
async fn kafka_subscriber_shares_cached_stream_per_host_topic() {
    let topic = String::from("shared_topic");
    let test_harness = Harness::with_topics(vec![topic.clone()]).await;
    let host = test_harness.host();

    let mut first = KafkaSubscriber::new(host.clone(), topic.clone());
    let mut second = KafkaSubscriber::new(host.clone(), topic.clone());

    let _stream_a = first.get_stream::<String, StringMessage>().await.unwrap();
    let _stream_b = second.get_stream::<String, StringMessage>().await.unwrap();

    let lock = CONSUMER_MAP.read().await;
    let entry = lock
        .get(&(host, topic))
        .expect("missing shared stream cache entry");
    assert_eq!(2, entry.stream.receiver_count());
}

#[tokio::test]
async fn kafka_snapshot() {
    let topic = String::from("test_topic");
    let test_harness = Harness::with_topics(vec![topic.clone()]).await;

    let message = StringMessage::new(None, "testing".to_string());
    let test_pub = KafkaPublisher::new(test_harness.host(), topic.clone());
    test_pub.publish(message.clone()).await.unwrap();

    let result = KafkaSnapshot::get(test_harness.host(), topic).await;
    assert!(result.unwrap().contains(&message));
}

#[tokio::test]
async fn kafka_snapshot_empty_topic_returns_empty_without_hanging() {
    let topic = String::from("empty_snapshot_topic");
    let test_harness = Harness::with_topics(vec![topic.clone()]).await;

    let result = timeout(
        Duration::from_secs(2),
        KafkaSnapshot::get::<String, StringMessage>(test_harness.host(), topic),
    )
    .await
    .expect("snapshot call timed out")
    .unwrap();

    assert!(result.is_empty());
}
