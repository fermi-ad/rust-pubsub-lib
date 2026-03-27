//! The tests for the Kafka Implementation Module

use super::*;
use crate::{ByteMessage, StringMessage, kafka_impl::testing_utils::Harness};
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

#[tokio::test]
async fn format_kafka_subscriber() {
    let topic = String::from("topic");
    let harness = Harness::with_topics(vec![topic.clone()]).await;

    let mut test_sub = KafkaSubscriber::new(harness.host(), topic.clone());
    assert_eq!(
        format!(
            "KafkaSubscriber {{ consumer: \"None\", host: \"{}\", topic: \"{topic}\", uuid: \"{}\" }}",
            harness.host(),
            test_sub.uuid.as_hyphenated()
        ),
        format!("{test_sub:?}")
    );

    let _ = test_sub.get_stream::<Vec<u8>, ByteMessage>();
    assert_eq!(
        format!(
            "KafkaSubscriber {{ consumer: \"Some(StreamConsumer)\", host: \"{}\", topic: \"{topic}\", uuid: \"{}\" }}",
            harness.host(),
            test_sub.uuid.as_hyphenated()
        ),
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
    let mut stream = test_sub.get_stream().unwrap();

    let message = StringMessage::from_value("testing".to_string());
    let test_pub = KafkaPublisher::new(test_harness.host(), topic);
    test_pub.publish(message.clone()).await.unwrap();

    assert_eq!(message, stream.next().await.unwrap().unwrap());
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
