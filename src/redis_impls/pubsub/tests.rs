//! Redis pub/sub implementation tests.
//!
//! These tests cover publishing and subscription behavior for the Redis native pub/sub backend.

use std::collections::HashMap;
use std::time::Duration;

use tokio::time::timeout;
use tokio_stream::StreamExt;

use super::*;
use crate::{Message, RedisTestHarness, StringMessage};

#[tokio::test]
async fn test_publish() {
    let mut context = RedisTestHarness::new(None).await;
    let publisher = RedisPublisher::new(context.get_host(), "test-topic".to_string());
    let message = StringMessage::from_value("Hello, Redis PubSub!".to_string());
    publisher.publish(message).await.unwrap();
    assert!(
        context
            .check_for_message("Hello, Redis PubSub!")
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn test_publish_ignores_message_key() {
    let mut context = RedisTestHarness::new(None).await;
    let publisher = RedisPublisher::new(context.get_host(), "test-topic".to_string());
    let message = StringMessage::new(
        Some("ignored-key".to_string()),
        "Hello, Redis PubSub!".to_string(),
    );

    publisher.publish(message).await.unwrap();

    assert!(
        context
            .check_for_message("Hello, Redis PubSub!")
            .await
            .is_ok()
    );
    assert!(context.check_for_message("ignored-key").await.is_err());
}

#[tokio::test]
async fn test_subscribe_plain_string_payload_round_trips() {
    let context = RedisTestHarness::new(Some(HashMap::from([(
        "test-topic".to_string(),
        vec!["Hello, Redis PubSub!".to_string()],
    )])))
    .await;
    let mut subscriber = RedisSubscriber::new(context.get_host(), "test-topic".to_string());

    let message = timeout(
        Duration::from_secs(5),
        subscriber
            .get_stream::<StringMessage>()
            .await
            .unwrap()
            .take(1)
            .all(|item| item.is_ok_and(|msg| msg.value_ref() == "Hello, Redis PubSub!")),
    )
    .await
    .unwrap();

    assert!(message);
}

#[tokio::test]
async fn test_subscribe_receives_multiple_messages_in_order() {
    let context = RedisTestHarness::new(Some(HashMap::from([(
        "ordered-topic".to_string(),
        vec![
            "first".to_string(),
            "second".to_string(),
            "third".to_string(),
        ],
    )])))
    .await;
    let mut subscriber = RedisSubscriber::new(context.get_host(), "ordered-topic".to_string());

    let received = timeout(
        Duration::from_secs(5),
        subscriber
            .get_stream::<StringMessage>()
            .await
            .unwrap()
            .take(3)
            .collect::<Vec<Result<StringMessage, PubSubError>>>(),
    )
    .await
    .unwrap();

    let messages = received
        .into_iter()
        .map(Result::unwrap)
        .map(StringMessage::extract_value)
        .collect::<Vec<_>>();

    assert_eq!(vec!["first", "second", "third"], messages);
}

#[tokio::test]
async fn test_subscribe_json_looking_payload_remains_plain_text() {
    let json_text = "{\"hello\":[1,2,3]}".to_string();
    let context = RedisTestHarness::new(Some(HashMap::from([(
        "json-payload-topic".to_string(),
        vec![json_text.clone()],
    )])))
    .await;
    let mut subscriber = RedisSubscriber::new(context.get_host(), "json-payload-topic".to_string());

    let message = timeout(
        Duration::from_secs(5),
        subscriber
            .get_stream::<StringMessage>()
            .await
            .unwrap()
            .take(1)
            .next(),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    assert_eq!(json_text, message.extract_value());
}

#[tokio::test]
async fn test_subscribe_fails_for_invalid_host() {
    let mut subscriber =
        RedisSubscriber::new("not-a-valid-redis-uri".to_string(), "topic".to_string());
    let result = subscriber.get_stream::<StringMessage>().await;
    assert!(result.is_err());

    let err = result.err().unwrap();
    assert!(format!("{err}").contains("The PubSub library encountered an error."));
}
