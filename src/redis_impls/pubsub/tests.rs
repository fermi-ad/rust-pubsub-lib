//! Redis PubSub implementation tests.
//!
//! Tests for the Redis pub/sub implementations of the public traits in this library.

use super::*;
use crate::{Message, RedisTestHarness, StringMessage};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;

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
async fn test_subscribe() {
    let context = RedisTestHarness::new(Some(HashMap::from([(
        "test-topic".to_string(),
        vec!["Hello, Redis PubSub!".to_string()],
    )])))
    .await;
    let mut subscriber = RedisSubscriber::new(context.get_host(), "test-topic".to_string());
    let message = StringMessage::from_value("\"Hello, Redis PubSub!\"".to_string());
    assert!(
        timeout(
            Duration::from_secs(5),
            subscriber
                .get_stream::<String, StringMessage>()
                .await
                .unwrap()
                .take(1)
                .all(|item| item.is_ok_and(|msg| msg == message))
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn test_subscribe_receives_multiple_messages_in_order() {
    let context = RedisTestHarness::new(Some(HashMap::from([(
        "test-topic".to_string(),
        vec![
            "first".to_string(),
            "second".to_string(),
            "third".to_string(),
        ],
    )])))
    .await;
    let mut subscriber = RedisSubscriber::new(context.get_host(), "test-topic".to_string());

    let future = async {
        subscriber
            .get_stream::<String, StringMessage>()
            .await
            .unwrap()
            .take(3)
            .collect::<Vec<Result<StringMessage, PubSubError>>>()
            .await
    };
    let received = timeout(Duration::from_secs(5), future).await.unwrap();

    let messages = received
        .into_iter()
        .map(Result::unwrap)
        .map(|msg| msg.value())
        .collect::<Vec<_>>();

    assert_eq!(
        vec![
            "\"first\"".to_string(),
            "\"second\"".to_string(),
            "\"third\"".to_string()
        ],
        messages
    );
}

#[tokio::test]
async fn test_subscribe_fails_for_invalid_host() {
    let mut subscriber =
        RedisSubscriber::new("not-a-valid-redis-uri".to_string(), "topic".to_string());
    let result = subscriber.get_stream::<String, StringMessage>().await;
    assert!(result.is_err());

    let err = result.err().unwrap();
    assert!(format!("{err}").contains("The PubSub library encountered an error."));
}
