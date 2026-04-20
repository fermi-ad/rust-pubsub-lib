//! Redis Stream implementation tests.

use super::*;
use crate::{Message, RedisTestHarness, StringMessage};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_stream::StreamExt;

#[tokio::test]
async fn redis_stream_publish_records_payload_on_mock_server() {
    let mut context = RedisTestHarness::new(None).await;
    let publisher = RedisPublisher::new(context.get_host(), "stream-topic".to_string());

    publisher
        .publish(StringMessage::new(
            Some("ignored-key".to_string()),
            "stream payload".to_string(),
        ))
        .await
        .unwrap();

    assert!(context.check_for_message("stream payload").await.is_ok());
    assert!(context.check_for_message("ignored-key").await.is_err());
}

#[tokio::test]
async fn redis_stream_snapshot_is_empty_for_unseen_stream() {
    let context = RedisTestHarness::new(None).await;

    let snapshot = RedisSnapshot::get::<String, StringMessage>(
        context.get_host(),
        "empty-stream-topic".to_string(),
    )
    .await
    .unwrap();

    assert!(snapshot.is_empty());
}

#[tokio::test]
async fn redis_stream_snapshot_returns_existing_entries() {
    let mut context = RedisTestHarness::new(None).await;
    let host = context.get_host();
    let publisher = RedisPublisher::new(host.clone(), "snapshot-stream-topic".to_string());

    publisher
        .publish(StringMessage::from_value("snapshot payload".to_string()))
        .await
        .unwrap();
    assert!(context.check_for_message("snapshot payload").await.is_ok());
    sleep(Duration::from_millis(100)).await;

    let snapshot =
        RedisSnapshot::get::<String, StringMessage>(host, "snapshot-stream-topic".to_string())
            .await
            .unwrap();

    assert!(
        snapshot
            .iter()
            .any(|msg| msg.value().contains("snapshot payload"))
    );
}

#[tokio::test]
async fn redis_stream_subscriber_receives_messages() {
    let mut context = RedisTestHarness::new(None).await;
    let host = context.get_host();
    let topic = "subscriber-stream-topic".to_string();
    let mut subscriber = RedisSubscriber::new(host.clone(), topic.clone());
    let publisher = RedisPublisher::new(host, topic);

    let mut stream = subscriber
        .get_stream::<String, StringMessage>()
        .await
        .unwrap();
    publisher
        .publish(StringMessage::from_value("live payload".to_string()))
        .await
        .unwrap();
    assert!(context.check_for_message("live payload").await.is_ok());

    let message = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(message.value().contains("live payload"));
}

#[tokio::test]
async fn redis_stream_fans_out_to_multiple_subscribers() {
    let mut context = RedisTestHarness::new(None).await;
    let host = context.get_host();
    let topic = "fanout-stream-topic".to_string();
    let mut first = RedisSubscriber::new(host.clone(), topic.clone());
    let mut second = RedisSubscriber::new(host.clone(), topic.clone());
    let publisher = RedisPublisher::new(host, topic);

    let mut stream_a = first.get_stream::<String, StringMessage>().await.unwrap();
    let mut stream_b = second.get_stream::<String, StringMessage>().await.unwrap();

    publisher
        .publish(StringMessage::from_value("fanout payload".to_string()))
        .await
        .unwrap();
    assert!(context.check_for_message("fanout payload").await.is_ok());

    let first_msg = timeout(Duration::from_secs(5), stream_a.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let second_msg = timeout(Duration::from_secs(5), stream_b.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert!(first_msg.value().contains("fanout payload"));
    assert!(second_msg.value().contains("fanout payload"));
}

#[tokio::test]
async fn redis_stream_subscriber_reports_connection_failure() {
    let mut subscriber =
        RedisSubscriber::new("not-a-valid-redis-uri".to_string(), "topic".to_string());
    let mut stream = subscriber
        .get_stream::<String, StringMessage>()
        .await
        .unwrap();

    let err = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .expect_err("expected a propagated connection error");

    assert!(format!("{err}").contains("The PubSub library encountered an error."));
}
