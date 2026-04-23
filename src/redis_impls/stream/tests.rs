//! Redis Stream implementation tests.
//!
//! These tests cover publishing, snapshots, subscriber fan-out, cache reuse, idle eviction, and
//! stream-entry conversion behavior for the Redis Stream backend.

use std::time::Duration;

use serde_json::json;
use tokio::time::{sleep, timeout};
use tokio_stream::StreamExt;

use super::*;
use crate::{Message, RedisTestHarness, StringMessage};

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
            .any(|msg| msg.value_ref().contains("snapshot payload"))
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
    assert!(message.value_ref().contains("live payload"));
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

    assert!(first_msg.value_ref().contains("fanout payload"));
    assert!(second_msg.value_ref().contains("fanout payload"));
}

#[tokio::test]
async fn redis_stream_reuses_one_cached_stream_per_host_and_topic() {
    let host = "not-a-valid-redis-uri".to_string();
    let topic = "shared-topic-reuse".to_string();

    let first = cache::get_redis_stream(host.clone(), topic.clone()).await;
    let second = cache::get_redis_stream(host.clone(), topic.clone()).await;

    drop(first);
    drop(second);

    let mut subscriber = RedisSubscriber::new(host, topic);
    let mut stream = subscriber
        .get_stream::<String, StringMessage>()
        .await
        .unwrap();

    let err = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .expect_err("expected the reused cached runtime to forward its connection failure");

    assert!(
        err.cause_message()
            .is_some_and(|cause| cause.contains("Redis URL did not parse"))
    );
}

#[tokio::test]
async fn redis_stream_subscriber_new_does_not_start_work_eagerly() {
    let host = "redis://lazy-host-never-started".to_string();
    let topic = "lazy-topic-never-started".to_string();

    let _subscriber = RedisSubscriber::new(host, topic);

    sleep(Duration::from_millis(300)).await;
}

#[tokio::test]
async fn redis_stream_cached_stream_is_evicted_after_going_idle() {
    let host = "not-a-valid-redis-uri".to_string();
    let topic = "idle-topic-eviction".to_string();

    {
        let stream = cache::get_redis_stream(host.clone(), topic.clone()).await;
        drop(stream);
    }

    sleep(Duration::from_secs(4)).await;

    let mut subscriber = RedisSubscriber::new(host, topic);
    let mut stream = subscriber
        .get_stream::<String, StringMessage>()
        .await
        .unwrap();

    let err = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .expect_err("expected a fresh runtime to report connection failure after idle eviction");

    assert!(
        err.cause_message()
            .is_some_and(|cause| cause.contains("Redis URL did not parse"))
    );
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

    assert!(
        err.cause_message()
            .is_some_and(|cause| cause.contains("Redis URL did not parse"))
    );
}

#[test]
fn stream_entry_to_json_bytes_preserves_nested_shapes() {
    let entry = StreamId {
        id: "1-0".to_string(),
        map: vec![(
            "outer".to_string(),
            Value::Map(vec![(
                Value::SimpleString("inner".to_string()),
                Value::Array(vec![
                    Value::SimpleString("leaf".to_string()),
                    Value::Int(42),
                ]),
            )]),
        )]
        .into_iter()
        .collect(),
        milliseconds_elapsed_from_delivery: None,
        delivered_count: None,
    };

    let payload = stream_entry_to_json_bytes(&entry).unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&payload).unwrap();

    assert_eq!(json!({"outer": {"inner": ["leaf", "42"]}}), decoded);
}

#[test]
fn stream_entry_to_byte_message_materializes_json_bytes() {
    let entry = StreamId {
        id: "1-0".to_string(),
        map: vec![(
            "data".to_string(),
            Value::SimpleString("payload".to_string()),
        )]
        .into_iter()
        .collect(),
        milliseconds_elapsed_from_delivery: None,
        delivered_count: None,
    };

    let message = stream_entry_to_byte_message(&entry).unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(message.value_ref()).unwrap();

    assert_eq!(json!({"data": "payload"}), decoded);
}
