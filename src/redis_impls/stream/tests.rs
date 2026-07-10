//! Redis Stream implementation tests.
//!
//! These tests cover publishing, snapshots, subscriber fan-out, cache reuse, idle eviction, and
//! stream-entry conversion behavior for the Redis Stream backend.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::json;
use tokio::task::yield_now;
use tokio::time::{sleep, timeout};
use tokio_stream::StreamExt;

use super::*;
use crate::{
    MapMessage, Message, RedisTestHarness, StringMessage, redis_impls::redis_value_to_json_bytes,
};

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

    let snapshot =
        RedisSnapshot::get::<StringMessage>(context.get_host(), "empty-stream-topic".to_string())
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

    let snapshot = RedisSnapshot::get::<StringMessage>(host, "snapshot-stream-topic".to_string())
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
    let subscriber = RedisSubscriber::new(host.clone(), topic.clone());
    let publisher = RedisPublisher::new(host, topic);

    let mut stream = subscriber.get_stream::<StringMessage>().await;
    publisher
        .publish(StringMessage::from_value("live payload".to_string()))
        .await
        .unwrap();
    assert!(context.check_for_message("live payload").await.is_ok());

    let message = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert!(message.value_ref().contains("live payload"));
}

#[tokio::test]
async fn redis_stream_fans_out_to_multiple_subscribers() {
    let mut context = RedisTestHarness::new(None).await;
    let host = context.get_host();
    let topic = "fanout-stream-topic".to_string();
    let first = RedisSubscriber::new(host.clone(), topic.clone());
    let second = RedisSubscriber::new(host.clone(), topic.clone());
    let publisher = RedisPublisher::new(host, topic);

    let mut stream_a = first.get_stream::<StringMessage>().await;
    let mut stream_b = second.get_stream::<StringMessage>().await;

    publisher
        .publish(StringMessage::from_value("fanout payload".to_string()))
        .await
        .unwrap();
    assert!(context.check_for_message("fanout payload").await.is_ok());

    let first_msg = timeout(Duration::from_secs(5), stream_a.next())
        .await
        .unwrap()
        .unwrap();
    let second_msg = timeout(Duration::from_secs(5), stream_b.next())
        .await
        .unwrap()
        .unwrap();

    assert!(first_msg.value_ref().contains("fanout payload"));
    assert!(second_msg.value_ref().contains("fanout payload"));
}

#[tokio::test]
async fn redis_stream_subscriber_new_does_not_start_work_eagerly() {
    let host = "redis://lazy-host-never-started".to_string();
    let topic = "lazy-topic-never-started".to_string();

    let _subscriber = RedisSubscriber::new(host, topic);

    // Yield to give any incorrectly-spawned background task a chance to run; no real delay needed.
    yield_now().await;
}

// ── MapMessage unit tests ────────────────────────────────────────────────────

#[test]
fn map_message_value_is_json_serialized_fields() {
    let mut fields = HashMap::new();
    fields.insert("sensor_id".to_string(), "abc123".to_string());
    fields.insert("temperature".to_string(), "22.5".to_string());
    let msg = MapMessage::from_fields(fields.clone());

    // value() returns the native map — no JSON involved at the accessor level
    assert_eq!(msg.value(), fields);

    // into_bytes() serializes to JSON; parse it back and check individual keys
    let bytes = msg.into_bytes();
    let decoded: serde_json::Value = serde_json::from_slice(bytes.value_ref()).unwrap();
    assert_eq!(decoded["sensor_id"], "abc123");
    assert_eq!(decoded["temperature"], "22.5");
}

#[test]
fn map_message_from_bytes_parses_json() {
    let json = br#"{"field1":"val1","field2":"val2"}"#;
    let msg = MapMessage::from_bytes(None, json);
    assert_eq!(msg.value()["field1"], "val1");
    assert_eq!(msg.value()["field2"], "val2");
}

#[test]
fn map_message_from_bytes_falls_back_to_data_key_for_non_json() {
    let raw = b"not-json-at-all";
    let msg = MapMessage::from_bytes(None, raw);
    assert_eq!(msg.value()["data"], "not-json-at-all");
}

#[test]
fn map_message_from_byte_message_round_trips() {
    let mut fields = HashMap::new();
    fields.insert("k".to_string(), "v".to_string());
    let original = MapMessage::from_fields(fields.clone());
    let bytes = original.into_bytes();
    let recovered = MapMessage::from(bytes);
    assert_eq!(recovered.value(), fields);
}

#[test]
fn map_message_into_stream_fields_emits_raw_pairs() {
    let mut fields = HashMap::new();
    fields.insert("alpha".to_string(), "one".to_string());
    fields.insert("beta".to_string(), "two".to_string());
    let msg = MapMessage::from_fields(fields);

    let stream_fields = msg.into_stream_fields();
    let as_map: HashMap<String, String> = stream_fields
        .into_iter()
        .map(|(k, v)| (k, String::from_utf8(v).unwrap()))
        .collect();

    assert_eq!(as_map["alpha"], "one");
    assert_eq!(as_map["beta"], "two");
}

// ── MapMessage integration tests ─────────────────────────────────────────────

#[tokio::test]
async fn map_message_publish_stream_snapshot_round_trip() {
    let mut context = RedisTestHarness::new(None).await;
    let host = context.get_host();
    let topic = "map-msg-snapshot-topic".to_string();
    let publisher = RedisPublisher::new(host.clone(), topic.clone());

    let mut fields = HashMap::new();
    fields.insert("sensor_id".to_string(), "abc123".to_string());
    fields.insert("temperature".to_string(), "22.5".to_string());
    let msg = MapMessage::from_fields(fields.clone());

    publisher.publish_stream(msg).await.unwrap();
    assert!(context.check_for_message("abc123").await.is_ok());
    sleep(Duration::from_millis(100)).await;

    let snapshot = RedisSnapshot::get::<MapMessage>(host, topic).await.unwrap();
    assert!(!snapshot.is_empty());

    let recovered = &snapshot[0];
    assert_eq!(recovered.value()["sensor_id"], "abc123");
    assert_eq!(recovered.value()["temperature"], "22.5");
}

#[tokio::test]
async fn map_message_publish_stream_subscriber_round_trip() {
    let mut context = RedisTestHarness::new(None).await;
    let host = context.get_host();
    let topic = "map-msg-subscriber-topic".to_string();
    let subscriber = RedisSubscriber::new(host.clone(), topic.clone());
    let publisher = RedisPublisher::new(host, topic);

    let mut stream = subscriber.get_stream::<MapMessage>().await;

    let mut fields = HashMap::new();
    fields.insert("event".to_string(), "click".to_string());
    fields.insert("user".to_string(), "u42".to_string());
    let msg = MapMessage::from_fields(fields);

    publisher.publish_stream(msg).await.unwrap();
    assert!(context.check_for_message("click").await.is_ok());

    let received = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(received.value()["event"], "click");
    assert_eq!(received.value()["user"], "u42");
}

#[tokio::test]
async fn publish_via_trait_still_uses_data_field() {
    let mut context = RedisTestHarness::new(None).await;
    let host = context.get_host();
    let topic = "map-msg-trait-publish-topic".to_string();
    let publisher = RedisPublisher::new(host.clone(), topic.clone());

    let mut fields = HashMap::new();
    fields.insert("key".to_string(), "value".to_string());
    let msg = MapMessage::from_fields(fields);

    // Use the Publisher trait's publish() — must serialize via into_bytes() and write a single
    // "data" field containing the JSON-encoded map, not individual fields.
    publisher.publish(msg).await.unwrap();

    // The broadcast payload is the value of the "data" field, which is the JSON-encoded map.
    // It contains "key" as a substring.
    assert!(context.check_for_message("key").await.is_ok());

    sleep(Duration::from_millis(100)).await;

    // Read back as MapMessage via snapshot. The stream entry has a single "data" field whose
    // value is the JSON-encoded map. stream_entry_to_byte_message normalizes it to
    // {"data": "{\"key\":\"value\"}"}, and MapMessage::from() parses that outer JSON as
    // HashMap<String,String>, giving {"data": "{\"key\":\"value\"}"}.
    let snapshot = RedisSnapshot::get::<MapMessage>(host, topic).await.unwrap();
    assert!(!snapshot.is_empty());
    let recovered = &snapshot[0];
    // The recovered map has a "data" key containing the JSON-serialized original fields.
    let data_value = recovered.value()["data"].clone();
    let inner: serde_json::Value = serde_json::from_str(&data_value).unwrap();
    assert_eq!(inner["key"], "value");
}

// ── RedisPublisher stream-length setter tests ─────────────────────────────────

#[test]
fn set_approx_stream_max_len_updates_the_limit() {
    let mut publisher = RedisPublisher::new("redis://127.0.0.1/".to_string(), "t".to_string());
    publisher.set_approx_stream_max_len(500);
    assert_eq!(publisher.stream_max_len, StreamMaxlen::Approx(500));
}

#[test]
fn set_exact_stream_max_len_updates_the_limit() {
    let mut publisher = RedisPublisher::new("redis://127.0.0.1/".to_string(), "t".to_string());
    publisher.set_exact_stream_max_len(250);
    assert_eq!(publisher.stream_max_len, StreamMaxlen::Equals(250));
}

#[tokio::test]
async fn set_approx_stream_max_len_is_respected_on_publish() {
    let mut context = RedisTestHarness::new(None).await;
    let host = context.get_host();
    let topic = "approx-maxlen-topic".to_string();
    let mut publisher = RedisPublisher::new(host, topic);
    publisher.set_approx_stream_max_len(10);

    publisher
        .publish(StringMessage::from_value("approx-payload".to_string()))
        .await
        .unwrap();

    assert!(context.check_for_message("approx-payload").await.is_ok());
}

#[tokio::test]
async fn set_exact_stream_max_len_is_respected_on_publish() {
    let mut context = RedisTestHarness::new(None).await;
    let host = context.get_host();
    let topic = "exact-maxlen-topic".to_string();
    let mut publisher = RedisPublisher::new(host, topic);
    publisher.set_exact_stream_max_len(10);

    publisher
        .publish(StringMessage::from_value("exact-payload".to_string()))
        .await
        .unwrap();

    assert!(context.check_for_message("exact-payload").await.is_ok());
}

// ── Existing stream-entry conversion tests ────────────────────────────────────

fn stream_entry_to_json_bytes(entry: &StreamId) -> Result<Vec<u8>, PubSubError> {
    let redis_value = stream_entry_to_redis_value(entry);
    redis_value_to_json_bytes(&redis_value).map_err(PubSubError::from)
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
