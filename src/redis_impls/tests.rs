//! Redis implementation module tests.
//!
//! Tests for shared Redis parsing, error conversion, and connection caching behavior.

use redis::{AsyncTypedCommands, Value};
use serde_json::json;

use super::*;
use crate::{Message, RedisTestHarness, StringMessage};

#[tokio::test]
async fn test_get_connection_reuses_cached_manager_for_same_host() {
    let context = RedisTestHarness::new(None).await;
    let host = context.get_host();

    let mut mgr = get_connection(&host).await.unwrap();
    let mut mgr2 = get_connection(&host).await.unwrap();

    assert_eq!(mgr.client_id().await, mgr2.client_id().await);
}

#[test]
fn redis_value_to_byte_message_handles_nested_map_and_array() {
    let message = StringMessage::from_value("{\"hello\":[{\"this-is\":\"42\"}]}".to_string());
    let redis_value = Value::Map(vec![(
        Value::SimpleString(String::from("hello")),
        Value::Array(vec![Value::Map(vec![(
            Value::SimpleString(String::from("this-is")),
            Value::Int(42),
        )])]),
    )]);
    let parsed_message = redis_value_to_byte_message(&redis_value).unwrap();
    assert_eq!(
        StringMessage::from_bytes(None, parsed_message.value_ref()),
        message
    );
}

#[test]
fn redis_value_to_json_value_handles_scalar_array_and_map_shapes() {
    assert_eq!(
        json!("hello"),
        redis_value_to_json_value(&Value::SimpleString("hello".to_string())).unwrap()
    );

    assert_eq!(
        json!(["one", "2"]),
        redis_value_to_json_value(&Value::Array(vec![
            Value::SimpleString("one".to_string()),
            Value::Int(2),
        ]))
        .unwrap()
    );

    assert_eq!(
        json!({"outer": {"inner": "7"}}),
        redis_value_to_json_value(&Value::Map(vec![(
            Value::SimpleString("outer".to_string()),
            Value::Map(vec![(
                Value::SimpleString("inner".to_string()),
                Value::Int(7)
            )]),
        )]))
        .unwrap()
    );
}

#[test]
fn redis_value_to_json_value_preserves_nested_map_inside_map() {
    let value = Value::Map(vec![(
        Value::SimpleString("outer".to_string()),
        Value::Map(vec![(
            Value::SimpleString("inner".to_string()),
            Value::SimpleString("leaf".to_string()),
        )]),
    )]);

    assert_eq!(
        json!({"outer": {"inner": "leaf"}}),
        redis_value_to_json_value(&value).unwrap()
    );
}

#[test]
fn redis_value_to_json_value_preserves_array_inside_map() {
    let value = Value::Map(vec![(
        Value::SimpleString("outer".to_string()),
        Value::Array(vec![
            Value::SimpleString("leaf".to_string()),
            Value::Int(42),
        ]),
    )]);

    assert_eq!(
        json!({"outer": ["leaf", "42"]}),
        redis_value_to_json_value(&value).unwrap()
    );
}

#[test]
fn redis_value_to_json_value_preserves_map_inside_array() {
    let value = Value::Array(vec![Value::Map(vec![(
        Value::SimpleString("outer".to_string()),
        Value::SimpleString("leaf".to_string()),
    )])]);

    assert_eq!(
        json!([{"outer": "leaf"}]),
        redis_value_to_json_value(&value).unwrap()
    );
}

#[test]
fn redis_value_to_json_value_stringifies_number_like_scalars() {
    assert_eq!(
        json!("42"),
        redis_value_to_json_value(&Value::Int(42)).unwrap()
    );
    assert_eq!(
        json!("3.14"),
        redis_value_to_json_value(&Value::Double(3.14)).unwrap()
    );
}

#[test]
fn redis_map_and_sequence_to_json_value_handle_empty_collections() {
    assert_eq!(
        json!({}),
        redis_map_to_json_value([].iter().copied()).unwrap()
    );
    assert_eq!(json!([]), redis_sequence_to_json_value(&[]).unwrap());
}

#[test]
fn redis_map_to_json_value_rejects_non_string_keys() {
    let err = redis_map_to_json_value(
        [(
            Value::Array(vec![]),
            Value::SimpleString("value".to_string()),
        )]
        .iter()
        .map(|(k, v)| (k, v)),
    )
    .unwrap_err();

    assert!(!format!("{err:?}").is_empty());
}

#[test]
fn pubsub_error_from_parsing_error_uses_debug_formatting() {
    let parsing_error = redis::ParsingError::from(String::from("bad parse"));

    let converted = PubSubError::from(parsing_error.clone());
    let expected = PubSubError::from_debug(parsing_error);

    assert_eq!(format!("{expected}"), format!("{converted}"));
}
