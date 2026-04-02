//! Redis Implementations Module Tests
//!
//! Tests for the Redis implementations of the public traits in this library.

use super::*;
use crate::{StringMessage, redis_impls::testing_utils::TestContext};

#[tokio::test]
async fn test_get_connection() {
    let context = TestContext::new().await;
    let conn1 = get_connection(&context.get_host()).await.unwrap();
    let conn2 = get_connection(&context.get_host()).await.unwrap();
    assert_eq!(format!("{:?}", conn1), format!("{:?}", conn2));
}

#[test]
fn test_from_redis_value() {
    let message = StringMessage::from_value("{\"hello\":[{\"this-is\":\"42\"}]}".to_string());
    let redis_value = Value::Map(vec![(
        Value::SimpleString(String::from("hello")),
        Value::Array(vec![Value::Map(vec![(
            Value::SimpleString(String::from("this-is")),
            Value::Int(42),
        )])]),
    )]);
    let parsed_message = ByteMessage::from_redis_value(redis_value).unwrap();
    assert_eq!(
        StringMessage::from_bytes(None, &parsed_message.value()),
        message
    );
}
