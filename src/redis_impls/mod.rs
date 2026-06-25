//! Redis-backed implementations of the crate's core messaging traits.
//!
//! This module groups the Redis support in the crate into two backend styles:
//! native pub/sub and Redis Streams. It also contains shared conversion and connection-management
//! helpers used by both styles.
//!
//! Redis values that need to move through the crate's byte-oriented message abstraction are
//! normalized into JSON-compatible [`serde_json::Value`](serde_json::Value) structures first and
//! then serialized into JSON bytes. This keeps support for arbitrary Redis value shapes explicit
//! and shared across the Redis backends that opt into this policy.

use std::collections::HashMap;
use std::sync::LazyLock;

use redis::aio::ConnectionManager;
use redis::{Client, FromRedisValue, ParsingError, RedisError, Value};
use serde_json::{Map, Value as JsonValue};
use tokio::sync::RwLock;

use crate::{ByteMessage, Message, PubSubError};

/// Redis pub/sub implementations for [`Publisher`](crate::Publisher) and
/// [`Subscriber`](crate::Subscriber).
#[cfg(any(feature = "redis-pubsub", test))]
pub mod pubsub;

/// Redis stream implementations for [`Publisher`](crate::Publisher),
/// [`Snapshot`](crate::Snapshot), and [`Subscriber`](crate::Subscriber).
#[cfg(any(feature = "redis-stream", test))]
pub mod stream;

/// Shared Redis testing utilities, including the in-process mock Redis server.
#[cfg(any(feature = "testing-utils", test))]
pub mod testing_utils;

#[cfg(test)]
mod tests;

/// Shared Redis connection cache keyed by host.
static HOST_MAP: LazyLock<RwLock<HashMap<String, ConnectionManager>>> =
    LazyLock::new(RwLock::default);

impl From<ParsingError> for PubSubError {
    fn from(value: ParsingError) -> Self {
        PubSubError::from_debug(value)
    }
}

impl From<RedisError> for PubSubError {
    fn from(value: RedisError) -> Self {
        PubSubError::from_debug(value)
    }
}

/// Normalizes Redis values into JSON-compatible shapes before serialization.
///
/// Contract:
/// - Redis maps become JSON objects.
/// - Redis sequences become JSON arrays.
/// - Scalar Redis values are stringified into JSON strings, even when they look numeric.
pub(crate) fn redis_value_to_json_value(value: &Value) -> Result<JsonValue, ParsingError> {
    match value {
        Value::Map(entries) => {
            redis_map_to_json_value(entries.iter().map(|(key, value)| (key, value)))
        }
        Value::Array(sequence) => redis_sequence_to_json_value(sequence),
        _ => {
            let scalar = String::from_redis_value_ref(value)?;
            Ok(JsonValue::String(scalar))
        }
    }
}

/// Converts a Redis map-like iterator into a JSON object using recursive normalization.
pub(crate) fn redis_map_to_json_value<'a>(
    redis: impl Iterator<Item = (&'a Value, &'a Value)>,
) -> Result<JsonValue, ParsingError> {
    let mut object = Map::new();
    for (key, redis_value) in redis {
        object.insert(
            String::from_redis_value_ref(key)?,
            redis_value_to_json_value(redis_value)?,
        );
    }
    Ok(JsonValue::Object(object))
}

/// Converts a Redis sequence into a JSON array using recursive normalization.
pub(crate) fn redis_sequence_to_json_value(redis: &[Value]) -> Result<JsonValue, ParsingError> {
    let mut array = Vec::new();
    for entry in redis {
        array.push(redis_value_to_json_value(entry)?);
    }
    Ok(JsonValue::Array(array))
}

/// Serializes a normalized JSON value into UTF-8 JSON bytes.
pub(crate) fn json_value_to_bytes(value: &JsonValue) -> Result<Vec<u8>, ParsingError> {
    serde_json::to_vec(value)
        .map_err(|err| ParsingError::from(format!("Failed to convert to bytes: {err:?}")))
}

/// Normalizes a Redis value and serializes the result into UTF-8 JSON bytes.
pub(crate) fn redis_value_to_json_bytes(value: &Value) -> Result<Vec<u8>, ParsingError> {
    let normalized = redis_value_to_json_value(value)?;
    json_value_to_bytes(&normalized)
}

/// Converts a Redis value into a payload-only [`ByteMessage`] via JSON normalization.
pub(crate) fn redis_value_to_byte_message(value: &Value) -> Result<ByteMessage, ParsingError> {
    let payload = redis_value_to_json_bytes(value)?;
    Ok(ByteMessage::from_value(payload))
}

async fn get_connection(host: &str) -> Result<ConnectionManager, PubSubError> {
    let naive_read = HOST_MAP.read().await.get(host).cloned();
    match naive_read {
        Some(conn) => Ok(conn),
        None => {
            let mut lock = HOST_MAP.write().await;
            match lock.get(host).cloned() {
                Some(conn) => Ok(conn),
                None => {
                    let client = Client::open(host)?;
                    let conn = ConnectionManager::new(client).await?;
                    lock.insert(host.to_string(), conn.clone());
                    Ok(conn)
                }
            }
        }
    }
}

async fn evict_connection(host: &str) {
    HOST_MAP.write().await.remove(host);
}
