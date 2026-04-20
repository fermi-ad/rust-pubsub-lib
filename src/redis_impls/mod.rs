//! Redis-backed implementations of the crate's core messaging traits.
//!
//! This module groups the Redis support in the crate into two backend styles:
//! native pub/sub and Redis Streams. It also contains shared conversion and connection-management
//! helpers used by both styles.

use crate::{ByteMessage, Message, PubSubError};
use redis::aio::ConnectionManager;
use redis::{Client, FromRedisValue, ParsingError, RedisError, Value};
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;
use std::sync::LazyLock;
use tokio::sync::RwLock;

/// Redis pub/sub implementations for [`Publisher`](crate::Publisher) and [`Subscriber`](crate::Subscriber).
#[cfg(any(feature = "redis-pubsub", test))]
pub mod pubsub;

/// Redis stream implementations for [`Publisher`](crate::Publisher), [`Snapshot`](crate::Snapshot), and [`Subscriber`](crate::Subscriber).
#[cfg(any(feature = "redis-stream", test))]
pub mod stream;

/// Shared Redis testing utilities, including the in-process mock Redis server.
#[cfg(any(feature = "testing-utils", test))]
pub mod testing_utils;

#[cfg(test)]
mod tests;

impl FromRedisValue for ByteMessage {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let parsed = parse_redis_value(&v)?;
        let vectorized = serde_json::to_vec(&parsed)
            .map_err(|e| ParsingError::from(format!("Failed to convert to bytes: {e:?}")))?;
        Ok(ByteMessage::from_value(vectorized))
    }
}

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

static HOST_MAP: LazyLock<RwLock<HashMap<String, ConnectionManager>>> =
    LazyLock::new(RwLock::default);

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

fn parse_redis_value(v: &Value) -> Result<JsonValue, ParsingError> {
    if let Some(map_iter) = v.as_map_iter() {
        parse_redis_map(map_iter)
    } else if let Some(arr) = v.as_sequence() {
        parse_redis_seq(arr)
    } else {
        let val = String::from_redis_value_ref(v)?;
        Ok(JsonValue::String(val))
    }
}

fn parse_redis_map<'a>(
    redis: impl Iterator<Item = (&'a Value, &'a Value)>,
) -> Result<JsonValue, ParsingError> {
    let mut obj = Map::new();
    for (key, redis_val) in redis {
        obj.insert(
            String::from_redis_value_ref(key)?,
            parse_redis_value(redis_val)?,
        );
    }
    Ok(JsonValue::Object(obj))
}

fn parse_redis_seq(redis: &[Value]) -> Result<JsonValue, ParsingError> {
    let mut arr: Vec<JsonValue> = Vec::new();
    for entry in redis {
        arr.push(parse_redis_value(entry)?);
    }
    Ok(JsonValue::Array(arr))
}
