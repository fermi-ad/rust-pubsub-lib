//! Redis Implementations Module
//!
//! Houses the implementations of the public traits in this library for different "flavors" of Redis.

use std::{collections::HashMap, sync::LazyLock};

use crate::{ByteMessage, Message, PubSubError};
use redis::{Client, FromRedisValue, ParsingError, RedisError, Value, aio::ConnectionManager};
use tokio::sync::RwLock;

#[cfg(any(feature = "redis-pubsub", test))]
pub mod pubsub;

#[cfg(any(feature = "redis-stream", test))]
pub mod stream;

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

impl FromRedisValue for ByteMessage {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        Ok(ByteMessage::from_value(redis::from_redis_value(v)?))
    }
}

impl From<RedisError> for PubSubError {
    fn from(value: RedisError) -> Self {
        PubSubError::from_debug(value)
    }
}
