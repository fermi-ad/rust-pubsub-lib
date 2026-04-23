//! Redis Stream implementations of the crate's messaging traits.
//!
//! This module targets Redis Streams rather than native Redis pub/sub.
//!
//! Special considerations:
//! - Redis assigns stream entry identifiers itself, so a source message key is not preserved by
//!   [`RedisPublisher`](crate::redis_impls::stream::RedisPublisher).
//! - [`RedisSubscriber`](crate::redis_impls::stream::RedisSubscriber) is a lightweight handle; Redis
//!   stream subscribers for the same host/topic pair share a cached background `XREAD` poller.
//! - Constructing [`RedisSubscriber`](crate::redis_impls::stream::RedisSubscriber) does not start
//!   background work. Polling begins when [`Subscriber::get_stream()`](crate::Subscriber::get_stream)
//!   first requests the shared runtime from the cache.
//! - Each stream returned by [`Subscriber::get_stream()`](crate::Subscriber::get_stream) receives
//!   future messages from the shared broadcast fan-out channel for that host/topic pair.
//! - [`RedisSnapshot`](crate::redis_impls::stream::RedisSnapshot) reads the currently retained
//!   entries from the stream at the time the request is made.
//! - Retained stream field/value structures are materialized into [`ByteMessage`](crate::ByteMessage)
//!   payloads by recursively normalizing Redis values into JSON-compatible data and serializing that
//!   normalized structure into JSON bytes.

use std::fmt::Debug;

use redis::streams::{StreamId, StreamRangeReply};
use redis::{AsyncCommands, FromRedisValue, Value};
use tokio_stream::{Stream, StreamExt};

use crate::redis_impls::{get_connection, redis_value_to_byte_message};
use crate::{ByteMessage, Message, PubSubError, Publisher, Snapshot, Subscriber};

mod cache;
mod runtime;

#[cfg(test)]
mod tests;

/// Redis-backed [`Publisher`](crate::Publisher) implementation that writes to Redis Streams via
/// `XADD`.
///
/// The message key is not preserved because Redis assigns its own stream entry identifier.
#[derive(Debug)]
pub struct RedisPublisher {
    host: String,
    topic: String,
}

#[async_trait::async_trait]
impl Publisher for RedisPublisher {
    fn new(host: String, topic: String) -> Self {
        RedisPublisher { host, topic }
    }

    async fn publish<T, M: Message<T>>(&self, message: M) -> Result<(), PubSubError> {
        let mut conn = get_connection(&self.host).await?;
        let bytes = message.into_bytes();
        Ok(conn
            .xadd(&self.topic, "*", &[("data", bytes.value_ref())])
            .await?)
    }
}

/// Redis-backed [`Snapshot`](crate::Snapshot) implementation that reads the entries currently
/// retained in a Redis Stream.
///
/// This snapshot is based on the stream contents returned by `XRANGE` at read time. Entries
/// trimmed before the read are not included. Entries added concurrently may or may not appear,
/// depending on Redis command timing.
///
/// Each retained stream entry is converted into a [`ByteMessage`](crate::ByteMessage) by
/// recursively normalizing the Redis field/value structure into JSON-compatible data and then
/// serializing that normalized structure into JSON bytes before conversion into the requested
/// message type.
pub struct RedisSnapshot;

#[async_trait::async_trait]
impl Snapshot for RedisSnapshot {
    async fn get<T, M: Message<T>>(host: String, topic: String) -> Result<Vec<M>, PubSubError> {
        let mut conn = get_connection(&host).await?;
        let raw_reply: Value = conn.xrange_all(topic).await?;
        let reply = StreamRangeReply::from_redis_value(raw_reply)?;
        let vals = reply
            .ids
            .iter()
            .map(stream_entry_to_byte_message)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(vals.into_iter().map(M::from).collect())
    }
}

/// Redis-backed [`Subscriber`](crate::Subscriber) implementation.
///
/// Subscribers reuse a shared cached background poller per host/topic pair.
/// Constructing a subscriber is side-effect free; the shared background runtime is started lazily
/// by the first call to [`Subscriber::get_stream()`](crate::Subscriber::get_stream).
///
/// Each call to [`Subscriber::get_stream()`](crate::Subscriber::get_stream) subscribes to the
/// shared broadcast channel for the matching host/topic pair and receives future messages from
/// that point onward.
pub struct RedisSubscriber {
    host: String,
    topic: String,
}

impl RedisSubscriber {
    fn convert_stream<T, M: Message<T>>(
        stream: tokio_stream::wrappers::BroadcastStream<Result<ByteMessage, PubSubError>>,
    ) -> impl Stream<Item = Result<M, PubSubError>> + Unpin + Send {
        stream.map(|incoming| {
            incoming
                .map_err(PubSubError::from_debug)
                .and_then(|response| response.map(M::from))
        })
    }
}

#[async_trait::async_trait]
impl Subscriber for RedisSubscriber {
    fn new(host: String, topic: String) -> Self {
        RedisSubscriber { host, topic }
    }

    async fn get_stream<T, M: Message<T>>(
        &mut self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>> + Unpin + Send, PubSubError> {
        let stream = cache::get_redis_stream(self.host.clone(), self.topic.clone()).await;
        Ok(Self::convert_stream(stream))
    }
}

impl Debug for RedisSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisSubscriber")
            .field("host", &self.host)
            .field("topic", &self.topic)
            .finish()
    }
}

fn stream_entry_to_redis_value(entry: &StreamId) -> Value {
    let data: Vec<(Value, Value)> = entry
        .map
        .iter()
        .map(|(key, value)| (Value::SimpleString(key.clone()), value.clone()))
        .collect();
    Value::Map(data)
}

fn stream_entry_to_byte_message(entry: &StreamId) -> Result<ByteMessage, PubSubError> {
    let redis_value = stream_entry_to_redis_value(entry);
    redis_value_to_byte_message(&redis_value).map_err(PubSubError::from)
}

#[cfg(test)]
fn stream_entry_to_json_bytes(entry: &StreamId) -> Result<Vec<u8>, PubSubError> {
    let redis_value = stream_entry_to_redis_value(entry);
    crate::redis_impls::redis_value_to_json_bytes(&redis_value).map_err(PubSubError::from)
}
