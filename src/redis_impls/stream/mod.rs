//! Redis Stream implementations of the crate's messaging traits.
//!
//! This module targets Redis Streams rather than native Redis pub/sub.
//!
//! ## Publish paths
//!
//! There are two ways to publish a message to a Redis Stream:
//!
//! - **[`Publisher::publish()`](crate::Publisher::publish)** — the standard trait method. It
//!   serializes the message into a single `"data"` field:
//!   `XADD <topic> * data <blob>`. Any [`Message`](crate::Message) is supported.
//!
//! - **[`RedisPublisher::publish_stream()`]** — an inherent method that accepts any
//!   [`StreamMessage`] and calls [`StreamMessage::into_stream_fields()`] to obtain the field map,
//!   then passes it directly to `XADD`. Use this when you need to produce real multi-field entries
//!   that interoperate with external producers (e.g. `XADD <topic> * sensor_id abc temperature 22.5`).
//!   [`MapMessage`] is the built-in type for this use case.
//!
//! ## Special considerations
//!
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

pub use stream_message::{MapMessage, StreamMessage};

use std::fmt::Debug;

use crate::cache::{self, Source};
use crate::redis_impls::{get_connection, redis_value_to_byte_message};
use crate::{ByteMessage, Message, MessageStream, PubSubError, Publisher, Snapshot, Subscriber};
use redis::streams::{StreamId, StreamMaxlen, StreamRangeReply};
use redis::{AsyncCommands, FromRedisValue, Value};

mod runtime;
mod stream_message;

#[cfg(test)]
mod tests;

/// The default value for the Redis stream length.
pub const STREAM_LEN_DEFAULT: usize = 100_000;

/// Redis-backed [`Publisher`](crate::Publisher) implementation that writes to Redis Streams via
/// `XADD`.
///
/// The message key is not preserved because Redis assigns its own stream entry identifier.
///
/// ## Publish paths
///
/// - [`Publisher::publish()`](crate::Publisher::publish) — writes a single `"data"` field
///   (`XADD <topic> * data <blob>`). Works with any [`Message`] type.
/// - [`RedisPublisher::publish_stream()`] — writes native multi-field entries by calling
///   [`StreamMessage::into_stream_fields()`]. Use this with [`MapMessage`] or any custom
///   [`StreamMessage`] implementation to interoperate with external producers.
#[derive(Debug)]
pub struct RedisPublisher {
    host: String,
    stream_max_len: StreamMaxlen,
    topic: String,
}

impl RedisPublisher {
    /// Configures the stream to be trimmed to approximately `new_max` entries on each `XADD`.
    ///
    /// Uses Redis `MAXLEN ~` (approximate trimming), which is more efficient than exact trimming
    /// because Redis can trim in bulk at macro-node boundaries. The default is
    /// `MAXLEN ~ 100_000`.
    ///
    /// See also [`set_exact_stream_max_len`](Self::set_exact_stream_max_len).
    pub fn set_approx_stream_max_len(&mut self, new_max: usize) {
        self.stream_max_len = StreamMaxlen::Approx(new_max);
    }

    /// Configures the stream to be trimmed to exactly `new_max` entries on each `XADD`.
    ///
    /// Uses Redis `MAXLEN =` (exact trimming). Prefer
    /// [`set_approx_stream_max_len`](Self::set_approx_stream_max_len) for high-throughput streams
    /// where approximate trimming is acceptable, as it is significantly cheaper.
    pub fn set_exact_stream_max_len(&mut self, new_max: usize) {
        self.stream_max_len = StreamMaxlen::Equals(new_max);
    }

    /// Publishes a message as native Redis Stream field/value pairs.
    ///
    /// Unlike [`Publisher::publish()`](crate::Publisher::publish), which always writes a single
    /// `"data"` field, this method calls [`StreamMessage::into_stream_fields()`] to obtain the
    /// field map and passes it directly to `XADD`. Use this when interoperating with external
    /// producers that write multi-field entries.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::collections::HashMap;
    /// use rust_pubsub_lib::{MapMessage, Publisher};
    /// use rust_pubsub_lib::RedisStreamPublisher;
    ///
    /// let mut fields = HashMap::new();
    /// fields.insert("sensor_id".to_string(), "abc123".to_string());
    /// fields.insert("temperature".to_string(), "22.5".to_string());
    ///
    /// let publisher = RedisStreamPublisher::new(
    ///     "redis://127.0.0.1/".to_string(),
    ///     "my-topic".to_string(),
    /// );
    /// let msg = MapMessage::from_fields(fields);
    /// publisher.publish_stream(msg).await?;
    /// ```
    pub async fn publish_stream<M: StreamMessage>(&self, message: M) -> Result<(), PubSubError> {
        let mut conn = get_connection(&self.host)
            .await
            .map_err(PubSubError::from)?;
        let fields = message.into_stream_fields();
        let field_refs: Vec<(&str, &[u8])> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_slice()))
            .collect();
        Ok(conn
            .xadd_maxlen(&self.topic, self.stream_max_len, "*", field_refs.as_slice())
            .await?)
    }
}

impl Publisher for RedisPublisher {
    fn new(host: String, topic: String) -> Self {
        RedisPublisher {
            host,
            stream_max_len: StreamMaxlen::Approx(STREAM_LEN_DEFAULT),
            topic,
        }
    }

    /// Publishes a message to the Redis Stream as a single `"data"` field.
    ///
    /// The message is serialized via [`Message::into_bytes()`] and written as:
    /// `XADD <topic> * data <blob>`.
    ///
    /// To write native multi-field entries instead, use
    /// [`RedisPublisher::publish_stream()`] with a [`MapMessage`] or any custom
    /// [`StreamMessage`] implementation.
    async fn publish<M: Message>(&self, message: M) -> Result<(), PubSubError> {
        let mut conn = get_connection(&self.host)
            .await
            .map_err(PubSubError::from)?;
        let bytes = message.into_bytes();
        Ok(conn
            .xadd_maxlen(
                &self.topic,
                self.stream_max_len,
                "*",
                &[("data", bytes.value_ref())],
            )
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

impl Snapshot for RedisSnapshot {
    async fn get<M: Message>(host: String, topic: String) -> Result<Vec<M>, PubSubError> {
        let mut conn = get_connection(&host).await.map_err(PubSubError::from)?;
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

/// Redis-backed [`Subscriber`](crate::Subscriber) implementation for Redis Streams.
///
/// Subscribers for the same host/topic pair share a single cached background `XREAD` poller.
/// Constructing a subscriber is side-effect free; the shared background runtime is started lazily
/// by the first call to [`Subscriber::get_stream()`](crate::Subscriber::get_stream).
///
/// Each call to [`Subscriber::get_stream()`](crate::Subscriber::get_stream) subscribes to the
/// shared broadcast channel for the matching host/topic pair and receives messages published from
/// that point onward.
///
/// Connection errors and `XREAD` failures are handled internally. The background task logs errors,
/// applies exponential backoff, and reconnects automatically. The stream yields only successfully
/// decoded messages; no error handling is required on the stream itself.
///
/// If the consumer falls behind the internal broadcast buffer, messages may be silently dropped.
/// A warning is logged when this occurs.
#[derive(Debug)]
pub struct RedisSubscriber {
    host: String,
    topic: String,
}

impl Subscriber for RedisSubscriber {
    fn new(host: String, topic: String) -> Self {
        RedisSubscriber { host, topic }
    }

    async fn get_stream<M: Message + 'static>(&self) -> MessageStream<M> {
        cache::get_stream(
            &self.host,
            &self.topic,
            Source::RedisStream,
            runtime::start_stream,
        )
        .await
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
