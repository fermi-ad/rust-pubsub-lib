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

use std::collections::HashMap;
use std::fmt::Debug;

use redis::streams::{StreamId, StreamMaxlen, StreamRangeReply};
use redis::{AsyncCommands, FromRedisValue, Value};
use tokio_stream::{Stream, StreamExt};

use crate::redis_impls::{get_connection, redis_value_to_byte_message};
use crate::{ByteMessage, Message, PubSubError, Publisher, Snapshot, Subscriber};

mod cache;
mod runtime;

#[cfg(test)]
mod tests;

/// The default value for the Redis stream length.
pub const STREAM_LEN_DEFAULT: usize = 100_000;

/// Extension of [`Message`] that knows how to serialize itself as Redis Stream field/value pairs.
///
/// The default [`into_stream_fields()`](StreamMessage::into_stream_fields) implementation
/// collapses the payload into a single `"data"` field. Types that want to emit real multi-field
/// entries (e.g. [`MapMessage`]) override that method.
///
/// This trait is a Redis Stream concept and lives in `redis_impls::stream` rather than in the
/// core trait layer.
///
/// ## Opting in
///
/// A blanket `impl<M: Message> StreamMessage for M` is not provided because Rust's
/// specialization feature (required to allow [`MapMessage`] to override the default) is not yet
/// stable on the stable toolchain. Each type opts in with an explicit impl instead:
///
/// ```rust,ignore
/// // One-liner to get the default single-"data"-field behavior:
/// impl StreamMessage for MyMessage {}
///
/// // Override to emit real multi-field entries:
/// impl StreamMessage for MyMessage {
///     fn into_stream_fields(self) -> Vec<(String, Vec<u8>)> {
///         // return your field/value pairs here
///     }
/// }
/// ```
pub trait StreamMessage: Message {
    /// Consumes this message and returns it as a list of Redis Stream field/value pairs.
    ///
    /// The default implementation collapses the payload into a single `"data"` field:
    /// equivalent to `XADD <topic> * data <blob>`. Override this to emit real multi-field entries.
    fn into_stream_fields(self) -> Vec<(String, Vec<u8>)>
    where
        Self: Sized,
    {
        vec![("data".to_string(), self.into_bytes().extract_value())]
    }
}

/// A [`Message`] backed by a map of string field/value pairs.
///
/// When used with [`RedisPublisher::publish_stream()`], each entry in the map becomes a
/// separate Redis Stream field rather than being collapsed into a single `"data"` blob.
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
#[derive(Clone, Debug, PartialEq)]
pub struct MapMessage {
    key: Option<String>,
    fields: HashMap<String, String>,
}

impl MapMessage {
    /// Creates a [`MapMessage`] from a map of field/value pairs without a key.
    pub fn from_fields(fields: HashMap<String, String>) -> Self {
        Self { key: None, fields }
    }
}

impl Message for MapMessage {
    type Key = String;
    type Value = HashMap<String, String>;
    type KeyRef<'a> = &'a str;
    type ValueRef<'a> = &'a HashMap<String, String>;

    fn new(key: Option<String>, value: HashMap<String, String>) -> Self {
        Self { key, fields: value }
    }

    fn from_value(value: HashMap<String, String>) -> Self {
        Self {
            key: None,
            fields: value,
        }
    }

    fn from_bytes(key: Option<&[u8]>, value: &[u8]) -> Self {
        Self {
            key: key.map(|k| String::from_utf8_lossy(k).to_string()),
            fields: parse_fields(value),
        }
    }

    fn extract_key(self) -> Option<String> {
        self.key
    }

    fn extract_key_value(self) -> (Option<String>, HashMap<String, String>) {
        (self.key, self.fields)
    }

    fn extract_value(self) -> HashMap<String, String> {
        self.fields
    }

    fn into_bytes(self) -> ByteMessage {
        let json = serde_json::to_vec(&self.fields)
            .expect("MapMessage fields are always JSON-serializable");
        ByteMessage::new(self.key.map(|k| k.into_bytes()), json)
    }

    fn key(&self) -> Option<String> {
        self.key.clone()
    }

    fn key_ref(&self) -> Option<&str> {
        self.key.as_deref()
    }

    fn value(&self) -> HashMap<String, String> {
        self.fields.clone()
    }

    fn value_ref(&self) -> &HashMap<String, String> {
        &self.fields
    }
}

impl From<ByteMessage> for MapMessage {
    fn from(bytes: ByteMessage) -> Self {
        let (byte_key, byte_val) = bytes.extract_key_value();
        Self {
            key: byte_key.map(|k| String::from_utf8_lossy(&k).to_string()),
            fields: parse_fields(&byte_val),
        }
    }
}

impl StreamMessage for MapMessage {
    fn into_stream_fields(self) -> Vec<(String, Vec<u8>)> {
        self.fields
            .into_iter()
            .map(|(k, v)| (k, v.into_bytes()))
            .collect()
    }
}

// Implement StreamMessage for the other built-in Message types so they get the "data" fallback.
// (The default method body handles this; these impls just opt the types in.)
impl StreamMessage for crate::ByteMessage {}
impl StreamMessage for crate::StringMessage {}

/// Attempts to parse `bytes` as a JSON `HashMap<String, String>`.
///
/// Falls back to a single-entry map `{"data": <lossy-utf8>}` when the bytes are not valid JSON
/// or do not deserialize as a string-to-string map.
fn parse_fields(bytes: &[u8]) -> HashMap<String, String> {
    serde_json::from_slice::<HashMap<String, String>>(bytes).unwrap_or_else(|_| {
        let mut map = HashMap::new();
        map.insert(
            "data".to_string(),
            String::from_utf8_lossy(bytes).to_string(),
        );
        map
    })
}

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
        let mut conn = get_connection(&self.host).await?;
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

#[async_trait::async_trait]
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
        let mut conn = get_connection(&self.host).await?;
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

#[async_trait::async_trait]
impl Snapshot for RedisSnapshot {
    async fn get<M: Message>(host: String, topic: String) -> Result<Vec<M>, PubSubError> {
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
    fn convert_stream<M: Message>(
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

    async fn get_stream<M: Message>(
        &mut self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>> + Unpin + Send, PubSubError> {
        let stream = cache::get_redis_stream(self.host.clone(), self.topic.clone()).await;
        Ok(Self::convert_stream::<M>(stream))
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
