//! Message types for the Redis Stream backend.
//!
//! This module defines two items that extend the core [`Message`](crate::Message) abstraction
//! with Redis Stream-specific serialization:
//!
//! - [`StreamMessage`] — a trait that knows how to convert a message into Redis Stream
//!   field/value pairs (`XADD` entries). The default implementation collapses the payload into a
//!   single `"data"` field; types that want real multi-field entries override
//!   [`into_stream_fields`](StreamMessage::into_stream_fields).
//!
//! - [`MapMessage`] — a concrete [`Message`](crate::Message) backed by a
//!   `HashMap<String, String>`. When published via
//!   [`RedisPublisher::publish_stream`](super::RedisPublisher::publish_stream), each map entry
//!   becomes a separate Redis Stream field rather than being collapsed into a single blob.

use std::collections::HashMap;

use crate::{ByteMessage, Message, StringMessage};

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
impl StreamMessage for ByteMessage {}
impl StreamMessage for StringMessage {}

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
