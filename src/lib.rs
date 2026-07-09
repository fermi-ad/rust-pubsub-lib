//! Abstractions for publishing to and subscribing from broker-backed topics.
//!
//! `rust-pubsub-lib` provides a backend-agnostic interface for three common messaging operations:
//! publishing a message, fetching an instantaneous snapshot of a topic, and subscribing to a stream
//! of messages over time.
//!
//! ## Feature flags
//!
//! Backends are enabled with Cargo features:
//!
//! - `kafka` exposes [`kafka_impl`](crate::kafka_impl).
//! - `redis-pubsub` exposes Redis pub/sub implementations under [`redis_impls`](crate::redis_impls).
//! - `redis-stream` exposes Redis Stream implementations under [`redis_impls`](crate::redis_impls).
//! - `testing-utils` exposes mock broker helpers intended for tests. Must be combined
//!   with `kafka` and/or `redis-pubsub`/`redis-stream` to have any effect.
//!
//! ## Choosing a message type
//!
//! Use [`ByteMessage`] when your application already works with serialized bytes. Use
//! [`StringMessage`] when your application prefers string payloads and can tolerate lossy decoding
//! for non-UTF-8 input.
//!
//! The [`Message`] trait has two associated types: [`Message::Key`] for the key type and
//! [`Message::Value`] for the value type. Both built-in message types use the same type for key
//! and value, but custom implementations may use different types for each.
//!
//! ## Examples
//!
//! Constructing a message:
//!
//! ```
//! use rust_pubsub_lib::{Message, StringMessage};
//!
//! let message = StringMessage::new(Some("key".to_string()), "value".to_string());
//! assert_eq!(message.key(), Some("key".to_string()));
//! assert_eq!(message.value(), "value".to_string());
//! ```
//!
//! Inspecting a message without cloning or extracting it:
//!
//! ```
//! use rust_pubsub_lib::{Message, StringMessage};
//!
//! let message = StringMessage::from_value("value".to_string());
//! assert_eq!(message.value_ref(), "value");
//! ```
//!
//! Consuming a message to extract both key and value by moving ownership out of it:
//!
//! ```
//! use rust_pubsub_lib::{Message, StringMessage};
//!
//! let message = StringMessage::new(Some("key".to_string()), "value".to_string());
//! let (key, value) = message.extract_key_value();
//! assert_eq!(key, Some("key".to_string()));
//! assert_eq!(value, "value".to_string());
//! ```
//!
//! Consuming a message to extract its stored value without an extra clone:
//!
//! ```
//! use rust_pubsub_lib::{Message, StringMessage};
//!
//! let message = StringMessage::from_value("value".to_string());
//! assert_eq!(message.extract_value(), "value".to_string());
//! ```
//!
//! A publisher implementation will typically be constructed from a broker URI and topic name:
//!
//! ```ignore
//! use rust_pubsub_lib::kafka_impl::KafkaPublisher;
//! use rust_pubsub_lib::{Message, Publisher, StringMessage};
//!
//! let publisher = KafkaPublisher::new("localhost:9092".to_string(), "events".to_string());
//! publisher.publish(StringMessage::from_value("hello".to_string())).await?;
//! # Ok::<(), rust_pubsub_lib::PubSubError>(())
//! ```
//!
//! A snapshot loads the messages currently available on a topic:
//!
//! ```ignore
//! use rust_pubsub_lib::RedisStreamSnapshot;
//! use rust_pubsub_lib::{Snapshot, StringMessage};
//!
//! let messages = RedisStreamSnapshot::get::<StringMessage>(
//!     "redis://127.0.0.1:6379".to_string(),
//!     "events".to_string(),
//! ).await?;
//! # Ok::<(), rust_pubsub_lib::PubSubError>(())
//! ```
//!
//! A subscriber yields a stream of messages over time:
//!
//! ```ignore
//! use rust_pubsub_lib::RedisPubSubSubscriber;
//! use rust_pubsub_lib::{StringMessage, Subscriber};
//! use tokio_stream::StreamExt;
//!
//! let mut subscriber = RedisPubSubSubscriber::new(
//!     "redis://127.0.0.1:6379".to_string(),
//!     "events".to_string(),
//! );
//! let mut stream = subscriber.get_stream::<StringMessage>().await;
//! let _next_message: Option<StringMessage> = stream.next().await;
//! # Ok::<(), rust_pubsub_lib::PubSubError>(())
//! ```

use std::{
    error::Error,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    pin::Pin,
};

use tokio_stream::Stream;

#[cfg(any(
    feature = "kafka",
    feature = "redis-pubsub",
    feature = "redis-stream",
    test
))]
pub(crate) mod backoff;

#[cfg(any(
    feature = "kafka",
    feature = "redis-pubsub",
    feature = "redis-stream",
    test
))]
pub(crate) mod cache;

#[cfg(any(feature = "kafka", test))]
pub mod kafka_impl;

#[cfg(any(feature = "redis-pubsub", feature = "redis-stream", test))]
pub mod redis_impls;

#[cfg(any(all(feature = "kafka", feature = "testing-utils"), test))]
pub use kafka_impl::testing_utils::Harness as KafkaTestHarness;

#[cfg(any(feature = "kafka", test))]
pub use kafka_impl::{KafkaPublisher, KafkaSnapshot, KafkaSubscriber};

#[cfg(any(feature = "redis-pubsub", test))]
pub use redis_impls::pubsub::{
    RedisPublisher as RedisPubSubPublisher, RedisSubscriber as RedisPubSubSubscriber,
};

#[cfg(any(feature = "redis-stream", test))]
pub use redis_impls::stream::{
    MapMessage, RedisPublisher as RedisStreamPublisher, RedisSnapshot as RedisStreamSnapshot,
    RedisSubscriber as RedisStreamSubscriber,
};

#[cfg(any(
    all(
        any(feature = "redis-pubsub", feature = "redis-stream"),
        feature = "testing-utils"
    ),
    test
))]
pub use redis_impls::testing_utils::TestHarness as RedisTestHarness;

#[cfg(test)]
mod tests;

const CANNED_ERR_MESSAGE: &str = "The PubSub library encountered an error.";

pub type MessageStream<M> = Pin<Box<dyn Stream<Item = M> + Send + 'static>>;

/// A trait describing a message from the pub/sub service.
///
/// Instances may be created with [`Message::new()`] when both key and value are available,
/// [`Message::from_value()`] when only the value is relevant, or [`Message::from_bytes()`] when a
/// backend is decoding raw transport bytes.
///
/// The trait defines four associated types: `Key` and `Value` for owned data, and
/// `KeyRef<'a>` and `ValueRef<'a>` for borrowed views. Each implementing type chooses
/// exactly one concrete type for each. Both built-in types ([`ByteMessage`] and
/// [`StringMessage`]) use the same type for key and value (e.g. `String`/`String`),
/// but custom implementations may freely use different types — for example, a domain
/// type that uses a `u64` key with a `String` value, or a `Vec<u8>` key with a
/// structured value type.
///
/// The ownership-oriented accessors are intentionally split into three modes:
///
/// - use [`Message::key()`] and [`Message::value()`] when you want owned clones of the stored data
/// - use [`Message::key_ref()`] and [`Message::value_ref()`] when read-only borrowed access is
///   enough; these borrowed views (`KeyRef<'_>` and `ValueRef<'_>`) may be more idiomatic than
///   a plain reference for a concrete implementation, such as `&str` or `&[u8]`
/// - use [`Message::extract_key()`], [`Message::extract_key_value()`], and
///   [`Message::extract_value()`] when consuming the message and transferring ownership out of it
pub trait Message: Clone + Debug + PartialEq + From<ByteMessage> + Send + Sync {
    /// The type of the message's Key, if one is present.
    type Key: Clone + Debug;
    /// The type of the message's Value.
    type Value: Clone + Debug;
    /// Borrowed key view tied to the lifetime of `&self`.
    type KeyRef<'a>
    where
        Self: 'a;

    /// Borrowed value view tied to the lifetime of `&self`.
    type ValueRef<'a>
    where
        Self: 'a;

    /// Creates a new [`Message`] with the provided key and value.
    fn new(key: Option<Self::Key>, value: Self::Value) -> Self;

    /// Creates a new [`Message`] with the provided value _without_ a key.
    fn from_value(value: Self::Value) -> Self;

    /// Creates a new [`Message`] from the byte-encoded key and value pair.
    fn from_bytes(key: Option<&[u8]>, value: &[u8]) -> Self;

    /// Consumes this [`Message`] and returns only the key, discarding the value.
    fn extract_key(self) -> Option<Self::Key>;

    /// Consumes this [`Message`] and returns the key and value as a tuple.
    fn extract_key_value(self) -> (Option<Self::Key>, Self::Value);

    /// Consumes this [`Message`] and returns only the value, discarding the key.
    fn extract_value(self) -> Self::Value;

    /// Converts this message into a [`ByteMessage`], consuming this instance.
    fn into_bytes(self) -> ByteMessage;

    /// Returns a clone of this message's key, if one is present.
    fn key(&self) -> Option<Self::Key>;

    /// Returns a borrowed view of this message's key, if one is present.
    fn key_ref(&self) -> Option<Self::KeyRef<'_>>;

    /// Returns a clone of this message's value.
    fn value(&self) -> Self::Value;

    /// Returns a borrowed view of this message's value.
    fn value_ref(&self) -> Self::ValueRef<'_>;
}

/// A trait for sending [`Message`]s to a configured topic.
///
/// Implementations usually encapsulate connection caching and reconnect behavior behind a backend-
/// specific concrete type.
pub trait Publisher: Debug {
    /// Configures a [`Publisher`] for the provided host and topic.
    fn new(host: String, topic: String) -> Self
    where
        Self: Sized;

    /// Sends the provided [`Message`] to the configured topic. If a call to this
    /// method fails, the Publisher will attempt to reconnect on the next call.
    fn publish<'a, M: Message>(
        &'a self,
        message: M,
    ) -> impl Future<Output = Result<(), PubSubError>> + Send + use<'a, Self, M>;
}

/// A trait for retrieving a backend-defined point-in-time view of [`Message`]s on a topic.
///
/// Snapshot implementations are useful for backends that can enumerate their retained messages
/// without keeping a long-lived subscription open. The exact inclusion boundary is backend-
/// specific: some implementations establish an upper bound first and then read up to that bound,
/// while others directly enumerate the entries retained at read time.
pub trait Snapshot {
    /// Retrieves a snapshot of a message topic.
    ///
    /// The returned [`Message`]s represent data retained and visible to the backend during the
    /// snapshot operation. Exact inclusion boundaries, ordering behavior, and interaction with
    /// concurrent publishes are backend-specific.
    fn get<M: Message>(
        host: String,
        topic: String,
    ) -> impl Future<Output = Result<Vec<M>, PubSubError>> + Send + use<Self, M>;
}

/// A trait for subscribing to a message topic.
///
/// Implementations return a stream of messages so callers can react to new messages over time
/// without coupling themselves to a specific broker client library.
///
/// Connection errors and broker-level failures are handled internally by each backend. The library
/// logs errors, applies exponential backoff, and reconnects automatically. Callers receive only
/// successfully decoded messages; no error handling is required on the stream itself.
///
/// If the consumer falls behind the internal broadcast buffer, messages may be silently dropped.
/// A warning is logged when this occurs. Callers that require guaranteed delivery should use the
/// corresponding [`Snapshot`] implementation to re-hydrate missed state.
pub trait Subscriber: Debug {
    /// Configures a [`Subscriber`] for the provided host and topic.
    ///
    /// Whether construction immediately starts background work is backend-specific. Some
    /// implementations defer connection setup until [`Subscriber::get_stream()`] is first called.
    fn new(host: String, topic: String) -> Self
    where
        Self: Sized;

    /// Returns a stream of [`Message`]s published to the subscribed topic.
    ///
    /// Each item yielded by the stream is a successfully decoded message. Backend errors are
    /// absorbed internally: the library logs them, applies backoff, and reconnects without
    /// surfacing error items to the caller.
    fn get_stream<'a, M: Message + 'static>(
        &'a self,
    ) -> impl Future<Output = MessageStream<M>> + Send + use<'a, Self, M>;
}

/// A [`Message`] containing already-serialized key and value bytes.
///
/// The key is optional because some brokers or calling patterns only care about a message payload.
#[derive(Clone, Debug, PartialEq)]
pub struct ByteMessage {
    key: Option<Vec<u8>>,
    value: Vec<u8>,
}

impl Message for ByteMessage {
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type KeyRef<'a> = &'a [u8];
    type ValueRef<'a> = &'a [u8];

    fn new(key: Option<Vec<u8>>, value: Vec<u8>) -> Self {
        Self { key, value }
    }

    fn from_value(value: Vec<u8>) -> Self {
        Self { key: None, value }
    }

    fn from_bytes(key: Option<&[u8]>, value: &[u8]) -> Self {
        Self {
            key: key.map(|arr| arr.to_vec()),
            value: value.to_vec(),
        }
    }

    fn extract_key(self) -> Option<Vec<u8>> {
        self.key
    }

    fn extract_key_value(self) -> (Option<Vec<u8>>, Vec<u8>) {
        (self.key, self.value)
    }

    fn extract_value(self) -> Vec<u8> {
        self.value
    }

    fn into_bytes(self) -> ByteMessage {
        self
    }

    fn key(&self) -> Option<Vec<u8>> {
        self.key.clone()
    }

    fn key_ref(&self) -> Option<Self::KeyRef<'_>> {
        self.key.as_deref()
    }

    fn value(&self) -> Vec<u8> {
        self.value.clone()
    }

    fn value_ref(&self) -> Self::ValueRef<'_> {
        self.value.as_slice()
    }
}

/// A [`Message`] represented as UTF-8-oriented strings.
///
/// The key remains optional to match brokers that do not require one. When converting from raw
/// bytes via [`StringMessage::from_bytes()`], invalid UTF-8 is decoded lossily with replacement
/// characters rather than returning an error.
#[derive(Clone, Debug, PartialEq)]
pub struct StringMessage {
    key: Option<String>,
    value: String,
}

impl Message for StringMessage {
    type Key = String;
    type Value = String;
    type KeyRef<'a> = &'a str;
    type ValueRef<'a> = &'a str;

    fn new(key: Option<String>, value: String) -> Self {
        Self { key, value }
    }

    fn from_value(value: String) -> Self {
        Self { key: None, value }
    }

    fn from_bytes(key: Option<&[u8]>, value: &[u8]) -> Self {
        Self {
            key: key.map(|k| String::from_utf8_lossy(k).to_string()),
            value: String::from_utf8_lossy(value).to_string(),
        }
    }

    fn extract_key(self) -> Option<String> {
        self.key
    }

    fn extract_key_value(self) -> (Option<String>, String) {
        (self.key, self.value)
    }

    fn extract_value(self) -> String {
        self.value
    }

    fn into_bytes(self) -> ByteMessage {
        ByteMessage {
            key: self.key.map(|k| k.into_bytes()),
            value: self.value.into_bytes(),
        }
    }

    fn key(&self) -> Option<String> {
        self.key.clone()
    }

    fn key_ref(&self) -> Option<Self::KeyRef<'_>> {
        self.key.as_deref()
    }

    fn value(&self) -> String {
        self.value.clone()
    }

    fn value_ref(&self) -> Self::ValueRef<'_> {
        self.value.as_str()
    }
}

impl From<ByteMessage> for StringMessage {
    fn from(bytes: ByteMessage) -> Self {
        let (byte_key, byte_val) = bytes.extract_key_value();
        Self {
            key: byte_key.map(|k| String::from_utf8_lossy(&k).to_string()),
            value: String::from_utf8_lossy(&byte_val).to_string(),
        }
    }
}

/// An [`Error`] returned when a pub/sub operation does not succeed.
///
/// User-facing formatting via [`Display`] is intentionally stable and generic.
/// Additional diagnostic context may be captured internally and exposed through
/// [`Debug`] output for logging and troubleshooting.
///
/// Use `format!("{err}")` for messages shown to users.
/// Use `format!("{err:?}")` when recording diagnostic detail in logs.
#[derive(Clone, Default)]
pub struct PubSubError {
    cause: Option<String>,
}

impl PubSubError {
    /// Creates a [`PubSubError`] from a debuggable error value.
    ///
    /// The captured value is stored as diagnostic context and is visible through
    /// [`Debug`] formatting, while [`Display`] remains generic.
    pub fn from_debug<E: Debug>(err: E) -> Self {
        Self {
            cause: Some(format!("{err:?}")),
        }
    }

    /// Returns the captured diagnostic cause message, if one was stored.
    pub fn cause_message(&self) -> Option<&str> {
        self.cause.as_deref()
    }
}

impl Debug for PubSubError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("PubSubError")
            .field("cause", &self.cause)
            .finish()
    }
}

impl Display for PubSubError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{CANNED_ERR_MESSAGE}")
    }
}

impl Error for PubSubError {}
