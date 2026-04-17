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
//! - `testing-utils` exposes mock broker helpers intended for tests.
//!
//! ## Choosing a message type
//!
//! Use [`ByteMessage`] when your application already works with serialized bytes. Use
//! [`StringMessage`] when your application prefers string payloads and can tolerate lossy decoding
//! for non-UTF-8 input.
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
//! A publisher implementation will typically be constructed from a broker URI and topic name:
//!
//! ```ignore
//! use rust_pubsub_lib::{Publisher, StringMessage};
//! use rust_pubsub_lib::kafka_impl::KafkaPublisher;
//!
//! let publisher = KafkaPublisher::new("localhost:9092".to_string(), "events".to_string());
//! publisher.publish(StringMessage::from_value("hello".to_string())).await?;
//! # Ok::<(), rust_pubsub_lib::PubSubError>(())
//! ```
//!
//! A snapshot loads the messages currently available on a topic:
//!
//! ```ignore
//! use rust_pubsub_lib::{Snapshot, StringMessage};
//! use rust_pubsub_lib::redis_impls::stream::RedisSnapshot;
//!
//! let messages = RedisSnapshot::get::<String, StringMessage>(
//!     "redis://127.0.0.1:6379".to_string(),
//!     "events".to_string(),
//! ).await?;
//! # Ok::<(), rust_pubsub_lib::PubSubError>(())
//! ```
//!
//! A subscriber yields a stream of results over time:
//!
//! ```ignore
//! use rust_pubsub_lib::{StringMessage, Subscriber};
//! use rust_pubsub_lib::redis_impls::pubsub::RedisSubscriber;
//! use tokio_stream::StreamExt;
//!
//! let mut subscriber = RedisSubscriber::new(
//!     "redis://127.0.0.1:6379".to_string(),
//!     "events".to_string(),
//! );
//! let mut stream = subscriber.get_stream::<String, StringMessage>().await?;
//! let _next_message = stream.next().await;
//! # Ok::<(), rust_pubsub_lib::PubSubError>(())
//! ```

use std::{
    error::Error,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
};
use tokio_stream::Stream;

/// Kafka-backed implementations of the core pub/sub traits.
#[cfg(any(feature = "kafka", test))]
pub mod kafka_impl;

/// Redis-backed implementations of the core pub/sub traits.
#[cfg(any(feature = "redis-pubsub", feature = "redis-stream", test))]
pub mod redis_impls;

#[cfg(test)]
mod tests;

/// A [`Message`] containing already-serialized key and value bytes.
///
/// Both the key and value are cloned on access. The key is optional because some brokers or
/// calling patterns only care about a message payload.
#[derive(Clone, Debug, PartialEq)]
pub struct ByteMessage {
    key: Option<Vec<u8>>,
    value: Vec<u8>,
}
impl Message<Vec<u8>> for ByteMessage {
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

    fn as_bytes(&self) -> ByteMessage {
        self.clone()
    }

    fn into_bytes(self) -> ByteMessage {
        self
    }

    fn key(&self) -> Option<Vec<u8>> {
        self.key.clone()
    }

    fn value(&self) -> Vec<u8> {
        self.value.clone()
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
impl Message<String> for StringMessage {
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

    fn as_bytes(&self) -> ByteMessage {
        ByteMessage {
            key: self.key.clone().map(|str_val| str_val.into_bytes()),
            value: self.value.clone().into_bytes(),
        }
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

    fn value(&self) -> String {
        self.value.clone()
    }
}
impl From<ByteMessage> for StringMessage {
    fn from(bytes: ByteMessage) -> Self {
        StringMessage::from_bytes(bytes.key.as_deref(), &bytes.value)
    }
}

/// An [`Error`] returned when a pub/sub operation does not succeed.
///
/// The public-facing display text is intentionally stable and generic. Additional diagnostic context
/// is stored as a stringified cause when available. Consumers should avoid surfacing that cause in
/// user-facing contexts if it may contain sensitive broker details.
#[derive(Clone, Debug)]
pub struct PubSubError {
    message: String,
    cause: Option<String>,
}
impl PubSubError {
    /// Creates a [`PubSubError`] from a displayable error value.
    ///
    /// This keeps the public message generic and stores the original error text
    /// in the internal cause for diagnostics.
    pub fn from_display<E: Display>(err: E) -> Self {
        Self {
            message: CANNED_ERR_MESSAGE.to_string(),
            cause: Some(format!("{err}")),
        }
    }

    /// Creates a [`PubSubError`] from a debuggable error value.
    ///
    /// This is useful when detailed formatting is needed to preserve context
    /// from low-level libraries.
    pub fn from_debug<E: Debug>(err: E) -> Self {
        Self {
            message: CANNED_ERR_MESSAGE.to_string(),
            cause: Some(format!("{err:?}")),
        }
    }
}
impl Default for PubSubError {
    fn default() -> Self {
        Self {
            message: CANNED_ERR_MESSAGE.to_string(),
            cause: None,
        }
    }
}
impl Display for PubSubError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let cause_message = if let Some(err) = &self.cause {
            format!("\n Cause: {err}")
        } else {
            String::new()
        };
        write!(f, "{}{}", self.message, cause_message)
    }
}
impl Error for PubSubError {}

/// A trait describing a message from the pub/sub service.
///
/// Instances may be created with [`Message::new()`] when both key and value are available,
/// [`Message::from_value()`] when only the value is relevant, or [`Message::from_bytes()`] when a
/// backend is decoding raw transport bytes.
pub trait Message<T>: Clone + Debug + PartialEq + From<ByteMessage> + Send + Sync {
    /// Creates a new [`Message`] with the provided key and value.
    fn new(key: Option<T>, value: T) -> Self;

    /// Creates a new [`Message`] with the provided value _without_ a key.
    fn from_value(value: T) -> Self;

    /// Creates a new [`Message`] from the byte-encoded key and value pair.
    fn from_bytes(key: Option<&[u8]>, value: &[u8]) -> Self;

    /// Translates the [`Message`] contents to serialized byte values.
    fn as_bytes(&self) -> ByteMessage;

    /// Same as [`Message::as_bytes()`], but consumes this instance.
    fn into_bytes(self) -> ByteMessage;

    /// Returns a clone of this message's key, if one is present.
    fn key(&self) -> Option<T>;

    /// Returns a clone of this message's value.
    fn value(&self) -> T;
}

/// A trait for sending [`Message`]s to a configured topic.
///
/// Implementations usually encapsulate connection caching and reconnect behavior behind a backend-
/// specific concrete type.
#[async_trait::async_trait]
pub trait Publisher: Debug {
    /// Configures a [`Publisher`] for the provided host and topic.
    fn new(host: String, topic: String) -> Self
    where
        Self: Sized;

    /// Sends the provided [`Message`] to the configured topic. If a call to this
    /// method fails, the Publisher will attempt to reconnect on the next call.
    async fn publish<T, M: Message<T>>(&self, message: M) -> Result<(), PubSubError>;
}

/// A trait for retrieving the instantaneous set of [`Message`]s on a topic.
///
/// Snapshot implementations are useful for backends that can enumerate their current retained
/// messages without keeping a long-lived subscription open.
#[async_trait::async_trait]
pub trait Snapshot {
    /// Retrieves a snapshot of a message topic.
    /// This function connects to the message broker,
    /// loads all [`Message`]s currently on the specified topic, and returns them
    /// to the caller.
    async fn get<T, M: Message<T>>(host: String, topic: String) -> Result<Vec<M>, PubSubError>;
}

/// A trait for subscribing to a message topic.
///
/// Implementations return a stream of results so callers can react to new messages over time
/// without coupling themselves to a specific broker client library.
#[async_trait::async_trait]
pub trait Subscriber: Debug {
    /// Generates a new [`Subscriber`] for the provided host and topic.
    /// A new thread will be started and run in the background to poll for
    /// [`Message`]s. The thread will terminate when this subscriber is dropped.
    fn new(host: String, topic: String) -> Self
    where
        Self: Sized;

    /// Streams [`Message`]s that appear on the subscribed topic. If an interruption occurs, the Subscriber will
    /// attempt to reconnect on its own.
    async fn get_stream<T, M: Message<T>>(
        &mut self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>> + Unpin + Send, PubSubError>;
}

const CANNED_ERR_MESSAGE: &str = "The PubSub library encountered an error.";
