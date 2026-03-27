//! Abstracts the concept of a Publisher/Subscriber resource.
//!
//! This library enhances the testability of code that is part of a pub/sub architecture, and makes
//! calls to the pub/sub service easier to set up and manage.

use std::{
    error::Error,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
};
use tokio_stream::Stream;

#[cfg(any(feature = "kafka", test))]
pub mod kafka_impl;

#[cfg(any(feature = "redis-pubsub", feature = "redis-stream", test))]
pub mod redis_impls;

#[cfg(test)]
mod tests;

/// A [`Message`] from the pub-sub service containing the serialized [`u8`] key/value.
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

/// A [`Message`] from the pub-sub service deserialized to a [`String`] key/value.
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

/// A trait describing a message from the pub/sub service.
///
/// Instances may be created with the [`new`](Message::new) method (specifying both key and value)
/// or the [`from_value`](Message::from_value) method (specifying only the value). The [`from_bytes`](Message::from_bytes)
/// method may also be used to create a [`Message`] from raw byte data.
pub trait Message<T>: Clone + Debug + PartialEq + From<ByteMessage> + Send + Sync {
    /// Creates a new [`Message`] with the provided key and value.
    fn new(key: Option<T>, value: T) -> Self;

    /// Creates a new [`Message`] with the provided value _without_ a key.
    fn from_value(value: T) -> Self;

    /// Creates a new [`Message`] from the byte-encoded key and value pair.
    fn from_bytes(key: Option<&[u8]>, value: &[u8]) -> Self;

    /// Translates the [`Message`] contents to serialized byte values.
    fn as_bytes(&self) -> ByteMessage;

    /// Same as [`as_bytes`](Message::as_bytes), but consumes this instance.
    fn into_bytes(self) -> ByteMessage;

    /// Access to this message's key.
    fn key(&self) -> Option<T>;

    /// Access to this message's value.
    fn value(&self) -> T;
}

/// A trait for sending [`Message`]s to a configured topic.
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
#[async_trait::async_trait]
pub trait Snapshot {
    /// Retrieves a snapshot of a message topic.
    /// This function connects to the message broker,
    /// loads all [`Message`]s currently on the specified topic, and returns them
    /// to the caller.
    async fn get<T, M: Message<T>>(host: String, topic: String) -> Result<Vec<M>, PubSubError>;
}

/// A trait for subscribing to a message topic. Returns the values as a stream of [`Message`]s for clients to handle.
pub trait Subscriber: Debug {
    /// Generates a new [`Subscriber`] for the provided host and topic.
    /// A new thread will be started and run in the background to poll for
    /// [`Message`]s. The thread will terminate when this subscriber is dropped.
    fn new(host: String, topic: String) -> Self
    where
        Self: Sized;

    /// Streams [`Message`]s that appear on the subscribed topic. If an interruption occurs, the Subscriber will
    /// attempt to reconnect on its own.
    fn get_stream<T, M: Message<T>>(
        &mut self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>> + Unpin + Send, PubSubError>;
}

const CANNED_ERR_MESSAGE: &str = "The PubSub library encountered an error.";

/// An implementation of [`Error`] to return when pub/sub operations do not succeed.
/// This will always contain a canned error message, with the underlying error recorded if possible.
/// Consumers of this library should be careful not to expose sensitive data to users.
#[derive(Clone, Debug)]
pub struct PubSubError {
    message: String,
    cause: Option<String>,
}
impl PubSubError {
    pub fn from_display<E: Display>(err: E) -> Self {
        Self {
            message: CANNED_ERR_MESSAGE.to_string(),
            cause: Some(format!("{err}")),
        }
    }

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
