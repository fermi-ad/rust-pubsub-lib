//! Abstracts the concept of a Publisher/Subscriber resource.
//!
//! This library enhances the testability of code that is part of a pub/sub architecture, and makes
//! calls to the pub/sub service easier to set up and manage.

/// Contains implementations of the traits in this library, configured for interactions with a Kafka instance.
pub mod kafka_impl;

use std::{
    error::Error,
    fmt::{self, Debug},
};
use tokio_stream::Stream;

/// A message from the pub-sub service.
/// Contains a key (optional) and a value.
///
/// Instances may be created with the [`new`](Message::new) method (specifying both key and value)
/// or the [`from_value`](Message::from_value) method (specifying only the value).
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub key: Option<String>,
    pub value: String,
}
impl Message {
    /// Creates a new [`Message`] with the provided key and value.
    pub fn new(key: Option<String>, value: String) -> Self {
        Self { key, value }
    }

    /// Creates a new [`Message`] with the provided value and no key.
    pub fn from_value(value: String) -> Self {
        Self { key: None, value }
    }
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
    async fn publish(&mut self, message: Message) -> Result<(), PubSubError>;
}

/// A trait for retrieving the instantaneous set of [`Message`]s on a topic.
#[async_trait::async_trait]
pub trait Snapshot {
    /// Retrieves a snapshot of a message topic.
    /// This function connects to the message broker,
    /// loads all [`Message`]s currently on the specified topic, and returns them
    /// to the caller.
    async fn get(host: String, topic: String) -> Result<Vec<Message>, PubSubError>;
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
    fn get_stream(
        &mut self,
    ) -> Result<impl Stream<Item = Result<Message, PubSubError>>, PubSubError>;
}

const CANNED_ERR_MESSAGE: &str = "The PubSub library encountered an error.";

/// An implementation of [`std::error::Error`] to return when pub/sub operations do not succeed.
/// This will always contain a canned error message, with the underlying error recorded if possible.
/// Consumers of this library should be careful not to expose sensitive data to users.
#[derive(Debug)]
pub struct PubSubError {
    message: String,
    cause: Option<Box<dyn Error>>,
}
impl Default for PubSubError {
    fn default() -> Self {
        Self {
            message: CANNED_ERR_MESSAGE.to_string(),
            cause: None,
        }
    }
}
impl fmt::Display for PubSubError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cause_message = if let Some(err) = &self.cause {
            format!("\n Cause: {err}")
        } else {
            String::new()
        };
        write!(f, "{}{}", self.message, cause_message)
    }
}
impl std::error::Error for PubSubError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pubsub_error_display() {
        let err = PubSubError::default();
        assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));
    }

    #[test]
    fn message_from_value() {
        let val = String::from("some text");
        let output = Message::from_value(val.clone());
        assert_eq!(output.key, None);
        assert_eq!(output.value, val);
    }

    #[test]
    fn message_from_key_value() {
        let key = Some(String::from("some key"));
        let val = String::from("some text");
        let output = Message::new(key.clone(), val.clone());
        assert_eq!(output.key, key);
        assert_eq!(output.value, val);
    }
}
