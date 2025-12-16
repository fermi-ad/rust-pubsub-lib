pub mod kafka_impl;

use std::fmt::{self, Debug};
use tokio_stream::wrappers::BroadcastStream;

/// A message from the pub-sub service.
/// Contains a key (optional) and a value.
///
/// Instances may be created with the `new` method (specifying both key and value)
/// or the `from_value` method (specifying only the value).
#[derive(Debug, Clone)]
pub struct Message {
    pub key: Option<String>,
    pub value: String,
}
impl Message {
    /// Creates a new message with the provided key and value.
    ///
    /// ### Arguments
    /// * `key` - An optional key for the message.
    /// * `value` - The value of the message.
    pub fn new(key: Option<String>, value: String) -> Self {
        Self { key, value }
    }

    /// Creates a new message with the provided value and no key.
    ///
    /// ### Arguments
    /// * `value` - The value of the message.
    pub fn from_value(value: String) -> Self {
        Self { key: None, value }
    }
}

/// A trait for sending messages to a configured topic.
pub trait Publisher: Debug {
    /// Configures a publisher for the provided host and topic.
    fn new(host: String, topic: String) -> Result<Self, PubSubError>
    where
        Self: Sized;

    /// Sends the provided message to the configured topic.
    fn publish(&mut self, message: Message) -> Result<(), PubSubError>;
}

pub trait Snapshot {
    /// Retrieves a snapshot of a message topic.
    /// This function connects to the message broker,
    /// loads all messages currently on the specified topic, and returns them
    /// to the caller.
    fn get(host: String, topic: String) -> Result<Vec<Message>, PubSubError>;
}

/// A trait for subscribing to a message topic. Returns the values as a stream of messages for clients to handle.
pub trait Subscriber: Debug {
    /// Generates a new subscriber for the provided host and topic.
    /// A new thread will be started and run in the background to poll for
    /// messages. The thread will terminate when this subscriber is dropped.
    fn new(host: String, topic: String) -> Result<Self, PubSubError>
    where
        Self: Sized;

    /// Streams messages that appear on the subscribed topic.
    fn get_stream(&self) -> BroadcastStream<Message>;
}

const CANNED_ERR_MESSAGE: &str = "An error occurred while attempting to connect to the message broker. See server logs for details.";

#[derive(Debug)]
pub struct PubSubError {
    message: &'static str,
}
impl Default for PubSubError {
    fn default() -> Self {
        Self {
            message: CANNED_ERR_MESSAGE,
        }
    }
}
impl fmt::Display for PubSubError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
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
