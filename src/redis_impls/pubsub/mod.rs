//! Redis pub/sub implementations of the crate's messaging traits.
//!
//! This module targets Redis' native pub/sub mechanism.
//!
//! - Redis pub/sub does not retain history, so there is no [`Snapshot`](crate::Snapshot)
//!   implementation in this module.
//! - Redis pub/sub payloads are forwarded as direct payload bytes. They are not normalized through
//!   the shared Redis value-to-JSON conversion helpers used by the Redis Stream snapshot and stream
//!   runtime paths.
//! - The source message key supplied to
//!   [`RedisPublisher::publish()`](crate::redis_impls::pubsub::RedisPublisher::publish) is ignored.
//! - Subscribers only observe messages published after subscription begins.
//! - Payload decode failures are logged as warnings and the affected message is skipped; the stream
//!   continues running.

use redis::AsyncCommands;

use crate::cache::{self, Source};
use crate::redis_impls::get_connection;
use crate::{Message, MessageStream, PubSubError, Publisher, Subscriber};

mod runtime;

#[cfg(test)]
mod tests;

/// Redis-backed [`Publisher`](crate::Publisher) implementation using native Redis pub/sub channels.
///
/// Redis pub/sub messages are delivered as payloads only, so any message key supplied to
/// [`Publisher::publish()`](crate::Publisher::publish) is ignored by this backend.
#[derive(Debug)]
pub struct RedisPublisher {
    host: String,
    topic: String,
}

impl Publisher for RedisPublisher {
    fn new(host: String, topic: String) -> Self {
        RedisPublisher { host, topic }
    }

    async fn publish<M: Message>(&self, message: M) -> Result<(), PubSubError> {
        let mut conn = get_connection(&self.host)
            .await
            .map_err(PubSubError::from)?;
        let bytes = message.into_bytes();
        Ok(conn.publish(&self.topic, bytes.extract_value()).await?)
    }
}

/// Redis-backed [`Subscriber`](crate::Subscriber) implementation using native Redis pub/sub
/// channels.
///
/// Each call to [`Subscriber::new()`](crate::Subscriber::new) creates a fresh subscriber.
/// Calling [`Subscriber::get_stream()`](crate::Subscriber::get_stream) subscribes to the shared
/// cached runtime for this host/topic pair, reusing an existing connection if one is already
/// active. Multiple subscribers to the same host and topic share one Redis connection
/// process-wide. The stream yields only successfully decoded messages; payload decode failures
/// are logged as warnings and skipped.
///
/// The shared background task reconnects automatically when the connection drops, applying
/// exponential backoff between attempts. Connection errors and reconnection events are logged;
/// the stream resumes delivering messages once the connection is restored.
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
            Source::RedisPubSub,
            runtime::start_stream,
        )
        .await
    }
}
