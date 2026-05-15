//! Redis pub/sub implementations of the crate's messaging traits.
//!
//! This module targets Redis' native pub/sub mechanism.
//!
//! Special considerations:
//! - Redis pub/sub does not retain history, so there is no [`Snapshot`](crate::Snapshot)
//!   implementation in this module.
//! - Redis pub/sub payloads are forwarded as direct payload bytes. They are not normalized through
//!   the shared Redis value-to-JSON conversion helpers used by the Redis Stream snapshot and stream
//!   runtime paths.
//! - The source message key supplied to
//!   [`RedisPublisher::publish()`](crate::redis_impls::pubsub::RedisPublisher::publish) is ignored.
//! - Subscribers only observe messages published after subscription begins.

use redis::{AsyncCommands, Client, FromRedisValue, Value};
use tokio_stream::{Stream, StreamExt};

use crate::redis_impls::get_connection;
use crate::{Message, PubSubError, Publisher, Subscriber};

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

#[async_trait::async_trait]
impl Publisher for RedisPublisher {
    fn new(host: String, topic: String) -> Self {
        RedisPublisher { host, topic }
    }

    async fn publish<M: Message>(&self, message: M) -> Result<(), PubSubError> {
        let mut conn = get_connection(&self.host).await?;
        let bytes = message.into_bytes();
        Ok(conn.publish(&self.topic, bytes.extract_value()).await?)
    }
}

/// Redis-backed [`Subscriber`](crate::Subscriber) implementation using native Redis pub/sub
/// channels.
///
/// Each call to [`Subscriber::new()`](crate::Subscriber::new) creates a fresh subscriber that opens
/// its own pub/sub connection when [`Subscriber::get_stream()`](crate::Subscriber::get_stream) is
/// called.
#[derive(Debug)]
pub struct RedisSubscriber {
    host: String,
    topic: String,
}

impl RedisSubscriber {
    async fn get_pubsub_stream<M: Message>(
        &self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>> + Unpin + Send, PubSubError> {
        let client = Client::open(self.host.as_str())?;
        let mut subscription = client.get_async_pubsub().await?;
        subscription.subscribe(self.topic.as_str()).await?;
        Ok(subscription.into_on_message().map(|incoming| {
            let payload: Value = incoming.get_payload()?;
            match payload {
                Value::BulkString(bytes) => Ok(M::from_bytes(None, &bytes)),
                other => {
                    let payload = String::from_redis_value(other)?;
                    Ok(M::from_bytes(None, &payload.into_bytes()))
                }
            }
        }))
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
        self.get_pubsub_stream().await
    }
}
