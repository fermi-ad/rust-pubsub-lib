//! Redis PubSub Implementations Module
//!
//! Contains implementations of the public traits in this library, configured for interactions with a Redis instance.
//! NOTE: The Redis instance must be configured for pub/sub interactions! Use feature `redis-stream` to interact with a streaming Redis instance.
//!
//! Special considerations with this module:
//! - Redis does not support persistence in its pub/sub mode. Therefore, this module does not provide a [`Snapshot`](crate::Snapshot) implementation.
//! - Redis messages are keyed by an auto-incrementing number. Therefore, the [`key`](Message::key) for each [`Message`] will be ignored on
//!   calls to [`RedisPublisher::publish`], and may not be useful when processing results from [`RedisSubscriber::get_stream`].

use crate::{
    ByteMessage, Message, PubSubError, Publisher, Subscriber, redis_impls::get_connection,
};
use redis::{AsyncCommands, Client};
use tokio_stream::{Stream, StreamExt};

#[cfg(test)]
mod tests;

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

    async fn publish<T, M: Message<T>>(&self, message: M) -> Result<(), PubSubError> {
        let mut conn = get_connection(&self.host).await?;
        let bytes = message.into_bytes();
        Ok(conn.publish(&self.topic, bytes.value()).await?)
    }
}

#[derive(Debug)]
pub struct RedisSubscriber {
    host: String,
    topic: String,
}
impl RedisSubscriber {
    async fn get_pubsub_stream<T, M: Message<T>>(
        &self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>> + Unpin + Send, PubSubError> {
        let client = Client::open(self.host.as_str())?;
        let mut subscription = client.get_async_pubsub().await?;
        subscription.subscribe(self.topic.as_str()).await?;
        Ok(subscription.into_on_message().map(|incoming| {
            let bytes: ByteMessage = incoming.get_payload()?;
            Ok(M::from(bytes))
        }))
    }
}
impl Subscriber for RedisSubscriber {
    fn new(host: String, topic: String) -> Self {
        RedisSubscriber { host, topic }
    }

    fn get_stream<T, M: Message<T>>(
        &mut self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>> + Unpin + Send, PubSubError> {
        tokio::runtime::Handle::current().block_on(self.get_pubsub_stream())
    }
}
