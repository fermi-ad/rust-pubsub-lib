//! Redis Stream implementations of the crate's messaging traits.
//!
//! This module targets Redis Streams rather than native Redis pub/sub.
//!
//! Special considerations:
//! - Redis assigns stream entry identifiers itself, so a source message key is not preserved by
//!   [`RedisPublisher`](crate::redis_impls::stream::RedisPublisher).
//! - [`RedisSubscriber`](crate::redis_impls::stream::RedisSubscriber) polls Redis in a background
//!   task and fans results out through a broadcast channel.
//! - [`RedisSnapshot`](crate::redis_impls::stream::RedisSnapshot) reads the currently retained
//!   entries from the stream at the time the request is made.

use crate::redis_impls::get_connection;
use crate::{ByteMessage, Message, PubSubError, Publisher, Snapshot, Subscriber};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, FromRedisValue, Value};
use std::fmt::Debug;
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

#[cfg(test)]
mod tests;

/// Redis-backed [`Publisher`](crate::Publisher) implementation that writes to Redis Streams via `XADD`.
///
/// The message key is not preserved because Redis assigns its own stream entry identifier.
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
        Ok(conn
            .xadd(&self.topic, "*", &[("data", &bytes.value)])
            .await?)
    }
}

/// Redis-backed [`Snapshot`](crate::Snapshot) implementation that reads the current stream contents.
///
/// The snapshot converts each retained stream entry into a [`ByteMessage`](crate::ByteMessage) and
/// then into the requested message type.
pub struct RedisSnapshot;
#[async_trait::async_trait]
impl Snapshot for RedisSnapshot {
    async fn get<T, M: Message<T>>(host: String, topic: String) -> Result<Vec<M>, PubSubError> {
        let mut conn = get_connection(&host).await?;
        let vals: Vec<ByteMessage> = conn.xrange_all(topic).await?;
        Ok(vals.into_iter().map(M::from).collect())
    }
}

/// Redis-backed [`Subscriber`](crate::Subscriber) implementation that polls a Redis Stream and broadcasts updates.
///
/// A background task started by [`RedisSubscriber::new()`](crate::redis_impls::stream::RedisSubscriber::new)
/// blocks on `XREAD`, forwards decoded entries through a broadcast channel, and stops once no
/// receivers remain.
pub struct RedisSubscriber {
    _channel_lock: Receiver<Result<ByteMessage, PubSubError>>,
    host: String,
    sender: Sender<Result<ByteMessage, PubSubError>>,
    topic: String,
}
#[async_trait::async_trait]
impl Subscriber for RedisSubscriber {
    fn new(host: String, topic: String) -> Self {
        let (sender, _channel_lock) = poll_redis(&host, &topic);
        RedisSubscriber {
            _channel_lock,
            host,
            sender,
            topic,
        }
    }

    async fn get_stream<T, M: Message<T>>(
        &mut self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>> + Unpin + Send, PubSubError> {
        Ok(BroadcastStream::new(self.sender.subscribe()).map(|stream| {
            stream
                .map_err(PubSubError::from_display)
                .and_then(|response| response.map(M::from))
        }))
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

type MsgSender = Sender<Result<ByteMessage, PubSubError>>;
type MsgReceiver = Receiver<Result<ByteMessage, PubSubError>>;

fn poll_redis(host: &str, topic: &str) -> (MsgSender, MsgReceiver) {
    let (sender, _channel_lock) = broadcast::channel(10);
    let cloned_host = host.to_owned();
    let cloned_sender = sender.clone();
    let cloned_topic = topic.to_owned();
    tokio::spawn(async move {
        let mut conn = match get_connection(&cloned_host).await {
            Ok(conn) => conn,
            Err(err) => {
                let _ = cloned_sender.send(Err(err));
                return;
            }
        };
        let mut latest_id = String::from("$");
        let opts = StreamReadOptions::default().block(0);
        while cloned_sender.receiver_count() > 0 {
            match conn
                .xread_options::<&str, &str, StreamReadReply>(
                    &[&cloned_topic],
                    &[&latest_id],
                    &opts,
                )
                .await
            {
                Ok(reply) => {
                    for stream in reply.keys {
                        for entry in stream.ids {
                            latest_id = entry.id;
                            let data: Vec<(Value, Value)> = entry
                                .map
                                .into_iter()
                                .map(|(key, val)| (Value::SimpleString(key), val))
                                .collect();
                            let map = Value::Map(data);
                            let message =
                                ByteMessage::from_redis_value(map).map_err(PubSubError::from_debug);
                            let _ = cloned_sender.send(message);
                        }
                    }
                }
                Err(e) => {
                    let _ = cloned_sender.send(Err(PubSubError::from(e)));
                }
            }
        }
    });
    (sender, _channel_lock)
}
