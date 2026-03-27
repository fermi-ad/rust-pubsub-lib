//! Redis Stream Implementations
//!
//! Contains implementations of the public traits in this library, configured for interactions with a Redis instance.
//! NOTE: The Redis instance must be configured for stream interactions! Use feature `redis-pubsub` to interact with a pub/sub Redis instance.
//!
//! Special considerations with this module:
//! - Redis messages are keyed by an auto-incrementing number. Therefore, the [`key`](Message::key) for each [`Message`] will be ignored on
//!   calls to [`RedisPublisher::publish`], and may not be useful when processing results from [`RedisSnapshot::get`] or [`RedisSubscriber::get_stream`].

use crate::{
    ByteMessage, Message, PubSubError, Publisher, Snapshot, Subscriber, redis_impls::get_connection,
};
use redis::{
    AsyncCommands, Value as RedisValue,
    streams::{StreamReadOptions, StreamReadReply},
};
use serde_json::{Map, Value as JSONValue, json, to_vec};
use std::{collections::HashMap, fmt::Debug};
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio_stream::{Stream, StreamExt, wrappers::BroadcastStream};

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

pub struct RedisSnapshot;
#[async_trait::async_trait]
impl Snapshot for RedisSnapshot {
    async fn get<T, M: Message<T>>(host: String, topic: String) -> Result<Vec<M>, PubSubError> {
        let mut conn = get_connection(&host).await?;
        let vals: Vec<ByteMessage> = conn.xrange_all(topic).await?;
        Ok(vals.into_iter().map(M::from).collect())
    }
}

pub struct RedisSubscriber {
    _channel_lock: Receiver<Result<ByteMessage, PubSubError>>,
    host: String,
    sender: Sender<Result<ByteMessage, PubSubError>>,
    topic: String,
}
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

    fn get_stream<T, M: Message<T>>(
        &mut self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>> + Unpin + Send, PubSubError> {
        Ok(BroadcastStream::new(self.sender.subscribe()).map(|stream| {
            stream
                .map_err(|e| PubSubError::from_display(e))
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

fn poll_redis(
    host: &str,
    topic: &str,
) -> (
    Sender<Result<ByteMessage, PubSubError>>,
    Receiver<Result<ByteMessage, PubSubError>>,
) {
    let (sender, _channel_lock) = broadcast::channel(10);
    let cloned_host = host.to_owned();
    let cloned_sender = sender.clone();
    let cloned_topic = topic.to_owned();
    tokio::spawn(async move {
        let mut conn = get_connection(&cloned_host).await.unwrap();
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
                            let message = redis_map_to_json(entry.map)
                                .and_then(|deserialized| {
                                    to_vec(&json!(deserialized)).map_err(PubSubError::from_debug)
                                })
                                .map(ByteMessage::from_value);
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

fn redis_map_to_json(redis: HashMap<String, RedisValue>) -> Result<JSONValue, PubSubError> {
    let mut obj = Map::new();
    for (key, redis_val) in redis {
        if let Some(map_iter) = redis_val.as_map_iter() {
            let mut inner = HashMap::new();
            for (key, val) in map_iter {
                let key_str = redis::from_redis_value_ref(key).map_err(PubSubError::from_debug)?;
                inner.insert(key_str, val.to_owned());
            }
            obj.insert(key, redis_map_to_json(inner)?);
        } else if let Some(arr) = redis_val.as_sequence() {
            obj.insert(key, redis_seq_to_json(arr)?);
        } else {
            let val = redis::from_redis_value(redis_val).map_err(PubSubError::from_debug)?;
            obj.insert(key, JSONValue::String(val));
        }
    }
    Ok(JSONValue::Object(obj))
}

fn redis_seq_to_json(redis: &[RedisValue]) -> Result<JSONValue, PubSubError> {
    let mut arr: Vec<JSONValue> = Vec::new();
    for entry in redis {
        if let Some(map_iter) = entry.as_map_iter() {
            let mut inner = HashMap::new();
            for (key, val) in map_iter {
                let key_str = redis::from_redis_value_ref(key).map_err(PubSubError::from_debug)?;
                inner.insert(key_str, val.to_owned());
            }
            arr.push(redis_map_to_json(inner)?);
        } else if let Some(arr_val) = entry.as_sequence() {
            arr.push(redis_seq_to_json(arr_val)?);
        } else {
            match redis::from_redis_value_ref(entry) {
                Ok(val) => arr.push(JSONValue::String(val)),
                Err(e) => return Err(PubSubError::from_debug(e)),
            };
        }
    }
    Ok(JSONValue::Array(arr))
}
