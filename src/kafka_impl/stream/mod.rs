//! Internal Kafka stream management used by [`KafkaSubscriber`](super::KafkaSubscriber).
//!
//! [`KafkaStream`](crate::kafka_impl::stream::KafkaStream) owns a background task that connects to
//! Kafka, subscribes to a single topic, and fans incoming messages out to all active subscribers via
//! a Tokio broadcast channel.
//!
//! The type is public because it participates in the crate's exported implementation surface, but it
//! is primarily intended for use by the Kafka subscriber implementation in [`super`](super).
//! Consumers should treat its behavior as backend infrastructure rather than as a first-choice API.

use crate::{ByteMessage, Message};
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, ConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message as RdMessage};
use std::time::Duration;
use tokio::spawn;
use tokio::sync::broadcast::{Sender, channel};
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;
use tracing::error;
use uuid::Uuid;

/// Shared Kafka message stream for one host/topic pair.
///
/// Creating a [`KafkaStream`](crate::kafka_impl::stream::KafkaStream) spawns a background task that
/// reconnects as needed and forwards messages to any listeners returned by
/// [`KafkaStream::get_stream()`](crate::kafka_impl::stream::KafkaStream::get_stream).
#[derive(Debug)]
pub struct KafkaStream {
    cancel_token: CancellationToken,
    sender: Sender<ByteMessage>,
}

impl KafkaStream {
    /// Returns the number of active broadcast receivers currently attached to this stream.
    ///
    /// This is used internally to decide when an idle cached stream can be evicted.
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }

    /// Creates a new shared Kafka stream for a host/topic pair.
    ///
    /// The returned instance starts a background task immediately. That task attempts to connect,
    /// subscribe, and keep forwarding messages until this value is dropped.
    pub fn new(host: String, topic: String) -> Self {
        let (sender, _) = channel(100);
        let remote_sender = sender.clone();

        let cancel_token = CancellationToken::new();

        spawn(start_stream(
            host,
            topic,
            remote_sender,
            cancel_token.child_token(),
        ));

        Self {
            cancel_token,
            sender,
        }
    }

    /// Returns a new broadcast-backed message stream for this Kafka topic.
    ///
    /// Each caller receives its own broadcast receiver and therefore sees messages published after
    /// the receiver is created.
    pub fn get_stream(&self) -> BroadcastStream<ByteMessage> {
        BroadcastStream::new(self.sender.subscribe())
    }
}

impl Drop for KafkaStream {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

const MAX_WAIT_TIME: Duration = Duration::from_secs(300);

async fn start_stream(
    host: String,
    topic: String,
    sender: Sender<ByteMessage>,
    cancel_token: CancellationToken,
) {
    let stream_id = Uuid::new_v4().as_hyphenated().to_string();
    let mut error_backoff_time = Duration::from_secs(1);
    while !cancel_token.is_cancelled() {
        let mut builder = ClientConfig::new();
        builder.set("bootstrap.servers", &host);
        builder.set("group.id", &stream_id);

        #[cfg(any(feature = "testing-utils", test))]
        builder.set("auto.offset.reset", "earliest");

        match builder.create::<StreamConsumer>() {
            Ok(consumer) => {
                if let Err(e) = consumer.subscribe(&[&topic]) {
                    error_backoff_time = handle_connection_err(e, error_backoff_time).await;
                } else {
                    let message_stream = consumer.stream();
                    cancel_token
                        .run_until_cancelled(monitor_stream(message_stream, &sender))
                        .await;
                    error_backoff_time = Duration::from_secs(1);
                }
            }
            Err(e) => {
                error_backoff_time = handle_connection_err(e, error_backoff_time).await;
            }
        }
    }
}

async fn monitor_stream<C: ConsumerContext>(
    mut message_stream: MessageStream<'_, C>,
    sender: &Sender<ByteMessage>,
) {
    while let Some(Ok(msg)) = message_stream.next().await {
        if let Some(message) = convert_to_message(&msg) {
            let _ = sender.send(message);
        }
    }
}

fn convert_to_message(incoming: &BorrowedMessage) -> Option<ByteMessage> {
    incoming
        .payload()
        .map(|value_bytes| ByteMessage::from_bytes(incoming.key(), value_bytes))
}

async fn handle_connection_err(err: KafkaError, mut wait_time: Duration) -> Duration {
    error!("{err:?}");
    sleep(wait_time).await;

    wait_time *= 2;
    if wait_time > MAX_WAIT_TIME {
        MAX_WAIT_TIME
    } else {
        wait_time
    }
}
