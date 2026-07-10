//! Internal Kafka streaming runtime used by [`KafkaSubscriber`](super::KafkaSubscriber).
//!
//! Exposes [`start_stream`], a `pub(crate)` async function that connects to Kafka, subscribes to a
//! single topic, and fans incoming messages out to all active subscribers via a Tokio broadcast
//! channel owned by the shared [`cache`](crate::cache) layer.
//!
//! Connection errors and per-message failures are absorbed internally. The function logs errors,
//! applies exponential backoff, and reconnects automatically. It is intended for use by the
//! shared cache and should be treated as backend infrastructure rather than a first-choice API.

use std::mem;

use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, ConsumerContext, MessageStream, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message as RdMessage};
use tokio::sync::broadcast::Sender;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::backoff::OutageState;
use crate::{ByteMessage, Message};

/// Runs the reconnect loop for a shared Kafka consumer until the cancellation token fires.
///
/// Creates a fresh [`StreamConsumer`] on each iteration. Broker and subscription errors are
/// deduplicated via [`OutageState`]: the same error variant is only logged once until the
/// connection recovers or a different variant appears. Exponential backoff (capped at
/// [`crate::backoff::MAX_BACKOFF`]) is applied between failed attempts.
pub(crate) async fn start_stream(
    cancel_token: CancellationToken,
    host: String,
    topic: String,
    sender: Sender<ByteMessage>,
) {
    let stream_id = Uuid::new_v4().as_hyphenated().to_string();
    let mut outage: OutageState<_> = OutageState::default();

    while !cancel_token.is_cancelled() {
        let mut builder = ClientConfig::new();
        builder.set("bootstrap.servers", &host);
        builder.set("group.id", &stream_id);

        #[cfg(any(feature = "testing-utils", test))]
        builder.set("auto.offset.reset", "earliest");

        match builder.create::<StreamConsumer>() {
            Ok(consumer) => match consumer.subscribe(&[&topic]) {
                Err(e) => {
                    outage
                        .on_error(mem::discriminant(&e), &e, "Kafka subscription error")
                        .await;
                }
                Ok(()) => {
                    if outage.on_recovery() {
                        info!("Kafka stream reconnected to {host}");
                    }
                    let message_stream = consumer.stream();
                    cancel_token
                        .run_until_cancelled(monitor_stream(message_stream, &sender))
                        .await;
                }
            },
            Err(e) => {
                outage
                    .on_error(mem::discriminant(&e), &e, "Kafka consumer creation error")
                    .await;
            }
        }
    }
}

async fn monitor_stream<C: ConsumerContext>(
    mut message_stream: MessageStream<'_, C>,
    sender: &Sender<ByteMessage>,
) {
    let mut stream_outage: OutageState<_> = OutageState::default();
    while let Some(res) = message_stream.next().await {
        match res {
            Ok(msg) => {
                if stream_outage.on_recovery() {
                    info!("Kafka stream recovered");
                }
                if let Some(message) = convert_to_message(&msg) {
                    let _ = sender.send(message);
                } else {
                    warn!("Failed to convert message: {msg:?}")
                }
            }
            Err(e) => {
                // This is a message-processing loop, not a reconnect loop. Use record_error
                // (no sleep) so that transient per-message errors do not stall the stream.
                // Backoff is applied by start_stream when it recreates the consumer.
                stream_outage.record_error(mem::discriminant(&e), &e, "Kafka stream error");
            }
        }
    }
}

fn convert_to_message(incoming: &BorrowedMessage) -> Option<ByteMessage> {
    incoming
        .payload()
        .map(|value_bytes| ByteMessage::from_bytes(incoming.key(), value_bytes))
}
