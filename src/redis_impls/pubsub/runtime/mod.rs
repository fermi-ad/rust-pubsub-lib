//! Background Redis PubSub streaming runtime used by shared subscribers.
//!
//! Exposes [`start_stream`], a `pub(crate)` async function that subscribes to a Redis pub/sub
//! channel, converts each incoming message into a [`ByteMessage`](crate::ByteMessage), and
//! broadcasts results to all active listeners for the associated host/topic pair via the shared
//! [`cache`](crate::cache) layer.
//!
//! Connection errors are absorbed internally. The function logs errors, applies exponential
//! backoff, and reconnects automatically. Callers receive only successfully decoded messages.

use redis::aio::PubSub;
use redis::{Client, ErrorKind, FromRedisValue, RedisError, Value};
use tokio::sync::broadcast::Sender;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::backoff::OutageState;
use crate::{ByteMessage, Message};

enum DrainOutcome {
    Reconnect,
    Stop,
}

/// Runs the reconnect loop for a shared Redis PubSub runtime until cancellation.
///
/// Errors are deduplicated via [`OutageState`]: the same error kind is only logged once until
/// either the connection recovers or a different error kind is observed.
pub(crate) async fn start_stream(
    cancel_token: CancellationToken,
    host: String,
    topic: String,
    sender: Sender<ByteMessage>,
) {
    let mut outage = OutageState::default();

    while !cancel_token.is_cancelled() {
        match connect_and_subscribe(&host, &topic).await {
            Ok(pubsub) => {
                if outage.on_recovery() {
                    info!("Redis pub/sub recovered for {host}");
                }

                // Drain messages until the connection breaks or the cancel token is invoked.
                match drain_messages(pubsub, &sender, cancel_token.clone()).await {
                    DrainOutcome::Reconnect => {
                        // Treat an unexpected connection drop as an IoError-shaped outage so that
                        // repeated drops are deduplicated and backoff is applied.
                        let drop_err = RedisError::from((
                            ErrorKind::Io,
                            "Redis pub/sub connection closed unexpectedly",
                            host.clone(),
                        ));
                        outage
                            .on_error(drop_err.kind(), &drop_err, "Redis pub/sub connection drop")
                            .await;
                    }
                    DrainOutcome::Stop => return,
                }
            }
            Err(err) => {
                outage
                    .on_error(err.kind(), &err, "Redis pub/sub connection error")
                    .await;
            }
        }
    }
}

/// Opens a new async pub/sub connection and subscribes to `topic`.
async fn connect_and_subscribe(host: &str, topic: &str) -> Result<PubSub, RedisError> {
    let client = Client::open(host)?;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(topic).await?;
    Ok(pubsub)
}

/// Forwards decoded messages from an active pub/sub connection into `sender`.
///
/// Returns [`DrainOutcome::Reconnect`] if the connection closed unexpectedly (the caller should reconnect),
/// or [`DrainOutcome::Stop`] if the cancellation token was fired (the caller should stop).
async fn drain_messages(
    mut pubsub: PubSub,
    sender: &Sender<ByteMessage>,
    cancel_token: CancellationToken,
) -> DrainOutcome {
    let mut messages = pubsub.on_message();
    loop {
        tokio::select! {
            res = messages.next() => {
                match res {
                    // The underlying connection closed; signal the caller to reconnect.
                    None => return DrainOutcome::Reconnect,
                    Some(incoming) => {
                        let payload: Value = match incoming.get_payload() {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("Redis pub/sub failed to decode message payload: {e:?}");
                                continue;
                            }
                        };
                        let bytes = match payload {
                            Value::BulkString(b) => b,
                            other => match String::from_redis_value(other) {
                                Ok(s) => s.into_bytes(),
                                Err(e) => {
                                    warn!("Redis pub/sub failed to convert message value: {e:?}");
                                    continue;
                                }
                            },
                        };
                        let _ = sender.send(ByteMessage::from_value(bytes));
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                return DrainOutcome::Stop;
            }
        }
    }
}
