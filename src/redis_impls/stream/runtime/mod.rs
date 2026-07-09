//! Background Redis Stream polling runtime used by shared subscribers.
//!
//! Exposes [`start_stream`], a `pub(crate)` async function that repeatedly issues `XREAD` calls,
//! converts each returned stream entry into a [`ByteMessage`](crate::ByteMessage), and broadcasts
//! results to all active listeners for the associated host/topic pair via the shared
//! [`cache`](crate::cache) layer.
//!
//! Connection errors and `XREAD` failures are absorbed internally. The function logs errors, applies
//! exponential backoff, and reconnects automatically. Callers receive only successfully decoded
//! messages.

use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisError};
use tokio::sync::broadcast::Sender;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::stream_entry_to_byte_message;
use crate::ByteMessage;
use crate::backoff::OutageState;
use crate::redis_impls::{evict_connection, get_connection};

/// Redis `XREAD BLOCK` duration used for each poll request.
const READ_BLOCK_TIME_MS: usize = 250;

/// Runs the reconnect loop for a shared Redis Stream runtime until cancellation.
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
        match get_connection(&host).await {
            Ok(mut conn) => {
                if outage.on_recovery() {
                    info!("Redis stream recovered for {host}");
                }

                // monitor_stream returns Some(xread_err) if XREAD failed, or None if cancelled.
                let xread_err = cancel_token
                    .run_until_cancelled(monitor_stream(&mut conn, &topic, &sender))
                    .await;

                if let Some(Err(err)) = xread_err {
                    evict_connection(&host).await;
                    outage
                        .on_error(err.kind(), &err, "Redis stream XREAD error")
                        .await;
                }
            }
            Err(err) => {
                outage
                    .on_error(err.kind(), &err, "Redis stream connection error")
                    .await;
            }
        }
    }
}

/// Polls Redis for new stream entries on an established connection and broadcasts results.
///
/// Returns the error if an `XREAD` call fails. The caller is responsible for evicting the connection and
/// applying backoff in that case.
async fn monitor_stream(
    conn: &mut redis::aio::ConnectionManager,
    topic: &str,
    sender: &Sender<ByteMessage>,
) -> Result<(), RedisError> {
    let mut latest_id = String::from("$");
    let opts = StreamReadOptions::default().block(READ_BLOCK_TIME_MS);

    loop {
        let reply = conn
            .xread_options::<&str, &str, StreamReadReply>(&[topic], &[latest_id.as_str()], &opts)
            .await?;
        let entries = reply.keys.into_iter().flat_map(|stream| stream.ids);
        for entry in entries {
            latest_id = entry.id.clone();
            if let Ok(message) = stream_entry_to_byte_message(&entry) {
                let _ = sender.send(message);
            } else {
                warn!("Failed to convert message: {entry:?}");
            }
        }
    }
}
