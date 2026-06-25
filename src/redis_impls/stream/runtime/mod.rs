//! Background Redis Stream polling runtime used by shared subscribers.
//!
//! A [`RedisStream`] owns a cancellable task that repeatedly issues `XREAD` calls, converts each
//! returned stream entry into a [`ByteMessage`](crate::ByteMessage), and broadcasts results to all
//! active listeners for the associated host/topic pair.

use std::sync::atomic::{AtomicBool, Ordering};

use redis::AsyncCommands;
use redis::streams::{StreamReadOptions, StreamReadReply};
use tokio::sync::broadcast::{self, Sender};
use tokio::task::spawn;
use tokio::time::{Duration, sleep};
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;
use tracing::error;

use super::stream_entry_to_byte_message;
use crate::redis_impls::{evict_connection, get_connection};
use crate::{ByteMessage, PubSubError};

/// Maximum exponential-backoff delay applied after connection-establishment failures.
const MAX_WAIT_TIME: Duration = Duration::from_secs(30);
/// Redis `XREAD BLOCK` duration used for each poll request.
const READ_BLOCK_TIME_MS: usize = 250;

/// Shared Redis polling runtime for one host/topic pair.
///
/// Constructing this type does not immediately start broker work. The first call to
/// [`RedisStream::get_stream()`](crate::redis_impls::stream::runtime::RedisStream::get_stream)
/// creates a receiver and lazily starts the shared background polling task.
#[derive(Debug)]
pub(crate) struct RedisStream {
    cancel_token: CancellationToken,
    host: String,
    topic: String,
    sender: Sender<Result<ByteMessage, PubSubError>>,
    started: AtomicBool,
}

impl RedisStream {
    /// Creates a new shared polling runtime handle for the given Redis Stream topic.
    pub(crate) fn new(host: String, topic: String) -> Self {
        let (sender, _) = broadcast::channel(100);
        let cancel_token = CancellationToken::new();

        Self {
            cancel_token,
            host,
            topic,
            sender,
            started: AtomicBool::new(false),
        }
    }

    /// Returns a new broadcast-backed stream subscribed to this runtime's fan-out channel.
    ///
    /// The first call lazily starts the shared background worker.
    pub(crate) fn get_stream(&self) -> BroadcastStream<Result<ByteMessage, PubSubError>> {
        let stream = BroadcastStream::new(self.sender.subscribe());
        self.ensure_started();
        stream
    }

    /// Returns the number of active listeners currently attached to this runtime.
    pub(crate) fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }

    fn ensure_started(&self) {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            spawn(start_stream(
                self.host.clone(),
                self.topic.clone(),
                self.sender.clone(),
                self.cancel_token.child_token(),
            ));
        }
    }
}

impl Drop for RedisStream {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

/// Runs the reconnect loop for a shared Redis Stream runtime until cancellation.
async fn start_stream(
    host: String,
    topic: String,
    sender: Sender<Result<ByteMessage, PubSubError>>,
    cancel_token: CancellationToken,
) {
    let mut error_backoff_time = Duration::from_secs(1);

    while !cancel_token.is_cancelled() {
        match get_connection(&host).await {
            Ok(mut conn) => {
                // Reset any error backoff, now that we have a connection
                error_backoff_time = Duration::from_secs(1);

                let ended_due_to_connection_err = cancel_token
                    .run_until_cancelled(monitor_stream(&mut conn, &topic, &sender))
                    .await
                    .is_some();

                if ended_due_to_connection_err {
                    evict_connection(&host).await;
                }
            }
            Err(err) => {
                let _ = sender.send(Err(err.clone()));
                error_backoff_time = handle_connection_err(err, error_backoff_time).await;
            }
        }
    }
}

/// Polls Redis for new stream entries on an established connection and broadcasts results.
async fn monitor_stream(
    conn: &mut redis::aio::ConnectionManager,
    topic: &str,
    sender: &Sender<Result<ByteMessage, PubSubError>>,
) {
    let mut latest_id = String::from("$");
    let opts = StreamReadOptions::default().block(READ_BLOCK_TIME_MS);

    loop {
        match conn
            .xread_options::<&str, &str, StreamReadReply>(&[topic], &[latest_id.as_str()], &opts)
            .await
        {
            Ok(reply) => {
                let entries = reply.keys.into_iter().flat_map(|stream| stream.ids);
                for entry in entries {
                    latest_id = entry.id.clone();
                    let message = stream_entry_to_byte_message(&entry);
                    let _ = sender.send(message);
                }
            }
            Err(err) => {
                let _ = sender.send(Err(PubSubError::from(err)));
                break;
            }
        }
    }
}

/// Logs a connection-establishment failure, waits, and returns the next backoff duration.
async fn handle_connection_err(err: PubSubError, mut wait_time: Duration) -> Duration {
    error!("{err:?}");
    sleep(wait_time).await;

    wait_time *= 2;
    if wait_time > MAX_WAIT_TIME {
        MAX_WAIT_TIME
    } else {
        wait_time
    }
}
