//! Test-only Redis helpers used to exercise the Redis-backed implementations.
//!
//! This module contains a lightweight in-process mock Redis server and support code for driving
//! publish/subscribe scenarios in tests. The API is exported behind the `testing-utils` feature for
//! repository and downstream test suites, but it should not be treated as a production Redis server.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

#[cfg(test)]
mod tests;

/// Test harness for running Redis-based tests against the in-process mock server.
///
/// This type is intended for integration tests and is not meant for production use. Each call to
/// [`TestHarness::new()`](crate::redis_impls::testing_utils::TestHarness::new) starts a fresh mock
/// server task and returns its connection URI.
pub struct TestHarness {
    host: String,
    message_receiver: broadcast::Receiver<String>,
    connection_receiver: broadcast::Receiver<String>,
    shutdown_token: CancellationToken,
}

impl TestHarness {
    /// Starts a new mock Redis server and returns a test context for interacting with it.
    ///
    /// The optional `pubsub_messages` map preloads messages that will be emitted after a client
    /// subscribes to the corresponding topic.
    pub async fn new(pubsub_messages: Option<HashMap<String, Vec<String>>>) -> Self {
        let shutdown_token = CancellationToken::new();
        let (host_sender, host_receiver) = oneshot::channel();
        let (message_sender, message_receiver) = broadcast::channel(32);
        let (connection_sender, connection_receiver) = broadcast::channel(32);

        let worker_shutdown = shutdown_token.clone();
        spawn(listen_for_requests(
            worker_shutdown,
            host_sender,
            message_sender,
            connection_sender,
            pubsub_messages.unwrap_or_default(),
        ));

        let host = host_receiver
            .await
            .expect("Failed to receive mock Redis host");
        TestHarness {
            host,
            message_receiver,
            connection_receiver,
            shutdown_token,
        }
    }

    /// Polls captured publish payloads and returns `Ok(())` once `expected` is observed.
    ///
    /// The match is substring-based so tests can assert on an expected value without depending on
    /// every byte of the serialized RESP frame.
    pub async fn check_for_message(&mut self, expected: &str) -> Result<(), String> {
        wait_for_broadcast_message(&mut self.message_receiver, expected, "Message").await
    }

    /// Returns the redis connection URI for the mock server.
    pub fn get_host(&self) -> String {
        format!("redis://{}", self.host)
    }

    /// Polls connection events and returns `Ok(())` once a client command containing `expected`
    /// is observed.
    pub async fn check_for_connection_command(&mut self, expected: &str) -> Result<(), String> {
        wait_for_broadcast_message(
            &mut self.connection_receiver,
            expected,
            "Connection command",
        )
        .await
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StreamEntry {
    id: String,
    /// Ordered list of field/value pairs for this stream entry.
    fields: Vec<(String, String)>,
}

#[derive(Debug, Default)]
struct StreamState {
    entries_by_topic: Mutex<HashMap<String, Vec<StreamEntry>>>,
}

impl StreamState {
    async fn push(&self, topic: &str, fields: Vec<(String, String)>) -> String {
        let mut entries_by_topic = self.entries_by_topic.lock().await;
        let entries = entries_by_topic.entry(topic.to_string()).or_default();
        let id = format!("{}-0", entries.len() + 1);
        entries.push(StreamEntry {
            id: id.clone(),
            fields,
        });
        id
    }

    async fn all(&self, topic: &str) -> Vec<StreamEntry> {
        self.entries_by_topic
            .lock()
            .await
            .get(topic)
            .cloned()
            .unwrap_or_default()
    }

    async fn after(&self, topic: &str, last_seen: &str) -> Vec<StreamEntry> {
        let entries_by_topic = self.entries_by_topic.lock().await;
        let Some(entries) = entries_by_topic.get(topic) else {
            return Vec::new();
        };
        if last_seen == "$" {
            return entries.last().cloned().into_iter().collect();
        }
        entries
            .iter()
            .filter(|entry| entry.id.as_str() > last_seen)
            .cloned()
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Command {
    Client {
        args: Vec<String>,
    },
    Publish {
        topic: String,
        payload: String,
    },
    /// An `XADD` command. `fields` holds the ordered field/value pairs after the entry ID (`*`).
    Xadd {
        topic: String,
        fields: Vec<(String, String)>,
    },
    Xrange {
        topic: String,
    },
    Xread {
        topic: String,
        last_seen: String,
    },
    Subscribe {
        topics: Vec<String>,
    },
    Unknown {
        name: String,
        args: Vec<String>,
    },
}

impl Command {
    fn from_parts(cmd: String, args: Vec<String>) -> Self {
        match cmd.as_str() {
            "CLIENT" => Command::Client { args },
            "PUBLISH" if args.len() >= 2 => {
                let [topic, ..] = args.as_slice() else {
                    unreachable!();
                };
                Command::Publish {
                    topic: topic.clone(),
                    payload: args.last().cloned().unwrap_or_default(),
                }
            }
            "XADD" if args.len() >= 4 => {
                // args layout (after MAXLEN trimming):
                //   [topic, maxlen_modifier?, maxlen_value?, id, field1, val1, field2, val2, ...]
                // We skip everything up to and including the `*` (or numeric) entry-ID token,
                // then collect the remaining tokens as field/value pairs.
                let [topic, rest @ ..] = args.as_slice() else {
                    unreachable!();
                };
                // Find the entry-ID token: it is either "*" or looks like "N-M".
                let field_start = rest
                    .iter()
                    .position(|a| a == "*" || a.contains('-'))
                    .map(|i| i + 1)
                    .unwrap_or(rest.len());
                let field_args = &rest[field_start..];
                let fields: Vec<(String, String)> = field_args
                    .chunks(2)
                    .filter_map(|chunk| {
                        if let [k, v] = chunk {
                            Some((k.clone(), v.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                Command::Xadd {
                    topic: topic.clone(),
                    fields,
                }
            }
            "XRANGE" if !args.is_empty() => {
                let [topic, ..] = args.as_slice() else {
                    unreachable!();
                };
                Command::Xrange {
                    topic: topic.clone(),
                }
            }
            "XREAD" => {
                let topic = args
                    .iter()
                    .position(|arg| arg == "STREAMS")
                    .and_then(|idx| args.get(idx + 1))
                    .cloned();
                match topic {
                    Some(topic) => Command::Xread {
                        topic,
                        last_seen: args.last().cloned().unwrap_or_else(|| "$".to_string()),
                    },
                    None => Command::Unknown { name: cmd, args },
                }
            }
            "SUBSCRIBE" => Command::Subscribe { topics: args },
            _ => Command::Unknown { name: cmd, args },
        }
    }

    fn describe(&self) -> String {
        match self {
            Command::Client { args } => format!("CLIENT {args:?}"),
            Command::Publish { topic, payload } => {
                format!("PUBLISH [{topic:?}, {payload:?}]")
            }
            Command::Xadd { topic, fields } => format!("XADD [{topic:?}, {fields:?}]"),
            Command::Xrange { topic } => format!("XRANGE [{topic:?}]"),
            Command::Xread { topic, last_seen } => {
                format!("XREAD [{topic:?}, {last_seen:?}]")
            }
            Command::Subscribe { topics } => format!("SUBSCRIBE {topics:?}"),
            Command::Unknown { name, args } => format!("{name} {args:?}"),
        }
    }
}

async fn listen_for_requests(
    shutdown_token: CancellationToken,
    host_sender: oneshot::Sender<String>,
    message_sender: broadcast::Sender<String>,
    connection_sender: broadcast::Sender<String>,
    pubsub_messages: HashMap<String, Vec<String>>,
) {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(e) => {
            error!("Error binding mock Redis server: {}", e);
            return;
        }
    };
    let stream_state = Arc::new(StreamState::default());
    let pubsub_messages = Arc::new(pubsub_messages);
    host_sender
        .send(listener.local_addr().unwrap().to_string())
        .unwrap();

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                debug!("Shutting down mock Redis server.");
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _)) => {
                        spawn(handle_connection(
                            stream,
                            message_sender.clone(),
                            connection_sender.clone(),
                            pubsub_messages.clone(),
                            stream_state.clone(),
                            shutdown_token.child_token(),
                        ));
                    }
                    Err(e) => {
                        error!("Error accepting connection: {}", e);
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    message_sender: broadcast::Sender<String>,
    connection_sender: broadcast::Sender<String>,
    pubsub_messages: Arc<HashMap<String, Vec<String>>>,
    stream_state: Arc<StreamState>,
    shutdown_token: CancellationToken,
) {
    let mut buffer = [0; 8192];
    let mut pending = String::new();
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                debug!("Closing connection due to server shutdown.");
                break;
            }
            val = stream.readable() => {
                if let Err(e) = val {
                    error!("Error waiting for stream to become readable: {}", e);
                    break;
                }
                if let Err(e) = read_stream(
                    &mut stream,
                    &mut pending,
                    &mut buffer,
                    &message_sender,
                    &connection_sender,
                    &pubsub_messages,
                    &stream_state,
                ).await {
                    error!("Error reading from connection: {}", e);
                    break;
                }
            }
        }
    }
}

async fn read_stream(
    stream: &mut TcpStream,
    pending: &mut String,
    buffer: &mut [u8],
    message_sender: &broadcast::Sender<String>,
    connection_sender: &broadcast::Sender<String>,
    pubsub_messages: &HashMap<String, Vec<String>>,
    stream_state: &Arc<StreamState>,
) -> Result<(), String> {
    let responses = match stream.read(buffer).await {
        Ok(0) => {
            debug!("Connection closed by client.");
            return Ok(());
        }
        Ok(n) => {
            pending.push_str(&String::from_utf8_lossy(&buffer[..n]));

            let (commands, consumed) = parse_resp_commands(pending.as_str());
            if consumed > 0 {
                pending.drain(..consumed);
            }

            let mut responses: Vec<String> = Vec::new();
            for command in commands {
                debug!("command={command:?}");
                let _ = connection_sender.send(command.describe());
                responses.push(
                    execute_command(command, message_sender, pubsub_messages, stream_state).await?,
                );
            }
            responses
        }
        Err(e) => {
            return Err(format!("Error reading from stream: {}", e));
        }
    };

    for response in responses {
        if let Err(e) = stream.write_all(response.as_bytes()).await {
            return Err(format!("Error writing response: {}", e));
        }
    }

    if let Err(e) = stream.flush().await {
        return Err(format!("Error flushing stream: {}", e));
    }
    Ok(())
}

async fn execute_command(
    command: Command,
    message_sender: &broadcast::Sender<String>,
    pubsub_messages: &HashMap<String, Vec<String>>,
    stream_state: &Arc<StreamState>,
) -> Result<String, String> {
    match command {
        Command::Client { .. } => Ok("+OK\r\n".to_string()),
        Command::Publish { payload, .. } => {
            let _ = message_sender.send(payload);
            Ok(":1\r\n".to_string())
        }
        Command::Xadd { topic, fields } => {
            // Broadcast all field values joined so check_for_message can match any of them.
            let broadcast_payload = fields
                .iter()
                .map(|(_, v)| v.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            let id = stream_state.push(&topic, fields).await;
            let _ = message_sender.send(broadcast_payload);
            Ok(encode_bulk_string(&id))
        }
        Command::Xrange { topic } => {
            let entries = stream_state.all(&topic).await;
            Ok(encode_stream_entries(&entries))
        }
        Command::Xread { topic, last_seen } => {
            let entries = stream_state.after(&topic, &last_seen).await;
            if entries.is_empty() {
                Ok("$-1\r\n".to_string())
            } else {
                Ok(encode_xread_response(&topic, &entries))
            }
        }
        Command::Subscribe { topics } => {
            let mut response = String::new();
            let subscribed_topics: HashSet<_> = topics.iter().cloned().collect();
            for (index, topic) in topics.iter().enumerate() {
                response.push_str(&encode_subscribe_confirmation(topic, index + 1));
            }
            for topic in &topics {
                if subscribed_topics.contains(topic)
                    && let Some(messages) = pubsub_messages.get(topic)
                {
                    for message in messages {
                        response.push_str(&encode_pubsub_message(topic, message));
                    }
                }
            }
            Ok(response)
        }
        Command::Unknown { .. } => Ok("+OK\r\n".to_string()),
    }
}

async fn wait_for_broadcast_message(
    receiver: &mut broadcast::Receiver<String>,
    expected: &str,
    label: &str,
) -> Result<(), String> {
    let timeout_at = Duration::from_secs(1);
    let expected = expected.to_string();

    timeout(timeout_at, async {
        loop {
            match receiver.recv().await {
                Ok(message) if message.contains(&expected) => return Ok(()),
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(format!(
                        "{label} channel closed before observing {expected}"
                    ));
                }
            }
        }
    })
    .await
    .map_err(|_| format!("{label} not found: {expected}"))?
}

fn encode_bulk_string(value: &str) -> String {
    format!("${}\r\n{}\r\n", value.len(), value)
}

fn encode_stream_entries(entries: &[StreamEntry]) -> String {
    let mut response = format!("*{}\r\n", entries.len());
    for entry in entries {
        response.push_str("*2\r\n");
        response.push_str(&encode_bulk_string(&entry.id));
        // Encode the field/value pairs as a flat RESP array: [field1, val1, field2, val2, ...]
        let field_count = entry.fields.len() * 2;
        response.push_str(&format!("*{field_count}\r\n"));
        for (field, value) in &entry.fields {
            response.push_str(&encode_bulk_string(field));
            response.push_str(&encode_bulk_string(value));
        }
    }
    response
}

fn encode_xread_response(topic: &str, entries: &[StreamEntry]) -> String {
    let mut response = String::from("*1\r\n*2\r\n");
    response.push_str(&encode_bulk_string(topic));
    response.push_str(&encode_stream_entries(entries));
    response
}

fn encode_subscribe_confirmation(topic: &str, subscription_count: usize) -> String {
    format!(
        "*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:{}\r\n",
        topic.len(),
        topic,
        subscription_count
    )
}

fn encode_pubsub_message(topic: &str, payload: &str) -> String {
    format!(
        "*3\r\n$7\r\nmessage\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        topic.len(),
        topic,
        payload.len(),
        payload
    )
}

fn parse_bulk_token(input: &str, len: usize) -> Option<(&str, &str)> {
    if input.len() < len + 2 {
        return None;
    }
    let token = &input[..len];
    let suffix = &input[len..];
    if !suffix.starts_with("\r\n") {
        return None;
    }
    Some((token, &suffix[2..]))
}

fn parse_one_resp_array(input: &str) -> Option<(Command, &str)> {
    let (header, mut rest) = split_line(input)?;
    if !header.starts_with('*') {
        return None;
    }

    let count: usize = header[1..].parse().ok()?;
    let mut tokens = Vec::with_capacity(count);

    for _ in 0..count {
        let (len_line, after_len_line) = split_line(rest)?;
        if !len_line.starts_with('$') {
            return None;
        }

        let len: usize = len_line[1..].parse().ok()?;
        let (token, after_token) = parse_bulk_token(after_len_line, len)?;
        tokens.push(token.to_string());
        rest = after_token;
    }

    build_command(tokens).map(|command| (command, rest))
}

fn parse_resp_command(data: &str) -> Option<Command> {
    let parts = data
        .split_whitespace()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    build_command(parts)
}

fn build_command(mut parts: Vec<String>) -> Option<Command> {
    if parts.is_empty() {
        return None;
    }
    let cmd = parts.remove(0).to_uppercase();
    Some(Command::from_parts(cmd, parts))
}

fn parse_resp_commands(input: &str) -> (Vec<Command>, usize) {
    let mut remaining = input;
    let mut commands = Vec::new();

    loop {
        let parsed = if remaining.starts_with('*') {
            parse_one_resp_array(remaining)
        } else {
            let (line, rest) = match split_line(remaining) {
                Some(vals) => vals,
                None => break,
            };
            parse_resp_command(line).map(|cmd| (cmd, rest))
        };

        let Some((cmd, rest)) = parsed else {
            break;
        };

        if rest.len() == remaining.len() {
            break;
        }

        commands.push(cmd);
        remaining = rest;
    }

    (commands, input.len() - remaining.len())
}

fn split_line(input: &str) -> Option<(&str, &str)> {
    let idx = input.find("\r\n")?;
    Some((&input[..idx], &input[idx + 2..]))
}
