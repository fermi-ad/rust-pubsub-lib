//! Test-only Redis helpers used to exercise the Redis-backed implementations.
//!
//! This module contains a lightweight in-process mock Redis server and support code for driving
//! publish/subscribe scenarios in tests. The API is exported behind the `testing-utils` feature for
//! repository and downstream test suites, but it should not be treated as a production Redis server.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests;

/// Test harness for running redis-based tests against the in-process mock server.
///
/// This type is intended for integration tests and is not meant for production use.
/// Each call to [`TestHarness::new()`](crate::redis_impls::testing_utils::TestHarness::new)
/// starts a fresh mock server task and returns its connection URI.
pub struct TestHarness {
    host: String,
    message_receiver: broadcast::Receiver<String>,
    shutdown_token: CancellationToken,
}
impl TestHarness {
    /// Polls captured publish payloads and returns `Ok(())` once `expected` is observed.
    ///
    /// The match is substring-based so tests can assert on an expected value without depending on
    /// every byte of the serialized RESP frame.
    pub async fn check_for_message(&mut self, expected: &str) -> Result<(), String> {
        for _ in 0..10 {
            if let Ok(message) = self.message_receiver.try_recv()
                && message.contains(expected)
            {
                return Ok(());
            }
            sleep(Duration::from_millis(100)).await;
        }
        Err(format!("Message not found: {}", expected))
    }

    /// Returns the redis connection URI for the mock server.
    pub fn get_host(&self) -> String {
        format!("redis://{}", self.host)
    }

    /// Starts a new mock Redis server and returns a test context for interacting with it.
    ///
    /// The optional `pubsub_messages` map preloads messages that will be emitted after a client
    /// subscribes to the corresponding topic.
    pub async fn new(pubsub_messages: Option<HashMap<String, Vec<String>>>) -> Self {
        let shutdown_token = CancellationToken::new();
        let (host_sender, host_receiver) = oneshot::channel();
        let (message_sender, message_receiver) = broadcast::channel(32);

        let worker_shutdown = shutdown_token.clone();
        spawn(listen_for_requests(
            worker_shutdown,
            host_sender,
            message_sender,
            pubsub_messages.unwrap_or_default(),
        ));

        let host = host_receiver
            .await
            .expect("Failed to receive mock Redis host");
        TestHarness {
            host,
            message_receiver,
            shutdown_token,
        }
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
    payload: String,
}

#[derive(Debug, Default)]
struct StreamState {
    entries: Mutex<Vec<StreamEntry>>,
}

impl StreamState {
    async fn push(&self, payload: String) -> String {
        let mut entries = self.entries.lock().await;
        let id = format!("{}-0", entries.len() + 1);
        entries.push(StreamEntry {
            id: id.clone(),
            payload,
        });
        id
    }

    async fn all(&self) -> Vec<StreamEntry> {
        self.entries.lock().await.clone()
    }

    async fn after(&self, last_seen: &str) -> Vec<StreamEntry> {
        let entries = self.entries.lock().await;
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

fn encode_bulk_string(value: &str) -> String {
    format!("${}\r\n{}\r\n", value.len(), value)
}

fn encode_stream_entries(entries: &[StreamEntry]) -> String {
    let mut response = format!("*{}\r\n", entries.len());
    for entry in entries {
        response.push_str("*2\r\n");
        response.push_str(&encode_bulk_string(&entry.id));
        response.push_str("*2\r\n");
        response.push_str("$4\r\ndata\r\n");
        response.push_str(&encode_bulk_string(&entry.payload));
    }
    response
}

fn encode_xread_response(topic: &str, entries: &[StreamEntry]) -> String {
    let mut response = String::from("*1\r\n*2\r\n");
    response.push_str(&encode_bulk_string(topic));
    response.push_str(&encode_stream_entries(entries));
    response
}

async fn handle_connection(
    mut stream: TcpStream,
    message_sender: broadcast::Sender<String>,
    pubsub_messages: HashMap<String, Vec<String>>,
    stream_state: Arc<StreamState>,
    shutdown_token: CancellationToken,
) {
    let mut buffer = [0; 8192];
    let mut pending = String::new();
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                println!("Closing connection due to server shutdown.");
                break;
            }
            val = stream.readable() => {
                if let Err(e) = val {
                    eprintln!("Error waiting for stream to become readable: {}", e);
                    break;
                }
                if let Err(e) = read_stream(
                    &mut stream,
                    &mut pending,
                    &mut buffer,
                    &message_sender,
                    &pubsub_messages,
                    &stream_state,
                ).await {
                    eprintln!("Error reading from connection: {}", e);
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
    pubsub_messages: &HashMap<String, Vec<String>>,
    stream_state: &Arc<StreamState>,
) -> Result<(), String> {
    match stream.read(buffer).await {
        Ok(0) => {
            println!("Connection closed by client.");
            return Ok(());
        }
        Ok(n) => {
            pending.push_str(&String::from_utf8_lossy(&buffer[..n]));

            let (commands, consumed) = parse_resp_commands(pending.as_str());
            if consumed > 0 {
                pending.drain(..consumed);
            }

            for (cmd, args) in commands {
                println!("cmd={cmd} args={args:?}");

                match cmd.as_str() {
                    "CLIENT" => {
                        if let Err(e) = stream.write_all(b"+OK\r\n").await {
                            return Err(format!("Error writing CLIENT response: {}", e));
                        }
                    }
                    "PUBLISH" => {
                        if let Some(payload) = args.last() {
                            let _ = message_sender.send(payload.clone());
                        }
                        if let Err(e) = stream.write_all(b":1\r\n").await {
                            return Err(format!("Error writing PUBLISH response: {}", e));
                        }
                    }
                    "XADD" => {
                        let payload = args
                            .last()
                            .cloned()
                            .ok_or_else(|| "XADD missing payload".to_string())?;
                        let id = stream_state.push(payload.clone()).await;
                        let _ = message_sender.send(payload);
                        let response = encode_bulk_string(&id);
                        if let Err(e) = stream.write_all(response.as_bytes()).await {
                            return Err(format!("Error writing XADD response: {}", e));
                        }
                    }
                    "XRANGE" => {
                        let entries = stream_state.all().await;
                        let response = encode_stream_entries(&entries);
                        if let Err(e) = stream.write_all(response.as_bytes()).await {
                            return Err(format!("Error writing XRANGE response: {}", e));
                        }
                    }
                    "XREAD" => {
                        let topic = args
                            .iter()
                            .position(|arg| arg == "STREAMS")
                            .and_then(|idx| args.get(idx + 1))
                            .cloned()
                            .unwrap_or_default();
                        let last_seen = args.last().map(String::as_str).unwrap_or("$");
                        let entries = stream_state.after(last_seen).await;
                        let response = if entries.is_empty() {
                            "$-1\r\n".to_string()
                        } else {
                            encode_xread_response(&topic, &entries)
                        };
                        if let Err(e) = stream.write_all(response.as_bytes()).await {
                            return Err(format!("Error writing XREAD response: {}", e));
                        }
                    }
                    "SUBSCRIBE" => {
                        for (num, topic) in args.iter().enumerate() {
                            let msg = format!(
                                "*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:{}\r\n",
                                topic.len(),
                                topic,
                                num + 1
                            );
                            if let Err(e) = stream.write_all(msg.as_bytes()).await {
                                return Err(format!("Error writing SUBSCRIBE response: {}", e));
                            }
                        }
                        for (topic, messages) in pubsub_messages.clone() {
                            for message in messages {
                                let msg = format!(
                                    "*3\r\n$7\r\nmessage\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                    topic.len(),
                                    topic,
                                    message.len(),
                                    message
                                );
                                if let Err(e) = stream.write_all(msg.as_bytes()).await {
                                    return Err(format!(
                                        "Error writing message for topic '{}': {}",
                                        topic, e
                                    ));
                                }
                            }
                        }
                    }
                    _ => {
                        if let Err(e) = stream.write_all(b"+OK\r\n").await {
                            return Err(format!("Error writing default response: {}", e));
                        }
                    }
                }
            }
        }
        Err(e) => {
            return Err(format!("Error reading from stream: {}", e));
        }
    }
    if let Err(e) = stream.flush().await {
        return Err(format!("Error flushing stream: {}", e));
    }
    Ok(())
}

async fn listen_for_requests(
    shutdown_token: CancellationToken,
    host_sender: oneshot::Sender<String>,
    message_sender: broadcast::Sender<String>,
    pubsub_messages: HashMap<String, Vec<String>>,
) {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Error binding mock Redis server: {}", e);
            return;
        }
    };
    let stream_state = Arc::new(StreamState::default());
    host_sender
        .send(listener.local_addr().unwrap().to_string())
        .unwrap();

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                println!("Shutting down mock Redis server.");
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _)) => {
                        spawn(handle_connection(
                            stream,
                            message_sender.clone(),
                            pubsub_messages.clone(),
                            stream_state.clone(),
                            shutdown_token.child_token(),
                        ));
                    }
                    Err(e) => {
                        eprintln!("Error accepting connection: {}", e);
                    }
                }
            }
        }
    }
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

fn parse_one_resp_array(input: &str) -> Option<((String, Vec<String>), &str)> {
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

    if tokens.is_empty() {
        return None;
    }

    let cmd = tokens[0].to_uppercase();
    let args = tokens.into_iter().skip(1).collect();
    Some(((cmd, args), rest))
}

fn parse_resp_command(data: &str) -> Option<(String, Vec<String>)> {
    let mut parts = data
        .split_whitespace()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }
    let cmd = parts.remove(0).to_uppercase();
    Some((cmd, parts))
}

fn parse_resp_commands(input: &str) -> (Vec<(String, Vec<String>)>, usize) {
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
