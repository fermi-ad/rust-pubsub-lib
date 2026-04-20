use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, oneshot};
use tokio::time::{Duration, timeout};

#[test]
fn parse_bulk_token_reads_complete_token() {
    let parsed = parse_bulk_token("hello\r\nrest", 5).unwrap();
    assert_eq!(("hello", "rest"), parsed);
}

#[test]
fn parse_bulk_token_returns_none_for_incomplete_input() {
    assert!(parse_bulk_token("hel", 5).is_none());
    assert!(parse_bulk_token("hello--rest", 5).is_none());
}

#[test]
fn parse_one_resp_array_parses_complete_command() {
    let input = "*3\r\n$7\r\nPUBLISH\r\n$5\r\ntopic\r\n$7\r\npayload\r\nrest";
    let parsed = parse_one_resp_array(input).unwrap();
    assert_eq!("PUBLISH", parsed.0.0);
    assert_eq!(vec!["topic".to_string(), "payload".to_string()], parsed.0.1);
    assert_eq!("rest", parsed.1);
}

#[test]
fn parse_one_resp_array_returns_none_for_partial_input() {
    let input = "*2\r\n$7\r\nPUBLISH\r\n$5\r\ntop";
    assert!(parse_one_resp_array(input).is_none());
}

#[test]
fn parse_resp_command_parses_inline_command() {
    let parsed = parse_resp_command("publish topic payload").unwrap();
    assert_eq!("PUBLISH", parsed.0);
    assert_eq!(vec!["topic".to_string(), "payload".to_string()], parsed.1);
}

#[test]
fn parse_resp_commands_parses_mixed_inline_and_resp_input() {
    let input = concat!(
        "PING\r\n",
        "*3\r\n$7\r\nPUBLISH\r\n$5\r\ntopic\r\n$7\r\npayload\r\n"
    );
    let (commands, consumed) = parse_resp_commands(input);

    assert_eq!(2, commands.len());
    assert_eq!(("PING".to_string(), vec![]), commands[0]);
    assert_eq!(
        (
            "PUBLISH".to_string(),
            vec!["topic".to_string(), "payload".to_string()]
        ),
        commands[1]
    );
    assert_eq!(input.len(), consumed);
}

#[test]
fn split_line_splits_resp_line() {
    let parsed = split_line("HELLO\r\nWORLD").unwrap();
    assert_eq!(("HELLO", "WORLD"), parsed);
}

#[tokio::test]
async fn read_stream_dispatches_client_publish_subscribe_and_default() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (message_sender, mut message_receiver) = broadcast::channel(8);
    let pubsub_messages = HashMap::from([("topic".to_string(), vec!["queued".to_string()])]);

    let server = tokio::spawn(async move {
        let (mut server_stream, _) = listener.accept().await.unwrap();
        let mut pending = String::new();
        let mut buffer = [0; 8192];

        let stream_state = Arc::new(StreamState::default());
        read_stream(
            &mut server_stream,
            &mut pending,
            &mut buffer,
            &message_sender,
            &pubsub_messages,
            &stream_state,
        )
        .await
        .unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    let payload = concat!(
        "CLIENT SETINFO LIB-NAME mock\r\n",
        "*3\r\n$7\r\nPUBLISH\r\n$5\r\ntopic\r\n$7\r\npayload\r\n",
        "*2\r\n$9\r\nSUBSCRIBE\r\n$5\r\ntopic\r\n",
        "UNKNOWN stuff\r\n"
    );
    client.write_all(payload.as_bytes()).await.unwrap();

    let mut response = vec![0_u8; 512];
    let bytes_read = timeout(Duration::from_secs(1), client.read(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response = String::from_utf8_lossy(&response[..bytes_read]).to_string();

    assert!(response.contains("+OK\r\n"));
    assert!(response.contains(":1\r\n"));
    assert!(response.contains("subscribe"));
    assert!(response.contains("message"));
    assert_eq!("payload", message_receiver.recv().await.unwrap());

    server.await.unwrap();
}

#[tokio::test]
async fn read_stream_supports_xadd_xrange_and_xread() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (message_sender, mut message_receiver) = broadcast::channel(8);
    let stream_state = Arc::new(StreamState::default());

    let server = tokio::spawn({
        let stream_state = stream_state.clone();
        async move {
            let (mut server_stream, _) = listener.accept().await.unwrap();
            let mut pending = String::new();
            let mut buffer = [0; 8192];

            read_stream(
                &mut server_stream,
                &mut pending,
                &mut buffer,
                &message_sender,
                &HashMap::new(),
                &stream_state,
            )
            .await
            .unwrap();
        }
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    let payload = concat!(
        "*5\r\n$4\r\nXADD\r\n$5\r\ntopic\r\n$1\r\n*\r\n$4\r\ndata\r\n$7\r\npayload\r\n",
        "*4\r\n$6\r\nXRANGE\r\n$5\r\ntopic\r\n$1\r\n-\r\n$1\r\n+\r\n",
        "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$1\r\n0\r\n$7\r\nSTREAMS\r\n$5\r\ntopic\r\n$3\r\n1-0\r\n"
    );
    client.write_all(payload.as_bytes()).await.unwrap();

    let mut response = vec![0_u8; 1024];
    let bytes_read = timeout(Duration::from_secs(1), client.read(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response = String::from_utf8_lossy(&response[..bytes_read]).to_string();

    assert!(response.contains("$3\r\n1-0\r\n"));
    assert!(response.contains("payload"));
    assert_eq!("payload", message_receiver.recv().await.unwrap());

    server.await.unwrap();
}

#[tokio::test]
async fn listen_for_requests_stops_after_shutdown() {
    let shutdown_token = CancellationToken::new();
    let (host_sender, host_receiver) = oneshot::channel();
    let (message_sender, _) = broadcast::channel(8);

    let server = tokio::spawn(listen_for_requests(
        shutdown_token.clone(),
        host_sender,
        message_sender,
        HashMap::new(),
    ));

    let host = host_receiver.await.unwrap();
    let _stream = TcpStream::connect(host).await.unwrap();

    shutdown_token.cancel();
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}
