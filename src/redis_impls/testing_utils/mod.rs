use std::{sync::LazyLock, thread::spawn, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{
        Mutex,
        oneshot::{Receiver, Sender, channel},
    },
    time::sleep,
};

static SERVER: LazyLock<MockRedis> = LazyLock::new(|| MockRedis::start());
static TEST_MESSAGES: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(vec![]));
const HOST: &str = "127.0.0.1:6379";

pub struct TestContext;
impl TestContext {
    pub async fn check_for_message(&self, expected: &str) -> Result<(), String> {
        for _ in 0..10 {
            {
                let messages = TEST_MESSAGES.lock().await;
                if messages.iter().any(|msg| msg.contains(expected)) {
                    return Ok(());
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        Err(format!("Message not found: {}", expected))
    }

    pub fn get_host(&self) -> String {
        let mut host = HOST.to_string();
        host.insert_str(0, "redis://");
        host
    }

    pub async fn new() -> Self {
        while !SERVER.is_running() {
            sleep(Duration::from_millis(50)).await;
        }
        TestContext
    }
}

struct MockRedis {
    sender: Option<Sender<()>>,
}
impl MockRedis {
    fn start() -> Self {
        let (sender, reciever) = channel();
        spawn(move || listen_for_requests(reciever));
        MockRedis {
            sender: Some(sender),
        }
    }

    fn is_running(&self) -> bool {
        self.sender
            .as_ref()
            .is_some_and(|sender| !sender.is_closed())
    }
}
impl Drop for MockRedis {
    fn drop(&mut self) {
        let _ = self.sender.take().and_then(|s| s.send(()).ok());
    }
}

fn listen_for_requests(receiver: Receiver<()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::select! {
                _ = async {
                    let listener = TcpListener::bind(HOST).await.unwrap();
                    let mut counter = 0;
                    loop {
                        match listener.accept().await {
                            Ok((stream, _)) => {
                                tokio::spawn(handle_connection(stream, counter));
                            }
                            Err(e) => {
                                eprintln!("Error accepting connection: {}", e);
                            }
                        }
                        counter += 1;
                    }
                } => {},
                _ = receiver => {
                    println!("Shutting down mock Redis server.");
                }
            }
        });
}

async fn handle_connection(mut stream: TcpStream, counter: u64) {
    let mut buffer = [0; 8192];
    loop {
        match stream.readable().await {
            Ok(_) => match stream.read(&mut buffer).await {
                Ok(0) => {
                    println!("Conn {counter} - Connection closed by client.");
                    break;
                }
                Ok(4) if &buffer[..4] == b"PING" => {
                    if let Err(e) = stream.write_all(b"+PONG\r\n").await {
                        eprintln!("Conn {counter} - Error writing PONG response: {}", e);
                        break;
                    }
                }
                Ok(n) => {
                    let data = String::from_utf8_lossy(&buffer[..n]);
                    println!("Conn {counter} - Received: {}", data);
                    TEST_MESSAGES.lock().await.push(data.to_string());

                    if data.starts_with("PING") {
                        if let Err(e) = stream.write_all(&buffer[5..n]).await {
                            eprintln!("Conn {counter} - Error writing PONG response: {}", e);
                            break;
                        }
                    } else if data.starts_with("HELLO") {
                        if let Err(e) = stream.write_all(b"%7\r\n+server\r\n+mockredis\r\n+version\r\n+1.0.0\r\n+proto\r\n:2\r\n+id\r\n:0\r\n+mode\r\n+standalone\r\n+role\r\n+master\r\n+modules\r\n*0\r\n").await {
                            eprintln!("Conn {counter} - Error writing HELLO response: {}", e);
                            break;
                        }
                    }
                    // Simulate a Redis response (e.g., "+OK\r\n")
                    else if let Err(e) = stream.write_all(b"+OK\r\n").await {
                        eprintln!("Conn {counter} - Error writing response: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Conn {counter} - Error reading from stream: {}", e);
                    break;
                }
            },
            Err(e) => {
                eprintln!(
                    "Conn {counter} - Error waiting for stream to be readable: {}",
                    e
                );
                break;
            }
        }
        stream.flush().await.unwrap();
    }
}
