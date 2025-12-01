use kafka::{
    client::{FetchOffset, GroupOffsetStorage, RequiredAcks},
    consumer::Consumer,
    producer::{Producer, Record},
};
use std::{
    env,
    error::Error,
    fmt::{self, Debug},
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio_stream::wrappers::BroadcastStream;
use tracing::error;

fn handle<E: Error>(result: Result<(), E>) {
    match result {
        Ok(_) => (),
        Err(err) => error!("{}", err),
    }
}

const KAFKA_HOST: &str = "KAFKA_HOST";
const DEFAULT_KAFKA_HOST: &str = "acsys-services.fnal.gov:9092";
fn get_connection<T: Send + 'static>(
    connect: impl Fn(String) -> Result<T, PubSubError> + Send + 'static,
) -> Result<T, PubSubError> {
    let host = env::var(KAFKA_HOST).unwrap_or_else(|_| String::from(DEFAULT_KAFKA_HOST));
    let (sender, receiver) = mpsc::channel();
    let _ = thread::spawn(move || {
        let connection = connect(host);
        handle(sender.send(connection));
    });
    match receiver.recv_timeout(Duration::from_secs(1)) {
        Ok(result) => result,
        Err(err) => {
            error!("{}", err);
            Err(PubSubError::default())
        }
    }
}

fn get_consumer(topic: String) -> Result<Consumer, PubSubError> {
    get_connection(move |host: String| {
        Consumer::from_hosts(vec![host])
            .with_topic(topic.clone())
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(Some(GroupOffsetStorage::Kafka))
            .create()
            .map_err(|err| {
                error!("{}", err);
                PubSubError::default()
            })
    })
}

fn do_poll<R, E: Error>(
    consumer: &mut Consumer,
    mut append_msg: impl FnMut(String) -> Result<R, E>,
) -> Result<(), PubSubError> {
    match consumer.poll() {
        Ok(message_sets) => {
            for set in message_sets.iter() {
                for msg in set.messages() {
                    match str::from_utf8(msg.value) {
                        Ok(decoded) => match append_msg(decoded.to_owned()) {
                            Ok(_) => (),
                            Err(err) => {
                                handle(Err(err));
                                return Err(PubSubError::default());
                            }
                        },
                        Err(err) => error!("{}", err),
                    };
                }
                handle(consumer.consume_messageset(set));
            }
            handle(consumer.commit_consumed());
        }
        Err(err) => {
            error!("{}", err);
            let _ = append_msg(String::from(
                "An error occurred while consuming messages. See server logs for details. Closing stream.",
            ));
            return Err(PubSubError::default());
        }
    };
    Ok(())
}

/// A structure for retrieving a snapshot of a message topic.
/// Exposes the `for_topic` method, which connects to the message broker,
/// loads all messages currently on the specified topic, and returns them
/// to the caller.
#[derive(Debug)]
pub struct Snapshot {
    pub data: Vec<String>,
}
impl Snapshot {
    /// Generates a snapshot of the messages on the given topic
    pub fn for_topic(topic: String) -> Result<Self, PubSubError> {
        let mut consumer = get_consumer(topic)?;
        let mut data: Vec<String> = Vec::new();

        let mut cur_size: usize = 0;
        loop {
            match do_poll(&mut consumer, |msg: String| {
                data.push(msg);
                Result::<(), PubSubError>::Ok(())
            }) {
                Ok(_) => {
                    if cur_size < data.len() {
                        cur_size = data.len();
                    } else {
                        break;
                    }
                }
                Err(err) => return Err(err),
            }
        }
        Ok(Self { data })
    }
}

struct MessageJob {
    consumer: Consumer,
    sender: Arc<Sender<String>>,
}
impl MessageJob {
    pub fn run(&mut self) {
        while do_poll(&mut self.consumer, |msg: String| self.sender.send(msg)).is_ok() {
            thread::sleep(Duration::from_millis(100));
        }
    }
}

/// A structure for subscribing to a message topic. Returns the values as a stream of messages for clients to handle.
#[derive(Debug)]
pub struct Subscriber {
    /// Keeps the channel open while the subscriber waits for clients to ask for a stream.
    _channel_lock: Receiver<String>,
    sender: Arc<Sender<String>>,
}
impl Subscriber {
    fn from(consumer: Consumer) -> Self {
        let (sender, _channel_lock) = broadcast::channel::<String>(20);
        let thread_sender = Arc::new(sender);
        let instance_sender = Arc::clone(&thread_sender);
        let mut message_job = MessageJob {
            consumer,
            sender: thread_sender,
        };
        let _task_handle = thread::spawn(move || {
            message_job.run();
        });

        Self {
            _channel_lock,
            sender: instance_sender,
        }
    }

    /// Generates a new subscriber for the provided topic.
    /// A new thread will be started and run in the background to poll for
    /// messages. The thread will terminate when this subscriber is dropped.
    pub fn for_topic(topic: String) -> Result<Self, PubSubError> {
        let consumer = get_consumer(topic)?;
        Ok(Self::from(consumer))
    }

    /// Streams messages that appear on the subscribed topic.
    pub fn get_stream(&self) -> BroadcastStream<String> {
        BroadcastStream::new(self.sender.subscribe())
    }
}

/// Struct for sending messages to a configured topic.
pub struct Publisher {
    producer: Producer,
    topic: String,
}
impl Publisher {
    /// Configures a publisher for the provided topic.
    pub fn for_topic(topic: String) -> Result<Self, PubSubError> {
        get_connection(|host: String| {
            Producer::from_hosts(vec![host])
                .with_ack_timeout(Duration::from_secs(1))
                .with_required_acks(RequiredAcks::One)
                .create()
                .map_err(|err| {
                    error!("{}", err);
                    PubSubError::default()
                })
        })
        .map(|producer| Self { producer, topic })
    }

    /// Sends the provided message to the configured topic.
    pub fn publish(&mut self, message: String) -> Result<(), PubSubError> {
        self.producer
            .send(&Record::from_value(self.topic.as_str(), message.as_bytes()))
            .map_err(|err| {
                error!("{}", err);
                PubSubError::default()
            })
    }
}
impl Debug for Publisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Publisher")
            .field("host", &self.producer.client().hosts())
            .field("topic", &self.topic)
            .finish()
    }
}

const CANNED_ERR_MESSAGE: &str = "An error occurred while attempting to connect to the message broker. See server logs for details.";

#[derive(Debug)]
pub struct PubSubError {
    message: &'static str,
}
impl Default for PubSubError {
    fn default() -> Self {
        Self {
            message: CANNED_ERR_MESSAGE,
        }
    }
}
impl fmt::Display for PubSubError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for PubSubError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pubsub_error_display() {
        let err = PubSubError::default();
        assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));
    }

    #[test]
    fn error_on_bad_kafka_consumer_host() {
        let result = Subscriber::for_topic(String::from("my_topic"));
        let err = result.expect_err("Expected the connection to fail, but it succeeded");
        assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));
    }

    #[test]
    fn error_on_bad_kafka_producer_host() {
        let result = Publisher::for_topic(String::from("my_topic"));
        let err = result.expect_err("Expected the connection to fail, but it succeeded");
        assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));
    }

    #[test]
    fn error_on_bad_snapshot_host() {
        let result = Snapshot::for_topic(String::from("my_topic"));
        let err = result.expect_err("Expected the connection to fail, but it succeeded");
        assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));
    }

    #[test]
    fn handles_err() {
        assert_eq!(handle::<PubSubError>(Ok(())), ());
        assert_eq!(handle(Err(PubSubError::default())), ());
    }
}
