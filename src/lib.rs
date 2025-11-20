use kafka::{
    client::{FetchOffset, GroupOffsetStorage, RequiredAcks},
    consumer::Consumer,
    producer::{Producer, Record},
};
use std::{
    env,
    error::Error,
    fmt::{self, Debug},
    sync::Arc,
    thread,
    time::Duration,
};
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio_stream::wrappers::BroadcastStream;
use tracing::error;

struct MessageJob {
    consumer: Consumer,
    sender: Arc<Sender<String>>,
}
impl MessageJob {
    fn handle<E: Error>(result: Result<(), E>) {
        match result {
            Ok(_) => (),
            Err(err) => error!("{}", err),
        }
    }

    pub fn run(&mut self) {
        loop {
            match self.consumer.poll() {
                Ok(message_sets) => {
                    for set in message_sets.iter() {
                        for msg in set.messages() {
                            match str::from_utf8(msg.value) {
                                Ok(decoded) => match self.sender.send(decoded.to_owned()) {
                                    Ok(_) => (),
                                    Err(err) => {
                                        error!("{}", err);
                                        return;
                                    }
                                },
                                Err(err) => error!("{}", err),
                            };
                        }
                        Self::handle(self.consumer.consume_messageset(set));
                    }
                    Self::handle(self.consumer.commit_consumed());
                }
                Err(err) => {
                    error!("{}", err);
                    let _ = self.sender.send(String::from("An error occurred while consuming messages. See server logs for details. Closing stream."));
                    break;
                }
            };
            thread::sleep(Duration::from_millis(100));
        }
    }
}

const DEFAULT_KAFKA_ADDR: &str = "acsys-services.fnal.gov:9092";

/// A structure for subscribing to a message topic. Returns the values as a stream of messages for clients to handle.
#[derive(Debug)]
pub struct Subscriber {
    /// Keeps the channel open while the subscriber waits for clients to ask for a stream.
    _channel_lock: Receiver<String>,
    sender: Arc<Sender<String>>,
}
impl Subscriber {
    fn get_consumer(topic: String) -> Result<Consumer, PubSubError> {
        let addr = env::var("KAFKA_HOST_ADDR").unwrap_or_else(|_| String::from(DEFAULT_KAFKA_ADDR));
        Consumer::from_hosts(vec![addr])
            .with_topic(topic)
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(Some(GroupOffsetStorage::Kafka))
            .create()
            .map_err(|err| {
                error!("{}", err);
                PubSubError::new()
            })
    }
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
        let consumer = Self::get_consumer(topic)?;
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
        let addr = env::var("KAFKA_HOST_ADDR").unwrap_or_else(|_| String::from(DEFAULT_KAFKA_ADDR));
        let result = Producer::from_hosts(vec![addr])
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create();
        match result {
            Ok(producer) => Ok(Self { producer, topic }),
            Err(err) => {
                error!("{}", err);
                Err(PubSubError::new())
            }
        }
    }

    /// Sends the provided message to the configured topic.
    pub fn publish(&mut self, message: String) -> Result<(), PubSubError> {
        self.producer
            .send(&Record::from_value(self.topic.as_str(), message.as_bytes()))
            .map_err(|err| {
                error!("{}", err);
                PubSubError::new()
            })
    }
}
impl Debug for Publisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaProducer")
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
impl PubSubError {
    pub fn new() -> Self {
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
        let err = PubSubError::new();
        assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));
    }

    #[test]
    fn error_on_bad_kafka_consumer_host() {
        unsafe {
            env::set_var("KAFKA_HOST_ADDR", "bad_host");
        }
        let result = Subscriber::for_topic(String::from("my_topic"));
        let err = result.expect_err("Expected the connection to fail, but it succeeded");
        assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));
        unsafe {
            env::remove_var("KAFKA_HOST_ADDR");
        }
    }

    #[test]
    fn error_on_bad_kafka_producer_host() {
        unsafe {
            env::set_var("KAFKA_HOST_ADDR", "bad_host");
        }
        let result = Publisher::for_topic(String::from("my_topic"));
        let err = result.expect_err("Expected the connection to fail, but it succeeded");
        assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));
        unsafe {
            env::remove_var("KAFKA_HOST_ADDR");
        }
    }

    #[test]
    fn handles_err() {
        assert_eq!(MessageJob::handle::<PubSubError>(Ok(())), ());
        assert_eq!(MessageJob::handle(Err(PubSubError::new())), ());
    }
}
