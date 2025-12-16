use crate::{Message, PubSubError, Publisher, Snapshot, Subscriber};

use kafka::{
    client::{FetchOffset, GroupOffsetStorage, RequiredAcks},
    consumer::Consumer,
    producer::{Producer, Record},
};
use rust_env_var_lib::env_var;
use std::{
    error::Error,
    fmt,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio_stream::wrappers::BroadcastStream;
use tracing::error;

/// Implementation of the Publisher trait for Kafka connections.
pub struct KafkaPublisher {
    producer: Producer,
    topic: String,
}
impl Publisher for KafkaPublisher {
    fn new(host: String, topic: String) -> Result<Self, PubSubError> {
        get_connection(move || {
            Producer::from_hosts(vec![host.clone()])
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

    fn publish(&mut self, message: Message) -> Result<(), PubSubError> {
        self.producer
            .send(&into_record(message, &self.topic))
            .map_err(|err| {
                error!("{}", err);
                PubSubError::default()
            })
    }
}
impl fmt::Debug for KafkaPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Publisher")
            .field("host", &self.producer.client().hosts())
            .field("topic", &self.topic)
            .finish()
    }
}

/// Implementation of the Snapshot trait for Kafka connections.
#[derive(Debug)]
pub struct KafkaSnapshot;
impl Snapshot for KafkaSnapshot {
    fn get(host: String, topic: String) -> Result<Vec<Message>, PubSubError> {
        let mut consumer = get_consumer(host, topic)?;
        let mut data: Vec<Message> = Vec::new();

        let mut cur_size: usize = 0;
        loop {
            match do_poll(&mut consumer, |msg: Message| {
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
        Ok(data)
    }
}

/// Implementation of the Subscriber trait for Kafka connections.
#[derive(Debug)]
pub struct KafkaSubscriber {
    /// Keeps the channel open while the subscriber waits for clients to ask for a stream.
    _channel_lock: Receiver<Message>,
    sender: Arc<Sender<Message>>,
}
impl KafkaSubscriber {
    fn from(consumer: Consumer) -> Self {
        let (sender, _channel_lock) = broadcast::channel::<Message>(20);
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
}
impl Subscriber for KafkaSubscriber {
    fn new(host: String, topic: String) -> Result<Self, PubSubError> {
        let consumer = get_consumer(host, topic)?;
        Ok(Self::from(consumer))
    }

    fn get_stream(&self) -> BroadcastStream<Message> {
        BroadcastStream::new(self.sender.subscribe())
    }
}

struct MessageJob {
    consumer: Consumer,
    sender: Arc<Sender<Message>>,
}
impl MessageJob {
    pub fn run(&mut self) {
        while do_poll(&mut self.consumer, |msg: Message| self.sender.send(msg)).is_ok() {
            thread::sleep(Duration::from_millis(100));
        }
    }
}

fn do_poll<R, E: Error>(
    consumer: &mut Consumer,
    mut append_msg: impl FnMut(Message) -> Result<R, E>,
) -> Result<(), PubSubError> {
    match consumer.poll() {
        Ok(message_sets) => {
            for set in message_sets.iter() {
                for msg in set.messages() {
                    let key = match String::from_utf8(msg.key.to_vec()) {
                        Ok(decoded) => Some(decoded),
                        Err(err) => {
                            error!("{}", err);
                            None
                        }
                    };
                    match String::from_utf8(msg.value.to_vec()) {
                        Ok(value) => match append_msg(Message { key, value }) {
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
            return Err(PubSubError::default());
        }
    };
    Ok(())
}

const KAFKA_CONN_SECS: &str = "KAFKA_CONNECTION_SECONDS";
const DEFAULT_CONN_TIME: u64 = 1;
fn get_connection<T: Send + 'static>(
    connect: impl Fn() -> Result<T, PubSubError> + Send + 'static,
) -> Result<T, PubSubError> {
    let (sender, receiver) = mpsc::channel();
    let _ = thread::spawn(move || {
        let connection = connect();
        handle(sender.send(connection));
    });
    let connection_seconds = env_var::get(KAFKA_CONN_SECS).or(DEFAULT_CONN_TIME);
    match receiver.recv_timeout(Duration::from_secs(connection_seconds)) {
        Ok(result) => result,
        Err(err) => {
            error!("{}", err);
            Err(PubSubError::default())
        }
    }
}

fn get_consumer(host: String, topic: String) -> Result<Consumer, PubSubError> {
    get_connection(move || {
        Consumer::from_hosts(vec![host.clone()])
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

fn handle<E: Error>(result: Result<(), E>) {
    match result {
        Ok(_) => (),
        Err(err) => error!("{}", err),
    }
}

fn into_record(msg: Message, topic: &str) -> Record<'_, Vec<u8>, Vec<u8>> {
    match msg.key {
        Some(k) => Record::from_key_value(topic, k.into_bytes(), msg.value.into_bytes()),
        None => Record::from_key_value(topic, Vec::new(), msg.value.into_bytes()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_on_bad_kafka_consumer_host() {
        let result = KafkaSubscriber::new(String::from("myHost"), String::from("my_topic"));
        let err = result.expect_err("Expected the connection to fail, but it succeeded");
        assert_eq!(super::super::CANNED_ERR_MESSAGE, format!("{}", err));
    }

    #[test]
    fn error_on_bad_kafka_producer_host() {
        let result = KafkaPublisher::new(String::from("myHost"), String::from("my_topic"));
        let err = result.expect_err("Expected the connection to fail, but it succeeded");
        assert_eq!(super::super::CANNED_ERR_MESSAGE, format!("{}", err));
    }

    #[test]
    fn error_on_bad_snapshot_host() {
        let result = KafkaSnapshot::get(String::from("myHost"), String::from("my_topic"));
        let err = result.expect_err("Expected the connection to fail, but it succeeded");
        assert_eq!(super::super::CANNED_ERR_MESSAGE, format!("{}", err));
    }

    #[test]
    fn handles_err() {
        assert_eq!(handle::<PubSubError>(Ok(())), ());
        assert_eq!(handle(Err(PubSubError::default())), ());
    }

    #[test]
    fn message_into_record() {
        let key = Some(String::from("some key"));
        let val = String::from("some text");
        let message = Message::new(key.clone(), val.clone());
        let topic = "my topic";
        let output = into_record(message, topic);
        assert_eq!(output.topic, topic);
        assert_eq!(output.key, key.unwrap().into_bytes());
        assert_eq!(output.value, val.into_bytes());
    }
}
