use crate::{Message, PubSubError, Publisher, Snapshot, Subscriber};

use rdkafka::{
    ClientConfig, Message as RdMessage,
    consumer::{BaseConsumer, Consumer},
    producer::{BaseProducer, BaseRecord},
};
use rust_env_var_lib::env_var;
use std::{
    error::Error,
    fmt,
    sync::{
        Arc,
        mpsc::{self, SendError},
    },
    thread,
    time::Duration,
};
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio_stream::wrappers::BroadcastStream;
use tracing::error;
use uuid::Uuid;

/// Implementation of the [`Publisher`] trait for Kafka connections.
pub struct KafkaPublisher {
    host: String,
    producer: Option<BaseProducer>,
    topic: String,
}
impl KafkaPublisher {
    fn check_connection(&mut self) {
        if self.producer.is_none() {
            let host_clone = self.host.clone();
            self.producer = get_connection(move || {
                ClientConfig::new()
                    .set("bootstrap.servers", &host_clone)
                    .create()
                    .inspect_err(handle)
                    .ok()
            });
        }
    }
}
impl Publisher for KafkaPublisher {
    fn new(host: String, topic: String) -> Self {
        Self {
            host,
            producer: None,
            topic,
        }
    }

    fn publish(&mut self, message: Message) -> Result<(), PubSubError> {
        self.check_connection();
        if let Some(producer) = &mut self.producer
            && let Err((err, _)) = producer.send(
                BaseRecord::to(&self.topic)
                    .key(&message.key.unwrap_or_default())
                    .payload(&message.value),
            )
        {
            handle(&err);
            self.producer = None;
        }

        match self.producer {
            Some(_) => Ok(()),
            None => Err(PubSubError::default()),
        }
    }
}
impl fmt::Debug for KafkaPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Publisher")
            .field("host", &self.host)
            .field("topic", &self.topic)
            .finish()
    }
}

/// Implementation of the [`Snapshot`] trait for Kafka connections.
#[derive(Debug)]
pub struct KafkaSnapshot;
impl Snapshot for KafkaSnapshot {
    fn get(host: String, topic: String) -> Result<Vec<Message>, PubSubError> {
        if let Some(consumer) = &mut get_consumer(host, topic, None) {
            let mut data: Vec<Message> = Vec::new();

            let mut cur_size: usize = 0;
            loop {
                match do_poll(consumer, |msg: Message| {
                    data.push(msg);
                    Ok::<(), PubSubError>(())
                }) {
                    Ok(_) => {
                        if cur_size < data.len() {
                            cur_size = data.len();
                        } else {
                            break;
                        }
                    }
                    Err(err) => {
                        error!("{err}");
                        return Err(PubSubError::default());
                    }
                }
            }
            Ok(data)
        } else {
            Err(PubSubError::default())
        }
    }
}

/// Implementation of the [`Subscriber`] trait for Kafka connections.
#[derive(Debug)]
pub struct KafkaSubscriber {
    // Keeps the channel open while the subscriber waits for clients to ask for a stream.
    _channel_lock: Receiver<Message>,
    sender: Arc<Sender<Message>>,
}
impl Subscriber for KafkaSubscriber {
    fn new(host: String, topic: String) -> Self {
        let (sender, _channel_lock) = broadcast::channel::<Message>(20);
        let thread_sender = Arc::new(sender);
        let instance_sender = Arc::clone(&thread_sender);
        let mut message_job = MessageJob::from(host, topic, thread_sender);
        let _task_handle = thread::spawn(move || {
            message_job.run();
        });

        Self {
            _channel_lock,
            sender: instance_sender,
        }
    }

    fn get_stream(&self) -> BroadcastStream<Message> {
        BroadcastStream::new(self.sender.subscribe())
    }
}

struct MessageJob {
    consumer: Option<BaseConsumer>,
    host: String,
    sender: Arc<Sender<Message>>,
    topic: String,
    uuid: Uuid,
}
impl MessageJob {
    fn from(host: String, topic: String, sender: Arc<Sender<Message>>) -> Self {
        let uuid = Uuid::new_v4();
        let consumer = get_consumer(host.clone(), topic.clone(), Some(uuid.to_string()));
        Self {
            consumer,
            host,
            sender,
            topic,
            uuid,
        }
    }

    fn check_connection(&mut self) {
        if self.consumer.is_none() {
            self.consumer = get_consumer(
                self.host.clone(),
                self.topic.clone(),
                Some(self.uuid.to_string()),
            );
        }
    }

    fn run(&mut self) {
        loop {
            self.check_connection();
            if let Some(consumer) = &mut self.consumer {
                // sender.send() returns Result<usize, SendError<T>>, where the Ok path is the number of receivers
                // that got the message. We don't really care about that, but still want to capture any errors, so
                // we use .map(drop) to just drop the Ok path.
                let result = do_poll(consumer, |msg| self.sender.send(msg).map(drop));
                if let Err(err) = result {
                    if err.downcast_ref::<SendError<Message>>().is_some() {
                        // The send stream is closed, so all receivers must have been dropped and there is no
                        // more need for this thread to run.
                        break;
                    } else {
                        // Something else went wrong with the consumer. Drop the current instance so we can
                        // attempt to reconnect on the next pass.
                        error!("{err}");
                        self.consumer = None;
                    }
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}

fn do_poll<E: Error + 'static>(
    consumer: &BaseConsumer,
    mut append_msg: impl FnMut(Message) -> Result<(), E>,
) -> Result<(), Box<dyn Error>> {
    if let Some(message_result) = consumer.poll(Duration::from_millis(100)) {
        let message = message_result?;
        let key = message
            .key()
            .map(|v| String::from_utf8_lossy(v).into_owned());
        let value =
            String::from_utf8_lossy(message.payload().expect("No body in message")).into_owned();
        append_msg(Message { key, value })?;
    }
    Ok(())
}

const KAFKA_CONN_SECS: &str = "KAFKA_CONNECTION_SECONDS";
const DEFAULT_CONN_TIME: u64 = 1;
fn get_connection<T: Send + 'static>(
    connect: impl Fn() -> Option<T> + Send + 'static,
) -> Option<T> {
    let (sender, receiver) = mpsc::channel();
    let _ = thread::spawn(move || {
        let connection = connect();
        sender.send(connection).inspect_err(handle)
    });
    let connection_seconds = env_var::get(KAFKA_CONN_SECS).or(DEFAULT_CONN_TIME);
    match receiver.recv_timeout(Duration::from_secs(connection_seconds)) {
        Ok(result) => result,
        Err(err) => {
            error!("{}", err);
            None
        }
    }
}

fn get_consumer(host: String, topic: String, group: Option<String>) -> Option<BaseConsumer> {
    let group_name = group.unwrap_or_default();
    get_connection(move || {
        let mut consumer: Option<BaseConsumer> = ClientConfig::new()
            .set("bootstrap.servers", host.clone())
            .set("group.id", &group_name)
            .set("enable.auto.commit", "true")
            .create()
            .inspect_err(handle)
            .ok();
        if let Some(cons) = consumer {
            consumer = cons
                .subscribe(&[&topic.clone()])
                .inspect_err(handle)
                .ok()
                .map(|_| cons);
        }
        consumer
    })
}

fn handle<E: Error + 'static>(err: &E) {
    error!("{}", err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_kafka_consumer() {
        let mut result = KafkaSubscriber::new(String::from("myHost"), String::from("my_topic"));
        let num_rec = result
            .sender
            .send(Message::new(None, "testing".to_string()))
            .unwrap();
        assert_eq!(num_rec, 1);
        assert_eq!(result._channel_lock.try_recv().unwrap().value, "testing");
    }

    #[test]
    fn new_kafka_producer() {
        let result = KafkaPublisher::new(String::from("myHost"), String::from("my_topic"));
        assert_eq!(result.host, "myHost");
        assert!(result.producer.is_none());
        assert_eq!(result.topic, "my_topic");
    }

    #[test]
    fn error_on_bad_snapshot_host() {
        let result = KafkaSnapshot::get(String::from("myHost"), String::from("my_topic"));
        let err = result.expect_err("Expected the connection to fail, but it succeeded");
        assert_eq!(super::super::CANNED_ERR_MESSAGE, format!("{}", err));
    }
}
