use crate::{ByteMessage, Message};
use rdkafka::{
    ClientConfig,
    consumer::{Consumer, ConsumerContext, MessageStream, StreamConsumer},
    error::KafkaError,
    message::{BorrowedMessage, Message as RdMessage},
};
use std::time::Duration;
use tokio::{
    select, spawn,
    sync::{
        broadcast::{Sender, channel},
        watch,
    },
    time::sleep,
};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use uuid::Uuid;

const MAX_WAIT_TIME: Duration = Duration::from_secs(300);

#[derive(Debug)]
pub(crate) struct KafkaStream {
    cancel_sender: watch::Sender<bool>,
    sender: Sender<ByteMessage>,
}

impl KafkaStream {
    pub(crate) fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }

    pub(crate) fn new(host: String, topic: String) -> Self {
        let (sender, _) = channel(100);
        let remote_sender = sender.clone();

        let (cancel_sender, cancel_receiver) = watch::channel(false);

        spawn(start_stream(host, topic, remote_sender, cancel_receiver));

        Self {
            cancel_sender,
            sender,
        }
    }

    pub(crate) fn get_stream(&self) -> BroadcastStream<ByteMessage> {
        BroadcastStream::new(self.sender.subscribe())
    }
}

impl Drop for KafkaStream {
    fn drop(&mut self) {
        let _ = self.cancel_sender.send(true);
    }
}

async fn start_stream(
    host: String,
    topic: String,
    sender: Sender<ByteMessage>,
    mut cancel_receiver: watch::Receiver<bool>,
) {
    let stream_id = Uuid::new_v4().as_hyphenated().to_string();
    let mut wait_time = Duration::from_secs(1);
    while !*cancel_receiver.borrow() {
        let mut builder = ClientConfig::new();
        builder.set("bootstrap.servers", &host);
        builder.set("group.id", &stream_id);

        #[cfg(any(feature = "testing-utils", test))]
        builder.set("auto.offset.reset", "earliest");

        match builder.create::<StreamConsumer>() {
            Ok(consumer) => {
                if let Err(e) = consumer.subscribe(&[&topic]) {
                    wait_time = handle_connection_err(e, wait_time, &mut cancel_receiver).await;
                } else {
                    let message_stream = consumer.stream();
                    monitor_stream(message_stream, &sender, cancel_receiver.clone()).await;
                    wait_time = Duration::from_secs(1);
                }
            }
            Err(e) => {
                wait_time = handle_connection_err(e, wait_time, &mut cancel_receiver).await;
            }
        }
    }
}

async fn monitor_stream<C: ConsumerContext>(
    mut message_stream: MessageStream<'_, C>,
    sender: &Sender<ByteMessage>,
    mut cancel_receiver: watch::Receiver<bool>,
) {
    loop {
        select! {
            _ = cancel_receiver.changed() => {
                if *cancel_receiver.borrow() {
                    break;
                }
            }
            incoming = message_stream.next() => {
                match incoming {
                    Some(Ok(msg)) => {
                        if let Some(message) = convert_to_message(&msg) {
                            let _ = sender.send(message);
                        }
                    }
                    Some(Err(_)) => break,
                    None => break,
                }
            }
        }
    }
}

fn convert_to_message(incoming: &BorrowedMessage) -> Option<ByteMessage> {
    incoming.payload().map(|value_bytes| {
        <ByteMessage as Message<Vec<u8>>>::from_bytes(incoming.key(), value_bytes)
    })
}

async fn handle_connection_err(
    _err: KafkaError,
    mut wait_time: Duration,
    cancel_receiver: &mut watch::Receiver<bool>,
) -> Duration {
    if *cancel_receiver.borrow() {
        return wait_time;
    }

    select! {
        _ = cancel_receiver.changed() => {},
        _ = sleep(wait_time) => {},
    }

    wait_time *= 2;
    if wait_time > MAX_WAIT_TIME {
        MAX_WAIT_TIME
    } else {
        wait_time
    }
}
