use crate::{KafkaSubscriber, KafkaTestHarness, StringMessage, Subscriber};

use super::*;

#[tokio::test]
async fn kafka_subscriber_shares_cached_stream_per_host_topic() {
    let topic = String::from("shared_topic");
    let test_harness = KafkaTestHarness::with_topics(vec![topic.clone()]).await;
    let host = test_harness.host().await;

    let mut first = KafkaSubscriber::new(host.clone(), topic.clone());
    let mut second = KafkaSubscriber::new(host.clone(), topic.clone());

    let _stream_a = first.get_stream::<String, StringMessage>().await.unwrap();
    let _stream_b = second.get_stream::<String, StringMessage>().await.unwrap();

    let lock = CONSUMER_MAP.read().await;
    let entry = lock
        .get(&(host, topic))
        .expect("missing shared stream cache entry");
    assert_eq!(2, entry.data.receiver_count());
}
