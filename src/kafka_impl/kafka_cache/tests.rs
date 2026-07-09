//! Kafka cache tests.
//!
//! These tests verify shared-stream caching and lazy runtime startup for Kafka subscribers.

use super::*;
use crate::KafkaTestHarness;

#[tokio::test]
async fn kafka_producer_hot_path_does_not_insert_duplicate_entry() {
    let topic = String::from("producer_hot_path_topic");
    let test_harness = KafkaTestHarness::with_topics(vec![topic]).await;
    let host = test_harness.host().await;

    // First call: cache miss — inserts the entry.
    let _ = get_kafka_producer(&host).await.unwrap();
    assert!(
        PRODUCER_MAP.read().await.contains_key(&host),
        "entry must exist after first call"
    );

    // Second call: cache hit — must reuse the existing entry. We verify this by checking that
    // the Arc pointer to `last_used_epoch_secs` is the same object, not a freshly allocated one.
    let arc_before = PRODUCER_MAP
        .read()
        .await
        .get(&host)
        .map(|e| Arc::as_ptr(&e.last_used_epoch_secs))
        .expect("entry must exist before second call");

    let _ = get_kafka_producer(&host).await.unwrap();

    let arc_after = PRODUCER_MAP
        .read()
        .await
        .get(&host)
        .map(|e| Arc::as_ptr(&e.last_used_epoch_secs))
        .expect("entry must still exist after second call");

    assert_eq!(
        arc_before, arc_after,
        "second get_kafka_producer call must reuse the cached entry, not replace it"
    );
}
