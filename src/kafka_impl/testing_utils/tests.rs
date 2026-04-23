//! Kafka testing utility tests.
//!
//! These tests verify shared mock-cluster behavior, topic creation helpers, and topic deduplication.

use super::*;

#[tokio::test]
async fn harness_host_matches_shared_cluster_host() {
    let harness = Harness::with_topics(vec![]).await;
    assert_eq!(harness.host().await, get_mock_cluster().await.host());
}

#[tokio::test]
async fn harness_new_topic_creates_distinct_topics() {
    let first = Harness::new_topic("kafka-harness-topic").await;
    let second = Harness::new_topic("kafka-harness-topic").await;

    assert_ne!(first, second);
    assert!(
        get_mock_cluster()
            .await
            .known_topics
            .read()
            .await
            .contains(&first)
    );
    assert!(
        get_mock_cluster()
            .await
            .known_topics
            .read()
            .await
            .contains(&second)
    );
}

#[tokio::test]
async fn harness_with_new_topic_returns_handle_and_created_topic() {
    let (harness, topic) = Harness::with_new_topic("kafka-harness-single").await;

    assert_eq!(harness.host().await, get_mock_cluster().await.host());
    assert!(
        get_mock_cluster()
            .await
            .known_topics
            .read()
            .await
            .contains(&topic)
    );
}

#[tokio::test]
async fn harness_with_new_topics_creates_each_requested_topic() {
    let (_harness, topics) = Harness::with_new_topics(["kafka-batch-a", "kafka-batch-b"]).await;
    let [first_topic, second_topic]: [String; 2] = topics.try_into().unwrap();

    assert_ne!(first_topic, second_topic);
    assert!(
        get_mock_cluster()
            .await
            .known_topics
            .read()
            .await
            .contains(&first_topic)
    );
    assert!(
        get_mock_cluster()
            .await
            .known_topics
            .read()
            .await
            .contains(&second_topic)
    );
}

#[tokio::test]
async fn mock_kafka_create_topic_deduplicates_requests() {
    let topic = "kafka-harness-dedup-topic".to_string();
    let _ = Harness::with_topics(vec![]).await;
    get_mock_cluster().await.create_topic(topic.clone()).await;
    get_mock_cluster().await.create_topic(topic.clone()).await;

    let count = get_mock_cluster()
        .await
        .known_topics
        .read()
        .await
        .iter()
        .filter(|known| known.as_str() == topic)
        .count();

    assert_eq!(1, count);
}
