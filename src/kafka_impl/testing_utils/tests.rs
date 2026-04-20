use super::*;

#[tokio::test]
async fn harness_host_matches_shared_cluster_host() {
    let harness = Harness::with_topics(vec![]).await;
    assert_eq!(harness.host().await, get_mock_cluster().await.host());
}

#[tokio::test]
async fn harness_with_topics_populates_known_topic_set_once() {
    let topic = "kafka-harness-known-topic".to_string();
    let _ = Harness::with_topics(vec![topic.clone(), topic.clone()]).await;

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
