//! Testing utilities for Kafka-backed code paths.
//!
//! This module wraps a shared [`MockCluster`](rdkafka::mocking::MockCluster) so tests can exercise
//! Kafka producers, consumers, snapshots, and subscribers without requiring an external broker.
//!
//! The shared cluster is global within the test process. Tests should use distinct topic names and
//! should not assume isolation from messages published by unrelated tests that reuse a topic.

use rdkafka::mocking::MockCluster;
use std::{collections::HashSet, thread::spawn};
use tokio::sync::{
    OnceCell, RwLock,
    mpsc::{self, Sender},
    oneshot,
};

/// A lightweight handle for using the shared test [`MockCluster`](rdkafka::mocking::MockCluster).
///
/// ## Isolation caveat
///
/// To conserve resources, this structure references a global instance of
/// [`MockCluster`](rdkafka::mocking::MockCluster). If topic names are reused between test cases,
/// messages from one test case may become visible to another.
///
/// Build tests on independent topics or design them to be agnostic about preexisting data.
pub struct Harness;
impl Harness {
    /// Ensures the shared mock cluster is running and pre-creates the requested topics.
    ///
    /// Topic creation requests are deduplicated internally, but callers should still prefer unique
    /// topic names when test isolation matters.
    pub async fn with_topics(topics: Vec<String>) -> Self {
        for topic in topics {
            get_mock_cluster().await.create_topic(topic).await;
        }
        Harness
    }

    /// Returns the comma-delimited bootstrap server list for the shared mock cluster.
    pub async fn host(&self) -> String {
        get_mock_cluster().await.host()
    }
}

/// A struct to manage the [`MockCluster`](rdkafka::mocking::MockCluster) thread across the test runs.
/// Maintains a list of the known topics to short-circuit the process of sending a new topic String across the threads.
struct MockKafka {
    host: String,
    known_topics: RwLock<HashSet<String>>,
    topic_sender: Sender<String>,
}
impl MockKafka {
    async fn new() -> Self {
        let (topic_sender, topic_receiver) = mpsc::channel::<String>(1);
        let (host_sender, host_receiver) = oneshot::channel();

        spawn(move || {
            run_cluster(host_sender, topic_receiver);
        });

        let host = host_receiver
            .await
            .expect("Failed to acquire host path for the mock cluster");

        MockKafka {
            host,
            known_topics: RwLock::default(),
            topic_sender,
        }
    }

    fn host(&self) -> String {
        self.host.clone()
    }

    async fn create_topic(&self, topic: String) {
        let has_topic = self.known_topics.read().await.contains(&topic);
        if !has_topic {
            let mut write_lock = self.known_topics.write().await;
            let is_missing_topic_after_acquiring_write_lock = !write_lock.contains(&topic);
            if is_missing_topic_after_acquiring_write_lock {
                self.topic_sender
                    .send(topic.clone())
                    .await
                    .expect("Failed to add topic to cluster");
                write_lock.insert(topic);
            }
        }
    }
}

/// Global instance of [`MockKafka`] to be used by all test cases.
/// Utilizes [`OnceCell`] to only instantiate it once it has been referenced for the first time.
/// All subsequent references will see the same instance.
static MOCK_CLUSTER: OnceCell<MockKafka> = OnceCell::const_new();

async fn get_mock_cluster() -> &'static MockKafka {
    MOCK_CLUSTER.get_or_init(|| MockKafka::new()).await
}

/// Drives the shared [`MockCluster`](rdkafka::mocking::MockCluster) worker thread.
///
/// The worker reports the bootstrap servers back to [`MockKafka`] and then listens for topic
/// creation requests until all senders have been dropped.
fn run_cluster(host_sender: oneshot::Sender<String>, mut topic_receiver: mpsc::Receiver<String>) {
    let mock_cluster = MockCluster::new(3).unwrap();
    host_sender
        .send(mock_cluster.bootstrap_servers())
        .expect("Could not send the host to the MockKafka instance");

    // `blocking_recv` will return `None` when all senders are dropped (i.e., when tests are over).
    while let Some(topic) = topic_receiver.blocking_recv() {
        let _ = mock_cluster.create_topic(&topic, 3, 1);
    }
}

#[cfg(test)]
mod tests {
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
}
