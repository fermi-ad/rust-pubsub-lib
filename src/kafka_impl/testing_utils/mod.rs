//! Testing utilities for Kafka-backed code paths.
//!
//! This module wraps a shared [`MockCluster`](rdkafka::mocking::MockCluster) so tests can exercise
//! Kafka producers, consumers, snapshots, and subscribers without requiring an external broker.
//!
//! The shared cluster is global within the test process. Tests should use distinct topic names and
//! should not assume isolation from messages published by unrelated tests that reuse a topic.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::spawn;

use rdkafka::mocking::MockCluster;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::{OnceCell, RwLock, oneshot};

#[cfg(test)]
mod tests;

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

    /// Creates and returns a unique topic name using the provided prefix.
    ///
    /// This is the preferred path for tests that need topic isolation while reusing the shared mock
    /// cluster.
    pub async fn new_topic(prefix: &str) -> String {
        get_mock_cluster().await.new_topic(prefix).await
    }

    /// Returns a harness handle and one freshly created topic name.
    pub async fn with_new_topic(prefix: &str) -> (Self, String) {
        let topic = Self::new_topic(prefix).await;
        (Harness, topic)
    }

    /// Returns a harness handle and a freshly created topic name for each requested prefix.
    pub async fn with_new_topics<'a>(
        prefixes: impl IntoIterator<Item = &'a str>,
    ) -> (Self, Vec<String>) {
        let mut topics = Vec::new();
        for prefix in prefixes {
            topics.push(Self::new_topic(prefix).await);
        }
        (Harness, topics)
    }

    /// Returns the comma-delimited bootstrap server list for the shared mock cluster.
    pub async fn host(&self) -> String {
        get_mock_cluster().await.host()
    }
}

/// Global [`MockKafka`] instance shared by all test cases in the process.
///
/// [`OnceCell`] ensures the backing mock cluster is started lazily and only once.
static MOCK_CLUSTER: OnceCell<MockKafka> = OnceCell::const_new();

async fn get_mock_cluster() -> &'static MockKafka {
    MOCK_CLUSTER.get_or_init(MockKafka::new).await
}

/// Shared controller for the test-process [`MockCluster`](rdkafka::mocking::MockCluster).
///
/// This type owns the worker-thread coordination channel and a cache of topic names already created
/// in the cluster so repeated requests can be deduplicated cheaply.
struct MockKafka {
    host: String,
    known_topics: RwLock<HashSet<String>>,
    next_topic_id: AtomicU64,
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
            next_topic_id: AtomicU64::new(1),
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

    async fn new_topic(&self, prefix: &str) -> String {
        let topic = format!(
            "{prefix}-{}",
            self.next_topic_id.fetch_add(1, Ordering::Relaxed)
        );
        self.create_topic(topic.clone()).await;
        topic
    }
}

/// Drives the shared [`MockCluster`](rdkafka::mocking::MockCluster) worker thread.
///
/// The worker reports the bootstrap servers back to [`MockKafka`] and then listens for topic
/// creation requests until all senders have been dropped.
fn run_cluster(host_sender: oneshot::Sender<String>, mut topic_receiver: mpsc::Receiver<String>) {
    let mock_cluster = MockCluster::new(3).expect("Failed to start Kafka mock cluster");
    host_sender
        .send(mock_cluster.bootstrap_servers())
        .expect("Could not send the host to the MockKafka instance");

    // `blocking_recv` will return `None` when all senders are dropped (i.e., when tests are over).
    while let Some(topic) = topic_receiver.blocking_recv() {
        let _ = mock_cluster.create_topic(&topic, 3, 1);
    }
}
