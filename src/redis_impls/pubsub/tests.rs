//! Redis PubSub Implementations Module Tests
//!
//! Tests for the Redis PubSub implementations of the public traits in this library.

use super::*;
use crate::{StringMessage, redis_impls::testing_utils::TestContext};

#[tokio::test]
async fn test_publish() {
    let context = TestContext::new().await;
    let publisher = RedisPublisher::new(context.get_host(), "test-topic".to_string());
    let message = StringMessage::from_value("Hello, Redis PubSub!".to_string());
    publisher.publish(message).await.unwrap();
    assert!(
        context
            .check_for_message("Hello, Redis PubSub!")
            .await
            .is_ok()
    );
}
