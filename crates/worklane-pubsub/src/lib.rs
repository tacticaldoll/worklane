//! Pub/Sub Topic Routing for worklane
//!
//! This crate provides a lightweight `Publisher` abstraction to map string topics
//! to multiple worker `Lane`s, allowing atomic fan-out of payloads without
//! changing the core `worklane` semantics.
//!
//! Depend on this crate only when topic routing is useful for an application.
//! The core job loop has no exchange or transport-routing model.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::HashMap;
use tracing::{debug, error, info};
use worklane::{Client, Error, Job, JobId, Lane};

/// A publisher that maps semantic topics to underlying worker lanes.
pub struct Publisher {
    client: Client,
    routes: HashMap<String, Vec<Lane>>,
}

impl Publisher {
    /// Create a new Publisher wrapping the given Client.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            routes: HashMap::new(),
        }
    }

    /// Register a route mapping a topic to one or more lanes.
    /// Overwrites any existing routes for this topic.
    pub fn route(mut self, topic: &str, lanes: impl IntoIterator<Item = Lane>) -> Self {
        let lanes: Vec<Lane> = lanes.into_iter().collect();
        info!(
            topic = topic,
            num_lanes = lanes.len(),
            "Registered topic route"
        );
        self.routes.insert(topic.to_string(), lanes);
        self
    }

    /// Create a `PublishBuilder` to configure a job before publishing it to a topic.
    pub fn build_publish<'a, J: Job>(
        &'a self,
        topic: &str,
        payload: J::Payload,
    ) -> Result<PublishBuilder<'a>, Error> {
        let lanes = match self.routes.get(topic) {
            Some(lanes) => lanes.clone(),
            None => {
                let msg = format!("unknown topic: {topic}");
                error!(topic = topic, "Publish failed: unknown topic");
                return Err(Error::Broker(msg));
            }
        };

        Ok(PublishBuilder {
            builder: self.client.build_job::<J>(payload)?,
            lanes,
        })
    }
}

/// A builder for configuring a job before publishing it to a topic.
#[must_use = "this value must be used"]
pub struct PublishBuilder<'a> {
    builder: worklane::JobBuilder<'a>,
    lanes: Vec<Lane>,
}

impl<'a> PublishBuilder<'a> {
    /// Set a delay before the job becomes visible to workers.
    #[must_use = "this value must be used"]
    pub fn with_delay(mut self, delay: std::time::Duration) -> Self {
        self.builder = self.builder.with_delay(delay);
        self
    }

    /// Set a unique key for deduplication.
    #[must_use = "this value must be used"]
    pub fn with_unique_key(mut self, key: impl Into<String>) -> Self {
        self.builder = self.builder.with_unique_key(key);
        self
    }

    /// Set the priority for this job. Higher values mean higher priority.
    #[must_use = "this value must be used"]
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.builder = self.builder.with_priority(priority);
        self
    }

    /// Publish the job to all lanes registered for the topic.
    pub async fn publish(self) -> Result<Vec<JobId>, Error> {
        if self.lanes.is_empty() {
            debug!("Topic has no registered lanes, skipping publish");
            return Ok(vec![]);
        }
        self.builder.enqueue_to_lanes(self.lanes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use worklane::JobContext;
    use worklane_memory::InMemoryBroker;

    #[derive(Serialize, Deserialize)]
    struct MyEvent {
        user_id: u32,
    }

    #[async_trait]
    impl Job for MyEvent {
        const KIND: &'static str = "my_event";
        type Payload = MyEvent;
        type Output = ();

        async fn run(
            &self,
            _ctx: JobContext,
            _payload: Self::Payload,
        ) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync + 'static>> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_publisher_routing_and_fanout() {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone());

        let publisher = Publisher::new(client)
            .route(
                "user.created",
                vec![
                    Lane::try_from("email").unwrap(),
                    Lane::try_from("crm").unwrap(),
                ],
            )
            .route(
                "user.deleted",
                vec![
                    Lane::try_from("crm").unwrap(),
                    Lane::try_from("log").unwrap(),
                    Lane::try_from("audit").unwrap(),
                ],
            );

        // Overwriting a route should work
        let publisher = publisher.route(
            "user.created",
            vec![
                Lane::try_from("email").unwrap(),
                Lane::try_from("crm").unwrap(),
                Lane::try_from("analytics").unwrap(),
            ],
        );

        // 1. Publish to an unknown topic
        let err = match publisher.build_publish::<MyEvent>("unknown", MyEvent { user_id: 1 }) {
            Err(e) => e,
            Ok(_) => panic!("expected an error"),
        };
        assert!(matches!(err, Error::Broker(msg) if msg.contains("unknown topic")));

        // 2. Publish to "user.created"
        let job_ids = publisher
            .build_publish::<MyEvent>("user.created", MyEvent { user_id: 2 })
            .unwrap()
            .publish()
            .await
            .unwrap();
        assert_eq!(job_ids.len(), 3);

        // Verify broker state
        assert_eq!(broker.len(), 3);
    }
}
