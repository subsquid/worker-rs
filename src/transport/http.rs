use std::time::Duration;

use anyhow::Result;
use subsquid_messages::WorkerAssignment;

use super::{State, Transport};

const PING_TIMEOUT: Duration = Duration::from_millis(200);

pub struct HttpTransport {
    worker_id: String,
    worker_url: String,
    router_url: String,
}

impl HttpTransport {
    pub fn new(worker_id: String, worker_url: String, router_url: String) -> Self {
        Self {
            worker_id,
            worker_url,
            router_url,
        }
    }
}

impl Transport for HttpTransport {
    async fn send_ping(&self, state: State) -> Result<()> {
        reqwest::Client::new()
            .post([&self.router_url, "/ping"].join(""))
            .json(&serde_json::json!({
                "worker_id": self.worker_id,
                "worker_url": self.worker_url,
                "state": state,
                "pause": false,
            }))
            .timeout(PING_TIMEOUT)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(())
    }

    #[allow(unreachable_code)]
    fn stream_assignments(
        &self,
    ) -> impl futures::Stream<Item = Option<WorkerAssignment>> + 'static {
        unimplemented!();
        futures::stream::empty()
    }

    fn stream_queries(&self) -> impl futures::Stream<Item = super::QueryTask> + 'static {
        // In case of HTTP transport, the queries are handled by the HTTP server directly
        futures::stream::pending()
    }
}
