use std::time::Duration;

use anyhow::Result;
use subsquid_messages::WorkerAssignment;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;

use crate::{types::state::Ranges, util::UseOnce};

use super::{State, Transport};

const PING_TIMEOUT: Duration = Duration::from_millis(200);

pub struct HttpTransport {
    worker_id: String,
    worker_url: String,
    router_url: String,
    assignments_rx: UseOnce<watch::Receiver<Ranges>>,
    assignments_tx: watch::Sender<Ranges>,
}

impl HttpTransport {
    pub fn new(worker_id: String, worker_url: String, router_url: String) -> Self {
        let (assignments_tx, assignments_rx) = watch::channel(Default::default());
        Self {
            worker_id,
            worker_url,
            router_url,
            assignments_rx: UseOnce::new(assignments_rx),
            assignments_tx,
        }
    }
}

impl Transport for HttpTransport {
    async fn send_ping(&self, state: State) -> Result<()> {
        let resp: State = reqwest::Client::new()
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
        self.assignments_tx.send(resp.datasets)?;
        Ok(())
    }

    fn stream_assignments(&self) -> impl futures::Stream<Item = WorkerAssignment> + 'static {
        futures::stream::pending() // FIXME
    }

    fn stream_queries(&self) -> impl futures::Stream<Item = super::QueryTask> + 'static {
        // In case of HTTP transport, the queries are handled by the HTTP server directly
        futures::stream::pending()
    }
}
