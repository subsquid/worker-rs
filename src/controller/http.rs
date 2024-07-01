use std::{sync::Arc, time::Duration};

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::storage::manager::Status;

use super::worker::Worker;

const PING_TIMEOUT: Duration = Duration::from_millis(200);

pub struct HttpController {
    worker: Arc<Worker>,
    ping_interval: Duration,
    worker_id: String,
    worker_url: String,
    router_url: String,
}

impl HttpController {
    pub fn new(
        worker: Arc<Worker>,
        ping_interval: Duration,
        worker_id: String,
        worker_url: String,
        router_url: String,
    ) -> Self {
        Self {
            worker,
            ping_interval,
            worker_id,
            worker_url,
            router_url,
        }
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        let mut timer = tokio::time::interval_at(
            tokio::time::Instant::now() + self.ping_interval,
            self.ping_interval,
        );
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select!(
                _ = timer.tick() => {},
                _ = cancellation_token.cancelled() => {
                    break;
                },
            );
            tracing::debug!("Sending ping");
            let status = self.worker.status();
            let result = self.send_ping(status).await;
            if let Err(err) = result {
                tracing::warn!("Couldn't send ping: {:?}", err);
            }
            // TODO: receive assignment
        }
    }

    async fn send_ping(&self, status: Status) -> anyhow::Result<()> {
        reqwest::Client::new()
            .post([&self.router_url, "/ping"].join(""))
            .json(&serde_json::json!({
                "worker_id": self.worker_id,
                "worker_url": self.worker_url,
                "state": {
                    "datasets": status.available,
                    "stored_bytes": status.stored_bytes,
                },
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
}
