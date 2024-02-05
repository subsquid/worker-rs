use anyhow::Result;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::types::state::Ranges;

use super::{State, Transport};

pub struct HttpTransport {
    worker_id: String,
    worker_url: String,
    router_url: String,
    updates_rx: Option<mpsc::Receiver<Ranges>>,
    updates_tx: mpsc::Sender<Ranges>,
}

impl HttpTransport {
    pub fn new(worker_id: String, worker_url: String, router_url: String) -> Self {
        let (tx, rx) = mpsc::channel(1);
        Self {
            worker_id,
            worker_url,
            router_url,
            updates_rx: Some(rx),
            updates_tx: tx,
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
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        self.updates_tx.send(resp.datasets).await?;
        Ok(())
    }

    fn subscribe_to_updates(&mut self) -> impl futures::Stream<Item = Ranges> + 'static {
        let rx = self
            .updates_rx
            .take()
            .expect("Attempted to subscribe to transport updates twice");
        ReceiverStream::new(rx)
    }
}
