use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use parking_lot::Mutex;
use subsquid_network_transport::PeerId;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::{compute_units_storage::ComputeUnitsStorage, Status};

const SINGLE_EXECUTION_COST: u64 = 1;

#[async_trait]
pub trait AllocationsChecker: Sync + Send {
    async fn run(&self, polling_interval: Duration, cancellation_token: CancellationToken);

    async fn try_spend(&self, gateway_id: PeerId) -> Result<Status>;
}

pub struct NoopAllocationsChecker {}

#[async_trait]
impl AllocationsChecker for NoopAllocationsChecker {
    async fn run(&self, _polling_interval: Duration, cancellation_token: CancellationToken) {
        cancellation_token.cancelled_owned().await;
    }

    async fn try_spend(&self, _gateway_id: PeerId) -> Result<Status> {
        Ok(Status::Spent)
    }
}

// TODO: persist data on disk
pub struct RpcAllocationsChecker {
    client: Box<dyn contract_client::Client>,
    own_id: contract_client::U256,
    storage: Mutex<ComputeUnitsStorage>,
}

impl RpcAllocationsChecker {
    pub async fn new(client: Box<dyn contract_client::Client>, peer_id: PeerId) -> Result<Self> {
        let own_id = client.worker_id(peer_id).await?;
        Ok(Self {
            client,
            own_id,
            storage: Default::default(),
        })
    }
}

#[async_trait]
impl AllocationsChecker for RpcAllocationsChecker {
    async fn run(&self, polling_interval: Duration, cancellation_token: CancellationToken) {
        let mut current_epoch = 0;

        let mut timer = tokio::time::interval(polling_interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select!(
                _ = timer.tick() => {},
                _ = cancellation_token.cancelled() => { break; },
            );

            debug!("Getting current epoch");
            let epoch = match self.client.current_epoch().await {
                Ok(epoch) => epoch,
                Err(e) => {
                    warn!("Couldn't get current epoch: {e:?}");
                    continue;
                }
            };
            if epoch > current_epoch {
                info!("New epoch started. Updating allocations");
                let clusters = match self.client.gateway_clusters(self.own_id).await {
                    Ok(clusters) => clusters,
                    Err(e) => {
                        warn!("Couldn't fetch gateway allocations: {e:?}");
                        continue;
                    }
                };
                self.storage.lock().update_allocations(clusters);
                current_epoch = epoch;
            }
        }
    }

    async fn try_spend(&self, gateway_id: PeerId) -> Result<Status> {
        Ok(self
            .storage
            .lock()
            .try_spend_cus(gateway_id, SINGLE_EXECUTION_COST))
    }
}
