use std::time::Duration;

use anyhow::Result;
use parking_lot::Mutex;
use subsquid_network_transport::PeerId;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::{compute_units_storage::ComputeUnitsStorage, Status};

const SINGLE_EXECUTION_COST: i64 = 1;

pub struct AllocationsChecker {
    client: Box<dyn contract_client::Client>,
    own_id: contract_client::U256,
    storage: Mutex<ComputeUnitsStorage>,
    polling_interval: Duration,
}

impl AllocationsChecker {
    pub async fn new(
        client: Box<dyn contract_client::Client>,
        peer_id: PeerId,
        polling_interval: Duration,
    ) -> Result<Self> {
        let own_id = client.worker_id(peer_id).await?;
        Ok(Self {
            client,
            own_id,
            storage: Default::default(),
            polling_interval,
        })
    }

    pub fn try_spend(&self, gateway_id: PeerId) -> Status {
        self.storage
            .lock()
            .try_spend_cus(gateway_id, SINGLE_EXECUTION_COST)
    }

    pub fn refund(&self, gateway_id: PeerId) {
        self.storage
            .lock()
            .try_spend_cus(gateway_id, -SINGLE_EXECUTION_COST);
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        let mut current_epoch = 0;

        let mut timer = tokio::time::interval(self.polling_interval);
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
}
