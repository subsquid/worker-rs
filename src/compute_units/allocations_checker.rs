use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::Result;
use parking_lot::Mutex;
use sqd_network_transport::PeerId;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::{rate_limiter::RateLimiter, RateLimitStatus};

pub struct AllocationsChecker {
    client: Box<dyn sqd_contract_client::Client>,
    own_id: sqd_contract_client::U256,
    rate_limiter: Mutex<RateLimiter>,
    polling_interval: Duration,
    current_epoch: AtomicU32,
}

impl AllocationsChecker {
    pub async fn new(
        client: Box<dyn sqd_contract_client::Client>,
        peer_id: PeerId,
        polling_interval: Duration,
    ) -> Result<Self> {
        let own_id = client.worker_id(peer_id).await?;
        Ok(Self {
            client,
            own_id,
            rate_limiter: Default::default(),
            polling_interval,
            current_epoch: AtomicU32::new(0),
        })
    }

    /// The latest on-chain epoch observed by the polling loop, or `None` if not yet fetched.
    pub fn current_epoch(&self) -> Option<u32> {
        match self.current_epoch.load(Ordering::Relaxed) {
            0 => None,
            epoch => Some(epoch),
        }
    }

    pub fn try_spend(&self, portal_id: PeerId, allocation_chip: f32) -> RateLimitStatus {
        self.rate_limiter
            .lock()
            .try_run_request(portal_id, allocation_chip)
    }

    pub fn refund(&self, portal_id: PeerId, allocation_chip: f32) {
        self.rate_limiter.lock().refund(portal_id, allocation_chip);
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
            self.current_epoch.store(epoch, Ordering::Relaxed);
            if epoch > current_epoch {
                info!("New epoch started. Updating allocations");
                match tokio::try_join!(
                    self.client.epoch_length(),
                    self.client.portal_clusters(self.own_id)
                ) {
                    Ok((epoch_length, clusters)) => {
                        self.rate_limiter
                            .lock()
                            .update_allocations(clusters, epoch_length);
                        current_epoch = epoch;
                    }
                    Err(e) => {
                        warn!("Couldn't fetch CU allocations: {e:?}");
                    }
                }
            }
        }
        info!("Allocations update task finished");
    }
}
