//! HC-8 — chain registry stub (IB-43): worker id lookup, epoch, per-operator allocations.
//!
//! A programmable `sqd_contract_client::Client`, so a test can advance the epoch, change a
//! portal's allocation, or make a read fail — the three things the metering path (RP-1
//! step 5, LIV-12) actually depends on.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use num_rational::Ratio;
use parking_lot::Mutex;
use sqd_contract_client::{
    Address, Allocation, Client, ClientError, PeerId, PortalCluster, Worker, U256,
};

const OPERATOR: &str = "0x0000000000000000000000000000000000000042";

#[derive(Clone)]
struct State {
    epoch: u32,
    epoch_length: Duration,
    epoch_start: u64,
    worker_id: U256,
    /// Portals with an allocation, and the CUs allocated to this worker for the epoch.
    clusters: Vec<(Vec<PeerId>, u64)>,
    /// When set, every read fails with it — FM-52's "registry error at startup".
    failure: Option<String>,
}

/// Handle for driving the stub during a test; cheap to clone.
#[derive(Clone)]
pub struct Registry {
    state: Arc<Mutex<State>>,
}

impl Registry {
    /// A registry that knows `worker` and grants `portals` `cus` compute units per epoch.
    pub fn new(portals: &[PeerId], cus: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                epoch: 1,
                epoch_length: Duration::from_secs(1200),
                epoch_start: 1_700_000_000,
                worker_id: U256::from(1u64),
                clusters: vec![(portals.to_vec(), cus)],
                failure: None,
            })),
        }
    }

    /// The `Box<dyn Client>` an `AllocationsChecker` is constructed from.
    pub fn client(&self) -> Box<dyn Client> {
        Box::new(StubClient {
            state: self.state.clone(),
        })
    }

    pub fn epoch(&self) -> u32 {
        self.state.lock().epoch
    }

    /// Advances the epoch, which is what makes the allocations loop refresh buckets.
    pub fn advance_epoch(&self) {
        let mut state = self.state.lock();
        state.epoch += 1;
        state.epoch_start += state.epoch_length.as_secs();
    }

    pub fn set_allocation(&self, portals: &[PeerId], cus: u64) {
        self.state.lock().clusters = vec![(portals.to_vec(), cus)];
    }

    /// Removes every allocation: queries then hit the no-allocation path (RP-20's
    /// `too_many_requests` without a hint).
    pub fn revoke_allocations(&self) {
        self.state.lock().clusters.clear();
    }

    pub fn fail_reads(&self, message: impl Into<String>) {
        self.state.lock().failure = Some(message.into());
    }

    pub fn heal(&self) {
        self.state.lock().failure = None;
    }
}

#[derive(Clone)]
struct StubClient {
    state: Arc<Mutex<State>>,
}

impl StubClient {
    // `ClientError` is large by design upstream; the trait we implement returns it.
    #[allow(clippy::result_large_err)]
    fn read(&self) -> Result<State, ClientError> {
        let state = self.state.lock().clone();
        match &state.failure {
            Some(msg) => Err(ClientError::Contract(msg.clone())),
            None => Ok(state),
        }
    }

    fn operator() -> Address {
        OPERATOR.parse().expect("operator address is valid")
    }
}

#[async_trait]
impl Client for StubClient {
    fn clone_client(&self) -> Box<dyn Client> {
        Box::new(self.clone())
    }

    async fn current_epoch(&self) -> Result<u32, ClientError> {
        Ok(self.read()?.epoch)
    }

    async fn current_epoch_start(&self) -> Result<SystemTime, ClientError> {
        Ok(UNIX_EPOCH + Duration::from_secs(self.read()?.epoch_start))
    }

    async fn epoch_length(&self) -> Result<Duration, ClientError> {
        Ok(self.read()?.epoch_length)
    }

    async fn worker_id(&self, _peer_id: PeerId) -> Result<U256, ClientError> {
        Ok(self.read()?.worker_id)
    }

    async fn active_workers(&self) -> Result<Vec<Worker>, ClientError> {
        Ok(Vec::new())
    }

    async fn is_portal_registered(&self, portal_id: PeerId) -> Result<bool, ClientError> {
        Ok(self
            .read()?
            .clusters
            .iter()
            .any(|(portals, _)| portals.contains(&portal_id)))
    }

    async fn worker_registration_time(
        &self,
        _peer_id: PeerId,
    ) -> Result<Option<SystemTime>, ClientError> {
        Ok(Some(
            UNIX_EPOCH + Duration::from_secs(self.read()?.epoch_start),
        ))
    }

    async fn active_portals(&self) -> Result<Vec<PeerId>, ClientError> {
        Ok(self
            .read()?
            .clusters
            .iter()
            .flat_map(|(portals, _)| portals.clone())
            .collect())
    }

    #[allow(deprecated)]
    async fn current_allocations(
        &self,
        _portal_id: PeerId,
        _worker_ids: Option<Vec<Worker>>,
    ) -> Result<Vec<Allocation>, ClientError> {
        Ok(Vec::new())
    }

    async fn portal_compute_units_per_epoch(&self, portal_id: PeerId) -> Result<u64, ClientError> {
        Ok(self
            .read()?
            .clusters
            .iter()
            .find(|(portals, _)| portals.contains(&portal_id))
            .map_or(0, |(_, cus)| *cus))
    }

    async fn portal_uses_default_strategy(&self, _portal_id: PeerId) -> Result<bool, ClientError> {
        Ok(true)
    }

    async fn portal_clusters(&self, _worker_id: U256) -> Result<Vec<PortalCluster>, ClientError> {
        Ok(self
            .read()?
            .clusters
            .iter()
            .map(|(portals, cus)| PortalCluster {
                operator_addr: Self::operator(),
                portal_ids: portals.clone(),
                allocated_computation_units: U256::from(*cus),
            })
            .collect())
    }

    async fn portal_sqd_locked(
        &self,
        _portal_id: PeerId,
    ) -> Result<Option<(String, Ratio<u128>)>, ClientError> {
        Ok(Some((OPERATOR.to_owned(), Ratio::new(1, 1))))
    }
}
