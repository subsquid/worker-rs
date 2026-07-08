//! Dependency seams for the query-serving pipeline (`process_query`, `generate_log`) in
//! [`super::p2p`]. Production runs against the real `Worker` and `AllocationsChecker`; the traits
//! let tests inject an engine with a fixed outcome and a rate limiter that records what it spent,
//! with no transport or storage.

use async_trait::async_trait;
use sqd_network_transport::PeerId;

use crate::{
    compute_units::{allocations_checker::AllocationsChecker, RateLimitStatus},
    controller::worker::{QueryType, Worker},
    query::result::QueryResult,
    types::dataset::Dataset,
};

/// Compute-unit rate limiting, behind a trait so tests can set a verdict and record spending.
pub trait CuChecker {
    fn try_spend(&self, portal_id: PeerId, allocation_chip: f32) -> RateLimitStatus;
    fn refund(&self, portal_id: PeerId, allocation_chip: f32);
}

impl CuChecker for AllocationsChecker {
    fn try_spend(&self, portal_id: PeerId, allocation_chip: f32) -> RateLimitStatus {
        AllocationsChecker::try_spend(self, portal_id, allocation_chip)
    }

    fn refund(&self, portal_id: PeerId, allocation_chip: f32) {
        AllocationsChecker::refund(self, portal_id, allocation_chip)
    }
}

/// The query engine, behind a trait so tests can supply a fixed outcome.
#[async_trait]
pub trait QueryRunner {
    async fn run_query(
        &self,
        query_str: &str,
        dataset: Dataset,
        block_range: (u64, u64),
        chunk_id: &str,
        client_id: Option<PeerId>,
        query_type: QueryType,
    ) -> QueryResult;
}

#[async_trait]
impl QueryRunner for Worker {
    async fn run_query(
        &self,
        query_str: &str,
        dataset: Dataset,
        block_range: (u64, u64),
        chunk_id: &str,
        client_id: Option<PeerId>,
        query_type: QueryType,
    ) -> QueryResult {
        Worker::run_query(
            self,
            query_str,
            dataset,
            block_range,
            chunk_id,
            client_id,
            query_type,
        )
        .await
    }
}

#[cfg(test)]
pub mod mocks {
    use std::sync::atomic::{AtomicU32, Ordering};

    use async_trait::async_trait;
    use parking_lot::Mutex;
    use sqd_network_transport::PeerId;

    use crate::{
        compute_units::RateLimitStatus, controller::worker::QueryType, query::result::QueryResult,
        types::dataset::Dataset,
    };

    use super::{CuChecker, QueryRunner};

    /// Returns a fixed verdict and tracks net compute units. Like the real checker, only a `Spent`
    /// verdict deducts.
    pub struct MockChecker {
        verdict: RateLimitStatus,
        net_spent: Mutex<f32>,
    }

    impl MockChecker {
        pub fn new(verdict: RateLimitStatus) -> Self {
            Self {
                verdict,
                net_spent: Mutex::new(0.0),
            }
        }

        /// Net compute units charged to the peer: spent minus refunded.
        pub fn net_spent(&self) -> f32 {
            *self.net_spent.lock()
        }
    }

    impl CuChecker for MockChecker {
        fn try_spend(&self, _portal_id: PeerId, allocation_chip: f32) -> RateLimitStatus {
            if let RateLimitStatus::Spent(_) = self.verdict {
                *self.net_spent.lock() += allocation_chip;
            }
            self.verdict
        }

        fn refund(&self, _portal_id: PeerId, allocation_chip: f32) {
            *self.net_spent.lock() -= allocation_chip;
        }
    }

    /// Returns a fixed outcome and counts its calls.
    pub struct MockWorker {
        outcome: QueryResult,
        calls: AtomicU32,
    }

    impl MockWorker {
        pub fn new(outcome: QueryResult) -> Self {
            Self {
                outcome,
                calls: AtomicU32::new(0),
            }
        }

        pub fn calls(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl QueryRunner for MockWorker {
        async fn run_query(
            &self,
            _query_str: &str,
            _dataset: Dataset,
            _block_range: (u64, u64),
            _chunk_id: &str,
            _client_id: Option<PeerId>,
            _query_type: QueryType,
        ) -> QueryResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.outcome.clone()
        }
    }
}
