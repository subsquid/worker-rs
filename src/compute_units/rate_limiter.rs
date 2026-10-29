use std::{collections::HashMap, time::Duration};

use sqd_contract_client::{Address, PortalCluster};
use sqd_network_transport::PeerId;
use tokio::time::Instant;

pub struct RateLimiter {
    operators: HashMap<Address, Bucket>,
    operator_by_portal_id: HashMap<PeerId, Address>,
    /// False drops the pacing only: a portal still needs a registered, allocated operator to be
    /// admitted, and its budget is still drawn down — an exhausted one just no longer waits.
    enforcing: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitStatus {
    Spent(Option<Duration>),
    Paused(Duration),
    NoAllocation,
}

const MAX_TOKENS: f32 = 3.0f32;

struct Bucket {
    request_interval: Duration, // 1 / RPS
    tokens: f32,
    last_update: Instant,
}

impl Bucket {
    fn update(&mut self, now: Instant) {
        let elapsed = now - self.last_update;
        let tokens_to_add = elapsed.as_nanos() / self.request_interval.as_nanos();
        if tokens_to_add <= u32::MAX as u128 {
            self.last_update += self.request_interval * (tokens_to_add as u32);
            self.tokens = (self.tokens + tokens_to_add as f32).min(MAX_TOKENS);
        } else {
            self.last_update = now;
            self.tokens = MAX_TOKENS;
        }
    }

    fn take(&mut self, allocation_chip: f32) -> bool {
        let clipped = allocation_chip.clamp(0., 1.);
        if self.tokens - clipped >= 0. {
            self.tokens -= clipped;
            true
        } else {
            false
        }
    }

    fn put(&mut self, allocation_chip: f32) {
        self.tokens = (self.tokens + allocation_chip.clamp(0., 1.)).min(MAX_TOKENS);
    }

    fn can_serve_one_token(&self) -> bool {
        self.tokens < 1.
    }

    fn until_next_token(&self, now: Instant) -> Duration {
        let next_update = self.last_update + self.request_interval;
        next_update - now
    }
}

impl RateLimiter {
    pub fn new(enforcing: bool) -> Self {
        Self {
            operators: HashMap::default(),
            operator_by_portal_id: HashMap::default(),
            enforcing,
        }
    }

    pub fn update_allocations(&mut self, clusters: Vec<PortalCluster>, epoch_length: Duration) {
        self.operator_by_portal_id.clear();
        let mut new_operators = HashMap::default();

        let now = Instant::now();

        for cluster in clusters {
            if cluster.allocated_computation_units <= 0.into() {
                continue;
            }
            match self.operators.remove(&cluster.operator_addr) {
                None => {
                    new_operators.insert(
                        cluster.operator_addr,
                        Bucket {
                            request_interval: epoch_length
                                .div_f64(cluster.allocated_computation_units.as_u64() as f64),
                            tokens: 0.0f32,
                            last_update: Instant::now(),
                        },
                    );
                }
                Some(mut bucket) => {
                    bucket.update(now);
                    bucket.request_interval =
                        epoch_length.div_f64(cluster.allocated_computation_units.as_u64() as f64);
                    new_operators.insert(cluster.operator_addr, bucket);
                }
            };
            for portal in cluster.portal_ids {
                self.operator_by_portal_id
                    .insert(portal, cluster.operator_addr);
            }
        }
        self.operators = new_operators;
    }

    // Returns whether the request was allowed and how long to wait until the next request can be made
    pub fn try_run_request(&mut self, portal_id: PeerId, allocation_chip: f32) -> RateLimitStatus {
        let Some(operator_id) = self.operator_by_portal_id.get(&portal_id) else {
            return RateLimitStatus::NoAllocation;
        };
        let bucket = self.operators.get_mut(operator_id).unwrap();

        let now = Instant::now();
        bucket.update(now);
        let within_budget = bucket.take(allocation_chip);

        // Unenforced, the budget is still drawn down but never gates: an exhausted operator is
        // served on, and no wait is hinted at either, since a hint paces just as a pause does.
        if !self.enforcing {
            return RateLimitStatus::Spent(None);
        }

        if within_budget {
            let retry_after = if bucket.can_serve_one_token() {
                Some(bucket.until_next_token(now))
            } else {
                None
            };
            RateLimitStatus::Spent(retry_after)
        } else {
            RateLimitStatus::Paused(bucket.until_next_token(now))
        }
    }

    pub fn refund(&mut self, portal_id: PeerId, allocation_chip: f32) {
        let Some(operator_id) = self.operator_by_portal_id.get(&portal_id) else {
            return;
        };
        let bucket = self.operators.get_mut(operator_id).unwrap();
        bucket.put(allocation_chip);
    }
}

#[cfg(test)]
mod tests {
    use sqd_contract_client::U256;

    use super::*;

    const OPERATOR: &str = "0x0000000000000000000000000000000000000042";
    const EPOCH: Duration = Duration::from_secs(1200);

    fn portal() -> PeerId {
        "12D3KooWSRvKpvNbsrGbLXGFZV7GYdcrYNh4W2nipwHHMYikzV58"
            .parse()
            .expect("valid peer id")
    }

    /// The only way to put tokens in a fresh bucket without waiting out a refill interval.
    fn fund(limiter: &mut RateLimiter, tokens: f32) {
        limiter
            .operators
            .values_mut()
            .next()
            .expect("an allocated operator")
            .tokens = tokens;
    }

    fn tokens(limiter: &RateLimiter) -> f32 {
        limiter
            .operators
            .values()
            .next()
            .expect("an allocated operator")
            .tokens
    }

    fn cluster(cus: u64) -> PortalCluster {
        PortalCluster {
            operator_addr: OPERATOR.parse().expect("valid operator address"),
            portal_ids: vec![portal()],
            allocated_computation_units: U256::from(cus),
        }
    }

    #[test]
    fn test_bucket() {
        let start = Instant::now();
        let mut bucket = Bucket {
            request_interval: Duration::from_secs(1),
            tokens: 0.0f32,
            last_update: start,
        };

        assert_eq!(bucket.take(1.), false);
        assert_eq!(bucket.until_next_token(start), Duration::from_millis(1000));

        let now = start + Duration::from_millis(1000);
        bucket.update(now);
        assert_eq!(bucket.take(1.), true);
        assert_eq!(bucket.take(1.), false);
        assert_eq!(bucket.until_next_token(now), Duration::from_secs(1));

        let now = start + Duration::from_millis(3600);
        bucket.update(now);
        assert_eq!(bucket.take(1.), true);
        assert_eq!(bucket.take(1.), true);
        assert!(bucket.can_serve_one_token());
        assert_eq!(bucket.until_next_token(now), Duration::from_millis(400));

        bucket.put(1.);
        assert_eq!(bucket.take(1.), true);
        assert!(bucket.can_serve_one_token());
        assert_eq!(bucket.until_next_token(now), Duration::from_millis(400));

        bucket.update(start + Duration::from_millis(1_200_000));
        assert_eq!(bucket.tokens, MAX_TOKENS);
    }

    #[test]
    fn test_bucket_put_fractional_chip() {
        let start = Instant::now();
        let mut bucket = Bucket {
            request_interval: Duration::from_secs(1),
            tokens: 0.0f32,
            last_update: start,
        };
        bucket.put(0.5);
        assert!(
            (bucket.tokens - 0.5).abs() < 1e-6,
            "put(0.5) should add 0.5 tokens, got {}",
            bucket.tokens
        );
    }

    #[test]
    fn a_portal_with_no_allocation_is_rejected() {
        for enforcing in [true, false] {
            let mut limiter = RateLimiter::new(enforcing);

            assert_eq!(
                limiter.try_run_request(portal(), 1.),
                RateLimitStatus::NoAllocation,
                "enforcing = {enforcing}: an unallocated operator is nobody's customer"
            );
        }
    }

    #[test]
    fn an_unenforcing_limiter_serves_an_exhausted_budget() {
        let mut limiter = RateLimiter::new(false);
        limiter.update_allocations(vec![cluster(1)], EPOCH);

        // One CU per 20-minute epoch, and a fresh bucket starts empty: enforcing, every one of
        // these would be paused.
        for i in 0..100 {
            assert_eq!(
                limiter.try_run_request(portal(), 1.),
                RateLimitStatus::Spent(None),
                "request {i}"
            );
        }
    }

    #[test]
    fn an_unenforcing_limiter_hints_no_wait_within_budget_either() {
        let mut limiter = RateLimiter::new(false);
        limiter.update_allocations(vec![cluster(1)], EPOCH);
        fund(&mut limiter, 1.5);

        // Enforcing, the half token left over would come back as a retry hint — pacing by
        // another name.
        assert_eq!(
            limiter.try_run_request(portal(), 1.),
            RateLimitStatus::Spent(None)
        );
    }

    #[test]
    fn the_budget_is_still_charged_and_refunded_while_unenforced() {
        let mut limiter = RateLimiter::new(false);
        limiter.update_allocations(vec![cluster(1)], EPOCH);
        fund(&mut limiter, 2.);

        limiter.try_run_request(portal(), 1.);
        assert_eq!(tokens(&limiter), 1., "the unit is charged");

        limiter.refund(portal(), 1.);
        assert_eq!(tokens(&limiter), 2., "and the unused fraction comes back");
    }

    #[test]
    fn allocations_are_tracked_even_while_unenforced() {
        let mut limiter = RateLimiter::new(false);

        limiter.update_allocations(vec![cluster(1_000)], EPOCH);

        assert_eq!(limiter.operator_by_portal_id.len(), 1);
        assert_eq!(limiter.operators.len(), 1, "the bucket is kept warm");
    }
}
