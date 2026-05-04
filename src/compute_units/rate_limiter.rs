use std::{collections::HashMap, time::Duration};

use sqd_contract_client::{Address, PortalCluster};
use sqd_network_transport::PeerId;
use tokio::time::Instant;

#[derive(Default)]
pub struct RateLimiter {
    operators: HashMap<Address, Bucket>,
    operator_by_portal_id: HashMap<PeerId, Address>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitStatus {
    Spent(Option<Duration>),
    Paused(Duration),
    NoAllocation,
}

impl RateLimitStatus {
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            RateLimitStatus::Spent(retry_after) => *retry_after,
            RateLimitStatus::Paused(retry_after) => Some(*retry_after),
            RateLimitStatus::NoAllocation => None,
        }
    }
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
        let clipped = allocation_chip.max(0.).min(1.);
        if self.tokens - clipped >= 0. {
            self.tokens -= clipped;
            true
        } else {
            false
        }
    }

    fn put(&mut self, allocation_chip: f32) {
        self.tokens = (self.tokens + allocation_chip.max(0.).min(1.)).min(MAX_TOKENS);
    }

    fn is_empty(&self) -> bool {
        self.tokens < 1.
    }

    fn until_next_token(&self, now: Instant) -> Duration {
        let next_update = self.last_update + self.request_interval;
        next_update - now
    }
}

impl RateLimiter {
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
        if bucket.take(allocation_chip) {
            let retry_after = if bucket.is_empty() {
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
    use super::*;

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
        assert!(bucket.is_empty());
        assert_eq!(bucket.until_next_token(now), Duration::from_millis(400));

        bucket.put(1.);
        assert_eq!(bucket.take(1.), true);
        assert!(bucket.is_empty());
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
}
