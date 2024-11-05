use std::{collections::HashMap, time::Duration};

use sqd_contract_client::{Address, GatewayCluster};
use sqd_network_transport::PeerId;
use tokio::time::Instant;

#[derive(Default)]
pub struct RateLimiter {
    operators: HashMap<Address, Bucket>,
    operator_by_gateway_id: HashMap<PeerId, Address>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Spent,
    Paused,
}

const MAX_TOKENS: u8 = 3;

struct Bucket {
    request_interval: Duration, // 1 / RPS
    tokens: u8,
    last_update: Instant,
}

impl Bucket {
    fn update(&mut self, now: Instant) {
        let elapsed = now - self.last_update;
        let tokens_to_add = elapsed.as_nanos() / self.request_interval.as_nanos();
        if tokens_to_add <= u32::MAX as u128 {
            self.last_update += self.request_interval * (tokens_to_add as u32);
            self.tokens = (self.tokens as u32)
                .saturating_add(tokens_to_add as u32)
                .min(MAX_TOKENS as u32) as u8;
        } else {
            self.last_update = now;
            self.tokens = MAX_TOKENS;
        }
    }

    fn take(&mut self) -> bool {
        if self.tokens > 0 {
            self.tokens -= 1;
            true
        } else {
            false
        }
    }

    fn put(&mut self) {
        self.tokens = (self.tokens + 1).min(MAX_TOKENS);
    }

    fn empty(&self) -> bool {
        self.tokens == 0
    }

    fn until_next_token(&self, now: Instant) -> Duration {
        let next_update = self.last_update + self.request_interval;
        next_update - now
    }
}

impl RateLimiter {
    pub fn update_allocations(&mut self, clusters: Vec<GatewayCluster>, epoch_length: Duration) {
        self.operator_by_gateway_id.drain();
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
                            tokens: 0,
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
            for gateway in cluster.gateway_ids {
                self.operator_by_gateway_id
                    .insert(gateway, cluster.operator_addr);
            }
        }
        self.operators = new_operators;
    }

    // Returns whether the request was allowed and how long to wait until the next request can be made
    pub fn try_run_request(&mut self, gateway_id: PeerId) -> (Status, Option<Duration>) {
        let Some(operator_id) = self.operator_by_gateway_id.get(&gateway_id) else {
            return (Status::Paused, None);
        };
        let bucket = self.operators.get_mut(operator_id).unwrap();

        let now = Instant::now();
        bucket.update(now);
        if bucket.take() {
            let retry_after = if bucket.empty() {
                Some(bucket.until_next_token(now))
            } else {
                None
            };
            (Status::Spent, retry_after)
        } else {
            (Status::Paused, Some(bucket.until_next_token(now)))
        }
    }

    pub fn refund(&mut self, gateway_id: PeerId) {
        let Some(operator_id) = self.operator_by_gateway_id.get(&gateway_id) else {
            return;
        };
        let bucket = self.operators.get_mut(operator_id).unwrap();
        bucket.put();
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
            tokens: 0,
            last_update: start,
        };

        assert_eq!(bucket.take(), false);
        assert_eq!(bucket.until_next_token(start), Duration::from_millis(1000));

        let now = start + Duration::from_millis(1000);
        bucket.update(now);
        assert_eq!(bucket.take(), true);
        assert_eq!(bucket.take(), false);
        assert_eq!(bucket.until_next_token(now), Duration::from_secs(1));

        let now = start + Duration::from_millis(3600);
        bucket.update(now);
        assert_eq!(bucket.take(), true);
        assert_eq!(bucket.take(), true);
        assert!(bucket.empty());
        assert_eq!(bucket.until_next_token(now), Duration::from_millis(400));

        bucket.put();
        assert_eq!(bucket.take(), true);
        assert!(bucket.empty());
        assert_eq!(bucket.until_next_token(now), Duration::from_millis(400));

        bucket.update(start + Duration::from_millis(1_200_000));
        assert_eq!(bucket.tokens, MAX_TOKENS);
    }
}
