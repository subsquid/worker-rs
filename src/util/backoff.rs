use std::time::Duration;

use tower::retry::backoff::ExponentialBackoffMaker;
use tower::util::rng::HasherRng;

/// Each wait is the doubling base plus up to half of it again, never past the cap, so workers
/// that failed together do not retry together.
const JITTER: f64 = 0.5;

/// Doubling waits from `min` to `max`, jittered.
pub fn exponential(min: Duration, max: Duration) -> ExponentialBackoffMaker {
    let max = max.max(Duration::from_millis(1));
    ExponentialBackoffMaker::new(min.min(max), max, JITTER, HasherRng::default())
        .expect("min is clamped below max and max above zero, which is all the maker checks")
}
