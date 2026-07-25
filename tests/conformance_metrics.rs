//! Conformance tests that read process-global metrics (spec/12 OB, INV-31).
//!
//! Separate test binary on purpose. The OB counters and gauges are `lazy_static`s shared
//! by the whole process, so a test that asserts on `running_queries` cannot share a
//! process with tests that run queries for other reasons — cargo would run them on
//! parallel threads and the reading would be somebody else's traffic. One test per
//! process-global signal; if this file grows a second gauge test, they need
//! `--test-threads=1` or their own binary in turn.

mod harness;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use harness::{corpus, Config, Harness};
use sqd_messages::{query_error, query_result};

/// RP-4 / REQ-22 / INV-31: at most P-Q-PAR queries execute concurrently, excess yields
/// `server_overloaded`, and the running-query gauge reflects the queries in flight.
///
/// This was GAP-1. The cap was inert because `run_query`'s scopeguard was bound to `_`,
/// so it dropped at the end of its own `let` statement — the slot was released before the
/// query ran, and the gauge fell back to zero immediately after rising.
#[tokio::test(flavor = "multi_thread")]
async fn concurrency_cap_is_enforced_and_gauge_tracks_it() {
    const CAP: usize = 2;
    const EXCESS: usize = 10;

    let mut h = Harness::with_config(Config {
        parallel_queries: CAP,
        ..Config::default()
    })
    .await;

    // Heavy rows over a wide range: each query outlives the others' admission checks.
    let chunk = corpus::chunk(4_000, 4_400, 64 * 1024);
    let placement = h.host_chunk(&chunk);
    h.publish_and_apply("assignment-1", &[placement]).await;
    h.await_all_chunks_available().await;

    let gauge_before = sqd_worker::metrics::RUNNING_QUERIES.get();
    assert_eq!(
        gauge_before, 0,
        "INV-31: gauge must be zero before any query runs; \
         another test in this process is interfering"
    );

    let queries: Vec<_> = (0..CAP + EXCESS)
        .map(|_| h.all_blocks_query(&chunk.id, (4_000, 4_400)))
        .collect();

    // Sampled from a separate task rather than a fixed sleep: the peak must be observed
    // while the queries are in flight, and how long that is depends on the machine.
    let done = Arc::new(AtomicBool::new(false));
    let sampler = tokio::spawn({
        let done = done.clone();
        async move {
            let mut peak = 0;
            while !done.load(Ordering::Relaxed) {
                peak = peak.max(sqd_worker::metrics::RUNNING_QUERIES.get());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            peak
        }
    });

    let results = futures::future::join_all(queries.into_iter().map(|q| h.serve(q))).await;
    done.store(true, Ordering::Relaxed);
    let peak = sampler.await.expect("sampler task");

    let overloaded = results
        .iter()
        .filter(|served| {
            matches!(
                served.response().result.as_ref().unwrap(),
                query_result::Result::Err(e)
                    if matches!(e.err, Some(query_error::Err::ServerOverloaded(_)))
            )
        })
        .count();

    assert!(
        overloaded >= 1,
        "RP-4/REQ-22: {} concurrent queries against a cap of {CAP} produced no \
         server_overloaded rejection — the cap is not being enforced",
        CAP + EXCESS
    );
    assert!(
        peak > 0,
        "INV-31/OB-6: running_queries never rose above zero while {} queries ran",
        CAP + EXCESS
    );
    assert!(
        peak <= CAP as i64,
        "RP-4: running_queries peaked at {peak}, above the configured cap of {CAP}"
    );

    // The slot is released on every path, including the overload early return, so the
    // gauge must come back down (INV-31: it tracks a set, it does not drift).
    h.await_condition("running_queries returns to zero", || async {
        sqd_worker::metrics::RUNNING_QUERIES.get() == 0
    })
    .await;
}
