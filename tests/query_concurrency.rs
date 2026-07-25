//! Query concurrency: the RP-4 cap and the OB-6 gauge that reports it.
//!
//! Its own binary because the OB signals are process-global: beside other query-running
//! tests, cargo's parallel threads would make the gauge read somebody else's traffic. Any
//! other process-global assertion needs the same — its own binary, or `--test-threads=1`.
//!
//! Part of the conformance tier (spec/13). A failure prints the run seed; replay with
//! `SQD_CONFORMANCE_SEED=0x…`.

mod harness;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use harness::{corpus, Config, Harness};
use sqd_messages::{query_error, query_result};

/// RP-4 / REQ-22 / INV-31: at most P-Q-PAR queries execute concurrently, excess yields
/// `server_overloaded`, and the running-query gauge reflects the queries in flight.
///
/// Was GAP-1: `run_query`'s scopeguard was bound to `_`, so it dropped at the end of its
/// own statement and the slot was freed before the query ran.
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

    // Sampled from a task, not a fixed sleep: how long the queries run is machine-dependent.
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

    // Released on every path, overload early return included, so the gauge must fall back.
    h.await_condition("running_queries returns to zero", || async {
        sqd_worker::metrics::RUNNING_QUERIES.get() == 0
    })
    .await;
}
