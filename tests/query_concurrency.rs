//! Query-concurrency cap and gauge tests. Kept separate because metrics are process-global.

mod harness;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use harness::{corpus, Config, Harness};
use sqd_messages::{query_error, query_result};

/// Enforces P-Q-PAR and keeps the running-query gauge accurate (RP-4, INV-31).
#[tokio::test(flavor = "multi_thread")]
async fn concurrency_cap_is_enforced_and_gauge_tracks_it() {
    for cap in [2, 3] {
        cap_is_honoured(cap).await;
    }
}

async fn cap_is_honoured(cap: usize) {
    const EXCESS: usize = 10;

    let mut h = Harness::with_config(Config {
        parallel_queries: cap,
        ..Config::default()
    })
    .await;

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

    let queries: Vec<_> = (0..cap + EXCESS)
        .map(|_| h.all_blocks_query(&chunk.id, (4_000, 4_400)))
        .collect();

    // Spin so short-lived gauge peaks are observed.
    let done = Arc::new(AtomicBool::new(false));
    let sampler = std::thread::spawn({
        let done = done.clone();
        move || {
            let mut peak = 0;
            while !done.load(Ordering::Relaxed) {
                peak = peak.max(sqd_worker::metrics::RUNNING_QUERIES.get());
                std::hint::spin_loop();
            }
            peak
        }
    });

    // Poll all futures together to guarantee contention.
    let results = futures::future::join_all(queries.into_iter().map(|q| h.serve(q))).await;
    done.store(true, Ordering::Relaxed);
    let peak = sampler.join().expect("sampler thread");

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
        "RP-4/REQ-22: {} concurrent queries against a cap of {cap} produced no \
         server_overloaded rejection — the cap is not being enforced",
        cap + EXCESS
    );
    // Equality, not a bound: `peak <= cap` also passes an implementation stuck at one slot.
    assert_eq!(
        peak, cap as i64,
        "RP-4/INV-31: running_queries peaked at {peak} against a configured cap of {cap}"
    );

    h.await_condition("running_queries returns to zero", || async {
        sqd_worker::metrics::RUNNING_QUERIES.get() == 0
    })
    .await;
}
