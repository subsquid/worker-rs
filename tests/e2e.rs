//! End-to-end path: assign → download → query → verify → logs pull.
//!
//! Part of the conformance tier (spec/13). Run the whole tier with
//! `cargo test --test e2e --test query_surface --test query_concurrency`. A failure
//! prints the run seed; replay with `SQD_CONFORMANCE_SEED=0x…`.

mod harness;

use harness::{corpus, validators, Harness};
use sqd_messages::query_result;

/// One test rather than four: the point is that the stubs, the real subsystems and the
/// validators compose into a working loop.
#[tokio::test(flavor = "multi_thread")]
async fn smoke_assign_download_query_verify_logs() {
    let mut h = Harness::start().await;

    // assign (IB-40/41 → WP-2)
    let chunk = corpus::chunk(1_000, 1_009, 1);
    let placement = h.host_chunk(&chunk);
    let assignment = h.publish_and_apply("assignment-1", &[placement]).await;

    let status = h.status().await;
    assert_eq!(status.assignment_id, assignment.id, "RP-21: applied id");
    validators::status(&status, 1).assert_none("status after application");

    // download (WP-11/12)
    h.await_all_chunks_available().await;

    // INV-13: what was committed is byte-identical to what the origin served.
    for (name, _) in &chunk.files {
        let served = h
            .origin
            .served_bytes(&chunk.id, name)
            .unwrap_or_else(|| panic!("origin never served {name}"));
        let on_disk = std::fs::read(h.chunk_dir(&chunk.id).join(name))
            .unwrap_or_else(|e| panic!("committed chunk is missing {name}: {e}"));
        assert_eq!(on_disk, served, "INV-13: {name} differs from origin bytes");
    }

    let status = h.status().await;
    validators::status(&status, 1).assert_none("status after download");
    assert!(
        status.missing_chunks.as_ref().unwrap().ones() == 0,
        "INV-30: no chunk should be missing after convergence"
    );
    assert!(status.stored_bytes.unwrap() > 0, "OB-5: stored bytes");

    // query (RP-1 → RP-15)
    let query = h.all_blocks_query(&chunk.id, (1_000, 1_009));
    let served = h.serve(query.clone()).await;
    let (response, log) = served.expect_admitted();

    validators::query_response(response, &query, h.worker_id).assert_none("query response");

    let query_result::Result::Ok(ok) = response.result.as_ref().unwrap() else {
        panic!("expected a successful result, got {:?}", response.result);
    };
    assert_eq!(ok.last_block, 1_009, "RP-11: whole range evaluated");

    let blocks: Vec<u64> = std::str::from_utf8(&ok.data)
        .unwrap()
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).unwrap()["header"]["number"]
                .as_u64()
                .unwrap()
        })
        .collect();
    assert_eq!(blocks, (1_000..=1_009).collect::<Vec<_>>(), "RP-12");

    // INV-23: the log records the same outcome the client saw.
    assert!(
        matches!(
            log.result,
            Some(sqd_messages::query_executed::Result::Ok(_))
        ),
        "INV-23: log outcome must match the response, got {:?}",
        log.result
    );

    // logs pull (RP-22): the serving lag withholds a record this fresh...
    let withheld = h.logs_page(None).await;
    assert!(
        withheld.queries_executed.is_empty(),
        "RP-22: records younger than P-LOGS-LAG must not be served"
    );

    // ...and reading past it returns exactly the one record the query produced.
    let now = sqd_worker::util::timestamp_now_ms();
    let page = h.logs_page_until(None, now).await;
    assert_eq!(page.queries_executed.len(), 1, "INV-32: one record");
    assert!(!page.has_more);
    validators::logs_page(&page, None, now, 0, Harness::logs_page_budget())
        .assert_none("logs page");

    let record = &page.queries_executed[0];
    assert_eq!(
        record.query.as_ref().unwrap().query_id,
        query.query_id,
        "IB-21: the record carries the original query"
    );
}
