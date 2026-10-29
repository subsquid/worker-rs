//! Query surface: admission outcomes and the RP-20 error taxonomy.
//!
//! Part of the conformance tier (spec/13). Run the whole tier with
//! `cargo test --test e2e --test query_surface --test query_concurrency`. A failure
//! prints the run seed; replay with `SQD_CONFORMANCE_SEED=0x…`.

mod harness;

use harness::{corpus, validators, Config, Harness};
use sqd_messages::query_result;

/// RP-10: a range disjoint from the chunk is not an error — it is an empty result whose
/// `last_block` still lets the client advance (RP-11's empty case).
#[tokio::test(flavor = "multi_thread")]
async fn disjoint_range_returns_empty_result_not_an_error() {
    let mut h = Harness::start().await;

    let chunk = corpus::chunk(2_000, 2_004, 1);
    let placement = h.host_chunk(&chunk);
    h.publish_and_apply("assignment-1", &[placement]).await;
    h.await_all_chunks_available().await;

    let query = h.all_blocks_query(&chunk.id, (9_000, 9_100));
    let served = h.serve(query.clone()).await;
    let (response, _) = served.expect_admitted();

    validators::query_response(response, &query, h.worker_id)
        .assert_none("disjoint-range response");

    let query_result::Result::Ok(ok) = response.result.as_ref().unwrap() else {
        panic!(
            "RP-10: a disjoint range must not be rejected, got {:?}",
            response.result
        );
    };
    assert!(ok.data.is_empty(), "RP-11: empty result");
    assert_eq!(ok.last_block, 9_100, "RP-11: last_block = range.end");
}

/// RP-20 / RP-1: an unassigned chunk id is `not_found`, and the query is still logged
/// because it was admitted (INV-32).
#[tokio::test(flavor = "multi_thread")]
async fn unknown_chunk_is_not_found_and_logged() {
    let mut h = Harness::start().await;

    let chunk = corpus::chunk(3_000, 3_004, 1);
    let placement = h.host_chunk(&chunk);
    h.publish_and_apply("assignment-1", &[placement]).await;
    h.await_all_chunks_available().await;

    let query = h.all_blocks_query("0000000000/0000000000-0000000004-deadbeef", (0, 4));
    let served = h.serve(query.clone()).await;
    let (response, log) = served.expect_admitted();

    validators::query_response(response, &query, h.worker_id).assert_none("not_found response");
    let query_result::Result::Err(err) = response.result.as_ref().unwrap() else {
        panic!("expected an error, got {:?}", response.result);
    };
    assert!(
        matches!(err.err, Some(sqd_messages::query_error::Err::NotFound(_))),
        "RP-20: expected not_found, got {:?}",
        err.err
    );
    assert!(log.result.is_some(), "INV-32: admitted query is logged");
}

/// RP-1 step 1: a query whose signature doesn't verify is rejected before admission —
/// no compute unit, no log record.
#[tokio::test(flavor = "multi_thread")]
async fn bad_signature_is_rejected_pre_admission() {
    let h = Harness::start().await;

    let query = h
        .portal
        .query(h.worker_id, harness::DATASET, "any/chunk", (0, 1))
        .body("{\"type\":\"evm\"}")
        .sign_corrupted();

    let served = h.serve(query).await;
    let harness::Served::PreAdmission { reason, .. } = &served else {
        panic!("RP-1: an unverifiable signature must not be admitted");
    };
    assert!(
        matches!(reason, sqd_messages::query_error::Err::BadRequest(_)),
        "RP-20: expected bad_request, got {reason:?}"
    );

    let now = sqd_worker::util::timestamp_now_ms();
    let page = h.logs_page_until(None, now).await;
    assert!(
        page.queries_executed.is_empty(),
        "RP-1: pre-admission failures produce no log record"
    );
}

/// RP-1 step 5 / REQ-21: a portal whose operator holds no allocation is turned away before
/// admission, and without a retry hint — only an on-chain change can bring it back. Enforcement
/// makes no difference here: the switch drops the pacing, never the gate.
#[tokio::test(flavor = "multi_thread")]
async fn a_portal_without_an_allocation_is_rejected() {
    for rate_limiting in [true, false] {
        let mut h = Harness::with_config(Config {
            rate_limiting,
            ..Config::default()
        })
        .await;

        let chunk = corpus::chunk(4_000, 4_004, 1);
        let placement = h.host_chunk(&chunk);
        h.publish_and_apply("assignment-1", &[placement]).await;

        // Buckets are re-read only when the observed epoch advances (DEF-22).
        h.registry.revoke_allocations();
        h.registry.advance_epoch();
        h.await_condition(
            "the revoked allocation reaches the admission bar",
            || async {
                let query = h.all_blocks_query(&chunk.id, (4_000, 4_005));
                matches!(h.serve(query).await, harness::Served::PreAdmission { .. })
            },
        )
        .await;

        let served = h.serve(h.all_blocks_query(&chunk.id, (4_000, 4_005))).await;
        let harness::Served::PreAdmission { response, reason } = &served else {
            panic!("rate_limiting = {rate_limiting}: an unallocated portal must not be admitted");
        };
        assert!(
            matches!(reason, sqd_messages::query_error::Err::TooManyRequests(_)),
            "RP-20: expected too_many_requests, got {reason:?}"
        );
        assert_eq!(
            response.retry_after_ms, None,
            "RP-20: no hint exists while the operator has no allocation"
        );
    }
}

/// P-CU-ENFORCE off: an allocated portal that has spent its epoch budget keeps being served,
/// and is never told to wait. The registry is read throughout — the epoch is reported and
/// follows the chain — so what the switch drops is the pacing, not the accounting.
#[tokio::test(flavor = "multi_thread")]
async fn unenforced_worker_serves_a_spent_allocation() {
    let mut h = Harness::with_config(Config {
        // One CU for a 20-minute epoch, and buckets start empty: enforcing, not one of the
        // queries below would get through.
        compute_units: 1,
        rate_limiting: false,
        ..Config::default()
    })
    .await;

    let chunk = corpus::chunk(5_000, 5_004, 1);
    let placement = h.host_chunk(&chunk);
    h.publish_and_apply("assignment-1", &[placement]).await;
    h.await_all_chunks_available().await;

    for i in 0..10 {
        let query = h.all_blocks_query(&chunk.id, (5_000, 5_005));
        let served = h.serve(query.clone()).await;
        let (response, _) = served.expect_admitted();

        validators::query_response(response, &query, h.worker_id)
            .assert_none("unenforced response");
        assert!(
            matches!(response.result, Some(query_result::Result::Ok(_))),
            "query {i} must be served, got {:?}",
            response.result
        );
        assert_eq!(
            response.retry_after_ms, None,
            "query {i}: nothing is being paced, so nothing is hinted"
        );
    }

    let epoch = h.registry.epoch();
    h.await_condition("the observed epoch reaches the status report", || async {
        h.status().await.current_epoch == Some(epoch)
    })
    .await;

    h.registry.advance_epoch();
    let advanced = h.registry.epoch();
    h.await_condition("the status report follows the chain", || async {
        h.status().await.current_epoch == Some(advanced)
    })
    .await;
}
