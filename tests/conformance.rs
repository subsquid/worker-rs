//! Conformance tier (spec/13). Phase 0: the harness skeleton plus one end-to-end smoke.
//!
//! Run with `cargo test --test conformance`. A failure prints the run seed; replay with
//! `SQD_CONFORMANCE_SEED=0x…`.

mod harness;

use harness::{corpus, validators, Harness};
use sqd_messages::query_result;

/// Phase 0's end-to-end smoke: assign → download → query → verify → logs pull.
///
/// It is deliberately one test rather than four: the point is that the stubs, the real
/// subsystems and the validators compose into a working loop, which is what unblocks the
/// unchecked rows of the traceability matrix.
#[tokio::test(flavor = "multi_thread")]
async fn smoke_assign_download_query_verify_logs() {
    let mut h = Harness::start().await;

    // ---- assign (IB-40/41 → WP-2) ----
    let chunk = corpus::chunk(1_000, 1_009, 1);
    let placement = h.host_chunk(&chunk);
    let assignment = h.publish_and_apply("assignment-1", &[placement]).await;

    let status = h.status().await;
    assert_eq!(status.assignment_id, assignment.id, "RP-21: applied id");
    validators::status(&status, 1).assert_none("status after application");

    // ---- download (WP-11/12) ----
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

    // ---- query (RP-1 → RP-15) ----
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

    // ---- logs pull (RP-22) ----
    // The serving lag withholds a record this fresh...
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
    validators::logs_page(&page, None, now, 0, 10 * 1024 * 1024).assert_none("logs page");

    let record = &page.queries_executed[0];
    assert_eq!(
        record.query.as_ref().unwrap().query_id,
        query.query_id,
        "IB-21: the record carries the original query"
    );
}

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

/// The harness declares what it cannot yet observe, so an empty result is never mistaken
/// for a passing one (spec/12 §lying metrics, spec/13 MG-8). Each declaration has to name
/// the spec identifier it defers to, otherwise the list decays into prose that nobody can
/// reconcile against the capability register.
#[test]
fn declared_gaps_cite_the_spec() {
    for line in harness::UNCOVERED.iter().chain(validators::MISSING) {
        assert!(
            cites_a_spec_id(line),
            "declared gap names no spec identifier: {line:?}"
        );
    }
    // The checker must be able to fail, or the loop above proves nothing.
    assert!(!cites_a_spec_id("no identifiers here, just prose"));
    assert!(cites_a_spec_id("blocked on GAP-32"));
}

/// True if the text contains a `PREFIX-<digits>` token, the suite's identifier shape
/// (`HC-1`, `GAP-32`, `INV-13`, `P-Q-PAR` … — the last matches on its `P-Q` segment only
/// if numbered, which is why parameter names alone don't satisfy this).
fn cites_a_spec_id(text: &str) -> bool {
    text.split(|c: char| !c.is_ascii_alphanumeric() && c != '-')
        .flat_map(|token| token.split_inclusive('-').collect::<Vec<_>>())
        .collect::<Vec<_>>()
        .windows(2)
        .any(|pair| {
            // `ends_with('-')` keeps the pair inside one token: without it, "ABC 1x"
            // would read as an identifier.
            let Some(prefix) = pair[0].strip_suffix('-') else {
                return false;
            };
            !prefix.is_empty()
                && prefix.chars().all(|c| c.is_ascii_uppercase())
                && pair[1].chars().next().is_some_and(|c| c.is_ascii_digit())
        })
}
