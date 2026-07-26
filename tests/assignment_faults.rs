//! CT-4 input-fault corpus, assignment side: what a hostile or merely wrong document may do
//! to a running worker (REQ-24, FM-1, FM-11/12/13/52, REQ-25).
//!
//! Part of the conformance tier (spec/13). Run the whole tier with
//! `cargo test --test e2e --test query_surface --test query_concurrency
//! --test assignment_faults`. A failure prints the run seed; replay with
//! `SQD_CONFORMANCE_SEED=0x…`.

mod harness;

use std::time::Duration;

use harness::scheduler::AssignmentFault;
use harness::{corpus, validators, Config, Harness};
use sqd_messages::query_result;

/// FM-11: one unusable address costs one chunk. The rest of the document applies and the
/// worker keeps serving. The panic this replaces was a whole-process outage under ADR-14.
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_chunk_address_costs_that_chunk_only() {
    let mut h = Harness::start().await;

    // The knob corrupts the first placement's base address.
    let broken = corpus::chunk(1_000, 1_009, 1);
    let sound = corpus::chunk(1_010, 1_019, 1);
    let placements = [h.host_chunk(&broken), h.host_chunk(&sound)];
    h.publish(
        "assignment-1",
        &placements,
        AssignmentFault::UnparseableFileUrl,
    );
    assert!(
        h.poll_and_apply().await,
        "FM-11: a per-chunk defect must not reject the document"
    );

    // The sound chunk converges: the loop is alive and the rest of the slice applied.
    h.await_condition("the sound chunk becomes available", || async {
        let status = h.worker.status().await;
        status.unavailability_map.len() == 2 && !status.unavailability_map[1]
    })
    .await;

    h.await_condition("the bad address is alarmed", || async {
        h.worker.alarms().unresolvable_chunks > 0
    })
    .await;

    let status = h.status().await;
    assert!(
        status.missing_chunks.as_ref().unwrap().ones() == 1,
        "FM-3: exactly one chunk may be lost to one bad address"
    );
    validators::status(&status, 2).assert_none("status with one unresolvable chunk");
    assert_eq!(
        h.origin.fetch_count(&broken.id, "blocks.parquet"),
        0,
        "an address that doesn't parse cannot have been fetched"
    );

    // REQ-24: the chunk that did arrive still answers queries.
    let query = h.all_blocks_query(&sound.id, (1_010, 1_019));
    let served = h.serve(query.clone()).await;
    let (response, _) = served.expect_admitted();
    validators::query_response(response, &query, h.worker_id).assert_none("query after the fault");
    assert!(
        matches!(response.result, Some(query_result::Result::Ok(_))),
        "the sound chunk must still be queryable, got {:?}",
        response.result
    );

    // A degrade, not a quarantine: a document that fixes the address converges.
    let placements = [h.host_chunk(&broken), h.host_chunk(&sound)];
    h.publish_and_apply("assignment-2", &placements).await;
    h.await_all_chunks_available().await;
    assert_eq!(
        h.worker.alarms().unresolvable_chunks,
        0,
        "the alarm must clear once the addresses resolve"
    );
}

/// FM-12: a roster entry the reader can't parse rejects the document. The reader panics on
/// it, and that panic must cost one document, not the process (FM-1).
#[tokio::test(flavor = "multi_thread")]
async fn an_unparseable_roster_entry_rejects_the_document() {
    let mut h = Harness::start().await;

    let chunk = corpus::chunk(2_000, 2_009, 1);
    let placement = h.host_chunk(&chunk);
    let good = h
        .publish_and_apply("assignment-1", std::slice::from_ref(&placement))
        .await;
    h.await_all_chunks_available().await;

    h.publish(
        "assignment-2",
        std::slice::from_ref(&placement),
        AssignmentFault::CorruptRosterPeerId,
    );
    assert!(
        !h.poll_and_apply().await,
        "FM-12: a document whose roster can't be read must be rejected"
    );

    // WP-2: rejection changes nothing.
    let status = h.status().await;
    assert_eq!(
        status.assignment_id, good.id,
        "WP-2: prior assignment holds"
    );
    validators::status(&status, 1).assert_none("status after a rejected document");

    let query = h.all_blocks_query(&chunk.id, (2_000, 2_009));
    let served = h.serve(query.clone()).await;
    let (response, _) = served.expect_admitted();
    validators::query_response(response, &query, h.worker_id)
        .assert_none("query after a rejected document");
    assert!(
        matches!(response.result, Some(query_result::Result::Ok(_))),
        "REQ-24: the worker must keep serving, got {:?}",
        response.result
    );

    // And a readable document still applies afterwards.
    let next = h.publish_and_apply("assignment-3", &[placement]).await;
    assert_eq!(h.status().await.assignment_id, next.id);
}

/// REQ-25 / FM-13: an assignment that drops everything is held, not obeyed. Otherwise a
/// scheduler bug wipes a multi-terabyte store in one pass.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_slice_cannot_wipe_the_store() {
    let mut h = Harness::start().await;

    let chunks: Vec<_> = (0..4)
        .map(|i| corpus::chunk(5_000 + i * 10, 5_009 + i * 10, 1))
        .collect();
    let placements: Vec<_> = chunks.iter().map(|c| h.host_chunk(c)).collect();
    h.publish_and_apply("assignment-1", &placements).await;
    h.await_all_chunks_available().await;

    // 4 of 4 stored chunks would go, well past P-DEL-FLOOR.
    h.publish(
        "assignment-2",
        &placements,
        AssignmentFault::NoChunksForWorker,
    );
    assert!(h.poll_and_apply().await, "the document is well-formed");

    h.await_condition("the deletion floor holds the batch", || async {
        h.worker.alarms().deletion_hold.is_some()
    })
    .await;
    assert_eq!(h.worker.alarms().deletion_hold, Some(4));

    // Still there, and still serving: eviction is what makes a chunk unavailable, not the
    // assignment.
    for chunk in &chunks {
        assert!(
            h.chunk_dir(&chunk.id).exists(),
            "REQ-25: {} was deleted by a wipe-inducing assignment",
            chunk.id
        );
    }
    let query = h.all_blocks_query(&chunks[0].id, (5_000, 5_009));
    let served = h.serve(query.clone()).await;
    let (response, _) = served.expect_admitted();
    validators::query_response(response, &query, h.worker_id).assert_none("query under the hold");
    assert!(
        matches!(response.result, Some(query_result::Result::Ok(_))),
        "held data must still serve, got {:?}",
        response.result
    );

    // A restoring assignment brings the batch under the floor: 1 of 4 is 25 %.
    h.publish_and_apply("assignment-3", &placements[..3]).await;
    h.await_condition("the released eviction completes", || async {
        !h.chunk_dir(&chunks[3].id).exists()
    })
    .await;
    assert_eq!(
        h.worker.alarms().deletion_hold,
        None,
        "the hold must lift once the batch is under the floor"
    );
    for chunk in &chunks[..3] {
        assert!(h.chunk_dir(&chunk.id).exists(), "{} evicted", chunk.id);
    }
}

/// LIV-4: the hold is a delay, not a veto. Nothing happens after the wipe-inducing document
/// lands — no new assignment, no download — so the eviction has to come off the hold's own
/// timer, which is what LIV-4's "without requiring any further input event" demands.
#[tokio::test(flavor = "multi_thread")]
async fn a_persistent_wipe_goes_through_when_the_hold_lapses() {
    let mut h = Harness::with_config(Config {
        deletion_hold_max: Duration::from_millis(300),
        ..Default::default()
    })
    .await;

    let chunks: Vec<_> = (0..4)
        .map(|i| corpus::chunk(7_000 + i * 10, 7_009 + i * 10, 1))
        .collect();
    let placements: Vec<_> = chunks.iter().map(|c| h.host_chunk(c)).collect();
    h.publish_and_apply("assignment-1", &placements).await;
    h.await_all_chunks_available().await;

    h.publish(
        "assignment-2",
        &placements,
        AssignmentFault::NoChunksForWorker,
    );
    assert!(h.poll_and_apply().await);

    h.await_condition("the hold lapses and the store drains", || async {
        chunks.iter().all(|c| !h.chunk_dir(&c.id).exists())
    })
    .await;
    assert_eq!(h.worker.alarms().deletion_hold, None);
}

/// ADR-17's override: a sanctioned rebalance sets the floor to 1 and gets the old behavior.
#[tokio::test(flavor = "multi_thread")]
async fn the_floor_override_permits_a_full_rebalance() {
    let mut h = Harness::with_config(Config {
        deletion_floor: 1.0,
        ..Default::default()
    })
    .await;

    let chunk = corpus::chunk(6_000, 6_009, 1);
    let placement = h.host_chunk(&chunk);
    h.publish_and_apply("assignment-1", std::slice::from_ref(&placement))
        .await;
    h.await_all_chunks_available().await;

    h.publish(
        "assignment-2",
        &[placement],
        AssignmentFault::NoChunksForWorker,
    );
    assert!(h.poll_and_apply().await);

    h.await_condition("the chunk is evicted", || async {
        !h.chunk_dir(&chunk.id).exists()
    })
    .await;
    assert_eq!(h.worker.alarms().deletion_hold, None);
}

/// FM-52: the chain registry is down before the worker starts. Failing startup here turns a
/// registry outage into a fleet outage.
#[tokio::test(flavor = "multi_thread")]
async fn a_registry_outage_at_startup_degrades_instead_of_refusing_to_start() {
    let mut h = Harness::with_config(Config {
        registry_failure: Some("registry is down".to_owned()),
        ..Default::default()
    })
    .await;

    // The write path doesn't touch the registry: assignments apply, chunks converge.
    let chunk = corpus::chunk(4_000, 4_009, 1);
    let placement = h.host_chunk(&chunk);
    h.publish_and_apply("assignment-1", &[placement]).await;
    h.await_all_chunks_available().await;

    // No allocation is known, so queries are refused rather than answered.
    let query = h.all_blocks_query(&chunk.id, (4_000, 4_009));
    let harness::Served::PreAdmission { reason, .. } = h.serve(query.clone()).await else {
        panic!("with no known allocation a query cannot be admitted");
    };
    assert!(
        matches!(reason, sqd_messages::query_error::Err::TooManyRequests(())),
        "RP-20: expected the no-allocation rejection, got {reason:?}"
    );

    // ...and recover once the registry answers again.
    h.registry.heal();
    h.await_metering_ready().await;
    let served = h.serve(query.clone()).await;
    let (response, _) = served.expect_admitted();
    validators::query_response(response, &query, h.worker_id).assert_none("query after recovery");
    assert!(
        matches!(response.result, Some(query_result::Result::Ok(_))),
        "the worker must serve once the registry recovers, got {:?}",
        response.result
    );
}
