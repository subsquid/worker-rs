//! End-to-end conformance tests. Replay failures with the reported `SQD_CONFORMANCE_SEED`.

mod harness;

use harness::{corpus, validators, Harness};
use sqd_messages::query_result;

#[tokio::test(flavor = "multi_thread")]
async fn smoke_assign_download_query_verify_logs() {
    let mut h = Harness::start().await;

    let chunk = corpus::chunk(1_000, 1_009, 1);
    let placement = h.host_chunk(&chunk);
    let assignment = h.publish_and_apply("assignment-1", &[placement]).await;

    let status = h.status().await;
    assert_eq!(status.assignment_id, assignment.id, "RP-21: applied id");
    validators::status(&status, 1).assert_none("status after application");

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

    // The serving lag withholds a fresh log record.
    let withheld = h.logs_page(None).await;
    assert!(
        withheld.queries_executed.is_empty(),
        "RP-22: records younger than P-LOGS-LAG must not be served"
    );

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

/// The worker fetches only chunks assigned to its slice (WP-2, DEF-13).
#[tokio::test(flavor = "multi_thread")]
async fn only_assigned_chunks_are_fetched() {
    let mut h = Harness::start().await;

    let mine = corpus::chunk(1_000, 1_009, 1);
    let theirs = corpus::chunk(1_010, 1_019, 1);
    let placements = [
        h.host_chunk_for(&mine, true),
        h.host_chunk_for(&theirs, false),
    ];

    h.publish_and_apply("assignment-1", &placements).await;
    h.await_all_chunks_available().await;

    let status = h.status().await;
    validators::status(&status, 1).assert_none("status with an unassigned chunk present");

    // HC-2's ledger is the proof: the unassigned chunk was reachable and never requested.
    for (name, _) in &theirs.files {
        assert_eq!(
            h.origin.fetch_count(&theirs.id, name),
            0,
            "WP-2: fetched {name} of a chunk this worker was not assigned"
        );
    }
    assert!(
        h.origin.fetch_count(&mine.id, "blocks.parquet") > 0,
        "WP-11: the assigned chunk should have been fetched"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_slice_assigns_nothing() {
    use harness::scheduler::AssignmentFault;

    let mut h = Harness::start().await;

    let chunk = corpus::chunk(3_000, 3_009, 1);
    let placement = h.host_chunk(&chunk);
    h.publish(
        "assignment-1",
        &[placement],
        AssignmentFault::NoChunksForWorker,
    );
    assert!(
        h.poll_and_apply().await,
        "the document itself is well-formed"
    );

    let status = h.status().await;
    assert_eq!(
        status
            .missing_chunks
            .as_ref()
            .unwrap()
            .to_vec()
            .unwrap()
            .len(),
        0,
        "DEF-13: an empty slice must leave the availability map empty"
    );
    assert_eq!(
        h.origin.fetch_count(&chunk.id, "blocks.parquet"),
        0,
        "no chunk is assigned, so nothing may be fetched"
    );
}

/// A chunk published only as a rewrite is fetched from its generation prefix and stored under its
/// version (IB-41b); a query names the copy it wants, and version 0 — never assigned — is not
/// found (IB-13).
#[tokio::test(flavor = "multi_thread")]
async fn worker_format_fresh_rewrite_is_fetched_from_its_generation_and_served_by_version() {
    use harness::scheduler::Scheduler;
    use harness::Config;
    use sqd_assignments::AssignmentType;

    let mut h = Harness::with_config(Config {
        assignment_type: AssignmentType::Split,
        ..Config::default()
    })
    .await;

    let chunk = corpus::chunk(5_000, 5_009, 1);
    let placement = h.host_republished_chunk(&chunk, 1);
    h.publish_and_apply("assignment-1", &[placement]).await;
    h.await_all_chunks_available().await;

    let generation = Scheduler::generation_prefix(1);
    for (name, _) in &chunk.files {
        let served = h
            .origin
            .served_bytes_in(&generation, &chunk.id, name)
            .unwrap_or_else(|| panic!("IB-41b: {name} was not fetched from the generation"));
        let on_disk = std::fs::read(h.chunk_dir_at_version(&chunk.id, 1).join(name))
            .unwrap_or_else(|e| panic!("committed chunk is missing {name}: {e}"));
        assert_eq!(on_disk, served, "INV-13: {name} differs from origin bytes");
    }
    assert_eq!(
        h.origin.fetch_count(&chunk.id, "blocks.parquet"),
        0,
        "the ingested copy is not where a republished chunk lives"
    );
    assert!(
        !h.chunk_dir(&chunk.id).exists(),
        "a rewrite is stored under its version, not where the ingested copy goes"
    );

    let query = h.all_blocks_query_at_version(&chunk.id, (5_000, 5_009), 1);
    let served = h.serve(query.clone()).await;
    let (response, _) = served.expect_admitted();
    validators::query_response(response, &query, h.worker_id).assert_none("query response");
    let query_result::Result::Ok(ok) = response.result.as_ref().unwrap() else {
        panic!("expected a successful result, got {:?}", response.result);
    };
    assert_eq!(ok.last_block, 5_009, "the rewrite answered the query");

    let unversioned = h.all_blocks_query(&chunk.id, (5_000, 5_009));
    let served = h.serve(unversioned).await;
    let (response, _) = served.expect_admitted();
    let query_result::Result::Err(err) = response.result.as_ref().unwrap() else {
        panic!("expected an error, got {:?}", response.result);
    };
    assert!(
        matches!(err.err, Some(sqd_messages::query_error::Err::NotFound(_))),
        "version 0 was never assigned, so it is not here: {:?}",
        err.err
    );
}

/// A document whose dataset address cannot be used contradicts itself, so it is refused whole
/// (FM-12): the previous assignment stays in force, nothing is fetched, and only a different
/// document moves things.
#[tokio::test(flavor = "multi_thread")]
async fn a_document_with_an_unusable_address_is_refused() {
    use harness::scheduler::AssignmentFault;

    let mut h = Harness::start().await;

    let chunk = corpus::chunk(7_000, 7_009, 1);
    let refused_before = sqd_worker::metrics::ASSIGNMENTS_REFUSED.get();
    h.publish(
        "assignment-1",
        &[h.host_chunk(&chunk)],
        AssignmentFault::UnparseableFileUrl,
    );
    assert!(
        !h.poll_and_apply().await,
        "the document names a dataset address the worker cannot use, so it is inapplicable"
    );
    assert!(
        sqd_worker::metrics::ASSIGNMENTS_REFUSED.get() > refused_before,
        "OB-18: a refused document is what separates a starved worker from a quiet network"
    );
    assert!(
        h.status().await.assignment_id.is_empty(),
        "nothing was applied, so no assignment id is reported"
    );
    assert_eq!(
        h.origin.fetch_count(&chunk.id, "blocks.parquet"),
        0,
        "no chunk is fetched for a document that never applied"
    );

    h.publish_and_apply("assignment-2", &[h.host_chunk(&chunk)])
        .await;
    h.await_all_chunks_available().await;
}

/// A chunk whose origin will not serve it exhausts its download budget and stalls the
/// assignment; the next assignment restores the budget — including one naming the same slice,
/// since registering an assignment always wakes reconciliation (WP-13).
#[tokio::test(flavor = "multi_thread")]
async fn a_chunk_the_origin_will_not_serve_is_retried_by_the_next_assignment() {
    use harness::stub::Fault;
    use sqd_worker::storage::manager::AssignmentOutcome;

    let mut h = Harness::start().await;

    let chunk = corpus::chunk(7_000, 7_009, 1);
    let placement = h.host_chunk(&chunk);
    h.origin
        .inject(&chunk.id, "blocks.parquet", Fault::Status(404));
    h.publish_and_apply("assignment-1", std::slice::from_ref(&placement))
        .await;
    let settled = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        h.worker.wait_until_assignment_settled(
            "assignment-1",
            tokio_util::sync::CancellationToken::new(),
        ),
    )
    .await
    .expect("the assignment settles once the budget is spent");
    assert_eq!(
        settled,
        Some(AssignmentOutcome::Stalled),
        "the origin never served the chunk, so the assignment stalls"
    );
    let attempts = h.origin.fetch_count(&chunk.id, "blocks.parquet");
    assert!(
        attempts > 1,
        "a fetch failure is retried within the budget: {attempts}"
    );

    // The origin comes back; the scheduler republishes the same slice under a new id.
    h.origin.clear_faults();
    h.publish_and_apply("assignment-2", &[placement]).await;
    h.await_all_chunks_available().await;
}

/// The ingested copy is fetched from the roster, reported and queried; a rewrite is then fetched
/// under its version and the superseded copy removed.
#[tokio::test(flavor = "multi_thread")]
async fn worker_format_v0_chunk_is_served_then_superseded_by_a_rewrite() {
    use harness::scheduler::Scheduler;
    use harness::Config;
    use sqd_assignments::AssignmentType;

    let mut h = Harness::with_config(Config {
        assignment_type: AssignmentType::Split,
        ..Config::default()
    })
    .await;

    let chunk = corpus::chunk(2_000, 2_009, 1);
    let placement = h.host_chunk(&chunk);
    let assignment = h.publish_and_apply("assignment-1", &[placement]).await;

    let status = h.status().await;
    assert_eq!(status.assignment_id, assignment.id, "RP-21: applied id");
    validators::status(&status, 1).assert_none("status after application");

    h.await_all_chunks_available().await;

    for (name, _) in &chunk.files {
        let served = h
            .origin
            .served_bytes(&chunk.id, name)
            .unwrap_or_else(|| panic!("IB-41b: {name} was never derived from the roster"));
        let on_disk = std::fs::read(h.chunk_dir(&chunk.id).join(name))
            .unwrap_or_else(|e| panic!("committed chunk is missing {name}: {e}"));
        assert_eq!(on_disk, served, "INV-13: {name} differs from origin bytes");
    }

    let query = h.all_blocks_query(&chunk.id, (2_000, 2_009));
    let served = h.serve(query.clone()).await;
    let (response, _) = served.expect_admitted();
    validators::query_response(response, &query, h.worker_id).assert_none("query response");

    let query_result::Result::Ok(ok) = response.result.as_ref().unwrap() else {
        panic!("expected a successful result, got {:?}", response.result);
    };
    let blocks: Vec<u64> = std::str::from_utf8(&ok.data)
        .unwrap()
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).unwrap()["header"]["number"]
                .as_u64()
                .unwrap()
        })
        .collect();
    assert_eq!(blocks, (2_000..=2_009).collect::<Vec<_>>(), "RP-12");

    h.publish_and_apply("assignment-2", &[h.host_republished_chunk(&chunk, 1)])
        .await;
    h.await_all_chunks_available().await;

    let generation = Scheduler::generation_prefix(1);
    for (name, _) in &chunk.files {
        let served = h
            .origin
            .served_bytes_in(&generation, &chunk.id, name)
            .unwrap_or_else(|| panic!("the rewrite's {name} was never fetched"));
        let on_disk = std::fs::read(h.chunk_dir_at_version(&chunk.id, 1).join(name))
            .unwrap_or_else(|e| panic!("committed rewrite is missing {name}: {e}"));
        assert_eq!(on_disk, served, "INV-13: {name} differs from origin bytes");
    }
    h.await_condition("the superseded copy is removed", || async {
        !h.chunk_dir(&chunk.id).exists()
    })
    .await;
}

/// An assignment is not applied without its schema bundle (FM-53b).
#[tokio::test(flavor = "multi_thread")]
async fn worker_format_assignment_waits_for_its_schema_bundle() {
    use harness::scheduler::AssignmentFault;
    use harness::Config;
    use sqd_assignments::AssignmentType;

    let mut h = Harness::with_config(Config {
        assignment_type: AssignmentType::Split,
        ..Config::default()
    })
    .await;

    let chunk = corpus::chunk(4_000, 4_009, 1);
    let placement = h.host_chunk(&chunk);

    h.scheduler.break_schema_bundle(503);
    h.publish("assignment-1", &[placement], AssignmentFault::None);
    assert!(
        !h.poll_and_apply().await,
        "FM-53b: the assignment must not apply without its schema bundle"
    );
    assert!(
        h.status().await.assignment_id.is_empty(),
        "nothing was applied, so no assignment id is reported"
    );
    assert_eq!(
        h.origin.fetch_count(&chunk.id, "blocks.parquet"),
        0,
        "no chunk may be fetched for an assignment that never applied"
    );
}

/// A bundle that does not cover its assignment causes the pair to be refused (FM-53c).
#[tokio::test(flavor = "multi_thread")]
async fn worker_format_refuses_an_assignment_its_bundle_does_not_cover() {
    use harness::scheduler::AssignmentFault;
    use harness::Config;
    use sqd_assignments::AssignmentType;

    let mut h = Harness::with_config(Config {
        assignment_type: AssignmentType::Split,
        ..Config::default()
    })
    .await;

    let first = corpus::chunk(6_000, 6_009, 1);
    let placement = h.host_chunk(&first);
    let applied = h.publish_and_apply("assignment-1", &[placement]).await;
    h.await_all_chunks_available().await;
    assert_eq!(h.status().await.assignment_id, applied.id);

    let second = corpus::chunk(7_000, 7_009, 1);
    let placement = h.host_chunk(&second);
    h.scheduler.publish_bundle_missing_the_assignment_schema();
    h.publish("assignment-2", &[placement], AssignmentFault::None);

    assert!(
        !h.poll_and_apply().await,
        "the bundle does not cover the assignment, so the pair must not apply"
    );
    assert_eq!(
        h.status().await.assignment_id,
        applied.id,
        "the previous assignment stays in force"
    );
    assert_eq!(
        h.origin.fetch_count(&second.id, "blocks.parquet"),
        0,
        "and nothing is downloaded for an assignment that never applied"
    );
}
