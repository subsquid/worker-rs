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

/// WP-2 / DEF-13: the worker fetches the chunks its own slice names and no others.
///
/// Guards the harness too — a placement that named this worker regardless of `assigned` would
/// leave both the flag and `AssignmentFault::NoChunksForWorker` inert.
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

    // The slice is one chunk wide, though the document describes two.
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

/// The GAP-3 fault input: a worker in the roster holding no chunks at all.
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

/// IB-40b/41b/44b: the file list is derived from the assignment's inline schema roster, and the
/// query executes against the chunk's `write_schema_id`, not the query's dataset type.
#[tokio::test(flavor = "multi_thread")]
async fn worker_format_assign_download_query() {
    use harness::scheduler::Format;
    use harness::Config;

    let mut h = Harness::with_config(Config {
        format: Format::Worker,
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

    // The document lists no files; these names come from the roster.
    for (name, _) in &chunk.files {
        let served = h
            .origin
            .served_bytes(&chunk.id, name)
            .unwrap_or_else(|| panic!("IB-41b: {name} was never derived from the roster"));
        let on_disk = std::fs::read(h.chunk_dir(&chunk.id).join(name))
            .unwrap_or_else(|e| panic!("committed chunk is missing {name}: {e}"));
        assert_eq!(on_disk, served, "INV-13: {name} differs from origin bytes");
    }

    // The schemas came from the bundle, not the CDN manifest.
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
}

/// IB-41b: a chunk at a non-zero `version` is a batch job's rewrite, and its files live under the
/// prefix the dataset registers for that version — not at the dataset root. The chunk is hosted
/// only there, so a worker that ignored `version` would fetch nothing.
#[tokio::test(flavor = "multi_thread")]
async fn worker_format_fetches_a_republished_chunk_from_its_generation() {
    use harness::scheduler::{Format, Scheduler};
    use harness::Config;

    let mut h = Harness::with_config(Config {
        format: Format::Worker,
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
        let on_disk = std::fs::read(h.chunk_dir(&chunk.id).join(name))
            .unwrap_or_else(|e| panic!("committed chunk is missing {name}: {e}"));
        assert_eq!(on_disk, served, "INV-13: {name} differs from origin bytes");
    }
    assert_eq!(
        h.origin.fetch_count(&chunk.id, "blocks.parquet"),
        0,
        "the ingested copy is not where a republished chunk lives"
    );
}

/// FM-53b: without the schema bundle the worker would hold chunks it cannot answer queries about.
#[tokio::test(flavor = "multi_thread")]
async fn worker_format_assignment_waits_for_its_schema_bundle() {
    use harness::scheduler::{AssignmentFault, Format};
    use harness::Config;

    let mut h = Harness::with_config(Config {
        format: Format::Worker,
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

/// FM-53c / ADR-21: the pair is one state. A bundle that does not cover the assignment it is
/// published with is the scheduler's invariant breaking, and the worker refuses the assignment
/// rather than covering for it — even where its own store could have answered.
#[tokio::test(flavor = "multi_thread")]
async fn worker_format_refuses_an_assignment_its_bundle_does_not_cover() {
    use harness::scheduler::{AssignmentFault, Format};
    use harness::Config;

    let mut h = Harness::with_config(Config {
        format: Format::Worker,
        ..Config::default()
    })
    .await;

    // First the covering pair, so the worker holds the assignment's schema in its store.
    let first = corpus::chunk(6_000, 6_009, 1);
    let placement = h.host_chunk(&first);
    let applied = h.publish_and_apply("assignment-1", &[placement]).await;
    h.await_all_chunks_available().await;
    assert_eq!(h.status().await.assignment_id, applied.id);

    // Then a pair that disagrees: the bundle carries a schema the assignment never references.
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
