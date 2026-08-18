//! Applies assignments announced by [`super::assignments`].

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use rand::Rng;
use sqd_network_transport::Keypair;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, Instrument};

use super::assignments::{self, AssignmentUpdate, NetworkPair};
use super::schema_bundle::{PreparedSchemaUpdate, SchemaManager};
use super::worker::Worker;
use crate::cli::AssignmentSource;
use crate::metrics;
use crate::storage::datasets_index::AssignmentBlob;
use crate::storage::manager::AssignmentOutcome;

const MAX_PENDING_ASSIGNMENTS: usize = 5;

/// The stream re-announces a pair only when its location moves, so retries cannot wait longer
/// than the poll period without idling past whatever the network last published.
const RETRY_BASE: Duration = Duration::from_secs(1);

/// What one attempt at an announced pair came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    /// Invalid pair; retry only after a different pair is announced.
    Refused,
    /// Transient network or local failure.
    Failed,
}

/// Whether the assignment that just failed is still the one to try.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wait {
    Retry,
    Superseded,
    Stop,
}

pub struct AssignmentApplier {
    worker: Arc<Worker>,
    schema_manager: Arc<SchemaManager>,
    keypair: Keypair,
    assignment_source: AssignmentSource,
    client: reqwest::Client,
    retry_cap: Duration,
}

impl AssignmentApplier {
    pub fn new(
        worker: Arc<Worker>,
        schema_manager: Arc<SchemaManager>,
        keypair: Keypair,
        assignment_source: AssignmentSource,
        client: reqwest::Client,
        retry_cap: Duration,
    ) -> Self {
        Self {
            worker,
            schema_manager,
            keypair,
            assignment_source,
            client,
            retry_cap,
        }
    }

    pub async fn run(
        &self,
        updates: impl Stream<Item = AssignmentUpdate>,
        cancellation_token: CancellationToken,
    ) {
        let mut updates = Box::pin(
            updates
                .take_until(cancellation_token.clone().cancelled_owned())
                .fuse(),
        );
        let mut pending: VecDeque<AssignmentUpdate> = VecDeque::new();
        // The last pair refused; a new location for it changes nothing. One slot, so alternating
        // bad pairs cost one wasted fetch each.
        let mut refused: Option<NetworkPair> = None;
        let mut processing_id: Option<String> = None;
        let mut processing_stalled = false;
        let base = RETRY_BASE.min(self.retry_cap);
        let mut backoff = base;

        'assignments: loop {
            if processing_id.is_none() {
                if let Some(update) = pending.pop_front() {
                    match self.apply(&update).await {
                        ApplyOutcome::Applied => {
                            backoff = base;
                            if self.assignment_source == AssignmentSource::Worker {
                                processing_id = Some(update.id);
                                processing_stalled = false;
                            }
                        }
                        ApplyOutcome::Refused => {
                            refused = Some(update.pair());
                            backoff = base;
                        }
                        ApplyOutcome::Failed => {
                            match self
                                .wait_before_retry(
                                    backoff,
                                    &mut pending,
                                    &refused,
                                    &mut updates,
                                    &cancellation_token,
                                )
                                .await
                            {
                                Wait::Retry => {
                                    backoff = (backoff * 2).min(self.retry_cap);
                                    requeue_pending_assignment(&mut pending, update);
                                }
                                Wait::Superseded => backoff = base,
                                Wait::Stop => break 'assignments,
                            }
                        }
                    }
                    continue;
                }
            }

            match processing_id.clone() {
                Some(id) if processing_stalled => {
                    if !pending.is_empty() {
                        let skipped = keep_only_latest_pending_assignment(&mut pending);
                        warn!(
                            assignment_id = %id,
                            skipped,
                            "Skipping stalled assignment in favor of the most recent one"
                        );
                        processing_id = None;
                        processing_stalled = false;
                        continue;
                    }
                    tokio::select! {
                        update = updates.next() => {
                            let Some(update) = update else {
                                break;
                            };
                            self.absorb(update, &mut pending, &refused);
                        }
                        _ = cancellation_token.cancelled() => break,
                    }
                }
                Some(id) => {
                    tokio::select! {
                        update = updates.next() => {
                            let Some(update) = update else {
                                break;
                            };
                            if self.absorb(update, &mut pending, &refused) {
                                warn!(assignment_id = %id, "Skipping current assignment because assignment queue exceeded {MAX_PENDING_ASSIGNMENTS}");
                                processing_id = None;
                            }
                        }
                        settled = self.worker.wait_until_assignment_settled(&id, cancellation_token.clone()) => {
                            match settled {
                                None => break,
                                Some(AssignmentOutcome::Applied) => {
                                    processing_id = None;
                                }
                                Some(AssignmentOutcome::Stalled) => {
                                    warn!(assignment_id = %id, "Assignment stalled: some chunks exhausted their download attempts");
                                    processing_stalled = true;
                                }
                            }
                        }
                    }
                }
                None => {
                    tokio::select! {
                        update = updates.next() => {
                            let Some(update) = update else {
                                break;
                            };
                            self.absorb(update, &mut pending, &refused);
                        }
                        _ = cancellation_token.cancelled() => break,
                    }
                }
            }
        }
        info!("Assignment processing task finished");
    }

    /// Downloads the pair, validates the assignment against the bundle, installs the schemas,
    /// and registers the assignment — in that order (ADR-21).
    pub async fn apply(&self, update: &AssignmentUpdate) -> ApplyOutcome {
        tracing::debug!("Downloading assignment \"{}\"", update.id);
        let (assignment, prepared_bundle) = match self.download(update).await {
            Ok(downloaded) => downloaded,
            Err(e) => {
                warn!(assignment_id = %update.id, error = %e, "Failed to download assignment");
                return ApplyOutcome::Failed;
            }
        };
        tracing::debug!("Downloaded assignment \"{}\"", update.id);

        let worker = Arc::clone(&self.worker);
        let keypair = self.keypair.clone();
        let id = update.id.clone();
        let prepared_ids = prepared_bundle.as_ref().map(|bundle| bundle.ids());
        let validated = tokio::task::spawn_blocking(move || {
            worker.prepare_assignment(assignment, id, &keypair, |id| {
                prepared_ids.as_ref().is_none_or(|ids| ids.contains(&id))
            })
        })
        .instrument(tracing::info_span!("validate_assignment", id = %update.id))
        .await;
        let prepared_assignment = match validated {
            Ok(Ok(assignment)) => assignment,
            Ok(Err(e)) => {
                return self.refuse(update, e);
            }
            // FlatBuffers verification does not validate peer-id bytes; contain reader panics.
            Err(e) if e.is_panic() => {
                return self.refuse(update, "reading it panicked");
            }
            Err(e) => {
                warn!(assignment_id = %update.id, error = %e, "Validation didn't finish");
                return ApplyOutcome::Failed;
            }
        };

        if let Some(bundle) = prepared_bundle {
            if let Err(e) = bundle.install() {
                metrics::SCHEMA_BUNDLE_FAILURES.inc();
                warn!(assignment_id = %update.id, error = ?e, "Failed to activate schema bundle");
                return ApplyOutcome::Failed;
            }
        }

        // Do not reset the download budget for a bundle-only update.
        if self.worker.registered_assignment_id().as_deref() != Some(update.id.as_str()) {
            let worker = Arc::clone(&self.worker);
            tokio::task::spawn_blocking(move || {
                worker.register_prepared_assignment(prepared_assignment)
            })
            .instrument(tracing::info_span!("set_assignment", id = %update.id))
            .await
            .expect("register_assignment shouldn't panic");
        }

        ApplyOutcome::Applied
    }

    /// Refuses the pair without replacing the active assignment.
    fn refuse(&self, update: &AssignmentUpdate, reason: impl std::fmt::Display) -> ApplyOutcome {
        if self.worker.registered_assignment_id().is_none() {
            metrics::set_status(metrics::WorkerStatus::NotRegistered);
        }
        metrics::ASSIGNMENTS_REFUSED.inc();
        warn!(
            assignment_id = %update.id, reason = %reason,
            "Refused assignment; only a different one can be applied now"
        );
        ApplyOutcome::Refused
    }

    /// The returned [`PreparedSchemaUpdate`] holds the schema store's mutation lock until it is
    /// installed or dropped.
    async fn download(
        &self,
        update: &AssignmentUpdate,
    ) -> anyhow::Result<(AssignmentBlob, Option<PreparedSchemaUpdate>)> {
        if self.assignment_source == AssignmentSource::Legacy {
            return Ok((
                AssignmentBlob::Legacy(
                    assignments::fetch_assignment(&update.fb_url_v1, &self.client).await?,
                ),
                None,
            ));
        }

        let bundle = update.schema_bundle.as_ref().ok_or_else(|| {
            anyhow::anyhow!("network state publishes a worker assignment but no schema bundle")
        })?;
        let prepared = self.schema_manager.prepare(bundle, &self.client).await?;

        Ok((
            AssignmentBlob::Worker(
                assignments::fetch_worker_assignment(&update.fb_url_v1, &self.client).await?,
            ),
            Some(prepared),
        ))
    }

    /// Queues an update unless nothing is outstanding for its pair. A location only matters
    /// while a fetch of that identity can still be rescued by it; under a pair already applied
    /// or refused, acting on one would re-fetch the document on every url rotation.
    fn absorb(
        &self,
        update: AssignmentUpdate,
        pending: &mut VecDeque<AssignmentUpdate>,
        refused: &Option<NetworkPair>,
    ) -> bool {
        let pair = update.pair();
        let applied = NetworkPair {
            assignment_id: self.worker.registered_assignment_id(),
            bundle_hash: self.schema_manager.installed_hash(),
        };
        if pair == applied || refused.as_ref() == Some(&pair) {
            tracing::debug!(
                assignment_id = %update.id,
                "Nothing outstanding for this pair; its location moved and that is all"
            );
            return false;
        }
        push_pending_assignment(pending, update)
    }

    /// Accepts newer assignments while waiting to retry.
    async fn wait_before_retry(
        &self,
        backoff: Duration,
        pending: &mut VecDeque<AssignmentUpdate>,
        refused: &Option<NetworkPair>,
        updates: &mut (impl Stream<Item = AssignmentUpdate> + Unpin),
        cancellation_token: &CancellationToken,
    ) -> Wait {
        if !pending.is_empty() {
            return Wait::Superseded;
        }
        let delay = tokio::time::sleep(jitter(backoff));
        tokio::pin!(delay);
        loop {
            tokio::select! {
                update = updates.next() => {
                    let Some(update) = update else {
                        return Wait::Stop;
                    };
                    self.absorb(update, pending, refused);
                    if !pending.is_empty() {
                        return Wait::Superseded;
                    }
                }
                _ = &mut delay => return Wait::Retry,
                _ = cancellation_token.cancelled() => return Wait::Stop,
            }
        }
    }
}

/// Spreads retries of workers that failed together.
fn jitter(delay: Duration) -> Duration {
    rand::rng().random_range((delay / 2)..=delay)
}

/// Takes an announced update into the queue, and answers whether that overflowed it.
///
/// The stream announces a change of *where* as well as of *what* (IB-40b), because a corrected
/// url is the only thing that can rescue a fetch that keeps failing. Identity still decides what
/// to do — so an update whose pair is already queued replaces it rather than queueing twice, and
/// the queued copy is the one carrying the locations the network published most recently.
fn push_pending_assignment(
    pending: &mut VecDeque<AssignmentUpdate>,
    update: AssignmentUpdate,
) -> bool {
    if let Some(queued) = pending
        .back_mut()
        .filter(|queued| queued.pair() == update.pair())
    {
        tracing::debug!(assignment_id = %update.id, "Already queued; taking the latest locations");
        *queued = update;
        return false;
    }
    if let Some(skipped) = push_pending_item(pending, update) {
        warn!(
            "Skipping {skipped} pending assignments because assignment queue exceeded {MAX_PENDING_ASSIGNMENTS}"
        );
        true
    } else {
        false
    }
}

fn push_pending_item<T>(pending: &mut VecDeque<T>, item: T) -> Option<usize> {
    pending.push_back(item);
    if pending.len() > MAX_PENDING_ASSIGNMENTS {
        Some(keep_only_latest_pending_assignment(pending))
    } else {
        None
    }
}

fn requeue_pending_assignment(
    pending: &mut VecDeque<AssignmentUpdate>,
    update: AssignmentUpdate,
) -> bool {
    if let Some(skipped) = requeue_pending_item(pending, update) {
        warn!(
            "Skipping {skipped} pending assignments because assignment queue exceeded {MAX_PENDING_ASSIGNMENTS}"
        );
        true
    } else {
        false
    }
}

fn requeue_pending_item<T>(pending: &mut VecDeque<T>, item: T) -> Option<usize> {
    pending.push_front(item);
    if pending.len() > MAX_PENDING_ASSIGNMENTS {
        Some(keep_only_latest_pending_assignment(pending))
    } else {
        None
    }
}

fn keep_only_latest_pending_assignment<T>(pending: &mut VecDeque<T>) -> usize {
    let latest = pending
        .pop_back()
        .expect("pending queue was just checked as non-empty");
    let skipped = pending.len();
    pending.clear();
    pending.push_back(latest);
    skipped
}

#[cfg(test)]
mod tests {
    use super::*;

    use sqd_network_transport::PeerId;

    use crate::controller::schema_bundle::test_support::{targz, SCHEMA};
    use crate::controller::schema_bundle::{BundleHash, SchemaBundle};
    use crate::controller::test_support::TestServer;
    use crate::storage::downloader::DownloadConfig;
    use crate::storage::manager::StateManager;
    use crate::types::schema::SchemaId;

    #[path = "assignment_loop_pbt.rs"]
    mod pbt;

    struct Fixture {
        applier: Arc<AssignmentApplier>,
        worker: Arc<Worker>,
        schema_manager: Arc<SchemaManager>,
        peer_id: PeerId,
        stub: TestServer,
        // Dropped last: the store outlives the worker that writes into it.
        _dir: tempfile::TempDir,
    }

    async fn fixture() -> Fixture {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let dir = tempfile::tempdir().unwrap();
        let root = camino::Utf8PathBuf::from_path_buf(dir.path().to_owned()).unwrap();
        let state_manager = StateManager::new(
            root.join("worker"),
            1,
            peer_id,
            DownloadConfig {
                s3_timeout: Duration::from_secs(1),
                s3_read_timeout: Duration::from_secs(1),
                downloads_max_delay: Duration::from_secs(1),
                max_download_attempts: crate::cli::DEFAULT_MAX_DOWNLOAD_ATTEMPTS,
            },
        )
        .await
        .unwrap();
        let schema_manager = Arc::new(SchemaManager::open(root.join("schemas")));
        let worker = Arc::new(Worker::new(state_manager, schema_manager.registry(), 1));
        let applier = Arc::new(AssignmentApplier::new(
            Arc::clone(&worker),
            Arc::clone(&schema_manager),
            keypair,
            AssignmentSource::Worker,
            assignments::new_reqwest_client(Duration::from_secs(5), peer_id),
            // Caps the retry backoff, and with it the base, in test time.
            Duration::from_millis(40),
        ));
        Fixture {
            applier,
            worker,
            schema_manager,
            peer_id,
            stub: TestServer::start().await,
            _dir: dir,
        }
    }

    /// One chunk on write schema 7, assigned to `assigned_to` and nobody else.
    fn worker_assignment(assigned_to: PeerId) -> Vec<u8> {
        let mut builder =
            sqd_assignments::WorkerAssignmentBuilder::new("test-secret").check_continuity(false);
        builder.register_write_schema(7, &["blocks"]).unwrap();
        let mut dataset = builder.new_dataset("s3://test", "https://example.com/");
        dataset
            .new_chunk()
            .id("0000000000/0000000000-0000000010-aaaaaaaa")
            .block_range(0..=10)
            .size(1)
            .write_schema_id(7)
            .worker_indexes(&[0])
            .finish()
            .unwrap();
        dataset.finish().unwrap();
        builder.add_worker(assigned_to, sqd_assignments::WorkerStatus::Ok);
        builder.finish()
    }

    /// The same document with its roster entry's peer id corrupted. A `WorkerId` is a
    /// fixed-size struct, so the flatbuffers verifier only checks its bytes are in bounds — the
    /// document still parses, and the reader panics when it decodes them. Corrupting an input,
    /// never the encoder: the builder only accepts a typed `PeerId`.
    fn unreadable_roster(assigned_to: PeerId) -> Vec<u8> {
        let mut document = worker_assignment(assigned_to);
        let id_bytes = assigned_to.to_bytes();
        let occurrences: Vec<usize> = document
            .windows(id_bytes.len())
            .enumerate()
            .filter(|(_, window)| *window == id_bytes.as_slice())
            .map(|(at, _)| at)
            .collect();
        assert_eq!(
            occurrences.len(),
            1,
            "the roster holds the peer id exactly once"
        );

        let at = occurrences[0];
        document[at] = 0xff;
        document[at + 1] = 0xff;
        assert!(
            PeerId::from_bytes(&document[at..at + id_bytes.len()]).is_err(),
            "the point of the corruption is that these bytes no longer decode"
        );
        document
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn update(id: &str, document_url: String, bundle: (BundleHash, String)) -> AssignmentUpdate {
        AssignmentUpdate {
            id: id.to_owned(),
            fb_url_v1: document_url,
            _effective_from: 0,
            schema_bundle: Some(SchemaBundle {
                hash: bundle.0,
                url: bundle.1,
            }),
        }
    }

    /// The bundle every test publishes: one schema, id 7.
    fn bundle(stub: &TestServer) -> (BundleHash, String) {
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let hash = BundleHash::of(&archive);
        (hash, stub.serve("/bundle.tar.gz", archive, 0))
    }

    async fn await_registered(worker: &Worker, id: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while worker.registered_assignment_id().as_deref() != Some(id) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("assignment {id} never registered"));
    }

    /// Drives the real loop over a fixed set of updates, then leaves it waiting like production
    /// (the network never closes the stream) until the returned token is cancelled.
    fn run_loop(
        applier: &Arc<AssignmentApplier>,
        updates: Vec<AssignmentUpdate>,
    ) -> (CancellationToken, tokio::task::JoinHandle<()>) {
        let token = CancellationToken::new();
        let applier = Arc::clone(applier);
        let stream = futures::stream::iter(updates).chain(futures::stream::pending());
        let running = tokio::spawn({
            let token = token.clone();
            async move { applier.run(stream, token).await }
        });
        (token, running)
    }

    #[tokio::test]
    async fn a_pair_installs_its_schemas_and_registers() {
        let f = fixture().await;
        let bundle = bundle(&f.stub);
        let document = f
            .stub
            .serve("/a1.fb.gz", gzip(&worker_assignment(f.peer_id)), 0);

        let outcome = f
            .applier
            .apply(&update("a1", document, bundle.clone()))
            .await;

        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(f.worker.registered_assignment_id().as_deref(), Some("a1"));
        assert_eq!(
            f.schema_manager.installed_hash(),
            Some(bundle.0),
            "the schemas are in force by the time the assignment is"
        );
        assert!(f.worker.query_schemas().get_by_id(SchemaId::new(7)).is_ok());
    }

    /// Reading a document can panic on bytes no verification checks. Contained where it happens,
    /// it is one more unusable pair (FM-12); re-raised, it forwards to the subsystem tree and the
    /// worker exits over a document the network can simply republish.
    #[tokio::test]
    async fn a_document_whose_roster_cannot_be_read_is_refused() {
        let f = fixture().await;
        let bundle = bundle(&f.stub);
        let document = f
            .stub
            .serve("/a1.fb.gz", gzip(&unreadable_roster(f.peer_id)), 0);
        let refused_before = crate::metrics::ASSIGNMENTS_REFUSED.get();

        let outcome = f.applier.apply(&update("a1", document, bundle)).await;

        assert_eq!(outcome, ApplyOutcome::Refused);
        assert!(f.worker.registered_assignment_id().is_none());
        assert!(
            crate::metrics::ASSIGNMENTS_REFUSED.get() > refused_before,
            "a worker refusing everything must be tellable from one that can't reach the network"
        );
    }

    #[tokio::test]
    async fn a_refused_pair_is_never_fetched_again() {
        let f = fixture().await;
        let bundle = bundle(&f.stub);
        let stranger = Keypair::generate_ed25519().public().to_peer_id();
        // FM-54's steady state: the roster this document was built from doesn't list us.
        let refused = f
            .stub
            .serve("/a1.fb.gz", gzip(&worker_assignment(stranger)), 0);
        let mine = f
            .stub
            .serve("/a2.fb.gz", gzip(&worker_assignment(f.peer_id)), 0);

        let (token, running) = run_loop(
            &f.applier,
            vec![
                update("a1", refused, bundle.clone()),
                update("a2", mine, bundle),
            ],
        );
        await_registered(&f.worker, "a2").await;
        token.cancel();
        running.await.unwrap();

        assert_eq!(
            f.stub.hits("/a1.fb.gz"),
            1,
            "no attempt at the same pair can end differently, so the loop asks once and waits \
             for a different one"
        );
    }

    #[tokio::test]
    async fn a_document_that_fails_to_download_is_tried_again() {
        let f = fixture().await;
        let bundle = bundle(&f.stub);
        let document = f
            .stub
            .serve("/a1.fb.gz", gzip(&worker_assignment(f.peer_id)), 1);

        let (token, running) = run_loop(&f.applier, vec![update("a1", document, bundle)]);
        await_registered(&f.worker, "a1").await;
        token.cancel();
        running.await.unwrap();

        assert_eq!(
            f.stub.hits("/a1.fb.gz"),
            2,
            "the network failing is not the pair's fault: back off and ask again"
        );
    }

    #[tokio::test]
    async fn a_bundle_that_fails_to_download_is_tried_again() {
        let f = fixture().await;
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let bundle = (
            BundleHash::of(&archive),
            f.stub.serve("/bundle-retry.tar.gz", archive, 1),
        );
        let document = f
            .stub
            .serve("/a1.fb.gz", gzip(&worker_assignment(f.peer_id)), 0);

        let (token, running) = run_loop(&f.applier, vec![update("a1", document, bundle)]);
        await_registered(&f.worker, "a1").await;
        token.cancel();
        running.await.unwrap();

        assert_eq!(
            f.stub.hits("/bundle-retry.tar.gz"),
            2,
            "observing a bundle hash must not consume a transiently failed pair"
        );
    }

    #[tokio::test]
    async fn a_truncated_assignment_download_is_superseded_by_a_usable_copy() {
        let f = fixture().await;
        let bundle = bundle(&f.stub);
        let document = f
            .stub
            .serve("/a1.fb.gz", gzip(&worker_assignment(f.peer_id)), 0);
        f.stub.serve(
            "/truncated.fb.gz",
            b"not a complete gzip stream".to_vec(),
            0,
        );

        let (token, running) = run_loop(
            &f.applier,
            vec![
                update("a1", f.stub.url("/truncated.fb.gz"), bundle.clone()),
                update("a1", document, bundle),
            ],
        );
        await_registered(&f.worker, "a1").await;
        token.cancel();
        running.await.unwrap();

        assert_eq!(f.stub.hits("/truncated.fb.gz"), 1);
        assert_eq!(f.stub.hits("/a1.fb.gz"), 1);
    }

    #[tokio::test]
    async fn a_bundle_activation_failure_is_tried_again_without_partial_state() {
        let f = fixture().await;
        let bundle = bundle(&f.stub);
        let document = f
            .stub
            .serve("/a1.fb.gz", gzip(&worker_assignment(f.peer_id)), 0);
        f.schema_manager.fail_next_install();
        let update = update("a1", document, bundle);

        assert_eq!(f.applier.apply(&update).await, ApplyOutcome::Failed);
        assert!(f.worker.registered_assignment_id().is_none());
        assert!(f.schema_manager.installed_hash().is_none());

        let (token, running) = run_loop(&f.applier, vec![update]);
        await_registered(&f.worker, "a1").await;
        token.cancel();
        running.await.unwrap();

        assert_eq!(f.stub.hits("/a1.fb.gz"), 2);
        assert_eq!(f.stub.hits("/bundle.tar.gz"), 2);
    }

    #[tokio::test]
    async fn a_new_bundle_reoffers_a_refused_assignment() {
        let f = fixture().await;
        let document = f
            .stub
            .serve("/a1.fb.gz", gzip(&worker_assignment(f.peer_id)), 0);
        let wrong_archive = targz(&[("9.yaml", SCHEMA.as_bytes())]);
        let wrong_bundle = (
            BundleHash::of(&wrong_archive),
            f.stub.serve("/bundle-wrong.tar.gz", wrong_archive, 0),
        );
        let corrected_bundle = bundle(&f.stub);

        let (token, running) = run_loop(
            &f.applier,
            vec![
                update("a1", document.clone(), wrong_bundle),
                update("a1", document, corrected_bundle),
            ],
        );
        await_registered(&f.worker, "a1").await;
        token.cancel();
        running.await.unwrap();

        assert_eq!(
            f.stub.hits("/a1.fb.gz"),
            2,
            "a bundle change makes a new pair whose assignment must be reconsidered"
        );
    }

    /// A location is not identity, so correcting one leaves the pair unchanged — and the applier
    /// keeps whatever url it was announced with. Without the stream saying the location moved,
    /// an expired or mistyped url is retried forever and no correction can reach the worker.
    #[tokio::test]
    async fn a_corrected_document_url_rescues_a_pair_that_will_not_download() {
        let f = fixture().await;
        let bundle = bundle(&f.stub);
        // Never served: only a different location can get the loop off it.
        let broken = f.stub.url("/broken.fb.gz");
        let corrected = f
            .stub
            .serve("/corrected.fb.gz", gzip(&worker_assignment(f.peer_id)), 0);

        // The same pair twice — same id, same bundle — announced at two locations.
        let (token, running) = run_loop(
            &f.applier,
            vec![
                update("a1", broken, bundle.clone()),
                update("a1", corrected, bundle),
            ],
        );
        await_registered(&f.worker, "a1").await;
        token.cancel();
        running.await.unwrap();

        assert_eq!(
            f.stub.hits("/corrected.fb.gz"),
            1,
            "the pair is unchanged, so only its location could have brought this on"
        );
    }

    /// The other half of the same rule: once a pair has applied, nothing is outstanding for it,
    /// so a rotating url must not re-fetch the document it already holds.
    #[tokio::test]
    async fn a_location_that_moves_under_an_applied_pair_is_not_refetched() {
        let f = fixture().await;
        let bundle = bundle(&f.stub);
        let document = gzip(&worker_assignment(f.peer_id));
        let first = f.stub.serve("/a1.fb.gz", document.clone(), 0);
        let rotated = f.stub.serve("/a1-rotated.fb.gz", document, 0);

        let (token, running) = run_loop(
            &f.applier,
            vec![
                update("a1", first, bundle.clone()),
                update("a1", rotated, bundle),
            ],
        );
        await_registered(&f.worker, "a1").await;
        // The second announcement is absorbed on the same task, so by the time the first has
        // registered the loop has already decided what to do with it.
        token.cancel();
        running.await.unwrap();

        assert_eq!(
            f.stub.hits("/a1-rotated.fb.gz"),
            0,
            "the pair applied, so a moved location is nothing to do"
        );
    }

    #[tokio::test]
    async fn a_newer_assignment_supersedes_one_that_will_not_download() {
        let f = fixture().await;
        let bundle = bundle(&f.stub);
        // Never served: nothing but a newer assignment can get the loop off it.
        let unreachable = f.stub.url("/a1.fb.gz");
        let mine = f
            .stub
            .serve("/a2.fb.gz", gzip(&worker_assignment(f.peer_id)), 0);

        let (token, running) = run_loop(
            &f.applier,
            vec![
                update("a1", unreachable, bundle.clone()),
                update("a2", mine, bundle),
            ],
        );
        await_registered(&f.worker, "a2").await;
        token.cancel();
        running.await.unwrap();

        assert_eq!(
            f.stub.hits("/a1.fb.gz"),
            1,
            "an assignment is absolute, so the newer one subsumes it rather than queueing \
             behind its backoff"
        );
    }

    #[test]
    fn keep_only_latest_pending_assignment_drops_intermediate_assignments() {
        let mut pending: VecDeque<usize> = (1..=4).collect();

        assert_eq!(keep_only_latest_pending_assignment(&mut pending), 3);
        assert_eq!(pending.into_iter().collect::<Vec<_>>(), vec![4]);
    }

    #[test]
    fn pending_assignments_below_threshold_keep_fifo_order() {
        let mut pending: VecDeque<usize> = VecDeque::new();

        for assignment in 1..=MAX_PENDING_ASSIGNMENTS {
            assert_eq!(push_pending_item(&mut pending, assignment), None);
        }
        assert_eq!(
            pending.iter().copied().collect::<Vec<_>>(),
            (1..=MAX_PENDING_ASSIGNMENTS).collect::<Vec<_>>()
        );
    }

    #[test]
    fn failed_assignment_requeue_overflow_keeps_latest_pending_assignment() {
        let mut pending: VecDeque<usize> = (1..=MAX_PENDING_ASSIGNMENTS).collect();

        assert_eq!(
            requeue_pending_item(&mut pending, 0),
            Some(MAX_PENDING_ASSIGNMENTS)
        );
        assert_eq!(pending.into_iter().collect::<Vec<_>>(), vec![5]);
    }
}
