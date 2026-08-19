//! Applies assignments announced by [`super::assignments`].

use std::mem;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use sqd_network_transport::Keypair;
use tokio_util::sync::CancellationToken;
use tower::retry::backoff::{Backoff, ExponentialBackoff, ExponentialBackoffMaker, MakeBackoff};
use tracing::{debug, info, warn, Instrument};

use super::assignments::{self, AssignmentUpdate, NetworkPair};
use super::schema_bundle::{BundleFault, PreparedSchemaUpdate, SchemaManager};
use super::worker::Worker;
use crate::cli::AssignmentSource;
use crate::metrics;
use crate::storage::datasets_index::DatasetsIndex;
use crate::storage::manager::AssignmentOutcome;
use crate::util::backoff;

/// The stream re-announces a pair only when its location moves, so retries cannot wait longer
/// than the poll period without idling past whatever the network last published.
const RETRY_BASE: Duration = Duration::from_secs(1);

/// What one attempt at an announced pair came to.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    /// Invalid pair; retry only after a different pair is announced.
    Refused,
    /// Transient network or local failure.
    Failed,
}

/// Applies announced pairs in order, retries what failed, and refuses what cannot apply.
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
        let mut intake = Intake::new(self.retry_cap);

        loop {
            let event = match &intake.in_flight {
                InFlight::Idle => match intake.pending.take() {
                    Some(update) => Event::Apply(update),
                    None => announcement(&mut updates, &cancellation_token).await,
                },
                InFlight::Stalled(_) if intake.pending.is_some() => Event::SkipStalled,
                InFlight::Stalled(_) => announcement(&mut updates, &cancellation_token).await,
                InFlight::Settling(id) => tokio::select! {
                    update = updates.next() => update.map_or(Event::Stop, Event::Announced),
                    settled = self.worker.wait_until_assignment_settled(id, cancellation_token.clone()) => {
                        settled.map_or(Event::Stop, Event::Settled)
                    }
                },
            };
            match event {
                Event::Stop => break,
                Event::Apply(update) => {
                    let flow = self
                        .apply_next(update, &mut intake, &mut updates, &cancellation_token)
                        .await;
                    if flow.is_break() {
                        break;
                    }
                }
                Event::Announced(update) => intake.absorb(update, &self.applied_pair()),
                Event::Settled(outcome) => intake.settle(outcome),
                Event::SkipStalled => intake.skip_stalled(),
            }
        }
        info!("Assignment processing task finished");
    }

    async fn apply_next(
        &self,
        update: AssignmentUpdate,
        intake: &mut Intake,
        updates: &mut (impl Stream<Item = AssignmentUpdate> + Unpin),
        cancellation_token: &CancellationToken,
    ) -> ControlFlow<()> {
        match self.apply(&update).await {
            ApplyOutcome::Applied => {
                intake.reset_backoff();
                if self.assignment_source == AssignmentSource::Worker {
                    intake.in_flight = InFlight::Settling(update.id);
                }
            }
            ApplyOutcome::Refused => {
                intake.refused = Some(update.pair());
                intake.reset_backoff();
            }
            ApplyOutcome::Failed => {
                match self
                    .wait_before_retry(intake, updates, cancellation_token)
                    .await
                {
                    Wait::Retry => intake.pending = Some(update),
                    Wait::Superseded => intake.reset_backoff(),
                    Wait::Stop => return ControlFlow::Break(()),
                }
            }
        }
        ControlFlow::Continue(())
    }

    /// Downloads the pair, validates the assignment against the bundle, installs the schemas,
    /// and registers the assignment — in that order (ADR-21).
    pub async fn apply(&self, update: &AssignmentUpdate) -> ApplyOutcome {
        debug!(assignment_id = %update.id, "Downloading assignment");
        let (document, bundle) = match self.download(update).await {
            Ok(fetched) => fetched,
            Err(unfetched) => return self.unfetched(update, unfetched),
        };
        debug!(assignment_id = %update.id, "Downloaded assignment");

        let index = match self.validate(update, document, bundle.as_ref()).await {
            Ok(index) => index,
            Err(outcome) => return outcome,
        };
        if let Some(bundle) = bundle {
            if let Err(e) = bundle.install().await {
                metrics::SCHEMA_BUNDLE_FAILURES.inc();
                warn!(assignment_id = %update.id, error = %chain(&e), "Failed to activate schema bundle");
                return ApplyOutcome::Failed;
            }
        }
        self.register(update, index).await;
        ApplyOutcome::Applied
    }

    /// The pair's bytes: the document as fetched, and the bundle prepared for install. Fails on
    /// transport only; what the bytes are is judged with the rest of validation. The returned
    /// [`PreparedSchemaUpdate`] holds the schema store's mutation lock until it is installed or
    /// dropped.
    async fn download(
        &self,
        update: &AssignmentUpdate,
    ) -> Result<(Vec<u8>, Option<PreparedSchemaUpdate>), Unfetched> {
        let bundle = match self.assignment_source {
            AssignmentSource::Legacy => None,
            AssignmentSource::Worker => {
                let bundle = update.schema_bundle.as_ref().ok_or_else(|| {
                    // The stream already withholds a half-published pair, so this is defensive;
                    // treat it as the network's to correct rather than the pair's fault.
                    Unfetched::Bundle(BundleFault::Transient(anyhow::anyhow!(
                        "network state publishes a worker assignment but no schema bundle"
                    )))
                })?;
                Some(
                    self.schema_manager
                        .prepare(bundle, &self.client)
                        .await
                        .map_err(Unfetched::Bundle)?,
                )
            }
        };
        let document = assignments::fetch_document(&update.fb_url_v1, &self.client)
            .await
            .map_err(Unfetched::Document)?;
        Ok((document, bundle))
    }

    /// Decoding is a verdict on the document like the rest of validation (FM-12), so it runs
    /// here, off the runtime thread, and a failure is a refusal.
    async fn validate(
        &self,
        update: &AssignmentUpdate,
        document: Vec<u8>,
        bundle: Option<&PreparedSchemaUpdate>,
    ) -> Result<DatasetsIndex, ApplyOutcome> {
        let keypair = self.keypair.clone();
        let id = update.id.clone();
        let assignment_source = self.assignment_source;
        let bundle_ids = bundle.map(PreparedSchemaUpdate::ids);
        let validated = tokio::task::spawn_blocking(move || {
            let assignment = assignments::decode_document(assignment_source, document)?;
            DatasetsIndex::new(assignment, id, &keypair, |id| {
                bundle_ids.as_ref().is_none_or(|ids| ids.contains(&id))
            })
        })
        .instrument(tracing::info_span!("validate_assignment", id = %update.id))
        .await;
        match validated {
            Ok(Ok(index)) => Ok(index),
            Ok(Err(e)) => Err(self.refuse(update, e)),
            // FlatBuffers verification does not validate peer-id bytes; contain reader panics.
            Err(e) if e.is_panic() => Err(self.refuse(update, "reading it panicked")),
            Err(e) => {
                warn!(assignment_id = %update.id, error = %chain(&e.into()), "Validation didn't finish");
                Err(ApplyOutcome::Failed)
            }
        }
    }

    async fn register(&self, update: &AssignmentUpdate, index: DatasetsIndex) {
        // A bundle-only update keeps the assignment's download budget.
        if self.worker.registered_assignment_id().as_deref() == Some(update.id.as_str()) {
            return;
        }
        let worker = Arc::clone(&self.worker);
        tokio::task::spawn_blocking(move || worker.register_prepared_assignment(index))
            .instrument(tracing::info_span!("set_assignment", id = %update.id))
            .await
            .expect("registering a validated assignment has no panic path of its own");
    }

    /// A pair half of which could not be fetched. A bundle fault the hash already vouched for is
    /// a verdict on the pair like a document that will not decode (FM-12), so it is refused
    /// rather than asked for again; everything else is the network or this worker's disk, and a
    /// retry — or a corrected location — can still rescue it.
    fn unfetched(&self, update: &AssignmentUpdate, unfetched: Unfetched) -> ApplyOutcome {
        match unfetched {
            Unfetched::Bundle(fault) if fault.is_permanent() => {
                self.refuse(update, chain(&fault.into_error()))
            }
            Unfetched::Bundle(fault) => {
                warn!(assignment_id = %update.id, error = %chain(&fault.into_error()), "Failed to prepare schema bundle");
                ApplyOutcome::Failed
            }
            Unfetched::Document(e) => {
                warn!(assignment_id = %update.id, error = %chain(&e), "Failed to download assignment");
                ApplyOutcome::Failed
            }
        }
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

    /// Accepts newer announcements while waiting to retry. Any announcement ends the wait: it
    /// differs from the failing update by pair or by location, so it is either the rescue to try
    /// next or the network moving off the failing pair — in both cases the failing update is not
    /// what to retry.
    async fn wait_before_retry(
        &self,
        intake: &mut Intake,
        updates: &mut (impl Stream<Item = AssignmentUpdate> + Unpin),
        cancellation_token: &CancellationToken,
    ) -> Wait {
        if intake.pending.is_some() {
            return Wait::Superseded;
        }
        let delay = intake.backoff.next_backoff();
        tokio::select! {
            update = updates.next() => {
                let Some(update) = update else {
                    return Wait::Stop;
                };
                intake.absorb(update, &self.applied_pair());
                Wait::Superseded
            }
            _ = delay => Wait::Retry,
            _ = cancellation_token.cancelled() => Wait::Stop,
        }
    }

    fn applied_pair(&self) -> NetworkPair {
        NetworkPair {
            assignment_id: self.worker.registered_assignment_id(),
            bundle_hash: self.schema_manager.installed_hash(),
        }
    }
}

async fn announcement(
    updates: &mut (impl Stream<Item = AssignmentUpdate> + Unpin),
    cancellation_token: &CancellationToken,
) -> Event {
    tokio::select! {
        update = updates.next() => update.map_or(Event::Stop, Event::Announced),
        _ = cancellation_token.cancelled() => Event::Stop,
    }
}

/// The whole cause chain on one line; `Display` alone shows only the outermost context.
fn chain(error: &anyhow::Error) -> String {
    format!("{error:#}")
}

enum Event {
    Apply(AssignmentUpdate),
    Announced(AssignmentUpdate),
    Settled(AssignmentOutcome),
    SkipStalled,
    Stop,
}

/// Whether the update that just failed is still the one to retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wait {
    Retry,
    /// The network announced something else meanwhile. Whatever that queued is next; if it
    /// queued nothing — the pair in force again — the failed pair is simply retracted.
    Superseded,
    Stop,
}

/// Which half of the pair could not be fetched. The bundle is prepared first, so a bundle
/// failure must not read as a document one.
enum Unfetched {
    Bundle(BundleFault),
    Document(anyhow::Error),
}

/// The assignment registered last, as the loop relates to it. Legacy mode never leaves `Idle`:
/// it applies each update as it arrives.
#[derive(Debug, PartialEq, Eq)]
enum InFlight {
    Idle,
    /// Registered; the next update waits for its verdict (strictly in-order application).
    Settling(String),
    /// Stalled, which is terminal; only a newer update moves things.
    Stalled(String),
}

/// The loop's bookkeeping: the newest pair waiting, what was refused, what is in flight, how
/// long the next retry waits.
struct Intake {
    /// Only the newest announcement waits: a pair the network has moved past is never started,
    /// and the one settling is never interrupted for it (WP-4).
    pending: Option<AssignmentUpdate>,
    /// The last pair refused; a new location for it changes nothing. One slot, so alternating
    /// bad pairs cost one wasted fetch each.
    refused: Option<NetworkPair>,
    in_flight: InFlight,
    backoff_maker: ExponentialBackoffMaker,
    backoff: ExponentialBackoff,
}

impl Intake {
    fn new(retry_cap: Duration) -> Self {
        let mut backoff_maker = backoff::exponential(RETRY_BASE, retry_cap);
        let backoff = backoff_maker.make_backoff();
        Self {
            pending: None,
            refused: None,
            in_flight: InFlight::Idle,
            backoff_maker,
            backoff,
        }
    }

    fn reset_backoff(&mut self) {
        self.backoff = self.backoff_maker.make_backoff();
    }

    /// Keeps an update as the one to apply next unless nothing is outstanding for its pair. A
    /// location only matters while a fetch of that identity can still be rescued by it; under a
    /// pair already applied or refused, acting on one would re-fetch the document on every url
    /// rotation. Such an announcement is still the network's latest word, though: whatever was
    /// waiting behind it is no longer published, so it is dropped rather than applied later.
    fn absorb(&mut self, update: AssignmentUpdate, applied: &NetworkPair) {
        let pair = update.pair();
        if pair == *applied || self.refused.as_ref() == Some(&pair) {
            if let Some(retracted) = self.pending.take() {
                info!(
                    assignment_id = %update.id,
                    retracted = %retracted.id,
                    "Network is back on a pair with nothing outstanding; dropping what was waiting"
                );
            } else {
                debug!(
                    assignment_id = %update.id,
                    "Nothing outstanding for this pair; its location moved and that is all"
                );
            }
            return;
        }
        if let Some(superseded) = self.pending.replace(update) {
            debug!(
                assignment_id = %superseded.id,
                "A newer announcement takes its place in the queue"
            );
        }
    }

    fn settle(&mut self, outcome: AssignmentOutcome) {
        let InFlight::Settling(id) = mem::replace(&mut self.in_flight, InFlight::Idle) else {
            return;
        };
        if outcome == AssignmentOutcome::Stalled {
            warn!(assignment_id = %id, "Assignment stalled: some chunks exhausted their download attempts");
            self.in_flight = InFlight::Stalled(id);
        }
    }

    fn skip_stalled(&mut self) {
        let InFlight::Stalled(id) = mem::replace(&mut self.in_flight, InFlight::Idle) else {
            return;
        };
        warn!(
            assignment_id = %id,
            "Skipping stalled assignment in favor of the newer one"
        );
    }
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
        fixture_with(AssignmentSource::Worker).await
    }

    async fn fixture_with(assignment_source: AssignmentSource) -> Fixture {
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
            assignment_source,
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

    /// A legacy document listing `assigned_to` with no chunks: enough to register.
    fn legacy_assignment(assigned_to: PeerId) -> Vec<u8> {
        let mut builder = sqd_assignments::AssignmentBuilder::new("test-secret");
        builder.add_worker(assigned_to, sqd_assignments::WorkerStatus::Ok, &[]);
        builder.finish()
    }

    fn legacy_update(id: &str, document_url: String) -> AssignmentUpdate {
        AssignmentUpdate {
            id: id.to_owned(),
            fb_url_v1: document_url,
            _effective_from: 0,
            schema_bundle: None,
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

    /// A document that fails verification is a verdict on the bytes, not on the network (FM-12):
    /// refused once and left alone, rather than fetched again — bundle and all — every retry
    /// period until the network publishes something else.
    #[tokio::test]
    async fn a_document_that_will_not_decode_is_refused_not_retried() {
        let f = fixture().await;
        let bundle = bundle(&f.stub);
        // A complete gzip whose payload is no worker assignment at all.
        let document = f
            .stub
            .serve("/garbage.fb.gz", gzip(b"not a worker assignment"), 0);
        let mine = f
            .stub
            .serve("/a2.fb.gz", gzip(&worker_assignment(f.peer_id)), 0);
        let refused_before = crate::metrics::ASSIGNMENTS_REFUSED.get();

        assert_eq!(
            f.applier
                .apply(&update("a1", document.clone(), bundle.clone()))
                .await,
            ApplyOutcome::Refused
        );
        assert!(f.worker.registered_assignment_id().is_none());
        assert!(
            crate::metrics::ASSIGNMENTS_REFUSED.get() > refused_before,
            "an unusable document is what OB-18 counts"
        );

        // In the loop: asked once, then only a different pair moves things.
        let (token, running) = run_loop(
            &f.applier,
            vec![
                update("a1", document, bundle.clone()),
                update("a2", mine, bundle),
            ],
        );
        await_registered(&f.worker, "a2").await;
        token.cancel();
        running.await.unwrap();
        assert_eq!(
            f.stub.hits("/garbage.fb.gz"),
            2,
            "one direct attempt above and one in the loop; no retry in between"
        );
    }

    /// The bundle half of FM-12. Past the hash check the bytes are the ones the network vouched
    /// for, so a bundle that carries no schemas cannot come out differently however often it is
    /// asked for: refused once, like a document that will not decode, rather than re-downloaded
    /// every retry period for as long as the scheduler keeps publishing it.
    #[tokio::test]
    async fn a_bundle_with_nothing_to_load_is_refused_not_retried() {
        let f = fixture().await;
        // A well-formed archive, hashed as announced, that simply holds no <id>.yaml entries.
        let empty = targz(&[("readme.txt", b"no schemas here")]);
        let unusable = (
            BundleHash::of(&empty),
            f.stub.serve("/bundle-empty.tar.gz", empty, 0),
        );
        let document = f
            .stub
            .serve("/a1.fb.gz", gzip(&worker_assignment(f.peer_id)), 0);
        let mine = f
            .stub
            .serve("/a2.fb.gz", gzip(&worker_assignment(f.peer_id)), 0);
        let refused_before = crate::metrics::ASSIGNMENTS_REFUSED.get();

        assert_eq!(
            f.applier
                .apply(&update("a1", document.clone(), unusable.clone()))
                .await,
            ApplyOutcome::Refused
        );
        assert!(f.worker.registered_assignment_id().is_none());
        assert!(
            crate::metrics::ASSIGNMENTS_REFUSED.get() > refused_before,
            "a pair the worker cannot use is what OB-18 counts, whichever half is at fault"
        );

        let (token, running) = run_loop(
            &f.applier,
            vec![
                update("a1", document, unusable),
                update("a2", mine, bundle(&f.stub)),
            ],
        );
        await_registered(&f.worker, "a2").await;
        token.cancel();
        running.await.unwrap();

        assert_eq!(
            f.stub.hits("/bundle-empty.tar.gz"),
            2,
            "one direct attempt above and one in the loop; no retry in between"
        );
        assert_eq!(
            f.stub.hits("/a1.fb.gz"),
            0,
            "the bundle is prepared first, so a refused one never costs a document fetch"
        );
    }

    /// The other side of the line. A hash mismatch says this location served the wrong bytes,
    /// which is precisely what a corrected url fixes — and a refusal is keyed on the pair, so it
    /// would drop that correction. It has to stay retryable.
    #[tokio::test]
    async fn a_bundle_whose_hash_does_not_match_is_retried_not_refused() {
        let f = fixture().await;
        let archive = targz(&[("7.yaml", SCHEMA.as_bytes())]);
        let mislabelled = (
            BundleHash::of(b"not what this url serves"),
            f.stub.serve("/bundle-mismatch.tar.gz", archive, 0),
        );
        let document = f
            .stub
            .serve("/a1.fb.gz", gzip(&worker_assignment(f.peer_id)), 0);

        let outcome = f.applier.apply(&update("a1", document, mislabelled)).await;

        // The outcome is the whole assertion: `ASSIGNMENTS_REFUSED` is process-global, so a
        // test cannot watch it stay put while the rest of the binary runs beside it.
        assert_eq!(
            outcome,
            ApplyOutcome::Failed,
            "nothing about the pair was refused; only where it was fetched from"
        );
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

    /// The network can take a pair back: X in force, Y published and failing to fetch, then X
    /// again. That announcement is dropped as nothing-to-do for X — but it is also the network's
    /// last word on Y, so Y must not be retried, let alone applied once its documents turn up.
    /// Legacy mode, where nothing waits on a settle, so the retry itself is what is exercised.
    #[tokio::test]
    async fn a_pair_the_network_retracts_is_not_retried() {
        let f = fixture_with(AssignmentSource::Legacy).await;
        let document = gzip(&legacy_assignment(f.peer_id));
        let x = f.stub.serve("/x.fb.gz", document.clone(), 0);
        // Fails once, so a retry would fetch it — and, being valid, apply it.
        let y = f.stub.serve("/y.fb.gz", document.clone(), 1);
        let x_again = f.stub.serve("/x-again.fb.gz", document, 0);

        let (token, running) = run_loop(
            &f.applier,
            vec![
                legacy_update("x", x),
                legacy_update("y", y),
                legacy_update("x", x_again),
            ],
        );
        await_registered(&f.worker, "x").await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while f.stub.hits("/y.fb.gz") == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("y is attempted once");
        // Several retry periods (the cap is 40 ms): a retry that was going to happen has by now.
        tokio::time::sleep(Duration::from_millis(400)).await;
        token.cancel();
        running.await.unwrap();

        assert_eq!(
            f.stub.hits("/y.fb.gz"),
            1,
            "the network took y back; it is not retried"
        );
        assert_eq!(
            f.worker.registered_assignment_id().as_deref(),
            Some("x"),
            "the network is on x, and so is the worker"
        );
        assert_eq!(
            f.stub.hits("/x-again.fb.gz"),
            0,
            "x is in force; its new location is nothing to do"
        );
    }

    /// The same word, heard while a pair is queued rather than mid-retry: what was queued behind
    /// the pair in force — or behind the refused one — is no longer published, so it goes.
    #[tokio::test]
    async fn an_announcement_of_a_pair_with_nothing_outstanding_retracts_the_queue() {
        let f = fixture().await;
        let bundle = bundle(&f.stub);
        let document = gzip(&worker_assignment(f.peer_id));
        let x = f.stub.serve("/x.fb.gz", document.clone(), 0);
        let x_moved = f.stub.serve("/x-moved.fb.gz", document, 0);
        assert_eq!(
            f.applier.apply(&update("x", x, bundle.clone())).await,
            ApplyOutcome::Applied
        );

        let mut intake = Intake::new(Duration::from_millis(40));
        intake.pending = Some(update("y", f.stub.url("/y.fb.gz"), bundle.clone()));
        intake.absorb(
            update("x", x_moved.clone(), bundle.clone()),
            &f.applier.applied_pair(),
        );
        assert!(
            intake.pending.is_none(),
            "y was waiting behind x; the network is back on x"
        );

        intake.refused = Some(NetworkPair {
            assignment_id: Some("r".to_owned()),
            bundle_hash: Some(bundle.0),
        });
        intake.pending = Some(update("y", f.stub.url("/y.fb.gz"), bundle.clone()));
        intake.absorb(
            update("r", f.stub.url("/r-moved.fb.gz"), bundle),
            &f.applier.applied_pair(),
        );
        assert!(
            intake.pending.is_none(),
            "y was waiting behind the refused r; the network is back on r"
        );
        assert_eq!(
            f.stub.hits("/x-moved.fb.gz"),
            0,
            "nothing was fetched for any of it"
        );
    }

    fn queued(id: &str) -> AssignmentUpdate {
        legacy_update(id, format!("http://example.test/{id}.fb.gz"))
    }

    fn waiting(intake: &Intake) -> Option<(&str, &str)> {
        intake
            .pending
            .as_ref()
            .map(|u| (u.id.as_str(), u.fb_url_v1.as_str()))
    }

    #[test]
    fn only_the_newest_announcement_waits() {
        let mut intake = Intake::new(Duration::from_secs(1));
        let nothing_applied = NetworkPair::default();

        for n in 1..=3 {
            intake.absorb(queued(&n.to_string()), &nothing_applied);
        }

        assert_eq!(
            waiting(&intake),
            Some(("3", "http://example.test/3.fb.gz")),
            "the pairs the network moved past are never started"
        );
    }

    #[test]
    fn a_waiting_pair_announced_again_takes_the_latest_location() {
        let mut intake = Intake::new(Duration::from_secs(1));
        let nothing_applied = NetworkPair::default();
        intake.absorb(queued("1"), &nothing_applied);

        intake.absorb(
            legacy_update("1", "http://example.test/1-moved.fb.gz".to_owned()),
            &nothing_applied,
        );

        assert_eq!(
            waiting(&intake),
            Some(("1", "http://example.test/1-moved.fb.gz"))
        );
    }

    /// Strict in-order application: a pair that is settling is never interrupted by what the
    /// network announces meanwhile, however much of it there is (WP-4). The fixture never settles
    /// anything, so the applier is known to be waiting for the whole test.
    #[tokio::test]
    async fn announcements_piling_up_while_a_pair_settles_do_not_interrupt_it() {
        let f = fixture().await;
        let bundle = bundle(&f.stub);
        let document = gzip(&worker_assignment(f.peer_id));
        let x = f.stub.serve("/x.fb.gz", document.clone(), 0);
        let newer: Vec<String> = (1..=7)
            .map(|n| f.stub.serve(&format!("/y{n}.fb.gz"), document.clone(), 0))
            .collect();

        let mut updates = vec![update("x", x, bundle.clone())];
        updates.extend(
            newer
                .iter()
                .enumerate()
                .map(|(n, url)| update(&format!("y{}", n + 1), url.clone(), bundle.clone())),
        );
        let (token, running) = run_loop(&f.applier, updates);
        await_registered(&f.worker, "x").await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        token.cancel();
        running.await.unwrap();

        assert_eq!(
            f.worker.registered_assignment_id().as_deref(),
            Some("x"),
            "x is still settling; nothing announced since was applied"
        );
        for n in 1..=7 {
            assert_eq!(
                f.stub.hits(&format!("/y{n}.fb.gz")),
                0,
                "y{n} was not fetched"
            );
        }
    }
}
