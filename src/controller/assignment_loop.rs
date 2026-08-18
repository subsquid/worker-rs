//! Applying the assignments the network announces.
//!
//! [`super::assignments`] answers "what is new?"; this module answers "what do we do about it?",
//! including whether a failed attempt deserves another. Keeping the second question here is the
//! point: only this side knows *why* an attempt failed, so only this side can tell a refusal
//! that no retry can change from a fetch that might succeed next time.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use rand::Rng;
use sqd_network_transport::Keypair;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, Instrument};

use super::assignments::{self, AssignmentUpdate};
use super::schema_bundle::{PreparedSchemaUpdate, SchemaManager};
use super::worker::Worker;
use crate::cli::AssignmentSource;
use crate::metrics;
use crate::storage::datasets_index::AssignmentBlob;
use crate::storage::manager::AssignmentOutcome;

/// How many announced assignments may queue up behind the one being applied.
const MAX_PENDING_ASSIGNMENTS: usize = 5;

/// P-ASSIGN-RETRY-BASE for the document stage; doubles per attempt, jittered, capped at the
/// poll period. The cap matters more than usual here: the stream does not re-announce a pair it
/// has already offered, so this backoff is the only thing that brings the worker back to a
/// failed fetch, and stretching it past one poll would idle the worker longer than the network
/// is quiet.
const RETRY_BASE: Duration = Duration::from_secs(1);

/// What one attempt at an announced pair came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// Registered. Under `--assignment-source worker` this is now the assignment being
    /// converged to.
    Applied,
    /// Refused for a reason no later attempt can change: the document carries no entry for this
    /// worker, a chunk's write schema has no roster, the bundle doesn't carry a schema the
    /// document uses, or the credentials won't decrypt. Each is a property of the pair itself,
    /// so the worker keeps the assignment in force and waits for a *different* pair rather than
    /// fetching this one again (WP-2, FM-12).
    Refused,
    /// The attempt failed on something outside the pair — the network, or the local disk. Worth
    /// another attempt.
    Failed,
}

/// Whether the assignment that just failed is still the one to try.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wait {
    /// The backoff elapsed; put it back at the front of the queue.
    Retry,
    /// A newer assignment is queued, which subsumes this one — drop it.
    Superseded,
    /// The update stream ended or the subsystem was cancelled.
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
        let mut processing_id: Option<String> = None;
        // The assignment in `processing_id` stalled: some of its chunks exhausted
        // their download attempts, so it will never become fully applied.
        let mut processing_stalled = false;
        // The base cannot outrun the ceiling it grows towards.
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
                        // Nothing to wait for and nothing to redo: the next *different* pair is
                        // the only thing that can move this on.
                        ApplyOutcome::Refused => backoff = base,
                        ApplyOutcome::Failed => {
                            match self
                                .wait_before_retry(
                                    backoff,
                                    &mut pending,
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
                // The stalled assignment can never be fully applied. Jump to
                // the most recent pending assignment as soon as one exists;
                // until then only watch the update stream — the stall is
                // terminal, so there is nothing to wait for.
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
                            push_pending_assignment(&mut pending, update);
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
                            if push_pending_assignment(&mut pending, update) {
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
                            push_pending_assignment(&mut pending, update);
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
            // Reading the document can panic where no verification helps: a roster entry's peer
            // id is a fixed-size struct, so the flatbuffers verifier only checks its bytes are
            // in bounds, and the reader panics when they don't decode. `spawn_blocking` has
            // already contained it, so this is one more unusable document (FM-12) — re-raising
            // it here would forward the panic to the subsystem tree and take the worker down
            // over a document the network can republish.
            Err(e) if e.is_panic() => {
                return self.refuse(update, "reading it panicked");
            }
            // Blocking tasks are not cancelled by dropping the handle, so this is the runtime
            // going away underneath us — nothing about the pair.
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

        // A bundle-only publication still revalidates the complete pair, but re-registering the
        // assignment would reset its exhausted download budget even though its document did not
        // change. Assignment ids are content identities, so the active id makes this a no-op.
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

    /// Refuses the pair, keeping whatever is in force. No later attempt at *this* pair can end
    /// differently, so the counter is what tells a worker starved of usable documents from one
    /// that cannot reach the network at all (OB-18).
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

    /// Backs off before another attempt, still taking in updates. A newer assignment ends the
    /// wait at once and supersedes the one that failed: assignments are absolute, so the newer
    /// one subsumes it, and sitting out the backoff first is head-of-line blocking.
    async fn wait_before_retry(
        &self,
        backoff: Duration,
        pending: &mut VecDeque<AssignmentUpdate>,
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
                    push_pending_assignment(pending, update);
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

fn push_pending_assignment(
    pending: &mut VecDeque<AssignmentUpdate>,
    update: AssignmentUpdate,
) -> bool {
    if pending
        .back()
        .is_some_and(|queued| queued.pair() == update.pair())
    {
        tracing::debug!(assignment_id = %update.id, "Already queued");
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

    use std::collections::HashMap;
    use std::sync::Mutex;

    use proptest::prelude::*;
    use sqd_network_transport::PeerId;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::controller::schema_bundle::test_support::{targz, SCHEMA};
    use crate::controller::schema_bundle::{BundleHash, SchemaBundle};
    use crate::storage::downloader::DownloadConfig;
    use crate::storage::manager::StateManager;
    use crate::types::schema::SchemaId;

    /// Serves fixed bodies by path and counts hits, so a test can assert what the loop fetched
    /// and how often. A path may 404 a set number of times first, standing in for a flaky origin.
    struct Stub {
        base: String,
        routes: Arc<Mutex<HashMap<String, (Vec<u8>, usize)>>>,
        hits: Arc<Mutex<HashMap<String, usize>>>,
    }

    impl Stub {
        async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let routes: Arc<Mutex<HashMap<String, (Vec<u8>, usize)>>> = Default::default();
            let hits: Arc<Mutex<HashMap<String, usize>>> = Default::default();
            let (served, counted) = (Arc::clone(&routes), Arc::clone(&hits));
            tokio::spawn(async move {
                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        return;
                    };
                    let (served, counted) = (Arc::clone(&served), Arc::clone(&counted));
                    tokio::spawn(async move {
                        let mut buf = [0u8; 4096];
                        let read = socket.read(&mut buf).await.unwrap_or(0);
                        let request = String::from_utf8_lossy(&buf[..read]).into_owned();
                        let path = request.split_whitespace().nth(1).unwrap_or("/").to_owned();
                        *counted.lock().unwrap().entry(path.clone()).or_default() += 1;
                        let body = match served.lock().unwrap().get_mut(&path) {
                            Some((_, failures)) if *failures > 0 => {
                                *failures -= 1;
                                None
                            }
                            Some((body, _)) => Some(body.clone()),
                            None => None,
                        };
                        let response = match body {
                            Some(body) => {
                                let mut response = format!(
                                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                                    body.len()
                                )
                                .into_bytes();
                                response.extend_from_slice(&body);
                                response
                            }
                            None => b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                                .to_vec(),
                        };
                        let _ = socket.write_all(&response).await;
                    });
                }
            });
            Self { base, routes, hits }
        }

        fn serve(&self, path: &str, body: Vec<u8>, failures: usize) -> String {
            self.routes
                .lock()
                .unwrap()
                .insert(path.to_owned(), (body, failures));
            self.url(path)
        }

        fn url(&self, path: &str) -> String {
            format!("{}{path}", self.base)
        }

        fn hits(&self, path: &str) -> usize {
            self.hits.lock().unwrap().get(path).copied().unwrap_or(0)
        }
    }

    struct Fixture {
        applier: Arc<AssignmentApplier>,
        worker: Arc<Worker>,
        schema_manager: Arc<SchemaManager>,
        peer_id: PeerId,
        stub: Stub,
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
            stub: Stub::start().await,
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
    fn bundle(stub: &Stub) -> (BundleHash, String) {
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

    #[derive(Clone, Copy, Debug)]
    enum PublishedFault {
        Good,
        Refused,
        Transient,
    }

    #[derive(Clone, Copy, Debug)]
    struct PublishedPair {
        assignment: u8,
        bundle: u8,
        assignment_fault: PublishedFault,
        bundle_fault: PublishedFault,
    }

    fn published_pair() -> impl Strategy<Value = PublishedPair> {
        let fault = || {
            prop_oneof![
                4 => Just(PublishedFault::Good),
                2 => Just(PublishedFault::Refused),
                2 => Just(PublishedFault::Transient),
            ]
        };
        (0u8..4, 0u8..4, fault(), fault()).prop_map(
            |(assignment, bundle, assignment_fault, bundle_fault)| PublishedPair {
                assignment,
                bundle,
                assignment_fault,
                bundle_fault,
            },
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn random_pair_failures_never_publish_partial_state_or_stall(
            history in prop::collection::vec(published_pair(), 1..16),
        ) {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(check_pair_history(history));
        }
    }

    async fn check_pair_history(history: Vec<PublishedPair>) {
        let f = fixture().await;
        let stranger = Keypair::generate_ed25519().public().to_peer_id();
        let mut expected_assignment = None;
        let mut expected_bundle = None;

        for (step, pair) in history.into_iter().enumerate() {
            let assignment_id = format!("a{}-{:?}", pair.assignment, pair.assignment_fault);
            let assigned_to = match pair.assignment_fault {
                PublishedFault::Refused => stranger,
                PublishedFault::Good | PublishedFault::Transient => f.peer_id,
            };
            let document_path = format!("/pbt-{step}.fb.gz");
            let document = f.stub.serve(
                &document_path,
                gzip(&worker_assignment(assigned_to)),
                usize::from(matches!(pair.assignment_fault, PublishedFault::Transient)),
            );

            let extra_schema = format!("{}.yaml", 100 + u16::from(pair.bundle));
            let archive = match pair.bundle_fault {
                PublishedFault::Refused => targz(&[(extra_schema.as_str(), SCHEMA.as_bytes())]),
                PublishedFault::Good | PublishedFault::Transient => targz(&[
                    ("7.yaml", SCHEMA.as_bytes()),
                    (extra_schema.as_str(), SCHEMA.as_bytes()),
                ]),
            };
            let bundle_hash = BundleHash::of(&archive);
            let bundle_path = format!("/pbt-{step}.tar.gz");
            let bundle = (
                bundle_hash,
                f.stub.serve(
                    &bundle_path,
                    archive,
                    usize::from(matches!(pair.bundle_fault, PublishedFault::Transient)),
                ),
            );
            let update = update(&assignment_id, document, bundle);

            // Bundle and assignment fetches can each fail once, so three attempts are enough
            // for every transient pair. Each attempt is bounded: a generated history must never
            // wedge the applier on one half of the pair.
            let mut outcome = ApplyOutcome::Failed;
            for _ in 0..3 {
                outcome = tokio::time::timeout(Duration::from_secs(2), f.applier.apply(&update))
                    .await
                    .expect("pair application stalled");
                if outcome != ApplyOutcome::Failed {
                    break;
                }
            }

            let applicable = !matches!(pair.assignment_fault, PublishedFault::Refused)
                && !matches!(pair.bundle_fault, PublishedFault::Refused);
            if applicable {
                assert_eq!(outcome, ApplyOutcome::Applied);
                expected_assignment = Some(assignment_id);
                expected_bundle = Some(bundle_hash);
            } else {
                assert_eq!(outcome, ApplyOutcome::Refused);
            }

            assert_eq!(
                f.worker.registered_assignment_id(),
                expected_assignment,
                "a refused or incomplete pair changed the active assignment at step {step}"
            );
            assert_eq!(
                f.schema_manager.installed_hash(),
                expected_bundle,
                "a refused or incomplete pair changed the active bundle at step {step}"
            );
        }
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
