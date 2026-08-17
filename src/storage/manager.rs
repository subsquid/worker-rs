use std::sync::Arc;

use anyhow::{Context, Result};
use camino::Utf8PathBuf as PathBuf;
use futures::StreamExt;
use parking_lot::Mutex;
use sqd_contract_client::PeerId;
use sqd_network_transport::Keypair;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::{
    metrics,
    types::{
        dataset::{self, Dataset},
        schema::SchemaId,
        state::{ChunkRef, ChunkSet},
    },
};

use super::{
    datasets_index::{AssignmentBlob, ChunkSchema, DatasetsIndex},
    downloader::{ChunkDownloader, DownloadConfig},
    layout::{self, DataChunk},
    local_fs::{add_temp_prefix, LocalFs},
    state::{State, UpdateStatus},
    Filesystem,
};

/// How many chunk removals may run at once. Overlapping a few syscall
/// round-trips captures most of the mass-removal speedup; going higher only
/// deepens filesystem-journal bursts on the SSD shared with latency-sensitive
/// query reads (reference worker: 4 vCPU, single SSD). Removals are rare
/// bursts and should yield to queries.
const CONCURRENT_REMOVALS: usize = 4;

pub struct StateManager {
    fs: LocalFs,
    datasets_index: Mutex<Option<DatasetsIndex>>,
    state: Mutex<State>,
    assignment_application: Mutex<AssignmentApplicationStatus>,
    assignment_settled_tx: tokio::sync::watch::Sender<Option<AssignmentSettled>>,
    notify: tokio::sync::Notify,
    concurrent_downloads: usize,
    worker_id: PeerId,
    download_config: DownloadConfig,
}

pub struct QueryChunk<F: FnOnce(PathBuf)> {
    pub path: scopeguard::ScopeGuard<PathBuf, F>,
    pub schema: ChunkSchema,
}

#[derive(Debug)]
pub struct Status {
    pub unavailability_map: Vec<bool>,
    pub stored_bytes: u64,
    pub assignment_id: Option<String>,
    pub last_applied_assignment_id: Option<String>,
}

// Public (but doc-hidden) so `tests/state_pbt.rs` and `tests/state_regression.rs`
// can drive the check-and-mark critical section directly; integration tests
// compile the lib without cfg(test), so narrower visibility can't work.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct AssignmentApplicationStatus {
    pub current_assignment_id: Option<String>,
    // Intentionally remains set while a newer assignment is being applied.
    // This reports the latest fully applied assignment, not the current target.
    pub last_applied_assignment_id: Option<String>,
}

/// Terminal per-assignment verdict published on the settled channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentSettled {
    pub id: String,
    pub outcome: AssignmentOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentOutcome {
    /// All desired chunks are present.
    Applied,
    /// Some chunks exhausted their download attempts and no download work is
    /// left — this assignment can never become fully applied. The assignment
    /// loop should jump over it as soon as a newer assignment is available.
    Stalled,
}

impl StateManager {
    pub async fn new(
        workdir: PathBuf,
        concurrent_downloads: usize,
        worker_id: PeerId,
        download_config: DownloadConfig,
    ) -> Result<Self> {
        let fs = LocalFs::new(workdir);
        remove_temps(&fs)?;
        let existing_chunks = load_state(&fs).await?;
        debug!("Loaded state: {:#?}", existing_chunks);

        let (assignment_settled_tx, _) = tokio::sync::watch::channel(None);

        Ok(Self {
            fs,
            state: Mutex::new(State::new(
                existing_chunks,
                download_config.max_download_attempts,
            )),
            concurrent_downloads,
            worker_id,
            notify: tokio::sync::Notify::new(),
            datasets_index: Mutex::new(None),
            assignment_application: Mutex::new(AssignmentApplicationStatus::default()),
            assignment_settled_tx,
            download_config,
        })
    }

    /// # Panics
    ///
    /// Panics — taking the whole worker down under the fail-fast subsystem
    /// tree — if a chunk removal fails on disk or an assigned chunk's file
    /// URLs cannot be resolved from the current assignment.
    pub async fn run(&self, cancellation_token: CancellationToken) {
        let mut downloader = ChunkDownloader::new(self.worker_id, self.download_config);
        loop {
            self.state.lock().report_status();
            let stored_bytes = get_directory_size(self.fs.root.clone()).await;
            metrics::STORED_BYTES.set(stored_bytes as i64);

            tokio::select! {
                _ = self.notify.notified() => {}
                (chunk, result) = downloader.downloaded() => {
                    match result {
                        Ok(()) => {
                            self.state.lock().complete_download(&chunk, true);
                            metrics::CHUNKS_DOWNLOADED.inc();
                        }
                        Err(e) => {
                            // TODO: skip logging if the download was cancelled
                            warn!("Failed to download chunk '{chunk}':\n{e:?}");
                            self.state.lock().complete_download(&chunk, false);
                            metrics::CHUNKS_FAILED_DOWNLOAD.inc();
                        }
                    }
                }
                _ = cancellation_token.cancelled() => { break }
            }

            for chunk in self.state.lock().get_stale_downloads() {
                downloader.cancel(&chunk);
            }

            let removals = self.state.lock().take_removals();
            // Concurrent, but a barrier for this batch: the downloads below
            // start only after every already-deletable chunk is gone. Draining
            // chunks are not in the batch — they stay on disk until their last
            // query ends and a later pass collects them.
            futures::stream::iter(&removals)
                .for_each_concurrent(CONCURRENT_REMOVALS, |chunk| async move {
                    info!("Removing chunk {chunk}");
                    self.drop_chunk(chunk)
                        .await
                        .unwrap_or_else(|_| panic!("Couldn't remove chunk {chunk}"));
                    metrics::CHUNKS_REMOVED.inc();
                })
                .await;
            self.sweep_removal_ancestors(&removals).await;

            let guard = self.datasets_index.lock();
            let Some(dataset_index) = guard.as_ref() else {
                continue;
            };
            while downloader.download_count() < self.concurrent_downloads {
                if let Some(chunk_ref) = self.state.lock().take_next_download() {
                    info!("Downloading chunk {chunk_ref}");
                    let dst = self.chunk_path(&chunk_ref);
                    let files = dataset_index
                        .list_files(&chunk_ref)
                        .unwrap_or_else(|| panic!("Dataset {} not found", chunk_ref.dataset));
                    let headers = dataset_index.get_headers().clone();
                    downloader.start_download(chunk_ref, dst, files, headers);
                } else {
                    break;
                }
            }
            self.mark_current_assignment_settled_if_ready();
        }
        info!("State manager loop finished");
    }

    /// Subscribe to the "assignment settled" signal: an event fires when the
    /// current assignment becomes fully applied (all chunks present) or stalls
    /// (some chunks exhausted their download attempts). Used to refresh the
    /// reported status promptly instead of waiting for the periodic timer.
    pub fn subscribe_assignment_settled(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<AssignmentSettled>> {
        self.assignment_settled_tx.subscribe()
    }

    #[instrument(skip_all)]
    pub async fn current_status(&self) -> Status {
        let status = self.state.lock().status();
        let stored_bytes = get_directory_size(self.fs.root.clone()).await;
        let Some(assignment_id) = self
            .datasets_index
            .lock()
            .as_ref()
            .map(|index| index.assignment_id().to_owned())
        else {
            debug!("Assignment is not present yet, can't report missing chunks");
            return Status {
                unavailability_map: Default::default(),
                stored_bytes,
                assignment_id: None,
                last_applied_assignment_id: self
                    .assignment_application
                    .lock()
                    .last_applied_assignment_id
                    .clone(),
            };
        };

        let unavailability_map = tokio::task::spawn_blocking(move || {
            let mut unavailability_map = Vec::with_capacity(status.desired.len());
            for chunk_ref in &status.desired {
                unavailability_map.push(!status.available.contains(chunk_ref));
            }
            unavailability_map
        })
        .await
        .unwrap();

        Status {
            unavailability_map,
            stored_bytes,
            assignment_id: Some(assignment_id.to_owned()),
            last_applied_assignment_id: self
                .assignment_application
                .lock()
                .last_applied_assignment_id
                .clone(),
        }
    }

    pub fn set_assignment(
        &self,
        assignment: AssignmentBlob,
        id: impl Into<String>,
        key: &Keypair,
        schema_available: impl Fn(SchemaId) -> bool,
    ) -> bool {
        let datasets_index = match self.prepare_assignment(assignment, id, key, schema_available) {
            Ok(result) => result,
            Err(e) => {
                if self.datasets_index.lock().is_none() {
                    metrics::set_status(metrics::WorkerStatus::NotRegistered);
                }
                error!("Can not get assigned chunks: {e}");
                return false;
            }
        };
        self.set_prepared_assignment(datasets_index);
        true
    }

    pub fn prepare_assignment(
        &self,
        assignment: AssignmentBlob,
        id: impl Into<String>,
        key: &Keypair,
        schema_available: impl Fn(SchemaId) -> bool,
    ) -> anyhow::Result<DatasetsIndex> {
        DatasetsIndex::new(assignment, id, key, schema_available)
    }

    pub fn set_prepared_assignment(&self, datasets_index: DatasetsIndex) {
        let current_assignment_id = datasets_index.assignment_id().to_owned();
        let status = datasets_index.status();
        let chunks: ChunkSet = datasets_index.chunks().keys().cloned().collect();

        let mut index = self.datasets_index.lock();
        // The settled-check correlates current_assignment_id with the chunk
        // state, so both must change under the application lock (lock order:
        // index → application → state); otherwise the check could pair the new
        // desired set with the old id and confirm a never-applied assignment
        // (see `super::regression`).
        let mut assignment_application = self.assignment_application.lock();
        let mut state = self.state.lock();

        match state.set_desired_chunks(chunks) {
            UpdateStatus::Unchanged => {}
            UpdateStatus::Updated => {
                info!("Got new assignment");
                self.notify.notify_one();
            }
        }
        *index = Some(datasets_index);
        {
            assignment_application.current_assignment_id = Some(current_assignment_id);
        }
        drop(state);
        drop(assignment_application);
        drop(index);

        self.mark_current_assignment_settled_if_ready();

        match status {
            sqd_assignments::WorkerStatus::Ok => {
                info!("New assignment applied");
                metrics::set_status(metrics::WorkerStatus::Active);
            }
            sqd_assignments::WorkerStatus::Unreliable => {
                warn!("Worker is considered unreliable");
                metrics::set_status(metrics::WorkerStatus::Unreliable);
            }
            sqd_assignments::WorkerStatus::DeprecatedVersion => {
                warn!("Worker should be updated");
                metrics::set_status(metrics::WorkerStatus::DeprecatedVersion);
            }
            sqd_assignments::WorkerStatus::UnsupportedVersion => {
                warn!("Worker version is unsupported");
                metrics::set_status(metrics::WorkerStatus::UnsupportedVersion);
            }
        }
    }

    /// Waits until the given assignment settles — fully applied or stalled.
    /// Returns `None` when cancelled or the manager is gone.
    pub async fn wait_until_assignment_settled(
        &self,
        assignment_id: &str,
        cancellation_token: CancellationToken,
    ) -> Option<AssignmentOutcome> {
        let mut assignment_settled_rx = self.assignment_settled_tx.subscribe();
        loop {
            if let Some(settled) = assignment_settled_rx.borrow_and_update().as_ref() {
                if settled.id == assignment_id {
                    return Some(settled.outcome);
                }
            }
            tokio::select! {
                changed = assignment_settled_rx.changed() => changed.ok()?,
                _ = cancellation_token.cancelled() => return None,
            }
        }
    }

    pub fn _stop_downloads(&self) {
        match self.state.lock()._stop_downloads() {
            UpdateStatus::Unchanged => {}
            UpdateStatus::Updated => {
                self.notify.notify_one();
            }
        }
    }

    /// Returns the on-disk path to a locally available chunk, or `None` if
    /// the chunk isn't present. The chunk is reference-counted for the
    /// lifetime of the returned guard — it won't be evicted by the state
    /// manager until every guard for it is dropped.
    pub fn get_chunk(
        self: Arc<Self>,
        dataset: Dataset,
        chunk_id: &str,
    ) -> Option<scopeguard::ScopeGuard<PathBuf, impl FnOnce(PathBuf)>> {
        Some(self.get_query_chunk(dataset, chunk_id)?.path)
    }

    /// Lock order: index, then state.
    ///
    /// A query names no version, so it asks for the ingested copy. A chunk the assignment
    /// republished at another version is held under that version and does not answer here.
    pub fn get_query_chunk(
        self: Arc<Self>,
        dataset: Dataset,
        chunk_id: &str,
    ) -> Option<QueryChunk<impl FnOnce(PathBuf)>> {
        let index = self.datasets_index.lock();
        let chunk = self.state.lock().get_and_lock_chunk(ChunkRef::new(
            Arc::new(dataset),
            Arc::from(chunk_id.to_string()),
        ))?;
        let schema = index.as_ref().map_or(ChunkSchema::NoAssignment, |index| {
            index.chunk_schema(&chunk)
        });
        drop(index);

        let path = self.chunk_path(&chunk);
        let guard = scopeguard::guard(path, move |_| {
            if self.state.lock().unlock_chunk(&chunk) {
                // The last query holding a removable chunk finished — wake the
                // state loop to delete it promptly.
                self.notify.notify_one();
            }
        });
        Some(QueryChunk {
            path: guard,
            schema,
        })
    }

    pub fn registered_assignment_id(&self) -> Option<String> {
        self.datasets_index
            .lock()
            .as_ref()
            .map(|index| index.assignment_id().to_owned())
    }

    pub fn assignment_schemas_covered_by(&self, covers: impl Fn(SchemaId) -> bool) -> bool {
        self.datasets_index
            .lock()
            .as_ref()
            .is_some_and(|index| index.schemas_covered_by(covers))
    }

    #[instrument(err, skip(self))]
    async fn drop_chunk(&self, chunk: &ChunkRef) -> Result<()> {
        let path = self.chunk_path(chunk);
        let tmp = add_temp_prefix(&path)?;
        tokio::fs::rename(&path, &tmp).await?;
        tokio::fs::remove_dir_all(tmp).await?;
        Ok(())
    }

    /// Prunes now-empty ancestor dirs after a removal pass. Deduplicated per
    /// parent dir — a mass removal touches each directory once, not once per
    /// chunk — and run off the async thread. Best-effort housekeeping: an
    /// empty leftover dir is harmless, so failures only warn.
    async fn sweep_removal_ancestors(&self, removals: &[ChunkRef]) {
        let mut seen_parents = std::collections::BTreeSet::new();
        let representatives: Vec<PathBuf> = removals
            .iter()
            .filter_map(|chunk| {
                let path = self.chunk_path(chunk);
                let parent = path.parent()?.to_owned();
                seen_parents.insert(parent).then_some(path)
            })
            .collect();
        if representatives.is_empty() {
            return;
        }
        let root = self.fs.root.clone();
        tokio::task::spawn_blocking(move || {
            for path in representatives {
                layout::clean_chunk_ancestors(&path, &root)
                    .unwrap_or_else(|e| warn!("Couldn't clean chunk ancestors: {e:?}"));
            }
        })
        .await
        .expect("ancestor sweep shouldn't panic");
    }

    fn chunk_path(&self, chunk_ref: &ChunkRef) -> PathBuf {
        self.fs
            .root
            .join(dataset::encode_dataset(&chunk_ref.dataset))
            .join(chunk_ref.store_path().as_ref())
    }

    fn mark_current_assignment_settled_if_ready(&self) {
        mark_assignment_settled_if_ready(
            &self.state,
            &self.assignment_application,
            &self.assignment_settled_tx,
        );
    }
}

// Free function so tests can drive the check-and-mark critical section without
// constructing a full `StateManager`.
#[doc(hidden)]
pub fn mark_assignment_settled_if_ready(
    state: &Mutex<State>,
    assignment_application: &Mutex<AssignmentApplicationStatus>,
    assignment_settled_tx: &tokio::sync::watch::Sender<Option<AssignmentSettled>>,
) {
    let mut assignment_application = assignment_application.lock();
    let Some(current_assignment_id) = assignment_application.current_assignment_id.clone() else {
        return;
    };
    if assignment_application.last_applied_assignment_id.as_deref()
        == Some(current_assignment_id.as_str())
    {
        return;
    }
    let (applied, stalled) = {
        let state = state.lock();
        (state.is_fully_applied(), state.is_stalled())
    };
    // send_replace, not send: send stores nothing without subscribers, and the
    // guards above never re-send a verdict — it would be lost forever.
    if applied {
        assignment_application.last_applied_assignment_id = Some(current_assignment_id.clone());
        assignment_settled_tx.send_replace(Some(AssignmentSettled {
            id: current_assignment_id,
            outcome: AssignmentOutcome::Applied,
        }));
    } else if stalled {
        let settled = AssignmentSettled {
            id: current_assignment_id,
            outcome: AssignmentOutcome::Stalled,
        };
        // watch notifies on every send; don't wake subscribers with duplicates
        if assignment_settled_tx.borrow().as_ref() != Some(&settled) {
            assignment_settled_tx.send_replace(Some(settled));
        }
    }
}

#[instrument(skip_all)]
fn remove_temps(fs: &LocalFs) -> Result<()> {
    for entry in glob::glob(fs.root.join("**/temp-*").as_str())? {
        match entry {
            Ok(path) => {
                info!("Removing temp dir '{}'", path.display());
                std::fs::remove_dir_all(&path)
                    .context(format!("Couldn't remove dir '{}'", path.display()))?;
                layout::clean_chunk_ancestors(PathBuf::try_from(path)?, &fs.root)?;
            }
            Err(e) => warn!("Couldn't read dir: {}", e),
        };
    }
    Ok(())
}

#[instrument(skip_all)]
async fn load_state(fs: &LocalFs) -> Result<ChunkSet> {
    tokio::fs::create_dir_all(&fs.root).await?;
    let mut result = ChunkSet::new();
    for dir in fs.ls_root().await? {
        if !dir.is_dir() {
            continue;
        }
        let dirname = dir.file_name().unwrap();
        if let Some(dataset) = dataset::decode_dataset(dirname) {
            let chunks: Vec<(u32, DataChunk)> = layout::read_all_versions(&fs.cd(dirname))
                .await
                .context(format!("Invalid layout in '{dir}'"))?;
            let dataset = Arc::new(dataset);
            for (version, chunk) in chunks {
                result.insert(ChunkRef {
                    dataset: dataset.clone(),
                    chunk: Arc::from(chunk.id),
                    version,
                });
            }
        } else {
            warn!("Invalid dataset in workdir: '{dir}'");
        }
    }
    Ok(result)
}

/// Walks the entire directory tree, so it also accounts for files not tracked by the worker.
/// The walk runs on the blocking thread pool — a full scan of a large workdir may take minutes
/// and must never run directly on the async runtime.
async fn get_directory_size(path: PathBuf) -> u64 {
    tokio::task::spawn_blocking(move || {
        let mut result = 0;
        for entry in walkdir::WalkDir::new(&path) {
            let entry = if let Ok(entry) = entry {
                entry
            } else {
                warn!("Couldn't read dir: {entry:?}");
                continue;
            };
            let metadata = if let Ok(metadata) = entry.metadata() {
                metadata
            } else {
                warn!("Couldn't read metadata: {entry:?}");
                continue;
            };
            if metadata.is_file() {
                result += metadata.len();
            }
        }
        result
    })
    .await
    .expect("Directory size calculation shouldn't panic")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use camino::Utf8PathBuf as PathBuf;
    use sqd_network_transport::Keypair;
    use tokio_util::sync::CancellationToken;

    use crate::storage::datasets_index::AssignmentBlob;

    fn no_schemas_needed(_: crate::types::schema::SchemaId) -> bool {
        unreachable!("a legacy assignment references no write schemas")
    }

    use super::{DownloadConfig, StateManager};
    use crate::types::dataset::encode_dataset;

    /// A valid assignment that assigns no chunks to `peer_id`.
    fn empty_assignment_for(peer_id: sqd_contract_client::PeerId) -> sqd_assignments::Assignment {
        let mut builder = sqd_assignments::AssignmentBuilder::new("test-secret");
        builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok, &[]);
        sqd_assignments::Assignment::from_owned(builder.finish()).unwrap()
    }

    async fn test_manager(
        workdir: PathBuf,
        worker_id: sqd_contract_client::PeerId,
    ) -> StateManager {
        let config = DownloadConfig {
            s3_timeout: Duration::from_secs(1),
            s3_read_timeout: Duration::from_secs(1),
            downloads_max_delay: Duration::from_secs(1),
            max_download_attempts: crate::cli::DEFAULT_MAX_DOWNLOAD_ATTEMPTS,
        };
        StateManager::new(workdir, 1, worker_id, config)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_chunk_the_assignment_does_not_cover_is_not_reported_as_legacy() {
        use std::sync::Arc;

        use super::super::datasets_index::ChunkSchema;
        use super::super::state::State;
        use crate::types::state::{ChunkRef, ChunkSet};

        let keypair = Keypair::generate_ed25519();
        let worker_id = keypair.public().to_peer_id();
        let dir = tempfile::tempdir().unwrap();
        let workdir = PathBuf::from_path_buf(dir.path().to_owned()).unwrap();
        let manager = Arc::new(test_manager(workdir, worker_id).await);

        // A chunk left on disk, the way a restart finds one.
        let dataset = "s3://test".to_owned();
        let chunk = ChunkRef::new(
            Arc::new(dataset.clone()),
            Arc::from("0000000000/0000000000-0000000010-aaaaaaaa"),
        );
        *manager.state.lock() = State::new(
            ChunkSet::from([chunk.clone()]),
            crate::cli::DEFAULT_MAX_DOWNLOAD_ATTEMPTS,
        );

        let held = manager
            .clone()
            .get_query_chunk(dataset.clone(), &chunk.chunk)
            .expect("the chunk is available");
        assert_eq!(
            held.schema,
            ChunkSchema::NoAssignment,
            "nothing has said what this chunk means yet"
        );
        drop(held);

        assert!(manager.set_assignment(
            AssignmentBlob::Legacy(empty_assignment_for(worker_id)),
            "A",
            &keypair,
            no_schemas_needed,
        ));

        let held = manager
            .clone()
            .get_query_chunk(dataset, &chunk.chunk)
            .expect("still available: removal is a later pass");
        assert_eq!(
            held.schema,
            ChunkSchema::Unassigned,
            "assignment A covers no chunks, so this one is not ours to answer for"
        );
    }

    /// A restart has only the store to go on, so the version has to be recoverable from where the
    /// chunk sits — otherwise a rewrite would read back as the copy it replaced.
    #[tokio::test]
    async fn a_restart_recovers_each_chunk_at_the_version_it_is_stored_under() {
        use std::sync::Arc;

        use crate::types::state::ChunkRef;

        let dataset = "s3://test".to_owned();
        let id = "0000000000/0000000000-0000000010-aaaaaaaa";
        let dir = tempfile::tempdir().unwrap();
        let workdir = PathBuf::from_path_buf(dir.path().to_owned()).unwrap();
        let dataset_dir = workdir.join(encode_dataset(&dataset));
        std::fs::create_dir_all(dataset_dir.join(id)).unwrap();
        std::fs::create_dir_all(dataset_dir.join("_v4").join(id)).unwrap();

        let manager =
            test_manager(workdir, Keypair::generate_ed25519().public().to_peer_id()).await;

        let available = manager.state.lock().status().available;
        let versioned = |version| ChunkRef {
            dataset: Arc::new(dataset.clone()),
            chunk: Arc::from(id),
            version,
        };
        assert_eq!(
            available.into_iter().collect::<Vec<_>>(),
            [versioned(0), versioned(4)],
            "one id, two copies — distinct chunks the worker holds independently"
        );
    }

    /// The path is the whole mechanism: two copies of one id must not contend for one directory,
    /// and the ingested one must land where a store written before versions existed has it.
    #[tokio::test]
    async fn a_rewrite_is_stored_beside_the_ingested_copy_not_over_it() {
        use std::sync::Arc;

        use crate::types::state::ChunkRef;

        let dir = tempfile::tempdir().unwrap();
        let workdir = PathBuf::from_path_buf(dir.path().to_owned()).unwrap();
        let manager = test_manager(
            workdir.clone(),
            Keypair::generate_ed25519().public().to_peer_id(),
        )
        .await;
        let dataset = Arc::new("s3://test".to_owned());
        let id = "0000000000/0000000000-0000000010-aaaaaaaa";
        let dataset_dir = workdir.join(encode_dataset(&dataset));

        assert_eq!(
            manager.chunk_path(&ChunkRef::new(dataset.clone(), Arc::from(id))),
            dataset_dir.join(id)
        );
        assert_eq!(
            manager.chunk_path(&ChunkRef {
                dataset,
                chunk: Arc::from(id),
                version: 1,
            }),
            dataset_dir.join("_v1").join(id)
        );
    }

    // Known limitation (not yet fixed): when registering an assignment fails
    // (here: the assignment has no entry for this worker), the worker silently
    // stays on the previous assignment. Combined with the id dedup in the
    // assignments stream (see `assignment_id_is_consumed_before_the_assignment_
    // is_known_to_apply` in controller::assignments), the failed assignment is
    // never offered again — the worker idles on stale data until the network
    // publishes a *different* assignment id.
    #[tokio::test]
    async fn failed_set_assignment_leaves_the_worker_on_the_old_assignment() {
        let keypair = Keypair::generate_ed25519();
        let worker_id = keypair.public().to_peer_id();
        let dir = tempfile::tempdir().unwrap();
        let workdir = PathBuf::from_path_buf(dir.path().to_owned()).unwrap();
        let manager = test_manager(workdir, worker_id).await;

        assert!(manager.set_assignment(
            AssignmentBlob::Legacy(empty_assignment_for(worker_id)),
            "A",
            &keypair,
            no_schemas_needed,
        ));
        assert_eq!(
            manager.current_status().await.assignment_id.as_deref(),
            Some("A")
        );

        // Assignment B doesn't include this worker, so registration fails...
        let other_worker = Keypair::generate_ed25519().public().to_peer_id();
        assert!(!manager.set_assignment(
            AssignmentBlob::Legacy(empty_assignment_for(other_worker)),
            "B",
            &keypair,
            no_schemas_needed,
        ));

        // ...and the worker keeps reporting A, with no retry path for B
        assert_eq!(
            manager.current_status().await.assignment_id.as_deref(),
            Some("A")
        );
    }

    // Known limitation (not yet fixed): a failed chunk removal panics the state
    // loop — and with it the whole worker — instead of being retried or surfaced
    // as an error. Any transient FS hiccup during cleanup is fatal.
    #[tokio::test]
    #[should_panic(expected = "Couldn't remove chunk")]
    async fn removal_failure_panics_the_state_loop() {
        let keypair = Keypair::generate_ed25519();
        let worker_id = keypair.public().to_peer_id();
        let dir = tempfile::tempdir().unwrap();
        let workdir = PathBuf::from_path_buf(dir.path().to_owned()).unwrap();

        // One chunk is on disk at startup, so it is loaded as available
        let chunk_dir = workdir
            .join(encode_dataset("s3://ds"))
            .join("0000000000/0000000000-0000000001-abcdef");
        std::fs::create_dir_all(&chunk_dir).unwrap();

        let manager = test_manager(workdir, worker_id).await;

        // The new assignment holds no chunks, scheduling the local one for removal
        assert!(manager.set_assignment(
            AssignmentBlob::Legacy(empty_assignment_for(worker_id)),
            "A",
            &keypair,
            no_schemas_needed,
        ));

        // Sabotage the removal: the chunk dir vanishes behind the manager's back
        std::fs::remove_dir_all(&chunk_dir).unwrap();

        manager.run(CancellationToken::new()).await;
    }

    #[test]
    fn test_join_glob() {
        // `remove_temps` depends on this behavior
        assert_eq!(PathBuf::from("a/b").join("**/*.c").as_str(), "a/b/**/*.c");
    }

    fn settled(id: &str, outcome: super::AssignmentOutcome) -> Option<super::AssignmentSettled> {
        Some(super::AssignmentSettled {
            id: id.to_owned(),
            outcome,
        })
    }

    // One specific interleaving between `set_assignment` and the state loop's
    // periodic check that must not report a not-yet-applied assignment as
    // applied. Not an exhaustive test of every interleaving.
    #[test]
    fn does_not_misattribute_applied_state_to_a_newer_assignment() {
        use std::sync::Arc;

        use parking_lot::Mutex;

        use super::{
            mark_assignment_settled_if_ready, AssignmentApplicationStatus, AssignmentOutcome, State,
        };
        use crate::cli::DEFAULT_MAX_DOWNLOAD_ATTEMPTS;
        use crate::types::state::{ChunkRef, ChunkSet};

        let chunk = |id: &str| ChunkRef::new(Arc::new("ds".to_owned()), Arc::from(id));
        let chunk_a = chunk("a");
        let chunk_b = chunk("b");

        // Assignment A only needs `chunk_a`, which is already available, and is
        // already marked as applied (steady state before the race begins).
        let state = Mutex::new(State::new(
            [chunk_a.clone()].into_iter().collect(),
            DEFAULT_MAX_DOWNLOAD_ATTEMPTS,
        ));
        let assignment_application = Mutex::new(AssignmentApplicationStatus {
            current_assignment_id: Some("A".to_owned()),
            last_applied_assignment_id: Some("A".to_owned()),
        });
        let (assignment_settled_tx, assignment_settled_rx) =
            tokio::sync::watch::channel(settled("A", AssignmentOutcome::Applied));

        // Step 1: `set_assignment(B)` updates the desired chunks first. B additionally
        // needs `chunk_b`, which isn't available yet, so state stops being fully applied.
        let desired: ChunkSet = [chunk_a.clone(), chunk_b.clone()].into_iter().collect();
        state.lock().set_desired_chunks(desired);
        assert!(!state.lock().is_fully_applied());

        // Step 2: the state loop's own check races in before `current_assignment_id`
        // has been updated to B. `current_assignment_id` is still A, which is already
        // marked applied, so this must be a no-op.
        mark_assignment_settled_if_ready(&state, &assignment_application, &assignment_settled_tx);
        assert_eq!(
            assignment_application.lock().last_applied_assignment_id,
            Some("A".to_owned())
        );

        // Step 3: `set_assignment` now points `current_assignment_id` at B.
        assignment_application.lock().current_assignment_id = Some("B".to_owned());

        // Step 4: `set_assignment`'s own post-update check runs. `chunk_b` is
        // still missing, so B must NOT be marked applied here.
        mark_assignment_settled_if_ready(&state, &assignment_application, &assignment_settled_tx);
        assert_eq!(
            assignment_application.lock().last_applied_assignment_id,
            Some("A".to_owned()),
            "B must not be marked applied while chunk_b is still missing"
        );
        assert_eq!(
            *assignment_settled_rx.borrow(),
            settled("A", AssignmentOutcome::Applied)
        );

        // Step 5: `chunk_b` finishes downloading, so B genuinely becomes fully applied.
        state.lock().take_next_download();
        state.lock().complete_download(&chunk_b, true);
        assert!(state.lock().is_fully_applied());

        // Step 6: only now should B be marked applied.
        mark_assignment_settled_if_ready(&state, &assignment_application, &assignment_settled_tx);
        assert_eq!(
            assignment_application.lock().last_applied_assignment_id,
            Some("B".to_owned())
        );
        assert_eq!(
            *assignment_settled_rx.borrow(),
            settled("B", AssignmentOutcome::Applied)
        );
    }

    // A stalled assignment (chunks exhausted their download attempts) is
    // published as Stalled on the settled channel but never advances
    // last_applied_assignment_id — the heartbeat stays honest.
    #[test]
    fn reports_stalled_assignment_without_marking_it_applied() {
        use std::sync::Arc;

        use parking_lot::Mutex;

        use super::{
            mark_assignment_settled_if_ready, AssignmentApplicationStatus, AssignmentOutcome, State,
        };
        use crate::cli::DEFAULT_MAX_DOWNLOAD_ATTEMPTS;
        use crate::types::state::{ChunkRef, ChunkSet};

        let chunk_a = ChunkRef::new(Arc::new("ds".to_owned()), Arc::from("a"));

        let mut state = State::new(ChunkSet::new(), DEFAULT_MAX_DOWNLOAD_ATTEMPTS);
        state.set_desired_chunks([chunk_a.clone()].into_iter().collect());
        for _ in 0..DEFAULT_MAX_DOWNLOAD_ATTEMPTS {
            assert_eq!(state.take_next_download(), Some(chunk_a.clone()));
            state.complete_download(&chunk_a, false);
        }
        assert!(state.is_stalled());

        let state = Mutex::new(state);
        let assignment_application = Mutex::new(AssignmentApplicationStatus {
            current_assignment_id: Some("A".to_owned()),
            last_applied_assignment_id: None,
        });
        let (assignment_settled_tx, assignment_settled_rx) = tokio::sync::watch::channel(None);

        mark_assignment_settled_if_ready(&state, &assignment_application, &assignment_settled_tx);

        assert_eq!(
            *assignment_settled_rx.borrow(),
            settled("A", AssignmentOutcome::Stalled)
        );
        assert_eq!(
            assignment_application.lock().last_applied_assignment_id,
            None,
            "a stalled assignment must not be reported as applied"
        );
    }
}
