use std::sync::Arc;

use anyhow::{Context, Result};
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
use parking_lot::Mutex;
use sqd_contract_client::PeerId;
use sqd_network_transport::Keypair;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::{
    cli::Args,
    metrics,
    types::{
        dataset::{self, Dataset},
        state::{ChunkRef, ChunkSet},
    },
};

use super::{
    datasets_index::DatasetsIndex,
    downloader::ChunkDownloader,
    layout::{self, DataChunk},
    local_fs::{add_temp_prefix, LocalFs},
    state::{State, UpdateStatus},
    Filesystem,
};

pub struct StateManager {
    fs: LocalFs,
    datasets_index: Mutex<Option<DatasetsIndex>>,
    state: Mutex<State>,
    #[cfg(feature = "mvcc-chunks")]
    assignment_application: Mutex<AssignmentApplicationStatus>,
    #[cfg(feature = "mvcc-chunks")]
    assignment_applied_tx: tokio::sync::watch::Sender<Option<String>>,
    notify: tokio::sync::Notify,
    concurrent_downloads: usize,
    worker_id: PeerId,
    args: Args,
}

pub struct Status {
    pub unavailability_map: Vec<bool>,
    pub stored_bytes: u64,
    pub assignment_id: Option<String>,
    #[cfg(feature = "mvcc-chunks")]
    pub last_applied_assignment_id: Option<String>,
}

#[cfg(feature = "mvcc-chunks")]
#[derive(Debug, Default)]
struct AssignmentApplicationStatus {
    current_assignment_id: Option<String>,
    // Intentionally remains set while a newer assignment is being applied.
    // This reports the latest fully applied assignment, not the current target.
    last_applied_assignment_id: Option<String>,
}

impl StateManager {
    pub async fn new(
        workdir: PathBuf,
        concurrent_downloads: usize,
        worker_id: PeerId,
        args: Args,
    ) -> Result<Self> {
        let fs = LocalFs::new(workdir);
        remove_temps(&fs)?;
        let existing_chunks = load_state(&fs).await?;
        debug!("Loaded state: {:#?}", existing_chunks);

        #[cfg(feature = "mvcc-chunks")]
        let (assignment_applied_tx, _) = tokio::sync::watch::channel(None);

        Ok(Self {
            fs,
            state: Mutex::new(State::new(existing_chunks)),
            concurrent_downloads,
            worker_id,
            notify: tokio::sync::Notify::new(),
            datasets_index: Mutex::new(None),
            #[cfg(feature = "mvcc-chunks")]
            assignment_application: Mutex::new(AssignmentApplicationStatus::default()),
            #[cfg(feature = "mvcc-chunks")]
            assignment_applied_tx,
            args,
        })
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        let mut downloader = ChunkDownloader::new(self.worker_id, self.args.clone());
        loop {
            self.state.lock().report_status();
            let stored_bytes = get_directory_size(&self.fs.root);
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
            for chunk in removals {
                info!("Removing chunk {chunk}");
                self.drop_chunk(&chunk)
                    .await
                    .unwrap_or_else(|_| panic!("Couldn't remove chunk {chunk}"));
                metrics::CHUNKS_REMOVED.inc();
            }

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
            #[cfg(feature = "mvcc-chunks")]
            {
                let fully_applied = self.state.lock().is_fully_applied();
                if fully_applied {
                    self.mark_current_assignment_applied();
                }
            }
        }
        info!("State manager loop finished");
    }

    #[instrument(skip_all)]
    pub async fn current_status(&self) -> Status {
        let status = self.state.lock().status();
        let stored_bytes = tokio::task::spawn_blocking({
            let root = self.fs.root.clone();
            move || get_directory_size(&root)
        })
        .await
        .unwrap();
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
                #[cfg(feature = "mvcc-chunks")]
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
            #[cfg(feature = "mvcc-chunks")]
            last_applied_assignment_id: self
                .assignment_application
                .lock()
                .last_applied_assignment_id
                .clone(),
        }
    }

    pub fn set_assignment(
        &self,
        assignment: sqd_assignments::Assignment,
        id: impl Into<String>,
        key: &Keypair,
    ) -> bool {
        let id = id.into();
        #[cfg(feature = "mvcc-chunks")]
        let current_assignment_id = id.clone();
        let datasets_index = match DatasetsIndex::new(assignment, id, key) {
            Ok(result) => result,
            Err(e) => {
                metrics::set_status(metrics::WorkerStatus::NotRegistered);
                error!("Can not get assigned chunks: {e}");
                return false;
            }
        };
        let status = datasets_index.status();
        let chunks: ChunkSet = datasets_index.chunks().keys().cloned().collect();

        let mut index = self.datasets_index.lock();
        let mut state = self.state.lock();

        match state.set_desired_chunks(chunks) {
            UpdateStatus::Unchanged => {}
            UpdateStatus::Updated => {
                info!("Got new assignment");
                self.notify.notify_one();
            }
        }
        #[cfg(feature = "mvcc-chunks")]
        let fully_applied = state.is_fully_applied();
        *index = Some(datasets_index);
        drop(state);
        drop(index);

        #[cfg(feature = "mvcc-chunks")]
        {
            self.assignment_application.lock().current_assignment_id = Some(current_assignment_id);
        }

        #[cfg(feature = "mvcc-chunks")]
        if fully_applied {
            self.mark_current_assignment_applied();
        }

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
        true
    }

    #[cfg(feature = "mvcc-chunks")]
    pub async fn wait_until_assignment_applied(
        &self,
        assignment_id: &str,
        cancellation_token: CancellationToken,
    ) -> bool {
        let mut assignment_applied_rx = self.assignment_applied_tx.subscribe();
        loop {
            if assignment_applied_rx.borrow().as_deref() == Some(assignment_id) {
                return true;
            }
            tokio::select! {
                changed = assignment_applied_rx.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                }
                _ = cancellation_token.cancelled() => return false,
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
        let chunk = self
            .state
            .lock()
            .get_and_lock_chunk(Arc::new(dataset), Arc::from(chunk_id.to_string()))?;
        let path = self.chunk_path(&chunk);
        let guard = scopeguard::guard(path, move |_| self.state.lock().unlock_chunk(&chunk));
        Some(guard)
    }

    #[instrument(err, skip(self))]
    async fn drop_chunk(&self, chunk: &ChunkRef) -> Result<()> {
        let path = self.chunk_path(chunk);
        let tmp = add_temp_prefix(&path)?;
        tokio::fs::rename(&path, &tmp).await?;
        tokio::fs::remove_dir_all(tmp).await?;
        layout::clean_chunk_ancestors(path)?;
        Ok(())
    }

    fn chunk_path(&self, chunk_ref: &ChunkRef) -> PathBuf {
        self.fs
            .root
            .join(dataset::encode_dataset(&chunk_ref.dataset))
            .join(chunk_ref.chunk.as_ref())
    }

    #[cfg(feature = "mvcc-chunks")]
    fn mark_current_assignment_applied(&self) {
        let mut assignment_application = self.assignment_application.lock();
        let Some(current_assignment_id) = assignment_application.current_assignment_id.clone()
        else {
            return;
        };
        assignment_application.last_applied_assignment_id = Some(current_assignment_id.clone());
        let _ = self.assignment_applied_tx.send(Some(current_assignment_id));
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
                layout::clean_chunk_ancestors(PathBuf::try_from(path)?)?;
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
            let chunks: Vec<DataChunk> = layout::read_all_chunks(&fs.cd(dirname))
                .await
                .context(format!("Invalid layout in '{dir}'"))?;
            let dataset = Arc::new(dataset);
            for chunk in chunks {
                result.insert(ChunkRef {
                    dataset: dataset.clone(),
                    chunk: Arc::from(chunk.id),
                });
            }
        } else {
            warn!("Invalid dataset in workdir: '{dir}'");
        }
    }
    Ok(result)
}

fn get_directory_size(path: &Path) -> u64 {
    let mut result = 0;
    for entry in walkdir::WalkDir::new(path) {
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
}

#[cfg(test)]
mod tests {
    use camino::Utf8PathBuf as PathBuf;

    #[test]
    fn test_join_glob() {
        // `remove_temps` depends on this behavior
        assert_eq!(PathBuf::from("a/b").join("**/*.c").as_str(), "a/b/**/*.c");
    }
}
