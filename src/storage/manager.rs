use std::sync::Arc;

use anyhow::{Context, Result};
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

use crate::{
    metrics,
    types::{
        dataset,
        state::{to_ranges, ChunkRef, ChunkSet, Ranges},
    },
};

use super::{
    datasets_index::DatasetsIndex,
    downloader::ChunkDownloader,
    layout::{self, BlockNumber, DataChunk},
    local_fs::{add_temp_prefix, LocalFs},
    state::{State, UpdateStatus},
    Filesystem,
};

#[derive(Default)]
pub struct StateManager {
    fs: LocalFs,
    state: Mutex<State>,
    notify: tokio::sync::Notify,
    datasets_index: Mutex<DatasetsIndex>,
    concurrent_downloads: usize,
}

pub struct Status {
    pub available: Ranges,
    pub downloading: Ranges,
    pub unavailability_map: Vec<bool>,
    pub stored_bytes: u64,
    pub assignment_id: Option<String>,
}

impl StateManager {
    pub async fn new(workdir: PathBuf, concurrent_downloads: usize) -> Result<Self> {
        let fs = LocalFs::new(workdir);
        remove_temps(&fs)?;
        let existing_chunks = load_state(&fs).await?;
        debug!("Loaded state: {:?}", existing_chunks);

        Ok(Self {
            fs,
            state: Mutex::new(State::new(existing_chunks)),
            concurrent_downloads,
            ..Default::default()
        })
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        let mut downloader = ChunkDownloader::default();
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

            for chunk in self.state.lock().take_removals() {
                info!("Removing chunk {chunk}");
                self.drop_chunk(&chunk)
                    .unwrap_or_else(|_| panic!("Couldn't remove chunk {chunk}"));
                metrics::CHUNKS_REMOVED.inc();
            }

            let index = self.datasets_index.lock();
            while downloader.download_count() < self.concurrent_downloads {
                if let Some(chunk) = self.state.lock().take_next_download() {
                    info!("Downloading chunk {chunk}");
                    let dst = self.chunk_path(&chunk);
                    downloader.start_download(chunk, dst, &index);
                } else {
                    break;
                }
            }
        }
        info!("State manager loop finished");
    }

    #[instrument(skip_all)]
    pub fn current_status(&self) -> Status {
        let status = self.state.lock().status();
        let stored_bytes = get_directory_size(&self.fs.root);
        let datasets_index = self.datasets_index.lock();
        let ordinals_len = datasets_index.get_ordinals_len();
        let mut unavailability_map: Vec<bool> = vec![true; ordinals_len];
        for chunk_ref in &status.available {
            if let Some(ordinal) = datasets_index.get_ordinal(&chunk_ref.dataset, &chunk_ref.chunk)
            {
                unavailability_map[ordinal as usize] = false
            } else {
                warn!(
                    "Ordinal for {:?} {:?} not set",
                    &chunk_ref.dataset, &chunk_ref.chunk
                );
            }
        }
        Status {
            available: to_ranges(status.available),
            downloading: to_ranges(status.downloading),
            unavailability_map,
            stored_bytes,
            assignment_id: datasets_index.get_assignment_id(),
        }
    }

    // TODO: prevent accidental massive removals
    #[instrument(skip_all)]
    pub fn set_desired_chunks(&self, desired_chunks: ChunkSet) {
        match self.state.lock().set_desired_chunks(desired_chunks) {
            UpdateStatus::Unchanged => {}
            UpdateStatus::Updated => {
                info!("Got new assignment");
                self.notify.notify_one();
            }
        }
    }

    pub fn set_datasets_index(&self, index: DatasetsIndex) {
        *self.datasets_index.lock() = index;
    }

    pub fn get_assignment_id(&self) -> Option<String> {
        self.datasets_index.lock().get_assignment_id()
    }

    pub fn stop_downloads(&self) {
        match self.state.lock().stop_downloads() {
            UpdateStatus::Unchanged => {}
            UpdateStatus::Updated => {
                self.notify.notify_one();
            }
        }
    }

    pub fn find_chunk(
        self: Arc<Self>,
        encoded_dataset: &str,
        block_number: BlockNumber,
    ) -> Result<scopeguard::ScopeGuard<Option<PathBuf>, impl FnOnce(Option<PathBuf>)>> {
        let dataset = dataset::decode_dataset(encoded_dataset)
            .with_context(|| format!("Couldn't decode dataset: {encoded_dataset}"))?;
        let chunk = self
            .state
            .lock()
            .find_and_lock_chunk(Arc::new(dataset), block_number);
        let path = chunk
            .as_ref()
            .map(|chunk| self.fs.root.join(encoded_dataset).join(chunk.chunk.path()));
        let guard = scopeguard::guard(path, move |_| self.state.lock().release_chunks(chunk));
        Ok(guard)
    }

    #[instrument(err, skip(self))]
    fn drop_chunk(&self, chunk: &ChunkRef) -> Result<()> {
        let path = self.chunk_path(chunk);
        let tmp = add_temp_prefix(&path)?;
        std::fs::rename(&path, &tmp)?;
        std::fs::remove_dir_all(tmp)?;
        layout::clean_chunk_ancestors(path)?;
        Ok(())
    }

    fn chunk_path(&self, chunk: &ChunkRef) -> PathBuf {
        self.fs
            .root
            .join(dataset::encode_dataset(&chunk.dataset))
            .join(chunk.chunk.path())
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
                    chunk,
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
