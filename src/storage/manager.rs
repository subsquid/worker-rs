use std::sync::Arc;

use anyhow::{Context, Result};
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
use futures::StreamExt;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

use crate::types::{
    dataset,
    state::{to_ranges, ChunkRef, ChunkSet, Ranges},
};

use super::{
    downloader::ChunkDownloader,
    layout::{self, BlockNumber, DataChunk},
    local_fs::{add_temp_prefix, LocalFs},
    s3_fs::S3Filesystem,
    state::{State, UpdateStatus},
    Filesystem,
};

pub struct StateManager {
    fs: LocalFs,
    state: Mutex<State>,
    desired_ranges: Mutex<Ranges>,
    notify: tokio::sync::Notify,
}

pub struct Status {
    pub available: Ranges,
    pub downloading: Ranges,
    pub stored_bytes: u64,
}

impl StateManager {
    pub async fn new(workdir: PathBuf) -> Result<Self> {
        let fs = LocalFs::new(workdir);
        remove_temps(&fs)?;
        let existing_chunks = load_state(&fs).await?;
        debug!("Loaded state: {:?}", existing_chunks);

        Ok(Self {
            fs,
            state: Mutex::new(State::new(existing_chunks)),
            desired_ranges: Default::default(),
            notify: tokio::sync::Notify::new(),
        })
    }

    pub async fn run(&self, cancellation_token: CancellationToken, concurrency: usize) {
        let mut downloader = ChunkDownloader::default();
        loop {
            self.state.lock().report_status();

            tokio::select! {
                _ = self.notify.notified() => {}
                (chunk, result) = downloader.downloaded() => {
                    match result {
                        Ok(()) => {
                            self.state.lock().complete_download(&chunk, true);
                        }
                        Err(e) => {
                            // TODO: skip logging if the download was cancelled
                            warn!("Failed to download chunk '{chunk}':\n{e:?}");
                            self.state.lock().complete_download(&chunk, false);
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
            }

            while downloader.download_count() < concurrency {
                if let Some(chunk) = self.state.lock().take_next_download() {
                    info!("Downloading chunk {chunk}");
                    let dst = self.chunk_path(&chunk);
                    downloader.start_download(chunk, dst);
                } else {
                    break;
                }
            }
        }
    }

    #[instrument(skip_all)]
    pub fn current_status(&self) -> Status {
        let status = self.state.lock().status();
        let stored_bytes = get_directory_size(&self.fs.root);
        Status {
            available: to_ranges(status.available),
            downloading: to_ranges(status.downloading),
            stored_bytes,
        }
    }

    // TODO: prevent accidental massive removals
    #[instrument(skip(self))]
    pub async fn set_desired_ranges(&self, ranges: Ranges) -> Result<()> {
        if *self.desired_ranges.lock() != ranges {
            let chunks = find_all_chunks(ranges.clone()).await?;

            let mut cache = self.desired_ranges.lock();
            let mut state = self.state.lock();
            let result = state.set_desired_chunks(chunks);
            *cache = ranges;

            match result {
                UpdateStatus::Updated => {
                    info!("Got new assignment");
                    self.notify.notify_one();
                }
                UpdateStatus::Unchanged => {}
            }
        }
        Ok(())
    }

    pub fn find_chunks<'s>(
        &'s self,
        encoded_dataset: &str,
        block_number: BlockNumber,
    ) -> Result<scopeguard::ScopeGuard<Vec<PathBuf>, impl FnOnce(Vec<PathBuf>) + 's>> {
        let dataset = dataset::decode_dataset(encoded_dataset)
            .with_context(|| format!("Couldn't decode dataset: {encoded_dataset}"))?;
        let chunks = self
            .state
            .lock()
            .find_and_lock_chunks(Arc::new(dataset), block_number);
        let paths = chunks
            .iter()
            .map(|chunk| self.fs.root.join(encoded_dataset).join(chunk.chunk.path()))
            .collect();
        let guard = scopeguard::guard(paths, move |_| self.state.lock().release_chunks(chunks));
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

// TODO: make it faster by only iterating added ranges
#[instrument(ret, level = "debug")]
async fn find_all_chunks(desired: Ranges) -> Result<ChunkSet> {
    let mut items = Vec::new();
    for (dataset, ranges) in desired {
        let rfs = S3Filesystem::with_bucket(&dataset)?;
        let dataset = Arc::new(dataset);
        let mut streams = Vec::new();
        for range in ranges.ranges {
            let rfs = rfs.clone();
            let stream_fut = async move {
                let results =
                    layout::stream_chunks(&rfs, Some(&range.begin.into()), Some(&range.end.into()))
                        .collect::<Vec<_>>()
                        .await;
                results.into_iter().collect::<Result<Vec<_>>>()
            };
            streams.push(stream_fut);
        }
        items.push(async move {
            futures::future::try_join_all(streams.into_iter())
                .await
                .map(|x| {
                    x.into_iter()
                        .flatten()
                        .map(|chunk| ChunkRef {
                            dataset: dataset.clone(),
                            chunk,
                        })
                        .collect::<ChunkSet>()
                })
        });
    }
    let chunks = futures::future::try_join_all(items.into_iter())
        .await?
        .into_iter()
        .flatten()
        .collect();
    Ok(chunks)
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
