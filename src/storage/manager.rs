use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Context, Result};
use camino::Utf8PathBuf as PathBuf;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, warn};

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
    notify: tokio::sync::Notify,
}

pub struct Status {
    pub available: Ranges,
    pub downloading: Ranges,
    // TODO: add stored_bytes
}

impl StateManager {
    pub async fn new(workdir: PathBuf) -> Result<Self> {
        let fs = LocalFs::new(workdir);
        remove_temps(&fs)?;
        let existing_chunks = load_state(&fs).await?;
        Ok(Self {
            fs,
            state: Mutex::new(State::new(existing_chunks)),
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

    pub fn current_status(&self) -> Status {
        let status = self.state.lock().status();
        Status {
            available: to_ranges(status.available),
            downloading: to_ranges(status.downloading),
        }
    }

    // TODO: prevent accidental massive removals
    #[instrument(err, skip(self))]
    pub async fn set_desired_ranges(&self, ranges: Ranges) -> Result<()> {
        Ok(self.set_desired_chunks(find_all_chunks(ranges).await?))
    }

    #[instrument(skip(self))]
    fn set_desired_chunks(&self, desired: ChunkSet) {
        match self.state.lock().set_desired_chunks(desired) {
            UpdateStatus::Updated => {
                info!("Got new assignment");
                self.notify.notify_one();
            },
            UpdateStatus::Unchanged => {},
        }
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

#[instrument(err)]
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
                        .collect::<BTreeSet<_>>()
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

#[instrument(err, skip_all)]
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

#[instrument(err, ret, skip_all)]
async fn load_state(fs: &LocalFs) -> Result<ChunkSet> {
    tokio::fs::create_dir_all(&fs.root).await?;
    let mut result = ChunkSet::new();
    for dir in fs.ls_root().await? {
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

#[cfg(test)]
mod tests {
    use camino::Utf8PathBuf as PathBuf;

    #[test]
    fn test_join_glob() {
        // `remove_temps` depends on this behavior
        assert_eq!(PathBuf::from("a/b").join("**/*.c").as_str(), "a/b/**/*.c");
    }
}
