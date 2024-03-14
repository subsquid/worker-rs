use std::collections::HashSet;

use anyhow::{Context, Result};
use camino::Utf8PathBuf as PathBuf;
use futures::{future, StreamExt};
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, warn};

use super::{
    layout::{self, BlockNumber, DataChunk},
    local_fs::{add_temp_prefix, LocalFs},
    s3_fs::S3Filesystem,
};
use crate::{
    storage::Filesystem,
    types::{
        dataset::{self, Dataset},
        state::{self, to_ranges, ChunkRef, ChunkSet, Ranges},
    },
    util::UseOnce,
};

pub struct StateManager {
    fs: LocalFs,
    state: Mutex<Inner>,
    tx: tokio::sync::mpsc::UnboundedSender<state::ChunkRef>,
    rx: UseOnce<tokio::sync::mpsc::UnboundedReceiver<state::ChunkRef>>,
    notify_sync: tokio::sync::Notify,
    concurrency: usize,
}

struct Inner {
    available: ChunkSet,
    downloading: ChunkSet,
    desired: ChunkSet,
}

pub struct Status {
    pub available: Ranges,
    pub downloading: Ranges,
    // TODO: add stored_bytes
}

// TODO: prioritize short jobs over long ones
impl StateManager {
    #[instrument(name = "state_manager")]
    pub async fn new(workdir: PathBuf, concurrency: usize) -> Result<Self> {
        let fs = LocalFs::new(workdir);
        Self::remove_temps(&fs)?;
        let state = Self::load_state(&fs).await?;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            fs,
            state: Mutex::new(Inner {
                available: state.clone(),
                downloading: ChunkSet::default(),
                desired: state,
            }),
            tx,
            rx: UseOnce::new(rx),
            notify_sync: tokio::sync::Notify::new(),
            concurrency,
        })
    }

    #[instrument(name = "state_manager", skip_all)]
    pub async fn run(&self, cancellation_token: CancellationToken) {
        UnboundedReceiverStream::new(self.rx.take().unwrap())
            .take_until(cancellation_token.cancelled())
            .for_each_concurrent(self.concurrency, |notification| {
                let cancellation_token = &cancellation_token;
                async move {
                    tokio::select!(
                        _ = self.process_notification(&notification) => {},
                        _ = cancellation_token.cancelled() => {
                            info!("Cancelled handling chunk {notification:?}");
                        }
                    )
                }
            })
            .await;
    }

    #[instrument(skip(self))]
    pub async fn set_desired_ranges(&self, desired: Ranges) -> Result<()> {
        let chunks = find_all_chunks(desired).await?;
        self.set_desired_chunks(chunks).await;
        Ok(())
    }

    // TODO: prevent accidental massive removals
    #[instrument(skip_all)]
    pub async fn set_desired_chunks(&self, desired: ChunkSet) {
        let mut state = self.state.lock().await;
        let previous = &mut state.desired;

        // All chunks in `previous` have already been scheduled for download or removal.
        // Now only the difference between new and previous assignment needs to be scheduled.

        for (dataset, ranges) in previous.inner().iter() {
            if !desired.inner().contains_key(dataset) {
                // Dataset was fully removed
                for chunk in ranges {
                    self.notify(dataset, chunk);
                }
            }
        }
        for (dataset, desired_ranges) in desired.inner().iter() {
            if let Some(previous_ranges) = previous.inner().get(dataset) {
                previous_ranges
                    .symmetric_difference(desired_ranges)
                    .for_each(|chunk| {
                        self.notify(dataset, chunk);
                    })
            } else {
                // New dataset was added
                for chunk in desired_ranges {
                    self.notify(dataset, chunk);
                }
            }
        }

        state.desired = desired;
    }

    // TODO: protect dir from removing while in use
    pub async fn find_chunk(
        &self,
        encoded_dataset: &str,
        block_number: BlockNumber,
    ) -> Option<PathBuf> {
        let fs = self.fs.cd(encoded_dataset);
        let stream = layout::stream_chunks(&fs, Some(&block_number), None);
        tokio::pin!(stream);
        match stream.next().await {
            Some(Ok(first)) => Some(self.fs.root.join(encoded_dataset).join(first.path())),
            Some(Err(e)) => {
                warn!("Couldn't get first chunk: {:?}", e);
                None
            }
            None => None,
        }
    }

    #[allow(dead_code)]
    pub async fn wait_sync(&self) {
        let state = self.state.lock().await;
        if state.available == state.desired {
            return;
        }
        let future = self.notify_sync.notified();
        drop(state); // release mutex before awaiting
        future.await
    }

    pub async fn current_status(&self) -> Status {
        let state = self.state.lock().await;
        let desired = state.desired.clone();
        let available = state.available.clone();
        drop(state); // release mutex
        let downloading = desired.difference(&available);
        Status {
            available: to_ranges(available),
            downloading: to_ranges(downloading),
        }
    }

    fn notify(&self, dataset: &Dataset, chunk: &DataChunk) {
        let _ = self.tx.send(ChunkRef {
            dataset: dataset.clone(),
            chunk: chunk.clone(),
        });
    }

    async fn process_notification(&self, chunk: &ChunkRef) {
        let mut state = self.state.lock().await;
        let available = state.available.contains(&chunk.dataset, &chunk.chunk);
        let downloading = state.downloading.contains(&chunk.dataset, &chunk.chunk);
        let desired = state.desired.contains(&chunk.dataset, &chunk.chunk);
        assert!(
            !(available && downloading),
            "Inconsisent state: chunk {:?} is both available and downloading",
            chunk
        );
        if desired {
            if available || downloading {
                // Chunk is already being processed. Do nothing.
                return;
            }
            state
                .downloading
                .insert(chunk.dataset.clone(), chunk.chunk.clone());
            drop(state); // unlock before awaiting

            let download_result = self.download_chunk(&chunk).await;

            let mut state = self.state.lock().await;
            state.downloading.remove(&chunk.dataset, &chunk.chunk);
            if download_result.is_err() {
                warn!("Failed to download chunk '{:?}', retrying", chunk);
                self.notify(&chunk.dataset, &chunk.chunk);
                return;
            }
            state
                .available
                .insert(chunk.dataset.clone(), chunk.chunk.clone());
            let desired_now = state.desired.contains(&chunk.dataset, &chunk.chunk);
            if !desired_now {
                info!(
                    "Chunk {:?} was unassigned during download. Scheduling removal.",
                    chunk
                );
                self.notify(&chunk.dataset, &chunk.chunk);
                return;
            }
            if state.available == state.desired {
                info!("State is in sync");
                self.notify_sync.notify_waiters();
            }
        } else {
            if available {
                // Wait until done because removals are fast.
                state.available.remove(&chunk.dataset, &chunk.chunk);
                self.drop_chunk(&chunk)
                    .await
                    .with_context(|| format!("Could not remove chunk {:?}", chunk))
                    .unwrap();
                if state.available == state.desired {
                    info!("State is in sync");
                    self.notify_sync.notify_waiters();
                }
            }
            if downloading {
                // Some data ranges scheduled for downloading are not needed anymore.
                // This should not usually happen so just panic in this case.
                todo!(
                    "Chunk removal requested while being downloaded: {:?}",
                    chunk
                );
            }
        }
    }

    #[instrument(err)]
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

    #[instrument(err, ret, skip(fs))]
    async fn load_state(fs: &LocalFs) -> Result<ChunkSet> {
        tokio::fs::create_dir_all(&fs.root).await?;
        let mut result = ChunkSet::new();
        for dir in fs.ls_root().await? {
            let dirname = dir.file_name().unwrap();
            if let Some(dataset) = dataset::decode_dataset(dirname) {
                let chunks: Vec<DataChunk> = layout::read_all_chunks(&fs.cd(dirname))
                    .await
                    .context(format!("Invalid layout in '{dir}'"))?;
                for chunk in chunks {
                    result.insert(dataset.clone(), chunk);
                }
            } else {
                warn!("Invalid dataset in workdir: '{dir}'");
            }
        }
        Ok(result)
    }

    fn chunk_path(&self, chunk: &ChunkRef) -> PathBuf {
        self.fs
            .root
            .join(dataset::encode_dataset(&chunk.dataset))
            .join(chunk.chunk.path())
    }

    async fn download_chunk(&self, chunk: &ChunkRef) -> Result<()> {
        S3Filesystem::with_bucket(&chunk.dataset)?
            .download_dir(chunk.chunk.path(), self.chunk_path(chunk))
            .await
            .with_context(|| format!("Could not download chunk {:?}", chunk))?;
        Ok(())
    }

    // TODO: lock used chunks
    #[instrument(err, skip(self))]
    async fn drop_chunk(&self, chunk: &ChunkRef) -> Result<()> {
        let path = self.chunk_path(chunk);
        let tmp = add_temp_prefix(&path)?;
        tokio::fs::rename(&path, &tmp).await?;
        tokio::fs::remove_dir_all(tmp).await?;
        layout::clean_chunk_ancestors(path)?;
        Ok(())
    }
}

#[instrument(err)]
async fn find_all_chunks(desired: Ranges) -> Result<ChunkSet> {
    let mut items = Vec::new();
    for (dataset, ranges) in desired {
        let rfs = S3Filesystem::with_bucket(&dataset)?;
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
        items.push(async {
            future::try_join_all(streams.into_iter())
                .await
                .map(|x| (dataset, x.into_iter().flatten().collect::<HashSet<_>>()))
        });
    }
    let chunks = ChunkSet::from_inner(
        futures::future::try_join_all(items.into_iter())
            .await?
            .into_iter()
            .collect(),
    );
    Ok(chunks)
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
