use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use anyhow::Result;
use itertools::Itertools;
use s3::Bucket;
use subsquid_messages::RangeSet;
use tokio::sync::RwLock;

use crate::types::state::{ChunkRef, ChunkSet, Ranges};

use super::{
    layout::{BlockNumber, DataChunk},
    s3_fs::S3Filesystem,
};

#[derive(Default)]
pub struct DataSource {
    dataset_state: RwLock<HashMap<String, RwLock<State>>>,
}

struct State {
    bucket: Bucket,
    last_key: Option<String>,
    known_chunks: BTreeSet<DataChunk>,
}

impl DataSource {
    #[tracing::instrument(ret, skip_all, level = "debug")]
    pub async fn find_all_chunks(&self, ranges: Ranges) -> Result<ChunkSet> {
        let mut chunks = ChunkSet::new();
        for (dataset, range_set) in ranges {
            if range_set.ranges.is_empty() {
                continue;
            }

            let mut ds_guard = self.dataset_state.read().await;
            let state = if let Some(state) = ds_guard.get(&dataset) {
                state
            } else {
                drop(ds_guard);
                self.dataset_state
                    .write()
                    .await
                    .entry(dataset.clone())
                    .or_insert(RwLock::new(State::new(
                        S3Filesystem::with_bucket(&dataset)?.bucket,
                    )));
                ds_guard = self.dataset_state.read().await;
                ds_guard.get(&dataset).unwrap()
            };

            let dataset = Arc::new(dataset);
            let last_required_block = BlockNumber::from(range_set.ranges.last().unwrap().end);
            if !state.read().await.contains_block(last_required_block) {
                tracing::debug!("Updating dataset objects: {}", dataset);
                state.write().await.update_chunks().await?;
            }
            for chunk in state.read().await.find_chunks(&range_set) {
                chunks.insert(ChunkRef {
                    dataset: dataset.clone(),
                    chunk,
                });
            }
        }
        Ok(chunks)
    }

    pub async fn update_dataset(&self, dataset: String) -> Result<()> {
        let mut ds_guard = self.dataset_state.write().await;
        let state = ds_guard
            .entry(dataset.clone())
            .or_insert(RwLock::new(State::new(
                S3Filesystem::with_bucket(&dataset)?.bucket,
            )));
        state.write().await.update_chunks().await?;
        Ok(())
    }
}

impl State {
    fn new(bucket: Bucket) -> Self {
        Self {
            bucket,
            known_chunks: Default::default(),
            last_key: None,
        }
    }

    fn find_chunks(&self, ranges: &RangeSet) -> Vec<DataChunk> {
        let mut chunks = Vec::new();
        for range in ranges.ranges.iter() {
            let first = DataChunk {
                last_block: BlockNumber::from(range.begin),
                ..Default::default()
            };
            for chunk in self.known_chunks.range(first..) {
                if chunk.first_block <= BlockNumber::from(range.end) {
                    chunks.push(chunk.clone());
                } else {
                    break;
                }
            }
        }
        chunks
    }

    fn contains_block(&self, block: BlockNumber) -> bool {
        self.known_chunks
            .last()
            .map(|chunk| chunk.last_block >= block)
            .unwrap_or(false)
    }

    #[tracing::instrument(skip(self), fields(bucket=self.bucket.name), level="debug")]
    async fn update_chunks(&mut self) -> Result<()> {
        let mut last_key = self.last_key.clone();
        loop {
            let (list_result, _) = self
                .bucket
                .list_page("".to_string(), None, None, last_key.clone(), None)
                .await?;
            last_key = list_result.contents.last().map(|object| object.key.clone());
            if last_key.is_none() {
                tracing::warn!("Didn't find new chunks in the bucket {}", self.bucket.name);
                return Ok(());
            }
            tracing::debug!("Found {} new keys", list_result.contents.len());
            self.known_chunks.extend(
                list_result
                    .contents
                    .into_iter()
                    .map(|object| {
                        if let Some((dirname, _)) = object.key.rsplit_once('/') {
                            dirname.parse()
                        } else {
                            Err(anyhow::anyhow!("Invalid key: {}", object.key))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .dedup(),
            );
            self.last_key = last_key.clone();
            if !list_result.is_truncated {
                break;
            }
        }
        Ok(())
    }
}
