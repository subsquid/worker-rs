use itertools::Itertools;
use std::{collections::BTreeMap, sync::Arc};
use tracing::{info, instrument};

use super::layout::DataChunk;
use crate::{
    metrics,
    types::{
        dataset::Dataset,
        state::{ChunkRef, ChunkSet},
    },
};

#[derive(Debug, Default)]
pub struct State {
    available: ChunkSet,
    downloading: ChunkSet, // available and downloading don't intersect
    desired: ChunkSet,
    to_download: ChunkSet, // to_download is always equal to desired.diff(available).diff(downloading)
    locks: BTreeMap<ChunkRef, u8>, // stores ref count for each chunk
}

#[derive(Debug)]
pub enum UpdateStatus {
    Unchanged,
    Updated,
}

pub struct Status {
    pub available: ChunkSet,
    pub downloading: ChunkSet,
}

impl State {
    pub fn new(available: ChunkSet) -> Self {
        Self {
            available: available.clone(),
            desired: available,
            ..Default::default()
        }
    }

    #[instrument(skip_all)]
    pub fn set_desired_chunks(&mut self, desired: ChunkSet) -> UpdateStatus {
        let status = if self.desired == desired {
            UpdateStatus::Unchanged
        } else {
            UpdateStatus::Updated
        };

        self.desired = desired;
        self.to_download = self
            .desired
            .iter()
            .filter(|chunk| !self.available.contains(chunk) && !self.downloading.contains(chunk))
            .cloned()
            .collect();

        status
    }

    // make desired = available + downloading
    pub fn stop_downloads(&mut self) -> UpdateStatus {
        if self.to_download.is_empty() {
            return UpdateStatus::Unchanged;
        };
        self.desired
            .retain(|chunk| !self.to_download.contains(chunk));
        self.to_download.clear();
        UpdateStatus::Updated
    }

    pub fn take_next_download(&mut self) -> Option<ChunkRef> {
        let chunk_ref = {
            // TODO: use priority queue if it's slow
            let (_dataset, chunks) = self
                .to_download
                .iter()
                .into_group_map_by(|chunk| chunk.dataset.clone())
                .into_iter()
                .min_by_key(|(_ds, chunks)| chunks.len())?;
            (*chunks.first()?).clone()
        };
        self.to_download.remove(&chunk_ref);
        self.downloading.insert(chunk_ref.clone());
        Some(chunk_ref)
    }

    pub fn take_removals(&mut self) -> Vec<ChunkRef> {
        let mut result = Vec::new();
        self.available.retain(|chunk| {
            if self.desired.contains(chunk) || self.locks.contains_key(chunk) {
                true
            } else {
                result.push(chunk.clone());
                false
            }
        });
        result
    }

    // Only works as a hint to speed up things.
    // Cancelled downloads still have to be reported with a `complete_download` call
    pub fn get_stale_downloads(&self) -> Vec<ChunkRef> {
        self.downloading
            .difference(&self.desired)
            .cloned()
            .collect()
    }

    pub fn complete_download(&mut self, chunk: &ChunkRef, success: bool) {
        let chunk = self
            .downloading
            .take(chunk)
            .unwrap_or_else(|| panic!("Completing download of unknown chunk: {chunk}"));
        if success {
            self.available.insert(chunk);
        } else if self.desired.contains(&chunk) {
            self.to_download.insert(chunk);
        }
    }

    pub fn get_and_lock_chunk(
        &mut self,
        dataset: Arc<Dataset>,
        chunk: DataChunk,
    ) -> Option<ChunkRef> {
        let chunk_ref = self.available.get(&ChunkRef { dataset, chunk }).cloned();

        if let Some(chunk_ref) = chunk_ref.as_ref() {
            self.lock_chunk(chunk_ref);
        }

        chunk_ref
    }

    pub fn release_chunks(&mut self, chunks: impl IntoIterator<Item = ChunkRef>) {
        for chunk in chunks {
            self.unlock_chunk(&chunk);
        }
    }

    #[instrument(skip_all)]
    pub fn status(&self) -> Status {
        Status {
            downloading: self.to_download.union(&self.downloading).cloned().collect(),
            available: self.available.clone(),
        }
    }

    pub fn unlock_chunk(&mut self, chunk: &ChunkRef) {
        let remove = self
            .locks
            .get_mut(chunk)
            .map(|count| {
                *count -= 1;
                *count == 0
            })
            .unwrap_or(false);
        if remove {
            self.locks.remove(chunk);
        }
    }

    fn lock_chunk(&mut self, chunk: &ChunkRef) {
        assert!(
            self.available.contains(chunk),
            "Trying to lock unknown chunk: {chunk}"
        );
        *self.locks.entry(chunk.clone()).or_insert(0) += 1;
    }

    pub fn report_status(&self) {
        info!(
            "Chunks available: {}, downloading: {}, pending downloads: {}",
            self.available.len(),
            self.downloading.len(),
            self.to_download.len()
        );
        metrics::CHUNKS_AVAILABLE.set(self.available.len() as i64);
        metrics::CHUNKS_DOWNLOADING.set(self.downloading.len() as i64);
        metrics::CHUNKS_PENDING.set(self.to_download.len() as i64);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;

    use crate::{storage::layout::DataChunk, types::state::ChunkRef};

    use super::State;

    #[test]
    fn test_state() {
        let ds = Arc::new("ds".to_owned());
        let chunk_ref = |x| ChunkRef {
            dataset: ds.clone(),
            chunk: DataChunk::from_path(&format!(
                "0000000000/000000000{}-000000000{}-00000000",
                x,
                x + 1
            ))
            .unwrap(),
        };
        let a = chunk_ref(0);
        let b = chunk_ref(1);
        let c = chunk_ref(2);
        let d = chunk_ref(3);

        let mut state = State::new([a.clone(), b.clone()].into_iter().collect());
        state.set_desired_chunks([a.clone(), b.clone(), c.clone()].into_iter().collect());
        assert_eq!(state.take_next_download(), Some(c.clone()));
        assert_eq!(state.take_next_download(), None);

        state.set_desired_chunks([b.clone(), d.clone()].into_iter().collect());
        assert_eq!(state.get_stale_downloads(), &[c.clone()]);
        assert_eq!(state.take_removals(), &[a.clone()]);
        assert_eq!(state.take_removals(), &[]);
        assert_eq!(state.get_stale_downloads(), &[c.clone()]);

        assert_eq!(state.take_next_download(), Some(d.clone()));
        assert_eq!(state.take_next_download(), None);
        state.complete_download(&d, true);
        state.complete_download(&c, false);

        assert_eq!(
            state.status().available.into_iter().collect_vec(),
            &[b.clone(), d.clone()]
        );
        assert_eq!(state.status().downloading.into_iter().collect_vec(), &[]);
    }
}
