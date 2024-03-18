use itertools::Itertools;
use std::{collections::BTreeMap, sync::Arc};
use tracing::{info, instrument};

use super::layout::{BlockNumber, DataChunk};
use crate::types::{
    dataset::Dataset,
    state::{ChunkRef, ChunkSet},
};

#[derive(Debug)]
pub struct State {
    available: BTreeMap<ChunkRef, u8>, // stores ref count for each chunk
    downloading: ChunkSet,
    to_download: ChunkSet,
    to_remove: ChunkSet,
}

#[derive(Default, Debug)]
pub struct UpdateResult {
    pub cancelled: ChunkSet,
    pub removed: ChunkSet,
}

#[derive(Debug)]
pub enum UpdateStatus {
    Unchanged,
    Updated(UpdateResult),
}

pub struct Status {
    pub available: ChunkSet,
    pub downloading: ChunkSet,
}

impl State {
    pub fn new(available: ChunkSet) -> Self {
        let counts = available.into_iter().map(|chunk| (chunk, 1)).collect();
        Self {
            available: counts,
            downloading: Default::default(),
            to_download: Default::default(),
            to_remove: Default::default(),
        }
    }

    #[instrument(skip_all)]
    pub fn set_desired_chunks(&mut self, desired: ChunkSet) -> UpdateStatus {
        let mut result = UpdateResult {
            removed: std::mem::take(&mut self.to_remove),
            ..Default::default()
        };
        let mut updated = !result.removed.is_empty();

        self.available.retain(|chunk, count| {
            let remove = if !desired.contains(chunk) {
                *count -= 1;
                *count == 0
            } else {
                false
            };
            if remove {
                result.removed.insert(chunk.clone());
                updated = true;
            }
            remove
        });

        self.downloading.retain(|chunk| {
            let remove = !desired.contains(chunk);
            if remove {
                result.cancelled.insert(chunk.clone());
                updated = true;
            }
            !remove
        });

        self.to_download.retain(|chunk| {
            let remove = !desired.contains(chunk);
            if remove {
                updated = true;
            }
            !remove
        });

        for chunk in desired.into_iter() {
            if !self.available.contains_key(&chunk)
                && !self.downloading.contains(&chunk)
                && !self.to_download.contains(&chunk)
            {
                self.to_download.insert(chunk);
                updated = true;
            }
        }

        match updated {
            false => UpdateStatus::Unchanged,
            true => UpdateStatus::Updated(result),
        }
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
            let chunk = *chunks.first()?;
            chunk.clone()
        };
        self.to_download.remove(&chunk_ref);
        self.downloading.insert(chunk_ref.clone());
        Some(chunk_ref)
    }

    pub fn complete_download(&mut self, chunk: &ChunkRef) {
        match self.downloading.take(chunk) {
            None => {
                // The chunk has finished download before being cancelled. It should be removed now
                self.to_remove.insert(chunk.clone());
            }
            Some(removed) => {
                self.available.insert(removed, 1);
            }
        }
    }

    pub fn find_and_lock_chunks(
        &mut self,
        dataset: Arc<Dataset>,
        block_number: BlockNumber,
    ) -> Vec<ChunkRef> {
        let from_chunk = DataChunk {
            last_block: block_number,
            first_block: block_number,
            ..Default::default()
        };
        let from = ChunkRef {
            dataset: dataset.clone(),
            chunk: from_chunk,
        };
        let mut range = self.available.range(from..);
        let first = match range.next() {
            None => return Vec::new(),
            Some((chunk, _count)) => chunk.clone(),
        };
        if first.chunk.first_block > block_number {
            return Vec::new();
        }

        let mut last_block = first.chunk.last_block;
        let mut result = vec![first];
        for (chunk, _count) in range {
            if chunk.dataset == dataset
                && *chunk.chunk.first_block.as_ref() == *last_block.as_ref() + 1
            {
                result.push(chunk.clone());
                last_block = chunk.chunk.last_block;
            } else {
                break;
            }
        }

        for chunk in result.iter() {
            self.lock_chunk(chunk);
        }
        result
    }

    pub fn release_chunks(&mut self, chunks: impl IntoIterator<Item = ChunkRef>) {
        for chunk in chunks {
            self.remove_chunk(&chunk);
        }
    }

    #[instrument(skip_all)]
    pub fn status(&self) -> Status {
        Status {
            downloading: self.to_download.union(&self.downloading).cloned().collect(),
            available: self.available.keys().cloned().collect(),
        }
    }

    pub fn schedule_download(&mut self, chunk: ChunkRef) {
        self.to_download.insert(chunk);
    }

    fn remove_chunk(&mut self, chunk: &ChunkRef) {
        let remove = self
            .available
            .get_mut(chunk)
            .map(|count| {
                *count -= 1;
                *count == 0
            })
            .unwrap_or(false);
        if remove {
            // Defer removal until the next call to `set_desired_chunks`
            self.to_remove.insert(chunk.clone());
            self.available.remove(chunk);
        }
    }

    fn lock_chunk(&mut self, chunk: &ChunkRef) {
        *self
            .available
            .get_mut(chunk)
            .unwrap_or_else(|| panic!("Trying to lock unknown chunk: {chunk}")) += 1;
    }

    fn report_status(&self) {
        info!(
            "Chunks available: {}, downloading: {}, pending downloads: {}",
            self.available.len(),
            self.downloading.len(),
            self.to_download.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        storage::{layout::DataChunk, state::UpdateStatus},
        types::state::ChunkRef,
    };

    use super::State;

    #[test]
    fn test_state() {
        let ds = Arc::new("ds".to_owned());
        let chunk_ref = |x| ChunkRef {
            dataset: ds.clone(),
            chunk: DataChunk::parse_range(&format!(
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
        state.schedule_download(c.clone());
        assert_eq!(state.take_next_download(), Some(c.clone()));

        match state.set_desired_chunks([b.clone(), d.clone()].into_iter().collect()) {
            UpdateStatus::Updated(result) => {
                assert_eq!(result.removed.into_iter().collect::<Vec<_>>(), &[a.clone()]);
                assert_eq!(
                    result.cancelled.into_iter().collect::<Vec<_>>(),
                    &[c.clone()]
                );
            }
            _ => panic!("Unexpected set_desired_chunks result"),
        }

        assert_eq!(state.take_next_download(), Some(d.clone()));
        assert_eq!(state.take_next_download(), None);
        state.complete_download(&d);

        match state.set_desired_chunks([b.clone(), d.clone()].into_iter().collect()) {
            UpdateStatus::Unchanged => {}
            _ => panic!("Unexpected set_desired_chunks result"),
        };
    }

    #[test]
    fn test_data_chunk_comparison() {
        // Chunks lookup depends on sorting by last_block
        assert!(
            DataChunk {
                first_block: 1.into(),
                last_block: 2.into(),
                ..Default::default()
            } < DataChunk {
                first_block: 0.into(),
                last_block: 3.into(),
                ..Default::default()
            }
        )
    }
}
