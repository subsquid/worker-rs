use tracing::instrument;

use super::layout::{BlockNumber, DataChunk};
use crate::{
    types::{
        dataset::Dataset,
        state::{ChunkRef, ChunkSet},
    },
    util::nested_map::NestedMap,
};

#[derive(Debug)]
pub struct State {
    available: NestedMap<Dataset, DataChunk, u8>, // stores ref count for each chunk
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
        let counts = available
            .into_iter()
            .map(|(dataset, chunk)| (dataset.clone(), chunk.clone(), 1))
            .collect();
        Self {
            available: counts,
            downloading: Default::default(),
            to_download: Default::default(),
            to_remove: Default::default(),
        }
    }

    #[instrument(ret, skip_all)]
    pub fn set_desired_chunks(&mut self, desired: ChunkSet) -> UpdateStatus {
        let mut result = UpdateResult {
            removed: self.to_remove.drain().collect(),
            ..Default::default()
        };
        let mut updated = !result.removed.inner().is_empty();

        for (dataset, chunk, _) in self.available.extract_if(|ds, chunk, count| {
            if !desired.contains(ds, chunk) {
                *count -= 1;
                *count == 0
            } else {
                false
            }
        }) {
            result.removed.insert(dataset, chunk);
            updated = true;
        }

        for (dataset, chunk) in self
            .downloading
            .extract_if(|ds, chunk| !desired.contains(ds, chunk))
        {
            result.cancelled.insert(dataset, chunk);
            updated = true;
        }

        let cancelled = self
            .to_download
            .extract_if(|ds, chunk| !desired.contains(ds, chunk));
        if !cancelled.is_empty() {
            updated = true;
        }

        for (ds, chunk) in desired.into_iter() {
            if !self.available.contains(&ds, &chunk)
                && !self.downloading.contains(&ds, &chunk)
                && !self.to_download.contains(&ds, &chunk)
            {
                self.to_download.insert(ds, chunk);
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
            let (dataset, chunks) = self
                .to_download
                .inner()
                .iter()
                .min_by_key(|(_ds, chunks)| chunks.len())?;
            let chunk = chunks.iter().next()?;
            ChunkRef {
                dataset: dataset.clone(),
                chunk: chunk.clone(),
            }
        };
        self.to_download
            .remove(&chunk_ref.dataset, &chunk_ref.chunk);
        self.downloading
            .insert(chunk_ref.dataset.clone(), chunk_ref.chunk.clone());
        Some(chunk_ref)
    }

    pub fn complete_download(&mut self, chunk: &ChunkRef) {
        let was_present = self.downloading.remove(&chunk.dataset, &chunk.chunk);
        if !was_present {
            // The chunk has finished download before being cancelled. It should be removed now
            self.to_remove
                .insert(chunk.dataset.clone(), chunk.chunk.clone());
        } else {
            self.available
                .insert(chunk.dataset.clone(), chunk.chunk.clone(), 1);
        }
    }

    pub fn find_and_lock_chunks<'a>(
        &mut self,
        dataset: &Dataset,
        block_number: BlockNumber,
    ) -> Vec<DataChunk> {
        let from = DataChunk {
            last_block: block_number,
            first_block: block_number,
            ..Default::default()
        };
        let mut range = match self.available.inner().get(dataset) {
            None => return Vec::new(),
            Some(nested) => nested.range(from..),
        };
        let first = match range.next() {
            None => return Vec::new(),
            Some((chunk, _count)) => chunk.clone(),
        };
        if first.first_block > block_number {
            return Vec::new();
        }

        let mut last_block = first.last_block;
        let mut result = vec![first];
        for (chunk, _count) in range {
            if *chunk.first_block.as_ref() == *last_block.as_ref() + 1 {
                result.push(chunk.clone());
                last_block = chunk.last_block;
            } else {
                break;
            }
        }

        for chunk in result.iter() {
            self.lock_chunk(dataset, chunk);
        }
        result
    }

    pub fn release_chunks(
        &mut self,
        dataset: &Dataset,
        chunks: impl IntoIterator<Item = DataChunk>,
    ) {
        for chunk in chunks {
            self.remove_chunk(dataset, &chunk);
        }
    }

    #[instrument(skip_all)]
    pub fn status(&self) -> Status {
        Status {
            downloading: self.to_download.clone().union(self.downloading.clone()),
            available: self
                .available
                .iter_keys()
                .map(|(ds, chunk)| (ds.clone(), chunk.clone()))
                .collect(),
        }
    }

    pub fn schedule_download(&mut self, chunk: ChunkRef) {
        self.to_download.insert(chunk.dataset, chunk.chunk);
    }

    fn remove_chunk(&mut self, dataset: &Dataset, chunk: &DataChunk) {
        let remove = self
            .available
            .get_mut(dataset, chunk)
            .map(|count| {
                *count -= 1;
                *count == 0
            })
            .unwrap_or(false);
        if remove {
            // Defer removal until the next call to `set_desired_chunks`
            self.to_remove.insert(dataset.clone(), chunk.clone());
            self.available.remove(dataset, chunk);
        }
    }

    fn lock_chunk(&mut self, dataset: &Dataset, chunk: &DataChunk) {
        *self
            .available
            .get_mut(dataset, chunk)
            .unwrap_or_else(|| panic!("Trying to lock unknown chunk: {}/{}", dataset, chunk)) += 1;
    }

    fn report_status(&self) {
        info!(
            "Chunks available: {}, downloading: {}, pending downloads: {}",
            self.available.len(), self.downloading.len(), self.to_download.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        storage::{layout::DataChunk, state::UpdateStatus},
        types::state::ChunkRef,
    };

    use super::State;

    #[test]
    fn test_state() {
        let chunk_ref = |x| ChunkRef {
            dataset: "ds".to_owned(),
            chunk: DataChunk::parse_range(&format!(
                "0000000000/000000000{}-000000000{}-00000000",
                x,
                x + 1
            ))
            .unwrap(),
        };
        let to_pair = |c: &ChunkRef| (c.dataset.clone(), c.chunk.clone());
        let a = chunk_ref(0);
        let b = chunk_ref(1);
        let c = chunk_ref(2);
        let d = chunk_ref(3);

        let mut state = State::new([to_pair(&a), to_pair(&b)].into_iter().collect());
        state.schedule_download(c.clone());
        assert_eq!(state.take_next_download(), Some(c.clone()));

        match state.set_desired_chunks([to_pair(&b), to_pair(&d)].into_iter().collect()) {
            UpdateStatus::Updated(result) => {
                assert_eq!(
                    result.removed.into_iter().collect::<Vec<_>>(),
                    &[(a.dataset, a.chunk)]
                );
                assert_eq!(
                    result.cancelled.into_iter().collect::<Vec<_>>(),
                    &[(c.dataset, c.chunk)]
                );
            }
            _ => panic!("Unexpected set_desired_chunks result"),
        }

        assert_eq!(state.take_next_download(), Some(d.clone()));
        assert_eq!(state.take_next_download(), None);
        state.complete_download(&d);

        match state.set_desired_chunks([to_pair(&b), to_pair(&d)].into_iter().collect()) {
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
