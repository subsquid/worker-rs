use super::dataset::Dataset;
use crate::storage::layout::DataChunk;
use std::collections::{HashMap, HashSet};
use subsquid_messages::{Range, RangeSet};

pub type ChunkSet = HashMap<Dataset, HashSet<DataChunk>>;
pub type Ranges = HashMap<Dataset, RangeSet>;

#[derive(Clone)]
pub struct ChunkRef {
    pub dataset: Dataset,
    pub chunk: DataChunk,
}

impl std::fmt::Debug for ChunkRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.dataset, self.chunk)
    }
}

pub fn has(state: &ChunkSet, chunk: &ChunkRef) -> bool {
    match state.get(&chunk.dataset) {
        Some(set) => set.contains(&chunk.chunk),
        None => false,
    }
}

pub fn add(state: &mut ChunkSet, chunk: &ChunkRef) {
    state
        .entry(chunk.dataset.clone())
        .or_default()
        .insert(chunk.chunk.clone());
}

pub fn remove(state: &mut ChunkSet, chunk: &ChunkRef) {
    let mut drained = false;
    if let Some(chunks) = state.get_mut(&chunk.dataset) {
        chunks.remove(&chunk.chunk);
        drained = chunks.is_empty();
    };
    if drained {
        state.remove(&chunk.dataset);
    }
}

pub fn difference(first: ChunkSet, second: &ChunkSet) -> ChunkSet {
    first
        .into_iter()
        .filter_map(|(key, mut chunks)| {
            if let Some(removed) = second.get(&key) {
                chunks.retain(|x| !removed.contains(x));
            }
            if chunks.is_empty() {
                None
            } else {
                Some((key, chunks))
            }
        })
        .collect()
}

pub fn to_ranges(state: ChunkSet) -> Ranges {
    state
        .into_iter()
        .map(|(dataset, chunks)| {
            let range: RangeSet = chunks
                .into_iter()
                .map(|chunk| Range::new(*chunk.first_block, *chunk.last_block))
                .into();
            (dataset, range)
        })
        .collect()
}
