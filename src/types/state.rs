use crate::storage::layout::DataChunk;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as base64, Engine};
use std::collections::{HashMap, HashSet};
use subsquid_messages::{Range, RangeSet};

pub type Dataset = String;
pub type ChunkSet = HashMap<Dataset, HashSet<DataChunk>>;
pub type Ranges = HashMap<Dataset, RangeSet>;

pub fn encode_dataset(dataset: &str) -> String {
    base64.encode(dataset.as_bytes())
}

pub fn decode_dataset(str: &str) -> Option<Dataset> {
    base64
        .decode(str)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
}

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
    first.into_iter().filter_map(|(key, mut chunks)| {
        if let Some(removed) = second.get(&key) {
            chunks.retain(|x| !removed.contains(x));
        }
        if chunks.is_empty() {
            None
        } else {
            Some((key, chunks))
        }
    }).collect()
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
