use crate::storage::layout::DataChunk;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as base64, Engine};
use std::collections::{HashMap, HashSet};

pub type Dataset = String;
pub type RangeSet = HashSet<DataChunk>;
pub type State = HashMap<Dataset, RangeSet>;

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

pub fn has(state: &State, chunk: &ChunkRef) -> bool {
    match state.get(&chunk.dataset) {
        Some(set) => set.contains(&chunk.chunk),
        None => false,
    }
}

pub fn add(state: &mut State, chunk: &ChunkRef) {
    state.entry(chunk.dataset.clone()).or_default().insert(chunk.chunk.clone());
}

pub fn remove(state: &mut State, chunk: &ChunkRef) {
    state.entry(chunk.dataset.clone()).or_default().remove(&chunk.chunk);
}
