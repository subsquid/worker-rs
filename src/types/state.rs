use super::dataset::Dataset;
use crate::storage::layout::DataChunk;
use std::{collections::BTreeSet, sync::Arc};

pub type ChunkSet = BTreeSet<ChunkRef>;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkRef {
    pub dataset: Arc<Dataset>,
    pub chunk: DataChunk,
}

impl std::fmt::Debug for ChunkRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl std::fmt::Display for ChunkRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.dataset, self.chunk)
    }
}
