use super::dataset::Dataset;
use crate::storage::layout::DataChunk;
use sqd_messages::RangeSet;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

pub type ChunkSet = BTreeSet<ChunkRef>;
pub type Ranges = HashMap<Dataset, RangeSet>;

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
