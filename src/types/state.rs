use super::dataset::Dataset;
use crate::storage::layout::DataChunk;
use itertools::Itertools;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};
use sqd_messages::{Range, RangeSet};

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

pub fn to_ranges(state: ChunkSet) -> Ranges {
    state
        .into_iter()
        .group_by(|chunk_ref| chunk_ref.dataset.clone())
        .into_iter()
        .map(|(dataset, chunks)| {
            let range: RangeSet = chunks
                .into_iter()
                .map(|chunk| Range::new(*chunk.chunk.first_block, *chunk.chunk.last_block))
                .into();
            ((*dataset).clone(), range)
        })
        .collect()
}
