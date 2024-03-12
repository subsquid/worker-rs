use super::dataset::Dataset;
use crate::{storage::layout::DataChunk, util::nested_set::NestedSet};
use std::collections::HashMap;
use subsquid_messages::{Range, RangeSet};

pub type ChunkSet = NestedSet<Dataset, DataChunk>;
pub type Ranges = HashMap<Dataset, RangeSet>;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ChunkRef {
    pub dataset: Dataset,
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
        .into_inner()
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
