use super::dataset::Dataset;
use std::{borrow::Cow, collections::BTreeSet, sync::Arc};

pub type ChunkSet = BTreeSet<ChunkRef>;
pub type DatasetId = Arc<Dataset>;
pub type ChunkId = Arc<str>;

/// Names the directory a non-zero version's chunks live under. Leading `_` so it can never be
/// read as a top directory, which is always ten digits.
pub const VERSION_PREFIX: &str = "_v";

/// The global key of a chunk (DEF-4). Field order is the order DEF-13's bit sequence follows, so
/// a republished chunk sorts beside the copy it replaces rather than after every other chunk in
/// its dataset.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkRef {
    pub dataset: DatasetId,
    pub chunk: ChunkId,
    /// Which copy of the chunk: 0 is what ingest wrote, anything else a batch job's rewrite of
    /// it (IB-41b). Legacy assignments carry no versions, so every chunk of one is 0.
    pub version: u32,
}

impl ChunkRef {
    /// The ingested copy — what a legacy assignment names, and what a query naming no version
    /// asks for.
    pub fn new(dataset: DatasetId, chunk: ChunkId) -> Self {
        Self {
            dataset,
            chunk,
            version: 0,
        }
    }

    /// Where the chunk's files live under its dataset's directory. Version 0 sits exactly where a
    /// legacy chunk does, so a store written before versions existed reads back unchanged; every
    /// other version gets its own subtree, so two copies of one chunk id never contend for one
    /// directory.
    pub fn store_path(&self) -> Cow<'_, str> {
        match self.version {
            0 => Cow::Borrowed(self.chunk.as_ref()),
            version => Cow::Owned(format!("{VERSION_PREFIX}{version}/{}", self.chunk)),
        }
    }
}

impl std::fmt::Debug for ChunkRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl std::fmt::Display for ChunkRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.dataset, self.store_path())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "0000001000/0000001000-0000001999-abcdef12";

    fn chunk_ref(chunk: &str, version: u32) -> ChunkRef {
        ChunkRef {
            dataset: Arc::new("ds".to_owned()),
            chunk: Arc::from(chunk),
            version,
        }
    }

    #[test]
    fn only_a_rewrite_is_stored_under_a_version() {
        assert_eq!(chunk_ref(ID, 0).store_path(), ID, "as a legacy chunk is");
        assert_eq!(chunk_ref(ID, 2).store_path(), format!("_v2/{ID}"));
    }

    /// DEF-13's bit order is chunk-ref order, and the scheduler reads that map having only the
    /// chunk ids. So a rewrite has to sort where its id does, not after every chunk in the
    /// dataset — which is why the version is the last field rather than part of the id.
    #[test]
    fn a_rewrite_sorts_beside_the_copy_it_replaces() {
        let ingested = chunk_ref(ID, 0);
        let rewritten = chunk_ref(ID, 7);
        let next = chunk_ref("0000001000/0000002000-0000002999-bbbbbbbb", 0);

        let ordered: Vec<_> = ChunkSet::from([next.clone(), rewritten.clone(), ingested.clone()])
            .into_iter()
            .collect();

        assert_eq!(ordered, [ingested, rewritten, next]);
    }
}
