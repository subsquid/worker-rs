use super::dataset::Dataset;
use std::{borrow::Cow, collections::BTreeSet, sync::Arc};

pub type ChunkSet = BTreeSet<ChunkRef>;
pub type DatasetId = Arc<Dataset>;
pub type ChunkId = Arc<str>;

/// Prefix for non-zero chunk-version directories.
pub const VERSION_PREFIX: &str = "_v";

/// Global chunk key (DEF-4). Field order preserves DEF-13's chunk-id ordering.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkRef {
    pub dataset: DatasetId,
    pub chunk: ChunkId,
    /// Zero for the ingested copy; non-zero for a rewrite (IB-41b).
    pub version: u32,
}

impl ChunkRef {
    /// Constructs a reference to the ingested copy.
    pub fn new(dataset: DatasetId, chunk: ChunkId) -> Self {
        Self {
            dataset,
            chunk,
            version: 0,
        }
    }

    /// Returns the path below the dataset directory, preserving the legacy path for version zero.
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
