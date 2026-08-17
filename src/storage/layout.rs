use std::ops::Deref;
use std::str::FromStr;

use crate::util::iterator::WithLookahead;
use anyhow::{anyhow, bail, Context, Result};
use camino::Utf8Path as Path;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use tracing::{info, instrument, warn};

use super::local_fs::LocalFs;
use super::Filesystem;
use crate::types::state::VERSION_PREFIX;

// TODO: use u64
#[derive(PartialOrd, Ord, PartialEq, Eq, Default, Debug, Clone, Copy, Hash)]
#[repr(transparent)]
pub struct BlockNumber(u64);

impl std::fmt::Display for BlockNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:010}", self.0)
    }
}

impl TryFrom<&str> for BlockNumber {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.len() != 10 {
            bail!("String is not 10-digit decimal number: {}", s);
        }
        Ok(BlockNumber(s.parse()?))
    }
}

impl From<u64> for BlockNumber {
    fn from(value: u64) -> Self {
        BlockNumber(value)
    }
}

impl Into<u64> for BlockNumber {
    fn into(self) -> u64 {
        self.0
    }
}

impl AsRef<u64> for BlockNumber {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl Deref for BlockNumber {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// On-disk chunk identity.
///
/// The original chunk path format was `<top>/<first>-<last>-<hash>`, e.g.
/// `0000001000/0000001024-0000002047-0xabcdef`. It has been extended with an
/// optional trailing suffix so multiple chunks can cover the same block
/// range, e.g. `0000001000/0000001024-0000002047-0xabcdef<suffix>`.
#[derive(PartialEq, Eq, Clone, Hash)]
pub struct DataChunk {
    pub id: String,
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
}

impl Ord for DataChunk {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.last_block
            .cmp(&other.last_block)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for DataChunk {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl DataChunk {
    // TODO: synchronize with other language implementations
    pub fn from_path(dirname: &str) -> Result<Self> {
        lazy_static! {
            static ref RE: Regex =
                Regex::new(r"((?:\d{10})/(\d{10})-(\d{10})-(?:\w{5,8}).*)$").unwrap();
        }
        let captures = RE
            .captures(dirname)
            .ok_or_else(|| anyhow!("Could not parse chunk dirname '{dirname}'"))?;
        let id = captures.get(1).unwrap().as_str().to_owned();
        let first_block = BlockNumber::try_from(captures.get(2).unwrap().as_str())?;
        let last_block = BlockNumber::try_from(captures.get(3).unwrap().as_str())?;
        Ok(Self {
            id,
            first_block,
            last_block,
        })
    }
}

impl FromStr for DataChunk {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::from_path(s)
    }
}

impl std::fmt::Display for DataChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl std::fmt::Debug for DataChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

#[instrument(skip_all, level = "debug")]
async fn list_top_dirs(fs: &impl Filesystem) -> Result<Vec<BlockNumber>> {
    let mut entries: Vec<_> = fs
        .ls_root()
        .await?
        .into_iter()
        .filter_map(|name| BlockNumber::try_from(name.file_name()?).ok())
        .collect();
    entries.sort_unstable();
    Ok(entries)
}

#[instrument(skip_all, level = "debug")]
async fn list_chunks(fs: &impl Filesystem, top: &BlockNumber) -> Result<Vec<DataChunk>> {
    let mut entries: Vec<_> = fs
        .ls(&top.to_string())
        .await?
        .into_iter()
        .filter_map(|dirname| dirname.as_str().parse().ok())
        .collect();
    entries.sort_unstable();
    Ok(entries)
}

/// Every chunk in a dataset's directory, with the version whose subtree holds it. Version 0 sits
/// at the root — where a legacy chunk is — so a store written before versions existed reads back
/// as it always did. Each version is validated on its own, so two versions of one chunk may
/// legally cover the same block range.
pub async fn read_all_versions(fs: &LocalFs) -> Result<Vec<(u32, DataChunk)>> {
    let mut result: Vec<(u32, DataChunk)> = read_all_chunks(fs)
        .await?
        .into_iter()
        .map(|chunk| (0, chunk))
        .collect();
    for dir in fs.ls_root().await? {
        let Some(name) = dir.file_name().filter(|n| n.starts_with(VERSION_PREFIX)) else {
            continue;
        };
        let Some(version) = parse_version_dir(name) else {
            warn!("Unrecognized version dir in the chunk store: '{dir}'");
            continue;
        };
        let chunks = read_all_chunks(&fs.cd(name))
            .await
            .context(format!("Invalid layout in '{dir}'"))?;
        result.extend(chunks.into_iter().map(|chunk| (version, chunk)));
    }
    Ok(result)
}

fn parse_version_dir(name: &str) -> Option<u32> {
    let version: u32 = name.strip_prefix(VERSION_PREFIX)?.parse().ok()?;
    // Version 0 is stored at the root and has no subtree, so `_v0` describes nothing.
    (version != 0).then_some(version)
}

pub async fn read_all_chunks(fs: &impl Filesystem) -> Result<Vec<DataChunk>> {
    let tops = list_top_dirs(fs).await?;
    let mut handles = Vec::new();
    for (&top, next_top) in tops.iter().lookahead() {
        handles.push(async move {
            let chunks = list_chunks(fs, &top).await?;
            for chunk in &chunks {
                if chunk.first_block > chunk.last_block {
                    bail!(
                        "Invalid data chunk {}: {} > {}",
                        chunk,
                        chunk.first_block,
                        chunk.last_block
                    );
                }
                if chunk.first_block < top {
                    bail!(
                        "Invalid data chunk {}: {} < {}",
                        chunk,
                        chunk.first_block,
                        top
                    );
                }
                if let Some(&next) = next_top {
                    if next <= chunk.last_block {
                        bail!(
                            "Invalid data chunk {}: range overlaps with {} top dir",
                            chunk,
                            next
                        );
                    }
                }
            }
            for (cur, next) in chunks.iter().tuple_windows() {
                // Two chunks sharing the exact same range are allowed —
                // suffix-distinguished forks. Anything else overlapping bails.
                let same_range =
                    cur.first_block == next.first_block && cur.last_block == next.last_block;
                if !same_range && cur.last_block >= next.first_block {
                    bail!("Overlapping ranges: {} and {}", cur, next);
                }
            }
            Ok(chunks)
        });
    }
    let nested_chunks: Vec<_> = futures::future::join_all(handles.into_iter())
        .await
        .into_iter()
        .try_collect()?;
    Ok(nested_chunks.into_iter().flatten().collect())
}

/// Removes the chunk's now-empty ancestors — its top directory, the version subtree when it is in
/// one, and the dataset directory — stopping at the workdir, which is not ours to remove.
pub fn clean_chunk_ancestors(path: impl AsRef<Path>, root: impl AsRef<Path>) -> Result<()> {
    let root = root.as_ref();
    for dir in path.as_ref().ancestors().skip(1) {
        if dir == root || !dir.starts_with(root) {
            break;
        }
        if is_dir_empty(dir) {
            info!("Removing empty dir '{dir}'");
            if let Err(e) = std::fs::remove_dir(dir) {
                // Racing housekeeping is benign: the dir may already be
                // gone (NotFound) or a download may have started repopulating
                // it between the emptiness check and the removal
                // (DirectoryNotEmpty). Either way the dir needs no action.
                match e.kind() {
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty => {}
                    _ => return Err(e).context(format!("Couldn't remove dir '{dir}'")),
                }
            }
        }
    }
    Ok(())
}

fn is_dir_empty(path: impl AsRef<Path>) -> bool {
    match std::fs::read_dir(path.as_ref()) {
        Ok(mut entries) => entries.next().is_none(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::storage::{local_fs::LocalFs, tests::TestFilesystem};
    use crate::util::tests::tests_data;

    use super::{read_all_chunks, BlockNumber, DataChunk};

    #[test]
    fn test_block_number_conversion() {
        assert_eq!(
            BlockNumber::try_from("1000000000").unwrap(),
            BlockNumber(1000000000)
        );
        BlockNumber::try_from("20000000000000000000").unwrap_err();
        BlockNumber::try_from("0xdeadbeef").unwrap_err();
        assert_eq!(BlockNumber(50).to_string(), "0000000050");
    }

    #[test]
    fn test_data_chunk() {
        let path = "0000001000/0000001024-0000002047-0xabcdef";
        let chunk0 = DataChunk::from_path(path).unwrap();
        assert_eq!(&*chunk0.id, path);
        assert_eq!(chunk0.first_block, 1024.into());
        assert_eq!(chunk0.last_block, 2047.into());

        let path = "0221000000/0221000000-0221000649-9QgFD";
        let chunk1 = DataChunk::from_path(path).unwrap();
        assert_eq!(&*chunk1.id, path);
        assert_eq!(chunk1.first_block, 221000000.into());
        assert_eq!(chunk1.last_block, 221000649.into());
    }

    #[tokio::test]
    async fn test_read_all_chunks() {
        let fs = TestFilesystem {
            files: HashMap::from([
                (
                    "0000001000".into(),
                    vec![
                        "0000001000/0000001000-0000001999-0xabcdef".into(),
                        "0000001000/0000002000-0000002999-0x191919".into(),
                        "0000001000/0000003000-0000003999-0xdedede".into(),
                    ],
                ),
                (
                    "0000004000".into(),
                    vec![
                        "0000004000/0000004000-0000004999-0xaaaaaa".into(),
                        "0000004000/1000000000-1000999999-0xbbbbbb".into(),
                    ],
                ),
            ]),
        };
        let chunks = read_all_chunks(&fs).await.unwrap();
        assert_eq!(
            chunks,
            vec![
                DataChunk {
                    id: "0000001000/0000001000-0000001999-0xabcdef".to_owned(),
                    first_block: 1000.into(),
                    last_block: 1999.into(),
                },
                DataChunk {
                    id: "0000001000/0000002000-0000002999-0x191919".to_owned(),
                    first_block: 2000.into(),
                    last_block: 2999.into(),
                },
                DataChunk {
                    id: "0000001000/0000003000-0000003999-0xdedede".to_owned(),
                    first_block: 3000.into(),
                    last_block: 3999.into(),
                },
                DataChunk {
                    id: "0000004000/0000004000-0000004999-0xaaaaaa".to_owned(),
                    first_block: 4000.into(),
                    last_block: 4999.into(),
                },
                DataChunk {
                    id: "0000004000/1000000000-1000999999-0xbbbbbb".to_owned(),
                    first_block: 1000000000.into(),
                    last_block: 1000999999.into(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_sample() {
        let fs = LocalFs::new(tests_data());
        let chunks = read_all_chunks(&fs).await.unwrap();
        assert_eq!(
            chunks,
            vec![DataChunk::from_path("0017881390/0017881390-0017882786-32ee9457").unwrap()]
        );
    }

    #[tokio::test]
    async fn test_chunks_with_same_block_range() {
        let chunk_a_id = "0000000000/0000000000-0000001000-abcdef12";
        let chunk_b_id = "0000000000/0000000000-0000001000-abcdef12-fork";

        let fs = TestFilesystem {
            files: HashMap::from([(
                "0000000000".into(),
                vec![chunk_a_id.into(), chunk_b_id.into()],
            )]),
        };

        let chunks = read_all_chunks(&fs)
            .await
            .expect("layout should accept both chunks");

        assert_eq!(chunks.len(), 2);
        let ids: std::collections::HashSet<&str> = chunks.iter().map(|c| c.id.as_str()).collect();
        assert!(ids.contains(chunk_a_id));
        assert!(ids.contains(chunk_b_id));

        for chunk in &chunks {
            assert_eq!(chunk.first_block, 0u64.into());
            assert_eq!(chunk.last_block, 1000u64.into());
        }
    }

    /// One id at two versions, which only the subtree tells apart. Legal because each version is
    /// validated on its own — as one tree they would be an illegal exact-range overlap.
    #[tokio::test]
    async fn every_version_subtree_is_read_with_its_version() {
        let dir = tempfile::tempdir().unwrap();
        let root = camino::Utf8PathBuf::from_path_buf(dir.path().to_owned()).unwrap();
        let id = "0000001000/0000001000-0000001999-abcdef12";
        std::fs::create_dir_all(root.join(id)).unwrap();
        std::fs::create_dir_all(root.join("_v3").join(id)).unwrap();

        let mut found = super::read_all_versions(&LocalFs::new(root)).await.unwrap();
        found.sort_by_key(|(version, _)| *version);

        assert_eq!(
            found
                .iter()
                .map(|(version, chunk)| (*version, chunk.id.as_str()))
                .collect::<Vec<_>>(),
            [(0, id), (3, id)],
            "the version comes from the subtree, the id from the chunk dir"
        );
    }

    /// A directory whose name promises a version it doesn't name is not a store the worker can
    /// read, so its chunks stay invisible rather than being adopted at the wrong version.
    #[tokio::test]
    async fn a_dir_that_names_no_version_holds_no_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let root = camino::Utf8PathBuf::from_path_buf(dir.path().to_owned()).unwrap();
        let id = "0000001000/0000001000-0000001999-abcdef12";
        std::fs::create_dir_all(root.join(id)).unwrap();
        // `_v0` too: version 0 lives at the root, so a subtree for it describes nothing.
        for bad in ["_v0", "_vfoo"] {
            std::fs::create_dir_all(root.join(bad).join(id)).unwrap();
        }

        let found = super::read_all_versions(&LocalFs::new(root)).await.unwrap();

        assert_eq!(found.len(), 1, "only the chunk at the root is adopted");
        assert_eq!(found[0].0, 0);
    }

    #[test]
    fn cleaning_ancestors_stops_at_the_workdir() {
        let dir = tempfile::tempdir().unwrap();
        let root = camino::Utf8PathBuf::from_path_buf(dir.path().to_owned()).unwrap();
        let chunk = root
            .join("ds")
            .join("_v1")
            .join("0000001000")
            .join("0000001000-0000001999-abcdef12");
        std::fs::create_dir_all(&chunk).unwrap();
        // The state of things right after a removal: the chunk dir is gone, its ancestors aren't.
        std::fs::remove_dir(&chunk).unwrap();

        super::clean_chunk_ancestors(&chunk, &root).unwrap();

        assert!(
            !root.join("ds").exists(),
            "an emptied dataset dir goes, version subtree and all"
        );
        assert!(root.exists(), "the workdir is not ours to remove");
    }

    #[tokio::test]
    async fn test_chunks_with_partial_overlap_rejected() {
        // Ranges share blocks but aren't identical — the suffix exception
        // shouldn't apply.
        let fs = TestFilesystem {
            files: HashMap::from([(
                "0000000000".into(),
                vec![
                    "0000000000/0000000000-0000001000-abcdef12".into(),
                    "0000000000/0000000500-0000001500-bbbbbbbb".into(),
                ],
            )]),
        };
        let err = read_all_chunks(&fs)
            .await
            .expect_err("partial overlap should be rejected");
        assert!(
            err.to_string().contains("Overlapping ranges"),
            "unexpected error: {err}"
        );
    }
}
