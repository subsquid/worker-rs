use std::ops::Deref;
use std::str::FromStr;

use crate::util::iterator::WithLookahead;
use anyhow::{anyhow, bail, Context, Result};
use camino::Utf8Path as Path;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use tracing::{info, instrument};

use super::Filesystem;

// TODO: use u64
#[derive(PartialOrd, Ord, PartialEq, Eq, Default, Debug, Clone, Copy, Hash)]
#[repr(transparent)]
pub struct BlockNumber(u32);

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

impl From<u32> for BlockNumber {
    fn from(value: u32) -> Self {
        BlockNumber(value)
    }
}

impl AsRef<u32> for BlockNumber {
    fn as_ref(&self) -> &u32 {
        &self.0
    }
}

impl Deref for BlockNumber {
    type Target = u32;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Default, PartialEq, Eq, Clone, Hash)]
pub struct DataChunk {
    pub last_block: BlockNumber,
    pub first_block: BlockNumber,
    pub last_hash: String,
    pub top: BlockNumber,
}

impl DataChunk {
    pub fn path(&self) -> String {
        format!(
            "{}/{}-{}-{}",
            self.top, self.first_block, self.last_block, self.last_hash
        )
    }

    // TODO: synchronize with other language implementations
    pub fn from_path(dirname: &str) -> Result<Self> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"(\d{10})/(\d{10})-(\d{10})-(\w{8})$").unwrap();
        }
        let (top, beg, end, hash) = RE
            .captures(dirname)
            .and_then(
                |cap| match (cap.get(1), cap.get(2), cap.get(3), cap.get(4)) {
                    (Some(top), Some(beg), Some(end), Some(hash)) => {
                        Some((top.as_str(), beg.as_str(), end.as_str(), hash.as_str()))
                    }
                    _ => None,
                },
            )
            .ok_or_else(|| anyhow!("Could not parse chunk dirname '{dirname}'"))?;
        Ok(Self {
            first_block: BlockNumber::try_from(beg)?,
            last_block: BlockNumber::try_from(end)?,
            last_hash: hash.into(),
            top: BlockNumber::try_from(top)?,
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
        write!(f, "{}", self.path())
    }
}

impl std::fmt::Debug for DataChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl Ord for DataChunk {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.last_block.cmp(&other.last_block)
    }
}

impl PartialOrd for DataChunk {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
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
                if cur.last_block >= next.first_block {
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

pub fn clean_chunk_ancestors(path: impl AsRef<Path>) -> Result<()> {
    // take(2) limits it to removing range dir and dataset dir but not the workdir itself
    for dir in path.as_ref().ancestors().skip(1).take(2) {
        if is_dir_empty(dir) {
            info!("Removing empty dir '{dir}'");
            std::fs::remove_dir(dir).context(format!("Couldn't remove dir '{dir}'"))?;
        }
    }
    Ok(())
}

fn is_dir_empty(path: impl AsRef<Path>) -> bool {
    match std::fs::read_dir(path.as_ref()) {
        Ok(entries) => entries.count() == 0,
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
        let chunk = DataChunk {
            first_block: 1024.into(),
            last_block: 2047.into(),
            last_hash: "0xabcdef".into(),
            top: 1000.into(),
        };
        let path = "0000001000/0000001024-0000002047-0xabcdef";
        assert_eq!(chunk.path(), path);

        assert_eq!(DataChunk::from_path(&path).unwrap(), chunk);
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
                    top: 1000.into(),
                    first_block: 1000.into(),
                    last_block: 1999.into(),
                    last_hash: "0xabcdef".to_owned()
                },
                DataChunk {
                    top: 1000.into(),
                    first_block: 2000.into(),
                    last_block: 2999.into(),
                    last_hash: "0x191919".to_owned()
                },
                DataChunk {
                    top: 1000.into(),
                    first_block: 3000.into(),
                    last_block: 3999.into(),
                    last_hash: "0xdedede".to_owned()
                },
                DataChunk {
                    top: 4000.into(),
                    first_block: 4000.into(),
                    last_block: 4999.into(),
                    last_hash: "0xaaaaaa".to_owned()
                },
                DataChunk {
                    top: 4000.into(),
                    first_block: 1000000000.into(),
                    last_block: 1000999999.into(),
                    last_hash: "0xbbbbbb".to_owned()
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
}
