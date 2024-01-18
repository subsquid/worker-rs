use std::path::PathBuf;

use crate::util::iterator::WithLookahead;
use anyhow::{anyhow, bail, Result};
use async_stream::try_stream;
use futures::Stream;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;

use super::Filesystem;

#[derive(PartialOrd, Ord, PartialEq, Eq, Default, Debug, Clone, Copy)]
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

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct DataChunk {
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
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

    pub fn parse_range(dirname: &str) -> Result<Self> {
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
            .ok_or_else(|| anyhow!("Could not parse chunk dirname '{}'", dirname))?;
        Ok(Self {
            first_block: BlockNumber::try_from(beg)?,
            last_block: BlockNumber::try_from(end)?,
            last_hash: hash.into(),
            top: BlockNumber::try_from(top)?,
        })
    }
}

impl std::fmt::Display for DataChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path())
    }
}

pub fn filename(str: &str) -> String {
    PathBuf::from(str)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_else(|| panic!("Couldn't parse filename from '{}'", str))
        .to_owned()
}

async fn list_top_dirs(fs: &impl Filesystem) -> Result<Vec<BlockNumber>> {
    let mut entries: Vec<_> = fs
        .ls_root()
        .await?
        .into_iter()
        .flat_map(|name| BlockNumber::try_from(PathBuf::from(name).file_name()?.to_str()?).ok())
        .collect();
    entries.sort_unstable();
    Ok(entries)
}

async fn list_chunks(fs: &impl Filesystem, top: &BlockNumber) -> Result<Vec<DataChunk>> {
    let mut entries: Vec<_> = fs
        .ls(&format!("{}", top))
        .await?
        .into_iter()
        .map(|dirname| DataChunk::parse_range(&dirname))
        .collect::<Result<_>>()?;
    entries.sort_unstable();
    Ok(entries)
}

// TODO: test it
pub fn stream_chunks<'a>(
    fs: &'a impl Filesystem,
    first_block: Option<&BlockNumber>,
    last_block: Option<&BlockNumber>,
) -> impl Stream<Item = Result<DataChunk>> + 'a {
    let first_block = match first_block {
        Some(&block) => block,
        None => 0.into(),
    };
    let last_block = match last_block {
        Some(&block) => block,
        None => u64::MAX.into(),
    };
    try_stream! {
        let tops = list_top_dirs(fs).await?;
        for (i, &top) in tops.iter().enumerate() {
            if i + 1 < tops.len() && tops[i + 1] <= first_block {
                continue;
            }
            if last_block < top {
                break;
            }
            let chunks = list_chunks(fs, &top).await?;
            for chunk in chunks {
                if last_block < chunk.first_block {
                    break;
                }
                if first_block > chunk.last_block {
                    continue;
                }
                yield chunk;
            }
        }
    }
}

pub async fn validate_layout(fs: &impl Filesystem) -> Result<()> {
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
            Ok(())
        });
    }
    futures::future::join_all(handles.into_iter())
        .await
        .into_iter()
        .try_collect()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf};

    use anyhow::Result;
    use futures::StreamExt;

    use crate::storage::{local_fs::LocalFs, tests::TestFilesystem};

    use super::{stream_chunks, validate_layout, BlockNumber, DataChunk};

    fn tests_data() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data")
    }

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

        assert_eq!(DataChunk::parse_range(&path).unwrap(), chunk);
    }

    #[tokio::test]
    async fn test_validate_layout() {
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
        validate_layout(&fs).await.unwrap()
    }

    #[tokio::test]
    async fn test_sample() {
        let fs = LocalFs { root: tests_data() };
        validate_layout(&fs).await.unwrap();

        let stream = stream_chunks(&fs, Some(&17881400.into()), None);
        let results: Vec<Result<DataChunk>> = stream.collect().await;
        let chunks = results.into_iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(
            chunks,
            vec![DataChunk::parse_range("0017881390/0017881390-0017882786-32ee9457").unwrap()]
        );
    }
}
