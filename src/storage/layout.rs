use std::path::PathBuf;

use crate::util::iterator::WithLookahead;
use anyhow::{anyhow, bail, Result};
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
    first_block: BlockNumber,
    last_block: BlockNumber,
    last_hash: String,
    top: BlockNumber,
}

impl DataChunk {
    pub fn path(&self) -> PathBuf {
        format!(
            "{}/{}-{}-{}",
            self.top, self.first_block, self.last_block, self.last_hash
        )
        .into()
    }

    pub fn parse_range(top: impl Into<BlockNumber>, dirname: &str) -> Result<Self> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"^(\d{10})-(\d{10})-(\w{8})$").unwrap();
        }
        let (beg, end, hash) = RE
            .captures(dirname)
            .and_then(|cap| match (cap.get(1), cap.get(2), cap.get(3)) {
                (Some(beg), Some(end), Some(hash)) => {
                    Some((beg.as_str(), end.as_str(), hash.as_str()))
                }
                _ => None,
            })
            .ok_or_else(|| anyhow!("Could not parse chunk dirname {}", dirname))?;
        Ok(Self {
            first_block: BlockNumber::try_from(beg)?,
            last_block: BlockNumber::try_from(end)?,
            last_hash: hash.into(),
            top: top.into(),
        })
    }
}

impl std::fmt::Display for DataChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path().to_string_lossy())
    }
}

pub fn list_top_dirs(fs: &impl Filesystem) -> Result<Vec<BlockNumber>> {
    let mut entries: Vec<_> = fs
        .ls_root()?
        .into_iter()
        .flat_map(|name| BlockNumber::try_from(name.as_str()))
        .collect();
    entries.sort_unstable();
    Ok(entries)
}

fn list_chunks(fs: &impl Filesystem, top: &BlockNumber) -> Result<Vec<DataChunk>> {
    let mut entries: Vec<_> = fs
        .ls(&top.to_string())?
        .into_iter()
        .map(|dirname| DataChunk::parse_range(*top, &dirname))
        .collect::<Result<_>>()?;
    entries.sort_unstable();
    Ok(entries)
}

// TODO: test it
pub fn iter_chunks<'a>(
    fs: &'a impl Filesystem,
    first_block: Option<&BlockNumber>,
    last_block: Option<&BlockNumber>,
) -> Result<impl DoubleEndedIterator<Item = Result<DataChunk>> + 'a> {
    let first_block = match first_block {
        Some(&block) => block,
        None => 0.into(),
    };
    let last_block = match last_block {
        Some(&block) => block,
        None => u64::MAX.into(),
    };
    let tops = list_top_dirs(fs)?;
    let mut filtered = Vec::new();
    for (i, &top) in tops.iter().enumerate() {
        if i + 1 < tops.len() && tops[i + 1] <= first_block {
            continue;
        }
        if last_block < top {
            break;
        }
        filtered.push(top);
    }
    let iter = filtered
        .into_iter()
        .map(move |top| {
            let chunks = match list_chunks(fs, &top) {
                Ok(chunks) => chunks,
                Err(e) => return vec![Err(e)],
            };
            // Pre-collect into vec because TakeWhile<SkipWhile<_>> is not a DoubleEndedIterator
            let mut result = Vec::new();
            for chunk in chunks {
                if last_block < chunk.first_block {
                    break;
                }
                if first_block > chunk.last_block {
                    continue;
                }
                result.push(Ok(chunk));
            }
            result
        })
        .flatten();
    Ok(iter)
}

pub fn validate_layout(fs: &impl Filesystem) -> Result<()> {
    let tops = list_top_dirs(fs)?;
    for (&top, next_top) in tops.iter().lookahead() {
        let chunks = list_chunks(fs, &top)?;
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
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf};

    use crate::storage::tests::TestFilesystem;

    use super::{BlockNumber, DataChunk, validate_layout};

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
        let filename = "0000001024-0000002047-0xabcdef";
        let path = PathBuf::from_iter(["0000001000", filename].iter());
        assert_eq!(chunk.path(), path);

        assert_eq!(DataChunk::parse_range(1000, filename).unwrap(), chunk);
    }

    #[test]
    fn test_layout() {
        let fs = TestFilesystem {
            files: HashMap::from([
                (
                    "0000001000".into(),
                    vec![
                        "0000001000-0000001999-0xabcdef".into(),
                        "0000002000-0000002999-0x191919".into(),
                        "0000003000-0000003999-0xdedede".into(),
                    ],
                ),
                (
                    "0000004000".into(),
                    vec![
                        "0000004000-0000004999-0xaaaaaa".into(),
                        "1000000000-1000999999-0xbbbbbb".into(),
                    ],
                ),
            ]),
        };
        validate_layout(&fs).unwrap()
    }
}
