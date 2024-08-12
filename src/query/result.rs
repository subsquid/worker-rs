use anyhow::Result;

use crate::util::hash::sha3_256;

use super::processor;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub raw_data: Vec<u8>,
    pub compressed_data: Vec<u8>,
    pub data_size: usize,
    pub compressed_size: usize,
    pub data_sha3_256: Vec<u8>,
    pub num_read_chunks: usize,
    pub last_block: Option<u64>,
}

impl QueryResult {
    pub fn new(values: processor::QueryResult, num_read_chunks: usize) -> Result<Self> {
        use flate2::write::GzEncoder;
        use std::io::Write;

        let last_block = values.last().map(|(_json, block_number)| *block_number);
        let data = join(values.into_iter().map(|(json, _block_number)| json), "\n").into_bytes();
        let data_size = data.len();

        let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&data)?;
        let compressed_data = encoder.finish()?;
        let compressed_size = compressed_data.len();

        let hash = sha3_256(&data);

        Ok(Self {
            raw_data: data,
            compressed_data,
            data_size,
            compressed_size,
            data_sha3_256: hash,
            num_read_chunks,
            last_block,
        })
    }
}

fn join(strings: impl Iterator<Item = String>, separator: &str) -> String {
    let mut result = String::new();
    for s in strings {
        result.push_str(&s);
        result.push_str(separator);
    }
    result
}
