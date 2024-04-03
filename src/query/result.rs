use anyhow::Result;

use crate::util::hash::sha3_256;

use super::processor;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub raw_data: Vec<u8>,
    pub compressed_data: Vec<u8>,
    pub data_size: usize,
    pub data_sha3_256: Vec<u8>,
    pub num_read_chunks: usize,
}

impl QueryResult {
    pub fn new(values: processor::QueryResult, num_read_chunks: usize) -> Result<Self> {
        use flate2::write::GzEncoder;
        use std::io::Write;

        let data = serde_json::to_vec(&values)?;
        let data_size = data.len();

        let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&data)?;
        let compressed_data = encoder.finish()?;

        let hash = sha3_256(&data);

        Ok(Self {
            raw_data: data,
            compressed_data,
            data_size,
            data_sha3_256: hash,
            num_read_chunks,
        })
    }
}
