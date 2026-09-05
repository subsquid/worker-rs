//! Deterministic chunk corpus (HC-12's data half).
//!
//! Generated, not checked in: kilobytes per chunk keeps the PR gate inside P-GATE-PR-TIME,
//! and HC-4 can later share the generator. `tests/data/` keeps the large real-world chunk.

use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

/// The dataset description served by the schema registry stub (IB-44) and used to compile
/// dynamic-engine queries. A cut-down `evm`: enough tables to exercise selection, field
/// projection, weights (and so the RP-13 budget) and boundary emission.
pub const SCHEMA_YAML: &str = r#"
name: evm
tables:
  blocks:
    output:
      name: block
      fields: [number]
    block_number_column: number
    sort_key: [number]
    columns:
      number:
        type: uint64
  logs:
    request:
      name: logs
      filters: []
    output:
      name: log
      fields: [transaction_index, log_index, data]
    block_number_column: block_number
    item_order_keys: [transaction_index, log_index]
    sort_key: [block_number, transaction_index, log_index]
    columns:
      block_number:
        type: uint64
      transaction_index:
        type: uint32
      log_index:
        type: uint32
      data:
        type: string
        encoding: hex_bytes
        weight: data_size
      data_size:
        type: uint64
        system: true
"#;

/// One generated chunk: the file bytes, keyed by file name, plus its block range.
pub struct Chunk {
    pub id: String,
    pub first_block: u64,
    pub last_block: u64,
    pub files: Vec<(String, Vec<u8>)>,
}

impl Chunk {
    pub fn size_bytes(&self) -> u32 {
        self.files.iter().map(|(_, b)| b.len() as u32).sum::<u32>()
    }

    pub fn file(&self, name: &str) -> Option<&[u8]> {
        self.files
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, b)| b.as_slice())
    }

    /// The chunk-id form the legacy layout parser expects:
    /// `<padded top dir>/<padded first>-<padded last>-<hash>`.
    fn make_id(first_block: u64, last_block: u64) -> String {
        format!(
            "{:010}/{:010}-{:010}-{:08x}",
            first_block / 1_000_000 * 1_000_000,
            first_block,
            last_block,
            first_block ^ last_block,
        )
    }
}

/// Builds a chunk covering `first_block..=last_block`, one log per block.
///
/// `log_weight` is the declared `data_size` per log row. The dynamic engine stops once the
/// accumulated weight reaches P-RESP-BUDGET, so a large value forces early truncation
/// (RP-13) with only a handful of rows.
pub fn chunk(first_block: u64, last_block: u64, log_weight: u64) -> Chunk {
    assert!(first_block <= last_block, "empty block range");
    let numbers: Vec<u64> = (first_block..=last_block).collect();

    let blocks_schema = Arc::new(Schema::new(vec![Field::new(
        "number",
        DataType::UInt64,
        false,
    )]));
    let blocks = RecordBatch::try_new(
        blocks_schema,
        vec![Arc::new(UInt64Array::from(numbers.clone())) as ArrayRef],
    )
    .expect("blocks batch is well-formed");

    let logs_schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("data", DataType::Binary, false),
        Field::new("data_size", DataType::UInt64, false),
    ]));
    let payloads: Vec<Vec<u8>> = numbers
        .iter()
        .map(|n| format!("log-{n:016x}").into_bytes())
        .collect();
    let logs = RecordBatch::try_new(
        logs_schema,
        vec![
            Arc::new(UInt64Array::from(numbers.clone())) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32; numbers.len()])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32; numbers.len()])) as ArrayRef,
            Arc::new(BinaryArray::from(
                payloads.iter().map(|p| p.as_slice()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt64Array::from(vec![log_weight; numbers.len()])) as ArrayRef,
        ],
    )
    .expect("logs batch is well-formed");

    Chunk {
        id: Chunk::make_id(first_block, last_block),
        first_block,
        last_block,
        files: vec![
            ("blocks.parquet".to_owned(), to_parquet(&blocks)),
            ("logs.parquet".to_owned(), to_parquet(&logs)),
        ],
    }
}

fn to_parquet(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buf, batch.schema(), None).expect("parquet writer opens");
    writer.write(batch).expect("parquet row group writes");
    writer.close().expect("parquet footer writes");
    buf
}
