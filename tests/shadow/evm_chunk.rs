//! A small EVM chunk in the layout the network stores, for the shadow tests in
//! `src/shadow` and in `tests/shadow_runner.rs`.

#![allow(dead_code)]

use std::sync::Arc;

use arrow::array::{
    ArrayRef, StringArray, TimestampMillisecondArray, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use serde_json::json;
use sqd_query_engine::metadata::DatasetDescription;
use tempfile::TempDir;

/// A copy of the pinned engine revision's catalog; refresh it when the pin
/// moves.
const EVM_CATALOG: &str = include_str!("../metadata/evm.yaml");
pub const FIRST_BLOCK: u64 = 100;
pub const LAST_BLOCK: u64 = 104;
pub const TOKEN: &str = "0xdac17f958d2ee523a2206206994597c13d831ec7";
pub const OTHER: &str = "0x4444444444444444444444444444444444444444";
const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/// Two transactions with a log each per block. `data_size` is the weight each
/// log claims, which need not be its real size.
pub fn write_chunk(data_size: u64) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let blocks: Vec<u64> = (FIRST_BLOCK..=LAST_BLOCK).collect();
    let hash = |block: u64| format!("0x{block:064x}");

    write_table(
        &dir,
        "blocks",
        vec![
            ("number", u32s(blocks.iter().map(|&b| b as u32))),
            ("hash", strings(blocks.iter().map(|&b| hash(b)))),
            ("parent_hash", strings(blocks.iter().map(|&b| hash(b - 1)))),
            (
                "timestamp",
                (Arc::new(TimestampMillisecondArray::from_iter_values(
                    blocks
                        .iter()
                        .map(|&b| (1_700_000_000 + b as i64 * 12) * 1000),
                )) as ArrayRef)
                    .into(),
            ),
        ],
    );

    let rows: Vec<(u64, u16)> = blocks
        .iter()
        .flat_map(|&b| [(b, 0u16), (b, 1u16)])
        .collect();
    let tx_hash = |&(b, i): &(u64, u16)| format!("0x{:064x}", b * 10 + i as u64);
    let address = |&(_, i): &(u64, u16)| if i == 0 { TOKEN } else { OTHER }.to_owned();

    write_table(
        &dir,
        "transactions",
        vec![
            ("block_number", u32s(rows.iter().map(|&(b, _)| b as u32))),
            ("transaction_index", u16s(rows.iter().map(|&(_, i)| i))),
            ("hash", strings(rows.iter().map(tx_hash))),
            ("from", strings(rows.iter().map(|_| OTHER.to_owned()))),
            ("to", strings(rows.iter().map(address))),
            (
                "input",
                strings(rows.iter().map(|_| "0xa9059cbb".to_owned())),
            ),
            ("input_size", u64s(rows.iter().map(|_| 4))),
            (
                "value",
                strings(rows.iter().map(|&(b, _)| format!("0x{b:x}"))),
            ),
        ],
    );
    write_table(
        &dir,
        "logs",
        vec![
            ("block_number", u32s(rows.iter().map(|&(b, _)| b as u32))),
            ("log_index", u16s(rows.iter().map(|&(_, i)| i))),
            ("transaction_index", u16s(rows.iter().map(|&(_, i)| i))),
            ("transaction_hash", strings(rows.iter().map(tx_hash))),
            ("address", strings(rows.iter().map(address))),
            (
                "data",
                strings(rows.iter().map(|&(b, _)| format!("0x{b:064x}"))),
            ),
            ("data_size", u64s(rows.iter().map(|_| data_size))),
            ("topic0", strings(rows.iter().map(|_| TRANSFER.to_owned()))),
            (
                "topic1",
                strings(rows.iter().map(|&(b, _)| format!("0x{:064x}", b + 1))),
            ),
            (
                "topic2",
                strings(rows.iter().map(|&(b, _)| format!("0x{:064x}", b + 2))),
            ),
            (
                "topic3",
                strings(rows.iter().map(|_| String::new())).nullable(),
            ),
        ],
    );
    dir
}

/// The catalog, with `edit` applied as a text replacement.
pub fn catalog(edit: Option<(&str, &str)>) -> DatasetDescription {
    let yaml = match edit {
        Some((from, to)) => {
            assert!(EVM_CATALOG.contains(from), "the catalog has no `{from}`");
            EVM_CATALOG.replace(from, to)
        }
        None => EVM_CATALOG.to_owned(),
    };
    sqd_query_engine::metadata::parse_dataset_description(&yaml).unwrap()
}

pub fn token_logs_query() -> serde_json::Value {
    json!({
        "type": "evm",
        "fields": {
            "block": {"number": true, "hash": true, "timestamp": true},
            "log": {"logIndex": true, "transactionIndex": true, "address": true, "topics": true, "data": true},
            "transaction": {"hash": true, "from": true, "to": true, "value": true}
        },
        "logs": [{"address": [TOKEN], "transaction": true}]
    })
}

fn write_table(dir: &TempDir, name: &str, columns: Vec<(&str, Column)>) {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column)| Field::new(*name, column.0.data_type().clone(), column.1))
        .collect();
    let arrays = columns.into_iter().map(|(_, column)| column.0).collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();

    let file = std::fs::File::create(dir.path().join(format!("{name}.parquet"))).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// A column and whether it is nullable.
struct Column(ArrayRef, bool);

impl Column {
    /// Empty strings become nulls, as absent topics are stored.
    fn nullable(self) -> Self {
        let strings = self.0.as_any().downcast_ref::<StringArray>().unwrap();
        let values: StringArray = strings
            .iter()
            .map(|value| value.filter(|value| !value.is_empty()))
            .collect();
        Column(Arc::new(values), true)
    }
}

impl From<ArrayRef> for Column {
    fn from(array: ArrayRef) -> Self {
        Column(array, false)
    }
}

fn u32s(values: impl Iterator<Item = u32>) -> Column {
    (Arc::new(UInt32Array::from_iter_values(values)) as ArrayRef).into()
}

fn u16s(values: impl Iterator<Item = u16>) -> Column {
    (Arc::new(UInt16Array::from_iter_values(values)) as ArrayRef).into()
}

fn u64s(values: impl Iterator<Item = u64>) -> Column {
    (Arc::new(UInt64Array::from_iter_values(values)) as ArrayRef).into()
}

fn strings(values: impl Iterator<Item = String>) -> Column {
    (Arc::new(StringArray::from_iter_values(values)) as ArrayRef).into()
}
