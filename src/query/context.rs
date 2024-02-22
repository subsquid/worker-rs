use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::datatypes::{self, DataType, Field, Schema};
use datafusion::common::{Constraint, Constraints};
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::{ParquetReadOptions, ReadOptions};
use datafusion::sql::TableReference;
use lazy_static::lazy_static;

lazy_static! {
    static ref BLOCKS_SCHEMA: Schema = Schema::new(vec![
        Field::new("number", DataType::Int32, false),
        Field::new("hash", DataType::Utf8, false),
        Field::new("parent_hash", DataType::Utf8, true),
        Field::new("nonce", DataType::Utf8, true),
        Field::new("sha3_uncles", DataType::Utf8, true),
        Field::new("logs_bloom", DataType::Utf8, true),
        Field::new("transactions_root", DataType::Utf8, true),
        Field::new("state_root", DataType::Utf8, true),
        Field::new("receipts_root", DataType::Utf8, true),
        Field::new("mix_hash", DataType::Utf8, true),
        Field::new("miner", DataType::Utf8, true),
        Field::new("difficulty", DataType::Utf8, true),
        Field::new("total_difficulty", DataType::Utf8, true),
        Field::new("extra_data", DataType::Utf8, true),
        Field::new("size", DataType::Int32, true),
        Field::new("gas_limit", DataType::Utf8, true),
        Field::new("gas_used", DataType::Utf8, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(datatypes::TimeUnit::Millisecond, None),
            true
        ),
        Field::new("base_fee_per_gas", DataType::Utf8, true),
        Field::new("extra_data_size", DataType::Int64, true),
    ]);
    static ref TRANSACTIONS_SCHEMA: Schema = Schema::new(vec![
        Field::new("block_number", DataType::Int32, false),
        Field::new("transaction_index", DataType::Int32, false),
        Field::new("from", DataType::Utf8, true),
        Field::new("gas", DataType::Utf8, true),
        Field::new("gas_price", DataType::Utf8, true),
        Field::new("max_fee_per_gas", DataType::Utf8, true),
        Field::new("max_priority_fee_per_gas", DataType::Utf8, true),
        Field::new("hash", DataType::Utf8, true),
        Field::new("input", DataType::Utf8, true),
        Field::new("nonce", DataType::Int64, true),
        Field::new("to", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("v", DataType::Utf8, true),
        Field::new("r", DataType::Utf8, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("y_parity", DataType::Int8, true),
        Field::new("chain_id", DataType::UInt64, true),
        Field::new("sighash", DataType::Utf8, true),
        Field::new("gas_used", DataType::Utf8, true),
        Field::new("cumulative_gas_used", DataType::Utf8, true),
        Field::new("effective_gas_price", DataType::Utf8, true),
        Field::new("contract_address", DataType::Utf8, true),
        Field::new("type", DataType::UInt8, true),
        Field::new("status", DataType::Int8, true),
        Field::new("input_size", DataType::UInt64, true),
        Field::new("_idx", DataType::Int32, true),
    ]);
    static ref LOGS_SCHEMA: Schema = Schema::new(vec![
        Field::new("block_number", DataType::Int32, false),
        Field::new("transaction_index", DataType::Int32, false),
        Field::new("log_index", DataType::Int32, false),
        Field::new("transaction_hash", DataType::Utf8, true),
        Field::new("address", DataType::Utf8, true),
        Field::new("data", DataType::Utf8, true),
        Field::new("topic0", DataType::Utf8, true),
        Field::new("topic1", DataType::Utf8, true),
        Field::new("topic2", DataType::Utf8, true),
        Field::new("topic3", DataType::Utf8, true),
        Field::new("data_size", DataType::UInt64, true),
        Field::new("_idx", DataType::Int32, true),
    ]);
}

// Allows setting primary key unlike `SessionContext::register_parquet`.
// It doesn't affect performance right now but it may change in the future.
fn register_parquet(
    ctx: &SessionContext,
    name: &str,
    table_path: impl AsRef<Path>,
    schema: &Schema,
    pk_indices: Vec<usize>,
) -> Result<()> {
    let options = ParquetReadOptions::default().schema(&schema);
    let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(pk_indices)]);
    let listing_options = options.to_listing_options(&ctx.copied_config());
    let table_url = ListingTableUrl::parse(table_path.as_ref().to_string_lossy())?;
    let schema = Arc::new(schema.to_owned());
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .with_schema(schema);
    let provider = ListingTable::try_new(config)?.with_constraints(constraints);
    ctx.register_table(
        TableReference::Bare { table: name.into() },
        Arc::new(provider),
    )?;
    Ok(())
}

pub async fn prepare_query_context(path: &std::path::Path) -> anyhow::Result<SessionContext> {
    let ctx = SessionContext::new();
    register_parquet(
        &ctx,
        "blocks",
        path.join("blocks.parquet"),
        &BLOCKS_SCHEMA,
        vec![0],
    )?;
    register_parquet(
        &ctx,
        "transactions",
        path.join("transactions.parquet"),
        &TRANSACTIONS_SCHEMA,
        vec![0, 1],
    )?;
    register_parquet(
        &ctx,
        "logs",
        path.join("logs.parquet"),
        &LOGS_SCHEMA,
        vec![0, 1, 2],
    )?;
    ctx.register_parquet(
        "traces",
        path.join("traces.parquet").to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "statediffs",
        path.join("statediffs.parquet").to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    Ok(ctx)
}
