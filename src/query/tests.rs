use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use datafusion::prelude::*;
use tracing::{info, warn};

use crate::query::eth::*;
use crate::query::processor::process_query;
use crate::util::tests::{setup_tracing, tests_data};

async fn prepare_context() -> Result<SessionContext> {
    let root = tests_data().join("0017881390/0017881390-0017882786-32ee9457");
    crate::query::context::prepare_query_context(&root).await
}

fn list_fixtures() -> HashMap<String, (PathBuf, PathBuf)> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut result = HashMap::new();
    for entry in std::fs::read_dir(&path).unwrap() {
        let entry = entry.unwrap();
        let filename = entry.file_name().into_string().unwrap();
        if filename.ends_with(".result.json") || filename.ends_with(".actual.json") {
            continue;
        }
        let name: String = filename.strip_suffix(".json").unwrap().to_owned();
        let result_path = path.join([&name, ".result.json"].join(""));
        assert!(result_path.exists());
        result.insert(name, (entry.path(), result_path));
    }
    result
}

#[tokio::test]
async fn test_schema() -> Result<()> {
    let ctx = prepare_context().await?;
    let blocks = ctx.table("blocks").await?;
    let _traces = ctx.table("traces").await?;
    let schema = blocks.schema();
    for field in schema.fields() {
        println!("{:?}: {:?}", field.name(), field.data_type());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_query() -> Result<()> {
    setup_tracing()?;
    let ctx = prepare_context().await?;
    for (query_name, (query_path, result_path)) in list_fixtures() {
        if let Ok(requested) = std::env::var("QUERY") {
            if query_name != requested {
                continue;
            }
        }
        let query: BatchRequest = serde_json::from_reader(std::fs::File::open(&query_path)?)?;
        let expected: Vec<serde_json::Value> =
            serde_json::from_reader(std::fs::File::open(result_path)?)?;
        info!("Running query {}", query_name);
        let result = process_query(&ctx, query).await;
        match result {
            Ok(result) => {
                if result != expected {
                    warn!("Test failed. Saving actual result");
                    let file = std::fs::File::create(query_path.with_extension("actual.json"))?;
                    serde_json::to_writer_pretty(std::io::BufWriter::new(file), &result)?;
                }
            }
            Err(err) => warn!("Query failed: {:?}", err),
        }
    }
    info!("Done");
    Ok(())
}

#[cfg(bench)]
mod bench {
    extern crate test;

    use duckdb::{params, Connection, Result};
    use test::Bencher;

    #[bench]
    fn bench_sql_full(b: &mut Bencher) -> Result<()> {
        const QUERY: &str = r#"
WITH
blocks AS (SELECT * FROM read_parquet($4)),
transactions AS (SELECT * FROM read_parquet($5)),
requested_transactions AS (SELECT block_number, transaction_index, $6 AS include_logs, $7 AS include_traces, $8 AS include_stateDiffs FROM transactions),
logs AS (SELECT * FROM read_parquet($9)),
requested_logs AS (SELECT block_number, log_index, transaction_index, $10 AS include_transaction, $11 AS include_transactionTraces FROM logs),
selected_transactions AS (SELECT block_number, transaction_index FROM requested_transactions),
selected_logs AS (SELECT block_number, log_index FROM requested_logs),
item_stats AS (SELECT block_number, SUM(weight) AS weight FROM (SELECT block_number, $12 AS weight FROM selected_transactions UNION ALL SELECT block_number, $13 AS weight FROM selected_logs) GROUP BY block_number),
block_stats AS (SELECT block_number, SUM($14 + weight) OVER (ORDER BY block_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS size FROM (SELECT number AS block_number, coalesce(item_stats.weight, 0) AS weight FROM blocks LEFT OUTER JOIN item_stats ON item_stats.block_number = blocks.number) WHERE block_number BETWEEN $2 AND $3),
selected_blocks AS (SELECT * FROM blocks WHERE number IN (SELECT block_number FROM block_stats WHERE size <= $1 UNION ALL (SELECT block_number FROM block_stats WHERE size > $1 ORDER BY block_number LIMIT 1)) ORDER BY number),
result AS (SELECT json_object('number', "number", 'hash', "hash", 'parentHash', "parent_hash", 'difficulty', "difficulty", 'size', "size", 'sha3Uncles', "sha3_uncles", 'gasLimit', "gas_limit", 'gasUsed', "gas_used", 'timestamp', epoch(timestamp), 'nonce', "nonce") AS header,
    coalesce((SELECT list(json_object('transactionIndex', t."transaction_index", 'from', t."from", 'to', t."to", 'hash', t."hash", 'cumulativeGasUsed', t."cumulative_gas_used", 'gas', t."gas", 'gasPrice', t."gas_price", 'gasUsed', t."gas_used", 'effectiveGasPrice', t."effective_gas_price", 'maxFeePerGas', t."max_fee_per_gas", 'maxPriorityFeePerGas', t."max_priority_fee_per_gas", 'nonce', t."nonce", 'input', t."input", 'contractAddress', t."contract_address", 'type', t."type", 'value', t."value", 'status', t."status", 'chainId', IF(t.chain_id > 9007199254740991, to_json(t.chain_id::text), to_json(t.chain_id))) ORDER BY t.transaction_index) FROM transactions AS t, selected_transactions AS s WHERE t.block_number = s.block_number AND t.transaction_index = s.transaction_index AND t.block_number = b.number ), list_value()) as transactions,
    coalesce((SELECT list(json_object('logIndex', t."log_index", 'transactionIndex', t."transaction_index", 'address', t."address", 'topics', [topic for topic in list_value(t.topic0, t.topic1, t.topic2, t.topic3) if topic is not null], 'data', t."data", 'transactionHash', t."transaction_hash") ORDER BY t.log_index) FROM logs AS t, selected_logs AS s WHERE t.block_number = s.block_number AND t.log_index = s.log_index AND t.block_number = b.number ), list_value()) AS logs
FROM selected_blocks AS b)
SELECT * FROM result"#;
        let conn = Connection::open_in_memory()?;
        let mut stmt = conn.prepare(QUERY)?;

        b.iter(|| {
            stmt.query_arrow(params![
                1000000,
                17881390,
                17881395,
                "tests/data/0017881390/0017881390-0017882786-32ee9457/blocks.parquet",
                "tests/data/0017881390/0017881390-0017882786-32ee9457/transactions.parquet",
                "False",
                "False",
                "False",
                "tests/data/0017881390/0017881390-0017882786-32ee9457/logs.parquet",
                "False",
                "False",
                25,
                9,
                10
            ])
            .unwrap()
            .collect::<Vec<_>>()
        });
        Ok(())
    }

    #[bench]
    fn bench_sql_simple(b: &mut Bencher) -> Result<()> {
        const QUERY: &str = r#"
WITH
blocks AS (SELECT * FROM read_parquet($4)),
transactions AS (SELECT * FROM read_parquet($5)),
logs AS (SELECT * FROM read_parquet($9)),
result AS (SELECT json_object('number', "number", 'hash', "hash", 'parentHash', "parent_hash", 'difficulty', "difficulty", 'size', "size", 'sha3Uncles', "sha3_uncles", 'gasLimit', "gas_limit", 'gasUsed', "gas_used", 'timestamp', epoch(timestamp), 'nonce', "nonce") AS header,
    coalesce((SELECT list(json_object('transactionIndex', t."transaction_index", 'from', t."from", 'to', t."to", 'hash', t."hash", 'cumulativeGasUsed', t."cumulative_gas_used", 'gas', t."gas", 'gasPrice', t."gas_price", 'gasUsed', t."gas_used", 'effectiveGasPrice', t."effective_gas_price", 'maxFeePerGas', t."max_fee_per_gas", 'maxPriorityFeePerGas', t."max_priority_fee_per_gas", 'nonce', t."nonce", 'input', t."input", 'contractAddress', t."contract_address", 'type', t."type", 'value', t."value", 'status', t."status", 'chainId', IF(t.chain_id > 9007199254740991, to_json(t.chain_id::text), to_json(t.chain_id))) ORDER BY t.transaction_index) FROM transactions AS t WHERE t.block_number = b.number ), list_value()) as transactions,
    coalesce((SELECT list(json_object('logIndex', t."log_index", 'transactionIndex', t."transaction_index", 'address', t."address", 'topics', [topic for topic in list_value(t.topic0, t.topic1, t.topic2, t.topic3) if topic is not null], 'data', t."data", 'transactionHash', t."transaction_hash") ORDER BY t.log_index) FROM logs AS t WHERE t.block_number = b.number ), list_value()) AS logs
FROM blocks AS b)
SELECT * FROM result"#;
        let conn = Connection::open_in_memory()?;
        let mut stmt = conn.prepare(QUERY)?;

        b.iter(|| {
            stmt.query_arrow(params![
                1000000,
                17881390,
                17881395,
                "tests/data/0017881390/0017881390-0017882786-32ee9457/blocks.parquet",
                "tests/data/0017881390/0017881390-0017882786-32ee9457/transactions.parquet",
                "False",
                "False",
                "False",
                "tests/data/0017881390/0017881390-0017882786-32ee9457/logs.parquet",
                "False",
                "False",
                25,
                9,
                10
            ])
            .unwrap()
            .collect::<Vec<_>>()
        });
        Ok(())
    }
}
