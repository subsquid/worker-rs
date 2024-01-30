use std::{collections::HashSet, hash::Hash};

use super::eth::{BatchRequest, NetworkType};
use anyhow::{ensure, Context, Result};
use datafusion::{
    arrow::{
        datatypes::DataType, json::writer::record_batches_to_json_rows, record_batch::RecordBatch,
    },
    prelude::*,
    scalar::ScalarValue,
};
use futures::join;
use itertools::{Itertools, Position};
use serde_json::{map::Map as JsonMap, Value};
use serde_rename_rule::RenameRule;
use tracing::instrument;

// TODO:
// - optimize queries
// - support traces and state diffs querying
// - support substrate networks
// - generalize this code

pub async fn process_query(ctx: &SessionContext, query: BatchRequest) -> Result<Vec<Value>> {
    anyhow::ensure!(
        query.r#type == NetworkType::Eth,
        "only eth queries are supported"
    );
    let (blocks, transactions, logs) = extract_data(ctx, &query).await?;
    let blocks = convert_to_json(blocks)?;
    let transactions = transactions.map(convert_to_json).transpose()?;
    let logs = logs.map(convert_to_json).transpose()?;
    build_response(&query, blocks, transactions, logs)
}

#[instrument(err, skip(ctx))]
async fn extract_data(
    ctx: &SessionContext,
    query: &BatchRequest,
) -> Result<(
    Vec<RecordBatch>,
    Option<Vec<RecordBatch>>,
    Option<Vec<RecordBatch>>,
)> {
    let blocks = ctx.table("blocks").await?;
    let transactions = ctx.table("transactions").await?;
    let logs = ctx.table("logs").await?;

    let range_filter = |column| {
        col(column).between(
            lit(query.from_block),
            lit(query
                .to_block
                .expect("Queries without 'to_block' are not supported yet")),
        )
    };
    let all_blocks = blocks
        .filter(range_filter("number"))?
        .with_column(
            "timestamp",
            cast(to_timestamp_seconds(col("timestamp")), DataType::UInt64),
        )?
        .sort(vec![col("number").sort(true, true)])?;
    let all_transactions = transactions.filter(range_filter("block_number"))?;
    let all_logs = logs.filter(range_filter("block_number"))?.with_column(
        "topics",
        array_remove_all(
            array(vec![
                col("topic0"),
                col("topic1"),
                col("topic2"),
                col("topic3"),
            ]),
            lit(ScalarValue::Utf8(None)),
        ),
    )?;

    let mut selected_logs = Vec::new();
    let mut selected_transcations = Vec::new();

    if let Some(requests) = &query.transactions {
        for request in requests {
            let mut filters = Vec::new();
            filters.extend(field_in("from", &request.from));
            filters.extend(field_in("to", &request.to));
            filters.extend(field_in("sighash", &request.sighash));
            let all_transactions = all_transactions.clone();
            let selected = if let Some(predicate) = all_of(filters) {
                all_transactions.filter(predicate)?
            } else {
                all_transactions
            };
            selected_transcations.push(selected.clone());
            if request.logs {
                let key = &["block_number", "transaction_index"];
                selected_logs.push(
                    selected
                        .clone()
                        .select_columns(key)?
                        .distinct()?
                        .join(all_logs.clone(), JoinType::Inner, key, key, None)?
                        .select(
                            all_logs
                                .schema()
                                .fields()
                                .iter()
                                .map(|x| col(x.qualified_name()))
                                .collect_vec(),
                        )?,
                );
            }
            ensure!(!request.traces, "Traces queries are not supported yet");
            ensure!(
                !request.state_diffs,
                "State diffs queries are not supported yet"
            );
        }
    }
    if let Some(requests) = &query.logs {
        for request in requests {
            let mut filters = Vec::new();
            filters.extend(field_in("address", &request.address));
            filters.extend(field_in("topic0", &request.topic0));
            filters.extend(field_in("topic1", &request.topic1));
            filters.extend(field_in("topic2", &request.topic2));
            filters.extend(field_in("topic3", &request.topic3));
            let all_logs = all_logs.clone();
            let selected = if let Some(predicate) = all_of(filters) {
                all_logs.filter(predicate)?
            } else {
                all_logs
            };
            selected_logs.push(selected.clone());
            if request.transaction {
                let key = &["block_number", "transaction_index"];
                selected_transcations.push(
                    selected
                        .select_columns(key)?
                        .distinct()?
                        .join(all_transactions.clone(), JoinType::Inner, key, key, None)?
                        .select(
                            all_transactions
                                .schema()
                                .fields()
                                .iter()
                                .map(|x| col(x.qualified_name()))
                                .collect_vec(),
                        )?,
                );
            }
            ensure!(
                !request.transaction_traces,
                "Traces queries are not supported yet"
            );
        }
    }
    ensure!(
        query.traces.is_none(),
        "Traces queries are not supported yet"
    );

    let blocks = camel_case_columns(all_blocks)?.select_columns(&block_columns(query))?;
    let transactions = union_all(selected_transcations)?
        .map(|transactions| -> Result<DataFrame> {
            Ok(camel_case_columns(transactions)?
                .select_columns(&tx_columns(query))?
                .sort(vec![
                    col("\"blockNumber\"").sort(true, true),
                    col("\"transactionIndex\"").sort(true, true),
                ])?)
        })
        .transpose()?;
    let logs = union_all(selected_logs)?
        .map(|logs| -> Result<DataFrame> {
            Ok(camel_case_columns(logs)?
                .select_columns(&log_columns(query))?
                .sort(vec![
                    col("\"blockNumber\"").sort(true, true),
                    col("\"transactionIndex\"").sort(true, true),
                    col("\"logIndex\"").sort(true, true),
                ])?)
        })
        .transpose()?;

    let tx_future = async move {
        match transactions {
            Some(transactions) => transactions.collect().await.map(Some),
            None => Ok(None),
        }
    };
    let logs_future = async {
        match logs {
            Some(logs) => logs.collect().await.map(Some),
            None => Ok(None),
        }
    };
    let (blocks_result, tx_result, logs_result) = join!(blocks.collect(), tx_future, logs_future);

    Ok((blocks_result?, tx_result?, logs_result?))
}

#[instrument(err)]
fn convert_to_json(result: Vec<RecordBatch>) -> Result<Vec<JsonMap<String, Value>>> {
    Ok(record_batches_to_json_rows(
        &result.iter().collect::<Vec<_>>(),
    )?)
}

#[instrument(err, skip_all)]
fn build_response(
    query: &BatchRequest,
    headers: Vec<JsonMap<String, Value>>,
    transactions: Option<Vec<JsonMap<String, Value>>>,
    logs: Option<Vec<JsonMap<String, Value>>>,
) -> Result<Vec<Value>> {
    let include_tx = transactions.is_some();
    let include_logs = logs.is_some();
    let block_txs = transactions
        .map(|transactions| {
            transactions
                .into_iter()
                .map(|mut tx: JsonMap<String, Value>| {
                    let number = tx.get("blockNumber").unwrap().as_u64().unwrap();
                    tx.retain(|key, _| key != "blockNumber");
                    add_nulls(&mut tx, query.fields.as_ref().map(|f| &f.transaction));
                    (number, tx)
                })
                .into_group_map()
        })
        .unwrap_or_default();
    let block_logs = logs
        .map(|logs| {
            logs.into_iter()
                .map(|mut log: JsonMap<String, Value>| {
                    let number = log.get("blockNumber").unwrap().as_u64().unwrap();
                    log.retain(|key, _| key != "blockNumber");
                    add_nulls(&mut log, query.fields.as_ref().map(|f| &f.log));
                    (number, log)
                })
                .into_group_map()
        })
        .unwrap_or_default();
    let mut blocks = Vec::new();
    for (block_position, header) in headers.into_iter().with_position() {
        let mut block = JsonMap::new();
        block.insert("header".to_owned(), serde_json::to_value(&header)?);
        let number = header
            .get("number")
            .context("Found block without number")?
            .as_u64()
            .unwrap();
        let mut include_block = false;
        if let Some(transactions) = block_txs.get(&number) {
            block.insert(
                "transactions".to_owned(),
                serde_json::to_value(transactions)?,
            );
            include_block = true;
        } else {
            if include_tx {
                block.insert("transactions".to_owned(), Value::Array(Vec::new()));
            }
        }
        if let Some(logs) = block_logs.get(&number) {
            block.insert("logs".to_owned(), serde_json::to_value(logs)?);
            include_block = true;
        } else {
            if include_logs {
                block.insert("logs".to_owned(), Value::Array(Vec::new()));
            }
        }
        if include_block || query.include_all_blocks || block_position != Position::Middle {
            blocks.push(Value::Object(block));
        }
    }
    Ok(blocks)
}

fn camel_case_columns(df: DataFrame) -> Result<DataFrame> {
    let columns = df
        .schema()
        .fields()
        .iter()
        .map(|f| f.qualified_column())
        .collect_vec();
    df.select(
        columns
            .into_iter()
            .map(|c| col(c.clone()).alias(RenameRule::CamelCase.apply_to_field(&c.name)))
            .collect_vec(),
    )
    .map_err(Into::into)
}

fn block_columns(query: &BatchRequest) -> Vec<&str> {
    merge_ordered(
        vec!["number", "hash", "parentHash"],
        query
            .fields
            .as_ref()
            .map(|fields| fields.block.iter().map(|s| s.as_str()).collect_vec())
            .unwrap_or_default(),
    )
}

fn tx_columns(query: &BatchRequest) -> Vec<&str> {
    merge_ordered(
        vec!["blockNumber", "transactionIndex"],
        query
            .fields
            .as_ref()
            .map(|fields| fields.transaction.iter().map(|s| s.as_str()).collect_vec())
            .unwrap_or_default(),
    )
}

fn log_columns(query: &BatchRequest) -> Vec<&str> {
    merge_ordered(
        vec!["blockNumber", "logIndex", "transactionIndex"],
        query
            .fields
            .as_ref()
            .map(|fields: &super::eth::FieldSelection| {
                fields.log.iter().map(|s| s.as_str()).collect_vec()
            })
            .unwrap_or_default(),
    )
}

fn merge_ordered<T: Clone + Hash + Eq>(left: Vec<T>, right: Vec<T>) -> Vec<T> {
    let mut result = left.clone();
    let mut seen: HashSet<T> = HashSet::from_iter(left);
    for entry in right {
        if !seen.contains(&entry) {
            result.push(entry.clone());
            seen.insert(entry);
        }
    }
    result
}

fn add_nulls(value: &mut JsonMap<String, Value>, fields: Option<&Vec<String>>) {
    if let Some(fields) = fields {
        for field in fields {
            value.entry(field).or_insert(Value::Null);
        }
    }
}

fn field_in(field: &str, values: &Option<Vec<String>>) -> Option<Expr> {
    values
        .as_ref()
        .map(|values| col(field).in_list(values.iter().map(lit).collect(), false))
}

fn all_of(predicates: Vec<Expr>) -> Option<Expr> {
    predicates.into_iter().reduce(and)
}

fn any_of(predicates: Vec<Expr>) -> Option<Expr> {
    predicates.into_iter().reduce(or)
}

fn union_all(dataframes: Vec<DataFrame>) -> Result<Option<DataFrame>> {
    let mut iter = dataframes.into_iter();
    if let Some(first) = iter.next() {
        let mut result = first;
        for df in iter {
            result = result.union_distinct(df)?;
        }
        Ok(Some(result))
    } else {
        Ok(None)
    }
}
