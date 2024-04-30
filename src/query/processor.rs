use std::{collections::HashSet, hash::Hash, pin::Pin};

use super::{
    error::QueryError,
    eth::{BatchRequest, NetworkType},
};
use anyhow::Context;
use async_stream::try_stream;
use datafusion::{
    arrow::{
        datatypes::DataType, json::writer::record_batches_to_json_rows, record_batch::RecordBatch,
    },
    error::DataFusionError,
    prelude::*,
    scalar::ScalarValue,
};
use futures::{stream::Peekable, try_join, Stream, StreamExt};
use itertools::Itertools;
use serde_json::{map::Map as JsonMap, Value};
use serde_rename_rule::RenameRule;
use tracing::instrument;

pub(super) type QueryResult = Vec<Value>;

// TODO:
// - optimize queries
// - stream the results
// - limit results size
// - support traces and state diffs querying
// - support substrate networks
// - generalize this code

#[instrument(skip_all)]
pub async fn process_query(
    ctx: &SessionContext,
    query: BatchRequest,
) -> Result<QueryResult, QueryError> {
    if query.r#type != NetworkType::Eth {
        return Err(QueryError::BadRequest(
            "only eth queries are supported".to_owned(),
        ));
    }
    let (blocks, transactions, logs) = extract_data(ctx, &query).await?;
    let blocks = convert_to_json(blocks);
    let transactions = transactions.map(convert_to_json);
    let logs = logs.map(convert_to_json);
    collect_result(build_response(&query, blocks, transactions, logs))
        .await
        .map_err(From::from)
}

#[instrument(skip_all)]
async fn extract_data(
    ctx: &SessionContext,
    query: &BatchRequest,
) -> Result<
    (
        impl Stream<Item = Result<RecordBatch, DataFusionError>>,
        Option<impl Stream<Item = Result<RecordBatch, DataFusionError>>>,
        Option<impl Stream<Item = Result<RecordBatch, DataFusionError>>>,
    ),
    QueryError,
> {
    let blocks = ctx.table("blocks").await?;
    let transactions = ctx.table("transactions").await?;
    let logs = ctx.table("logs").await?;

    let range_filter = |column| {
        if let Some(to_block) = query.to_block {
            col(column).between(lit(query.from_block), lit(to_block))
        } else {
            col(column).gt_eq(lit(query.from_block))
        }
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

    let mut logs_filters = Vec::new();
    let mut tx_filters = Vec::new();
    let mut tx_by_logs_filters = Vec::new();
    let mut logs_by_tx_filters = Vec::new();

    for tx_request in query.transactions.as_ref().unwrap_or(&Vec::new()) {
        let mut filters = Vec::new();
        filters.extend(field_in("from", &tx_request.from));
        filters.extend(field_in("to", &tx_request.to));
        filters.extend(field_in("sighash", &tx_request.sighash));

        let predicate = all_of(filters).unwrap_or(lit(true));
        if tx_request.logs {
            logs_by_tx_filters.push(predicate.clone());
        }
        tx_filters.push(predicate);

        if tx_request.traces {
            return Err(QueryError::BadRequest(
                "Traces queries are not supported yet".to_owned(),
            ));
        }
        if tx_request.state_diffs {
            return Err(QueryError::BadRequest(
                "State diffs queries are not supported yet".to_owned(),
            ));
        }
    }

    for log_request in query.logs.as_ref().unwrap_or(&Vec::new()) {
        let mut filters = Vec::new();
        filters.extend(field_in("address", &log_request.address));
        filters.extend(field_in("topic0", &log_request.topic0));
        filters.extend(field_in("topic1", &log_request.topic1));
        filters.extend(field_in("topic2", &log_request.topic2));
        filters.extend(field_in("topic3", &log_request.topic3));

        let predicate = all_of(filters).unwrap_or(lit(true));
        if log_request.transaction {
            tx_by_logs_filters.push(predicate.clone());
        }
        logs_filters.push(predicate);

        if log_request.transaction_traces {
            return Err(QueryError::BadRequest(
                "Traces queries are not supported yet".to_owned(),
            ));
        }
    }
    if query.traces.is_some() {
        return Err(QueryError::BadRequest(
            "Traces queries are not supported yet".to_owned(),
        ));
    }

    let blocks = camel_case_columns(all_blocks)?.select_columns(&block_columns(query))?;

    let mut tx_selections = Vec::new();
    if let Some(filter) = any_of(tx_filters) {
        tx_selections.push(all_transactions.clone().filter(filter)?);
    }
    if let Some(filter) = any_of(tx_by_logs_filters) {
        let key = ["block_number", "transaction_index"];
        tx_selections.push(all_transactions.clone().join(
            all_logs.clone(),
            JoinType::LeftSemi,
            &key,
            &key,
            Some(filter),
        )?);
    }
    let transactions = match union_all(tx_selections)? {
        Some(union) => {
            let result = camel_case_columns(union)?
                .select_columns(&tx_columns(query))?
                .sort(vec![
                    col("\"blockNumber\"").sort(true, true),
                    col("\"transactionIndex\"").sort(true, true),
                ])?;
            Some(result)
        }
        None => None,
    };

    let mut logs_selections = Vec::new();
    if let Some(filter) = any_of(logs_filters) {
        logs_selections.push(all_logs.clone().filter(filter)?);
    }
    if let Some(filter) = any_of(logs_by_tx_filters) {
        let key = ["block_number", "transaction_index"];
        logs_selections.push(all_logs.clone().join(
            all_transactions.clone(),
            JoinType::LeftSemi,
            &key,
            &key,
            Some(filter),
        )?);
    }
    let logs = match union_all(logs_selections)? {
        Some(union) => {
            let result = camel_case_columns(union)?
                .select_columns(&log_columns(query))?
                .sort(vec![
                    col("\"blockNumber\"").sort(true, true),
                    col("\"transactionIndex\"").sort(true, true),
                    col("\"logIndex\"").sort(true, true),
                ])?;
            Some(result)
        }
        None => None,
    };

    let blocks_future = tokio::spawn(blocks.execute_stream());
    let tx_future = tokio::spawn(async move {
        match transactions {
            Some(transactions) => transactions.execute_stream().await.map(Some),
            None => Ok(None),
        }
    });
    let logs_future = tokio::spawn(async {
        match logs {
            Some(logs) => logs.execute_stream().await.map(Some),
            None => Ok(None),
        }
    });
    let (blocks_result, tx_result, logs_result) = try_join!(blocks_future, tx_future, logs_future)
        .context("Subqueries execution panicked")?;

    Ok((blocks_result?, tx_result?, logs_result?))
}

#[instrument(skip_all)]
fn convert_to_json(
    stream: impl Stream<Item = Result<RecordBatch, DataFusionError>>,
) -> impl Stream<Item = Result<JsonMap<String, Value>, DataFusionError>> {
    stream.flat_map(|record_batch| {
        let entries = match record_batch
            .and_then(|batch| record_batches_to_json_rows(&[&batch]).map_err(From::from))
        {
            Ok(vec) => vec.into_iter().map(Ok).collect_vec(),
            Err(e) => vec![Err(e)],
        };
        futures::stream::iter(entries)
    })
}
#[instrument(skip_all)]
fn build_response<'l>(
    query: &'l BatchRequest,
    headers: impl Stream<Item = Result<JsonMap<String, Value>, DataFusionError>> + Unpin + 'l,
    transactions: Option<impl Stream<Item = Result<JsonMap<String, Value>, DataFusionError>> + 'l>,
    logs: Option<impl Stream<Item = Result<JsonMap<String, Value>, DataFusionError>> + 'l>,
) -> impl Stream<Item = anyhow::Result<Value>> + 'l {
    let include_tx = transactions.is_some();
    let include_logs = logs.is_some();
    let mut transactions = transactions.map(|stream| Box::pin(stream.peekable()));
    let mut logs = logs.map(|stream| Box::pin(stream.peekable()));

    async fn block_rows<S: Stream<Item = Result<JsonMap<String, Value>, DataFusionError>>>(
        stream: &mut Option<Pin<Box<Peekable<S>>>>,
        block_number: u64,
    ) -> Result<Vec<JsonMap<String, Value>>, DataFusionError> {
        if let Some(stream) = stream.as_mut() {
            consume_while(stream.as_mut(), |tx: &JsonMap<String, Value>| {
                tx.get("blockNumber").unwrap().as_u64().unwrap() == block_number
            })
            .await
        } else {
            Ok(Vec::new())
        }
    }

    try_stream! {
        let mut last_block = None;
        let mut headers = headers.enumerate();
        while let Some((header_index, header)) = headers.next().await {
            let header = header?;
            let mut block = JsonMap::new();
            let number = header
                .get("number")
                .context("Found block without number")?
                .as_u64()
                .unwrap();
            let mut include_block = false;

            block.insert("header".to_owned(), serde_json::to_value(&header)?);

            let mut transactions = block_rows(&mut transactions, number).await?;
            for tx in transactions.iter_mut() {
                tx.retain(|key, _| key != "blockNumber");
                add_nulls(tx, query.fields.as_ref().map(|f| &f.transaction));
            }
            if !transactions.is_empty() {
                include_block = true;
            }
            if !transactions.is_empty() || include_tx {
                block.insert(
                    "transactions".to_owned(),
                    serde_json::to_value(transactions)?,
                );
            }

            let mut logs = block_rows(&mut logs, number).await?;
            for log in logs.iter_mut() {
                log.retain(|key, _| key != "blockNumber");
                add_nulls(log, query.fields.as_ref().map(|f| &f.log));
            }
            if !logs.is_empty() {
                include_block = true;
            }
            if !logs.is_empty() || include_logs {
                block.insert(
                    "logs".to_owned(),
                    serde_json::to_value(logs)?,
                );
            }

            if include_block || query.include_all_blocks || header_index == 0 {
                yield Value::Object(block);
                last_block = None;
            } else {
                last_block = Some(block);
            }
        }
        if let Some(block) = last_block {
            yield Value::Object(block);
        }
    }
}

async fn consume_while<T, E>(
    mut stream: std::pin::Pin<&mut Peekable<impl Stream<Item = Result<T, E>>>>,
    f: impl Fn(&T) -> bool,
) -> Result<Vec<T>, E> {
    let mut result = Vec::new();
    while let Some(item) = stream
        .as_mut()
        .next_if(|item| match item {
            Err(_) => true,
            Ok(item) => f(item),
        })
        .await
    {
        match item {
            Ok(item) => {
                result.push(item);
            }
            Err(e) => return Err(e),
        }
    }
    Ok(result)
}

#[instrument(skip_all)]
async fn collect_result(
    stream: impl Stream<Item = anyhow::Result<Value>>,
) -> anyhow::Result<QueryResult> {
    let mut result = Vec::new();
    tokio::pin!(stream);
    while let Some(row) = stream.next().await {
        if result.is_empty() {
            tracing::trace!("Got first result row");
        }
        result.push(row?);
    }
    tracing::trace!("Got all result rows");
    Ok(result)
}

fn camel_case_columns(df: DataFrame) -> Result<DataFrame, DataFusionError> {
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

fn union_all(dataframes: Vec<DataFrame>) -> Result<Option<DataFrame>, DataFusionError> {
    let mut iter = dataframes.into_iter();
    if let Some(first) = iter.next() {
        let mut result = first;
        for df in iter {
            // TODO: use distinct_by
            result = result.union_distinct(df)?;
        }
        Ok(Some(result))
    } else {
        Ok(None)
    }
}
