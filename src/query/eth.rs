// Copied from https://github.com/subsquid/firehose-grpc/blob/80bc1da04e4197df6fdf632937dec372794ddea0/src/archive.rs

use serde::{Deserialize, Serialize};
use serde_json::{Number, map::Map as JsonMap};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct BatchRequest {
    pub from_block: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_block: Option<u64>,
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub include_all_blocks: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<FieldSelection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs: Option<Vec<LogRequest>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions: Option<Vec<TxRequest>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traces: Option<Vec<TraceRequest>>,
    #[serde(default)]
    pub r#type: NetworkType,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FieldSelection {
    #[serde(deserialize_with = "parse_selection", default)]
    pub block: Vec<String>,
    #[serde(deserialize_with = "parse_selection", default)]
    pub log: Vec<String>,
    #[serde(deserialize_with = "parse_selection", default)]
    pub transaction: Vec<String>,
    #[serde(deserialize_with = "parse_selection", default)]
    pub trace: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct LogRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic0: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic1: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic2: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic3: Option<Vec<String>>,
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub transaction: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub transaction_traces: bool
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct TxRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sighash: Option<Vec<String>>,
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub logs: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub traces: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub state_diffs: bool,
    // TODO: add nonce
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all(deserialize = "camelCase"), default)]
pub struct TraceRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub call_to: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub call_sighash: Vec<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub transaction: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub transaction_logs: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub parents: bool,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all(deserialize = "camelCase"), default)]
pub struct _BlockFieldSelection {
    pub number: bool,
    pub hash: bool,
    pub parent_hash: bool,
    pub difficulty: bool,
    pub total_difficulty: bool,
    pub size: bool,
    pub sha3_uncles: bool,
    pub gas_limit: bool,
    pub gas_used: bool,
    pub timestamp: bool,
    pub miner: bool,
    pub state_root: bool,
    pub transactions_root: bool,
    pub receipts_root: bool,
    pub logs_bloom: bool,
    pub extra_data: bool,
    pub mix_hash: bool,
    pub base_fee_per_gas: bool,
    pub nonce: bool,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all(deserialize = "camelCase"), default)]
pub struct _LogFieldSelection {
    pub log_index: bool,
    pub transaction_index: bool,
    pub address: bool,
    pub data: bool,
    pub topics: bool,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all(deserialize = "camelCase"), default)]
pub struct _TxFieldSelection {
    pub transaction_index: bool,
    pub hash: bool,
    pub nonce: bool,
    pub from: bool,
    pub to: bool,
    pub input: bool,
    pub value: bool,
    pub gas: bool,
    pub gas_price: bool,
    pub max_fee_per_gas: bool,
    pub max_priority_fee_per_gas: bool,
    pub v: bool,
    pub r: bool,
    pub s: bool,
    pub y_parity: bool,
    pub gas_used: bool,
    pub cumulative_gas_used: bool,
    pub effective_gas_price: bool,
    pub r#type: bool,
    pub status: bool,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all(deserialize = "camelCase"), default)]
pub struct _TraceFieldSelection {
    pub transaction_index: bool,
    pub r#type: bool,
    pub error: bool,
    pub create_from: bool,
    pub create_value: bool,
    pub create_gas: bool,
    pub create_result_gas_used: bool,
    pub create_result_address: bool,
    pub call_from: bool,
    pub call_to: bool,
    pub call_value: bool,
    pub call_gas: bool,
    pub call_input: bool,
    pub call_type: bool,
    pub call_result_gas_used: bool,
    pub call_result_output: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct BlockHeader {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
    pub size: u64,
    pub sha3_uncles: String,
    pub miner: String,
    pub state_root: String,
    pub transactions_root: String,
    pub receipts_root: String,
    pub logs_bloom: String,
    pub difficulty: String,
    pub total_difficulty: String,
    pub gas_limit: String,
    pub gas_used: String,
    pub timestamp: Number,
    pub extra_data: String,
    pub mix_hash: String,
    pub nonce: String,
    pub base_fee_per_gas: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct Log {
    pub address: String,
    pub data: String,
    pub topics: Vec<String>,
    pub log_index: u32,
    pub transaction_index: u32,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct Transaction {
    pub transaction_index: u32,
    pub hash: String,
    pub nonce: u64,
    pub from: String,
    pub to: Option<String>,
    pub input: String,
    pub value: String,
    pub gas: String,
    pub gas_price: String,
    pub max_fee_per_gas: Option<String>,
    pub max_priority_fee_per_gas: Option<String>,
    pub v: String,
    pub r: String,
    pub s: String,
    pub y_parity: Option<u8>,
    pub gas_used: String,
    pub cumulative_gas_used: String,
    pub effective_gas_price: String,
    pub r#type: i32,
    pub status: i32,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum TraceType {
    Create,
    Call,
    Suicide,
    Reward,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum CallType {
    Call,
    Callcode,
    Delegatecall,
    Staticcall,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum NetworkType {
    #[default]
    Eth,
    Substrate,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct TraceAction {
    pub from: Option<String>,
    pub to: Option<String>,
    pub value: Option<String>,
    pub gas: Option<String>,
    pub input: Option<String>,
    pub r#type: Option<CallType>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct TraceResult {
    pub gas_used: Option<String>,
    pub address: Option<String>,
    pub output: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct Trace {
    pub transaction_index: u32,
    pub r#type: TraceType,
    pub error: Option<String>,
    #[serde(default)]
    pub revert_reason: Option<String>,
    pub action: Option<TraceAction>,
    pub result: Option<TraceResult>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Block {
    pub header: BlockHeader,
    pub logs: Option<Vec<Log>>,
    pub transactions: Option<Vec<Transaction>>,
    pub traces: Option<Vec<Trace>>,
}

fn parse_selection<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let map =JsonMap::deserialize(deserializer)?;
    let mut result = Vec::new();
    for (key, value) in map {
        if value.as_bool() == Some(true) {
            result.push(key);
        }
    }
    Ok(result)
}