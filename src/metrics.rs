use std::fmt::Write;

use prometheus_client::encoding::{EncodeLabelSet, LabelValueEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::{family::Family, gauge::Gauge, histogram::Histogram, info::Info};
use prometheus_client::registry::{Registry, Unit};

use crate::query::result::{QueryError, QueryResult};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum WorkerStatus {
    Starting,
    NotRegistered,
    UnsupportedVersion,
    Jailed,
    Active,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum QueryStatus {
    Ok,
    BadRequest,
    NoAllocation,
    ServerError,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StatusLabels {
    worker_status: WorkerStatus,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct QueryExecutedLabels {
    status: QueryStatus,
}

lazy_static::lazy_static! {
    static ref STATUS: Family<StatusLabels, Gauge> = Default::default();
    pub static ref CHUNKS_AVAILABLE: Gauge = Default::default();
    pub static ref CHUNKS_DOWNLOADING: Gauge = Default::default();
    pub static ref CHUNKS_PENDING: Gauge = Default::default();
    pub static ref CHUNKS_DOWNLOADED: Counter = Default::default();
    pub static ref CHUNKS_FAILED_DOWNLOAD: Counter = Default::default();
    pub static ref CHUNKS_REMOVED: Counter = Default::default();
    pub static ref STORED_BYTES: Gauge = Default::default();

    static ref QUERY_EXECUTED: Family<QueryExecutedLabels, Counter> = Default::default();
    static ref QUERY_RESULT_SIZE: Histogram = Histogram::new(std::iter::empty());
    static ref READ_CHUNKS: Histogram = Histogram::new(std::iter::empty());
    pub static ref RUNNING_QUERIES: Gauge = Default::default();
}

pub fn set_status(status: WorkerStatus) {
    STATUS.clear();
    STATUS
        .get_or_create(&StatusLabels {
            worker_status: status,
        })
        .set(1);
}

pub fn query_executed(result: &QueryResult) {
    let (status, result) = match result {
        Ok(result) => (QueryStatus::Ok, Some(result)),
        Err(QueryError::NoAllocation) => (QueryStatus::NoAllocation, None),
        Err(QueryError::NotFound | QueryError::BadRequest(_)) => {
            (QueryStatus::BadRequest, None)
        }
        Err(QueryError::Other(_) | QueryError::ServiceOverloaded) => {
            (QueryStatus::ServerError, None)
        }
    };
    QUERY_EXECUTED
        .get_or_create(&QueryExecutedLabels { status })
        .inc();
    if let Some(result) = result {
        QUERY_RESULT_SIZE.observe(result.compressed_size as f64);
        READ_CHUNKS.observe(result.num_read_chunks as f64);
    }
}

pub fn register_metrics(registry: &mut Registry, info: Info<Vec<(String, String)>>) {
    registry.register("worker_info", "Worker info", info);
    registry.register(
        "chunks_available",
        "Number of available chunks",
        CHUNKS_AVAILABLE.clone(),
    );
    registry.register(
        "chunks_downloading",
        "Number of chunks being downloaded",
        CHUNKS_DOWNLOADING.clone(),
    );
    registry.register(
        "chunks_pending",
        "Number of chunks pending download",
        CHUNKS_PENDING.clone(),
    );
    registry.register(
        "chunks_downloaded",
        "Number of chunks downloaded",
        CHUNKS_DOWNLOADED.clone(),
    );
    registry.register(
        "chunks_failed_download",
        "Number of chunks failed to download",
        CHUNKS_FAILED_DOWNLOAD.clone(),
    );
    registry.register(
        "chunks_removed",
        "Number of removed chunks",
        CHUNKS_REMOVED.clone(),
    );
    registry.register_with_unit(
        "used_storage",
        "Total bytes stored in the data directory",
        Unit::Bytes,
        STORED_BYTES.clone(),
    );

    registry.register(
        "num_queries_executed",
        "Number of executed queries",
        QUERY_EXECUTED.clone(),
    );
    registry.register_with_unit(
        "query_result_size",
        "(Gzipped) result size of an executed query (bytes)",
        Unit::Bytes,
        QUERY_RESULT_SIZE.clone(),
    );
    registry.register(
        "num_read_chunks",
        "Number of chunks read during query execution",
        READ_CHUNKS.clone(),
    );
    registry.register(
        "running_queries",
        "Current number of queries being executed",
        RUNNING_QUERIES.clone(),
    );
}

pub fn register_p2p_metrics(registry: &mut Registry) {
    registry.register("worker_status", "Status of the worker", STATUS.clone());
    set_status(WorkerStatus::Starting);
}

impl prometheus_client::encoding::EncodeLabelValue for WorkerStatus {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        let status = match self {
            WorkerStatus::Starting => "starting",
            WorkerStatus::NotRegistered => "not_registered",
            WorkerStatus::UnsupportedVersion => "unsupported_version",
            WorkerStatus::Jailed => "jailed",
            WorkerStatus::Active => "active",
        };
        encoder.write_str(status)?;
        Ok(())
    }
}

impl prometheus_client::encoding::EncodeLabelValue for QueryStatus {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        let status = match self {
            QueryStatus::Ok => "ok",
            QueryStatus::BadRequest => "bad_request",
            QueryStatus::NoAllocation => "no_allocation",
            QueryStatus::ServerError => "server_error",
        };
        encoder.write_str(status)?;
        Ok(())
    }
}
