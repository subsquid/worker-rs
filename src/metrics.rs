use std::fmt::Write;

use prometheus_client::encoding::{EncodeLabelSet, LabelValueEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::{family::Family, gauge::Gauge, histogram::Histogram};
use prometheus_client::registry::{Registry, Unit};

use crate::query::result::{QueryError, QueryResult};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum WorkerStatus {
    Starting,
    NotRegistered,
    DeprecatedVersion,
    UnsupportedVersion,
    Unreliable,
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
pub struct WorkerInfoLabels {
    pub version: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StatusLabels {
    worker_status: WorkerStatus,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct QueryExecutedLabels {
    status: QueryStatus,
}

/// The blob a network state resolved to nothing for, or `assignment_type` when the picker
/// itself would not read. A fixed set, so the label space stays bounded (OB-14).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct UnresolvedLabels {
    reason: &'static str,
}

lazy_static::lazy_static! {
    // Worker info metric (kept as worker_info_info for backward compatibility)
    pub static ref WORKER_INFO: Family<WorkerInfoLabels, Gauge> = Default::default();

    static ref STATUS: Family<StatusLabels, Gauge> = Default::default();
    pub static ref CHUNKS_AVAILABLE: Gauge = Default::default();
    pub static ref CHUNKS_DOWNLOADING: Gauge = Default::default();
    pub static ref CHUNKS_PENDING: Gauge = Default::default();
    pub static ref CHUNKS_GIVEN_UP: Gauge = Default::default();
    pub static ref CHUNKS_DRAINING: Gauge = Default::default();
    pub static ref CHUNKS_DOWNLOADED: Counter = Default::default();
    pub static ref CHUNKS_FAILED_DOWNLOAD: Counter = Default::default();
    pub static ref CHUNKS_UNADDRESSABLE: Counter = Default::default();
    pub static ref CHUNKS_REMOVED: Counter = Default::default();
    pub static ref STORED_BYTES: Gauge = Default::default();

    pub static ref ASSIGNMENTS_REFUSED: Counter = Default::default();
    static ref NETWORK_STATE_UNRESOLVED: Family<UnresolvedLabels, Counter> = Default::default();

    pub static ref SCHEMA_BUNDLE_LOADED: Gauge = Default::default();
    pub static ref SCHEMA_BUNDLE_FAILURES: Counter = Default::default();
    pub static ref SCHEMA_BUNDLE_MISMATCHES: Counter = Default::default();

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

/// Counts one poll whose network state named no assignment this worker could read (OB-19).
/// Counted per poll, so what a stalled fleet shows is persistence rather than a single edge.
pub fn network_state_unresolved(reason: &'static str) {
    NETWORK_STATE_UNRESOLVED
        .get_or_create(&UnresolvedLabels { reason })
        .inc();
}

#[cfg(test)]
pub fn unresolved_count(reason: &'static str) -> u64 {
    NETWORK_STATE_UNRESOLVED
        .get_or_create(&UnresolvedLabels { reason })
        .get()
}

pub fn query_executed(result: &QueryResult) {
    let (status, result) = match result {
        Ok(result) => (QueryStatus::Ok, Some(result)),
        Err(QueryError::NoAllocation) => (QueryStatus::NoAllocation, None),
        Err(QueryError::NotFound | QueryError::BadRequest(_)) => (QueryStatus::BadRequest, None),
        Err(QueryError::Other(_) | QueryError::ServiceOverloaded) => {
            (QueryStatus::ServerError, None)
        }
    };
    QUERY_EXECUTED
        .get_or_create(&QueryExecutedLabels { status })
        .inc();
    if let Some(result) = result {
        QUERY_RESULT_SIZE.observe(result.data.len() as f64);
        READ_CHUNKS.observe(result.num_read_chunks as f64);
    }
}

pub fn register_metrics(registry: &mut Registry, version: String) {
    WORKER_INFO
        .get_or_create(&WorkerInfoLabels { version })
        .set(1);
    registry.register(
        "worker_info_info", // Keep the _info suffix for backward compatibility
        "Worker information with version",
        WORKER_INFO.clone(),
    );
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
        "chunks_given_up",
        "Number of assigned chunks that exhausted their download attempts",
        CHUNKS_GIVEN_UP.clone(),
    );
    registry.register(
        "chunks_draining",
        "Number of unassigned chunks kept on disk until running queries release them",
        CHUNKS_DRAINING.clone(),
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
        "chunks_unaddressable",
        "Number of chunks the applied assignment carries no usable address for",
        CHUNKS_UNADDRESSABLE.clone(),
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
        "assignments_refused",
        "Number of announced assignments refused as unusable",
        ASSIGNMENTS_REFUSED.clone(),
    );
    registry.register(
        "schema_bundle_loaded",
        "Whether a schema bundle is currently installed",
        SCHEMA_BUNDLE_LOADED.clone(),
    );
    registry.register(
        "schema_bundle_failures",
        "Number of times a schema bundle failed to install",
        SCHEMA_BUNDLE_FAILURES.clone(),
    );
    registry.register(
        "schema_bundle_mismatches",
        "Number of pairs the scheduler published that do not hold together",
        SCHEMA_BUNDLE_MISMATCHES.clone(),
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
    registry.register(
        "network_state_unresolved",
        "Polls whose network state named no assignment this worker could read, by reason",
        NETWORK_STATE_UNRESOLVED.clone(),
    );
    registry.register("worker_status", "Status of the worker", STATUS.clone());
    set_status(WorkerStatus::Starting);
}

impl prometheus_client::encoding::EncodeLabelValue for WorkerStatus {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        let status = match self {
            WorkerStatus::Starting => "starting",
            WorkerStatus::NotRegistered => "not_registered",
            WorkerStatus::DeprecatedVersion => "deprecated_version",
            WorkerStatus::UnsupportedVersion => "unsupported_version",
            WorkerStatus::Unreliable => "unreliable",
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
