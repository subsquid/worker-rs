use std::time::Duration;

use axum::{http::StatusCode, response::IntoResponse};
use tracing::instrument;

use crate::util::hash::sha3_256;

pub type QueryResult = std::result::Result<QueryOk, QueryError>;

#[derive(Debug, Clone, Default)]
pub struct WorkerTimeReport {
    pub parsing_time: Duration,
    pub execution_time: Duration,
    pub serialization_time: Duration,
    pub compression_time: Duration,
    pub signing_time: Duration,
}

#[derive(Debug, Clone)]
pub struct QueryOk {
    pub data: Vec<u8>,
    pub num_read_chunks: usize,
    pub last_block: u64,
    pub time_report: WorkerTimeReport,
}

impl QueryOk {
    pub fn new(
        data: Vec<u8>,
        num_read_chunks: usize,
        last_block: u64,
        parsing_time: Duration,
        execution_time: Duration,
        serialization_time: Duration,
    ) -> Self {
        Self {
            data,
            num_read_chunks,
            time_report: WorkerTimeReport {
                parsing_time,
                execution_time,
                serialization_time,
                compression_time: Duration::from_secs(0),
                signing_time: Duration::from_secs(0),
            },
            last_block,
        }
    }

    #[instrument(skip_all)]
    pub async fn data_gzip(&self) -> Vec<u8> {
        use flate2::write::GzEncoder;
        use std::io::Write;
        tokio::task::block_in_place(|| {
            let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(&self.data).expect("Couldn't gzip data");
            encoder.finish().expect("Couldn't finish gzipping data")
        })
    }

    #[instrument(skip_all)]
    pub async fn data_zstd(&self) -> Vec<u8> {
        use std::io::Write;
        tokio::task::block_in_place(|| {
            let mut encoder = zstd::Encoder::new(Vec::new(), zstd::DEFAULT_COMPRESSION_LEVEL)
                .expect("Couldn't create zstd encoder");
            encoder
                .write_all(&self.data)
                .expect("Couldn't zstd compress data");
            encoder.finish().expect("Couldn't finish zstd compression")
        })
    }

    pub async fn sha3_256(&self) -> Vec<u8> {
        tokio::task::block_in_place(|| sha3_256(&self.data))
    }
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum QueryError {
    #[error("This worker doesn't have any chunks in requested range")]
    NotFound,
    #[error("This worker doesn't have enough CU allocated")]
    NoAllocation,
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Service overloaded")]
    ServiceOverloaded,
    #[error("Internal error")]
    Other(String),
}

impl From<std::io::Error> for QueryError {
    fn from(value: std::io::Error) -> Self {
        Self::Other(value.to_string())
    }
}

impl From<anyhow::Error> for QueryError {
    fn from(value: anyhow::Error) -> Self {
        Self::Other(value.to_string())
    }
}

impl IntoResponse for QueryError {
    fn into_response(self) -> axum::response::Response {
        lazy_static::lazy_static! {
            static ref SERVICE_OVERLOADED: StatusCode = StatusCode::from_u16(529).unwrap();
        }
        match self {
            s @ Self::NotFound => (StatusCode::NOT_FOUND, s.to_string()).into_response(),
            s @ Self::NoAllocation => {
                (StatusCode::TOO_MANY_REQUESTS, s.to_string()).into_response()
            }
            s @ Self::BadRequest(_) => (StatusCode::BAD_REQUEST, s.to_string()).into_response(),
            s @ Self::ServiceOverloaded => (*SERVICE_OVERLOADED, s.to_string()).into_response(),
            Self::Other(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Couldn't execute query: {:?}", err),
            )
                .into_response(),
        }
    }
}

impl From<&QueryError> for sqd_messages::query_error::Err {
    fn from(e: &QueryError) -> Self {
        use sqd_messages::query_error::Err;
        match e {
            QueryError::NotFound => Err::NotFound(e.to_string()),
            QueryError::BadRequest(e) => Err::BadRequest(e.clone()),
            QueryError::ServiceOverloaded => Err::ServerOverloaded(()),
            QueryError::NoAllocation => Err::TooManyRequests(()),
            QueryError::Other(e) => Err::ServerError(e.to_string()),
        }
    }
}

impl From<QueryError> for sqd_messages::query_error::Err {
    fn from(e: QueryError) -> Self {
        use sqd_messages::query_error::Err;
        match e {
            QueryError::NotFound => Err::NotFound(e.to_string()),
            QueryError::BadRequest(e) => Err::BadRequest(e),
            QueryError::ServiceOverloaded => Err::ServerOverloaded(()),
            QueryError::NoAllocation => Err::TooManyRequests(()),
            QueryError::Other(e) => Err::ServerError(e.to_string()),
        }
    }
}
