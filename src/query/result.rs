use anyhow::Result;
use axum::{http::StatusCode, response::IntoResponse};

use crate::util::hash::sha3_256;

pub type QueryResult = std::result::Result<QueryOk, QueryError>;

#[derive(Debug, Clone)]
pub struct QueryOk {
    pub raw_data: Vec<u8>,
    pub compressed_data: Vec<u8>,
    pub data_size: usize,
    pub compressed_size: usize,
    pub data_sha3_256: Vec<u8>,
    pub num_read_chunks: usize,
}

impl QueryOk {
    pub fn new(data: Vec<u8>, num_read_chunks: usize) -> Result<Self> {
        use flate2::write::GzEncoder;
        use std::io::Write;

        let data_size = data.len();

        let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&data)?;
        let compressed_data = encoder.finish()?;
        let compressed_size = compressed_data.len();

        let hash = sha3_256(&data);

        Ok(Self {
            raw_data: data,
            compressed_data,
            data_size,
            compressed_size,
            data_sha3_256: hash,
            num_read_chunks,
        })
    }
}

#[derive(thiserror::Error, Debug)]
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
    Other(#[from] anyhow::Error),
}

impl From<std::io::Error> for QueryError {
    fn from(value: std::io::Error) -> Self {
        Self::Other(anyhow::Error::from(value))
    }
}

impl IntoResponse for QueryError {
    fn into_response(self) -> axum::response::Response {
        match self {
            s @ Self::NotFound => (StatusCode::NOT_FOUND, s.to_string()).into_response(),
            s @ Self::NoAllocation => {
                (StatusCode::TOO_MANY_REQUESTS, s.to_string()).into_response()
            }
            s @ Self::BadRequest(_) => (StatusCode::BAD_REQUEST, s.to_string()).into_response(),
            s @ Self::ServiceOverloaded => {
                (StatusCode::SERVICE_UNAVAILABLE, s.to_string()).into_response()
            }
            Self::Other(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Couldn't execute query: {:?}", err),
            )
                .into_response(),
        }
    }
}
