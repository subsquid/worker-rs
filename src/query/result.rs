use std::time::Duration;

use axum::{http::StatusCode, response::IntoResponse};

use crate::util::hash::sha3_256;

pub type QueryResult = std::result::Result<QueryOk, QueryError>;

#[derive(Debug, Clone)]
pub struct QueryOk {
    pub data: Vec<u8>,
    pub num_read_chunks: usize,
    pub exec_time: Duration,
}

impl QueryOk {
    pub fn new(data: Vec<u8>, num_read_chunks: usize, exec_time: Duration) -> Self {
        Self {
            data,
            num_read_chunks,
            exec_time,
        }
    }

    pub fn compressed_data(&self) -> Vec<u8> {
        use flate2::write::GzEncoder;
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&self.data).expect("Couldn't gzip data");
        encoder.finish().expect("Couldn't finish gzipping data")
    }

    pub fn sha3_256(&self) -> Vec<u8> {
        sha3_256(&self.data)
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
