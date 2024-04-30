use axum::{http::StatusCode, response::IntoResponse};

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

impl From<datafusion::error::DataFusionError> for QueryError {
    fn from(value: datafusion::error::DataFusionError) -> Self {
        Self::Other(value.context("DataFusion error").into())
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
