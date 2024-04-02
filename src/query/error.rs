use axum::{http::StatusCode, response::IntoResponse};

#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    #[error("This worker doesn't have any chunks in requested range")]
    NotFound,
    #[error("This worker doesn't have enough CU allocated")]
    NoAllocation,
    #[error("Internal error")]
    Other(#[from] anyhow::Error),
}

impl IntoResponse for QueryError {
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::NotFound => (StatusCode::NOT_FOUND, self.to_string()).into_response(),
            Self::NoAllocation => (StatusCode::BAD_REQUEST, self.to_string()).into_response(),
            Self::Other(err) => (
                StatusCode::BAD_REQUEST,
                format!("Couldn't execute query: {:?}", err),
            )
                .into_response(),
        }
    }
}
