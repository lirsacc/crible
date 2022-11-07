use std::convert::From;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::operations::OperationError;

#[derive(Debug)]
pub enum APIError {
    Operation(OperationError),
    TooManyRequests,
    Eyre(eyre::Report),
}

impl IntoResponse for APIError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            APIError::Operation(e) => match e {
                OperationError::ReadOnly => (
                    StatusCode::FORBIDDEN,
                    "Server is in read-only mode".to_owned(),
                ),
                OperationError::Expression(e) => match e {
                    crible_lib::expression::Error::Invalid(_)
                    | crible_lib::expression::Error::InvalidEndOfInput(_)
                    | crible_lib::expression::Error::InputStringToolLong => {
                        (StatusCode::BAD_REQUEST, "Invalid query".to_owned())
                    }
                },
                OperationError::Index(e) => match e {
                    crible_lib::index::Error::PropertyDoesNotExist(p) => (
                        StatusCode::BAD_REQUEST,
                        format!("Property {} does not exist", p),
                    ),
                },
            },
            APIError::TooManyRequests => {
                (StatusCode::TOO_MANY_REQUESTS, "".to_owned())
            }
            _ => {
                tracing::error!("Unhandled error: {0:?}", self);
                (StatusCode::INTERNAL_SERVER_ERROR, "".to_owned())
            }
        };

        let body = Json(json!({
            "error": error_message,
        }));

        (status, body).into_response()
    }
}

impl From<OperationError> for APIError {
    fn from(e: OperationError) -> Self {
        APIError::Operation(e)
    }
}

impl From<eyre::Report> for APIError {
    fn from(e: eyre::Report) -> Self {
        APIError::Eyre(e)
    }
}

impl From<crate::executor::Error> for APIError {
    fn from(e: crate::executor::Error) -> Self {
        match e {
            crate::executor::Error::TooManyRequests => {
                APIError::TooManyRequests
            }
            crate::executor::Error::Unknown(e) => APIError::Eyre(e),
        }
    }
}
