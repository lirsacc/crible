use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use std::convert::From;

#[derive(Debug)]
pub enum APIError {
    ReadOnly,
    Expression(crible_lib::expression::Error),
    Index(crible_lib::index::Error),
    Eyre(eyre::Report),
}

impl IntoResponse for APIError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            APIError::ReadOnly => (
                StatusCode::FORBIDDEN,
                "Server is in read-only mode".to_owned(),
            ),
            APIError::Expression(e) => match e {
                crible_lib::expression::Error::Invalid(_)
                | crible_lib::expression::Error::InvalidEndOfInput(_)
                | crible_lib::expression::Error::InputStringToolLong => {
                    (StatusCode::BAD_REQUEST, "Invalid query".to_owned())
                }
            },
            APIError::Index(e) => match e {
                crible_lib::index::Error::PropertyDoesNotExist(p) => (
                    StatusCode::BAD_REQUEST,
                    format!("Property {} does not exist", p),
                ),
            },
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

impl From<crible_lib::expression::Error> for APIError {
    fn from(e: crible_lib::expression::Error) -> Self {
        APIError::Expression(e)
    }
}

impl From<crible_lib::index::Error> for APIError {
    fn from(e: crible_lib::index::Error) -> Self {
        APIError::Index(e)
    }
}

impl From<eyre::Report> for APIError {
    fn from(e: eyre::Report) -> Self {
        APIError::Eyre(e)
    }
}
