use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use std::convert::From;

#[derive(Debug)]
pub enum APIError {
    Expression(crate::expression::Error),
    Index(crate::index::Error),
    Eyre(eyre::Report),
}

impl IntoResponse for APIError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            APIError::Expression(e) => match e {
                crate::expression::Error::Invalid(_)
                | crate::expression::Error::InvalidEndOfInput(_)
                | crate::expression::Error::InputStringToolLong => {
                    (StatusCode::BAD_REQUEST, "Invalid query".to_owned())
                }
            },
            APIError::Index(e) => match e {
                crate::index::Error::PropertyDoesNotExist(p) => (
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

impl From<crate::expression::Error> for APIError {
    fn from(e: crate::expression::Error) -> Self {
        APIError::Expression(e)
    }
}

impl From<crate::index::Error> for APIError {
    fn from(e: crate::index::Error) -> Self {
        APIError::Index(e)
    }
}

impl From<eyre::Report> for APIError {
    fn from(e: eyre::Report) -> Self {
        APIError::Eyre(e)
    }
}
