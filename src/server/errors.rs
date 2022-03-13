use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use std::convert::From;

use crate::expression::ExpressionError;
use crate::index::IndexError;

#[derive(Debug)]
pub enum APIError {
    Expression(ExpressionError),
    Index(IndexError),
    Eyre(eyre::Report),
}

impl IntoResponse for APIError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            APIError::Expression(e) => match e {
                ExpressionError::ParserError(_)
                | ExpressionError::InvalidEndOfInput(_)
                | ExpressionError::InputStringToolLong => {
                    (StatusCode::BAD_REQUEST, "Invalid query".to_owned())
                }
            },
            APIError::Index(e) => match e {
                IndexError::PropertyDoesNotExist(p) => (
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

impl From<ExpressionError> for APIError {
    fn from(e: ExpressionError) -> Self {
        APIError::Expression(e)
    }
}

impl From<IndexError> for APIError {
    fn from(e: IndexError) -> Self {
        APIError::Index(e)
    }
}

impl From<eyre::Report> for APIError {
    fn from(e: eyre::Report) -> Self {
        APIError::Eyre(e)
    }
}
