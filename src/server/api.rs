use axum::{
    extract::{Extension, Path},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_derive::{Deserialize, Serialize};

use std::collections::HashMap;

use crible_lib::{expression::Expression, index::Stats};

use super::readwrite::handle_write;
use super::{errors::APIError, State};

pub async fn handler_home() -> impl IntoResponse {
    format!("Crible Server {}", env!("CARGO_PKG_VERSION"))
}

pub async fn handler_not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "Not found.")
}

pub type APIResult<T> = Result<(StatusCode, T), APIError>;
pub type JSONAPIResult<T> = Result<(StatusCode, Json<T>), APIError>;
pub type StaticAPIResult = APIResult<&'static str>;

#[derive(Deserialize)]
pub struct QueryPayload {
    query: String,
    include_cardinalities: Option<bool>,
}

#[derive(Serialize)]
pub struct QueryResponse {
    values: Vec<u32>,
    cardinalities: Option<HashMap<String, u64>>,
}

/// Run a query against the index. The result will include all unique elements
/// matching the query and optionally (if `include_cardinalities` is provided
/// and true) a map containing the cardinality of the intersection of the query
/// and every property included in the index.
pub async fn handler_query(
    Json(payload): Json<QueryPayload>,
    Extension(state): Extension<State>,
) -> JSONAPIResult<QueryResponse> {
    let expr = Expression::parse(&*payload.query)?;
    let idx = state.index.as_ref().read().unwrap();
    let bm = idx.execute(&expr)?;
    Ok((
        StatusCode::OK,
        Json(QueryResponse {
            values: bm.to_vec(),
            cardinalities: match payload.include_cardinalities {
                Some(true) => Some(idx.cardinalities(&bm, None)),
                _ => None,
            },
        }),
    ))
}

/// Count elements matching a query.
pub async fn handler_count(
    Json(payload): Json<QueryPayload>,
    Extension(state): Extension<State>,
) -> JSONAPIResult<u64> {
    let expr = Expression::parse(&*payload.query)?;
    let idx = state.index.as_ref().read().unwrap();
    let bm = idx.execute(&expr)?;
    Ok((StatusCode::OK, Json(bm.cardinality())))
}

#[derive(Serialize)]
pub struct StatsResponse {
    root: Stats,
    properties: HashMap<String, Stats>,
}

pub async fn handler_stats(
    Extension(state): Extension<State>,
) -> JSONAPIResult<StatsResponse> {
    let idx = state.index.as_ref().read().unwrap();
    Ok((
        StatusCode::OK,
        Json(StatsResponse {
            root: (&*idx).into(),
            properties: idx
                .into_iter()
                .map(|(k, v)| (k.clone(), v.into()))
                .collect(),
        }),
    ))
}

#[derive(Deserialize)]
pub struct SetPayload {
    property: String,
    bit: u32,
}

pub async fn handler_set(
    Json(payload): Json<SetPayload>,
    Extension(state): Extension<State>,
) -> StaticAPIResult {
    if state.read_only {
        return Err(APIError::ReadOnly);
    }

    let added = state
        .index
        .as_ref()
        .write()
        .unwrap()
        .set(&payload.property, payload.bit);
    let status_code =
        if added { StatusCode::OK } else { StatusCode::NO_CONTENT };
    handle_write(&state).await?;
    Ok((status_code, ""))
}

pub async fn handler_set_many(
    Json(payload): Json<Vec<(String, Vec<u32>)>>,
    Extension(state): Extension<State>,
) -> StaticAPIResult {
    if state.read_only {
        return Err(APIError::ReadOnly);
    }

    {
        let mut idx = state.index.as_ref().write().unwrap();
        for (property, bits) in &payload {
            idx.set_many(property, bits);
        }
    }
    handle_write(&state).await?;
    Ok((StatusCode::OK, ""))
}

pub async fn handler_unset(
    Json(payload): Json<SetPayload>,
    Extension(state): Extension<State>,
) -> StaticAPIResult {
    if state.read_only {
        return Err(APIError::ReadOnly);
    }

    let deleted = state
        .index
        .as_ref()
        .write()
        .unwrap()
        .unset(&payload.property, payload.bit);
    let status_code =
        if deleted { StatusCode::OK } else { StatusCode::NO_CONTENT };
    handle_write(&state).await?;
    Ok((status_code, ""))
}

pub async fn handler_unset_many(
    Json(payload): Json<Vec<(String, Vec<u32>)>>,
    Extension(state): Extension<State>,
) -> StaticAPIResult {
    if state.read_only {
        return Err(APIError::ReadOnly);
    }

    {
        let mut idx = state.index.as_ref().write().unwrap();
        for (property, bits) in &payload {
            idx.unset_many(property, bits);
        }
    }
    handle_write(&state).await?;
    Ok((StatusCode::OK, ""))
}

pub async fn handler_get_bit(
    Path(bit): Path<u32>,
    Extension(state): Extension<State>,
) -> JSONAPIResult<Vec<String>> {
    let properties =
        state.index.as_ref().read().unwrap().get_properties_with_bit(bit);
    Ok((StatusCode::OK, Json(properties)))
}

pub async fn handler_set_bit(
    Path(bit): Path<u32>,
    Json(properties): Json<Vec<String>>,
    Extension(state): Extension<State>,
) -> StaticAPIResult {
    let changed = state
        .index
        .as_ref()
        .write()
        .unwrap()
        .set_properties_with_bit(bit, &properties);
    let status_code =
        if changed { StatusCode::OK } else { StatusCode::NO_CONTENT };
    Ok((status_code, ""))
}

pub async fn handler_delete_bit(
    Path(bit): Path<u32>,
    Extension(state): Extension<State>,
) -> StaticAPIResult {
    if state.read_only {
        return Err(APIError::ReadOnly);
    }

    state.index.as_ref().write().unwrap().unset_all(&[bit]);
    handle_write(&state).await?;
    Ok((StatusCode::OK, ""))
}

pub async fn handler_delete_bits(
    Json(bits): Json<Vec<u32>>,
    Extension(state): Extension<State>,
) -> StaticAPIResult {
    if state.read_only {
        return Err(APIError::ReadOnly);
    }

    state.index.as_ref().write().unwrap().unset_all(&bits);
    handle_write(&state).await?;
    Ok((StatusCode::OK, ""))
}
