use axum::{
    extract::{Extension, Path},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_derive::{Deserialize, Serialize};
use tokio::sync::RwLock;

use std::collections::HashMap;
use std::sync::Arc;

use crate::backends::Backend;
use crate::expression::Expression;
use crate::index::{Index, Stats};

use super::errors::APIError;

type IndexShared = Arc<RwLock<Index>>;
type IndexExt = Extension<IndexShared>;
type BackendShared = Arc<RwLock<Box<dyn Backend>>>;
type BackendExt = Extension<BackendShared>;

async fn flush(
    backend: BackendShared,
    index: IndexShared,
) -> Result<(), eyre::Report> {
    let mut _backend = backend.as_ref().write().await;
    let _index = index.as_ref().read().await;
    _backend.dump(&_index).await
}

pub async fn handler_home() -> impl IntoResponse {
    format!("Crible Server {}", env!("CARGO_PKG_VERSION"))
}

pub async fn handler_read_only() -> impl IntoResponse {
    (StatusCode::FORBIDDEN, "Server is in read-only mode")
}

pub async fn handler_404() -> impl IntoResponse {
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

/// Run a query against the index.
/// The result will include all unique elements matching the query and
/// optionally (if include_cardinalities is provided and true) a map containing
/// the cardinality of the intersection of the query and every property included
/// in the index.
pub async fn handler_query(
    Json(payload): Json<QueryPayload>,
    Extension(index): IndexExt,
) -> JSONAPIResult<QueryResponse> {
    let expr = Expression::parse(&*payload.query)?;
    let idx = index.as_ref().read().await;
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
    Extension(index): IndexExt,
) -> JSONAPIResult<u64> {
    let expr = Expression::parse(&*payload.query)?;
    let idx = index.as_ref().read().await;
    let bm = idx.execute(&expr)?;
    Ok((StatusCode::OK, Json(bm.cardinality())))
}

/// Get the base64 encoded Bitmap for a query.
pub async fn handler_bitmap(
    Json(payload): Json<QueryPayload>,
    Extension(index): IndexExt,
) -> JSONAPIResult<String> {
    let expr = Expression::parse(&*payload.query)?;
    let idx = index.as_ref().read().await;
    let bm = idx.execute(&expr)?;
    Ok((StatusCode::OK, Json(base64::encode(bm.serialize()))))
}

#[derive(Serialize)]
pub struct StatsResponse {
    root: Stats,
    properties: HashMap<String, Stats>,
}

pub async fn handler_stats(
    Extension(index): IndexExt,
) -> JSONAPIResult<StatsResponse> {
    let idx = index.as_ref().read().await;
    Ok((
        StatusCode::OK,
        Json(StatsResponse {
            root: idx.stats(),
            properties: idx.property_stats(),
        }),
    ))
}

pub async fn handler_clear(
    Extension(index): IndexExt,
    Extension(backend): BackendExt,
) -> StaticAPIResult {
    index.as_ref().write().await.clear();
    flush(backend, index).await?;
    Ok((StatusCode::OK, ""))
}

#[derive(Deserialize)]
pub struct SetPayload {
    property: String,
    value: u32,
}

pub async fn handler_set(
    Json(payload): Json<SetPayload>,
    Extension(index): IndexExt,
    Extension(backend): BackendExt,
) -> StaticAPIResult {
    let added =
        index.as_ref().write().await.set(&payload.property, payload.value);
    let status_code =
        if added { StatusCode::OK } else { StatusCode::NO_CONTENT };
    flush(backend, index).await?;
    Ok((status_code, ""))
}

pub async fn handler_set_many(
    Json(payload): Json<Vec<(String, Vec<u32>)>>,
    Extension(index): IndexExt,
    Extension(backend): BackendExt,
) -> StaticAPIResult {
    {
        let mut idx = index.as_ref().write().await;
        for (property, values) in payload.iter() {
            idx.set_many(property, values);
        }
    }
    flush(backend, index).await?;
    Ok((StatusCode::OK, ""))
}

pub async fn handler_unset(
    Json(payload): Json<SetPayload>,
    Extension(index): IndexExt,
    Extension(backend): BackendExt,
) -> StaticAPIResult {
    let deleted =
        index.as_ref().write().await.unset(&payload.property, payload.value);
    let status_code =
        if deleted { StatusCode::OK } else { StatusCode::NO_CONTENT };
    flush(backend, index).await?;
    Ok((status_code, ""))
}

pub async fn handler_item_get(
    Path(id): Path<u32>,
    Extension(index): IndexExt,
) -> JSONAPIResult<Vec<String>> {
    let properties = index
        .as_ref()
        .read()
        .await
        .properties_matching_id(id)
        .iter()
        .map(|x| (*x).to_owned())
        .collect::<Vec<String>>();
    Ok((StatusCode::OK, Json(properties)))
}

pub async fn handler_item_delete(
    Path(id): Path<u32>,
    Extension(index): IndexExt,
    Extension(backend): BackendExt,
) -> StaticAPIResult {
    let deleted = index.as_ref().write().await.remove_id(id);
    let status_code =
        if deleted { StatusCode::OK } else { StatusCode::NO_CONTENT };
    flush(backend, index).await?;
    Ok((status_code, ""))
}
