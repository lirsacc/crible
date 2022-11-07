use axum::extract::State as ExtractState;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;

use super::errors::APIError;
use super::State;
use crate::operations::{self, Operation};

pub async fn handler_home() -> impl IntoResponse {
    format!("Crible Server {}", env!("CARGO_PKG_VERSION"))
}

pub async fn handler_not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "Not found.")
}

pub type APIResult<T> = Result<(StatusCode, T), APIError>;
pub type JSONAPIResult<T> = Result<(StatusCode, Json<T>), APIError>;
pub type StaticAPIResult = APIResult<&'static str>;

pub async fn handler_query(
    ExtractState(state): ExtractState<State>,
    Json(payload): Json<operations::Query>,
) -> JSONAPIResult<operations::QueryResult> {
    Ok((
        StatusCode::OK,
        Json(state.0.spawn(move |index| payload.run(index.as_ref())).await??),
    ))
}

/// Count elements matching a query.
pub async fn handler_count(
    ExtractState(state): ExtractState<State>,
    Json(payload): Json<operations::Count>,
) -> JSONAPIResult<u64> {
    Ok((
        StatusCode::OK,
        Json(state.0.spawn(move |index| payload.run(index.as_ref())).await??),
    ))
}

pub async fn handler_stats(
    ExtractState(state): ExtractState<State>,
) -> JSONAPIResult<operations::StatsResult> {
    Ok((
        StatusCode::OK,
        Json(
            state
                .0
                .spawn(move |index| (operations::Stats {}).run(index.as_ref()))
                .await?,
        ),
    ))
}

pub async fn handler_set(
    ExtractState(state): ExtractState<State>,
    Json(payload): Json<operations::Set>,
) -> StaticAPIResult {
    if state.0.read_only {
        return Err(operations::OperationError::ReadOnly.into());
    }

    if state.0.spawn(move |index| payload.run(index.as_ref())).await? {
        state.0.flush().await?;
        Ok((StatusCode::OK, ""))
    } else {
        Ok((StatusCode::NO_CONTENT, ""))
    }
}

pub async fn handler_set_many(
    ExtractState(state): ExtractState<State>,
    Json(payload): Json<operations::SetMany>,
) -> StaticAPIResult {
    if state.0.read_only {
        return Err(operations::OperationError::ReadOnly.into());
    }

    state.0.spawn(move |index| payload.run(index.as_ref())).await?;
    state.0.flush().await?;
    Ok((StatusCode::OK, ""))
}

pub async fn handler_unset(
    ExtractState(state): ExtractState<State>,
    Json(payload): Json<operations::Unset>,
) -> StaticAPIResult {
    if state.0.read_only {
        return Err(operations::OperationError::ReadOnly.into());
    }

    if state.0.spawn(move |index| payload.run(index.as_ref())).await? {
        state.0.flush().await?;
        Ok((StatusCode::OK, ""))
    } else {
        Ok((StatusCode::NO_CONTENT, ""))
    }
}

pub async fn handler_unset_many(
    ExtractState(state): ExtractState<State>,
    Json(payload): Json<operations::UnsetMany>,
) -> StaticAPIResult {
    if state.0.read_only {
        return Err(operations::OperationError::ReadOnly.into());
    }

    state.0.spawn(move |index| payload.run(index.as_ref())).await?;
    state.0.flush().await?;
    Ok((StatusCode::OK, ""))
}

pub async fn handler_get_bit(
    ExtractState(state): ExtractState<State>,
    Json(payload): Json<operations::GetBit>,
) -> JSONAPIResult<Vec<String>> {
    Ok((
        StatusCode::OK,
        Json(state.0.spawn(move |index| payload.run(index.as_ref())).await?),
    ))
}

pub async fn handler_set_bit(
    ExtractState(state): ExtractState<State>,
    Json(payload): Json<operations::SetBit>,
) -> StaticAPIResult {
    if state.0.spawn(move |index| payload.run(index.as_ref())).await? {
        state.0.flush().await?;
        Ok((StatusCode::OK, ""))
    } else {
        Ok((StatusCode::NO_CONTENT, ""))
    }
}

pub async fn handler_delete_bits(
    ExtractState(state): ExtractState<State>,
    Json(payload): Json<operations::DeleteBits>,
) -> StaticAPIResult {
    if state.0.read_only {
        return Err(operations::OperationError::ReadOnly.into());
    }

    state.0.spawn(move |index| payload.run(index.as_ref())).await?;
    state.0.flush().await?;
    Ok((StatusCode::OK, ""))
}
