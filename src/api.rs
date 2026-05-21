use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::pool::{ProcessPool, SlotError, SlotStatus};

pub type SharedPool = Arc<Mutex<ProcessPool>>;

#[derive(Clone)]
struct AppState {
    pool: SharedPool,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Deserialize)]
struct ReserveRequest {
    owner: String,
}

#[derive(Deserialize)]
struct FailRequest {
    reason: String,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ErrorResponse>)>;

pub fn router(pool: SharedPool) -> Router {
    let state = AppState { pool };

    Router::new()
        .route("/health", get(health))
        .route("/slots", get(list_slots))
        .route("/slots/{slot}", get(get_slot))
        .route("/slots/{slot}/reserve", post(reserve_slot))
        .route("/slots/{slot}/booted", post(mark_booted))
        .route("/slots/{slot}/release", post(release_slot))
        .route("/slots/{slot}/failed", post(mark_failed))
        .route("/slots/{slot}/reset", post(reset_slot))
        .with_state(state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn list_slots(State(state): State<AppState>) -> ApiResult<Vec<SlotStatus>> {
    let pool = state.pool.lock().await;
    let slots = pool.list_vm_slots().await.map_err(slot_error_response)?;
    Ok(Json(slots))
}

async fn get_slot(Path(slot): Path<usize>, State(state): State<AppState>) -> ApiResult<SlotStatus> {
    let pool = state.pool.lock().await;
    let status = pool
        .get_vm_slot_status(slot)
        .await
        .map_err(slot_error_response)?;
    Ok(Json(status))
}

async fn reserve_slot(
    Path(slot): Path<usize>,
    State(state): State<AppState>,
    Json(payload): Json<ReserveRequest>,
) -> ApiResult<SlotStatus> {
    let pool = state.pool.lock().await;
    let status = pool
        .reserve_vm_slot(slot, payload.owner)
        .await
        .map_err(slot_error_response)?;
    Ok(Json(status))
}

async fn mark_booted(
    Path(slot): Path<usize>,
    State(state): State<AppState>,
) -> ApiResult<SlotStatus> {
    let pool = state.pool.lock().await;
    let status = pool
        .mark_vm_slot_booted(slot)
        .await
        .map_err(slot_error_response)?;
    Ok(Json(status))
}

async fn release_slot(
    Path(slot): Path<usize>,
    State(state): State<AppState>,
) -> ApiResult<SlotStatus> {
    let pool = state.pool.lock().await;
    let status = pool
        .release_vm_slot(slot)
        .await
        .map_err(slot_error_response)?;
    Ok(Json(status))
}

async fn mark_failed(
    Path(slot): Path<usize>,
    State(state): State<AppState>,
    Json(payload): Json<FailRequest>,
) -> ApiResult<SlotStatus> {
    let pool = state.pool.lock().await;
    let status = pool
        .mark_vm_slot_failed(slot, payload.reason)
        .await
        .map_err(slot_error_response)?;
    Ok(Json(status))
}

async fn reset_slot(
    Path(slot): Path<usize>,
    State(state): State<AppState>,
) -> ApiResult<SlotStatus> {
    let pool = state.pool.lock().await;
    let status = pool
        .reset_vm_slot(slot)
        .await
        .map_err(slot_error_response)?;
    Ok(Json(status))
}

fn slot_error_response(error: SlotError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match error {
        SlotError::SlotNotFound(_) => StatusCode::NOT_FOUND,
        SlotError::InvalidTransition { .. } => StatusCode::CONFLICT,
        SlotError::ChannelClosed => StatusCode::SERVICE_UNAVAILABLE,
    };

    (
        status,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
}
