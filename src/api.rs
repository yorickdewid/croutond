use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{Response, header},
    routing::{get, put},
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::error::ApiError;
use crate::host_metrics::{HostMetrics, collect_host_metrics};
use crate::pool::{ProxyResponse, VmRuntime};
use crate::pool_facade::PoolFacade;
use crate::service::{self, BootConfig, SharedPool};

#[derive(Clone)]
struct AppState {
    pool: SharedPool,
}

#[derive(Serialize)]
struct HealthResponse {
    service: &'static str,
    version: &'static str,
    #[serde(rename = "poolSize")]
    pool_size: usize,
    #[serde(rename = "poolInUse")]
    pool_in_use: usize,
    #[serde(rename = "poolIdle")]
    pool_idle: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotRequest {
    dest_path: PathBuf,
}

#[derive(Serialize)]
struct OkResponse {
    ok: bool,
}

#[derive(Serialize)]
struct ListResponse {
    vms: Vec<VmRuntime>,
}

type ApiResult<T> = Result<Json<T>, ApiError>;

pub fn router(pool: SharedPool) -> Router {
    let state = AppState { pool };

    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(host_metrics))
        .route("/vms", get(list_vms).post(create_vm))
        .route("/vms/{name}", get(get_vm).delete(delete_vm))
        .route("/vms/{name}/reboot", put(reboot_vm))
        .route("/vms/{name}/pause", put(pause_vm))
        .route("/vms/{name}/resume", put(resume_vm))
        .route("/vms/{name}/snapshot", put(snapshot_vm))
        .route("/vms/{name}/info", get(proxy_info))
        .route("/vms/{name}/counters", get(proxy_counters))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> ApiResult<HealthResponse> {
    let pool = state.pool.read().await;
    let (pool_in_use, pool_idle) = pool.pool_usage_counts().await?;

    Ok(Json(HealthResponse {
        service: "croutond",
        version: env!("CARGO_PKG_VERSION"),
        pool_size: pool.pool_size(),
        pool_in_use,
        pool_idle,
    }))
}

async fn host_metrics() -> ApiResult<HostMetrics> {
    let metrics = collect_host_metrics().map_err(|error| ApiError::Backend(error.to_string()))?;
    Ok(Json(metrics))
}

async fn list_vms(State(state): State<AppState>) -> ApiResult<ListResponse> {
    let pool = state.pool.read().await;
    let vms = pool.list_runtimes().await?;
    Ok(Json(ListResponse { vms }))
}

async fn get_vm(Path(name): Path<String>, State(state): State<AppState>) -> ApiResult<VmRuntime> {
    let pool = state.pool.read().await;
    if let Some(runtime) = pool.find_runtime_by_name(&name).await? {
        return Ok(Json(runtime));
    }

    if pool.find_slot_by_name(&name).await?.is_some() {
        return Err(ApiError::Conflict(format!("VM '{name}' is not running")));
    }

    Err(ApiError::NotFound(format!("VM '{name}' not found")))
}

async fn create_vm(
    State(state): State<AppState>,
    Json(payload): Json<BootConfig>,
) -> ApiResult<VmRuntime> {
    Ok(Json(service::create_vm(&state.pool, payload).await?))
}

async fn delete_vm(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<OkResponse>, ApiError> {
    service::delete_vm(&state.pool, &name).await?;
    Ok(Json(OkResponse { ok: true }))
}

async fn reboot_vm(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<OkResponse>, ApiError> {
    service::proxy_action_by_name(&state.pool, &name, "/api/v1/vm.reboot").await?;
    Ok(Json(OkResponse { ok: true }))
}

async fn pause_vm(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<OkResponse>, ApiError> {
    service::proxy_action_by_name(&state.pool, &name, "/api/v1/vm.pause").await?;
    Ok(Json(OkResponse { ok: true }))
}

async fn resume_vm(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<OkResponse>, ApiError> {
    service::proxy_action_by_name(&state.pool, &name, "/api/v1/vm.resume").await?;
    Ok(Json(OkResponse { ok: true }))
}

async fn snapshot_vm(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SnapshotRequest>,
) -> Result<Json<OkResponse>, ApiError> {
    if !payload.dest_path.is_absolute() {
        return Err(ApiError::Validation {
            field: Some("destPath".to_string()),
            error: "destPath must be absolute".to_string(),
        });
    }

    let body = serde_json::json!({"destPath": payload.dest_path});
    service::proxy_action_by_name_with_body(&state.pool, &name, "/api/v1/vm.snapshot", body)
        .await?;
    Ok(Json(OkResponse { ok: true }))
}

async fn proxy_info(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Response<Body>, ApiError> {
    let response =
        service::proxy_passthrough_by_name(&state.pool, &name, "/api/v1/vm.info").await?;
    build_proxy_response(response)
}

async fn proxy_counters(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Response<Body>, ApiError> {
    let response =
        service::proxy_passthrough_by_name(&state.pool, &name, "/api/v1/vm.counters").await?;
    build_proxy_response(response)
}

fn build_proxy_response(response: ProxyResponse) -> Result<Response<Body>, ApiError> {
    let mut builder = Response::builder().status(response.status);
    if let Some(content_type) = response.content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }

    builder
        .body(Body::from(response.body))
        .map_err(|error| ApiError::Backend(error.to_string()))
}
