use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{Response, StatusCode, header},
    response::IntoResponse,
    routing::{get, put},
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::pool::{PoolError, ProcessPool, ProxyResponse, VmRuntime};
use crate::service;

pub type SharedPool = Arc<Mutex<ProcessPool>>;

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

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    field: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BootConfig {
    pub(crate) name: String,
    pub(crate) cpus: u16,
    pub(crate) memory_mb: u64,
    pub(crate) boot_mode: String,
    pub(crate) disks: Vec<PathBuf>,
    pub(crate) kernel_path: Option<PathBuf>,
    pub(crate) initrd_path: Option<PathBuf>,
    pub(crate) cmdline: Option<String>,
    pub(crate) firmware_path: Option<PathBuf>,
    pub(crate) snapshot_path: Option<PathBuf>,
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

#[derive(Debug)]
pub(crate) enum ApiError {
    Validation {
        field: Option<String>,
        error: String,
    },
    NotFound(String),
    Conflict(String),
    PoolExhausted(String),
    Backend(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, field, error) = match self {
            Self::Validation { field, error } => (StatusCode::BAD_REQUEST, field, error),
            Self::NotFound(error) => (StatusCode::NOT_FOUND, None, error),
            Self::Conflict(error) => (StatusCode::CONFLICT, None, error),
            Self::PoolExhausted(error) => (StatusCode::SERVICE_UNAVAILABLE, None, error),
            Self::Backend(error) => (StatusCode::INTERNAL_SERVER_ERROR, None, error),
        };

        (status, Json(ErrorResponse { error, field })).into_response()
    }
}

pub fn router(pool: SharedPool) -> Router {
    let state = AppState { pool };

    Router::new()
        .route("/health", get(health))
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
    let pool = state.pool.lock().await;

    Ok(Json(HealthResponse {
        service: "croutond",
        version: env!("CARGO_PKG_VERSION"),
        pool_size: pool.size(),
        pool_in_use: pool.pool_in_use().await.map_err(map_pool_error)?,
        pool_idle: pool.pool_idle().await.map_err(map_pool_error)?,
    }))
}

async fn list_vms(State(state): State<AppState>) -> ApiResult<ListResponse> {
    let pool = state.pool.lock().await;
    let vms = pool.list_running_vms().await.map_err(map_pool_error)?;
    Ok(Json(ListResponse { vms }))
}

async fn get_vm(Path(name): Path<String>, State(state): State<AppState>) -> ApiResult<VmRuntime> {
    let pool = state.pool.lock().await;
    if let Some(runtime) = pool
        .find_vm_runtime_by_name(&name)
        .await
        .map_err(map_pool_error)?
    {
        return Ok(Json(runtime));
    }

    if pool
        .find_vm_slot_status_by_name(&name)
        .await
        .map_err(map_pool_error)?
        .is_some()
    {
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

pub(crate) fn map_pool_error(error: PoolError) -> ApiError {
    match error {
        PoolError::SlotNotFound(slot) => ApiError::NotFound(format!("slot {slot} does not exist")),
        PoolError::VmNotFound(name) => ApiError::NotFound(format!("VM '{name}' not found")),
        PoolError::VmAlreadyRunning(name) => {
            ApiError::Conflict(format!("VM '{name}' is already running"))
        }
        PoolError::NoAvailableSlot => ApiError::PoolExhausted("pool exhausted".to_string()),
        PoolError::InvalidTransition { slot, from, action } => ApiError::Conflict(format!(
            "slot {slot} cannot perform {action} from state {from:?}"
        )),
        PoolError::ChannelClosed => ApiError::Backend("slot worker channel is closed".to_string()),
        PoolError::Backend(error) => ApiError::Backend(error),
    }
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
