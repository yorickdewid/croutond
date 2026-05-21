use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{Method, Response, StatusCode, header},
    response::IntoResponse,
    routing::{get, put},
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::pool::{PoolError, ProcessPool, SlotState, VmRuntime, VmState, mac_for_name};

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
struct BootConfig {
    name: String,
    cpus: u16,
    memory_mb: u64,
    boot_mode: String,
    disks: Vec<PathBuf>,
    kernel_path: Option<PathBuf>,
    initrd_path: Option<PathBuf>,
    cmdline: Option<String>,
    firmware_path: Option<PathBuf>,
    snapshot_path: Option<PathBuf>,
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
enum ApiError {
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
    validate_boot_config(&payload)?;

    let name = payload.name.clone();
    let mac = mac_for_name(&name);

    let reserved = {
        let pool = state.pool.lock().await;
        pool.allocate_vm_slot(name.clone(), mac.clone())
            .await
            .map_err(map_pool_error)?
    };

    let slot = reserved.slot;
    wait_for_slot_ready(&state, slot).await?;

    let boot_started_at = boot_started_at();
    let proxy_body = serde_json::to_vec(&create_request_body(&payload, &reserved, slot))
        .map_err(|error| ApiError::Backend(error.to_string()))?;

    let pool = state.pool.lock().await;
    let create_path = if payload.snapshot_path.is_some() {
        "/vm.restore"
    } else {
        "/vm.create"
    };

    let create_response = pool
        .proxy_vm_request_with_content_type(
            slot,
            Method::PUT,
            create_path,
            proxy_body,
            Some("application/json"),
        )
        .await
        .map_err(map_pool_error)?;

    if create_response.status >= 400 {
        drop(pool);
        cleanup_reserved_vm(&state, slot).await;
        return Err(ApiError::Backend(format!(
            "backend returned status {}",
            create_response.status
        )));
    }

    if payload.snapshot_path.is_none() {
        let boot_response = pool
            .proxy_vm_request_with_content_type(
                slot,
                Method::PUT,
                "/vm.boot",
                serde_json::to_vec(&serde_json::json!({"name": payload.name}))
                    .map_err(|error| ApiError::Backend(error.to_string()))?,
                Some("application/json"),
            )
            .await
            .map_err(map_pool_error)?;

        if boot_response.status >= 400 {
            drop(pool);
            cleanup_reserved_vm(&state, slot).await;
            return Err(ApiError::Backend(format!(
                "backend returned status {}",
                boot_response.status
            )));
        }
    }

    let status = pool
        .mark_vm_slot_booted(slot, boot_started_at)
        .await
        .map_err(map_pool_error)?;

    Ok(Json(vm_runtime_from_status(&status)?))
}

async fn delete_vm(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<OkResponse>, ApiError> {
    let pool = state.pool.lock().await;
    let status = pool
        .find_vm_slot_status_by_name(&name)
        .await
        .map_err(map_pool_error)?
        .ok_or_else(|| ApiError::NotFound(format!("VM '{name}' not found")))?;

    if status.state != SlotState::Occupied {
        return Err(ApiError::Conflict(format!("VM '{name}' is not running")));
    }

    let slot = status.slot;
    let shutdown = pool
        .proxy_vm_request_with_content_type(slot, Method::PUT, "/vm.shutdown", Vec::new(), None)
        .await
        .map_err(map_pool_error)?;
    if shutdown.status >= 400 {
        drop(pool);
        cleanup_reserved_vm(&state, slot).await;
        return Err(ApiError::Backend(format!(
            "backend returned status {}",
            shutdown.status
        )));
    }

    let delete = pool
        .proxy_vm_request_with_content_type(slot, Method::PUT, "/vm.delete", Vec::new(), None)
        .await
        .map_err(map_pool_error)?;
    if delete.status >= 400 {
        drop(pool);
        cleanup_reserved_vm(&state, slot).await;
        return Err(ApiError::Backend(format!(
            "backend returned status {}",
            delete.status
        )));
    }

    let _ = pool.release_vm_slot(slot).await.map_err(map_pool_error)?;

    Ok(Json(OkResponse { ok: true }))
}

async fn reboot_vm(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<OkResponse>, ApiError> {
    proxy_action_by_name(&state, &name, "/vm.reboot").await
}

async fn pause_vm(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<OkResponse>, ApiError> {
    proxy_action_by_name(&state, &name, "/vm.pause").await
}

async fn resume_vm(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<OkResponse>, ApiError> {
    proxy_action_by_name(&state, &name, "/vm.resume").await
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
    proxy_action_by_name_with_body(&state, &name, "/vm.snapshot", body).await
}

async fn proxy_info(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Response<Body>, ApiError> {
    proxy_passthrough_by_name(&state, &name, "/vm.info").await
}

async fn proxy_counters(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Response<Body>, ApiError> {
    proxy_passthrough_by_name(&state, &name, "/vm.counters").await
}

fn map_pool_error(error: PoolError) -> ApiError {
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

fn validate_boot_config(config: &BootConfig) -> Result<(), ApiError> {
    if config.name.trim().is_empty() {
        return Err(ApiError::Validation {
            field: Some("name".to_string()),
            error: "name is required".to_string(),
        });
    }

    if config.cpus == 0 {
        return Err(ApiError::Validation {
            field: Some("cpus".to_string()),
            error: "cpus must be greater than zero".to_string(),
        });
    }

    if config.memory_mb == 0 {
        return Err(ApiError::Validation {
            field: Some("memoryMb".to_string()),
            error: "memoryMb must be greater than zero".to_string(),
        });
    }

    if config.boot_mode.trim().is_empty() {
        return Err(ApiError::Validation {
            field: Some("bootMode".to_string()),
            error: "bootMode is required".to_string(),
        });
    }

    for disk in &config.disks {
        if !disk.is_absolute() {
            return Err(ApiError::Validation {
                field: Some("disks".to_string()),
                error: format!("disk path '{}' must be absolute", disk.display()),
            });
        }
    }

    for (field, path) in [
        ("kernelPath", config.kernel_path.as_ref()),
        ("initrdPath", config.initrd_path.as_ref()),
        ("firmwarePath", config.firmware_path.as_ref()),
        ("snapshotPath", config.snapshot_path.as_ref()),
    ] {
        if let Some(path) = path
            && !path.is_absolute()
        {
            return Err(ApiError::Validation {
                field: Some(field.to_string()),
                error: format!("{field} must be absolute"),
            });
        }
    }

    Ok(())
}

fn create_request_body(
    config: &BootConfig,
    reserved: &crate::pool::SlotStatus,
    slot: usize,
) -> serde_json::Value {
    serde_json::json!({
        "name": config.name,
        "cpus": config.cpus,
        "memoryMb": config.memory_mb,
        "bootMode": config.boot_mode,
        "disks": config.disks,
        "kernelPath": config.kernel_path,
        "initrdPath": config.initrd_path,
        "cmdline": config.cmdline,
        "firmwarePath": config.firmware_path,
        "snapshotPath": config.snapshot_path,
        "slot": slot,
        "tap": reserved.tap,
        "mac": reserved.mac,
    })
}

fn boot_started_at() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn vm_runtime_from_status(status: &crate::pool::SlotStatus) -> Result<VmRuntime, ApiError> {
    if status.state != SlotState::Occupied {
        return Err(ApiError::Conflict("VM is not running".to_string()));
    }

    Ok(VmRuntime {
        name: status
            .name
            .clone()
            .ok_or_else(|| ApiError::Backend("missing VM name".to_string()))?,
        mac: status
            .mac
            .clone()
            .ok_or_else(|| ApiError::Backend("missing MAC address".to_string()))?,
        tap: status
            .tap
            .clone()
            .ok_or_else(|| ApiError::Backend("missing tap device".to_string()))?,
        pid: status
            .pid
            .ok_or_else(|| ApiError::Backend("missing pid".to_string()))?,
        state: VmState::Running,
        started_at: status
            .started_at
            .clone()
            .ok_or_else(|| ApiError::Backend("missing started_at".to_string()))?,
    })
}

async fn proxy_action_by_name(
    state: &AppState,
    name: &str,
    path: &str,
) -> Result<Json<OkResponse>, ApiError> {
    proxy_action_by_name_with_body(state, name, path, serde_json::json!({})).await
}

async fn proxy_action_by_name_with_body(
    state: &AppState,
    name: &str,
    path: &str,
    body: serde_json::Value,
) -> Result<Json<OkResponse>, ApiError> {
    let pool = state.pool.lock().await;
    let status = pool
        .find_vm_slot_status_by_name(name)
        .await
        .map_err(map_pool_error)?
        .ok_or_else(|| ApiError::NotFound(format!("VM '{name}' not found")))?;

    if status.state != SlotState::Occupied {
        return Err(ApiError::Conflict(format!("VM '{name}' is not running")));
    }

    let response = pool
        .proxy_vm_request_with_content_type(
            status.slot,
            Method::PUT,
            path,
            serde_json::to_vec(&body).map_err(|error| ApiError::Backend(error.to_string()))?,
            Some("application/json"),
        )
        .await
        .map_err(map_pool_error)?;

    if response.status >= 400 {
        return Err(ApiError::Backend(format!(
            "backend returned status {}",
            response.status
        )));
    }

    Ok(Json(OkResponse { ok: true }))
}

async fn proxy_passthrough_by_name(
    state: &AppState,
    name: &str,
    path: &str,
) -> Result<Response<Body>, ApiError> {
    let pool = state.pool.lock().await;
    let status = pool
        .find_vm_slot_status_by_name(name)
        .await
        .map_err(map_pool_error)?
        .ok_or_else(|| ApiError::NotFound(format!("VM '{name}' not found")))?;

    if status.state != SlotState::Occupied {
        return Err(ApiError::Conflict(format!("VM '{name}' is not running")));
    }

    let response = pool
        .proxy_vm_request(status.slot, Method::GET, path, Vec::new())
        .await
        .map_err(map_pool_error)?;

    let mut builder = Response::builder().status(response.status);
    if let Some(content_type) = response.content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }

    builder
        .body(Body::from(response.body))
        .map_err(|error| ApiError::Backend(error.to_string()))
}

async fn wait_for_slot_ready(state: &AppState, slot: usize) -> Result<(), ApiError> {
    let deadline = Duration::from_secs(5);
    let start = tokio::time::Instant::now();

    loop {
        let pool = state.pool.lock().await;
        let status = pool
            .get_vm_slot_status(slot)
            .await
            .map_err(map_pool_error)?;
        let socket_exists = pool.vm_socket_path(slot).exists();
        let ready = status.pid.is_some() && socket_exists;

        if ready {
            return Ok(());
        }

        drop(pool);

        if start.elapsed() >= deadline {
            return Err(ApiError::Backend(format!(
                "slot {slot} did not become ready"
            )));
        }

        sleep(Duration::from_millis(50)).await;
    }
}

async fn cleanup_reserved_vm(state: &AppState, slot: usize) {
    let pool = state.pool.lock().await;
    let _ = pool.reset_vm_slot(slot).await;
}
