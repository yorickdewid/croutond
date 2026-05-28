use std::path::Path;
use std::time::Duration;

use axum::http::Method;
use tokio::net::UnixStream;
use tokio::time::sleep;

use crate::api::{ApiError, BootConfig, SharedPool, map_pool_error};
use crate::pool::{PoolError, ProxyResponse, SlotState, VmRuntime, VmState};

pub(crate) async fn create_vm(
    pool: &SharedPool,
    payload: BootConfig,
) -> Result<VmRuntime, ApiError> {
    validate_boot_config(&payload)?;

    let name = payload.name.clone();
    let mac = mac_for_name(&name);

    let reserved = {
        let pool = pool.lock().await;
        pool.allocate_vm_slot(name.clone(), mac)
            .await
            .map_err(map_pool_error)?
    };

    let slot = reserved.slot;
    wait_for_slot_ready(pool, slot).await?;

    let boot_started_at = boot_started_at();
    let proxy_body = if payload.snapshot_path.is_some() {
        serde_json::to_vec(&create_restore_request_body(&payload))
            .map_err(|error| ApiError::Backend(error.to_string()))?
    } else {
        serde_json::to_vec(&create_vm_request_body(&payload, &reserved))
            .map_err(|error| ApiError::Backend(error.to_string()))?
    };

    let pool_guard = pool.lock().await;
    let create_path = if payload.snapshot_path.is_some() {
        "/api/v1/vm.restore"
    } else {
        "/api/v1/vm.create"
    };

    let create_response = pool_guard
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
        drop(pool_guard);
        cleanup_reserved_vm(pool, slot).await;
        return Err(ApiError::Backend(format_backend_error(&create_response)));
    }

    if payload.snapshot_path.is_none() {
        let boot_response = pool_guard
            .proxy_vm_request_with_content_type(
                slot,
                Method::PUT,
                "/api/v1/vm.boot",
                Vec::new(),
                None,
            )
            .await
            .map_err(map_pool_error)?;

        if boot_response.status >= 400 {
            drop(pool_guard);
            cleanup_reserved_vm(pool, slot).await;
            return Err(ApiError::Backend(format_backend_error(&boot_response)));
        }
    }

    drop(pool_guard);
    wait_for_slot_ready(pool, slot).await?;
    let pool_guard = pool.lock().await;

    let status = pool_guard
        .mark_vm_slot_booted(slot, boot_started_at)
        .await
        .map_err(map_pool_error)?;

    (&status).try_into().map_err(map_pool_error)
}

pub(crate) async fn delete_vm(pool: &SharedPool, name: &str) -> Result<(), ApiError> {
    let pool_guard = pool.lock().await;
    let status = pool_guard
        .find_vm_slot_status_by_name(name)
        .await
        .map_err(map_pool_error)?
        .ok_or_else(|| ApiError::NotFound(format!("VM '{name}' not found")))?;

    if status.state != SlotState::Occupied {
        return Err(ApiError::Conflict(format!("VM '{name}' is not running")));
    }

    let slot = status.slot;
    let shutdown = pool_guard
        .proxy_vm_request_with_content_type(
            slot,
            Method::PUT,
            "/api/v1/vm.shutdown",
            Vec::new(),
            None,
        )
        .await
        .map_err(map_pool_error)?;
    if shutdown.status >= 400 {
        drop(pool_guard);
        cleanup_reserved_vm(pool, slot).await;
        return Err(ApiError::Backend(format_backend_error(&shutdown)));
    }

    let delete = pool_guard
        .proxy_vm_request_with_content_type(
            slot,
            Method::PUT,
            "/api/v1/vm.delete",
            Vec::new(),
            None,
        )
        .await
        .map_err(map_pool_error)?;
    if delete.status >= 400 {
        drop(pool_guard);
        cleanup_reserved_vm(pool, slot).await;
        return Err(ApiError::Backend(format_backend_error(&delete)));
    }

    let _ = pool_guard
        .release_vm_slot(slot)
        .await
        .map_err(map_pool_error)?;
    Ok(())
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

    let boot_mode = config.boot_mode.to_ascii_lowercase();
    if boot_mode != "linux" && boot_mode != "uefi" {
        return Err(ApiError::Validation {
            field: Some("bootMode".to_string()),
            error: "bootMode must be one of: linux, uefi".to_string(),
        });
    }

    if config.memory_mb > (u64::MAX >> 20) {
        return Err(ApiError::Validation {
            field: Some("memoryMb".to_string()),
            error: "memoryMb is too large".to_string(),
        });
    }

    if config.snapshot_path.is_none() {
        if boot_mode == "linux" && config.kernel_path.is_none() {
            return Err(ApiError::Validation {
                field: Some("kernelPath".to_string()),
                error: "kernelPath is required when bootMode=linux".to_string(),
            });
        }

        if boot_mode == "uefi" && config.firmware_path.is_none() {
            return Err(ApiError::Validation {
                field: Some("firmwarePath".to_string()),
                error: "firmwarePath is required when bootMode=uefi".to_string(),
            });
        }
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

fn create_vm_request_body(
    config: &BootConfig,
    reserved: &crate::pool::SlotStatus,
) -> serde_json::Value {
    let boot_mode = config.boot_mode.to_ascii_lowercase();
    let payload = if boot_mode == "uefi" {
        serde_json::json!({
            "firmware": config.firmware_path,
        })
    } else {
        serde_json::json!({
            "kernel": config.kernel_path,
            "initramfs": config.initrd_path,
            "cmdline": config.cmdline,
        })
    };

    let disks: Vec<serde_json::Value> = config
        .disks
        .iter()
        .map(|path| match disk_image_type(path) {
            Some(image_type) => serde_json::json!({
                "path": path,
                "image_type": image_type,
            }),
            None => serde_json::json!({"path": path}),
        })
        .collect();

    serde_json::json!({
        "cpus": {
            "boot_vcpus": config.cpus,
            "max_vcpus": config.cpus,
        },
        "memory": {
            "size": config.memory_mb << 20,
        },
        "payload": payload,
        "disks": disks,
        "net": [{
            "tap": reserved.tap,
            "mac": reserved.mac,
        }],
        "rng": {
            "src": "/dev/urandom",
        }
    })
}

fn create_restore_request_body(config: &BootConfig) -> serde_json::Value {
    let source_url = config
        .snapshot_path
        .as_ref()
        .map(|path| format!("file://{}", path.display()));

    serde_json::json!({
        "source_url": source_url,
    })
}

fn disk_image_type(path: &Path) -> Option<&'static str> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    match extension.as_str() {
        "qcow2" | "qcow" => Some("qcow2"),
        "raw" => Some("raw"),
        _ => None,
    }
}

fn format_backend_error(response: &ProxyResponse) -> String {
    if response.body.is_empty() {
        return format!("backend returned status {}", response.status);
    }

    let body = String::from_utf8_lossy(&response.body);
    let body = body.trim();
    if body.is_empty() {
        format!("backend returned status {}", response.status)
    } else {
        format!("backend returned status {}: {}", response.status, body)
    }
}

fn boot_started_at() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn mac_for_name(name: &str) -> String {
    use sha2::{Digest, Sha256};

    let hash = Sha256::digest(name.as_bytes());
    format!(
        "02:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        hash[0], hash[1], hash[2], hash[3], hash[4]
    )
}

impl TryFrom<&crate::pool::SlotStatus> for VmRuntime {
    type Error = PoolError;

    fn try_from(status: &crate::pool::SlotStatus) -> Result<Self, Self::Error> {
        if status.state != SlotState::Occupied {
            return Err(PoolError::InvalidTransition {
                slot: status.slot,
                from: status.state,
                action: "convert_runtime",
            });
        }

        Ok(Self {
            name: status
                .name
                .clone()
                .ok_or_else(|| PoolError::Backend("missing VM name".to_string()))?,
            mac: status
                .mac
                .clone()
                .ok_or_else(|| PoolError::Backend("missing MAC address".to_string()))?,
            tap: status
                .tap
                .clone()
                .ok_or_else(|| PoolError::Backend("missing tap device".to_string()))?,
            pid: status
                .pid
                .ok_or_else(|| PoolError::Backend("missing pid".to_string()))?,
            state: VmState::Running,
            started_at: status
                .started_at
                .clone()
                .ok_or_else(|| PoolError::Backend("missing started_at".to_string()))?,
        })
    }
}

async fn wait_for_slot_ready(pool: &SharedPool, slot: usize) -> Result<(), ApiError> {
    let deadline = Duration::from_secs(5);
    let start = tokio::time::Instant::now();

    loop {
        let pool = pool.lock().await;
        let status = pool
            .get_vm_slot_status(slot)
            .await
            .map_err(map_pool_error)?;
        let socket_path = pool.vm_socket_path(slot);
        let has_pid = status.pid.is_some();

        drop(pool);

        // File existence can race; a successful connect confirms the server socket is usable.
        if has_pid && UnixStream::connect(&socket_path).await.is_ok() {
            return Ok(());
        }

        if start.elapsed() >= deadline {
            return Err(ApiError::Backend(format!(
                "slot {slot} did not become ready"
            )));
        }

        sleep(Duration::from_millis(50)).await;
    }
}

async fn cleanup_reserved_vm(pool: &SharedPool, slot: usize) {
    let pool = pool.lock().await;
    let _ = pool.reset_vm_slot(slot).await;
}
