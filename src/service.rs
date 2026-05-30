use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::http::Method;
use serde::Deserialize;
use tokio::net::UnixStream;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::error::ApiError;
use crate::pool::{PoolError, ProxyResponse, SlotState, VmRuntime, VmState};
use crate::pool_facade::PoolFacade;
use crate::vm_payload::{
    create_restore_request_body, create_vm_request_body, format_backend_error,
};
use crate::vm_validation::validate_boot_config;

pub(crate) type SharedPool = Arc<RwLock<crate::pool::ProcessPool>>;

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

pub(crate) async fn create_vm(
    pool: &SharedPool,
    payload: BootConfig,
) -> Result<VmRuntime, ApiError> {
    create_vm_with_pool(pool, payload).await
}

async fn create_vm_with_pool<P: PoolFacade>(
    pool: &Arc<RwLock<P>>,
    payload: BootConfig,
) -> Result<VmRuntime, ApiError> {
    validate_boot_config(&payload)?;

    let name = payload.name.clone();
    let mac = mac_for_name(&name);

    let reserved = {
        let mut pool = pool.write().await;
        pool.allocate_slot(name.clone(), mac).await?
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

    let pool_guard = pool.read().await;
    let create_path = if payload.snapshot_path.is_some() {
        "/api/v1/vm.restore"
    } else {
        "/api/v1/vm.create"
    };

    let create_response = pool_guard
        .request_with_content_type(
            slot,
            Method::PUT,
            create_path,
            proxy_body,
            Some("application/json"),
        )
        .await?;

    if create_response.status >= 400 {
        drop(pool_guard);
        cleanup_reserved_vm(pool, slot).await;
        return Err(ApiError::Backend(format_backend_error(&create_response)));
    }

    if payload.snapshot_path.is_none() {
        let boot_response = pool_guard
            .request_with_content_type(slot, Method::PUT, "/api/v1/vm.boot", Vec::new(), None)
            .await?;

        if boot_response.status >= 400 {
            drop(pool_guard);
            cleanup_reserved_vm(pool, slot).await;
            return Err(ApiError::Backend(format_backend_error(&boot_response)));
        }
    }

    drop(pool_guard);
    wait_for_slot_ready(pool, slot).await?;
    let pool_guard = pool.read().await;

    let status = pool_guard.mark_slot_booted(slot, boot_started_at).await?;

    (&status).try_into().map_err(ApiError::from)
}

pub(crate) async fn delete_vm(pool: &SharedPool, name: &str) -> Result<(), ApiError> {
    delete_vm_with_pool(pool, name).await
}

async fn delete_vm_with_pool<P: PoolFacade>(
    pool: &Arc<RwLock<P>>,
    name: &str,
) -> Result<(), ApiError> {
    let pool_guard = pool.read().await;
    let status = pool_guard
        .find_slot_by_name(name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("VM '{name}' not found")))?;

    if status.state != SlotState::Occupied {
        return Err(ApiError::Conflict(format!("VM '{name}' is not running")));
    }

    let slot = status.slot;
    let shutdown = pool_guard
        .request_with_content_type(slot, Method::PUT, "/api/v1/vm.shutdown", Vec::new(), None)
        .await?;
    if shutdown.status >= 400 {
        drop(pool_guard);
        cleanup_reserved_vm(pool, slot).await;
        return Err(ApiError::Backend(format_backend_error(&shutdown)));
    }

    let delete = pool_guard
        .request_with_content_type(slot, Method::PUT, "/api/v1/vm.delete", Vec::new(), None)
        .await?;
    if delete.status >= 400 {
        drop(pool_guard);
        cleanup_reserved_vm(pool, slot).await;
        return Err(ApiError::Backend(format_backend_error(&delete)));
    }

    let _ = pool_guard.release_slot(slot).await?;
    Ok(())
}

pub(crate) async fn proxy_action_by_name(
    pool: &SharedPool,
    name: &str,
    path: &str,
) -> Result<(), ApiError> {
    proxy_action_by_name_with_body(pool, name, path, serde_json::json!({})).await
}

pub(crate) async fn proxy_action_by_name_with_body(
    pool: &SharedPool,
    name: &str,
    path: &str,
    body: serde_json::Value,
) -> Result<(), ApiError> {
    proxy_action_by_name_with_body_for_pool(pool, name, path, body).await
}

async fn proxy_action_by_name_with_body_for_pool<P: PoolFacade>(
    pool: &Arc<RwLock<P>>,
    name: &str,
    path: &str,
    body: serde_json::Value,
) -> Result<(), ApiError> {
    let pool_guard = pool.read().await;
    let status = pool_guard
        .find_slot_by_name(name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("VM '{name}' not found")))?;

    if status.state != SlotState::Occupied {
        return Err(ApiError::Conflict(format!("VM '{name}' is not running")));
    }

    let response = pool_guard
        .request_with_content_type(
            status.slot,
            Method::PUT,
            path,
            serde_json::to_vec(&body).map_err(|error| ApiError::Backend(error.to_string()))?,
            Some("application/json"),
        )
        .await?;

    if response.status >= 400 {
        return Err(ApiError::Backend(format_backend_error(&response)));
    }

    Ok(())
}

pub(crate) async fn proxy_passthrough_by_name(
    pool: &SharedPool,
    name: &str,
    path: &str,
) -> Result<ProxyResponse, ApiError> {
    proxy_passthrough_by_name_for_pool(pool, name, path).await
}

async fn proxy_passthrough_by_name_for_pool<P: PoolFacade>(
    pool: &Arc<RwLock<P>>,
    name: &str,
    path: &str,
) -> Result<ProxyResponse, ApiError> {
    let pool_guard = pool.read().await;
    let status = pool_guard
        .find_slot_by_name(name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("VM '{name}' not found")))?;

    if status.state != SlotState::Occupied {
        return Err(ApiError::Conflict(format!("VM '{name}' is not running")));
    }

    pool_guard
        .request(status.slot, Method::GET, path, Vec::new())
        .await
        .map_err(ApiError::from)
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

async fn wait_for_slot_ready<P: PoolFacade>(
    pool: &Arc<RwLock<P>>,
    slot: usize,
) -> Result<(), ApiError> {
    let deadline = Duration::from_secs(5);
    let start = tokio::time::Instant::now();

    loop {
        let pool = pool.read().await;
        let status = pool.get_slot_status(slot).await?;
        let socket_path = pool.slot_socket_path(slot);
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

async fn cleanup_reserved_vm<P: PoolFacade>(pool: &Arc<RwLock<P>>, slot: usize) {
    let pool = pool.read().await;
    let _ = pool.reset_slot(slot).await;
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;

    use axum::http::Method;
    use tokio::sync::RwLock;

    use super::{
        delete_vm_with_pool, proxy_action_by_name_with_body_for_pool,
        proxy_passthrough_by_name_for_pool,
    };
    use crate::error::ApiError;
    use crate::pool::{PoolError, ProxyResponse, SlotState, SlotStatus, VmRuntime};
    use crate::pool_facade::PoolFacade;

    struct FakePool {
        slot_by_name: Option<SlotStatus>,
        passthrough_response: Option<ProxyResponse>,
        scripted_request_responses: Mutex<VecDeque<ProxyResponse>>,
        request_paths: Mutex<Vec<String>>,
        released_slots: Mutex<Vec<usize>>,
    }

    impl FakePool {
        fn occupied_status(name: &str, slot: usize) -> SlotStatus {
            SlotStatus {
                slot,
                generation: 1,
                state: SlotState::Occupied,
                name: Some(name.to_string()),
                mac: Some("02:aa:bb:cc:dd:ee".to_string()),
                tap: Some(format!("tap{slot}")),
                pid: Some(1234),
                started_at: Some("2026-05-30T00:00:00Z".to_string()),
                last_error: None,
            }
        }
    }

    impl PoolFacade for FakePool {
        fn pool_size(&self) -> usize {
            1
        }

        async fn pool_usage_counts(&self) -> Result<(usize, usize), PoolError> {
            Ok((1, 0))
        }

        async fn list_runtimes(&self) -> Result<Vec<VmRuntime>, PoolError> {
            Ok(Vec::new())
        }

        async fn find_runtime_by_name(&self, _name: &str) -> Result<Option<VmRuntime>, PoolError> {
            Ok(None)
        }

        async fn allocate_slot(
            &mut self,
            _name: String,
            _mac: String,
        ) -> Result<SlotStatus, PoolError> {
            Err(PoolError::Backend("unused in test".to_string()))
        }

        async fn find_slot_by_name(&self, name: &str) -> Result<Option<SlotStatus>, PoolError> {
            Ok(self
                .slot_by_name
                .as_ref()
                .filter(|status| status.name.as_deref() == Some(name))
                .cloned())
        }

        async fn mark_slot_booted(
            &self,
            _slot: usize,
            _started_at: String,
        ) -> Result<SlotStatus, PoolError> {
            Err(PoolError::Backend("unused in test".to_string()))
        }

        async fn release_slot(&self, slot: usize) -> Result<SlotStatus, PoolError> {
            self.released_slots
                .lock()
                .expect("released_slots lock poisoned")
                .push(slot);
            Ok(SlotStatus {
                slot,
                generation: 2,
                state: SlotState::Empty,
                name: None,
                mac: None,
                tap: None,
                pid: None,
                started_at: None,
                last_error: None,
            })
        }

        async fn reset_slot(&self, _slot: usize) -> Result<SlotStatus, PoolError> {
            Ok(SlotStatus {
                slot: 0,
                generation: 0,
                state: SlotState::Empty,
                name: None,
                mac: None,
                tap: None,
                pid: None,
                started_at: None,
                last_error: None,
            })
        }

        async fn get_slot_status(&self, _slot: usize) -> Result<SlotStatus, PoolError> {
            Err(PoolError::Backend("unused in test".to_string()))
        }

        fn slot_socket_path(&self, _slot: usize) -> PathBuf {
            PathBuf::from("/tmp/unused.sock")
        }

        async fn request(
            &self,
            _slot: usize,
            _method: Method,
            _path: &str,
            _body: Vec<u8>,
        ) -> Result<ProxyResponse, PoolError> {
            self.passthrough_response
                .clone()
                .ok_or_else(|| PoolError::Backend("missing passthrough response".to_string()))
        }

        async fn request_with_content_type(
            &self,
            _slot: usize,
            method: Method,
            path: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<ProxyResponse, PoolError> {
            self.request_paths
                .lock()
                .expect("request_paths lock poisoned")
                .push(format!("{} {}", method, path));

            self.scripted_request_responses
                .lock()
                .expect("scripted_request_responses lock poisoned")
                .pop_front()
                .ok_or_else(|| PoolError::Backend("missing scripted response".to_string()))
        }
    }

    #[tokio::test]
    async fn delete_vm_returns_conflict_for_non_running_vm() {
        let pool = FakePool {
            slot_by_name: Some(SlotStatus {
                state: SlotState::Booting,
                ..FakePool::occupied_status("vm-a", 3)
            }),
            passthrough_response: None,
            scripted_request_responses: Mutex::new(VecDeque::new()),
            request_paths: Mutex::new(Vec::new()),
            released_slots: Mutex::new(Vec::new()),
        };

        let shared = Arc::new(RwLock::new(pool));
        let error = delete_vm_with_pool(&shared, "vm-a")
            .await
            .expect_err("expected conflict");

        match error {
            ApiError::Conflict(message) => assert!(message.contains("is not running")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn proxy_action_returns_not_found_for_unknown_vm() {
        let pool = FakePool {
            slot_by_name: None,
            passthrough_response: None,
            scripted_request_responses: Mutex::new(VecDeque::new()),
            request_paths: Mutex::new(Vec::new()),
            released_slots: Mutex::new(Vec::new()),
        };

        let shared = Arc::new(RwLock::new(pool));
        let error = proxy_action_by_name_with_body_for_pool(
            &shared,
            "missing",
            "/api/v1/vm.pause",
            serde_json::json!({}),
        )
        .await
        .expect_err("expected not found");

        match error {
            ApiError::NotFound(message) => assert!(message.contains("not found")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn proxy_passthrough_returns_backend_response_for_running_vm() {
        let pool = FakePool {
            slot_by_name: Some(FakePool::occupied_status("vm-a", 5)),
            passthrough_response: Some(ProxyResponse {
                status: 200,
                content_type: Some("application/json".to_string()),
                body: br#"{"ok":true}"#.to_vec(),
            }),
            scripted_request_responses: Mutex::new(VecDeque::new()),
            request_paths: Mutex::new(Vec::new()),
            released_slots: Mutex::new(Vec::new()),
        };

        let shared = Arc::new(RwLock::new(pool));
        let response = proxy_passthrough_by_name_for_pool(&shared, "vm-a", "/api/v1/vm.info")
            .await
            .expect("expected passthrough response");

        assert_eq!(response.status, 200);
        assert_eq!(response.content_type.as_deref(), Some("application/json"));
        assert_eq!(response.body, br#"{"ok":true}"#.to_vec());
    }

    #[tokio::test]
    async fn delete_vm_success_calls_shutdown_then_delete_then_release() {
        let pool = FakePool {
            slot_by_name: Some(FakePool::occupied_status("vm-a", 7)),
            passthrough_response: None,
            scripted_request_responses: Mutex::new(VecDeque::from(vec![
                ProxyResponse {
                    status: 200,
                    content_type: None,
                    body: Vec::new(),
                },
                ProxyResponse {
                    status: 200,
                    content_type: None,
                    body: Vec::new(),
                },
            ])),
            request_paths: Mutex::new(Vec::new()),
            released_slots: Mutex::new(Vec::new()),
        };

        let shared = Arc::new(RwLock::new(pool));
        delete_vm_with_pool(&shared, "vm-a")
            .await
            .expect("expected delete success");

        let guard = shared.read().await;
        let paths = guard
            .request_paths
            .lock()
            .expect("request_paths lock poisoned")
            .clone();
        let released = guard
            .released_slots
            .lock()
            .expect("released_slots lock poisoned")
            .clone();

        assert_eq!(
            paths,
            vec!["PUT /api/v1/vm.shutdown", "PUT /api/v1/vm.delete"]
        );
        assert_eq!(released, vec![7]);
    }

    #[tokio::test]
    async fn proxy_action_returns_backend_error_on_unsuccessful_status() {
        let pool = FakePool {
            slot_by_name: Some(FakePool::occupied_status("vm-a", 2)),
            passthrough_response: None,
            scripted_request_responses: Mutex::new(VecDeque::from(vec![ProxyResponse {
                status: 500,
                content_type: Some("text/plain".to_string()),
                body: b"boom".to_vec(),
            }])),
            request_paths: Mutex::new(Vec::new()),
            released_slots: Mutex::new(Vec::new()),
        };

        let shared = Arc::new(RwLock::new(pool));
        let error = proxy_action_by_name_with_body_for_pool(
            &shared,
            "vm-a",
            "/api/v1/vm.pause",
            serde_json::json!({}),
        )
        .await
        .expect_err("expected backend error");

        match error {
            ApiError::Backend(message) => {
                assert!(message.contains("status 500"));
                assert!(message.contains("boom"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
