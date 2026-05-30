use std::path::PathBuf;

use axum::http::Method;

use crate::pool::{PoolError, ProcessPool, ProxyResponse, SlotStatus};

/// Service-facing interface for VM slot pool operations.
///
/// This trait isolates orchestration logic from `ProcessPool` internals so service
/// code can be tested against alternative pool implementations.
pub(crate) trait PoolFacade {
    /// Returns the current total number of managed slots.
    ///
    /// This includes both occupied and available slots.
    fn pool_size(&self) -> usize;

    /// Returns `(used, free)` slot counts.
    ///
    /// # Errors
    ///
    /// Returns an error if pool state cannot be read.
    async fn pool_usage_counts(&self) -> Result<(usize, usize), PoolError>;

    /// Lists runtimes for all currently running VMs.
    ///
    /// # Errors
    ///
    /// Returns an error if runtime metadata cannot be collected.
    async fn list_runtimes(&self) -> Result<Vec<crate::pool::VmRuntime>, PoolError>;

    /// Finds runtime metadata for a VM by name.
    ///
    /// Returns `Ok(None)` when no running VM matches `name`.
    ///
    /// # Errors
    ///
    /// Returns an error if lookup fails.
    async fn find_runtime_by_name(
        &self,
        name: &str,
    ) -> Result<Option<crate::pool::VmRuntime>, PoolError>;

    /// Allocates a slot for a VM and records its identity.
    ///
    /// # Errors
    ///
    /// Returns an error if no slot can be allocated or reservation fails.
    async fn allocate_slot(&mut self, name: String, mac: String) -> Result<SlotStatus, PoolError>;

    /// Returns the slot status associated with a VM name.
    ///
    /// Returns `Ok(None)` when no slot is associated with `name`.
    ///
    /// # Errors
    ///
    /// Returns an error if lookup fails.
    async fn find_slot_by_name(&self, name: &str) -> Result<Option<SlotStatus>, PoolError>;

    /// Marks a slot as booted and stores the VM start timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error if the slot cannot be updated.
    async fn mark_slot_booted(
        &self,
        slot: usize,
        started_at: String,
    ) -> Result<SlotStatus, PoolError>;

    /// Releases a slot back to the pool for reuse.
    ///
    /// # Errors
    ///
    /// Returns an error if the slot cannot be transitioned.
    async fn release_slot(&self, slot: usize) -> Result<SlotStatus, PoolError>;

    /// Resets a slot to an empty, reusable state.
    ///
    /// # Errors
    ///
    /// Returns an error if reset fails.
    async fn reset_slot(&self, slot: usize) -> Result<SlotStatus, PoolError>;

    /// Returns the current status for `slot`.
    ///
    /// # Errors
    ///
    /// Returns an error if the slot does not exist or cannot be queried.
    async fn get_slot_status(&self, slot: usize) -> Result<SlotStatus, PoolError>;

    /// Returns the control-plane UNIX socket path for `slot`.
    fn slot_socket_path(&self, slot: usize) -> PathBuf;

    /// Proxies an HTTP request to the VM assigned to `slot`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request cannot be delivered or the slot is invalid.
    async fn request(
        &self,
        slot: usize,
        method: Method,
        path: &str,
        body: Vec<u8>,
    ) -> Result<ProxyResponse, PoolError>;

    /// Proxies an HTTP request with an optional explicit `Content-Type` header.
    ///
    /// # Errors
    ///
    /// Returns an error if the request cannot be delivered or the slot is invalid.
    async fn request_with_content_type(
        &self,
        slot: usize,
        method: Method,
        path: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<ProxyResponse, PoolError>;
}

impl PoolFacade for ProcessPool {
    fn pool_size(&self) -> usize {
        self.size()
    }

    async fn pool_usage_counts(&self) -> Result<(usize, usize), PoolError> {
        self.pool_usage().await
    }

    async fn list_runtimes(&self) -> Result<Vec<crate::pool::VmRuntime>, PoolError> {
        self.list_running_vms().await
    }

    async fn find_runtime_by_name(
        &self,
        name: &str,
    ) -> Result<Option<crate::pool::VmRuntime>, PoolError> {
        self.find_vm_runtime_by_name(name).await
    }

    async fn allocate_slot(&mut self, name: String, mac: String) -> Result<SlotStatus, PoolError> {
        self.allocate_vm_slot(name, mac).await
    }

    async fn find_slot_by_name(&self, name: &str) -> Result<Option<SlotStatus>, PoolError> {
        self.find_vm_slot_status_by_name(name).await
    }

    async fn mark_slot_booted(
        &self,
        slot: usize,
        started_at: String,
    ) -> Result<SlotStatus, PoolError> {
        self.mark_vm_slot_booted(slot, started_at).await
    }

    async fn release_slot(&self, slot: usize) -> Result<SlotStatus, PoolError> {
        self.release_vm_slot(slot).await
    }

    async fn reset_slot(&self, slot: usize) -> Result<SlotStatus, PoolError> {
        self.reset_vm_slot(slot).await
    }

    async fn get_slot_status(&self, slot: usize) -> Result<SlotStatus, PoolError> {
        self.get_vm_slot_status(slot).await
    }

    fn slot_socket_path(&self, slot: usize) -> PathBuf {
        self.vm_socket_path(slot)
    }

    async fn request(
        &self,
        slot: usize,
        method: Method,
        path: &str,
        body: Vec<u8>,
    ) -> Result<ProxyResponse, PoolError> {
        self.proxy_vm_request(slot, method, path, body).await
    }

    async fn request_with_content_type(
        &self,
        slot: usize,
        method: Method,
        path: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<ProxyResponse, PoolError> {
        self.proxy_vm_request_with_content_type(slot, method, path, body, content_type)
            .await
    }
}
