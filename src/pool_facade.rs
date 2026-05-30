use std::path::PathBuf;

use axum::http::Method;

use crate::pool::{PoolError, ProcessPool, ProxyResponse, SlotStatus};

pub(crate) trait PoolFacade {
    async fn allocate_slot(&mut self, name: String, mac: String) -> Result<SlotStatus, PoolError>;

    async fn find_slot_by_name(&self, name: &str) -> Result<Option<SlotStatus>, PoolError>;

    async fn mark_slot_booted(
        &self,
        slot: usize,
        started_at: String,
    ) -> Result<SlotStatus, PoolError>;

    async fn release_slot(&self, slot: usize) -> Result<SlotStatus, PoolError>;

    async fn reset_slot(&self, slot: usize) -> Result<SlotStatus, PoolError>;

    async fn get_slot_status(&self, slot: usize) -> Result<SlotStatus, PoolError>;

    fn slot_socket_path(&self, slot: usize) -> PathBuf;

    async fn request(
        &self,
        slot: usize,
        method: Method,
        path: &str,
        body: Vec<u8>,
    ) -> Result<ProxyResponse, PoolError>;

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
