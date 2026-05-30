use std::ffi::OsString;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::http::Method;
use serde::Serialize;
use tokio::{
    sync::{oneshot, watch},
    time::Instant,
};
use tracing::debug;
use tracing::{error, info, warn};

use crate::ch_client::send_unix_http_request;
use crate::slot_supervisor::{SlotCommand, SlotHandle, initialize_slot};

pub struct ProcessPool {
    shutdown_tx: watch::Sender<bool>,
    slots: Vec<SlotHandle>,
    vm_path: PathBuf,
    program: String,
    args: Vec<OsString>,
    bridge: Option<String>,
    min_pool_size: usize,
    max_pool_size: usize,
    scale_down_cooldown: Duration,
    last_scale_down_at: Option<Instant>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VmRuntime {
    pub name: String,
    pub mac: String,
    pub tap: String,
    pub pid: u32,
    pub state: VmState,
    pub started_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VmState {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SlotState {
    Empty,
    Booting,
    Occupied,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct SlotStatus {
    pub slot: usize,
    pub generation: u64,
    pub state: SlotState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mac: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tap: Option<String>,
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug)]
pub enum PoolError {
    InvalidTransition {
        slot: usize,
        from: SlotState,
        action: &'static str,
    },
    SlotNotFound(usize),
    VmAlreadyRunning(String),
    NoAvailableSlot,
    ChannelClosed,
    Backend(String),
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTransition { slot, from, action } => {
                write!(f, "slot {slot} cannot perform {action} from state {from:?}")
            }
            Self::SlotNotFound(slot) => write!(f, "slot {slot} does not exist"),
            Self::VmAlreadyRunning(name) => write!(f, "VM '{name}' is already running"),
            Self::NoAvailableSlot => write!(f, "pool is exhausted"),
            Self::ChannelClosed => write!(f, "slot worker channel is closed"),
            Self::Backend(error) => write!(f, "backend request failed: {error}"),
        }
    }
}

impl std::error::Error for PoolError {}

#[derive(Debug, Clone)]
pub struct ProxyResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}

impl ProcessPool {
    /// Creates a new process pool and starts one worker per slot.
    ///
    /// Each worker is configured with its own API socket under `vm_path` and
    /// receives the supplied program arguments plus the slot-specific socket
    /// path.
    pub async fn spawn(
        min_pool_size: usize,
        max_pool_size: usize,
        scale_down_cooldown: Duration,
        program: &str,
        args: &[OsString],
        bridge: Option<&str>,
        vm_path: &Path,
    ) -> std::io::Result<Self> {
        if max_pool_size < min_pool_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "max_pool_size ({max_pool_size}) must be >= min_pool_size ({min_pool_size})"
                ),
            ));
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut slots = Vec::with_capacity(min_pool_size);

        for slot in 0..min_pool_size {
            let shutdown_rx = shutdown_rx.clone();
            slots.push(initialize_slot(
                slot,
                program,
                args,
                bridge,
                vm_path,
                shutdown_rx,
            ));
        }

        Ok(Self {
            shutdown_tx,
            slots,
            vm_path: vm_path.to_path_buf(),
            program: program.to_string(),
            args: args.to_vec(),
            bridge: bridge.map(str::to_string),
            min_pool_size,
            max_pool_size,
            scale_down_cooldown,
            last_scale_down_at: None,
        })
    }

    pub async fn extend(&mut self, additional_size: usize) {
        let current_size = self.slots.len();
        let new_size = current_size + additional_size;

        for slot in current_size..new_size {
            let shutdown_rx = self.shutdown_tx.subscribe();
            self.slots.push(initialize_slot(
                slot,
                &self.program,
                &self.args,
                self.bridge.as_deref(),
                &self.vm_path,
                shutdown_rx,
            ));
        }

        info!(old_size = current_size, new_size, "pool scaled up");
        self.log_pool_occupancy("extend").await;
    }

    pub async fn list_vm_slots(&self) -> Result<Vec<SlotStatus>, PoolError> {
        let mut statuses = Vec::with_capacity(self.slots.len());

        for slot in 0..self.slots.len() {
            statuses.push(self.get_vm_slot_status(slot).await?);
        }

        Ok(statuses)
    }

    pub async fn get_vm_slot_status(&self, slot: usize) -> Result<SlotStatus, PoolError> {
        let handle = self.slots.get(slot).ok_or(PoolError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::GetVmStatus { response: tx })
            .await
            .map_err(|_| PoolError::ChannelClosed)?;

        rx.await.map_err(|_| PoolError::ChannelClosed)
    }

    pub async fn pool_usage(&self) -> Result<(usize, usize), PoolError> {
        let statuses = self.list_vm_slots().await?;
        let idle = statuses
            .iter()
            .filter(|status| status.state == SlotState::Empty)
            .count();
        Ok((statuses.len() - idle, idle))
    }

    pub fn size(&self) -> usize {
        self.slots.len()
    }

    pub fn vm_socket_path(&self, slot: usize) -> PathBuf {
        self.vm_path.join(format!("vmm{slot}.sock"))
    }

    pub async fn find_vm_slot_status_by_name(
        &self,
        name: &str,
    ) -> Result<Option<SlotStatus>, PoolError> {
        Ok(self
            .list_vm_slots()
            .await?
            .into_iter()
            .find(|status| status.name.as_deref() == Some(name)))
    }

    pub async fn find_vm_runtime_by_name(
        &self,
        name: &str,
    ) -> Result<Option<VmRuntime>, PoolError> {
        Ok(self
            .list_running_vms()
            .await?
            .into_iter()
            .find(|runtime| runtime.name == name))
    }

    pub async fn list_running_vms(&self) -> Result<Vec<VmRuntime>, PoolError> {
        let mut running = Vec::new();

        for status in self.list_vm_slots().await? {
            if let Some(runtime) = runtime_from_slot_status(&status) {
                running.push(runtime);
            }
        }

        Ok(running)
    }

    pub async fn reserve_vm_slot(
        &self,
        slot: usize,
        name: String,
        mac: String,
        tap: String,
    ) -> Result<SlotStatus, PoolError> {
        let handle = self.slots.get(slot).ok_or(PoolError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::ReserveVm {
                name,
                mac,
                tap,
                response: tx,
            })
            .await
            .map_err(|_| PoolError::ChannelClosed)?;

        let status = rx.await.map_err(|_| PoolError::ChannelClosed)??;
        info!(
            slot = status.slot,
            generation = status.generation,
            state = ?status.state,
            name = status.name.as_deref().unwrap_or("-"),
            tap = status.tap.as_deref().unwrap_or("-"),
            "slot reserved"
        );
        self.log_pool_occupancy("reserve_vm_slot").await;
        Ok(status)
    }

    pub async fn allocate_vm_slot(
        &mut self,
        name: String,
        mac: String,
    ) -> Result<SlotStatus, PoolError> {
        if self.find_vm_slot_status_by_name(&name).await?.is_some() {
            return Err(PoolError::VmAlreadyRunning(name));
        }

        loop {
            if let Some(slot) = self
                .list_vm_slots()
                .await?
                .into_iter()
                .find(|status| status.state == SlotState::Empty)
                .map(|status| status.slot)
            {
                let tap = format!("tap{slot}");
                return self.reserve_vm_slot(slot, name, mac, tap).await;
            }

            if self.slots.len() >= self.max_pool_size {
                return Err(PoolError::NoAvailableSlot);
            }

            self.extend(1).await;
        }
    }

    pub async fn autoscale_tick(&mut self) -> Result<bool, PoolError> {
        if self.slots.len() <= self.min_pool_size {
            return Ok(false);
        }

        if let Some(last_scale_down_at) = self.last_scale_down_at
            && last_scale_down_at.elapsed() < self.scale_down_cooldown
        {
            return Ok(false);
        }

        let shrink_slot = self.slots.len() - 1;
        let status = self.get_vm_slot_status(shrink_slot).await?;
        if status.state != SlotState::Empty {
            return Ok(false);
        }

        if let Some(slot) = self.slots.pop() {
            if let Err(error) = slot.worker.await {
                error!(%error, "worker task failed during scale down");
            }
            self.last_scale_down_at = Some(Instant::now());
            info!(
                slot = shrink_slot,
                size = self.slots.len(),
                "pool scaled down"
            );
            self.log_pool_occupancy("autoscale_tick.scale_down").await;
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn mark_vm_slot_booted(
        &self,
        slot: usize,
        started_at: String,
    ) -> Result<SlotStatus, PoolError> {
        let handle = self.slots.get(slot).ok_or(PoolError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::MarkVmBooted {
                started_at,
                response: tx,
            })
            .await
            .map_err(|_| PoolError::ChannelClosed)?;

        let status = rx.await.map_err(|_| PoolError::ChannelClosed)??;
        info!(
            slot = status.slot,
            generation = status.generation,
            state = ?status.state,
            name = status.name.as_deref().unwrap_or("-"),
            pid = status.pid.unwrap_or_default(),
            started_at = status.started_at.as_deref().unwrap_or("-"),
            "slot marked booted"
        );
        self.log_pool_occupancy("mark_vm_slot_booted").await;
        Ok(status)
    }

    pub async fn release_vm_slot(&self, slot: usize) -> Result<SlotStatus, PoolError> {
        let handle = self.slots.get(slot).ok_or(PoolError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::ReleaseVm { response: tx })
            .await
            .map_err(|_| PoolError::ChannelClosed)?;

        let status = rx.await.map_err(|_| PoolError::ChannelClosed)??;
        info!(
            slot = status.slot,
            generation = status.generation,
            state = ?status.state,
            "slot released"
        );
        self.log_pool_occupancy("release_vm_slot").await;
        Ok(status)
    }

    pub async fn reset_vm_slot(&self, slot: usize) -> Result<SlotStatus, PoolError> {
        let handle = self.slots.get(slot).ok_or(PoolError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::ResetVm { response: tx })
            .await
            .map_err(|_| PoolError::ChannelClosed)?;

        let status = rx.await.map_err(|_| PoolError::ChannelClosed)??;
        info!(
            slot = status.slot,
            generation = status.generation,
            state = ?status.state,
            "slot reset"
        );
        self.log_pool_occupancy("reset_vm_slot").await;
        Ok(status)
    }

    pub async fn proxy_vm_request(
        &self,
        slot: usize,
        method: Method,
        path: &str,
        body: Vec<u8>,
    ) -> Result<ProxyResponse, PoolError> {
        self.proxy_vm_request_with_content_type(slot, method, path, body, None)
            .await
    }

    pub async fn proxy_vm_request_with_content_type(
        &self,
        slot: usize,
        method: Method,
        path: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<ProxyResponse, PoolError> {
        let socket_path = self.vm_socket_path(slot);
        send_unix_http_request(&socket_path, method, path, body, content_type)
            .await
            .map_err(|error| PoolError::Backend(error.to_string()))
    }

    pub async fn shutdown(&mut self) {
        if self.shutdown_tx.send(true).is_err() {
            warn!("failed to broadcast shutdown to workers");
        }

        while let Some(slot) = self.slots.pop() {
            if let Err(error) = slot.worker.await {
                error!(%error, "worker task failed during shutdown");
            }
        }
    }

    async fn log_pool_occupancy(&self, reason: &'static str) {
        match self.list_vm_slots().await {
            Ok(statuses) => {
                let occupied = statuses
                    .iter()
                    .filter(|status| status.state == SlotState::Occupied)
                    .count();
                let booting = statuses
                    .iter()
                    .filter(|status| status.state == SlotState::Booting)
                    .count();
                let empty = statuses
                    .iter()
                    .filter(|status| status.state == SlotState::Empty)
                    .count();
                let failed = statuses
                    .iter()
                    .filter(|status| status.state == SlotState::Failed)
                    .count();

                info!(
                    reason,
                    size = statuses.len(),
                    occupied,
                    booting,
                    empty,
                    failed,
                );
                debug!(
                    slots = %format_slot_summary(&statuses),
                );
            }
            Err(error) => {
                warn!(reason, %error, "failed to collect pool occupancy snapshot");
            }
        }
    }
}

fn format_slot_summary(statuses: &[SlotStatus]) -> String {
    statuses
        .iter()
        .map(|status| {
            let name = status.name.as_deref().unwrap_or("-");
            format!("{}:{:?}({})", status.slot, status.state, name)
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn runtime_from_slot_status(status: &SlotStatus) -> Option<VmRuntime> {
    if status.state != SlotState::Occupied {
        return None;
    }

    Some(VmRuntime {
        name: status.name.clone()?,
        mac: status.mac.clone()?,
        tap: status.tap.clone()?,
        pid: status.pid?,
        state: VmState::Running,
        started_at: status.started_at.clone()?,
    })
}
