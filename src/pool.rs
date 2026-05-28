use std::ffi::OsString;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use axum::http::Method;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, body::Incoming, client::conn::http1, header};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use tokio::{
    net::UnixStream,
    process::{Child, Command},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};

pub struct ProcessPool {
    shutdown_tx: watch::Sender<bool>,
    slots: Vec<SlotHandle>,
    vm_path: PathBuf,
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
    Booting,
    Running,
    Failed,
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
    VmNotFound(String),
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
            Self::VmNotFound(name) => write!(f, "VM '{name}' not found"),
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

struct SlotHandle {
    tx: mpsc::Sender<SlotCommand>,
    worker: JoinHandle<()>,
}

struct SlotRuntime {
    status: SlotStatus,
    child: Option<Child>,
}

enum SlotCommand {
    ReserveVm {
        name: String,
        mac: String,
        tap: String,
        response: oneshot::Sender<Result<SlotStatus, PoolError>>,
    },
    MarkVmBooted {
        started_at: String,
        response: oneshot::Sender<Result<SlotStatus, PoolError>>,
    },
    ReleaseVm {
        response: oneshot::Sender<Result<SlotStatus, PoolError>>,
    },
    MarkVmFailed {
        reason: String,
        response: oneshot::Sender<Result<SlotStatus, PoolError>>,
    },
    ResetVm {
        response: oneshot::Sender<Result<SlotStatus, PoolError>>,
    },
    GetVmStatus {
        response: oneshot::Sender<SlotStatus>,
    },
}

fn initialize_slot(
    slot: usize,
    program: &str,
    args: &[OsString],
    vm_path: &Path,
    shutdown_rx: watch::Receiver<bool>,
) -> SlotHandle {
    let (tx, rx) = mpsc::channel(16);

    let program = program.to_owned();
    let mut args = args.to_vec();
    let api_socket = vm_path.join(format!("vmm{slot}.sock"));
    args.push("--api-socket".into());
    args.push(api_socket.clone().into_os_string());

    let worker = tokio::spawn(async move {
        supervise_slot(slot, program, args, api_socket, shutdown_rx, rx).await;
    });

    SlotHandle { tx, worker }
}

impl ProcessPool {
    /// Creates a new process pool and starts one worker per slot.
    ///
    /// Each worker is configured with its own API socket under `vm_path` and
    /// receives the supplied program arguments plus the slot-specific socket
    /// path.
    pub async fn spawn(
        pool_size: usize,
        program: &str,
        args: &[OsString],
        vm_path: &Path,
    ) -> std::io::Result<Self> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut slots = Vec::with_capacity(pool_size);

        for slot in 0..pool_size {
            let shutdown_rx = shutdown_rx.clone();
            slots.push(initialize_slot(slot, program, args, vm_path, shutdown_rx));
        }

        Ok(Self {
            shutdown_tx,
            slots,
            vm_path: vm_path.to_path_buf(),
        })
    }

    #[allow(dead_code)]
    pub async fn extend(
        &mut self,
        additional_size: usize,
        program: &str,
        args: &[OsString],
        vm_path: &Path,
    ) {
        let current_size = self.slots.len();
        let new_size = current_size + additional_size;

        for slot in current_size..new_size {
            let shutdown_rx = self.shutdown_tx.subscribe();
            self.slots
                .push(initialize_slot(slot, program, args, vm_path, shutdown_rx));
        }
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

    pub async fn pool_idle(&self) -> Result<usize, PoolError> {
        Ok(self
            .list_vm_slots()
            .await?
            .into_iter()
            .filter(|status| status.state == SlotState::Empty)
            .count())
    }

    pub async fn pool_in_use(&self) -> Result<usize, PoolError> {
        Ok(self
            .list_vm_slots()
            .await?
            .into_iter()
            .filter(|status| status.state != SlotState::Empty)
            .count())
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

        rx.await.map_err(|_| PoolError::ChannelClosed)?
    }

    pub async fn allocate_vm_slot(
        &self,
        name: String,
        mac: String,
    ) -> Result<SlotStatus, PoolError> {
        if self.find_vm_slot_status_by_name(&name).await?.is_some() {
            return Err(PoolError::VmAlreadyRunning(name));
        }

        let slot = self
            .list_vm_slots()
            .await?
            .into_iter()
            .find(|status| status.state == SlotState::Empty)
            .map(|status| status.slot)
            .ok_or(PoolError::NoAvailableSlot)?;

        let tap = format!("tap{slot}");
        self.reserve_vm_slot(slot, name, mac, tap).await
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

        rx.await.map_err(|_| PoolError::ChannelClosed)?
    }

    pub async fn release_vm_slot(&self, slot: usize) -> Result<SlotStatus, PoolError> {
        let handle = self.slots.get(slot).ok_or(PoolError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::ReleaseVm { response: tx })
            .await
            .map_err(|_| PoolError::ChannelClosed)?;

        rx.await.map_err(|_| PoolError::ChannelClosed)?
    }

    pub async fn mark_vm_slot_failed(
        &self,
        slot: usize,
        reason: String,
    ) -> Result<SlotStatus, PoolError> {
        let handle = self.slots.get(slot).ok_or(PoolError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::MarkVmFailed {
                reason,
                response: tx,
            })
            .await
            .map_err(|_| PoolError::ChannelClosed)?;

        rx.await.map_err(|_| PoolError::ChannelClosed)?
    }

    pub async fn reset_vm_slot(&self, slot: usize) -> Result<SlotStatus, PoolError> {
        let handle = self.slots.get(slot).ok_or(PoolError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::ResetVm { response: tx })
            .await
            .map_err(|_| PoolError::ChannelClosed)?;

        rx.await.map_err(|_| PoolError::ChannelClosed)?
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
            eprintln!("failed to broadcast shutdown to workers");
        }

        while let Some(slot) = self.slots.pop() {
            if let Err(error) = slot.worker.await {
                eprintln!("worker task failed during shutdown: {error}");
            }
        }
    }
}

async fn supervise_slot(
    slot: usize,
    program: String,
    args: Vec<OsString>,
    api_socket: PathBuf,
    mut shutdown_rx: watch::Receiver<bool>,
    mut commands: mpsc::Receiver<SlotCommand>,
) {
    let mut runtime = SlotRuntime {
        status: SlotStatus {
            slot,
            generation: 0,
            state: SlotState::Empty,
            name: None,
            mac: None,
            tap: None,
            pid: None,
            started_at: None,
            last_error: None,
        },
        child: None,
    };

    loop {
        if *shutdown_rx.borrow() {
            if let Some(child) = runtime.child.as_mut() {
                stop_child(child).await;
            }
            cleanup_api_socket(&api_socket);
            break;
        }

        if runtime.child.is_none() && should_spawn(runtime.status.state) {
            cleanup_api_socket(&api_socket);

            match Command::new(&program)
                .args(&args)
                .env("VMM_SLOT", slot.to_string())
                .env(
                    "VMM_PATH",
                    api_socket
                        .parent()
                        .unwrap_or(Path::new("/"))
                        .display()
                        .to_string(),
                )
                .spawn()
            {
                Ok(child) => {
                    runtime.status.pid = child.id();
                    runtime.status.last_error = None;
                    println!(
                        "spawned vmm {} with pid {:?}",
                        slot,
                        runtime.status.pid.unwrap_or(0)
                    );
                    runtime.child = Some(child);
                }
                Err(error) => {
                    runtime.status.state = SlotState::Failed;
                    runtime.status.last_error = Some(error.to_string());
                    runtime.status.pid = None;
                    eprintln!("vmm {} failed to spawn child: {error}", slot);
                }
            }
        }

        while let Ok(command) = commands.try_recv() {
            handle_command(command, &mut runtime, &api_socket).await;
        }

        if let Some(child) = runtime.child.as_mut() {
            match child.try_wait() {
                Ok(Some(status)) => {
                    runtime.child = None;
                    runtime.status.pid = None;
                    cleanup_api_socket(&api_socket);
                    println!("vmm {} exited with status {}", slot, status);

                    match runtime.status.state {
                        SlotState::Empty => {}
                        SlotState::Booting | SlotState::Occupied => {
                            // Treat guest shutdown/exit as a terminal VM lifecycle event and
                            // return the slot to the reusable pool.
                            runtime.status.state = SlotState::Empty;
                            runtime.status.name = None;
                            runtime.status.mac = None;
                            runtime.status.tap = None;
                            runtime.status.started_at = None;
                            runtime.status.last_error = None;
                            runtime.status.generation += 1;
                        }
                        SlotState::Failed => {}
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    runtime.child = None;
                    runtime.status.pid = None;
                    runtime.status.state = SlotState::Failed;
                    runtime.status.last_error = Some(error.to_string());
                    cleanup_api_socket(&api_socket);
                    eprintln!("vmm {} failed while polling exit: {error}", slot);
                }
            }
        }

        tokio::select! {
            result = shutdown_rx.changed() => {
                if result.is_err() || *shutdown_rx.borrow() {
                    if let Some(child) = runtime.child.as_mut() {
                        stop_child(child).await;
                    }
                    cleanup_api_socket(&api_socket);
                    break;
                }
            }
            maybe_command = commands.recv() => {
                if let Some(command) = maybe_command {
                    handle_command(command, &mut runtime, &api_socket).await;
                } else {
                    if let Some(child) = runtime.child.as_mut() {
                        stop_child(child).await;
                    }
                    cleanup_api_socket(&api_socket);
                    break;
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {}
        }
    }
}

fn should_spawn(state: SlotState) -> bool {
    matches!(
        state,
        SlotState::Empty | SlotState::Booting | SlotState::Occupied
    )
}

async fn handle_command(command: SlotCommand, runtime: &mut SlotRuntime, api_socket: &Path) {
    match command {
        SlotCommand::GetVmStatus { response } => {
            let _ = response.send(runtime.status.clone());
        }
        SlotCommand::ReserveVm {
            name,
            mac,
            tap,
            response,
        } => {
            let result = if runtime.status.state == SlotState::Empty {
                runtime.status.state = SlotState::Booting;
                runtime.status.name = Some(name);
                runtime.status.mac = Some(mac);
                runtime.status.tap = Some(tap);
                runtime.status.generation += 1;
                runtime.status.last_error = None;
                runtime.status.started_at = None;
                Ok(runtime.status.clone())
            } else {
                Err(PoolError::InvalidTransition {
                    slot: runtime.status.slot,
                    from: runtime.status.state,
                    action: "claim",
                })
            };
            let _ = response.send(result);
        }
        SlotCommand::MarkVmBooted {
            started_at,
            response,
        } => {
            let result = if runtime.status.state == SlotState::Booting {
                runtime.status.state = SlotState::Occupied;
                runtime.status.started_at = Some(started_at);
                Ok(runtime.status.clone())
            } else {
                Err(PoolError::InvalidTransition {
                    slot: runtime.status.slot,
                    from: runtime.status.state,
                    action: "mark_booted",
                })
            };
            let _ = response.send(result);
        }
        SlotCommand::ReleaseVm { response } => {
            let result = if runtime.status.state == SlotState::Occupied {
                runtime.status.state = SlotState::Empty;
                runtime.status.name = None;
                runtime.status.mac = None;
                runtime.status.tap = None;
                runtime.status.generation += 1;
                runtime.status.last_error = None;
                runtime.status.started_at = None;
                runtime.status.pid = None;

                if let Some(child) = runtime.child.as_mut() {
                    stop_child(child).await;
                    runtime.child = None;
                }
                cleanup_api_socket(api_socket);

                Ok(runtime.status.clone())
            } else {
                Err(PoolError::InvalidTransition {
                    slot: runtime.status.slot,
                    from: runtime.status.state,
                    action: "release",
                })
            };
            let _ = response.send(result);
        }
        SlotCommand::MarkVmFailed { reason, response } => {
            runtime.status.state = SlotState::Failed;
            runtime.status.last_error = Some(reason);
            runtime.status.pid = None;
            runtime.status.started_at = None;

            if let Some(child) = runtime.child.as_mut() {
                stop_child(child).await;
                runtime.child = None;
            }
            cleanup_api_socket(api_socket);

            let _ = response.send(Ok(runtime.status.clone()));
        }
        SlotCommand::ResetVm { response } => {
            if let Some(child) = runtime.child.as_mut() {
                stop_child(child).await;
                runtime.child = None;
            }
            cleanup_api_socket(api_socket);

            runtime.status.state = SlotState::Empty;
            runtime.status.name = None;
            runtime.status.mac = None;
            runtime.status.tap = None;
            runtime.status.pid = None;
            runtime.status.last_error = None;
            runtime.status.started_at = None;
            runtime.status.generation += 1;

            let _ = response.send(Ok(runtime.status.clone()));
        }
    }
}

async fn stop_child(child: &mut Child) {
    if let Some(pid) = child.id() {
        println!("stopping child with pid {}", pid);
    }

    if let Err(error) = child.start_kill() {
        eprintln!("failed to signal child for shutdown: {error}");
        return;
    }

    if let Err(error) = child.wait().await {
        eprintln!("failed while waiting for child shutdown: {error}");
    }
}

fn cleanup_api_socket(api_socket: &Path) {
    match std::fs::remove_file(api_socket) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => eprintln!(
            "failed to remove api socket {}: {error}",
            api_socket.display()
        ),
    }
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

async fn send_unix_http_request(
    socket_path: &Path,
    method: Method,
    path: &str,
    body: Vec<u8>,
    content_type: Option<&str>,
) -> std::io::Result<ProxyResponse> {
    let stream = UnixStream::connect(socket_path).await?;
    let io = TokioIo::new(stream);

    let (mut sender, connection) = http1::handshake(io).await.map_err(io_other)?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("hyper client connection error: {error}");
        }
    });

    let uri = if path.is_empty() { "/" } else { path };
    let mut request_builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::HOST, "localhost")
        .header(header::CONNECTION, "close");

    if let Some(value) = content_type {
        request_builder = request_builder.header(header::CONTENT_TYPE, value);
    }

    let request = request_builder
        .body(Full::new(Bytes::from(body)))
        .map_err(io_invalid_input)?;

    let response = sender.send_request(request).await.map_err(io_other)?;
    response_to_proxy_response(response).await
}

async fn response_to_proxy_response(
    response: hyper::Response<Incoming>,
) -> io::Result<ProxyResponse> {
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string());

    let body = response
        .into_body()
        .collect()
        .await
        .map_err(io_other)?
        .to_bytes()
        .to_vec();

    Ok(ProxyResponse {
        status,
        content_type,
        body,
    })
}

fn io_other(error: impl fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

fn io_invalid_input(error: impl fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}
