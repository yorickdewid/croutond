use std::ffi::OsString;
use std::path::{Path, PathBuf};

use tokio::{
    process::{Child, Command},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tracing::{error, info, warn};

use crate::net_tap::ensure_tap_device;
use crate::pool::{PoolError, SlotState, SlotStatus};

pub(crate) struct SlotHandle {
    pub(crate) tx: mpsc::Sender<SlotCommand>,
    pub(crate) worker: JoinHandle<()>,
}

struct SlotRuntime {
    status: SlotStatus,
    child: Option<Child>,
}

pub(crate) enum SlotCommand {
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
    ResetVm {
        response: oneshot::Sender<Result<SlotStatus, PoolError>>,
    },
    GetVmStatus {
        response: oneshot::Sender<SlotStatus>,
    },
}

pub(crate) fn initialize_slot(
    slot: usize,
    program: &str,
    args: &[OsString],
    bridge: Option<&str>,
    vm_path: &Path,
    shutdown_rx: watch::Receiver<bool>,
) -> SlotHandle {
    let (tx, rx) = mpsc::channel(16);

    let program = program.to_owned();
    let bridge = bridge.map(str::to_string);
    let mut args = args.to_vec();
    let api_socket = vm_path.join(format!("vmm{slot}.sock"));
    args.push("--api-socket".into());
    args.push(api_socket.clone().into_os_string());

    let worker = tokio::spawn(async move {
        supervise_slot(slot, program, args, bridge, api_socket, shutdown_rx, rx).await;
    });

    SlotHandle { tx, worker }
}

async fn supervise_slot(
    slot: usize,
    program: String,
    args: Vec<OsString>,
    bridge: Option<String>,
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
                    info!(slot, pid = runtime.status.pid.unwrap_or(0), "spawned vmm");
                    runtime.child = Some(child);
                }
                Err(error) => {
                    runtime.status.state = SlotState::Failed;
                    runtime.status.last_error = Some(error.to_string());
                    runtime.status.pid = None;
                    error!(slot, %error, "vmm failed to spawn child");
                }
            }
        }

        while let Ok(command) = commands.try_recv() {
            handle_command(command, &mut runtime, bridge.as_deref(), &api_socket).await;
        }

        if let Some(child) = runtime.child.as_mut() {
            match child.try_wait() {
                Ok(Some(status)) => {
                    runtime.child = None;
                    runtime.status.pid = None;
                    cleanup_api_socket(&api_socket);
                    info!(slot, %status, "vmm exited");

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
                    error!(slot, %error, "vmm failed while polling exit");
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
                    handle_command(command, &mut runtime, bridge.as_deref(), &api_socket).await;
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

async fn handle_command(
    command: SlotCommand,
    runtime: &mut SlotRuntime,
    bridge: Option<&str>,
    api_socket: &Path,
) {
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
                match ensure_tap_device(&tap, bridge) {
                    Ok(()) => {
                        runtime.status.state = SlotState::Booting;
                        runtime.status.name = Some(name);
                        runtime.status.mac = Some(mac);
                        runtime.status.tap = Some(tap);
                        runtime.status.generation += 1;
                        runtime.status.last_error = None;
                        runtime.status.started_at = None;
                        Ok(runtime.status.clone())
                    }
                    Err(error) => Err(error),
                }
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
        info!(pid, "stopping child");
    }

    if let Err(error) = child.start_kill() {
        warn!(%error, "failed to signal child for shutdown");
        return;
    }

    if let Err(error) = child.wait().await {
        warn!(%error, "failed while waiting for child shutdown");
    }
}

fn cleanup_api_socket(api_socket: &Path) {
    match std::fs::remove_file(api_socket) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => warn!(path = %api_socket.display(), %error, "failed to remove api socket"),
    }
}
