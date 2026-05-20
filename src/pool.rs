use std::ffi::OsString;

use tokio::{
    io,
    process::{Child, Command},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};

pub struct ProcessPool {
    shutdown_tx: watch::Sender<bool>,
    slots: Vec<SlotHandle>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Empty,
    Booting,
    Occupied,
    Failed,
}

#[derive(Debug, Clone)]
pub struct SlotStatus {
    pub slot: usize,
    pub generation: u64,
    pub state: SlotState,
    pub owner: Option<String>,
    pub pid: Option<u32>,
    pub last_error: Option<String>,
}

#[derive(Debug)]
pub enum SlotError {
    InvalidTransition {
        slot: usize,
        from: SlotState,
        action: &'static str,
    },
    SlotNotFound(usize),
    ChannelClosed,
}

impl std::fmt::Display for SlotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTransition { slot, from, action } => {
                write!(f, "slot {slot} cannot perform {action} from state {from:?}")
            }
            Self::SlotNotFound(slot) => write!(f, "slot {slot} does not exist"),
            Self::ChannelClosed => write!(f, "slot worker channel is closed"),
        }
    }
}

impl std::error::Error for SlotError {}

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
        owner: String,
        response: oneshot::Sender<Result<SlotStatus, SlotError>>,
    },
    MarkVmBooted {
        response: oneshot::Sender<Result<SlotStatus, SlotError>>,
    },
    ReleaseVm {
        response: oneshot::Sender<Result<SlotStatus, SlotError>>,
    },
    MarkVmFailed {
        reason: String,
        response: oneshot::Sender<Result<SlotStatus, SlotError>>,
    },
    ResetVm {
        response: oneshot::Sender<Result<SlotStatus, SlotError>>,
    },
    GetVmStatus {
        response: oneshot::Sender<SlotStatus>,
    },
}

fn initialize_slot(
    slot: usize,
    program: &str,
    args: &[OsString],
    shutdown_rx: watch::Receiver<bool>,
) -> SlotHandle {
    let program = program.to_owned();
    let args = args.to_vec();
    let (tx, rx) = mpsc::channel(16);

    let worker = tokio::spawn(async move {
        supervise_slot(slot, program, args, shutdown_rx, rx).await;
    });

    SlotHandle { tx, worker }
}

impl ProcessPool {
    pub async fn spawn(pool_size: usize, program: &str, args: &[OsString]) -> io::Result<Self> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut slots = Vec::with_capacity(pool_size);

        for slot in 0..pool_size {
            let shutdown_rx = shutdown_rx.clone();
            slots.push(initialize_slot(slot, program, args, shutdown_rx));
        }

        Ok(Self { shutdown_tx, slots })
    }

    #[allow(dead_code)]
    pub async fn extend(&mut self, additional_size: usize, program: &str, args: &[OsString]) {
        let current_size = self.slots.len();
        let new_size = current_size + additional_size;

        for slot in current_size..new_size {
            let shutdown_rx = self.shutdown_tx.subscribe();
            self.slots
                .push(initialize_slot(slot, program, args, shutdown_rx));
        }
    }

    pub async fn list_vm_slots(&self) -> Result<Vec<SlotStatus>, SlotError> {
        let mut statuses = Vec::with_capacity(self.slots.len());

        for slot in 0..self.slots.len() {
            statuses.push(self.get_vm_slot_status(slot).await?);
        }

        Ok(statuses)
    }

    pub async fn get_vm_slot_status(&self, slot: usize) -> Result<SlotStatus, SlotError> {
        let handle = self.slots.get(slot).ok_or(SlotError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::GetVmStatus { response: tx })
            .await
            .map_err(|_| SlotError::ChannelClosed)?;

        rx.await.map_err(|_| SlotError::ChannelClosed)
    }

    pub async fn reserve_vm_slot(
        &self,
        slot: usize,
        owner: String,
    ) -> Result<SlotStatus, SlotError> {
        let handle = self.slots.get(slot).ok_or(SlotError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::ReserveVm {
                owner,
                response: tx,
            })
            .await
            .map_err(|_| SlotError::ChannelClosed)?;

        rx.await.map_err(|_| SlotError::ChannelClosed)?
    }

    pub async fn mark_vm_slot_booted(&self, slot: usize) -> Result<SlotStatus, SlotError> {
        let handle = self.slots.get(slot).ok_or(SlotError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::MarkVmBooted { response: tx })
            .await
            .map_err(|_| SlotError::ChannelClosed)?;

        rx.await.map_err(|_| SlotError::ChannelClosed)?
    }

    pub async fn release_vm_slot(&self, slot: usize) -> Result<SlotStatus, SlotError> {
        let handle = self.slots.get(slot).ok_or(SlotError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::ReleaseVm { response: tx })
            .await
            .map_err(|_| SlotError::ChannelClosed)?;

        rx.await.map_err(|_| SlotError::ChannelClosed)?
    }

    pub async fn mark_vm_slot_failed(
        &self,
        slot: usize,
        reason: String,
    ) -> Result<SlotStatus, SlotError> {
        let handle = self.slots.get(slot).ok_or(SlotError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::MarkVmFailed {
                reason,
                response: tx,
            })
            .await
            .map_err(|_| SlotError::ChannelClosed)?;

        rx.await.map_err(|_| SlotError::ChannelClosed)?
    }

    pub async fn reset_vm_slot(&self, slot: usize) -> Result<SlotStatus, SlotError> {
        let handle = self.slots.get(slot).ok_or(SlotError::SlotNotFound(slot))?;
        let (tx, rx) = oneshot::channel();

        handle
            .tx
            .send(SlotCommand::ResetVm { response: tx })
            .await
            .map_err(|_| SlotError::ChannelClosed)?;

        rx.await.map_err(|_| SlotError::ChannelClosed)?
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

    pub fn size(&self) -> usize {
        self.slots.len()
    }
}

async fn supervise_slot(
    slot: usize,
    program: String,
    args: Vec<OsString>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut commands: mpsc::Receiver<SlotCommand>,
) {
    let mut runtime = SlotRuntime {
        status: SlotStatus {
            slot,
            generation: 0,
            state: SlotState::Empty,
            owner: None,
            pid: None,
            last_error: None,
        },
        child: None,
    };

    loop {
        if *shutdown_rx.borrow() {
            if let Some(child) = runtime.child.as_mut() {
                stop_child(child).await;
            }
            break;
        }

        if runtime.child.is_none() && should_spawn(runtime.status.state) {
            match Command::new(&program).args(&args).spawn() {
                Ok(child) => {
                    runtime.status.pid = child.id();
                    runtime.status.last_error = None;
                    println!("spawned child {} with pid {:?}", slot, runtime.status.pid);
                    runtime.child = Some(child);
                }
                Err(error) => {
                    runtime.status.state = SlotState::Failed;
                    runtime.status.last_error = Some(error.to_string());
                    runtime.status.pid = None;
                    eprintln!("worker {} failed to spawn child: {error}", slot);
                }
            }
        }

        while let Ok(command) = commands.try_recv() {
            handle_command(command, &mut runtime).await;
        }

        if let Some(child) = runtime.child.as_mut() {
            match child.try_wait() {
                Ok(Some(status)) => {
                    runtime.child = None;
                    runtime.status.pid = None;
                    println!("child {} exited with status {}", slot, status);

                    match runtime.status.state {
                        SlotState::Empty => {}
                        SlotState::Booting | SlotState::Occupied => {
                            runtime.status.state = SlotState::Failed;
                            runtime.status.last_error =
                                Some("vm process exited unexpectedly".to_string());
                            runtime.status.owner = None;
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
                    runtime.status.owner = None;
                    eprintln!("child {} failed while polling exit: {error}", slot);
                }
            }
        }

        tokio::select! {
            result = shutdown_rx.changed() => {
                if result.is_err() || *shutdown_rx.borrow() {
                    if let Some(child) = runtime.child.as_mut() {
                        stop_child(child).await;
                    }
                    break;
                }
            }
            maybe_command = commands.recv() => {
                if let Some(command) = maybe_command {
                    handle_command(command, &mut runtime).await;
                } else {
                    if let Some(child) = runtime.child.as_mut() {
                        stop_child(child).await;
                    }
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

async fn handle_command(command: SlotCommand, runtime: &mut SlotRuntime) {
    match command {
        SlotCommand::GetVmStatus { response } => {
            let _ = response.send(runtime.status.clone());
        }
        SlotCommand::ReserveVm { owner, response } => {
            let result = if runtime.status.state == SlotState::Empty {
                runtime.status.state = SlotState::Booting;
                runtime.status.owner = Some(owner);
                runtime.status.generation += 1;
                runtime.status.last_error = None;
                Ok(runtime.status.clone())
            } else {
                Err(SlotError::InvalidTransition {
                    slot: runtime.status.slot,
                    from: runtime.status.state,
                    action: "claim",
                })
            };
            let _ = response.send(result);
        }
        SlotCommand::MarkVmBooted { response } => {
            let result = if runtime.status.state == SlotState::Booting {
                runtime.status.state = SlotState::Occupied;
                Ok(runtime.status.clone())
            } else {
                Err(SlotError::InvalidTransition {
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
                runtime.status.owner = None;
                runtime.status.generation += 1;
                runtime.status.last_error = None;
                Ok(runtime.status.clone())
            } else {
                Err(SlotError::InvalidTransition {
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
            runtime.status.owner = None;
            runtime.status.pid = None;

            if let Some(child) = runtime.child.as_mut() {
                stop_child(child).await;
                runtime.child = None;
            }

            let _ = response.send(Ok(runtime.status.clone()));
        }
        SlotCommand::ResetVm { response } => {
            if let Some(child) = runtime.child.as_mut() {
                stop_child(child).await;
                runtime.child = None;
            }

            runtime.status.state = SlotState::Empty;
            runtime.status.owner = None;
            runtime.status.pid = None;
            runtime.status.last_error = None;
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
