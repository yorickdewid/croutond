use std::ffi::OsString;
use std::io;

use clap::Parser;

mod pool;
use pool::ProcessPool;

const ENV_PROGRAM: &str = "CLOUD_HYPERVISOR";
const ENV_ARGS: &str = "CLOUD_HYPERVISOR_ARGS";

#[derive(Debug, Parser)]
#[command(name = "croutond", about = "Virtual machine orchestration daemon")]
struct Cli {
    #[arg(long, help = "Run a VM slot smoke test and exit")]
    smoke_slot: bool,

    #[arg(
        short,
        long,
        value_parser = parse_pool_size,
        default_value_t = 4,
        help = "Number of VM slots"
    )]
    pool_size: usize,
}

fn resolve_program_and_args() -> Result<(String, Vec<OsString>), String> {
    let program = std::env::var(ENV_PROGRAM).map_err(|_| {
        format!("missing {ENV_PROGRAM}: set it to the cloud-hypervisor executable path")
    })?;

    let args = parse_env_args()?;

    Ok((program, args))
}

fn parse_env_args() -> Result<Vec<OsString>, String> {
    match std::env::var(ENV_ARGS) {
        Ok(raw_args) => {
            let parsed = shlex::split(&raw_args).ok_or_else(|| {
                format!("invalid {ENV_ARGS}: could not parse shell-style arguments")
            })?;
            Ok(parsed.into_iter().map(OsString::from).collect())
        }
        Err(std::env::VarError::NotPresent) => Ok(Vec::new()),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(format!("invalid {ENV_ARGS}: value is not valid Unicode"))
        }
    }
}

fn parse_pool_size(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("invalid pool size: {value}"))?;

    if parsed == 0 {
        return Err("pool size must be at least 1".to_string());
    }

    Ok(parsed)
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;

    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();
    let (program, args) = resolve_program_and_args().map_err(io::Error::other)?;

    let mut pool = ProcessPool::spawn(cli.pool_size, &program, &args).await?;

    if cli.smoke_slot {
        run_slot_smoke(&pool).await?;
        pool.shutdown().await;
        return Ok(());
    }

    println!(
        "process pool is running with {} child processes; press Ctrl+C to stop",
        pool.size()
    );

    wait_for_shutdown_signal().await?;
    println!("shutdown signal received");

    pool.shutdown().await;
    Ok(())
}

async fn run_slot_smoke(pool: &ProcessPool) -> io::Result<()> {
    let initial = pool.get_vm_slot_status(0).await.map_err(io::Error::other)?;
    println!("smoke: initial slot0 state={:?}", initial.state);

    let booting = pool
        .reserve_vm_slot(0, "smoke-owner".to_string())
        .await
        .map_err(io::Error::other)?;
    println!("smoke: after reserve slot0 state={:?}", booting.state);

    let occupied = pool
        .mark_vm_slot_booted(0)
        .await
        .map_err(io::Error::other)?;
    println!("smoke: after mark_booted slot0 state={:?}", occupied.state);

    let empty = pool.release_vm_slot(0).await.map_err(io::Error::other)?;
    println!("smoke: after release slot0 state={:?}", empty.state);

    Ok(())
}
