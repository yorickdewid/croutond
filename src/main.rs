use std::ffi::OsString;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::Mutex;

mod api;
mod pool;
use pool::ProcessPool;

const ENV_ARGS: &str = "CLOUD_HYPERVISOR_ARGS";

#[derive(Debug, Parser)]
#[command(
    name = "croutond",
    about = "Crouton virtual machine orchestrator daemon"
)]
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

    #[arg(long, default_value = "/tmp", help = "Path to the VM data directory")]
    runtime_dir: PathBuf,

    #[arg(long, help = "Cloud Hypervisor binary path")]
    ch_bin: String,

    #[arg(long, default_value = "127.0.0.1:7777", help = "REST listen address")]
    listen_addr: SocketAddr,
}

fn resolve_program_and_args(program: String) -> Result<(String, Vec<OsString>), String> {
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
    let (program, args) = resolve_program_and_args(cli.ch_bin).map_err(io::Error::other)?;
    std::fs::create_dir_all(&cli.runtime_dir)?;

    let mut pool = ProcessPool::spawn(cli.pool_size, &program, &args, &cli.runtime_dir).await?;

    if cli.smoke_slot {
        run_slot_smoke(&pool).await?;
        pool.shutdown().await;
        return Ok(());
    }

    let shared_pool = Arc::new(Mutex::new(pool));
    println!(
        "orchestrator pool is running with {} vmm slots",
        shared_pool.lock().await.size(),
    );

    let app = api::router(shared_pool.clone());
    let listener = tokio::net::TcpListener::bind(cli.listen_addr).await?;

    println!("listening on {}", cli.listen_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            if let Err(error) = wait_for_shutdown_signal().await {
                eprintln!("shutdown signal listener failed: {error}");
            }
        })
        .await
        .map_err(io::Error::other)?;

    println!("shutdown signal received");
    shared_pool.lock().await.shutdown().await;

    Ok(())
}

async fn run_slot_smoke(pool: &ProcessPool) -> io::Result<()> {
    let initial = pool.get_vm_slot_status(0).await.map_err(io::Error::other)?;
    println!("smoke: initial slot0 state={:?}", initial.state);

    let booting = pool
        .reserve_vm_slot(
            0,
            "smoke-owner".to_string(),
            "02:00:00:00:00:00".to_string(),
            "tap0".to_string(),
        )
        .await
        .map_err(io::Error::other)?;
    println!("smoke: after reserve slot0 state={:?}", booting.state);

    let occupied = pool
        .mark_vm_slot_booted(0, chrono::Utc::now().to_rfc3339())
        .await
        .map_err(io::Error::other)?;
    println!("smoke: after mark_booted slot0 state={:?}", occupied.state);

    let empty = pool.release_vm_slot(0).await.map_err(io::Error::other)?;
    println!("smoke: after release slot0 state={:?}", empty.state);

    Ok(())
}
