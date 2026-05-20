use std::ffi::OsString;
use std::io;

use clap::Parser;

mod pool;
use pool::ProcessPool;

#[derive(Debug, Parser)]
#[command(name = "croutond", about = "Supervised pool of external programs.")]
struct Cli {
    #[arg(long, help = "Run a VM slot smoke test and exit")]
    smoke_slot: bool,

    #[arg(
        short,
        long,
        value_parser = parse_pool_size,
        default_value_t = 4,
        help = "Number of VM slots [default: 4]"
    )]
    pool_size: usize,

    #[arg(help = "Program to launch in each slot")]
    program: String,

    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        help = "Arguments for the program"
    )]
    args: Vec<OsString>,
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

    let mut pool = ProcessPool::spawn(cli.pool_size, &cli.program, &cli.args).await?;

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
