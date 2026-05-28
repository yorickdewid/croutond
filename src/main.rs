use std::ffi::OsString;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::sync::Mutex;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

mod api;
mod pool;
mod service;
use pool::ProcessPool;

const ENV_ARGS: &str = "CLOUD_HYPERVISOR_ARGS";

#[derive(Debug, Parser)]
#[command(
    name = "croutond",
    about = "Crouton virtual machine orchestrator daemon"
)]
struct Cli {
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

    #[arg(long, default_value = "[::]:7777", help = "REST listen address")]
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

fn init_logging() {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("croutond=info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .init();
}

async fn bind_listener(listen_addr: SocketAddr) -> io::Result<tokio::net::TcpListener> {
    match listen_addr {
        SocketAddr::V4(_) => Ok(tokio::net::TcpListener::bind(listen_addr).await?),
        SocketAddr::V6(ipv6_addr) if ipv6_addr.ip().is_unspecified() => {
            let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
            socket.set_reuse_address(true)?;
            socket.set_only_v6(false)?;
            socket.bind(&listen_addr.into())?;
            socket.listen(1024)?;

            let listener = std::net::TcpListener::from(socket);
            listener.set_nonblocking(true)?;
            tokio::net::TcpListener::from_std(listener)
        }
        SocketAddr::V6(_) => Ok(tokio::net::TcpListener::bind(listen_addr).await?),
    }
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
    init_logging();

    let cli = Cli::parse();
    let (program, args) = resolve_program_and_args(cli.ch_bin).map_err(io::Error::other)?;
    std::fs::create_dir_all(&cli.runtime_dir)?;

    let pool = ProcessPool::spawn(cli.pool_size, &program, &args, &cli.runtime_dir).await?;

    info!(slots = pool.size(), "orchestrator pool is running");

    let shared_pool = Arc::new(Mutex::new(pool));
    let app = api::router(shared_pool.clone());
    let listener = bind_listener(cli.listen_addr).await?;

    info!(address = %listener.local_addr()?, "listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            if let Err(error) = wait_for_shutdown_signal().await {
                error!(%error, "shutdown signal listener failed");
            }
        })
        .await
        .map_err(io::Error::other)?;

    info!("shutdown signal received");
    shared_pool.lock().await.shutdown().await;

    Ok(())
}
