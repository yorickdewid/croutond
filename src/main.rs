use std::ffi::OsString;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

mod api;
mod ch_client;
mod error;
mod net_tap;
mod pool;
mod pool_facade;
mod service;
mod slot_supervisor;
mod vm_payload;
mod vm_validation;
use pool::ProcessPool;

const ENV_ARGS: &str = "CLOUD_HYPERVISOR_ARGS";
const VERSION: &str = env!("CARGO_PKG_VERSION");

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
        help = "Minimum number of warm VM slots"
    )]
    pool_size: usize,

    #[arg(
        long,
        value_parser = parse_pool_size,
        help = "Maximum number of VM slots (defaults to --pool-size when omitted)"
    )]
    max_pool_size: Option<usize>,

    #[arg(
        long,
        value_parser = parse_cooldown_secs,
        default_value_t = 15,
        help = "Cooldown in seconds between pool scale-down operations"
    )]
    scale_down_cooldown_secs: u64,

    #[arg(
        long,
        value_parser = parse_interval_ms,
        default_value_t = 1_000,
        help = "Autoscale background tick interval in milliseconds"
    )]
    autoscale_interval_ms: u64,

    #[arg(long, default_value = "/tmp", help = "Path to the VM data directory")]
    runtime_dir: PathBuf,

    #[arg(long, help = "Cloud Hypervisor binary path")]
    ch_bin: String,

    #[arg(long, help = "Bridge interface used for VM TAP devices")]
    bridge: String,

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

fn parse_cooldown_secs(value: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("invalid cooldown seconds: {value}"))?;

    if parsed == 0 {
        return Err("scale-down cooldown must be at least 1 second".to_string());
    }

    Ok(parsed)
}

fn parse_interval_ms(value: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("invalid autoscale interval milliseconds: {value}"))?;

    if parsed == 0 {
        return Err("autoscale interval must be at least 1 millisecond".to_string());
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
    let max_pool_size = cli.max_pool_size.unwrap_or(cli.pool_size);
    if max_pool_size < cli.pool_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "--max-pool-size ({max_pool_size}) must be >= --pool-size ({})",
                cli.pool_size
            ),
        ));
    }

    let (program, args) = resolve_program_and_args(cli.ch_bin).map_err(io::Error::other)?;
    std::fs::create_dir_all(&cli.runtime_dir)?;

    let pool = ProcessPool::spawn(
        cli.pool_size,
        max_pool_size,
        Duration::from_secs(cli.scale_down_cooldown_secs),
        &program,
        &args,
        &cli.bridge,
        &cli.runtime_dir,
    )
    .await?;

    info!("==================== croutond startup ====================",);
    info!("version: {}", VERSION);
    info!(
        bridge = %cli.bridge,
        min_slots = cli.pool_size,
        max_slots = max_pool_size,
        autoscale_interval_ms = cli.autoscale_interval_ms,
        slots = pool.size(),
    );
    info!("==========================================================",);

    let shared_pool = Arc::new(RwLock::new(pool));
    let autoscale_pool = shared_pool.clone();
    let autoscale_interval = Duration::from_millis(cli.autoscale_interval_ms);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(autoscale_interval);
        loop {
            interval.tick().await;
            let mut pool = autoscale_pool.write().await;
            if let Err(error) = pool.autoscale_tick().await {
                warn!(%error, "autoscale tick failed");
            }
        }
    });

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
    shared_pool.write().await.shutdown().await;

    Ok(())
}
