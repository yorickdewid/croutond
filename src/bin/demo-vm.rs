use std::ffi::OsString;
use std::io;
use std::path::PathBuf;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

#[tokio::main]
async fn main() -> io::Result<()> {
    let api_socket = parse_api_socket(std::env::args_os())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;

    if let Some(parent) = api_socket.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let listener = UnixListener::bind(&api_socket)?;
    let slot = std::env::var("VMM_SLOT").unwrap_or_else(|_| "unknown".to_string());

    println!(
        "demo-vm started: slot={}, api_socket={}",
        slot,
        api_socket.display()
    );

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _)) => {
                        let slot = slot.clone();
                        tokio::spawn(async move {
                            if let Err(error) = handle_connection(stream, &slot).await {
                                eprintln!("demo-vm connection error: {error}");
                            }
                        });
                    }
                    Err(error) => eprintln!("demo-vm accept error: {error}"),
                }
            }
            signal_result = wait_for_shutdown_signal() => {
                if let Err(error) = signal_result {
                    eprintln!("demo-vm shutdown signal error: {error}");
                }
                break;
            }
        }
    }

    println!("demo-vm stopped: api_socket={}", api_socket.display());
    Ok(())
}

fn parse_api_socket(args: impl IntoIterator<Item = OsString>) -> Result<PathBuf, String> {
    let mut iter = args.into_iter();
    let _program = iter.next();

    while let Some(arg) = iter.next() {
        if arg == "--api-socket" {
            let Some(path) = iter.next() else {
                return Err("missing value for --api-socket".to_string());
            };
            return Ok(PathBuf::from(path));
        }
    }

    Err("missing required argument --api-socket <PATH>".to_string())
}

async fn handle_connection(mut stream: UnixStream, slot: &str) -> io::Result<()> {
    let mut buffer = [0_u8; 4096];
    let _ = stream.read(&mut buffer).await?;

    let body = format!(r#"{{"status":"ok","vm":"demo-vm","slot":"{slot}"}}"#);
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
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
