use tokio_tun::TunBuilder;

use crate::pool::PoolError;

pub(crate) fn ensure_tap_device(name: &str, bridge: Option<&str>) -> Result<(), PoolError> {
    match TunBuilder::new().name(name).tap().persist().up().build() {
        Ok(_) => {
            if let Some(bridge) = bridge {
                attach_tap_to_bridge(name, bridge)
            } else {
                Ok(())
            }
        }
        Err(error) if is_tap_exists_error(&error) => {
            if let Some(bridge) = bridge {
                attach_tap_to_bridge(name, bridge)
            } else {
                Ok(())
            }
        }
        Err(error) => Err(PoolError::Backend(format!(
            "failed to create tap interface '{name}': {error}"
        ))),
    }
}

fn attach_tap_to_bridge(name: &str, bridge: &str) -> Result<(), PoolError> {
    let output = std::process::Command::new("ip")
        .args(["link", "set", "dev", name, "master", bridge])
        .output()
        .map_err(|error| {
            PoolError::Backend(format!(
                "failed to attach tap interface '{name}' to bridge '{bridge}': {error}"
            ))
        })?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let detail = if stderr.is_empty() {
        format!("ip link exited with status {}", output.status)
    } else {
        stderr
    };

    Err(PoolError::Backend(format!(
        "failed to attach tap interface '{name}' to bridge '{bridge}': {detail}"
    )))
}

fn is_tap_exists_error(error: &tokio_tun::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("file exists")
        || message.contains("already exists")
        || message.contains("device or resource busy")
}
