use std::path::Path;

use crate::pool::{ProxyResponse, SlotStatus};
use crate::service::BootConfig;

pub(crate) fn create_vm_request_body(config: &BootConfig, reserved: &SlotStatus) -> serde_json::Value {
    let boot_mode = config.boot_mode.to_ascii_lowercase();
    let payload = if boot_mode == "uefi" {
        serde_json::json!({
            "firmware": config.firmware_path,
        })
    } else {
        serde_json::json!({
            "kernel": config.kernel_path,
            "initramfs": config.initrd_path,
            "cmdline": config.cmdline,
        })
    };

    let disks: Vec<serde_json::Value> = config
        .disks
        .iter()
        .map(|path| match disk_image_type(path) {
            Some(image_type) => serde_json::json!({
                "path": path,
                "image_type": image_type,
            }),
            None => serde_json::json!({"path": path}),
        })
        .collect();

    serde_json::json!({
        "cpus": {
            "boot_vcpus": config.cpus,
            "max_vcpus": config.cpus,
        },
        "memory": {
            "size": config.memory_mb << 20,
        },
        "payload": payload,
        "disks": disks,
        "net": [{
            "tap": reserved.tap,
            "mac": reserved.mac,
        }],
        "rng": {
            "src": "/dev/urandom",
        }
    })
}

pub(crate) fn create_restore_request_body(config: &BootConfig) -> serde_json::Value {
    let source_url = config
        .snapshot_path
        .as_ref()
        .map(|path| format!("file://{}", path.display()));

    serde_json::json!({
        "source_url": source_url,
    })
}

pub(crate) fn format_backend_error(response: &ProxyResponse) -> String {
    if response.body.is_empty() {
        return format!("backend returned status {}", response.status);
    }

    let body = String::from_utf8_lossy(&response.body);
    let body = body.trim();
    if body.is_empty() {
        format!("backend returned status {}", response.status)
    } else {
        format!("backend returned status {}: {}", response.status, body)
    }
}

fn disk_image_type(path: &Path) -> Option<&'static str> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    match extension.as_str() {
        "qcow2" | "qcow" => Some("Qcow2"),
        "raw" => Some("Raw"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::Value;

    use super::{create_restore_request_body, create_vm_request_body, format_backend_error};
    use crate::pool::{ProxyResponse, SlotState, SlotStatus};
    use crate::service::BootConfig;

    fn base_linux_config() -> BootConfig {
        BootConfig {
            name: "vm-b".to_string(),
            cpus: 4,
            memory_mb: 1024,
            boot_mode: "linux".to_string(),
            disks: vec![
                PathBuf::from("/var/lib/vms/os.qcow2"),
                PathBuf::from("/var/lib/vms/data.raw"),
                PathBuf::from("/var/lib/vms/misc.img"),
            ],
            kernel_path: Some(PathBuf::from("/var/lib/vms/vmlinux")),
            initrd_path: Some(PathBuf::from("/var/lib/vms/initrd")),
            cmdline: Some("console=ttyS0".to_string()),
            firmware_path: None,
            snapshot_path: None,
        }
    }

    fn reserved_slot() -> SlotStatus {
        SlotStatus {
            slot: 1,
            generation: 10,
            state: SlotState::Booting,
            name: Some("vm-b".to_string()),
            mac: Some("02:aa:bb:cc:dd:ee".to_string()),
            tap: Some("tap1".to_string()),
            pid: Some(42),
            started_at: None,
            last_error: None,
        }
    }

    #[test]
    fn linux_payload_contains_expected_fields_and_disk_types() {
        let payload = create_vm_request_body(&base_linux_config(), &reserved_slot());

        assert_eq!(payload["cpus"]["boot_vcpus"], Value::from(4));
        assert_eq!(payload["memory"]["size"], Value::from(1024_u64 << 20));
        assert_eq!(payload["net"][0]["tap"], Value::from("tap1"));
        assert_eq!(payload["net"][0]["mac"], Value::from("02:aa:bb:cc:dd:ee"));
        assert_eq!(payload["payload"]["kernel"], Value::from("/var/lib/vms/vmlinux"));

        let disks = payload["disks"].as_array().expect("disks should be array");
        assert_eq!(disks[0]["image_type"], Value::from("Qcow2"));
        assert_eq!(disks[1]["image_type"], Value::from("Raw"));
        assert!(disks[2].get("image_type").is_none());
    }

    #[test]
    fn restore_payload_contains_file_url() {
        let mut config = base_linux_config();
        config.snapshot_path = Some(PathBuf::from("/var/lib/vms/snaps/vm-b.snap"));

        let payload = create_restore_request_body(&config);
        assert_eq!(
            payload["source_url"],
            Value::from("file:///var/lib/vms/snaps/vm-b.snap")
        );
    }

    #[test]
    fn backend_error_formats_status_and_trimmed_body() {
        let response = ProxyResponse {
            status: 500,
            content_type: Some("text/plain".to_string()),
            body: b"  failure details\n".to_vec(),
        };

        let message = format_backend_error(&response);
        assert_eq!(message, "backend returned status 500: failure details");
    }

    #[test]
    fn backend_error_formats_status_only_for_empty_body() {
        let response = ProxyResponse {
            status: 404,
            content_type: None,
            body: Vec::new(),
        };

        let message = format_backend_error(&response);
        assert_eq!(message, "backend returned status 404");
    }
}
