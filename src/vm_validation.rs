use crate::error::ApiError;
use crate::service::BootConfig;

pub(crate) fn validate_boot_config(config: &BootConfig) -> Result<(), ApiError> {
    if config.name.trim().is_empty() {
        return Err(ApiError::Validation {
            field: Some("name".to_string()),
            error: "name is required".to_string(),
        });
    }

    if config.cpus == 0 {
        return Err(ApiError::Validation {
            field: Some("cpus".to_string()),
            error: "cpus must be greater than zero".to_string(),
        });
    }

    if config.memory_mb == 0 {
        return Err(ApiError::Validation {
            field: Some("memoryMb".to_string()),
            error: "memoryMb must be greater than zero".to_string(),
        });
    }

    if config.boot_mode.trim().is_empty() {
        return Err(ApiError::Validation {
            field: Some("bootMode".to_string()),
            error: "bootMode is required".to_string(),
        });
    }

    let boot_mode = config.boot_mode.to_ascii_lowercase();
    if boot_mode != "linux" && boot_mode != "uefi" {
        return Err(ApiError::Validation {
            field: Some("bootMode".to_string()),
            error: "bootMode must be one of: linux, uefi".to_string(),
        });
    }

    if config.memory_mb > (u64::MAX >> 20) {
        return Err(ApiError::Validation {
            field: Some("memoryMb".to_string()),
            error: "memoryMb is too large".to_string(),
        });
    }

    if config.snapshot_path.is_none() {
        if boot_mode == "linux" && config.kernel_path.is_none() {
            return Err(ApiError::Validation {
                field: Some("kernelPath".to_string()),
                error: "kernelPath is required when bootMode=linux".to_string(),
            });
        }

        if boot_mode == "uefi" && config.firmware_path.is_none() {
            return Err(ApiError::Validation {
                field: Some("firmwarePath".to_string()),
                error: "firmwarePath is required when bootMode=uefi".to_string(),
            });
        }
    }

    for disk in &config.disks {
        if !disk.is_absolute() {
            return Err(ApiError::Validation {
                field: Some("disks".to_string()),
                error: format!("disk path '{}' must be absolute", disk.display()),
            });
        }
    }

    for (field, path) in [
        ("kernelPath", config.kernel_path.as_ref()),
        ("initrdPath", config.initrd_path.as_ref()),
        ("firmwarePath", config.firmware_path.as_ref()),
        ("snapshotPath", config.snapshot_path.as_ref()),
    ] {
        if let Some(path) = path
            && !path.is_absolute()
        {
            return Err(ApiError::Validation {
                field: Some(field.to_string()),
                error: format!("{field} must be absolute"),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::validate_boot_config;
    use crate::error::ApiError;
    use crate::service::BootConfig;

    fn base_linux_config() -> BootConfig {
        BootConfig {
            name: "vm-a".to_string(),
            cpus: 2,
            memory_mb: 512,
            boot_mode: "linux".to_string(),
            disks: vec![PathBuf::from("/var/lib/vms/disk.qcow2")],
            kernel_path: Some(PathBuf::from("/var/lib/vms/vmlinux")),
            initrd_path: Some(PathBuf::from("/var/lib/vms/initrd")),
            cmdline: Some("console=ttyS0".to_string()),
            firmware_path: None,
            snapshot_path: None,
        }
    }

    #[test]
    fn accepts_valid_linux_config() {
        let config = base_linux_config();
        assert!(validate_boot_config(&config).is_ok());
    }

    #[test]
    fn rejects_empty_name() {
        let mut config = base_linux_config();
        config.name = "  ".to_string();

        let error = validate_boot_config(&config).expect_err("expected name validation error");
        match error {
            ApiError::Validation { field, error } => {
                assert_eq!(field.as_deref(), Some("name"));
                assert_eq!(error, "name is required");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_linux_without_kernel_when_not_restoring() {
        let mut config = base_linux_config();
        config.kernel_path = None;

        let error = validate_boot_config(&config).expect_err("expected kernelPath validation");
        match error {
            ApiError::Validation { field, error } => {
                assert_eq!(field.as_deref(), Some("kernelPath"));
                assert_eq!(error, "kernelPath is required when bootMode=linux");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_relative_disk_path() {
        let mut config = base_linux_config();
        config.disks = vec![PathBuf::from("relative/disk.raw")];

        let error = validate_boot_config(&config).expect_err("expected disks validation");
        match error {
            ApiError::Validation { field, .. } => {
                assert_eq!(field.as_deref(), Some("disks"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn allows_restore_without_linux_or_uefi_specific_boot_files() {
        let mut config = base_linux_config();
        config.kernel_path = None;
        config.snapshot_path = Some(PathBuf::from("/var/lib/vms/snapshots/vm-a.snap"));

        assert!(validate_boot_config(&config).is_ok());
    }
}
