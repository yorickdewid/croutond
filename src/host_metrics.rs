use std::collections::HashMap;
use std::io;

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HostMetrics {
    pub(crate) collected_at: String,
    pub(crate) hostname: String,
    pub(crate) uptime_seconds: f64,
    pub(crate) load_average: LoadAverage,
    pub(crate) cpu: CpuMetrics,
    pub(crate) memory: MemoryMetrics,
    pub(crate) processes: ProcessMetrics,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LoadAverage {
    pub(crate) one_minute: f64,
    pub(crate) five_minutes: f64,
    pub(crate) fifteen_minutes: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CpuMetrics {
    pub(crate) logical_cores: usize,
    pub(crate) total_ticks: u64,
    pub(crate) idle_ticks: u64,
    pub(crate) busy_ticks: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemoryMetrics {
    pub(crate) total_bytes: u64,
    pub(crate) available_bytes: u64,
    pub(crate) used_bytes: u64,
    pub(crate) free_bytes: u64,
    pub(crate) buffers_bytes: u64,
    pub(crate) cached_bytes: u64,
    pub(crate) swap_total_bytes: u64,
    pub(crate) swap_free_bytes: u64,
    pub(crate) swap_used_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProcessMetrics {
    pub(crate) running: u64,
    pub(crate) total: u64,
}

pub(crate) fn collect_host_metrics() -> io::Result<HostMetrics> {
    let collected_at = chrono::Utc::now().to_rfc3339();
    let hostname = read_hostname()?;
    let uptime_seconds = read_uptime_seconds()?;
    let (load_average, processes) = read_loadavg_and_processes()?;
    let cpu = read_cpu_metrics()?;
    let memory = read_memory_metrics()?;

    Ok(HostMetrics {
        collected_at,
        hostname,
        uptime_seconds,
        load_average,
        cpu,
        memory,
        processes,
    })
}

fn read_hostname() -> io::Result<String> {
    let raw = std::fs::read_to_string("/proc/sys/kernel/hostname")?;
    Ok(raw.trim().to_string())
}

fn read_uptime_seconds() -> io::Result<f64> {
    let raw = std::fs::read_to_string("/proc/uptime")?;
    let first = raw
        .split_whitespace()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing uptime"))?;

    first.parse::<f64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid uptime value: {error}"),
        )
    })
}

fn read_loadavg_and_processes() -> io::Result<(LoadAverage, ProcessMetrics)> {
    let raw = std::fs::read_to_string("/proc/loadavg")?;
    let mut fields = raw.split_whitespace();

    let one_minute = parse_f64_field(fields.next(), "loadavg.one")?;
    let five_minutes = parse_f64_field(fields.next(), "loadavg.five")?;
    let fifteen_minutes = parse_f64_field(fields.next(), "loadavg.fifteen")?;

    let proc_field = fields
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing loadavg proc field"))?;

    let (running_str, total_str) = proc_field.split_once('/').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid loadavg process field format",
        )
    })?;

    let running = running_str.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid running process count: {error}"),
        )
    })?;

    let total = total_str.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid total process count: {error}"),
        )
    })?;

    Ok((
        LoadAverage {
            one_minute,
            five_minutes,
            fifteen_minutes,
        },
        ProcessMetrics { running, total },
    ))
}

fn parse_f64_field(field: Option<&str>, name: &str) -> io::Result<f64> {
    let raw = field
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing {name}")))?;

    raw.parse::<f64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {name}: {error}"),
        )
    })
}

fn read_cpu_metrics() -> io::Result<CpuMetrics> {
    let raw = std::fs::read_to_string("/proc/stat")?;
    let cpu_line = raw
        .lines()
        .find(|line| line.starts_with("cpu "))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing cpu stat line"))?;

    let mut fields = cpu_line.split_whitespace();
    let _ = fields.next();

    let mut ticks = Vec::new();
    for field in fields {
        let parsed = field.parse::<u64>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid cpu tick value: {error}"),
            )
        })?;
        ticks.push(parsed);
    }

    if ticks.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cpu stat line has fewer than 4 fields",
        ));
    }

    let user = ticks[0];
    let nice = ticks[1];
    let system = ticks[2];
    let idle = ticks[3];
    let iowait = *ticks.get(4).unwrap_or(&0);

    let total_ticks = ticks.iter().sum::<u64>();
    let idle_ticks = idle.saturating_add(iowait);
    let busy_ticks = total_ticks.saturating_sub(idle_ticks);
    let logical_cores = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);

    let _ = (user, nice, system);

    Ok(CpuMetrics {
        logical_cores,
        total_ticks,
        idle_ticks,
        busy_ticks,
    })
}

fn read_memory_metrics() -> io::Result<MemoryMetrics> {
    let raw = std::fs::read_to_string("/proc/meminfo")?;
    let meminfo = parse_meminfo_kib(&raw)?;

    let total_kib = required_meminfo_kib(&meminfo, "MemTotal")?;
    let available_kib = required_meminfo_kib(&meminfo, "MemAvailable")?;
    let free_kib = required_meminfo_kib(&meminfo, "MemFree")?;
    let buffers_kib = meminfo.get("Buffers").copied().unwrap_or(0);
    let cached_kib = meminfo.get("Cached").copied().unwrap_or(0);
    let swap_total_kib = meminfo.get("SwapTotal").copied().unwrap_or(0);
    let swap_free_kib = meminfo.get("SwapFree").copied().unwrap_or(0);

    let used_kib = total_kib.saturating_sub(available_kib);
    let swap_used_kib = swap_total_kib.saturating_sub(swap_free_kib);

    Ok(MemoryMetrics {
        total_bytes: kib_to_bytes(total_kib),
        available_bytes: kib_to_bytes(available_kib),
        used_bytes: kib_to_bytes(used_kib),
        free_bytes: kib_to_bytes(free_kib),
        buffers_bytes: kib_to_bytes(buffers_kib),
        cached_bytes: kib_to_bytes(cached_kib),
        swap_total_bytes: kib_to_bytes(swap_total_kib),
        swap_free_bytes: kib_to_bytes(swap_free_kib),
        swap_used_bytes: kib_to_bytes(swap_used_kib),
    })
}

fn parse_meminfo_kib(raw: &str) -> io::Result<HashMap<String, u64>> {
    let mut values = HashMap::new();

    for line in raw.lines() {
        let Some((key_raw, value_raw)) = line.split_once(':') else {
            continue;
        };

        let key = key_raw.trim();
        let mut parts = value_raw.split_whitespace();
        let value = match parts.next() {
            Some(v) => v,
            None => continue,
        };

        let parsed = value.parse::<u64>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid meminfo value for {key}: {error}"),
            )
        })?;

        values.insert(key.to_string(), parsed);
    }

    Ok(values)
}

fn required_meminfo_kib(meminfo: &HashMap<String, u64>, key: &str) -> io::Result<u64> {
    meminfo.get(key).copied().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("missing meminfo key: {key}"),
        )
    })
}

fn kib_to_bytes(value: u64) -> u64 {
    value.saturating_mul(1024)
}

#[cfg(test)]
mod tests {
    use super::parse_meminfo_kib;

    #[test]
    fn parse_meminfo_extracts_numeric_values() {
        let raw = "MemTotal:       16384 kB\nMemAvailable:   8192 kB\nSwapTotal:      4096 kB\n";
        let parsed = parse_meminfo_kib(raw).expect("parse should succeed");

        assert_eq!(parsed.get("MemTotal"), Some(&16384));
        assert_eq!(parsed.get("MemAvailable"), Some(&8192));
        assert_eq!(parsed.get("SwapTotal"), Some(&4096));
    }
}
