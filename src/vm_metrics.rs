use std::collections::HashMap;
use std::io;

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct VmProcessMetrics {
    pub(crate) collected_at: String,
    pub(crate) pid: u32,
    pub(crate) state: String,
    pub(crate) rss_bytes: u64,
    pub(crate) virtual_memory_bytes: u64,
    pub(crate) cpu_user_ticks: u64,
    pub(crate) cpu_system_ticks: u64,
    pub(crate) cpu_total_ticks: u64,
    pub(crate) thread_count: u32,
}

pub(crate) fn collect_vm_process_metrics(pid: u32) -> io::Result<VmProcessMetrics> {
    let stat_raw = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let stat = parse_proc_stat(&stat_raw)?;

    let status_raw = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    let status = parse_status_kib(&status_raw)?;

    let rss_kib = status.get("VmRSS").copied().unwrap_or(0);
    let vm_size_kib = status.get("VmSize").copied().unwrap_or(0);

    Ok(VmProcessMetrics {
        collected_at: chrono::Utc::now().to_rfc3339(),
        pid,
        state: stat.state.to_string(),
        rss_bytes: kib_to_bytes(rss_kib),
        virtual_memory_bytes: kib_to_bytes(vm_size_kib),
        cpu_user_ticks: stat.utime,
        cpu_system_ticks: stat.stime,
        cpu_total_ticks: stat.utime.saturating_add(stat.stime),
        thread_count: stat.num_threads,
    })
}

#[derive(Debug)]
struct ProcStat {
    state: char,
    utime: u64,
    stime: u64,
    num_threads: u32,
}

fn parse_proc_stat(raw: &str) -> io::Result<ProcStat> {
    // /proc/<pid>/stat has a comm field wrapped in parentheses and may include spaces.
    let right_paren = raw.rfind(')').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid /proc stat format: missing ')'",
        )
    })?;

    let after = raw.get((right_paren + 1)..).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid /proc stat format: truncated fields",
        )
    })?;

    let fields: Vec<&str> = after.split_whitespace().collect();
    if fields.len() < 18 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid /proc stat format: too few fields",
        ));
    }

    let state = fields[0]
        .chars()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process state"))?;

    // Offsets in this sliced array start at original field #3.
    let utime = parse_u64(fields[11], "utime")?;
    let stime = parse_u64(fields[12], "stime")?;
    let num_threads = parse_u32(fields[17], "num_threads")?;

    Ok(ProcStat {
        state,
        utime,
        stime,
        num_threads,
    })
}

fn parse_status_kib(raw: &str) -> io::Result<HashMap<String, u64>> {
    let mut out = HashMap::new();

    for line in raw.lines() {
        let Some((key_raw, value_raw)) = line.split_once(':') else {
            continue;
        };

        let mut parts = value_raw.split_whitespace();
        let Some(value) = parts.next() else {
            continue;
        };

        let parsed = match value.parse::<u64>() {
            Ok(parsed) => parsed,
            Err(_) => continue,
        };

        out.insert(key_raw.trim().to_string(), parsed);
    }

    Ok(out)
}

fn parse_u64(raw: &str, field: &str) -> io::Result<u64> {
    raw.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {field}: {error}"),
        )
    })
}

fn parse_u32(raw: &str, field: &str) -> io::Result<u32> {
    raw.parse::<u32>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {field}: {error}"),
        )
    })
}

fn kib_to_bytes(value: u64) -> u64 {
    value.saturating_mul(1024)
}

#[cfg(test)]
mod tests {
    use super::parse_proc_stat;

    #[test]
    fn parse_proc_stat_handles_name_with_spaces() {
        let raw = "42 (demo vm) S 1 2 3 4 5 6 7 8 9 10 120 80 13 14 15 16 8 18 19";
        let parsed = parse_proc_stat(raw).expect("parse should succeed");

        assert_eq!(parsed.state, 'S');
        assert_eq!(parsed.utime, 120);
        assert_eq!(parsed.stime, 80);
        assert_eq!(parsed.num_threads, 8);
    }
}
