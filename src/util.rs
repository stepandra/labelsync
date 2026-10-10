use std::{
    fs,
    io::Write,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Write a file atomically: temp file in the same dir, fsync, rename, fsync dir.
pub fn atomic_write(path: &Path, data: &[u8], mode: Option<u32>) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(dir)?;
    let fname = path.file_name().and_then(|s| s.to_str()).unwrap_or("out");
    let tmp = dir.join(format!(".{fname}.tmp.{}", std::process::id()));
    {
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        if let Some(m) = mode {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(m);
        }
        #[cfg(not(unix))]
        let _ = mode;
        let mut f = opts.open(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    fsync_dir(dir);
    Ok(())
}

/// Atomic write by streaming through a closure.
pub fn atomic_write_with<F>(path: &Path, f: F) -> std::io::Result<()>
where
    F: FnOnce(&mut std::io::BufWriter<fs::File>) -> std::io::Result<()>,
{
    let dir = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(dir)?;
    let fname = path.file_name().and_then(|s| s.to_str()).unwrap_or("out");
    let tmp = dir.join(format!(".{fname}.tmp.{}", std::process::id()));
    {
        let file = fs::File::create(&tmp)?;
        let mut w = std::io::BufWriter::new(file);
        f(&mut w)?;
        w.flush()?;
        w.get_ref().sync_all()?;
    }
    fs::rename(&tmp, path)?;
    fsync_dir(dir);
    Ok(())
}

fn fsync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(d) = fs::File::open(dir) {
        let _ = d.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

/// Parse an observed_at value: unix seconds, unix millis, or RFC3339. Returns ms.
pub fn parse_time_ms(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(n) = s.parse::<u64>() {
        return Some(if n < 100_000_000_000 { n * 1000 } else { n });
    }
    if let Ok(f) = s.parse::<f64>()
        && f > 0.0 {
            return Some((f * 1000.0) as u64);
        }
    humantime::parse_rfc3339_weak(s)
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
}

pub fn fmt_ms(ms: u64) -> String {
    humantime::format_rfc3339_seconds(UNIX_EPOCH + std::time::Duration::from_millis(ms)).to_string()
}
