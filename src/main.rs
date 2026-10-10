use std::{
    io::{Read, Write},
    path::PathBuf,
    process::ExitCode,
};

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use labelsync::{daemon, ipc::Request, util};
use serde_json::{Value, json};

#[derive(Parser, Debug)]
#[command(name = "labelsync", version, about = "Sync crypto address-label datasets between agents over iroh")]
struct Cli {
    /// Data directory (state, iroh stores, labels.db, inbox/, export/)
    #[arg(long, global = true, env = "LABELSYNC_DIR")]
    data_dir: Option<PathBuf>,
    /// Unix socket path (default: <data-dir>/labelsync.sock)
    #[arg(long, global = true, env = "LABELSYNC_SOCKET")]
    socket: Option<PathBuf>,
    /// Agent name to act as (claims/appends are signed with a per-agent author)
    #[arg(long, global = true, env = "LABELSYNC_AGENT")]
    agent: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run the daemon in the foreground
    Daemon {
        /// Default claim lease TTL in seconds
        #[arg(long, default_value_t = 2700)]
        ttl_secs: u64,
        /// Seconds between periodic re-syncs with each peer
        #[arg(long, default_value_t = 30)]
        sync_interval: u64,
        /// Max rows per segment
        #[arg(long, default_value_t = 50_000)]
        segment_rows: usize,
        /// Start with network sync paused (useful for tests / offline work)
        #[arg(long)]
        start_paused: bool,
        /// Re-export per-chain TSVs after every materialization
        #[arg(long)]
        auto_export: bool,
    },
    /// Print a ticket for this node's namespace (write by default; a bearer secret!)
    Share {
        #[arg(long)]
        read_only: bool,
        /// Addressing info embedded in the ticket: all | relay | direct | id
        #[arg(long)]
        addr: Option<String>,
        /// Write the ticket to this file (mode 0600) instead of stdout
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Join a namespace from a ticket (string, @file, or - for stdin)
    Join {
        ticket: String,
        /// Switch even if this node already has data in another namespace
        #[arg(long)]
        force: bool,
    },
    /// Append labels from stdin (TSV with header, CSV, or JSONL) or --file
    Append {
        #[arg(long)]
        source_key: Option<String>,
        #[arg(long)]
        label_source: Option<String>,
        /// Default chain for rows without a chain column
        #[arg(long)]
        chain: Option<String>,
        #[arg(long)]
        observed_at: Option<String>,
        /// auto|tsv|csv|jsonl
        #[arg(long)]
        format: Option<String>,
        /// Read from this file instead of stdin
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Export active labels to per-chain TSV files (atomic)
    Export {
        #[arg(long)]
        chain: Option<String>,
        #[arg(long)]
        out_dir: Option<PathBuf>,
    },
    /// Show node / sync / db status (optionally claim state for one source)
    Status {
        #[arg(long)]
        source_key: Option<String>,
    },
    /// Claim a source before mining it. Exit: 0 won, 2 lost, 3 skip (done/seen)
    Claim {
        source_key: String,
        #[arg(long)]
        source_url: Option<String>,
        /// BLAKE3 hex of the raw source file (skips if already ingested under any URL)
        #[arg(long)]
        source_hash: Option<String>,
        /// Compute --source-hash from this file
        #[arg(long)]
        source_file: Option<PathBuf>,
        #[arg(long)]
        ttl_secs: Option<u64>,
        /// Let the daemon renew the lease every TTL/3 until done/release
        #[arg(long)]
        auto_renew: bool,
    },
    /// Renew a claim (call every TTL/3). Exit: 0 won, 2 lost (stop work), 3 skip, 4 not claimed
    Renew { source_key: String },
    /// Mark a source finished; overrides all claims
    Done {
        source_key: String,
        #[arg(long)]
        source_hash: Option<String>,
        #[arg(long)]
        source_file: Option<PathBuf>,
    },
    /// Release a claim without finishing
    Release { source_key: String },
    /// pause | resume | now
    Sync { action: String },
    /// Ask the daemon to exit
    Stop,
    /// Print the BLAKE3 hash of a file (for --source-hash)
    Hash { file: PathBuf },
}

fn default_data_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".labelsync")
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "agent".into())
}

fn hash_file(p: &PathBuf) -> Result<String> {
    let mut h = blake3::Hasher::new();
    let mut f = std::fs::File::open(p).with_context(|| format!("open {}", p.display()))?;
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(h.finalize().to_hex().to_string())
}

#[cfg(unix)]
fn call(socket: &PathBuf, req: &Request) -> Result<Value> {
    use std::io::BufRead;
    let mut s = std::os::unix::net::UnixStream::connect(socket)
        .map_err(|e| anyhow!("cannot connect to daemon at {}: {e} (is `labelsync daemon` running?)", socket.display()))?;
    let mut line = serde_json::to_vec(req)?;
    line.push(b'\n');
    s.write_all(&line)?;
    let mut r = std::io::BufReader::new(s);
    let mut resp = String::new();
    r.read_line(&mut resp)?;
    if resp.trim().is_empty() {
        return Err(anyhow!("daemon closed connection without a response"));
    }
    Ok(serde_json::from_str(&resp)?)
}

#[cfg(not(unix))]
fn call(_socket: &PathBuf, _req: &Request) -> Result<Value> {
    Err(anyhow!("CLI<->daemon IPC over Windows named pipes is not implemented yet"))
}

fn print(v: &Value) {
    println!("{}", serde_json::to_string(v).unwrap_or_default());
}

fn exit_for(v: &Value) -> ExitCode {
    if v["ok"] != json!(true) {
        return ExitCode::from(1);
    }
    match v["result"].as_str() {
        Some("lost") => ExitCode::from(2),
        Some("skip") => ExitCode::from(3),
        Some("not_claimed") => ExitCode::from(4),
        _ => ExitCode::SUCCESS,
    }
}

fn run() -> Result<ExitCode> {
    let cli = Cli::parse();
    let data_dir = cli.data_dir.clone().unwrap_or_else(default_data_dir);
    let socket = cli.socket.clone().unwrap_or_else(|| data_dir.join("labelsync.sock"));
    let agent = cli.agent.clone();
    let source_hash = |h: Option<String>, f: Option<PathBuf>| -> Result<Option<String>> {
        Ok(match (h, f) {
            (Some(h), _) => Some(h.to_ascii_lowercase()),
            (None, Some(f)) => Some(hash_file(&f)?),
            _ => None,
        })
    };
    let req = match cli.cmd {
        Cmd::Daemon { ttl_secs, sync_interval, segment_rows, start_paused, auto_export } => {
            tracing_subscriber::fmt()
                .with_writer(std::io::stderr)
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_env("LABELSYNC_LOG")
                        .unwrap_or_else(|_| "labelsync=info,iroh=warn,iroh_docs=error".into()),
                )
                .init();
            let cfg = daemon::Config {
                data_dir,
                socket,
                agent: agent.unwrap_or_else(hostname),
                ttl_secs,
                sync_interval_secs: sync_interval.max(1),
                segment_rows,
                start_paused,
                auto_export,
            };
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(daemon::run(cfg))?;
            return Ok(ExitCode::SUCCESS);
        }
        Cmd::Hash { file } => {
            print(&json!({"ok": true, "file": file, "blake3": hash_file(&file)?}));
            return Ok(ExitCode::SUCCESS);
        }
        Cmd::Share { read_only, addr, out } => {
            let v = call(&socket, &Request::Share { read_only, addr })?;
            if let (Some(out), Some(t)) = (out, v["ticket"].as_str()) {
                util::atomic_write(&out, format!("{t}\n").as_bytes(), Some(0o600))?;
                print(&json!({"ok": true, "ticket_file": out, "mode": v["mode"], "namespace": v["namespace"]}));
            } else {
                print(&v);
            }
            return Ok(exit_for(&v));
        }
        Cmd::Join { ticket, force } => {
            let t = if ticket == "-" {
                let mut s = String::new();
                std::io::stdin().read_to_string(&mut s)?;
                s
            } else if let Some(p) = ticket.strip_prefix('@') {
                std::fs::read_to_string(p)?
            } else {
                ticket
            };
            Request::Join { ticket: t.trim().to_string(), force }
        }
        Cmd::Append { source_key, label_source, chain, observed_at, format, file } => {
            let (path, delete_after) = match file {
                Some(f) => (std::fs::canonicalize(&f)?, false),
                None => {
                    let tmpdir = data_dir.join("tmp");
                    std::fs::create_dir_all(&tmpdir)?;
                    let p = tmpdir.join(format!("append-{}-{}.in", std::process::id(), util::now_ms()));
                    let mut f = std::fs::File::create(&p)?;
                    std::io::copy(&mut std::io::stdin().lock(), &mut f)?;
                    f.sync_all()?;
                    (p, true)
                }
            };
            Request::Append {
                path: path.to_string_lossy().into_owned(),
                delete_after,
                agent,
                source_key,
                label_source,
                chain,
                observed_at,
                format,
            }
        }
        Cmd::Export { chain, out_dir } => Request::Export {
            chain,
            out_dir: out_dir.map(|p| std::path::absolute(&p).unwrap_or(p).to_string_lossy().into_owned()),
        },
        Cmd::Status { source_key } => Request::Status { source_key },
        Cmd::Claim { source_key, source_url, source_hash: h, source_file, ttl_secs, auto_renew } => Request::Claim {
            source_key,
            agent,
            source_url,
            source_hash: source_hash(h, source_file)?,
            ttl_secs,
            auto_renew,
        },
        Cmd::Renew { source_key } => Request::Renew { source_key, agent },
        Cmd::Done { source_key, source_hash: h, source_file } => Request::Done {
            source_key,
            agent,
            source_hash: source_hash(h, source_file)?,
        },
        Cmd::Release { source_key } => Request::Release { source_key, agent },
        Cmd::Sync { action } => Request::Sync { action },
        Cmd::Stop => Request::Shutdown,
    };
    let v = call(&socket, &req)?;
    print(&v);
    Ok(exit_for(&v))
}

fn main() -> ExitCode {
    match run() {
        Ok(c) => c,
        Err(e) => {
            print(&json!({"ok": false, "error": format!("{e:#}")}));
            ExitCode::from(1)
        }
    }
}
