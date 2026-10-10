//! labelsync CLI. Contract (see README "CLI contract"):
//! * stdout: exactly one JSON value per invocation (default) or `key: value` text with `--output text`
//! * stderr: diagnostics, warnings, error message + hint
//! * exit: 0 ok/won · 1 error · 2 lost · 3 skip · 4 not claimed · 5 nothing free · 64 usage error

use std::{
    io::{IsTerminal, Read, Write},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};

use anyhow::{Context, Result};
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum, parser::ValueSource};
use labelsync::{daemon, ipc::Request, segment, util};
use serde_json::{Value, json};

pub const EXIT_OK: u8 = 0;
pub const EXIT_ERROR: u8 = 1;
pub const EXIT_LOST: u8 = 2;
pub const EXIT_SKIP: u8 = 3;
pub const EXIT_NOT_CLAIMED: u8 = 4;
pub const EXIT_NONE: u8 = 5;
/// BSD EX_USAGE. Was clap's default 2 before v0.2, which collided with "lost".
pub const EXIT_USAGE: u8 = 64;

const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (commit ",
    env!("LABELSYNC_COMMIT"),
    ", ",
    env!("LABELSYNC_TARGET"),
    ", ",
    env!("LABELSYNC_PROFILE"),
    ")"
);

const ROOT_TEMPLATE: &str = "\
{about-with-newline}
{usage-heading} {usage}

Mining loop (what agents call):
  next      Pick the best free source by expected yield; --claim to take it
  claim     Take an advisory lease on a source before mining it
  renew     Extend your lease (every TTL/3); tells you if you lost it
  append    Add labels from stdin or --file (TSV/CSV/JSONL)
  report    Publish an immutable run record (rows, duration, errors, method)
  done      Mark a source finished; overrides every claim
  release   Give up your lease without finishing

Shared knowledge:
  howto     Co-edited markdown playbook per source (Automerge; get/put/log)
  queue     Shared list of sources to mine (add/rm/list)
  status    Node, sync, DB counts and claims (or one source with --source-key)
  export    Write active labels to per-chain TSV files (atomic)

Node and network:
  daemon    Run the node in the foreground (needed by every command above)
  share     Create a ticket that lets another node join (bearer secret)
  join      Join a namespace from a ticket (@file or - recommended)
  sync      Pause, resume or force network sync
  stop      Ask the daemon to exit

Tools:
  hash          BLAKE3 of a file (value for --source-hash)
  config        Show effective client configuration and where each value came from
  capabilities  Machine-readable inventory of commands, flags and exit codes (JSON)
  completions   Print a shell completion script
  man           Print the man page (roff)
  help          Print help for a command

Global options:
{options}{after-help}";

const ROOT_AFTER: &str = "\
Examples:
  labelsync daemon &                                   # start the node (logs on stderr)
  labelsync share --out ticket && labelsync join @ticket   # on the other machine
  labelsync next --claim --auto-renew                  # get work
  labelsync append --source-key SRC < labels.tsv       # push labels
  labelsync report SRC --duration-s 300 --method api && labelsync done SRC

Output: one JSON object on stdout ({\"ok\":true,...}); errors also go to stderr.
Exit codes: 0 ok/won, 1 error, 2 lost, 3 skip (done/seen), 4 not claimed, 5 nothing free, 64 usage.
Docs: README.md and DESIGN.md in the labelsync source tree (/workspace/labelsync).";

const EX_DAEMON: &str = "\
Examples:
  labelsync --agent hound-a daemon &
  LABELSYNC_LOG=labelsync=debug labelsync daemon --ttl-secs 1800
  labelsync daemon --start-paused          # offline; resume with `labelsync sync resume`

Side effects: creates the data dir (0700), binds a UDP socket for iroh, contacts n0 relays/DNS
for discovery, listens on the Unix socket. Stop with Ctrl-C, SIGTERM or `labelsync stop`.";

const EX_SHARE: &str = "\
Examples:
  labelsync share --out ~/labelsync.ticket          # file mode 0600, ticket not printed
  labelsync share --read-only --addr relay

A write ticket grants permanent, non-revocable write access. Never paste it into logs or chats.
Next: copy the file to the other machine and run `labelsync join @labelsync.ticket`.";

const EX_JOIN: &str = "\
Examples:
  labelsync join @labelsync.ticket
  labelsync join - < labelsync.ticket

Passing the ticket literally works but exposes it in shell history and process listings.";

const EX_APPEND: &str = "\
Examples:
  labelsync append --source-key etherscan:p3 < labels.tsv
  labelsync append --source-key dune:42 --chain eth --file out.csv
  labelsync append --source-key x --dry-run < labels.tsv   # validate only, no daemon needed

Input: header row with chain,address,label[,label_source,category,op,observed_at,source_key].
Rejected rows are reported in \"errors\"; accepted rows are still written. Idempotent.";

const EX_EXPORT: &str = "\
Examples:
  labelsync export
  labelsync export --chain eth --out-dir ./out

Writes <out-dir>/<chain>.tsv atomically (temp file + fsync + rename).";

const EX_STATUS: &str = "\
Examples:
  labelsync status
  labelsync status --source-key etherscan:p3 | jq .source.winner";

const EX_CLAIM: &str = "\
Examples:
  labelsync claim etherscan:p3 --source-url https://... && ./mine.sh
  labelsync claim dune:42 --source-file raw.csv --auto-renew

Exit codes: 0 won (go), 2 lost (someone else has it), 3 skip (done or same raw file seen), 1 error.
Claims are advisory: after a partition two agents may both win; `renew` resolves it.
Next: `labelsync renew SRC` every TTL/3 (or --auto-renew), then `append`, `report`, `done`.";

const EX_RENEW: &str = "\
Examples:
  labelsync renew etherscan:p3 || echo \"stop mining\"

Exit codes: 0 still yours, 2 lost after a merge (stop at the next checkpoint), 3 done elsewhere,
4 you hold no live claim (re-claim), 1 error.";

const EX_DONE: &str = "\
Examples:
  labelsync done etherscan:p3
  labelsync done dune:42 --source-file raw.csv     # also blocks the same file under other keys";

const EX_RELEASE: &str = "\
Examples:
  labelsync release etherscan:p3";

const EX_SYNC: &str = "\
Examples:
  labelsync sync pause     # stop syncing; incoming sync connections are refused
  labelsync sync resume
  labelsync sync now       # reset peer backoff and sync immediately";

const EX_HOWTO: &str = "\
Examples:
  labelsync howto get arkham | jq -r .content > playbook.md
  V=$(labelsync howto get arkham | jq -r .version)
  $EDITOR playbook.md && labelsync howto put arkham --base \"$V\" --file playbook.md
  labelsync howto log arkham";

const EX_HOWTO_PUT: &str = "\
Examples:
  labelsync howto put arkham --base \"$V\" --file playbook.md
  printf '# notes\\n' | labelsync howto put newsource

Always pass --base with the version you read; without it the diff is taken against the current
version and may undo someone's fresh edit.";

const EX_REPORT: &str = "\
Examples:
  labelsync report etherscan:p3 --duration-s 540 --errors 2 --method api-v2 --howto-version \"$V\"
  echo '{\"duration_s\":540,\"rows_new\":300}' | labelsync report etherscan:p3 --json -

Rows are computed from your segments for the source since your claim unless given.
Next: `labelsync done SRC`.";

const EX_NEXT: &str = "\
Examples:
  labelsync next                               # best candidate from the shared queue
  labelsync next --claim --auto-renew          # and take it
  labelsync next --sources my-list.txt --claim
  ls sources/ | labelsync next --sources -

Exit codes: 0 found/won, 5 nothing free, 1 error.
Output: source_key, score, stats, best_method, playbook.latest/best {version, content}.
Next: mine it, `append`, `report SRC --howto-version <playbook version>`, `done SRC`.";

const EX_QUEUE: &str = "\
Examples:
  labelsync queue add etherscan:p1 etherscan:p2
  labelsync queue add --file sources.txt --source-url https://...
  labelsync queue list | jq -r '.queue[].source_key'
  labelsync queue rm etherscan:p1";

const EX_STOP: &str = "\
Examples:
  labelsync stop";

const EX_HASH: &str = "\
Examples:
  labelsync hash raw.csv | jq -r .blake3";

const EX_CONFIG: &str = "\
Examples:
  labelsync config
  LABELSYNC_DIR=/srv/ls labelsync config --output text";

const EX_CAPS: &str = "\
Examples:
  labelsync capabilities | jq '.commands[].name'";

const EX_COMPLETIONS: &str = "\
Examples:
  labelsync completions bash > ~/.local/share/bash-completion/completions/labelsync
  labelsync completions zsh > \"${fpath[1]}/_labelsync\"
  labelsync completions fish > ~/.config/fish/completions/labelsync.fish";

const EX_MAN: &str = "\
Examples:
  labelsync man > ~/.local/share/man/man1/labelsync.1 && man labelsync";

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum OutputMode {
    Json,
    Text,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum AddrMode {
    All,
    Relay,
    Direct,
    Id,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum InputFormat {
    Auto,
    Tsv,
    Csv,
    Jsonl,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum SyncAction {
    Pause,
    Resume,
    Now,
}

#[derive(Parser, Debug)]
#[command(
    name = "labelsync",
    version = VERSION,
    about = "Sync crypto address-label datasets between agents over iroh, and coordinate who mines what",
    help_template = ROOT_TEMPLATE,
    after_help = ROOT_AFTER,
    arg_required_else_help = true,
    max_term_width = 100
)]
struct Cli {
    /// Data directory [default: ~/.labelsync if it exists, else $XDG_DATA_HOME/labelsync]
    #[arg(long, global = true, help_heading = "Global options", env = "LABELSYNC_DIR", value_name = "DIR")]
    data_dir: Option<PathBuf>,
    /// Daemon socket [default: <data-dir>/labelsync.sock]
    #[arg(long, global = true, help_heading = "Global options", env = "LABELSYNC_SOCKET", value_name = "PATH")]
    socket: Option<PathBuf>,
    /// Agent name to act as; each agent gets its own signing author [default: hostname]
    #[arg(long, global = true, help_heading = "Global options", env = "LABELSYNC_AGENT", value_name = "NAME")]
    agent: Option<String>,
    /// stdout format: json = one JSON object (stable contract), text = `key: value` lines for people
    #[arg(long, global = true, help_heading = "Global options", env = "LABELSYNC_OUTPUT", value_enum, default_value = "json", value_name = "MODE")]
    output: OutputMode,
    /// Seconds to wait for the daemon's answer
    #[arg(long, global = true, help_heading = "Global options", env = "LABELSYNC_TIMEOUT", default_value_t = 300, value_name = "SECS")]
    timeout: u64,
    /// Suppress warnings and hints on stderr (errors are still printed); daemon: log warnings only
    #[arg(long, short = 'q', global = true, help_heading = "Global options")]
    quiet: bool,
    /// Daemon: debug logging (overridden by LABELSYNC_LOG)
    #[arg(long, global = true, help_heading = "Global options")]
    verbose: bool,
    /// Never prompt (labelsync has no prompts; accepted for scripts that always pass it)
    #[arg(long, global = true, help_heading = "Global options")]
    no_input: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Pick the best free source by expected yield; --claim to take it
    #[command(after_help = EX_NEXT)]
    Next {
        /// Candidate source keys, one per line, from FILE or - (stdin) [default: shared queue]
        #[arg(long, value_name = "FILE|-")]
        sources: Option<String>,
        /// Also claim it; if the claim is lost, try the next candidate
        #[arg(long)]
        claim: bool,
        /// With --claim: the daemon renews the lease every TTL/3 until done/release
        #[arg(long)]
        auto_renew: bool,
        /// With --claim: lease length in seconds [default: daemon --ttl-secs]
        #[arg(long, value_name = "SECS")]
        ttl_secs: Option<u64>,
    },
    /// Take an advisory lease on a source before mining it
    #[command(after_help = EX_CLAIM)]
    Claim {
        /// Stable identifier of the source (any string up to 512 bytes)
        source_key: String,
        /// URL recorded in the claim (informational)
        #[arg(long, value_name = "URL")]
        source_url: Option<String>,
        /// BLAKE3 hex of the raw source file; skip if it was already ingested under any key
        #[arg(long, value_name = "HEX", conflicts_with = "source_file")]
        source_hash: Option<String>,
        /// Compute --source-hash from this file
        #[arg(long, value_name = "FILE")]
        source_file: Option<PathBuf>,
        /// Lease length in seconds [default: daemon --ttl-secs, 2700]
        #[arg(long, value_name = "SECS")]
        ttl_secs: Option<u64>,
        /// The daemon renews the lease every TTL/3 until done/release
        #[arg(long)]
        auto_renew: bool,
    },
    /// Extend your lease (every TTL/3); tells you if you lost it
    #[command(after_help = EX_RENEW)]
    Renew {
        /// Source key you claimed
        source_key: String,
    },
    /// Add labels from stdin or --file (TSV/CSV/JSONL)
    #[command(after_help = EX_APPEND)]
    Append {
        /// Source key for rows without a source_key column (or `# source_key=` directive)
        #[arg(long, value_name = "KEY")]
        source_key: Option<String>,
        /// label_source for rows without one [default: the source key]
        #[arg(long, value_name = "NAME")]
        label_source: Option<String>,
        /// Chain for rows without a chain column (e.g. eth, btc, sol)
        #[arg(long, value_name = "CHAIN")]
        chain: Option<String>,
        /// observed_at for rows without one (unix s/ms or RFC3339) [default: write time]
        #[arg(long, value_name = "TIME")]
        observed_at: Option<String>,
        /// Input format
        #[arg(long, value_enum, default_value = "auto")]
        format: InputFormat,
        /// Read from FILE instead of stdin
        #[arg(long, value_name = "FILE")]
        file: Option<PathBuf>,
        /// Parse and validate locally, print the segments that would be written; no daemon, no writes
        #[arg(long)]
        dry_run: bool,
    },
    /// Publish an immutable run record (rows, duration, errors, method)
    #[command(after_help = EX_REPORT)]
    Report {
        /// Source key the run was about
        source_key: String,
        /// Read a JSON object with the fields below from FILE or - (stdin); flags override it
        #[arg(long, value_name = "FILE|-")]
        json: Option<String>,
        /// Rows produced [default: computed from your segments]
        #[arg(long, value_name = "N")]
        rows_total: Option<u64>,
        /// Rows new after dedup [default: computed from your segments since the run started]
        #[arg(long, value_name = "N")]
        rows_new: Option<u64>,
        /// Wall-clock duration of the run in seconds
        #[arg(long, value_name = "SECS")]
        duration_s: Option<f64>,
        /// Number of errors hit
        #[arg(long, value_name = "N")]
        errors: Option<u64>,
        /// Number of retries
        #[arg(long, value_name = "N")]
        retries: Option<u64>,
        /// Method used (free text or id), aggregated in per-method stats
        #[arg(long, value_name = "TEXT")]
        method: Option<String>,
        /// Playbook version used (from `howto get` / `next`)
        #[arg(long, value_name = "VERSION")]
        howto_version: Option<String>,
        /// Free-text notes
        #[arg(long, value_name = "TEXT")]
        notes: Option<String>,
        /// Run start (unix s/ms or RFC3339) [default: your claim time, else now - duration]
        #[arg(long, value_name = "TIME")]
        started_at: Option<String>,
    },
    /// Mark a source finished; overrides every claim
    #[command(after_help = EX_DONE)]
    Done {
        /// Source key to mark done
        source_key: String,
        /// BLAKE3 hex of the raw source file, recorded so the same file is skipped under other keys
        #[arg(long, value_name = "HEX", conflicts_with = "source_file")]
        source_hash: Option<String>,
        /// Compute --source-hash from this file
        #[arg(long, value_name = "FILE")]
        source_file: Option<PathBuf>,
    },
    /// Give up your lease without finishing
    #[command(after_help = EX_RELEASE)]
    Release {
        /// Source key you claimed
        source_key: String,
    },
    /// Co-edited markdown playbook per source (Automerge; get/put/log)
    #[command(after_help = EX_HOWTO, arg_required_else_help = true)]
    Howto {
        #[command(subcommand)]
        cmd: HowtoCmd,
    },
    /// Shared list of sources to mine (add/rm/list)
    #[command(after_help = EX_QUEUE, arg_required_else_help = true)]
    Queue {
        #[command(subcommand)]
        cmd: QueueCmd,
    },
    /// Node, sync, DB counts and claims (or one source with --source-key)
    #[command(after_help = EX_STATUS)]
    Status {
        /// Show done record, claims and winner for this source
        #[arg(long, value_name = "KEY")]
        source_key: Option<String>,
    },
    /// Write active labels to per-chain TSV files (atomic)
    #[command(after_help = EX_EXPORT)]
    Export {
        /// Only this chain
        #[arg(long, value_name = "CHAIN")]
        chain: Option<String>,
        /// Output directory [default: <data-dir>/export]
        #[arg(long, value_name = "DIR")]
        out_dir: Option<PathBuf>,
    },
    /// Run the node in the foreground (needed by every command above)
    #[command(after_help = EX_DAEMON)]
    Daemon {
        /// Default claim lease TTL in seconds
        #[arg(long, default_value_t = 2700, value_name = "SECS")]
        ttl_secs: u64,
        /// Seconds between periodic re-syncs with each peer
        #[arg(long, default_value_t = 30, value_name = "SECS")]
        sync_interval: u64,
        /// Max rows per segment
        #[arg(long, default_value_t = 50_000, value_name = "N")]
        segment_rows: usize,
        /// Start with network sync paused (offline work, tests)
        #[arg(long)]
        start_paused: bool,
        /// Re-export per-chain TSVs after every change (costly for millions of rows)
        #[arg(long)]
        auto_export: bool,
    },
    /// Create a ticket that lets another node join (bearer secret)
    #[command(after_help = EX_SHARE)]
    Share {
        /// Read-only ticket (holders can sync but not write)
        #[arg(long)]
        read_only: bool,
        /// Addressing info embedded in the ticket
        #[arg(long, value_enum, default_value = "all")]
        addr: AddrMode,
        /// Write the ticket to FILE (mode 0600) instead of printing it
        #[arg(long, value_name = "FILE")]
        out: Option<PathBuf>,
    },
    /// Join a namespace from a ticket (@file or - recommended)
    #[command(after_help = EX_JOIN)]
    Join {
        /// Ticket string, @FILE, or - to read it from stdin
        ticket: String,
        /// Abandon this node's current namespace even if it holds data (that data stays local only)
        #[arg(long)]
        force: bool,
    },
    /// Pause, resume or force network sync
    #[command(after_help = EX_SYNC)]
    Sync {
        /// pause: stop syncing and refuse incoming sync; resume: restart; now: reset backoff and sync
        #[arg(value_enum)]
        action: SyncAction,
    },
    /// Ask the daemon to exit
    #[command(after_help = EX_STOP)]
    Stop,
    /// BLAKE3 of a file (value for --source-hash)
    #[command(after_help = EX_HASH)]
    Hash {
        /// File to hash
        file: PathBuf,
    },
    /// Show effective client configuration and where each value came from
    #[command(after_help = EX_CONFIG)]
    Config,
    /// Machine-readable inventory of commands, flags and exit codes (JSON)
    #[command(after_help = EX_CAPS)]
    Capabilities,
    /// Print a shell completion script
    #[command(after_help = EX_COMPLETIONS)]
    Completions {
        /// Target shell
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
    /// Print the man page (roff)
    #[command(after_help = EX_MAN)]
    Man,
}

#[derive(Subcommand, Debug)]
enum HowtoCmd {
    /// Merged markdown and its version id (or a historical --version)
    #[command(after_help = "Examples:\n  labelsync howto get arkham | jq -r .content\n  labelsync howto get arkham --version \"$V\"")]
    Get {
        /// Source key
        source_key: String,
        /// Render this historical version (comma-separated change hashes from `version`)
        #[arg(long, value_name = "VERSION")]
        version: Option<String>,
    },
    /// Submit the full new markdown; only the changed spans are applied, others' edits are kept
    #[command(after_help = EX_HOWTO_PUT)]
    Put {
        /// Source key
        source_key: String,
        /// Read the markdown from FILE instead of stdin
        #[arg(long, value_name = "FILE")]
        file: Option<PathBuf>,
        /// Version your edit started from (from `howto get`) [default: current version]
        #[arg(long, value_name = "VERSION")]
        base: Option<String>,
    },
    /// Change history: hash, author, agent, time, message
    #[command(after_help = "Examples:\n  labelsync howto log arkham | jq -r '.changes[] | \"\\(.time) \\(.agent)\"'")]
    Log {
        /// Source key
        source_key: String,
    },
}

#[derive(Subcommand, Debug)]
enum QueueCmd {
    /// Add source keys (arguments and/or one per line from --file)
    #[command(after_help = "Examples:\n  labelsync queue add src-a src-b\n  labelsync queue add --file sources.txt")]
    Add {
        /// Source keys to add
        items: Vec<String>,
        /// Also read keys, one per line, from FILE or - (stdin); lines starting with # are ignored
        #[arg(long, value_name = "FILE|-")]
        file: Option<String>,
        /// URL stored with every added key
        #[arg(long, value_name = "URL")]
        source_url: Option<String>,
    },
    /// Remove source keys from the shared queue
    #[command(after_help = "Examples:\n  labelsync queue rm src-a")]
    Rm {
        /// Source keys to remove
        #[arg(required = true)]
        items: Vec<String>,
    },
    /// List queued sources (oldest first)
    #[command(after_help = "Examples:\n  labelsync queue list | jq -r '.queue[].source_key'")]
    List,
}

// ---------------------------------------------------------------------------------------------

struct Ctx {
    data_dir: PathBuf,
    socket: PathBuf,
    agent: Option<String>,
    output: OutputMode,
    timeout: Duration,
    quiet: bool,
}

fn default_data_dir() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    let legacy = home.join(".labelsync");
    if legacy.exists() {
        return legacy; // keep existing installs working
    }
    std::env::var_os("XDG_DATA_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/share"))
        .join("labelsync")
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "agent".into())
}

fn hash_file(p: &Path) -> Result<String> {
    let mut h = blake3::Hasher::new();
    let mut f = std::fs::File::open(p).with_context(|| format!("cannot open {}", p.display()))?;
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

/// A failure the CLI itself detected, with a stable code, exit status and recovery hint.
#[derive(Debug)]
struct CliError {
    code: &'static str,
    exit: u8,
    message: String,
    hint: Option<String>,
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for CliError {}

fn cli_err(code: &'static str, exit: u8, message: impl Into<String>, hint: Option<&str>) -> anyhow::Error {
    anyhow::Error::new(CliError { code, exit, message: message.into(), hint: hint.map(String::from) })
}

/// Read stdin, refusing to block on an interactive terminal.
fn read_stdin(what: &str, alt: &str) -> Result<String> {
    if std::io::stdin().is_terminal() {
        return Err(cli_err(
            "no_input",
            EXIT_USAGE,
            format!("{what} expected on stdin, but stdin is a terminal"),
            Some(&format!("pipe the data in, or use {alt}")),
        ));
    }
    let mut s = String::new();
    std::io::stdin().read_to_string(&mut s)?;
    Ok(s)
}

fn read_arg_file(spec: &str, what: &str) -> Result<String> {
    if spec == "-" {
        read_stdin(what, "a file path instead of -")
    } else {
        std::fs::read_to_string(spec).with_context(|| format!("cannot read {spec}"))
    }
}

#[cfg(unix)]
fn call(ctx: &Ctx, req: &Request) -> Result<Value> {
    use std::io::BufRead;
    let mut s = std::os::unix::net::UnixStream::connect(&ctx.socket).map_err(|e| {
        cli_err(
            "daemon_unreachable",
            EXIT_ERROR,
            format!("cannot connect to the labelsync daemon at {}: {e}", ctx.socket.display()),
            Some("start it with `labelsync daemon &`, or point --data-dir/LABELSYNC_DIR (or --socket) at the running one"),
        )
    })?;
    s.set_read_timeout(Some(ctx.timeout))?;
    s.set_write_timeout(Some(ctx.timeout))?;
    let mut line = serde_json::to_vec(req)?;
    line.push(b'\n');
    s.write_all(&line)?;
    let mut r = std::io::BufReader::new(s);
    let mut resp = String::new();
    if let Err(e) = r.read_line(&mut resp) {
        if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) {
            return Err(cli_err(
                "timeout",
                EXIT_ERROR,
                format!("no answer from the daemon within {}s", ctx.timeout.as_secs()),
                Some("the request may still complete; check `labelsync status`, or raise --timeout / LABELSYNC_TIMEOUT"),
            ));
        }
        return Err(e.into());
    }
    if resp.trim().is_empty() {
        return Err(cli_err("daemon_error", EXIT_ERROR, "the daemon closed the connection without an answer", Some("see the daemon's stderr log")));
    }
    Ok(serde_json::from_str(&resp)?)
}

#[cfg(not(unix))]
fn call(_ctx: &Ctx, _req: &Request) -> Result<Value> {
    Err(cli_err("unsupported", EXIT_ERROR, "CLI<->daemon IPC over Windows named pipes is not implemented yet", Some("run labelsync under WSL")))
}

/// Exit status for a result object. Public contract; see README.
pub fn exit_code_for(v: &Value) -> u8 {
    if v["ok"] != json!(true) {
        return v["exit"].as_u64().map(|x| x as u8).unwrap_or(EXIT_ERROR);
    }
    match v["result"].as_str() {
        Some("lost") => EXIT_LOST,
        Some("skip") => EXIT_SKIP,
        Some("not_claimed") => EXIT_NOT_CLAIMED,
        Some("none") => EXIT_NONE,
        _ => EXIT_OK,
    }
}

/// Recovery hints for errors reported by the daemon.
fn daemon_hint(msg: &str) -> Option<&'static str> {
    if msg.contains("refusing to switch") {
        Some("this node already holds data in another namespace; re-run with --force to abandon it")
    } else if msg.contains("invalid ticket") {
        Some("pass the ticket exactly as printed by `labelsync share` (or use @file)")
    } else if msg.contains("base version not known") {
        Some("wait for sync (`labelsync sync now`) or omit --base to edit the current version")
    } else if msg.contains("unknown playbook version") {
        Some("get valid versions with `labelsync howto log SRC`")
    } else {
        None
    }
}

fn render_text(v: &Value) -> String {
    let mut out = String::new();
    if let Value::Object(m) = v {
        for (k, val) in m {
            if k == "ok" {
                continue;
            }
            match val {
                Value::String(s) if s.contains('\n') => out.push_str(&format!("{k}:\n{s}\n")),
                Value::String(s) => out.push_str(&format!("{k}: {s}\n")),
                Value::Null => {}
                other => out.push_str(&format!("{k}: {other}\n")),
            }
        }
    } else {
        out.push_str(&format!("{v}\n"));
    }
    out
}

/// Print a result: stdout gets the result, stderr gets error message + hint.
fn emit(ctx: &Ctx, mut v: Value) -> u8 {
    if v["ok"] != json!(true) {
        if v.get("code").is_none() {
            v["code"] = json!("daemon_error");
        }
        if v.get("hint").is_none() {
            if let Some(h) = v["error"].as_str().and_then(daemon_hint) {
                v["hint"] = json!(h);
            }
        }
        let mut err = std::io::stderr().lock();
        let _ = writeln!(err, "labelsync: error: {}", v["error"].as_str().unwrap_or("unknown error"));
        if let (Some(h), false) = (v["hint"].as_str(), ctx.quiet) {
            let _ = writeln!(err, "  hint: {h}");
        }
    }
    let code = exit_code_for(&v);
    let mut out = std::io::stdout().lock();
    match ctx.output {
        // JSON mode keeps the error object on stdout too, so agents always parse one value.
        OutputMode::Json => {
            let _ = writeln!(out, "{}", serde_json::to_string(&v).unwrap_or_default());
        }
        OutputMode::Text => {
            if v["ok"] == json!(true) {
                let _ = write!(out, "{}", render_text(&v));
            }
        }
    }
    code
}

fn warn(ctx: &Ctx, msg: &str) {
    if !ctx.quiet {
        eprintln!("labelsync: warning: {msg}");
    }
}

fn capabilities() -> Value {
    fn args(c: &clap::Command) -> Vec<Value> {
        c.get_arguments()
            .filter(|a| !a.is_hide_set() && a.get_id() != "help" && a.get_id() != "version")
            .map(|a| {
                json!({
                    "name": a.get_id().as_str(),
                    "long": a.get_long(),
                    "positional": a.is_positional(),
                    "required": a.is_required_set(),
                    "takes_value": a.get_action().takes_values(),
                    "values": a.get_possible_values().iter().map(|p| p.get_name().to_string()).collect::<Vec<_>>(),
                    "default": a.get_default_values().iter().map(|d| d.to_string_lossy().into_owned()).collect::<Vec<_>>(),
                    "env": a.get_env().map(|e| e.to_string_lossy().into_owned()),
                    "global": a.is_global_set(),
                    "help": a.get_help().map(|h| h.to_string()),
                })
            })
            .collect()
    }
    fn cmd(c: &clap::Command) -> Value {
        json!({
            "name": c.get_name(),
            "about": c.get_about().map(|s| s.to_string()),
            "args": args(c),
            "subcommands": c.get_subcommands().filter(|s| s.get_name() != "help").map(cmd).collect::<Vec<_>>(),
        })
    }
    let root = Cli::command();
    json!({
        "ok": true,
        "schema": "labelsync.capabilities/v1",
        "name": "labelsync",
        "version": env!("CARGO_PKG_VERSION"),
        "commit": env!("LABELSYNC_COMMIT"),
        "target": env!("LABELSYNC_TARGET"),
        "output": {"default": "json", "contract": "exactly one JSON object on stdout; \"ok\" is true on success; on failure \"error\", \"code\", optional \"hint\""},
        "exit_codes": {"0": "ok / won / found", "1": "error", "2": "lost (claim/renew)", "3": "skip: done or source seen",
                        "4": "not claimed (renew)", "5": "nothing free (next)", "64": "usage error"},
        "global_args": args(&root),
        "commands": root.get_subcommands().filter(|s| s.get_name() != "help").map(cmd).collect::<Vec<_>>(),
    })
}

fn source_hash(h: Option<String>, f: Option<PathBuf>) -> Result<Option<String>> {
    Ok(match (h, f) {
        (Some(h), _) => Some(h.to_ascii_lowercase()),
        (None, Some(f)) => Some(hash_file(&f)?),
        _ => None,
    })
}

fn fmt_name(f: InputFormat) -> Option<String> {
    match f {
        InputFormat::Auto => None,
        InputFormat::Tsv => Some("tsv".into()),
        InputFormat::Csv => Some("csv".into()),
        InputFormat::Jsonl => Some("jsonl".into()),
    }
}

fn run(cli: Cli, matches: &clap::ArgMatches) -> Result<u8> {
    let output = cli.output;
    let data_dir = cli.data_dir.clone().unwrap_or_else(default_data_dir);
    let socket = cli.socket.clone().unwrap_or_else(|| data_dir.join("labelsync.sock"));
    let ctx = Ctx { data_dir, socket, agent: cli.agent.clone(), output, timeout: Duration::from_secs(cli.timeout.max(1)), quiet: cli.quiet };
    let agent = ctx.agent.clone();

    let req = match cli.cmd {
        Cmd::Daemon { ttl_secs, sync_interval, segment_rows, start_paused, auto_export } => {
            let default_filter = if cli.quiet {
                "labelsync=warn,iroh=error,iroh_docs=error"
            } else if cli.verbose {
                "labelsync=debug,iroh=info,iroh_docs=warn"
            } else {
                "labelsync=info,iroh=warn,iroh_docs=error"
            };
            let color = std::io::stderr().is_terminal()
                && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty())
                && std::env::var("TERM").map(|t| t != "dumb").unwrap_or(true);
            tracing_subscriber::fmt()
                .with_writer(std::io::stderr)
                .with_ansi(color)
                .with_env_filter(tracing_subscriber::EnvFilter::try_from_env("LABELSYNC_LOG").unwrap_or_else(|_| default_filter.into()))
                .init();
            let cfg = daemon::Config {
                data_dir: ctx.data_dir.clone(),
                socket: ctx.socket.clone(),
                agent: agent.unwrap_or_else(hostname),
                ttl_secs,
                sync_interval_secs: sync_interval.max(1),
                segment_rows,
                start_paused,
                auto_export,
            };
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(daemon::run(cfg))?;
            return Ok(EXIT_OK);
        }
        Cmd::Hash { file } => return Ok(emit(&ctx, json!({"ok": true, "file": file, "blake3": hash_file(&file)?}))),
        Cmd::Capabilities => return Ok(emit(&ctx, capabilities())),
        Cmd::Completions { shell } => {
            let mut buf = Vec::new();
            clap_complete::generate(shell, &mut Cli::command(), "labelsync", &mut buf);
            std::io::stdout().write_all(&buf)?;
            return Ok(EXIT_OK);
        }
        Cmd::Man => {
            clap_mangen::Man::new(Cli::command()).render(&mut std::io::stdout())?;
            return Ok(EXIT_OK);
        }
        Cmd::Config => {
            let src = |id: &str| match matches.value_source(id) {
                Some(ValueSource::CommandLine) => "flag",
                Some(ValueSource::EnvVariable) => "env",
                Some(ValueSource::DefaultValue) => "default",
                _ => "default",
            };
            let v = json!({
                "ok": true,
                "data_dir": {"value": ctx.data_dir, "from": src("data_dir")},
                "socket": {"value": ctx.socket, "from": if matches.value_source("socket").is_some() { src("socket") } else { "derived from data_dir" }},
                "agent": {"value": ctx.agent.clone().unwrap_or_else(hostname), "from": if ctx.agent.is_some() { src("agent") } else { "hostname (daemon's default agent)" }},
                "output": {"value": format!("{:?}", ctx.output).to_lowercase(), "from": src("output")},
                "timeout_secs": {"value": ctx.timeout.as_secs(), "from": src("timeout")},
                "precedence": "flags > environment > defaults (no config file)",
                "daemon_running": ctx.socket.exists() && { #[cfg(unix)] { std::os::unix::net::UnixStream::connect(&ctx.socket).is_ok() } #[cfg(not(unix))] { false } },
            });
            return Ok(emit(&ctx, v));
        }
        Cmd::Share { read_only, addr, out } => {
            let addr = format!("{addr:?}").to_lowercase();
            let v = call(&ctx, &Request::Share { read_only, addr: Some(addr) })?;
            if let (Some(out), Some(t)) = (out, v["ticket"].as_str()) {
                util::atomic_write(&out, format!("{t}\n").as_bytes(), Some(0o600))?;
                return Ok(emit(&ctx, json!({"ok": true, "ticket_file": out, "mode": v["mode"], "namespace": v["namespace"]})));
            }
            return Ok(emit(&ctx, v));
        }
        Cmd::Join { ticket, force } => {
            let t = if ticket == "-" {
                read_stdin("a ticket", "@FILE")?
            } else if let Some(p) = ticket.strip_prefix('@') {
                std::fs::read_to_string(p).with_context(|| format!("cannot read ticket file {p}"))?
            } else {
                warn(&ctx, "a ticket on the command line is visible in shell history and process listings; prefer `join @FILE` or `join -`");
                ticket
            };
            Request::Join { ticket: t.trim().to_string(), force }
        }
        Cmd::Append { source_key, label_source, chain, observed_at, format, file, dry_run } => {
            if dry_run {
                let data = match &file {
                    Some(f) => std::fs::read(f).with_context(|| format!("cannot read {}", f.display()))?,
                    None => read_stdin("label rows", "--file FILE")?.into_bytes(),
                };
                let ext = file.as_ref().and_then(|f| f.extension()).and_then(|e| e.to_str());
                let fmt = match format {
                    InputFormat::Auto => segment::detect_format(&data, ext),
                    InputFormat::Tsv => segment::Format::Tsv,
                    InputFormat::Csv => segment::Format::Csv,
                    InputFormat::Jsonl => segment::Format::Jsonl,
                };
                let d = segment::Defaults { source_key, label_source, chain, observed_at, agent: None };
                let p = segment::parse_input(&data, fmt, &d)?;
                let (accepted, rejected, errors) = (p.rows.len(), p.rejected, p.errors.clone());
                let segs = segment::segments_from_parsed(p.rows, segment::DEFAULT_MAX_ROWS);
                return Ok(emit(&ctx, json!({
                    "ok": true, "dry_run": true, "rows_in": accepted + rejected, "rows_accepted": accepted, "rejected": rejected,
                    "errors": errors,
                    "segments": segs.iter().map(|s| json!({"key": s.key(), "chain": s.chain, "source_key": s.source_key, "hash": s.hash_hex, "rows": s.rows})).collect::<Vec<_>>(),
                })));
            }
            let (path, delete_after) = match file {
                Some(f) => (std::fs::canonicalize(&f).with_context(|| format!("cannot read {}", f.display()))?, false),
                None => {
                    let data = read_stdin("label rows", "--file FILE")?;
                    let tmpdir = ctx.data_dir.join("tmp");
                    std::fs::create_dir_all(&tmpdir)
                        .with_context(|| format!("cannot create {} (is --data-dir right?)", tmpdir.display()))?;
                    let p = tmpdir.join(format!("append-{}-{}.in", std::process::id(), util::now_ms()));
                    let mut f = std::fs::File::create(&p)?;
                    f.write_all(data.as_bytes())?;
                    f.sync_all()?;
                    (p, true)
                }
            };
            Request::Append { path: path.to_string_lossy().into_owned(), delete_after, agent, source_key, label_source, chain, observed_at, format: fmt_name(format) }
        }
        Cmd::Export { chain, out_dir } => Request::Export {
            chain,
            out_dir: out_dir.map(|p| std::path::absolute(&p).unwrap_or(p).to_string_lossy().into_owned()),
        },
        Cmd::Status { source_key } => Request::Status { source_key },
        Cmd::Claim { source_key, source_url, source_hash: h, source_file, ttl_secs, auto_renew } => {
            Request::Claim { source_key, agent, source_url, source_hash: source_hash(h, source_file)?, ttl_secs, auto_renew }
        }
        Cmd::Renew { source_key } => Request::Renew { source_key, agent },
        Cmd::Done { source_key, source_hash: h, source_file } => Request::Done { source_key, agent, source_hash: source_hash(h, source_file)? },
        Cmd::Release { source_key } => Request::Release { source_key, agent },
        Cmd::Sync { action } => Request::Sync { action: format!("{action:?}").to_lowercase() },
        Cmd::Howto { cmd: HowtoCmd::Put { source_key, file, base } } => {
            let content = match file {
                Some(f) => std::fs::read_to_string(&f).with_context(|| format!("cannot read {}", f.display()))?,
                None => read_stdin("playbook markdown", "--file FILE")?,
            };
            Request::HowtoPut { source_key, content, agent, base }
        }
        Cmd::Howto { cmd: HowtoCmd::Get { source_key, version } } => Request::HowtoGet { source_key, version },
        Cmd::Howto { cmd: HowtoCmd::Log { source_key } } => Request::HowtoLog { source_key },
        Cmd::Report { source_key, json, rows_total, rows_new, duration_s, errors, retries, method, howto_version, notes, started_at } => {
            let mut rec: serde_json::Map<String, Value> = match json {
                Some(spec) => match serde_json::from_str(&read_arg_file(&spec, "a JSON run record")?)
                    .map_err(|e| cli_err("bad_input", EXIT_USAGE, format!("--json: not valid JSON: {e}"), Some("pass one JSON object, e.g. '{\"duration_s\":60}'")))?
                {
                    Value::Object(m) => m,
                    _ => return Err(cli_err("bad_input", EXIT_USAGE, "--json must contain a JSON object", None)),
                },
                None => Default::default(),
            };
            if let Some(v) = rec.remove("rows_new") {
                rec.entry("rows_new_after_dedup").or_insert(v);
            }
            let mut set = |k: &str, v: Option<Value>| {
                if let Some(v) = v {
                    rec.insert(k.into(), v);
                }
            };
            set("rows_total", rows_total.map(|x| json!(x)));
            set("rows_new_after_dedup", rows_new.map(|x| json!(x)));
            set("duration_s", duration_s.map(|x| json!(x)));
            set("errors", errors.map(|x| json!(x)));
            set("retries", retries.map(|x| json!(x)));
            set("method", method.map(|x| json!(x)));
            set("howto_version", howto_version.map(|x| json!(x)));
            set("notes", notes.map(|x| json!(x)));
            set("started_at", started_at.map(|x| json!(x)));
            Request::Report { source_key, agent, record: Value::Object(rec) }
        }
        Cmd::Next { sources, claim, auto_renew, ttl_secs } => {
            let sources = match sources {
                Some(spec) => Some(read_arg_file(&spec, "source keys")?.lines().map(|l| l.to_string()).collect()),
                None => None,
            };
            Request::Next { sources, claim, agent, ttl_secs, auto_renew }
        }
        Cmd::Queue { cmd: QueueCmd::Add { mut items, file, source_url } } => {
            if let Some(spec) = file {
                items.extend(read_arg_file(&spec, "source keys")?.lines().map(|l| l.to_string()));
            }
            if items.iter().all(|i| i.trim().is_empty()) {
                return Err(cli_err("bad_input", EXIT_USAGE, "no source keys given", Some("labelsync queue add SRC... or --file FILE")));
            }
            Request::QueueAdd { items, source_url, agent }
        }
        Cmd::Queue { cmd: QueueCmd::Rm { items } } => Request::QueueRm { items, agent },
        Cmd::Queue { cmd: QueueCmd::List } => Request::QueueList,
        Cmd::Stop => Request::Shutdown,
    };
    let v = call(&ctx, &req)?;
    Ok(emit(&ctx, v))
}

fn main() -> ExitCode {
    let matches = match Cli::command().try_get_matches() {
        Ok(m) => m,
        Err(e) => {
            use clap::error::ErrorKind::*;
            let code = match e.kind() {
                DisplayHelp | DisplayVersion => EXIT_OK,
                _ => EXIT_USAGE, // includes no-args help (DisplayHelpOnMissingArgumentOrSubcommand)
            };
            let _ = e.print(); // help/version -> stdout, errors -> stderr
            return ExitCode::from(code);
        }
    };
    let cli = match Cli::from_arg_matches(&matches) {
        Ok(c) => c,
        Err(e) => {
            let _ = e.print();
            return ExitCode::from(EXIT_USAGE);
        }
    };
    let output = cli.output;
    let quiet = cli.quiet;
    match run(cli, &matches) {
        Ok(c) => ExitCode::from(c),
        Err(e) if e.downcast_ref::<std::io::Error>().is_some_and(|e| e.kind() == std::io::ErrorKind::BrokenPipe) => {
            ExitCode::from(EXIT_OK) // reader went away (e.g. `| head`); nothing useful to say
        }
        Err(e) => {
            let (code, exit, hint) = match e.downcast_ref::<CliError>() {
                Some(c) => (c.code, c.exit, c.hint.clone()),
                None => ("error", EXIT_ERROR, None),
            };
            let ctx = Ctx { data_dir: PathBuf::new(), socket: PathBuf::new(), agent: None, output, timeout: Duration::ZERO, quiet };
            let v = json!({"ok": false, "error": format!("{e:#}"), "code": code, "hint": hint, "exit": exit});
            ExitCode::from(emit(&ctx, v))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_code_mapping() {
        assert_eq!(exit_code_for(&json!({"ok": true})), EXIT_OK);
        assert_eq!(exit_code_for(&json!({"ok": true, "result": "won"})), EXIT_OK);
        assert_eq!(exit_code_for(&json!({"ok": true, "result": "candidate"})), EXIT_OK);
        assert_eq!(exit_code_for(&json!({"ok": true, "result": "lost"})), EXIT_LOST);
        assert_eq!(exit_code_for(&json!({"ok": true, "result": "skip"})), EXIT_SKIP);
        assert_eq!(exit_code_for(&json!({"ok": true, "result": "not_claimed"})), EXIT_NOT_CLAIMED);
        assert_eq!(exit_code_for(&json!({"ok": true, "result": "none"})), EXIT_NONE);
        assert_eq!(exit_code_for(&json!({"ok": false, "error": "x"})), EXIT_ERROR);
        assert_eq!(exit_code_for(&json!({"ok": false, "error": "x", "exit": 64})), EXIT_USAGE);
    }

    #[test]
    fn clap_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn text_renderer() {
        let t = render_text(&json!({"ok": true, "a": 1, "content": "x\ny", "n": null}));
        assert_eq!(t, "a: 1\ncontent:\nx\ny\n");
    }
}
