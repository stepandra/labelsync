# labelsync

Sync crypto address-label datasets between independent agents (label hound, other bots,
scripts in any language) over [iroh](https://iroh.computer), and keep them from mining the
same source twice.

* One binary: `labelsync daemon` plus a small CLI. Every CLI command prints **one JSON line** on stdout.
* Labels travel as immutable, content-addressed, zstd-compressed TSV **segments** in an
  iroh-docs namespace; each node materializes them into a local **SQLite** DB and can export per-chain TSV.
* Work coordination: **advisory leases** (`claim` / `renew` / `done` / `release`) stored in the same namespace.
  Duplicate work under a network partition is harmless: rows are deduplicated.

See [DESIGN.md](DESIGN.md) for the internals.

## Build

```sh
# Rust >= 1.91
cargo build --release            # -> target/release/labelsync (glibc, ~22 MB stripped)
# fully static Linux binary (needs musl-tools):
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

Copy the single binary to every machine that runs agents.

## Quick start

```sh
# machine A
labelsync --agent hound-a daemon &              # data in ~/.labelsync (override: --data-dir / LABELSYNC_DIR)
labelsync share --out ~/labelsync.ticket        # write ticket, file mode 0600

# copy the ticket file to machine B over a trusted channel, then:
labelsync --agent hound-b daemon &
labelsync join @labelsync.ticket                # or: labelsync join <ticket>   or: ... join - < file
```

**The write ticket is a bearer secret that cannot be revoked.** Anyone holding it can write to the
namespace forever. Keep it in a 0600 file and never paste it in logs or chats. Use `share --read-only`
for consumers that only need to read.

`share --addr all|relay|direct|id` controls the addressing info inside the ticket (default `all`).
Peers find each other through n0's DNS/pkarr discovery and relays, so NAT'd machines work.

## Agent workflow (any language)

```sh
SRC="etherscan:labelcloud:exchange:page=3"      # any string up to 512 bytes; stable per source
labelsync claim "$SRC" --source-url "https://…" # exit 0 = won, 2 = lost, 3 = skip (already done / same file seen)
case $? in
  0) ;;                 # go mine it
  2|3) exit 0 ;;        # someone else has it / it is done
  *) exit 1 ;;          # daemon error
esac

# while mining, every TTL/3 (default TTL 45 min -> every 15 min):
labelsync renew "$SRC"   # exit 0 keep going; 2 = you lost after a merge, stop at the next checkpoint
# (or: labelsync claim "$SRC" --auto-renew   and the daemon renews until done/release)

# push results (as often as you like; appends are idempotent)
labelsync append --source-key "$SRC" < labels.tsv

# finished: mark done (overrides every claim). With --source-file the BLAKE3 of the raw
# downloaded file is recorded too, so the same file under another URL/key is skipped.
labelsync done "$SRC" --source-file raw_download.csv

# gave up without finishing:
labelsync release "$SRC"
```

Exit codes: `0` ok / won, `1` error (`{"ok":false,"error":"…"}`), `2` lost, `3` skip, `4` not claimed (renew).

Act as a different agent with `--agent NAME` or `LABELSYNC_AGENT`. Each agent name gets its own iroh-docs
author (signing key), so several agents can share one daemon and still compete fairly for claims.

Python:

```python
import json, subprocess
def ls(*args, stdin=None):
    p = subprocess.run(["labelsync", *args], input=stdin, capture_output=True, text=True)
    return p.returncode, json.loads(p.stdout)

code, r = ls("claim", "arkham:entity:binance")
if code == 0:
    rows = "chain\taddress\tlabel\tcategory\neth\t0x28C6c06298d514Db089934071355E5743bf21d60\tBinance 14\texchange\n"
    ls("append", "--source-key", "arkham:entity:binance", stdin=rows)
    ls("done", "arkham:entity:binance")
```

Check a source before claiming: `labelsync status --source-key "$SRC"` (shows `done`, all claims and the current winner).

## Input format

### TSV (preferred)

Header row required. Columns (case-insensitive; aliases in parentheses):

| column | required | notes |
|---|---|---|
| `chain` (`network`, `blockchain`) | yes, unless `--chain` / `# chain=` | lowercased, `[a-z0-9._-]` |
| `address` (`addr`) | yes | EVM `0x…` and bech32 are lowercased; base58 (BTC legacy, Solana, Tron…) kept as-is |
| `label` (`name`, `tag`) | yes | |
| `label_source` (`source`, `label_src`) | no | defaults to the source key |
| `category` (`type`, `label_type`) | no | e.g. `exchange`, `mixer`, `scam` |
| `op` (`action`) | no | `add` (default) or `del` (tombstone that revokes **your own** earlier `add`) |
| `observed_at` (`ts`, `timestamp`, `seen_at`) | no | unix s / ms or RFC3339; default = time the segment was written |
| `source_key` | no | per-row source key (overrides `--source-key`) |

Tabs/newlines inside fields are turned into spaces. Rows missing `address`/`label`/`chain` are rejected and
reported in `"errors"` (first 20) and `"rejected"` (count); the rest are accepted.

Leading directive lines are allowed (CLI flags win over them):

```
# source_key=dune:query:123456
# chain=eth
# label_source=dune
# agent=hound-a
address	label	category
0xdAC17F958D2ee523a2206206994597C13D831ec7	Tether USDT	token
```

### CSV

Same columns, comma separated, standard quoting (`--format csv` or `.csv` extension in inbox).

### JSONL

One object per line with the same keys:

```json
{"chain":"sol","address":"So11111111111111111111111111111111111111112","label":"Wrapped SOL","category":"token"}
```

## Inbox (no CLI needed)

Drop files into `<data-dir>/inbox/`. **Write them atomically**: write `name.tsv.tmp` (or a dotfile) and then
`rename` to `name.tsv`; files starting with `.` or ending in `.tmp` / `.part` are ignored.

* source key: `# source_key=` directive, a `source_key` column, or else the file name without extension
* format: by extension (`.tsv`, `.csv`, `.jsonl`/`.ndjson`), otherwise auto-detected
* on success the file goes to `processed/<ms>-<name>` with a `…result.json` next to it;
  on failure to `failed/<ms>-<name>` with `…error.txt`
* the directory is watched (debounced 400 ms) and also rescanned every 15 s

## Reading the data

* SQLite: `<data-dir>/labels.db`. The daemon is the only writer; open it **read-only**:
  `sqlite3 'file:/home/me/.labelsync/labels.db?mode=ro' "select * from active_labels where address='0x…'"`.
  * `active_labels(id, chain, address, label_source, label, category, agents, first_seen, last_seen)`: what you normally want
  * `labels`: every label identity ever seen, revoked ones included
  * `provenance_v(chain, address, label_source, label, op, category, agent, author, source_key, observed_at, eff_at, segment)`: who said what, when
  * `segments`: every materialized segment
* TSV export: `labelsync export [--chain eth] [--out-dir DIR]` writes `DIR/<chain>.tsv` atomically
  (temp file + fsync + rename). Default DIR is `<data-dir>/export/`. `daemon --auto-export` re-exports after every
  change (fine for small sets, costly for millions of rows).

  Export columns: `chain address label label_source category agents first_seen last_seen`
  (`agents` = comma-separated agents that assert the label, times in RFC3339 UTC).

## Commands

| command | does |
|---|---|
| `daemon [--ttl-secs 2700] [--sync-interval 30] [--segment-rows 50000] [--start-paused] [--auto-export]` | run the node in the foreground (logs to stderr, `LABELSYNC_LOG` = tracing filter) |
| `share [--read-only] [--addr all\|relay\|direct\|id] [--out FILE]` | print or save a ticket |
| `join <ticket\|@file\|->  [--force]` | join a namespace (refuses to abandon a namespace that already holds data unless `--force`) |
| `append [--source-key K] [--chain C] [--label-source S] [--observed-at T] [--format auto\|tsv\|csv\|jsonl] [--file F]` | ingest from stdin or a file |
| `export [--chain C] [--out-dir D]` | per-chain TSV |
| `status [--source-key K]` | node id, namespace, peers, DB counts, active claims (or one source's claims/done) |
| `claim K [--source-url U] [--source-hash H \| --source-file F] [--ttl-secs N] [--auto-renew]` | take a lease |
| `renew K` | extend your lease; steps back (releases) if you lost after a merge |
| `done K [--source-hash H \| --source-file F]` | mark finished; lists the source's segments |
| `release K` | drop your lease |
| `sync pause\|resume\|now` | stop / restart network sync (pause also refuses incoming sync connections) |
| `hash FILE` | BLAKE3 of a file (what `--source-file` computes) |
| `stop` | ask the daemon to exit |

Global flags / env: `--data-dir` (`LABELSYNC_DIR`, default `~/.labelsync`), `--socket` (`LABELSYNC_SOCKET`,
default `<data-dir>/labelsync.sock`; keep the path under ~100 chars), `--agent` (`LABELSYNC_AGENT`, default: hostname).

## Data directory

```
labels.db            SQLite materialization (read-only for agents)
labelsync.sock       CLI socket (0600)
secret.key           iroh node key (0600)
state.json           namespace id, peers, agent->author map (0600, no tickets)
iroh/blobs, iroh/docs  iroh stores (the namespace write capability lives in iroh/docs: protect it like the ticket)
inbox/ processed/ failed/ export/ tmp/
```

## Clocks

Leases use wall-clock milliseconds and iroh-docs rejects entries more than 10 minutes in the future, so keep
machines NTP-synced. The lease TTL (default 45 min) must stay far above any clock skew.

## Platform status

Linux: built and tested. macOS: should work (Unix socket), not tested. Windows: the daemon IPC is Unix-socket
only, a named-pipe transport is TODO, so on Windows run it under WSL for now.
