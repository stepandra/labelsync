# labelsync design

## Goal

Several agents on different machines collect public address-label datasets in parallel. Their label
databases must converge and they should not mine the same source twice. Labels are append-mostly,
can reach millions of rows, and need provenance.

## Stack

iroh 1.3 endpoint (n0 preset: DNS/pkarr discovery and relays) + iroh-gossip 0.101 + iroh-blobs 0.103
(fs store) + iroh-docs 0.101 (persistent). One namespace (doc) is shared by all nodes; joining is done
with a `DocTicket`. Automerge is **not** used: per-row CRDT history does not scale to millions of rows,
and labels do not need concurrent editing.

## Keys in the namespace

| key | value (blob) | writer |
|---|---|---|
| `seg/<chain>/<enc(source_key)>/<blake3>` | zstd(canonical TSV) | the appending agent's author |
| `claim/<enc(source_key)>/` | JSON `ClaimRecord {agent, claimed_at, lease_until, source_url, source_hash, released, ttl_ms}` | one entry per author |
| `done/<enc(source_key)>/` | JSON `DoneRecord {agent, source_key, source_content_hash, segments[], finished_at}` | the finishing author |
| `src/<blake3 of raw source file>` | JSON `{agent, source_key, finished_at}` | the finishing author |
| `agent/<author hex>` | JSON `{name, node_id, created_at}` | that author (maps author -> agent name) |

| `howto/<enc(source_key)>/` | Automerge `save()` of the whole playbook doc (legacy: plain md) | one entry per author, each holding that author's full merged doc |
| `runs/<enc(source_key)>/<enc(agent)>/<ts_ms>/` | JSON run record | the reporting author; immutable (new key per run) |
| `queue/<enc(source_key)>/` | JSON `{agent, added_at, source_url, removed}` | one entry per author; newest entry across authors decides membership |

`enc` = percent-encoding of everything outside `[A-Za-z0-9._~:@+=,-]` (so no `/`). `claim/` and `done/` keys end
with `/` because iroh-docs deletes same-author entries whose key *starts with* a newly written key (prefix delete).
Without the terminator `claim/foo` would wipe `claim/foobar`.

## Segments

* Input (TSV/CSV/JSONL) is normalized per row: chain lowercased, EVM `0x…40hex` and bech32 lowercased, base58 kept,
  tabs/newlines collapsed, `op` mapped to `add|del`, `label_source` defaulted to the source key.
* Rows are grouped by source key and chain, **sorted and deduplicated**, chunked at 50k rows (configurable),
  and serialized as TSV with the fixed header
  `chain address label label_source category op observed_at`.
* Segment id = **BLAKE3 of the uncompressed canonical TSV**. The same data gives the same key whichever agent produced
  it and whichever zstd version compressed it. The stored blob is the zstd-compressed TSV (level 3); readers verify the hash
  after decompressing (and cap decompressed size at 512 MiB).
* Agent identity is not inside the segment: it is the iroh-docs author of the entry. Two agents ingesting identical
  data produce two entries pointing at the same blob.

## Materialization (SQLite, per node)

A materializer runs on every doc event (debounced 150 ms) and every 10 s. It lists `seg/` entries, skips
`(key, author)` pairs already in `segments`, fetches blobs that are present (missing ones count as `pending_blobs` and are
retried), verifies them, and inserts them in one transaction per segment:

* `labels` with UNIQUE `(chain, address, label_source, label)`: the label identity
* `provenance(label_id, author_id, segment_id, op, category, observed_at, eff_at)`: one row per assertion
* `active_labels` view: a label is active if for at least one author that author's latest assertion (by `eff_at`,
  `del` wins ties) is `add`. LWW is therefore per (label, author): a tombstone only revokes your own assertion, and
  conflicting labels from different agents all survive with provenance.

Re-ingesting the same rows is a no-op (unique constraints + `INSERT OR IGNORE`).

## Claims (advisory leases)

1. `claim K`: if any `done/K/` exists → **skip**. If `--source-hash H` and `src/H` exists → **skip**.
2. Read all `claim/K/` entries (waiting up to 5 s for freshly synced content). If another live claim is visible and we hold
   none → **lost** without writing. Otherwise write/refresh our record (`claimed_at` stays stable across renewals,
   `lease_until = now + ttl`).
3. Winner = live claim (`!released && lease_until > now`) with minimal `(claimed_at, author_id)`. Every node computes the same
   function over the same merged set, so they converge once entries have synced.
4. `renew K`: if we are still the winner, extend; if we lost after a merge, write `released=true` (step back) and return **lost**.
   `--auto-renew` makes the daemon renew every TTL/3.
5. `done K`: writes `done/K/` (listing all `seg/` keys of K) and `src/H`, and releases our claim. `done` overrides any claim.

There is no CAS in iroh-docs, so this is advisory. Under a partition both sides can win and mine the same source; the
result is duplicate *work*, not duplicate *data* (identical segments share a hash; different chunks dedup in SQLite).
At most one lease window is wasted. A strict lock would need a single coordinator node (e.g. an irpc `claim` method on an
always-on node); this is not built.

## Experience sharing (phase 1.5)

* **Playbooks** are Automerge 0.12 documents (one per source, root key `text` = Text, unicode-code-point indexing).
  * Transport: each author writes its **full `save()`** of the merged doc under its own `howto/<src>/` entry. Readers load
    every author's save and `merge` them. Chosen over per-change append logs because it needs no new key scheme, no ordering
    or gap handling, and a single entry per author can never be "missing a chunk": any one save is a complete doc. Cost:
    each author entry holds the full history (columnar-compressed; playbooks are KB-sized), and every put leaves the previous
    blob orphaned in the blob store (no GC). Identical changes in several saves dedupe by change hash.
  * Genesis: every node starts from the same deterministic change (fixed actor, time 0, creates `text`). Without it, two
    nodes creating `text` independently would conflict and one text would be hidden.
  * `put`: fork at `--base` heads (default: current), diff base text → new md with `dissimilar` (char diff), apply as
    `splice_text` calls in one change, merge into the current doc, save, write. Concurrent edits made after the base are
    kept. Same-span conflicts are resolved by Automerge's deterministic ordering (both inserts survive, both deletes apply);
    every node converges on identical text.
  * Actor per put = author id (32 B) ‖ now_ms (8 B): a fresh actor per change, so editing from an older base never reuses
    a sequence number. `howto log` maps the actor back to the author/agent.
  * Version id = sorted change-hash heads, comma-joined. `report --howto-version` records it; `next` renders
    `playbook.best` at those heads (`text_at`). Unknown/legacy version ids fall back to latest with a `note`.
  * Migration: phase 1.5 per-author plain-md entries are converted in memory by a deterministic change (fixed actor derived
    from source key + content, time 0) applied on genesis, using the newest legacy entry, only while no Automerge save exists.
    The first `put` stores the converted doc. Edge: two nodes that convert *different* newest legacy versions concurrently
    would both insert their text (duplication, no loss).
  * Mirrored into SQLite `playbooks(source_key, version, content, ...)`, recomputed only when the set of author entries changes.
* **Run records** are immutable JSON: `rows_total`, `rows_new_after_dedup`, `duration_s`, `errors`, `retries`, `method`,
  `howto_version`, `notes`, plus `segments`, `started_at`, `ts`, `agent`. If rows are not given, `report` computes them from
  the reporter's own segments for that source written since the run start (claim time, `--started-at`, or now - duration);
  "new" = labels that have no provenance from any segment outside that set.
* All three are mirrored into SQLite (`howto`, `runs`, `queue`) on every materializer pass; `source_stats` and
  `method_stats` views aggregate runs.
* **Scoring** (`score.rs`): new labels/min = Σrows_new / Σduration × 60; error rate = errors per run;
  expected yield = new/min ÷ (1 + error rate). Sources with no usable history get `max(known yield) × 1.25 + 1`
  (exploration bonus). Ties break by source key.
* **`next`**: candidates = `--sources` list or the shared queue; drop sources with a `done/` record or a live claim won by
  someone else; rank; without `--claim` return the best; with `--claim` run the normal claim on each in rank order until one
  is won. Concurrent `next --claim` can still collide (advisory claims); after sync the loser's `renew` returns lost and its
  next `next --claim` skips the taken source. `src/<hash>` dedup is not applied in `next` because queue entries carry no
  raw-file hash.

## Sync

* `join` imports the ticket's capability and stores the ticket's peer addresses (not the capability) in `state.json`.
* `start_sync` in iroh-docs does not retry on failure, so a sync loop re-calls it for every known peer (ticket peers +
  iroh-docs' stored sync peers + gossip neighbours) every `--sync-interval` (30 s), with exponential backoff per peer on errors
  (2 s … 120 s). Live changes also arrive via gossip immediately.
* iroh-docs 0.101 often reports `Failed to close connection` after a sync whose reconciliation already finished
  (the final `read_to_end` races with the remote closing). Entries are applied during the exchange, so labelsync treats this as
  success for backoff purposes and the default log filter hides iroh_docs warnings.
* `sync pause` leaves the doc's live sync and refuses incoming docs-ALPN connections (a gate wrapped around the Docs protocol
  handler). `resume` restarts sync. This is what the partition test uses.

## IPC

Unix socket at `<data-dir>/labelsync.sock` (0600), one JSON request line → one JSON response line per connection.
`append` from stdin is spooled by the CLI to `<data-dir>/tmp/` and handed to the daemon by path (no size limit on the socket).
Windows named pipe: TODO.

## Security notes

* Write tickets are non-revocable bearer secrets: `share --out` writes 0600, the daemon never logs them. The data dir is 0700,
  `secret.key` and `state.json` are 0600. The namespace secret itself is stored inside `iroh/docs/docs.redb`.
* Anyone with write access can write arbitrary entries (e.g. fake `done/` records). The trust model is "all ticket holders
  are cooperating agents".

## Known gaps / TODO

* Windows named-pipe IPC.
* No compaction: re-appending the same source with slightly different data creates new segments; old ones stay.
  Merging small segments (phase 2) is not done.
* Materialization scans every `seg/` entry each pass (fine up to many thousands of segments; an index by entry timestamp
  would be needed beyond that).
* `active_labels` uses correlated subqueries; export of 1M labels takes ~5 s.
* No per-chain namespaces, no read-only enforcement of SQLite beyond documentation, no metrics endpoint.
* Phase 3 (generic md/csv file sync) not started.
* Experience: scoring has no time decay, run records can be faked by any ticket holder, `next` does not consult `src/`
  hashes, playbook blobs are never compacted/GC'd, no `howto edit` ($EDITOR) helper.

## CLI layer (v0.2 audit against the engineering-cli-tools standard)

`src/main.rs` is the only process boundary: clap parses (help/version/usage errors never touch the daemon or
disk), commands become one `ipc::Request`, and `emit` renders the daemon's JSON either as the stable JSON
contract (default) or as `key: value` text, writing error message + hint to stderr. Exit codes are mapped in one
place (`exit_code_for`, unit-tested); usage errors exit 64 so they can't be confused with "lost" (2). The
client waits at most `--timeout` for the daemon. `capabilities` is generated from the same clap metadata as
help, completions and the man page, so they can't drift. Contract tests: `tests/cli.rs`.
