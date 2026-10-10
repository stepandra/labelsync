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
