//! The labelsync daemon: iroh endpoint + blobs + gossip + docs, IPC server,
//! segment materializer, sync retry loop, inbox watcher and lease auto-renewal.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use futures_lite::StreamExt;
use iroh::{
    Endpoint, EndpointAddr, PublicKey, SecretKey,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_blobs::{BlobsProtocol, store::fs::FsStore};
use iroh_docs::{
    AuthorId, DocTicket, Entry, NamespaceId,
    api::{
        Doc,
        protocol::{AddrInfoOptions, ShareMode},
    },
    engine::LiveEvent,
    protocol::Docs,
    store::Query,
};
use iroh_gossip::net::Gossip;
use iroh_tickets::Ticket;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::{debug, info, warn};

mod experience;

use crate::{
    claims::{self, ClaimRecord, DoneRecord, Outcome, SrcRecord},
    db, ipc::Request, keys, segment, util,
};

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    pub socket: PathBuf,
    pub agent: String,
    pub ttl_secs: u64,
    pub sync_interval_secs: u64,
    pub segment_rows: usize,
    pub start_paused: bool,
    pub auto_export: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct State {
    namespace: Option<String>,
    /// peers learned from tickets (id + addressing hints). No secrets in here.
    #[serde(default)]
    peers: Vec<EndpointAddr>,
    /// agent name -> author id (hex). The daemon's default agent uses the default author.
    #[serde(default)]
    agents: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct PeerState {
    fails: u32,
    #[serde(skip)]
    next: Option<Instant>,
    last_ok_ms: Option<u64>,
    last_err: Option<String>,
}

/// Wraps a protocol handler so incoming connections can be refused while sync is paused.
#[derive(Debug, Clone)]
struct Gated<P> {
    inner: P,
    paused: Arc<AtomicBool>,
}

impl<P: ProtocolHandler + Clone> ProtocolHandler for Gated<P> {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        if self.paused.load(Ordering::SeqCst) {
            conn.close(1u32.into(), b"labelsync: sync paused");
            return Ok(());
        }
        self.inner.accept(conn).await
    }
    async fn shutdown(&self) {
        self.inner.shutdown().await
    }
}

pub struct Node {
    cfg: Config,
    endpoint: Endpoint,
    router: Router,
    blobs: FsStore,
    docs: Docs,
    doc: RwLock<Option<Doc>>,
    doc_gen: AtomicU64,
    state: Mutex<State>,
    db: Arc<std::sync::Mutex<rusqlite::Connection>>,
    paused: Arc<AtomicBool>,
    claim_lock: Mutex<()>,
    ingest_lock: Mutex<()>,
    materialize_lock: Mutex<()>,
    materialize_notify: Notify,
    sync_notify: Notify,
    peers: Mutex<HashMap<PublicKey, PeerState>>,
    auto_renew: Mutex<BTreeMap<(String, String), Instant>>,
    agents_published: Mutex<BTreeSet<(NamespaceId, AuthorId)>>,
    default_author: AuthorId,
    shutdown: Notify,
    pending_blobs: AtomicU64,
}

fn mkdir_private(p: &Path) -> Result<()> {
    std::fs::create_dir_all(p)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn load_or_create_secret(path: &Path) -> Result<SecretKey> {
    if let Ok(s) = std::fs::read_to_string(path) {
        let bytes = hex::decode(s.trim()).context("secret.key is not hex")?;
        let arr: [u8; 32] = bytes.try_into().map_err(|_| anyhow!("secret.key wrong length"))?;
        return Ok(SecretKey::from_bytes(&arr));
    }
    let sk = SecretKey::generate();
    util::atomic_write(path, hex::encode(sk.to_bytes()).as_bytes(), Some(0o600))?;
    Ok(sk)
}

impl Node {
    fn state_path(&self) -> PathBuf {
        self.cfg.data_dir.join("state.json")
    }

    async fn save_state(&self, st: &State) -> Result<()> {
        let data = serde_json::to_vec_pretty(st)?;
        util::atomic_write(&self.state_path(), &data, Some(0o600))?;
        Ok(())
    }

    pub async fn spawn(cfg: Config) -> Result<Arc<Self>> {
        mkdir_private(&cfg.data_dir)?;
        for d in ["inbox", "processed", "failed", "export", "tmp", "iroh/blobs", "iroh/docs"] {
            std::fs::create_dir_all(cfg.data_dir.join(d))?;
        }
        let state: State = std::fs::read(cfg.data_dir.join("state.json"))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();

        let sk = load_or_create_secret(&cfg.data_dir.join("secret.key"))?;
        let endpoint = Endpoint::builder(presets::N0).secret_key(sk).bind().await?;
        let blobs = FsStore::load(cfg.data_dir.join("iroh/blobs")).await?;
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let docs = Docs::persistent(cfg.data_dir.join("iroh/docs"))
            .spawn(endpoint.clone(), (*blobs).clone(), gossip.clone())
            .await?;
        let paused = Arc::new(AtomicBool::new(cfg.start_paused));
        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, BlobsProtocol::new(&blobs, None))
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(iroh_docs::ALPN, Gated { inner: docs.clone(), paused: paused.clone() })
            .spawn();
        let default_author = docs.author_default().await?;
        let conn = db::open(&cfg.data_dir.join("labels.db"))?;

        let node = Arc::new(Node {
            cfg,
            endpoint,
            router,
            blobs,
            docs,
            doc: RwLock::new(None),
            doc_gen: AtomicU64::new(0),
            state: Mutex::new(state.clone()),
            db: Arc::new(std::sync::Mutex::new(conn)),
            paused,
            claim_lock: Mutex::new(()),
            ingest_lock: Mutex::new(()),
            materialize_lock: Mutex::new(()),
            materialize_notify: Notify::new(),
            sync_notify: Notify::new(),
            peers: Mutex::new(HashMap::new()),
            auto_renew: Mutex::new(BTreeMap::new()),
            agents_published: Mutex::new(BTreeSet::new()),
            default_author,
            shutdown: Notify::new(),
            pending_blobs: AtomicU64::new(0),
        });

        if let Some(ns) = &state.namespace {
            let id = NamespaceId::from_str(ns)?;
            match node.docs.open(id).await? {
                Some(doc) => node.set_doc(doc).await?,
                None => warn!("namespace {ns} from state.json not found in docs store"),
            }
        }
        info!(node_id = %node.endpoint.id(), author = %default_author, agent = %node.cfg.agent, "labelsync daemon started");
        Ok(node)
    }

    async fn set_doc(self: &Arc<Self>, doc: Doc) -> Result<()> {
        let generation = self.doc_gen.fetch_add(1, Ordering::SeqCst) + 1;
        let events = doc.subscribe().await?;
        *self.doc.write().await = Some(doc.clone());
        let this = self.clone();
        tokio::spawn(async move { this.event_loop(events, generation).await });
        if !self.paused.load(Ordering::SeqCst) {
            let peers = self.state.lock().await.peers.clone();
            if let Err(e) = doc.start_sync(peers).await {
                warn!("start_sync failed: {e:#}");
            }
        }
        self.materialize_notify.notify_one();
        self.sync_notify.notify_one();
        Ok(())
    }

    async fn current_doc(&self) -> Option<Doc> {
        self.doc.read().await.clone()
    }

    /// The doc, creating a fresh namespace if none exists yet.
    async fn doc(self: &Arc<Self>) -> Result<Doc> {
        if let Some(d) = self.current_doc().await {
            return Ok(d);
        }
        let doc = self.docs.create().await?;
        {
            let mut st = self.state.lock().await;
            st.namespace = Some(doc.id().to_string());
            self.save_state(&st).await?;
        }
        info!(namespace = %doc.id(), "created new namespace");
        self.set_doc(doc.clone()).await?;
        Ok(doc)
    }

    async fn event_loop(
        self: Arc<Self>,
        mut events: impl futures_lite::Stream<Item = Result<LiveEvent>> + Unpin,
        generation: u64,
    ) {
        while let Some(ev) = events.next().await {
            if self.doc_gen.load(Ordering::SeqCst) != generation {
                break;
            }
            let ev = match ev {
                Ok(ev) => ev,
                Err(e) => {
                    warn!("event stream error: {e:#}");
                    continue;
                }
            };
            match ev {
                LiveEvent::InsertRemote { .. }
                | LiveEvent::ContentReady { .. }
                | LiveEvent::PendingContentReady
                | LiveEvent::InsertLocal { .. } => self.materialize_notify.notify_one(),
                LiveEvent::SyncFinished(se) => {
                    let mut peers = self.peers.lock().await;
                    let p = peers.entry(se.peer).or_default();
                    // iroh-docs 0.101 reports "Failed to close connection" when the
                    // reconciliation itself completed but the final stream shutdown raced
                    // (read_to_end on an already-finished stream). Entries are applied
                    // during the exchange, so treat it as success for backoff purposes.
                    let result = match &se.result {
                        Err(e) if e.starts_with("Failed to close") => Ok(None),
                        Ok(d) => Ok(Some(d.entries_received)),
                        Err(e) => Err(e.clone()),
                    };
                    match &result {
                        Ok(d) => {
                            debug!(peer = %se.peer.fmt_short(), received = ?d, "sync finished");
                            p.fails = 0;
                            p.last_ok_ms = Some(util::now_ms());
                            p.last_err = None;
                            p.next = Some(Instant::now() + Duration::from_secs(self.cfg.sync_interval_secs));
                        }
                        Err(e) => {
                            p.fails += 1;
                            p.last_err = Some(e.clone());
                            let backoff = Duration::from_secs((1u64 << p.fails.min(7)).min(120));
                            p.next = Some(Instant::now() + backoff);
                            info!(peer = %se.peer.fmt_short(), fails = p.fails, ?backoff, "sync failed: {e}");
                        }
                    }
                    self.materialize_notify.notify_one();
                }
                LiveEvent::NeighborUp(pk) => {
                    debug!(peer = %pk.fmt_short(), "neighbor up");
                    self.peers.lock().await.entry(pk).or_default();
                }
                LiveEvent::NeighborDown(pk) => debug!(peer = %pk.fmt_short(), "neighbor down"),
            }
        }
        debug!("event loop {generation} ended");
    }

    /// Periodically (re)start sync with every known peer, honoring per-peer backoff.
    /// iroh-docs `start_sync` does not retry on its own.
    async fn sync_loop(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                _ = self.sync_notify.notified() => {}
            }
            if self.paused.load(Ordering::SeqCst) {
                continue;
            }
            let Some(doc) = self.current_doc().await else { continue };
            let mut addrs: HashMap<PublicKey, EndpointAddr> = HashMap::new();
            for a in self.state.lock().await.peers.iter() {
                addrs.insert(a.id, a.clone());
            }
            if let Ok(Some(known)) = doc.get_sync_peers().await {
                for b in known {
                    if let Ok(pk) = PublicKey::from_bytes(&b) {
                        addrs.entry(pk).or_insert_with(|| EndpointAddr::new(pk));
                    }
                }
            }
            for pk in self.peers.lock().await.keys() {
                addrs.entry(*pk).or_insert_with(|| EndpointAddr::new(*pk));
            }
            addrs.remove(&self.endpoint.id());
            let now = Instant::now();
            let mut due = Vec::new();
            {
                let mut peers = self.peers.lock().await;
                for (pk, addr) in addrs {
                    let p = peers.entry(pk).or_default();
                    if p.next.map(|n| n <= now).unwrap_or(true) {
                        // provisional: SyncFinished will set the real next time
                        p.next = Some(now + Duration::from_secs(self.cfg.sync_interval_secs));
                        due.push(addr);
                    }
                }
            }
            if due.is_empty() {
                continue;
            }
            debug!(n = due.len(), "start_sync");
            if let Err(e) = doc.start_sync(due.clone()).await {
                warn!("start_sync error: {e:#}");
                let mut peers = self.peers.lock().await;
                for a in due {
                    let p = peers.entry(a.id).or_default();
                    p.fails += 1;
                    p.last_err = Some(format!("{e:#}"));
                    p.next = Some(now + Duration::from_secs((1u64 << p.fails.min(7)).min(120)));
                }
            }
        }
    }

    async fn materialize_loop(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                _ = self.materialize_notify.notified() => {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                }
            }
            match self.materialize_once().await {
                Ok(n) if n > 0 && self.cfg.auto_export => {
                    if let Err(e) = self.export(None, None).await {
                        warn!("auto-export failed: {e:#}");
                    }
                }
                Ok(_) => {}
                Err(e) => warn!("materialize failed: {e:#}"),
            }
        }
    }

    async fn collect(doc: &Doc, q: Query) -> Result<Vec<Entry>> {
        let stream = doc.get_many(q).await?;
        tokio::pin!(stream);
        let mut v = Vec::new();
        while let Some(e) = stream.next().await {
            v.push(e?);
        }
        Ok(v)
    }

    async fn content(&self, e: &Entry) -> Option<bytes::Bytes> {
        self.blobs.get_bytes(e.content_hash()).await.ok()
    }

    /// Materialize every segment entry we have not seen yet. Returns number of new segments.
    pub async fn materialize_once(&self) -> Result<usize> {
        let _g = self.materialize_lock.lock().await;
        let Some(doc) = self.current_doc().await else { return Ok(0) };
        // agent names
        for e in Self::collect(&doc, Query::key_prefix(keys::AGENT).build()).await? {
            if let Some(b) = self.content(&e).await
                && let Ok(v) = serde_json::from_slice::<Value>(&b)
                    && let Some(name) = v.get("name").and_then(|n| n.as_str()) {
                        let db = self.db.clone();
                        let (a, n) = (e.author().to_string(), name.to_string());
                        tokio::task::spawn_blocking(move || db::upsert_agent(&db.lock().unwrap(), &a, &n)).await??;
                    }
        }
        if let Err(e) = self.materialize_experience(&doc).await {
            warn!("materialize howto/runs/queue: {e:#}");
        }
        let entries = Self::collect(&doc, Query::key_prefix(keys::SEG).build()).await?;
        let db = self.db.clone();
        let seen = tokio::task::spawn_blocking(move || db::materialized_set(&db.lock().unwrap())).await??;
        let mut new = 0usize;
        let mut pending = 0u64;
        for e in entries {
            let key = String::from_utf8_lossy(e.key()).to_string();
            let author = e.author().to_string();
            if seen.contains(&(key.clone(), author.clone())) {
                continue;
            }
            let Some((chain, source_key, hash)) = keys::parse_seg_key(e.key()) else {
                warn!("ignoring malformed segment key {key:?}");
                continue;
            };
            let Some(bytes) = self.content(&e).await else {
                pending += 1;
                continue;
            };
            let db = self.db.clone();
            let ts_ms = e.timestamp() / 1000;
            let res = tokio::task::spawn_blocking(move || -> Result<usize> {
                let tsv = segment::decompress(&bytes)?;
                let rows = segment::parse_segment(&tsv, &hash)?;
                let m = db::SegMeta { key: &key, author: &author, hash: &hash, chain: &chain, source_key: &source_key, entry_ts_ms: ts_ms };
                db::materialize_segment(&mut db.lock().unwrap(), &m, &rows)
            })
            .await?;
            match res {
                Ok(n) => {
                    new += 1;
                    debug!(new_provenance = n, "materialized segment");
                }
                Err(err) => warn!("bad segment {}: {err:#}", String::from_utf8_lossy(e.key())),
            }
        }
        self.pending_blobs.store(pending, Ordering::SeqCst);
        if new > 0 {
            info!(new, pending, "materialized segments");
        }
        Ok(new)
    }

    // ---------- agents / authors ----------

    async fn author_for(self: &Arc<Self>, agent: &str) -> Result<AuthorId> {
        let author = if agent == self.cfg.agent {
            self.default_author
        } else {
            let mut st = self.state.lock().await;
            if let Some(a) = st.agents.get(agent) {
                AuthorId::from_str(a)?
            } else {
                let a = self.docs.author_create().await?;
                st.agents.insert(agent.to_string(), a.to_string());
                self.save_state(&st).await?;
                a
            }
        };
        let doc = self.doc().await?;
        let mut published = self.agents_published.lock().await;
        if !published.contains(&(doc.id(), author)) {
            let key = keys::agent_key(&author.to_string());
            let existing = doc.get_exact(author, key.clone(), false).await?;
            if existing.is_none() {
                let v = json!({"name": agent, "node_id": self.endpoint.id().to_string(), "created_at": util::now_ms()});
                doc.set_bytes(author, key, serde_json::to_vec(&v)?).await?;
            }
            published.insert((doc.id(), author));
        }
        Ok(author)
    }

    fn agent_or_default(&self, a: &Option<String>) -> String {
        a.clone().filter(|s| !s.trim().is_empty()).unwrap_or_else(|| self.cfg.agent.clone())
    }

    // ---------- claims ----------

    /// Read all claim records for a source. Content of freshly synced entries may still be
    /// downloading; wait briefly for it. Returns (claims, unresolved_count).
    async fn read_claims_full(&self, doc: &Doc, source_key: &str) -> Result<(Vec<([u8; 32], ClaimRecord)>, usize)> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut out = Vec::new();
            let mut missing = 0usize;
            for e in Self::collect(doc, Query::key_exact(keys::claim_key(source_key)).build()).await? {
                match self.content(&e).await {
                    Some(b) => match serde_json::from_slice::<ClaimRecord>(&b) {
                        Ok(c) => out.push((e.author().to_bytes(), c)),
                        Err(err) => warn!("bad claim record: {err}"),
                    },
                    None => missing += 1,
                }
            }
            if missing == 0 || Instant::now() >= deadline {
                if missing > 0 {
                    warn!(source_key, missing, "claim content still unavailable");
                }
                return Ok((out, missing));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn read_claims(&self, doc: &Doc, source_key: &str) -> Result<Vec<([u8; 32], ClaimRecord)>> {
        Ok(self.read_claims_full(doc, source_key).await?.0)
    }

    async fn read_done(&self, doc: &Doc, source_key: &str) -> Result<Option<DoneRecord>> {
        let mut best: Option<DoneRecord> = None;
        for e in Self::collect(doc, Query::key_exact(keys::done_key(source_key)).build()).await? {
            if let Some(b) = self.content(&e).await
                && let Ok(d) = serde_json::from_slice::<DoneRecord>(&b)
                    && best.as_ref().map(|x| d.finished_at < x.finished_at).unwrap_or(true) {
                        best = Some(d);
                    }
        }
        Ok(best)
    }

    async fn read_src(&self, doc: &Doc, hash: &str) -> Result<Option<SrcRecord>> {
        for e in Self::collect(doc, Query::key_exact(keys::src_key(hash)).build()).await? {
            if let Some(b) = self.content(&e).await
                && let Ok(s) = serde_json::from_slice::<SrcRecord>(&b) {
                    return Ok(Some(s));
                }
        }
        Ok(None)
    }

    fn claim_json(author: &[u8; 32], c: &ClaimRecord, now: u64) -> Value {
        json!({
            "agent": c.agent, "author": hex::encode(author), "claimed_at": c.claimed_at,
            "lease_until": c.lease_until, "live": c.is_live(now), "released": c.released,
            "source_url": c.source_url,
        })
    }

    async fn write_claim(&self, doc: &Doc, author: AuthorId, source_key: &str, c: &ClaimRecord) -> Result<()> {
        doc.set_bytes(author, keys::claim_key(source_key), serde_json::to_vec(c)?).await?;
        Ok(())
    }

    fn outcome_json(outcome: Outcome, extra: Value) -> Value {
        let mut v = json!({"ok": true, "result": outcome});
        if let (Value::Object(m), Value::Object(e)) = (&mut v, extra) {
            m.extend(e);
        }
        v
    }

    async fn claim(
        self: &Arc<Self>,
        agent: String,
        source_key: String,
        source_url: Option<String>,
        source_hash: Option<String>,
        ttl_secs: Option<u64>,
        auto_renew: bool,
    ) -> Result<Value> {
        keys::validate_source_key(&source_key).map_err(|e| anyhow!(e))?;
        let _g = self.claim_lock.lock().await;
        let doc = self.doc().await?;
        if let Some(d) = self.read_done(&doc, &source_key).await? {
            return Ok(Self::outcome_json(Outcome::Skip, json!({"reason": "done", "done": d})));
        }
        if let Some(h) = &source_hash
            && let Some(s) = self.read_src(&doc, h).await? {
                return Ok(Self::outcome_json(Outcome::Skip, json!({"reason": "source_seen", "src": s})));
            }
        let author = self.author_for(&agent).await?;
        let me = author.to_bytes();
        let ttl_ms = ttl_secs.unwrap_or(self.cfg.ttl_secs).max(1) * 1000;
        let now = util::now_ms();
        let (claims, unresolved) = self.read_claims_full(&doc, &source_key).await?;
        let mine = claims.iter().find(|(a, c)| *a == me && c.is_live(now)).map(|(_, c)| c.clone());
        if unresolved > 0 && mine.is_none() {
            // someone else's claim exists but we can't read it yet: don't start work
            return Ok(Self::outcome_json(
                Outcome::Lost,
                json!({"source_key": source_key, "reason": "unresolved_claims", "unresolved": unresolved, "retry": true}),
            ));
        }
        let rec = match mine {
            Some(mut c) => {
                c.lease_until = now + ttl_ms;
                c.ttl_ms = ttl_ms;
                if source_url.is_some() {
                    c.source_url = source_url;
                }
                if source_hash.is_some() {
                    c.source_hash = source_hash;
                }
                c
            }
            None => {
                if let Some(w) = claims::winner(&claims, now) {
                    let (a, c) = &claims[w];
                    return Ok(Self::outcome_json(
                        Outcome::Lost,
                        json!({"source_key": source_key, "winner": Self::claim_json(a, c, now)}),
                    ));
                }
                ClaimRecord { agent: agent.clone(), claimed_at: now, lease_until: now + ttl_ms, source_url, source_hash, released: false, ttl_ms }
            }
        };
        self.write_claim(&doc, author, &source_key, &rec).await?;
        let (claims, unresolved) = self.read_claims_full(&doc, &source_key).await?;
        let w = claims::winner(&claims, now);
        let won = unresolved == 0 && w.map(|i| claims[i].0 == me).unwrap_or(false);
        if won && auto_renew {
            let next = Instant::now() + Duration::from_millis(ttl_ms / 3);
            self.auto_renew.lock().await.insert((agent.clone(), source_key.clone()), next);
        }
        let winner = w.map(|i| Self::claim_json(&claims[i].0, &claims[i].1, now));
        Ok(Self::outcome_json(
            if won { Outcome::Won } else { Outcome::Lost },
            json!({"source_key": source_key, "agent": agent, "lease_until": rec.lease_until, "claimed_at": rec.claimed_at, "winner": winner, "advisory": true}),
        ))
    }

    async fn renew(self: &Arc<Self>, agent: String, source_key: String) -> Result<Value> {
        let _g = self.claim_lock.lock().await;
        let doc = self.doc().await?;
        if let Some(d) = self.read_done(&doc, &source_key).await? {
            self.auto_renew.lock().await.remove(&(agent.clone(), source_key.clone()));
            return Ok(Self::outcome_json(Outcome::Skip, json!({"reason": "done", "done": d})));
        }
        let author = self.author_for(&agent).await?;
        let me = author.to_bytes();
        let now = util::now_ms();
        let (claims, unresolved) = self.read_claims_full(&doc, &source_key).await?;
        let Some(mut mine) = claims.iter().find(|(a, c)| *a == me && c.is_live(now)).map(|(_, c)| c.clone()) else {
            self.auto_renew.lock().await.remove(&(agent.clone(), source_key.clone()));
            return Ok(Self::outcome_json(Outcome::NotClaimed, json!({"source_key": source_key})));
        };
        let w = claims::winner(&claims, now).expect("own live claim exists");
        if claims[w].0 == me {
            let ttl = if mine.ttl_ms > 0 { mine.ttl_ms } else { self.cfg.ttl_secs * 1000 };
            mine.lease_until = now + ttl;
            self.write_claim(&doc, author, &source_key, &mine).await?;
            Ok(Self::outcome_json(
                Outcome::Won,
                json!({"source_key": source_key, "lease_until": mine.lease_until, "unresolved": unresolved}),
            ))
        } else {
            // lost after merge: step back so everyone converges on the winner
            mine.released = true;
            mine.lease_until = now;
            self.write_claim(&doc, author, &source_key, &mine).await?;
            self.auto_renew.lock().await.remove(&(agent.clone(), source_key.clone()));
            Ok(Self::outcome_json(
                Outcome::Lost,
                json!({"source_key": source_key, "winner": Self::claim_json(&claims[w].0, &claims[w].1, now)}),
            ))
        }
    }

    async fn release(self: &Arc<Self>, agent: String, source_key: String) -> Result<Value> {
        let _g = self.claim_lock.lock().await;
        let doc = self.doc().await?;
        let author = self.author_for(&agent).await?;
        let me = author.to_bytes();
        let now = util::now_ms();
        self.auto_renew.lock().await.remove(&(agent.clone(), source_key.clone()));
        let claims = self.read_claims(&doc, &source_key).await?;
        let released = if let Some((_, c)) = claims.iter().find(|(a, c)| *a == me && !c.released) {
            let mut c = c.clone();
            c.released = true;
            c.lease_until = now;
            self.write_claim(&doc, author, &source_key, &c).await?;
            true
        } else {
            false
        };
        Ok(json!({"ok": true, "source_key": source_key, "released": released}))
    }

    async fn done(self: &Arc<Self>, agent: String, source_key: String, source_hash: Option<String>) -> Result<Value> {
        keys::validate_source_key(&source_key).map_err(|e| anyhow!(e))?;
        let _g = self.claim_lock.lock().await;
        let doc = self.doc().await?;
        let author = self.author_for(&agent).await?;
        let now = util::now_ms();
        let mut segs: Vec<String> = Self::collect(&doc, Query::key_prefix(keys::SEG).build())
            .await?
            .into_iter()
            .filter_map(|e| {
                let (_, s, _) = keys::parse_seg_key(e.key())?;
                (s == source_key).then(|| String::from_utf8_lossy(e.key()).to_string())
            })
            .collect();
        segs.sort();
        segs.dedup();
        let rec = DoneRecord { agent: agent.clone(), source_key: source_key.clone(), source_content_hash: source_hash.clone(), segments: segs, finished_at: now };
        doc.set_bytes(author, keys::done_key(&source_key), serde_json::to_vec(&rec)?).await?;
        if let Some(h) = &source_hash {
            let s = SrcRecord { agent: agent.clone(), source_key: source_key.clone(), finished_at: now };
            doc.set_bytes(author, keys::src_key(h), serde_json::to_vec(&s)?).await?;
        }
        // drop our own claim
        let me = author.to_bytes();
        let claims = self.read_claims(&doc, &source_key).await?;
        if let Some((_, c)) = claims.iter().find(|(a, c)| *a == me && !c.released) {
            let mut c = c.clone();
            c.released = true;
            c.lease_until = now;
            self.write_claim(&doc, author, &source_key, &c).await?;
        }
        self.auto_renew.lock().await.remove(&(agent, source_key));
        Ok(json!({"ok": true, "done": rec}))
    }

    async fn renew_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let now = Instant::now();
            let due: Vec<(String, String)> = self
                .auto_renew
                .lock()
                .await
                .iter()
                .filter(|(_, t)| **t <= now)
                .map(|(k, _)| k.clone())
                .collect();
            for (agent, sk) in due {
                match self.renew(agent.clone(), sk.clone()).await {
                    Ok(v) if v["result"] == "won" => {
                        let ttl = self.cfg.ttl_secs * 1000;
                        let next = Instant::now() + Duration::from_millis((ttl / 3).max(1000));
                        // renew() keeps the per-claim ttl; schedule using the default as an upper bound
                        if let Some(t) = self.auto_renew.lock().await.get_mut(&(agent, sk)) {
                            *t = next;
                        }
                    }
                    Ok(v) => info!(source_key = %sk, result = %v["result"], "auto-renew stopped"),
                    Err(e) => warn!("auto-renew {sk}: {e:#}"),
                }
            }
        }
    }

    // ---------- ingest ----------

    async fn ingest_file(
        self: &Arc<Self>,
        path: &Path,
        agent: Option<String>,
        defaults: segment::Defaults,
        format: segment::Format,
    ) -> Result<Value> {
        let _g = self.ingest_lock.lock().await;
        let data = tokio::fs::read(path).await.with_context(|| format!("reading {}", path.display()))?;
        let ext = path.extension().and_then(|e| e.to_str()).map(|s| s.to_string());
        let max_rows = self.cfg.segment_rows;
        let (parsed, segs) = tokio::task::spawn_blocking(move || -> Result<_> {
            let fmt = if format == segment::Format::Auto { segment::detect_format(&data, ext.as_deref()) } else { format };
            let parsed = segment::parse_input(&data, fmt, &defaults)?;
            let rows = parsed.rows.clone();
            let segs = segment::segments_from_parsed(rows, max_rows);
            Ok((parsed, segs))
        })
        .await??;
        let agent = agent.or(parsed.defaults.agent.clone()).filter(|s| !s.is_empty()).unwrap_or_else(|| self.cfg.agent.clone());
        let author = self.author_for(&agent).await?;
        let doc = self.doc().await?;
        let mut out = Vec::new();
        let mut source_keys = BTreeSet::new();
        for s in &segs {
            let compressed = s.compressed()?;
            doc.set_bytes(author, s.key(), compressed.clone()).await?;
            source_keys.insert(s.source_key.clone());
            out.push(json!({"key": s.key(), "chain": s.chain, "source_key": s.source_key, "hash": s.hash_hex, "rows": s.rows, "bytes": compressed.len()}));
        }
        drop(_g);
        self.materialize_once().await?;
        let mut warnings = Vec::new();
        for sk in &source_keys {
            if let Some(d) = self.read_done(&doc, sk).await?
                && d.agent != agent {
                    warnings.push(format!("source_key '{sk}' was already marked done by '{}'", d.agent));
                }
        }
        Ok(json!({
            "ok": true, "agent": agent, "rows_in": parsed.rows.len() + parsed.rejected,
            "rows_accepted": parsed.rows.len(), "rejected": parsed.rejected, "errors": parsed.errors,
            "segments": out, "warnings": warnings,
        }))
    }

    async fn process_inbox(self: &Arc<Self>) -> Result<usize> {
        let inbox = self.cfg.data_dir.join("inbox");
        let mut rd = tokio::fs::read_dir(&inbox).await?;
        let mut files = Vec::new();
        while let Some(e) = rd.next_entry().await? {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with('.') || name.ends_with(".tmp") || name.ends_with(".part") {
                continue;
            }
            if !e.file_type().await?.is_file() {
                continue;
            }
            files.push((name, e.path()));
        }
        files.sort();
        let mut n = 0;
        for (name, path) in files {
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or(&name).to_string();
            let defaults = segment::Defaults { source_key: None, ..Default::default() };
            // file directives win; fall back to the file stem as source_key
            let res = async {
                let data = tokio::fs::read(&path).await?;
                let mut fdef = defaults.clone();
                let has_src_directive = String::from_utf8_lossy(&data[..data.len().min(8192)])
                    .lines()
                    .take_while(|l| l.trim().starts_with('#') || l.trim().is_empty())
                    .any(|l| l.contains("source_key="));
                let has_src_column = String::from_utf8_lossy(&data[..data.len().min(8192)]).contains("source_key");
                if !has_src_directive && !has_src_column {
                    fdef.source_key = Some(stem.clone());
                }
                self.ingest_file(&path, None, fdef, segment::Format::Auto).await
            }
            .await;
            let ts = util::now_ms();
            match res {
                Ok(v) => {
                    let dest = self.cfg.data_dir.join("processed").join(format!("{ts}-{name}"));
                    tokio::fs::rename(&path, &dest).await?;
                    tokio::fs::write(dest.with_extension("result.json"), serde_json::to_vec_pretty(&v)?).await?;
                    info!(file = %name, "inbox file ingested");
                }
                Err(e) => {
                    let dest = self.cfg.data_dir.join("failed").join(format!("{ts}-{name}"));
                    tokio::fs::rename(&path, &dest).await?;
                    tokio::fs::write(dest.with_extension("error.txt"), format!("{e:#}\n")).await?;
                    warn!(file = %name, "inbox file failed: {e:#}");
                }
            }
            n += 1;
        }
        Ok(n)
    }

    async fn export(&self, chain: Option<String>, out_dir: Option<String>) -> Result<Value> {
        self.materialize_once().await?;
        let dir = out_dir.map(PathBuf::from).unwrap_or_else(|| self.cfg.data_dir.join("export"));
        let db = self.db.clone();
        let files = tokio::task::spawn_blocking(move || db::export(&db.lock().unwrap(), &dir, chain.as_deref())).await??;
        Ok(json!({"ok": true, "files": files}))
    }

    // ---------- status / share / join / sync ----------

    async fn status(self: &Arc<Self>, source_key: Option<String>) -> Result<Value> {
        let db = self.db.clone();
        let stats = tokio::task::spawn_blocking(move || db::stats(&db.lock().unwrap())).await??;
        let doc = self.current_doc().await;
        let now = util::now_ms();
        let mut v = json!({
            "ok": true,
            "node_id": self.endpoint.id().to_string(),
            "agent": self.cfg.agent,
            "default_author": self.default_author.to_string(),
            "namespace": doc.as_ref().map(|d| d.id().to_string()),
            "sync": if self.paused.load(Ordering::SeqCst) { "paused" } else { "online" },
            "db": stats,
            "pending_blobs": self.pending_blobs.load(Ordering::SeqCst),
            "data_dir": self.cfg.data_dir,
            "labels_db": self.cfg.data_dir.join("labels.db"),
        });
        let peers: BTreeMap<String, PeerState> =
            self.peers.lock().await.iter().map(|(k, p)| (k.to_string(), p.clone())).collect();
        v["peers"] = serde_json::to_value(peers)?;
        if let Some(doc) = &doc {
            if let Some(sk) = source_key {
                let claims = self.read_claims(doc, &sk).await?;
                let w = claims::winner(&claims, now);
                v["source"] = json!({
                    "source_key": sk,
                    "done": self.read_done(doc, &sk).await?,
                    "winner": w.map(|i| Self::claim_json(&claims[i].0, &claims[i].1, now)),
                    "claims": claims.iter().map(|(a, c)| Self::claim_json(a, c, now)).collect::<Vec<_>>(),
                });
            } else {
                let claim_entries = Self::collect(doc, Query::key_prefix(keys::CLAIM).build()).await?;
                let mut by_src: BTreeMap<String, Vec<([u8; 32], ClaimRecord)>> = BTreeMap::new();
                for e in claim_entries {
                    let Some(sk) = keys::parse_claim_or_done_key(e.key(), keys::CLAIM) else { continue };
                    if let Some(b) = self.content(&e).await
                        && let Ok(c) = serde_json::from_slice::<ClaimRecord>(&b) {
                            by_src.entry(sk).or_default().push((e.author().to_bytes(), c));
                        }
                }
                let active: Vec<Value> = by_src
                    .iter()
                    .filter_map(|(sk, cl)| {
                        let w = claims::winner(cl, now)?;
                        Some(json!({"source_key": sk, "holder": cl[w].1.agent, "lease_until": cl[w].1.lease_until, "contenders": cl.iter().filter(|(_, c)| c.is_live(now)).count()}))
                    })
                    .collect();
                let done = Self::collect(doc, Query::key_prefix(keys::DONE).build()).await?;
                let done_keys: BTreeSet<String> =
                    done.iter().filter_map(|e| keys::parse_claim_or_done_key(e.key(), keys::DONE)).collect();
                v["active_claims"] = json!(active);
                v["done_sources"] = json!(done_keys.len());
            }
        }
        Ok(v)
    }

    async fn share(self: &Arc<Self>, read_only: bool, addr: Option<String>) -> Result<Value> {
        let doc = self.doc().await?;
        let mode = if read_only { ShareMode::Read } else { ShareMode::Write };
        let opts = match addr.as_deref().unwrap_or("all") {
            "all" => AddrInfoOptions::RelayAndAddresses,
            "relay" => AddrInfoOptions::Relay,
            "direct" => AddrInfoOptions::Addresses,
            "id" => AddrInfoOptions::Id,
            o => bail!("unknown addr mode '{o}' (all|relay|direct|id)"),
        };
        if matches!(opts, AddrInfoOptions::RelayAndAddresses | AddrInfoOptions::Relay) {
            // make sure we have a home relay before minting the ticket
            let _ = tokio::time::timeout(Duration::from_secs(5), self.endpoint.online()).await;
        }
        let ticket = doc.share(mode, opts).await?;
        // NOTE: never log the ticket: a write ticket is a non-revocable bearer secret.
        info!(read_only, "ticket generated");
        Ok(json!({"ok": true, "ticket": ticket.encode_string(), "mode": if read_only {"read"} else {"write"}, "namespace": doc.id().to_string()}))
    }

    async fn join(self: &Arc<Self>, ticket: String, force: bool) -> Result<Value> {
        let ticket = DocTicket::from_str(ticket.trim()).map_err(|e| anyhow!("invalid ticket: {e}"))?;
        let ns = ticket.capability.id();
        if let Some(cur) = self.current_doc().await
            && cur.id() != ns {
                let n = Self::collect(&cur, Query::all().limit(1).build()).await?.len();
                if n > 0 && !force {
                    bail!("this node already has data in namespace {}; refusing to switch (use --force)", cur.id());
                }
                let _ = cur.leave().await;
            }
        {
            let mut st = self.state.lock().await;
            for p in &ticket.nodes {
                if !st.peers.iter().any(|x| x.id == p.id) && p.id != self.endpoint.id() {
                    st.peers.push(p.clone());
                }
            }
            st.namespace = Some(ns.to_string());
            self.save_state(&st).await?;
        }
        let doc = self.docs.import_namespace(ticket.capability.clone()).await?;
        self.set_doc(doc).await?;
        info!(namespace = %ns, peers = ticket.nodes.len(), "joined namespace");
        Ok(json!({"ok": true, "namespace": ns.to_string(), "peers": ticket.nodes.iter().map(|n| n.id.to_string()).collect::<Vec<_>>()}))
    }

    async fn sync_cmd(self: &Arc<Self>, action: &str) -> Result<Value> {
        match action {
            "pause" => {
                self.paused.store(true, Ordering::SeqCst);
                if let Some(doc) = self.current_doc().await {
                    doc.leave().await?;
                }
            }
            "resume" | "now" => {
                self.paused.store(false, Ordering::SeqCst);
                for p in self.peers.lock().await.values_mut() {
                    p.next = None;
                    p.fails = 0;
                }
                if let Some(doc) = self.current_doc().await {
                    let peers = self.state.lock().await.peers.clone();
                    doc.start_sync(peers).await?;
                }
                self.sync_notify.notify_one();
            }
            o => bail!("unknown sync action '{o}' (pause|resume|now)"),
        }
        Ok(json!({"ok": true, "sync": if self.paused.load(Ordering::SeqCst) {"paused"} else {"online"}}))
    }

    pub async fn handle(self: &Arc<Self>, req: Request) -> Result<Value> {
        match req {
            Request::Status { source_key } => self.status(source_key).await,
            Request::Share { read_only, addr } => self.share(read_only, addr).await,
            Request::Join { ticket, force } => self.join(ticket, force).await,
            Request::Append { path, delete_after, agent, source_key, label_source, chain, observed_at, format } => {
                let fmt = match format {
                    Some(f) => segment::Format::from_str(&f)?,
                    None => segment::Format::Auto,
                };
                let d = segment::Defaults { source_key, label_source, chain, observed_at, agent: None };
                let p = PathBuf::from(&path);
                let res = self.ingest_file(&p, agent, d, fmt).await;
                if delete_after {
                    let _ = tokio::fs::remove_file(&p).await;
                }
                res
            }
            Request::Export { chain, out_dir } => self.export(chain, out_dir).await,
            Request::Claim { source_key, agent, source_url, source_hash, ttl_secs, auto_renew } => {
                let a = self.agent_or_default(&agent);
                self.claim(a, source_key, source_url, source_hash, ttl_secs, auto_renew).await
            }
            Request::Renew { source_key, agent } => self.renew(self.agent_or_default(&agent), source_key).await,
            Request::Done { source_key, agent, source_hash } => {
                self.done(self.agent_or_default(&agent), source_key, source_hash).await
            }
            Request::Release { source_key, agent } => self.release(self.agent_or_default(&agent), source_key).await,
            Request::Sync { action } => self.sync_cmd(&action).await,
            Request::HowtoPut { source_key, content, agent } => {
                self.howto_put(self.agent_or_default(&agent), source_key, content).await
            }
            Request::HowtoGet { source_key } => self.howto_get(source_key).await,
            Request::Report { source_key, agent, record } => {
                self.report(self.agent_or_default(&agent), source_key, record).await
            }
            Request::Next { sources, claim, agent, ttl_secs, auto_renew } => {
                self.next(self.agent_or_default(&agent), sources, claim, ttl_secs, auto_renew).await
            }
            Request::QueueAdd { items, source_url, agent } => {
                self.queue_set(self.agent_or_default(&agent), items, source_url, false).await
            }
            Request::QueueRm { items, agent } => self.queue_set(self.agent_or_default(&agent), items, None, true).await,
            Request::QueueList => self.queue_list().await,
            Request::Shutdown => {
                self.shutdown.notify_one();
                Ok(json!({"ok": true, "shutting_down": true}))
            }
        }
    }

    async fn shutdown_all(&self) {
        info!("shutting down");
        if let Err(e) = self.router.shutdown().await {
            warn!("router shutdown: {e}");
        }
        let _ = self.blobs.shutdown().await;
        let _ = std::fs::remove_file(&self.cfg.socket);
    }
}

#[cfg(unix)]
async fn serve_ipc(node: Arc<Node>, listener: tokio::net::UnixListener) {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                warn!("ipc accept: {e}");
                continue;
            }
        };
        let node = node.clone();
        tokio::spawn(async move {
            let (r, mut w) = stream.into_split();
            let mut line = String::new();
            let mut reader = BufReader::new(r.take(4 * 1024 * 1024));
            if reader.read_line(&mut line).await.is_err() {
                return;
            }
            let resp = match serde_json::from_str::<Request>(&line) {
                Ok(req) => match node.handle(req).await {
                    Ok(v) => v,
                    Err(e) => json!({"ok": false, "error": format!("{e:#}")}),
                },
                Err(e) => json!({"ok": false, "error": format!("bad request: {e}")}),
            };
            let mut out = serde_json::to_vec(&resp).unwrap_or_default();
            out.push(b'\n');
            let _ = w.write_all(&out).await;
            let _ = w.shutdown().await;
        });
    }
}

pub async fn run(cfg: Config) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = cfg;
        bail!("labelsync daemon IPC is only implemented for Unix sockets (Windows named pipe: TODO)");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // refuse to start twice on the same socket
        if cfg.socket.exists() {
            if std::os::unix::net::UnixStream::connect(&cfg.socket).is_ok() {
                bail!("a daemon is already listening on {}", cfg.socket.display());
            }
            std::fs::remove_file(&cfg.socket)?;
        }
        let node = Node::spawn(cfg.clone()).await?;
        let listener = tokio::net::UnixListener::bind(&cfg.socket)
            .with_context(|| format!("binding {}", cfg.socket.display()))?;
        std::fs::set_permissions(&cfg.socket, std::fs::Permissions::from_mode(0o600))?;
        info!(socket = %cfg.socket.display(), "listening");

        tokio::spawn(serve_ipc(node.clone(), listener));
        tokio::spawn(node.clone().sync_loop());
        tokio::spawn(node.clone().materialize_loop());
        tokio::spawn(node.clone().renew_loop());

        // inbox watcher (debounced) + periodic rescan as a safety net
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let mut debouncer = notify_debouncer_full::new_debouncer(
            Duration::from_millis(400),
            None,
            move |res: notify_debouncer_full::DebounceEventResult| {
                if res.is_ok() {
                    let _ = tx.send(());
                }
            },
        )?;
        debouncer.watch(cfg.data_dir.join("inbox"), notify::RecursiveMode::NonRecursive)?;
        let inbox_node = node.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = inbox_node.process_inbox().await {
                    warn!("inbox: {e:#}");
                }
                tokio::select! {
                    _ = rx.recv() => {}
                    _ = tokio::time::sleep(Duration::from_secs(15)) => {}
                }
            }
        });

        #[cfg(unix)]
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
            _ = node.shutdown.notified() => {}
        }
        // notify's Drop does not join its thread; stop explicitly.
        debouncer.stop();
        node.shutdown_all().await;
        Ok(())
    }
}
