//! Phase 1.5: shared experience (howto playbooks, run reports, shared queue, `next`).

use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use iroh_docs::{api::Doc, store::Query};
use serde_json::{Value, json};

use super::Node;
use crate::{claims, db, keys, playbook, score, util};

struct Loaded {
    doc: automerge::AutoCommit,
    /// blake3 over sorted (author, content hash) of all howto entries
    sig: String,
    ts_ms: u64,
    authors: usize,
    legacy: bool,
}

impl Node {
    async fn dbq<T: Send + 'static>(
        &self,
        f: impl FnOnce(&rusqlite::Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || f(&db.lock().unwrap())).await?
    }

    /// Mirror howto/, runs/ and queue/ entries into SQLite.
    pub(super) async fn materialize_experience(&self, doc: &Doc) -> Result<()> {
        let mut srcs = std::collections::BTreeSet::new();
        for e in Self::collect(doc, Query::key_prefix(keys::HOWTO).build()).await? {
            if let Some(src) = keys::parse_claim_or_done_key(e.key(), keys::HOWTO) {
                srcs.insert(src);
            }
        }
        for src in srcs {
            let Some(l) = self.load_playbook(doc, &src).await? else { continue };
            let s2 = src.clone();
            if self.dbq(move |c| db::playbook_sig(c, &s2)).await?.as_deref() == Some(l.sig.as_str()) {
                continue;
            }
            let mut d = l.doc;
            let (version, content) = (playbook::heads_str(&d.get_heads()), playbook::text(&d)?);
            let (sig, ts) = (l.sig, l.ts_ms);
            self.dbq(move |c| db::upsert_playbook(c, &src, &version, &sig, ts, &content)).await?;
        }
        let known = self.dbq(db::run_keys).await?;
        for e in Self::collect(doc, Query::key_prefix(keys::RUNS).build()).await? {
            let key = String::from_utf8_lossy(e.key()).into_owned();
            if known.contains(&key) || keys::parse_run_key(e.key()).is_none() {
                continue;
            }
            let Some(b) = self.content(&e).await else { continue };
            let Ok(rec) = serde_json::from_slice::<Value>(&b) else { continue };
            let author = e.author().to_string();
            self.dbq(move |c| db::insert_run(c, &key, &author, &rec)).await?;
        }
        for e in Self::collect(doc, Query::key_prefix(keys::QUEUE).build()).await? {
            let Some(src) = keys::parse_claim_or_done_key(e.key(), keys::QUEUE) else { continue };
            let Some(b) = self.content(&e).await else { continue };
            let v: Value = serde_json::from_slice(&b).unwrap_or_default();
            let removed = v["removed"].as_bool().unwrap_or(false);
            let url = v["source_url"].as_str().map(|s| s.to_string());
            let (author, ts) = (e.author().to_string(), e.timestamp() / 1000);
            self.dbq(move |c| db::upsert_queue(c, &src, &author, ts, removed, url.as_deref())).await?;
        }
        Ok(())
    }

    /// Load and merge every author's playbook save (plus legacy plain-md entries).
    /// Returns None if the source has no playbook entries at all.
    async fn load_playbook(&self, doc: &Doc, source_key: &str) -> Result<Option<Loaded>> {
        let entries = Self::collect(doc, Query::key_exact(keys::howto_key(source_key)).build()).await?;
        if entries.is_empty() {
            return Ok(None);
        }
        let mut saves = Vec::new();
        let mut legacy: Option<(u64, String, String)> = None; // (ts, author, md)
        let mut sig_parts = Vec::new();
        let mut ts_ms = 0;
        for e in &entries {
            let Some(b) = self.content(e).await else { continue };
            sig_parts.push(format!("{}:{}", e.author(), e.content_hash().to_hex()));
            ts_ms = ts_ms.max(e.timestamp() / 1000);
            if playbook::is_automerge(&b) {
                saves.push(b);
            } else {
                let cand = (e.timestamp(), e.author().to_string(), String::from_utf8_lossy(&b).into_owned());
                if legacy.as_ref().map(|l| (cand.0, &cand.1) > (l.0, &l.1)).unwrap_or(true) {
                    legacy = Some(cand);
                }
            }
        }
        sig_parts.sort();
        let sig = blake3::hash(sig_parts.join("\n").as_bytes()).to_hex().to_string();
        // Legacy (phase 1.5) plain-md entries are converted only while no Automerge save exists.
        // The first put stores the converted doc; after that leftover legacy entries are ignored
        // (newest-wins, as in phase 1.5), so a different "newest legacy" can't be inserted twice.
        if !saves.is_empty() {
            legacy = None;
        }
        let refs: Vec<&[u8]> = saves.iter().map(|b| b.as_ref()).collect();
        let doc = playbook::load_merged(&refs, legacy.as_ref().map(|l| (source_key, l.2.as_str())))?;
        Ok(Some(Loaded { doc, sig, ts_ms, authors: entries.len(), legacy: legacy.is_some() }))
    }

    pub(super) async fn howto_put(
        self: &Arc<Self>,
        agent: String,
        source_key: String,
        content: String,
        base: Option<String>,
    ) -> Result<Value> {
        keys::validate_source_key(&source_key).map_err(|e| anyhow!(e))?;
        let author = self.author_for(&agent).await?;
        let doc = self.doc().await?;
        let _g = self.howto_lock.lock().await;
        let mut pb = match self.load_playbook(&doc, &source_key).await? {
            Some(l) => l.doc,
            None => playbook::load_merged(&[], None)?,
        };
        let base_heads = match &base {
            Some(b) => {
                let h = playbook::parse_heads(b)?;
                if !playbook::has_heads(&mut pb, &h) {
                    bail!("base version not known on this node yet (not synced?): {b}");
                }
                h
            }
            None => pb.get_heads(),
        };
        let now = util::now_ms();
        let actor = playbook::actor_for(&author.to_bytes(), now);
        let st = playbook::put(&mut pb, actor, &base_heads, &content, now as i64, &format!("{agent}: howto put"))?;
        if st.splices > 0 {
            doc.set_bytes(author, keys::howto_key(&source_key), pb.save()).await?;
            self.materialize_once().await?;
        }
        Ok(json!({
            "ok": true, "source_key": source_key, "agent": agent, "changed": st.splices > 0,
            "inserted_chars": st.inserted_chars, "deleted_chars": st.deleted_chars, "splices": st.splices,
            "base": playbook::heads_str(&base_heads), "version": playbook::heads_str(&pb.get_heads()),
            "merged_concurrent_edits": base.is_some() && pb.get_heads().len() > 1,
        }))
    }

    /// Merged markdown (or a historical version) + version id.
    pub(super) async fn howto_get(self: &Arc<Self>, source_key: String, version: Option<String>) -> Result<Value> {
        let doc = self.doc().await?;
        let Some(mut l) = self.load_playbook(&doc, &source_key).await? else {
            return Ok(json!({"ok": true, "source_key": source_key, "version": null, "content": null}));
        };
        let heads = l.doc.get_heads();
        let (content, v) = match version {
            Some(v) => {
                let h = playbook::parse_heads(&v)?;
                if !playbook::has_heads(&mut l.doc, &h) {
                    bail!("unknown playbook version {v}");
                }
                (playbook::text_at(&l.doc, &h)?, playbook::heads_str(&h))
            }
            None => (playbook::text(&l.doc)?, playbook::heads_str(&heads)),
        };
        Ok(json!({
            "ok": true, "source_key": source_key, "version": v, "content": content,
            "concurrent_heads": heads.len(), "author_saves": l.authors, "legacy_migrated": l.legacy,
            "updated": util::fmt_ms(l.ts_ms),
        }))
    }

    pub(super) async fn howto_log(self: &Arc<Self>, source_key: String) -> Result<Value> {
        let doc = self.doc().await?;
        let Some(mut l) = self.load_playbook(&doc, &source_key).await? else {
            return Ok(json!({"ok": true, "source_key": source_key, "changes": []}));
        };
        let mut out = Vec::new();
        for c in playbook::log(&mut l.doc) {
            let agent = match &c.author {
                Some(a) => {
                    let a2 = a.clone();
                    self.dbq(move |db| db::agent_name(db, &a2)).await?
                }
                None => None,
            };
            out.push(json!({
                "hash": c.hash, "author": c.author, "agent": agent, "time_ms": c.time_ms,
                "time": (c.time_ms > 0).then(|| util::fmt_ms(c.time_ms as u64)), "message": c.message, "deps": c.deps,
            }));
        }
        Ok(json!({"ok": true, "source_key": source_key, "version": playbook::heads_str(&l.doc.get_heads()), "changes": out}))
    }

    pub(super) async fn report(self: &Arc<Self>, agent: String, source_key: String, input: Value) -> Result<Value> {
        keys::validate_source_key(&source_key).map_err(|e| anyhow!(e))?;
        let Value::Object(input) = input else { bail!("run record must be a JSON object") };
        let author = self.author_for(&agent).await?;
        let doc = self.doc().await?;
        self.materialize_once().await?;
        let now = util::now_ms();
        let num = |k: &str| input.get(k).and_then(|v| v.as_f64());
        let duration_s = num("duration_s");
        // run window start: explicit, else our claim's claimed_at, else now - duration
        let started_at = match input.get("started_at") {
            Some(Value::String(s)) => util::parse_time_ms(s),
            Some(v) => v.as_u64().map(|n| if n < 100_000_000_000 { n * 1000 } else { n }),
            None => None,
        }
        ;
        let started_at = match started_at {
            Some(t) => Some(t),
            None => {
                let me = author.to_bytes();
                let claims = self.read_claims(&doc, &source_key).await?;
                claims
                    .iter()
                    .find(|(a, _)| *a == me)
                    .map(|(_, c)| c.claimed_at)
                    .or_else(|| duration_s.map(|d| now.saturating_sub((d * 1000.0) as u64 + 1000)))
            }
        };
        let (src2, auth2) = (source_key.clone(), author.to_string());
        let since = started_at.unwrap_or(0);
        let (segments, total, new) = self.dbq(move |c| db::run_rows(c, &src2, &auth2, since)).await?;
        let mut rec = serde_json::Map::new();
        for k in ["duration_s", "errors", "retries", "method", "howto_version", "notes"] {
            rec.insert(k.into(), input.get(k).cloned().unwrap_or(Value::Null));
        }
        let computed = !segments.is_empty();
        rec.insert("rows_total".into(), input.get("rows_total").cloned().unwrap_or(if computed { json!(total) } else { Value::Null }));
        rec.insert(
            "rows_new_after_dedup".into(),
            input.get("rows_new_after_dedup").cloned().unwrap_or(if computed { json!(new) } else { Value::Null }),
        );
        rec.insert("rows_computed".into(), json!(computed && !input.contains_key("rows_new_after_dedup")));
        rec.insert("segments".into(), json!(segments));
        rec.insert("source_key".into(), json!(source_key));
        rec.insert("agent".into(), json!(agent));
        rec.insert("ts".into(), json!(now));
        rec.insert("started_at".into(), json!(started_at));
        let rec = Value::Object(rec);
        let key = keys::run_key(&source_key, &agent, now);
        doc.set_bytes(author, key.clone(), serde_json::to_vec(&rec)?).await?;
        self.materialize_once().await?;
        Ok(json!({"ok": true, "key": key, "run": rec}))
    }

    pub(super) async fn queue_set(
        self: &Arc<Self>,
        agent: String,
        items: Vec<String>,
        source_url: Option<String>,
        removed: bool,
    ) -> Result<Value> {
        let author = self.author_for(&agent).await?;
        let doc = self.doc().await?;
        let mut n = 0;
        for it in items.iter().map(|s| s.trim()).filter(|s| !s.is_empty() && !s.starts_with('#')) {
            keys::validate_source_key(it).map_err(|e| anyhow!(e))?;
            let v = json!({"agent": agent, "added_at": util::now_ms(), "source_url": source_url, "removed": removed});
            doc.set_bytes(author, keys::queue_key(it), serde_json::to_vec(&v)?).await?;
            n += 1;
        }
        self.materialize_once().await?;
        Ok(json!({"ok": true, if removed {"removed"} else {"added"}: n}))
    }

    pub(super) async fn queue_list(self: &Arc<Self>) -> Result<Value> {
        self.materialize_once().await?;
        let q = self.dbq(db::queue_list).await?;
        Ok(json!({"ok": true, "queue": q.iter().map(|(k, u)| json!({"source_key": k, "source_url": u})).collect::<Vec<_>>()}))
    }

    /// Stats + playbooks for one source (used by `next`).
    async fn source_info(&self, source_key: &str) -> Result<Value> {
        let now = util::now_ms();
        let s = source_key.to_string();
        let (agg, methods, best_hash, latest) = self
            .dbq(move |c| {
                Ok((db::source_agg(c, &s)?, db::method_aggs(c, &s)?, db::best_howto_version(c, &s)?, db::latest_playbook(c, &s)?))
            })
            .await?;
        let best_method = methods
            .iter()
            .max_by(|a, b| {
                let y = |m: &score::Agg| score::expected_yield(Some(m)).unwrap_or(-1.0);
                y(&a.1).partial_cmp(&y(&b.1)).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(m, a)| json!({"method": m, "stats": score::stats(a, now)}));
        let latest_v = latest.as_ref().map(|(v, ts, c)| json!({"version": v, "updated": util::fmt_ms(*ts as u64), "content": c}));
        let mut best = Value::Null;
        if let Some(v) = best_hash {
            // render the playbook version recorded by the best run, if it is a known Automerge version
            let mut rendered = None;
            if let (Ok(h), Some(doc)) = (playbook::parse_heads(&v), self.current_doc().await)
                && let Some(mut l) = self.load_playbook(&doc, source_key).await?
                    && playbook::has_heads(&mut l.doc, &h) {
                        rendered = Some(playbook::text_at(&l.doc, &h)?);
                    }
            best = match rendered {
                Some(c) => json!({"version": v, "content": c, "historical": true}),
                None => match &latest_v {
                    Some(lv) => {
                        let mut lv = lv.clone();
                        lv["note"] = json!(format!("recorded version {v} is not renderable (legacy/unknown); showing latest"));
                        lv
                    }
                    None => Value::Null,
                },
            };
        }
        Ok(json!({
            "stats": agg.as_ref().map(|a| score::stats(a, now)),
            "best_method": best_method,
            "methods": methods.iter().map(|(m, a)| json!({"method": m, "stats": score::stats(a, now)})).collect::<Vec<_>>(),
            "playbook": {
                "latest": latest_v,
                "best": best,
            },
        }))
    }

    pub(super) async fn next(
        self: &Arc<Self>,
        agent: String,
        sources: Option<Vec<String>>,
        claim: bool,
        ttl_secs: Option<u64>,
        auto_renew: bool,
    ) -> Result<Value> {
        let doc = self.doc().await?;
        self.materialize_once().await?;
        let (cands, urls): (Vec<String>, Vec<Option<String>>) = match sources {
            Some(v) => v.into_iter().map(|s| s.trim().to_string()).filter(|s| !s.is_empty() && !s.starts_with('#')).map(|s| (s, None)).unzip(),
            None => self.dbq(db::queue_list).await?.into_iter().unzip(),
        };
        let me = self.author_for(&agent).await?.to_bytes();
        let now = util::now_ms();
        let mut free = Vec::new();
        let mut skipped = 0usize;
        for (src, url) in cands.iter().zip(urls) {
            if self.read_done(&doc, src).await?.is_some() {
                skipped += 1;
                continue;
            }
            let claims = self.read_claims(&doc, src).await?;
            if let Some(w) = claims::winner(&claims, now)
                && claims[w].0 != me {
                    skipped += 1;
                    continue;
                }
            let s = src.clone();
            let agg = self.dbq(move |c| db::source_agg(c, &s)).await?;
            free.push((src.clone(), agg, url));
        }
        let ranked = score::rank(&free.iter().map(|(k, a, _)| (k.clone(), a.clone())).collect::<Vec<_>>());
        let url_of = |k: &str| free.iter().find(|f| f.0 == k).and_then(|f| f.2.clone());
        for (src, score, explore) in &ranked {
            let mut out = json!({"ok": true, "source_key": src, "source_url": url_of(src), "score": score, "exploration": explore,
                                  "candidates": cands.len(), "skipped": skipped});
            if claim {
                let r = self.claim(agent.clone(), src.clone(), url_of(src), None, ttl_secs, auto_renew).await?;
                if r["result"] != "won" {
                    continue; // lost the race / became done: try the next one
                }
                out["result"] = json!("won");
                out["claim"] = r;
            } else {
                out["result"] = json!("candidate");
            }
            if let (Value::Object(o), Value::Object(i)) = (&mut out, self.source_info(src).await?) {
                o.extend(i);
            }
            return Ok(out);
        }
        Ok(json!({"ok": true, "result": "none", "candidates": cands.len(), "skipped": skipped}))
    }
}
