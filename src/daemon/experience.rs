//! Phase 1.5: shared experience (howto playbooks, run reports, shared queue, `next`).

use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use iroh_docs::{api::Doc, store::Query};
use serde_json::{Value, json};

use super::Node;
use crate::{claims, db, keys, score, util};

fn howto_json(author: &str, agent: &str, hash: &str, ts_ms: i64, content: &str) -> Value {
    json!({"author": author, "agent": agent, "hash": hash, "ts": ts_ms, "time": util::fmt_ms(ts_ms as u64), "content": content})
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
        for e in Self::collect(doc, Query::key_prefix(keys::HOWTO).build()).await? {
            let Some(src) = keys::parse_claim_or_done_key(e.key(), keys::HOWTO) else { continue };
            let (author, hash, ts) = (e.author().to_string(), e.content_hash().to_hex().to_string(), e.timestamp() / 1000);
            let (s2, a2) = (src.clone(), author.clone());
            if self.dbq(move |c| db::howto_hash(c, &s2, &a2)).await?.as_deref() == Some(hash.as_str()) {
                continue;
            }
            let Some(b) = self.content(&e).await else { continue };
            let content = String::from_utf8_lossy(&b).into_owned();
            self.dbq(move |c| db::upsert_howto(c, &src, &author, &hash, ts, &content)).await?;
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

    pub(super) async fn howto_put(self: &Arc<Self>, agent: String, source_key: String, content: String) -> Result<Value> {
        keys::validate_source_key(&source_key).map_err(|e| anyhow!(e))?;
        if content.trim().is_empty() {
            bail!("empty playbook");
        }
        let author = self.author_for(&agent).await?;
        let doc = self.doc().await?;
        let hash = doc.set_bytes(author, keys::howto_key(&source_key), content.into_bytes()).await?;
        self.materialize_once().await?;
        Ok(json!({"ok": true, "source_key": source_key, "agent": agent, "hash": hash.to_hex().to_string()}))
    }

    /// All playbook versions (one per author), newest first.
    pub(super) async fn howto_get(self: &Arc<Self>, source_key: String) -> Result<Value> {
        let doc = self.doc().await?;
        let mut out = Vec::new();
        for e in Self::collect(&doc, Query::key_exact(keys::howto_key(&source_key)).build()).await? {
            let author = e.author().to_string();
            let a2 = author.clone();
            let agent = self.dbq(move |c| db::agent_name(c, &a2)).await?.unwrap_or_default();
            let content = match self.content(&e).await {
                Some(b) => String::from_utf8_lossy(&b).into_owned(),
                None => continue,
            };
            out.push(howto_json(&author, &agent, &e.content_hash().to_hex(), (e.timestamp() / 1000) as i64, &content));
        }
        out.sort_by(|a, b| b["ts"].as_i64().cmp(&a["ts"].as_i64()));
        Ok(json!({"ok": true, "source_key": source_key, "versions": out}))
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
                Ok((db::source_agg(c, &s)?, db::method_aggs(c, &s)?, db::best_howto_version(c, &s)?, db::latest_howto(c, &s)?))
            })
            .await?;
        let best_method = methods
            .iter()
            .max_by(|a, b| {
                let y = |m: &score::Agg| score::expected_yield(Some(m)).unwrap_or(-1.0);
                y(&a.1).partial_cmp(&y(&b.1)).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(m, a)| json!({"method": m, "stats": score::stats(a, now)}));
        let mut best = Value::Null;
        if let Some(h) = best_hash {
            let h2 = h.clone();
            if let Some((author, agent, ts, content)) = self.dbq(move |c| db::howto_by_hash(c, &h2)).await? {
                best = howto_json(&author, &agent, &h, ts, &content);
            } else if let Ok(hash) = h.parse::<iroh_blobs::Hash>() {
                // overwritten version: blob may still be in the store
                if let Ok(b) = self.blobs.get_bytes(hash).await {
                    best = json!({"hash": h, "content": String::from_utf8_lossy(&b)});
                }
            }
        }
        Ok(json!({
            "stats": agg.as_ref().map(|a| score::stats(a, now)),
            "best_method": best_method,
            "methods": methods.iter().map(|(m, a)| json!({"method": m, "stats": score::stats(a, now)})).collect::<Vec<_>>(),
            "playbook": {
                "latest": latest.map(|(a, n, h, ts, c)| howto_json(&a, &n, &h, ts, &c)),
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
