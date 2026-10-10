//! Local SQLite materialization of all segments in the namespace.
//!
//! The daemon is the only writer. Agents may open `labels.db` read-only
//! (`sqlite3 'file:labels.db?mode=ro'`); WAL mode lets them read while the daemon writes.

use std::{
    collections::HashSet,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

use crate::{segment::Row, util};

const SCHEMA: &str = r#"
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
-- iroh-docs authors (one per agent); name comes from the agent/<author> entry
CREATE TABLE IF NOT EXISTS authors(
  id INTEGER PRIMARY KEY,
  author TEXT NOT NULL UNIQUE,
  name TEXT
);
CREATE TABLE IF NOT EXISTS segments(
  id INTEGER PRIMARY KEY,
  key TEXT NOT NULL,
  author_id INTEGER NOT NULL REFERENCES authors(id),
  hash TEXT NOT NULL,
  chain TEXT NOT NULL,
  source_key TEXT NOT NULL,
  rows INTEGER NOT NULL,
  entry_ts INTEGER NOT NULL,
  materialized_at INTEGER NOT NULL,
  UNIQUE(key, author_id)
);
CREATE INDEX IF NOT EXISTS segments_source ON segments(source_key);
-- label identity: (chain, address_normalized, label_source, label)
CREATE TABLE IF NOT EXISTS labels(
  id INTEGER PRIMARY KEY,
  chain TEXT NOT NULL,
  address TEXT NOT NULL,
  label_source TEXT NOT NULL,
  label TEXT NOT NULL,
  UNIQUE(chain, address, label_source, label)
);
CREATE INDEX IF NOT EXISTS labels_addr ON labels(address);
-- provenance: who asserted / revoked what, from which segment
CREATE TABLE IF NOT EXISTS provenance(
  label_id INTEGER NOT NULL REFERENCES labels(id),
  author_id INTEGER NOT NULL REFERENCES authors(id),
  segment_id INTEGER NOT NULL REFERENCES segments(id),
  op TEXT NOT NULL CHECK(op IN ('add','del')),
  category TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  eff_at INTEGER NOT NULL,
  PRIMARY KEY(label_id, author_id, segment_id, op, category, observed_at)
) WITHOUT ROWID;
-- experience sharing (phase 1.5)
DROP TABLE IF EXISTS howto; -- phase 1.5 per-author table, replaced by merged playbooks
-- merged Automerge playbook per source (version = sorted change-hash heads, comma-separated)
CREATE TABLE IF NOT EXISTS playbooks(
  source_key TEXT PRIMARY KEY,
  version TEXT NOT NULL,
  sig TEXT NOT NULL,
  ts INTEGER NOT NULL,
  content TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs(
  key TEXT PRIMARY KEY,
  author TEXT NOT NULL,
  source_key TEXT NOT NULL,
  agent TEXT NOT NULL,
  ts INTEGER NOT NULL,
  rows_total INTEGER,
  rows_new INTEGER,
  duration_s REAL,
  errors INTEGER,
  retries INTEGER,
  method TEXT,
  howto_version TEXT,
  notes TEXT,
  json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS runs_source ON runs(source_key, method);
CREATE TABLE IF NOT EXISTS queue(
  source_key TEXT NOT NULL,
  author TEXT NOT NULL,
  ts INTEGER NOT NULL,
  removed INTEGER NOT NULL,
  source_url TEXT,
  PRIMARY KEY(source_key, author)
);
CREATE VIEW IF NOT EXISTS source_stats AS
  SELECT source_key, count(*) AS runs, sum(COALESCE(rows_total,0)) AS rows_total,
    sum(COALESCE(rows_new,0)) AS rows_new, sum(COALESCE(duration_s,0)) AS duration_s,
    sum(COALESCE(errors,0)) AS errors, sum(COALESCE(retries,0)) AS retries, max(ts) AS last_ts,
    CASE WHEN sum(COALESCE(duration_s,0)) > 0 THEN sum(COALESCE(rows_new,0)) * 60.0 / sum(duration_s) END AS new_per_min,
    sum(COALESCE(errors,0)) * 1.0 / count(*) AS error_rate
  FROM runs GROUP BY source_key;
CREATE VIEW IF NOT EXISTS method_stats AS
  SELECT source_key, COALESCE(method,'') AS method, count(*) AS runs, sum(COALESCE(rows_total,0)) AS rows_total,
    sum(COALESCE(rows_new,0)) AS rows_new, sum(COALESCE(duration_s,0)) AS duration_s,
    sum(COALESCE(errors,0)) AS errors, sum(COALESCE(retries,0)) AS retries, max(ts) AS last_ts,
    CASE WHEN sum(COALESCE(duration_s,0)) > 0 THEN sum(COALESCE(rows_new,0)) * 60.0 / sum(duration_s) END AS new_per_min,
    sum(COALESCE(errors,0)) * 1.0 / count(*) AS error_rate
  FROM runs GROUP BY source_key, COALESCE(method,'');
CREATE VIEW IF NOT EXISTS provenance_v AS
  SELECT l.chain, l.address, l.label_source, l.label, p.op, p.category,
         COALESCE(a.name, substr(a.author,1,12)) AS agent, a.author, s.source_key,
         p.observed_at, p.eff_at, s.hash AS segment
  FROM provenance p JOIN labels l ON l.id = p.label_id
  JOIN authors a ON a.id = p.author_id JOIN segments s ON s.id = p.segment_id;
-- A label is active if, for at least one author, that author's latest assertion is 'add'
-- (LWW per (label identity, author); a 'del' at the same eff_at wins).
CREATE VIEW IF NOT EXISTS active_labels AS
  SELECT l.id, l.chain, l.address, l.label_source, l.label,
    COALESCE((SELECT p.category FROM provenance p WHERE p.label_id=l.id AND p.op='add' AND p.category<>''
              ORDER BY p.eff_at DESC, p.category LIMIT 1), '') AS category,
    (SELECT group_concat(DISTINCT COALESCE(a.name, substr(a.author,1,12)))
       FROM provenance p JOIN authors a ON a.id=p.author_id
       WHERE p.label_id=l.id AND p.op='add') AS agents,
    (SELECT min(eff_at) FROM provenance p WHERE p.label_id=l.id) AS first_seen,
    (SELECT max(eff_at) FROM provenance p WHERE p.label_id=l.id) AS last_seen
  FROM labels l
  WHERE EXISTS (
    SELECT 1 FROM provenance p WHERE p.label_id=l.id AND p.op='add'
      AND NOT EXISTS (SELECT 1 FROM provenance d WHERE d.label_id=l.id AND d.op='del'
                      AND d.author_id=p.author_id AND d.eff_at>=p.eff_at));
"#;

pub fn open(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(std::time::Duration::from_secs(10))?;
    conn.execute_batch(SCHEMA)?;
    Ok(conn)
}

pub fn materialized_set(conn: &Connection) -> Result<HashSet<(String, String)>> {
    let mut st = conn.prepare("SELECT s.key, a.author FROM segments s JOIN authors a ON a.id = s.author_id")?;
    let rows = st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}

pub struct SegMeta<'a> {
    pub key: &'a str,
    pub author: &'a str,
    pub hash: &'a str,
    pub chain: &'a str,
    pub source_key: &'a str,
    pub entry_ts_ms: u64,
}

fn author_id(conn: &Connection, author: &str) -> Result<i64> {
    conn.execute("INSERT INTO authors(author) VALUES(?1) ON CONFLICT(author) DO NOTHING", [author])?;
    Ok(conn.query_row("SELECT id FROM authors WHERE author=?1", [author], |r| r.get(0))?)
}

/// Insert all rows of one segment in a single transaction. Idempotent.
pub fn materialize_segment(conn: &mut Connection, m: &SegMeta, rows: &[Row]) -> Result<usize> {
    let tx = conn.transaction()?;
    let mut new_prov = 0usize;
    {
        let aid = author_id(&tx, m.author)?;
        tx.execute(
            "INSERT OR IGNORE INTO segments(key,author_id,hash,chain,source_key,rows,entry_ts,materialized_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![m.key, aid, m.hash, m.chain, m.source_key, rows.len() as i64, m.entry_ts_ms as i64, util::now_ms() as i64],
        )?;
        let sid: i64 = tx.query_row(
            "SELECT id FROM segments WHERE key=?1 AND author_id=?2",
            params![m.key, aid],
            |r| r.get(0),
        )?;
        let mut ins_label = tx.prepare_cached(
            "INSERT INTO labels(chain,address,label_source,label) VALUES(?1,?2,?3,?4)
             ON CONFLICT(chain,address,label_source,label) DO NOTHING",
        )?;
        let mut get_label = tx.prepare_cached(
            "SELECT id FROM labels WHERE chain=?1 AND address=?2 AND label_source=?3 AND label=?4",
        )?;
        let mut ins_prov = tx.prepare_cached(
            "INSERT OR IGNORE INTO provenance(label_id,author_id,segment_id,op,category,observed_at,eff_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7)",
        )?;
        for r in rows {
            ins_label.execute(params![r.chain, r.address, r.label_source, r.label])?;
            let id: i64 = get_label.query_row(params![r.chain, r.address, r.label_source, r.label], |x| x.get(0))?;
            let eff = util::parse_time_ms(&r.observed_at).unwrap_or(m.entry_ts_ms) as i64;
            new_prov += ins_prov.execute(params![id, aid, sid, r.op, r.category, r.observed_at, eff])?;
        }
    }
    tx.commit()?;
    Ok(new_prov)
}

pub fn upsert_agent(conn: &Connection, author: &str, name: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO authors(author,name) VALUES(?1,?2) ON CONFLICT(author) DO UPDATE SET name=excluded.name",
        params![author, name],
    )?;
    Ok(())
}

#[derive(Debug, Serialize, Default)]
pub struct Stats {
    pub segments: i64,
    pub labels: i64,
    pub active_labels: i64,
    pub provenance_rows: i64,
    pub chains: Vec<(String, i64)>,
}

pub fn stats(conn: &Connection) -> Result<Stats> {
    let one = |q: &str| -> Result<i64> { Ok(conn.query_row(q, [], |r| r.get(0))?) };
    let mut st = conn.prepare("SELECT chain, count(*) FROM labels GROUP BY chain ORDER BY chain")?;
    let chains = st
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?;
    Ok(Stats {
        segments: one("SELECT count(*) FROM segments")?,
        labels: one("SELECT count(*) FROM labels")?,
        active_labels: one("SELECT count(*) FROM active_labels")?,
        provenance_rows: one("SELECT count(*) FROM provenance")?,
        chains,
    })
}

pub fn segments_for_source(conn: &Connection, source_key: &str) -> Result<Vec<String>> {
    let mut st = conn.prepare("SELECT DISTINCT key FROM segments WHERE source_key=?1 ORDER BY key")?;
    let v = st
        .query_map([source_key], |r| r.get(0))?
        .collect::<std::result::Result<_, _>>()?;
    Ok(v)
}

pub const EXPORT_HEADER: &str = "chain\taddress\tlabel\tlabel_source\tcategory\tagents\tfirst_seen\tlast_seen";

#[derive(Debug, Serialize)]
pub struct ExportedFile {
    pub chain: String,
    pub path: PathBuf,
    pub rows: u64,
}

/// Export active labels to `<out_dir>/<chain>.tsv`, each written atomically.
pub fn export(conn: &Connection, out_dir: &Path, chain: Option<&str>) -> Result<Vec<ExportedFile>> {
    let chains: Vec<String> = match chain {
        Some(c) => vec![c.to_string()],
        None => {
            let mut st = conn.prepare("SELECT DISTINCT chain FROM labels ORDER BY chain")?;
            st.query_map([], |r| r.get(0))?.collect::<std::result::Result<_, _>>()?
        }
    };
    let mut out = Vec::new();
    for c in chains {
        let path = out_dir.join(format!("{c}.tsv"));
        let mut n = 0u64;
        let mut st = conn.prepare(
            "SELECT chain,address,label,label_source,category,COALESCE(agents,''),first_seen,last_seen
             FROM active_labels WHERE chain=?1 ORDER BY address,label_source,label",
        )?;
        util::atomic_write_with(&path, |w| {
            writeln!(w, "{EXPORT_HEADER}")?;
            let mut rows = st.query([&c]).map_err(std::io::Error::other)?;
            while let Some(r) = rows.next().map_err(std::io::Error::other)? {
                let g = |i| r.get::<_, String>(i).map_err(std::io::Error::other);
                let first: i64 = r.get(6).map_err(std::io::Error::other)?;
                let last: i64 = r.get(7).map_err(std::io::Error::other)?;
                writeln!(
                    w,
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    g(0)?, g(1)?, g(2)?, g(3)?, g(4)?, g(5)?,
                    util::fmt_ms(first as u64), util::fmt_ms(last as u64)
                )?;
                n += 1;
            }
            Ok(())
        })?;
        out.push(ExportedFile { chain: c, path, rows: n });
    }
    Ok(out)
}

pub fn agent_name(conn: &Connection, author: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row("SELECT name FROM authors WHERE author=?1", [author], |r| r.get::<_, Option<String>>(0))
        .optional()?.flatten())
}

// ---------- experience sharing ----------

pub fn upsert_playbook(conn: &Connection, source_key: &str, version: &str, sig: &str, ts: u64, content: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO playbooks(source_key,version,sig,ts,content) VALUES(?1,?2,?3,?4,?5)
         ON CONFLICT(source_key) DO UPDATE SET version=excluded.version, sig=excluded.sig, ts=excluded.ts, content=excluded.content",
        params![source_key, version, sig, ts as i64, content],
    )?;
    Ok(())
}

pub fn playbook_sig(conn: &Connection, source_key: &str) -> Result<Option<String>> {
    Ok(conn.query_row("SELECT sig FROM playbooks WHERE source_key=?1", [source_key], |r| r.get(0)).optional()?)
}

/// (version, ts, content)
pub fn latest_playbook(conn: &Connection, source_key: &str) -> Result<Option<(String, i64, String)>> {
    Ok(conn
        .query_row("SELECT version, ts, content FROM playbooks WHERE source_key=?1", [source_key], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .optional()?)
}

pub fn run_keys(conn: &Connection) -> Result<HashSet<String>> {
    let mut st = conn.prepare("SELECT key FROM runs")?;
    let v = st.query_map([], |r| r.get(0))?.collect::<std::result::Result<_, _>>()?;
    Ok(v)
}

pub fn insert_run(conn: &Connection, key: &str, author: &str, rec: &serde_json::Value) -> Result<()> {
    let i = |k: &str| rec.get(k).and_then(|v| v.as_f64()).map(|f| f as i64);
    let f = |k: &str| rec.get(k).and_then(|v| v.as_f64());
    let t = |k: &str| rec.get(k).and_then(|v| v.as_str()).map(|s| s.to_string());
    conn.execute(
        "INSERT OR IGNORE INTO runs(key,author,source_key,agent,ts,rows_total,rows_new,duration_s,errors,retries,method,howto_version,notes,json)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            key, author, t("source_key").unwrap_or_default(), t("agent").unwrap_or_default(), i("ts").unwrap_or(0),
            i("rows_total"), i("rows_new_after_dedup"), f("duration_s"), i("errors"), i("retries"),
            t("method"), t("howto_version"), t("notes"), rec.to_string()
        ],
    )?;
    Ok(())
}

pub fn upsert_queue(conn: &Connection, source_key: &str, author: &str, ts: u64, removed: bool, url: Option<&str>) -> Result<()> {
    conn.execute(
        "INSERT INTO queue(source_key,author,ts,removed,source_url) VALUES(?1,?2,?3,?4,?5)
         ON CONFLICT(source_key,author) DO UPDATE SET ts=excluded.ts, removed=excluded.removed, source_url=excluded.source_url
         WHERE excluded.ts >= queue.ts",
        params![source_key, author, ts as i64, removed as i64, url],
    )?;
    Ok(())
}

/// Active queue: sources whose newest queue entry (across authors) is not a removal.
pub fn queue_list(conn: &Connection) -> Result<Vec<(String, Option<String>)>> {
    let mut st = conn.prepare(
        "SELECT q.source_key, (SELECT source_url FROM queue u WHERE u.source_key=q.source_key AND u.source_url IS NOT NULL ORDER BY ts DESC LIMIT 1)
         FROM queue q GROUP BY q.source_key
         HAVING (SELECT removed FROM queue z WHERE z.source_key=q.source_key ORDER BY ts DESC LIMIT 1) = 0
         ORDER BY min(q.ts), q.source_key",
    )?;
    let v = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<std::result::Result<_, _>>()?;
    Ok(v)
}

fn agg_from_row(r: &rusqlite::Row) -> rusqlite::Result<crate::score::Agg> {
    Ok(crate::score::Agg {
        runs: r.get::<_, i64>("runs")? as u64,
        rows_total: r.get::<_, f64>("rows_total")?,
        rows_new: r.get::<_, f64>("rows_new")?,
        duration_s: r.get::<_, f64>("duration_s")?,
        errors: r.get::<_, f64>("errors")?,
        retries: r.get::<_, f64>("retries")?,
        last_ts_ms: r.get::<_, i64>("last_ts")? as u64,
    })
}

pub fn source_agg(conn: &Connection, source_key: &str) -> Result<Option<crate::score::Agg>> {
    Ok(conn
        .query_row("SELECT * FROM source_stats WHERE source_key=?1", [source_key], agg_from_row)
        .optional()?)
}

pub fn method_aggs(conn: &Connection, source_key: &str) -> Result<Vec<(String, crate::score::Agg)>> {
    let mut st = conn.prepare("SELECT * FROM method_stats WHERE source_key=?1 ORDER BY method")?;
    let v = st
        .query_map([source_key], |r| Ok((r.get::<_, String>("method")?, agg_from_row(r)?)))?
        .collect::<std::result::Result<_, _>>()?;
    Ok(v)
}

/// howto_version used by the run with the best new-labels-per-minute for this source.
pub fn best_howto_version(conn: &Connection, source_key: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT howto_version FROM runs WHERE source_key=?1 AND howto_version IS NOT NULL AND duration_s > 0
             ORDER BY COALESCE(rows_new,0) * 1.0 / duration_s DESC, ts DESC LIMIT 1",
            [source_key],
            |r| r.get(0),
        )
        .optional()?)
}

/// Rows contributed by `author` to `source_key` in segments written at/after `since_ms`:
/// (segment keys, rows_total, rows_new_after_dedup). "New" = labels with no provenance from any other segment.
pub fn run_rows(conn: &Connection, source_key: &str, author: &str, since_ms: u64) -> Result<(Vec<String>, i64, i64)> {
    let mut st = conn.prepare(
        "SELECT s.id, s.key, s.rows FROM segments s JOIN authors a ON a.id=s.author_id
         WHERE s.source_key=?1 AND a.author=?2 AND s.entry_ts >= ?3 ORDER BY s.key",
    )?;
    let segs: Vec<(i64, String, i64)> = st
        .query_map(params![source_key, author, since_ms as i64], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<std::result::Result<_, _>>()?;
    if segs.is_empty() {
        return Ok((vec![], 0, 0));
    }
    let ids = segs.iter().map(|s| s.0.to_string()).collect::<Vec<_>>().join(",");
    let total: i64 = segs.iter().map(|s| s.2).sum();
    let new: i64 = conn.query_row(
        &format!(
            "SELECT count(DISTINCT p.label_id) FROM provenance p WHERE p.segment_id IN ({ids}) AND p.op='add'
             AND NOT EXISTS (SELECT 1 FROM provenance q WHERE q.label_id=p.label_id AND q.segment_id NOT IN ({ids}))"
        ),
        [],
        |r| r.get(0),
    )?;
    Ok((segs.into_iter().map(|s| s.1).collect(), total, new))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::{Defaults, Format, parse_input, segments_from_parsed};

    #[test]
    fn dedup_and_tombstones() {
        let dir = std::env::temp_dir().join(format!("lsdbtest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut conn = open(&dir.join("t.db")).unwrap();
        let d = Defaults { source_key: Some("s".into()), chain: Some("eth".into()), ..Default::default() };
        let data = "address\tlabel\n0x0000000000000000000000000000000000000001\tA\n0x0000000000000000000000000000000000000002\tB\n";
        let segs = segments_from_parsed(parse_input(data.as_bytes(), Format::Tsv, &d).unwrap().rows, 100);
        for (author, ts) in [("aa", 1000u64), ("bb", 2000)] {
            for s in &segs {
                let rows = crate::segment::parse_segment(&s.tsv, &s.hash_hex).unwrap();
                let key = s.key();
                let m = SegMeta { key: &key, author, hash: &s.hash_hex, chain: &s.chain, source_key: "s", entry_ts_ms: ts };
                materialize_segment(&mut conn, &m, &rows).unwrap();
                // idempotent
                materialize_segment(&mut conn, &m, &rows).unwrap();
            }
        }
        let st = stats(&conn).unwrap();
        assert_eq!(st.labels, 2);
        assert_eq!(st.active_labels, 2);
        assert_eq!(st.provenance_rows, 4);
        // author aa revokes A later -> still active because bb asserts it
        let del = "address\tlabel\top\n0x0000000000000000000000000000000000000001\tA\tdel\n";
        let segs = segments_from_parsed(parse_input(del.as_bytes(), Format::Tsv, &d).unwrap().rows, 100);
        let s = &segs[0];
        let rows = crate::segment::parse_segment(&s.tsv, &s.hash_hex).unwrap();
        let key = s.key();
        let m = SegMeta { key: &key, author: "aa", hash: &s.hash_hex, chain: "eth", source_key: "s", entry_ts_ms: 3000 };
        materialize_segment(&mut conn, &m, &rows).unwrap();
        assert_eq!(stats(&conn).unwrap().active_labels, 2);
        let m = SegMeta { author: "bb", ..m };
        materialize_segment(&mut conn, &m, &rows).unwrap();
        assert_eq!(stats(&conn).unwrap().active_labels, 1);
        let files = export(&conn, &dir.join("export"), None).unwrap();
        assert_eq!(files[0].rows, 1);
        std::fs::remove_dir_all(&dir).ok();
    }
}
