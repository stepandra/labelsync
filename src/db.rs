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
