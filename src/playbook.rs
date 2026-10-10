//! Co-edited source playbooks: one Automerge document (root key `text`, a Text object) per source.
//!
//! Transport: every author stores its *full* `save()` of the merged document under its own
//! `howto/<src>/` entry (iroh-docs keeps one entry per author per key). Readers load every
//! author's save and merge them. Automerge deduplicates identical changes by hash, so overlap
//! between saves costs only bytes, never correctness.
//!
//! All nodes start from the same deterministic *genesis* change (fixed actor, time 0) that creates
//! the text object. Without it, two nodes creating `text` independently would produce two
//! conflicting objects and one side's text would be hidden.

use anyhow::{Context, Result, anyhow, bail};
use automerge::{
    ActorId, AutoCommit, ChangeHash, LoadOptions, ObjId, ObjType, ROOT, ReadDoc, TextEncoding,
    transaction::{CommitOptions, Transactable},
};

pub const MAGIC: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];
const ENC: TextEncoding = TextEncoding::UnicodeCodePoint;

pub fn is_automerge(b: &[u8]) -> bool {
    b.len() >= 4 && b[..4] == MAGIC
}

fn fixed_actor(label: &[u8]) -> ActorId {
    ActorId::from(&blake3::hash(label).as_bytes()[..16])
}

/// Deterministic genesis: identical bytes (and change hash) on every node.
pub fn genesis() -> AutoCommit {
    let mut d = AutoCommit::new_with_encoding(ENC).with_actor(fixed_actor(b"labelsync/playbook/genesis/v1"));
    d.put_object(ROOT, "text", ObjType::Text).expect("put text");
    d.commit_with(CommitOptions::default().with_time(0).with_message("genesis"));
    d
}

pub fn text_obj(d: &AutoCommit) -> Result<ObjId> {
    match d.get(ROOT, "text")? {
        Some((automerge::Value::Object(ObjType::Text), id)) => Ok(id),
        _ => bail!("playbook document has no text object"),
    }
}

pub fn text(d: &AutoCommit) -> Result<String> {
    Ok(d.text(text_obj(d)?)?)
}

pub fn text_at(d: &AutoCommit, heads: &[ChangeHash]) -> Result<String> {
    let obj = text_obj(d)?;
    Ok(d.text_at(obj, heads)?)
}

pub fn heads_str(heads: &[ChangeHash]) -> String {
    let mut v: Vec<String> = heads.iter().map(|h| h.to_string()).collect();
    v.sort();
    v.join(",")
}

pub fn parse_heads(s: &str) -> Result<Vec<ChangeHash>> {
    s.split(',')
        .map(|h| h.trim())
        .filter(|h| !h.is_empty())
        .map(|h| h.parse::<ChangeHash>().map_err(|e| anyhow!("bad change hash {h:?}: {e}")))
        .collect()
}

pub fn has_heads(d: &mut AutoCommit, heads: &[ChangeHash]) -> bool {
    !heads.is_empty() && heads.iter().all(|h| d.get_change_by_hash(h).is_some())
}

/// Build the merged doc from every author's save, plus a deterministic conversion of a legacy
/// (phase 1.5, plain markdown) playbook if one exists.
pub fn load_merged(saves: &[&[u8]], legacy_md: Option<(&str, &str)>) -> Result<AutoCommit> {
    let mut doc = genesis();
    for s in saves {
        let mut d = AutoCommit::load_with_options(s, LoadOptions::new().text_encoding(ENC)).context("loading playbook save")?;
        doc.merge(&mut d)?;
    }
    if let Some((source_key, md)) = legacy_md {
        // deterministic: same legacy content -> same change hash on every node -> merges once
        let mut f = genesis();
        f.set_actor(fixed_actor(format!("labelsync/playbook/legacy/{source_key}/{md}").as_bytes()));
        let obj = text_obj(&f)?;
        f.splice_text(&obj, 0, 0, md)?;
        f.commit_with(CommitOptions::default().with_time(0).with_message("migrated legacy playbook"));
        doc.merge(&mut f)?;
    }
    Ok(doc)
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PutStats {
    pub inserted_chars: usize,
    pub deleted_chars: usize,
    pub splices: usize,
}

/// Apply `new_md` as minimal splices against the text at `base`, as a new change by `actor`,
/// then merge it into `doc`. Concurrent edits made after `base` are preserved.
pub fn put(doc: &mut AutoCommit, actor: ActorId, base: &[ChangeHash], new_md: &str, time_ms: i64, message: &str) -> Result<PutStats> {
    let mut fork = doc.fork_at(base).context("base version not found in this playbook")?;
    fork.set_actor(actor);
    let obj = text_obj(&fork)?;
    let old = fork.text(&obj)?;
    let mut st = PutStats::default();
    if old == new_md {
        return Ok(st);
    }
    let mut pos = 0usize; // in unicode code points
    for chunk in dissimilar::diff(&old, new_md) {
        match chunk {
            dissimilar::Chunk::Equal(s) => pos += s.chars().count(),
            dissimilar::Chunk::Delete(s) => {
                let n = s.chars().count();
                fork.splice_text(&obj, pos, n as isize, "")?;
                st.deleted_chars += n;
                st.splices += 1;
            }
            dissimilar::Chunk::Insert(s) => {
                fork.splice_text(&obj, pos, 0, s)?;
                let n = s.chars().count();
                pos += n;
                st.inserted_chars += n;
                st.splices += 1;
            }
        }
    }
    fork.commit_with(CommitOptions::default().with_time(time_ms).with_message(message));
    doc.merge(&mut fork)?;
    Ok(st)
}

/// Per-put actor: author id (32 bytes) + time (8 bytes). A fresh actor per put avoids reusing a
/// sequence number when editing from an older base.
pub fn actor_for(author: &[u8; 32], now_ms: u64) -> ActorId {
    let mut b = author.to_vec();
    b.extend_from_slice(&now_ms.to_be_bytes());
    ActorId::from(b)
}

/// Author id hex if the actor was produced by [`actor_for`].
pub fn actor_author(actor: &ActorId) -> Option<String> {
    let b = actor.to_bytes();
    (b.len() == 40).then(|| hex::encode(&b[..32]))
}

pub struct LogEntry {
    pub hash: String,
    pub author: Option<String>,
    pub actor: String,
    pub time_ms: i64,
    pub message: Option<String>,
    pub deps: Vec<String>,
}

pub fn log(doc: &mut AutoCommit) -> Vec<LogEntry> {
    doc.get_changes(&[])
        .iter()
        .map(|c| LogEntry {
            hash: c.hash().to_string(),
            author: actor_author(c.actor_id()),
            actor: c.actor_id().to_hex_string(),
            time_ms: c.timestamp(),
            message: c.message().map(|s| s.to_string()),
            deps: c.deps().iter().map(|d| d.to_string()).collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "# Playbook\n\nIntro paragraph.\n\nRate limits: unknown.\n\nTips: none yet.\n";

    fn a(n: u8) -> ActorId {
        actor_for(&[n; 32], 1000 + n as u64)
    }

    /// Simulate two nodes: each starts from the shared saves, edits, saves; then both merge.
    fn two_node_edit(edit_a: &str, edit_b: &str) -> (String, String) {
        let mut seed = load_merged(&[], None).unwrap();
        let base_heads = seed.get_heads();
        put(&mut seed, a(9), &base_heads, BASE, 1, "base").unwrap();
        let seed_save = seed.save();

        let mut na = load_merged(&[&seed_save], None).unwrap();
        let mut nb = load_merged(&[&seed_save], None).unwrap();
        let base = na.get_heads();
        assert_eq!(base, nb.get_heads());
        put(&mut na, a(1), &base, edit_a, 2, "A").unwrap();
        put(&mut nb, a(2), &base, edit_b, 3, "B").unwrap();
        let (sa, sb) = (na.save(), nb.save());
        // node A sees [A, B], node B sees [B, A]
        let mut ma = load_merged(&[&sa, &sb], None).unwrap();
        let mut mb = load_merged(&[&sb, &sa], None).unwrap();
        assert_eq!(ma.get_heads().len(), 2);
        assert_eq!(heads_str(&ma.get_heads()), heads_str(&mb.get_heads()));
        (text(&ma).unwrap(), text(&mb).unwrap())
    }

    #[test]
    fn genesis_is_deterministic() {
        assert_eq!(genesis().get_heads(), genesis().get_heads());
        assert_eq!(genesis().save(), genesis().save());
    }

    #[test]
    fn concurrent_edits_on_different_paragraphs_both_survive() {
        let ea = BASE.replace("Rate limits: unknown.", "Rate limits: 5 req/s, back off on 429.");
        let eb = BASE.replace("Tips: none yet.", "Tips: use the CSV export, not the HTML pages.");
        let (ta, tb) = two_node_edit(&ea, &eb);
        assert_eq!(ta, tb);
        assert!(ta.contains("5 req/s, back off on 429."), "{ta}");
        assert!(ta.contains("use the CSV export"), "{ta}");
        assert!(ta.contains("Intro paragraph."));
        assert!(!ta.contains("unknown.") && !ta.contains("none yet."), "{ta}");
    }

    #[test]
    fn same_line_conflict_is_deterministic_and_converges() {
        let ea = BASE.replace("Rate limits: unknown.", "Rate limits: 10/s.");
        let eb = BASE.replace("Rate limits: unknown.", "Rate limits: 2/s per IP.");
        let (ta, tb) = two_node_edit(&ea, &eb);
        assert_eq!(ta, tb, "both nodes converge");
        let (ta2, _) = two_node_edit(&ea, &eb);
        assert_eq!(ta, ta2, "deterministic across runs");
        assert!(ta.contains("Tips: none yet."));
        assert!(ta.contains("10") && ta.contains("2/s"), "no edit lost: {ta}");
    }

    #[test]
    fn history_and_noop_and_bad_base() {
        let mut d = load_merged(&[], None).unwrap();
        let g = d.get_heads();
        put(&mut d, a(1), &g, "v1\n", 5, "one").unwrap();
        let v1 = d.get_heads();
        put(&mut d, a(2), &v1, "v1\nv2\n", 6, "two").unwrap();
        assert_eq!(text_at(&d, &v1).unwrap(), "v1\n");
        assert_eq!(text(&d).unwrap(), "v1\nv2\n");
        let h = d.get_heads();
        assert_eq!(put(&mut d, a(3), &h, "v1\nv2\n", 7, "noop").unwrap(), PutStats::default());
        assert_eq!(d.get_heads(), h);
        assert_eq!(parse_heads(&heads_str(&h)).unwrap(), h);
        assert!(has_heads(&mut d, &v1));
        let bogus = parse_heads(&"ab".repeat(32)).unwrap();
        assert!(!has_heads(&mut d, &bogus));
        let l = log(&mut d);
        assert_eq!(l.len(), 3); // genesis + 2
        assert_eq!(l[1].author.as_deref(), Some(hex::encode([1u8; 32]).as_str()));
    }

    #[test]
    fn legacy_conversion_is_deterministic() {
        let x = load_merged(&[], Some(("src", "# old md\n"))).unwrap();
        let mut x2 = load_merged(&[], Some(("src", "# old md\n"))).unwrap();
        assert_eq!(text(&x).unwrap(), "# old md\n");
        let mut x = x;
        assert_eq!(x.get_heads(), x2.get_heads());
        // a node that already saved the converted doc merges cleanly with fresh conversion
        let s = x.save();
        let mut y = load_merged(&[&s], Some(("src", "# old md\n"))).unwrap();
        assert_eq!(text(&y).unwrap(), "# old md\n");
        assert_eq!(y.get_heads(), x2.get_heads());
    }
}
