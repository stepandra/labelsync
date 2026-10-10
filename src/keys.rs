//! iroh-docs key layout.
//!
//! iroh-docs deletes *same-author* entries whose key has the newly written key as a
//! prefix ("prefix delete" semantics). To make sure `claim/foo` never wipes
//! `claim/foobar`, variable-length keys get a trailing `/` terminator and source keys
//! are percent-encoded so they never contain `/`.
//!
//! * `seg/<chain>/<enc source_key>/<blake3 hex of canonical TSV>`
//! * `claim/<enc source_key>/`
//! * `done/<enc source_key>/`
//! * `src/<blake3 hex of raw source file>`
//! * `agent/<author hex>`

pub const SEG: &str = "seg/";
pub const CLAIM: &str = "claim/";
pub const DONE: &str = "done/";
pub const SRC: &str = "src/";
pub const AGENT: &str = "agent/";
pub const HOWTO: &str = "howto/";
pub const RUNS: &str = "runs/";
pub const QUEUE: &str = "queue/";

fn keep(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"._~:@+=,-".contains(&b)
}

pub fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if keep(b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

pub fn dec(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len()
            && let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn validate_source_key(k: &str) -> Result<(), String> {
    if k.trim().is_empty() {
        return Err("source_key must not be empty".into());
    }
    if k.len() > 512 {
        return Err("source_key too long (max 512 bytes)".into());
    }
    Ok(())
}

pub fn validate_chain(c: &str) -> Result<(), String> {
    if c.is_empty() || c.len() > 64 {
        return Err(format!("invalid chain '{c}'"));
    }
    if !c.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.') {
        return Err(format!("invalid chain '{c}': use [a-z0-9._-]"));
    }
    Ok(())
}

pub fn seg_key(chain: &str, source_key: &str, hash_hex: &str) -> String {
    format!("{SEG}{chain}/{}/{hash_hex}", enc(source_key))
}

/// Parse `seg/<chain>/<enc>/<hash>` -> (chain, source_key, hash)
pub fn parse_seg_key(key: &[u8]) -> Option<(String, String, String)> {
    let s = std::str::from_utf8(key).ok()?;
    let rest = s.strip_prefix(SEG)?;
    let mut parts = rest.splitn(3, '/');
    let chain = parts.next()?.to_string();
    let src = dec(parts.next()?);
    let hash = parts.next()?.to_string();
    if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some((chain, src, hash))
}

pub fn claim_key(source_key: &str) -> String {
    format!("{CLAIM}{}/", enc(source_key))
}

pub fn done_key(source_key: &str) -> String {
    format!("{DONE}{}/", enc(source_key))
}

pub fn parse_claim_or_done_key(key: &[u8], prefix: &str) -> Option<String> {
    let s = std::str::from_utf8(key).ok()?;
    let rest = s.strip_prefix(prefix)?.strip_suffix('/')?;
    Some(dec(rest))
}

pub fn src_key(hash_hex: &str) -> String {
    format!("{SRC}{}", hash_hex.to_ascii_lowercase())
}

pub fn howto_key(source_key: &str) -> String {
    format!("{HOWTO}{}/", enc(source_key))
}

pub fn queue_key(source_key: &str) -> String {
    format!("{QUEUE}{}/", enc(source_key))
}

pub fn run_key(source_key: &str, agent: &str, ts_ms: u64) -> String {
    format!("{RUNS}{}/{}/{ts_ms}/", enc(source_key), enc(agent))
}

/// `runs/<enc src>/<enc agent>/<ts>/` -> (source_key, agent, ts)
pub fn parse_run_key(key: &[u8]) -> Option<(String, String, u64)> {
    let s = std::str::from_utf8(key).ok()?;
    let rest = s.strip_prefix(RUNS)?.strip_suffix('/')?;
    let mut p = rest.split('/');
    let (src, agent, ts) = (p.next()?, p.next()?, p.next()?);
    if p.next().is_some() {
        return None;
    }
    Some((dec(src), dec(agent), ts.parse().ok()?))
}

pub fn agent_key(author_hex: &str) -> String {
    format!("{AGENT}{author_hex}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        for k in ["etherscan/labels/page=1", "a b%c", "простой", "x"] {
            let e = enc(k);
            assert!(!e.contains('/'));
            assert_eq!(dec(&e), k);
        }
    }

    #[test]
    fn claim_keys_not_prefix_of_each_other() {
        let a = claim_key("foo");
        let b = claim_key("foobar");
        assert!(!b.starts_with(&a));
        assert_eq!(parse_claim_or_done_key(b.as_bytes(), CLAIM).unwrap(), "foobar");
    }

    #[test]
    fn run_key_parse() {
        let k = run_key("a/b", "hound a", 123);
        assert_eq!(parse_run_key(k.as_bytes()).unwrap(), ("a/b".into(), "hound a".into(), 123));
        assert!(!howto_key("x").starts_with(&howto_key("xy")) && !howto_key("xy").starts_with(&howto_key("x")));
    }

    #[test]
    fn seg_key_parse() {
        let h = "a".repeat(64);
        let k = seg_key("eth", "ds/one", &h);
        let (c, s, hh) = parse_seg_key(k.as_bytes()).unwrap();
        assert_eq!((c.as_str(), s.as_str(), hh.as_str()), ("eth", "ds/one", h.as_str()));
    }
}
