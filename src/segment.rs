//! Input parsing (TSV / CSV / JSONL) and canonical immutable segments.
//!
//! A segment is a canonical TSV: fixed header, every field normalized, rows deduplicated
//! and sorted bytewise, `\n` line endings. Its identity is the BLAKE3 hash of that
//! uncompressed canonical TSV, so the same label data always yields the same key no
//! matter which agent produced it or which zstd version compressed it. On the wire / in
//! iroh-blobs the TSV is stored zstd-compressed.

use std::{collections::BTreeMap, io::Read};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;

use crate::{keys, norm};

pub const HEADER: &str = "chain\taddress\tlabel\tlabel_source\tcategory\top\tobserved_at";
pub const DEFAULT_MAX_ROWS: usize = 50_000;
/// Hard cap on decompressed segment size (protects against zstd bombs).
pub const MAX_SEGMENT_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Row {
    pub chain: String,
    pub address: String,
    pub label: String,
    pub label_source: String,
    pub category: String,
    pub op: String,
    pub observed_at: String,
}

impl Row {
    fn write_tsv(&self, out: &mut Vec<u8>) {
        for (i, f) in [
            &self.chain,
            &self.address,
            &self.label,
            &self.label_source,
            &self.category,
            &self.op,
            &self.observed_at,
        ]
        .iter()
        .enumerate()
        {
            if i > 0 {
                out.push(b'\t');
            }
            out.extend_from_slice(f.as_bytes());
        }
        out.push(b'\n');
    }
}

#[derive(Clone, Debug, Default)]
pub struct Defaults {
    pub source_key: Option<String>,
    pub label_source: Option<String>,
    pub chain: Option<String>,
    pub observed_at: Option<String>,
    pub agent: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Auto,
    Tsv,
    Csv,
    Jsonl,
}

impl std::str::FromStr for Format {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "auto" => Format::Auto,
            "tsv" => Format::Tsv,
            "csv" => Format::Csv,
            "jsonl" | "ndjson" | "json" => Format::Jsonl,
            o => bail!("unknown format {o}"),
        })
    }
}

#[derive(Debug, Default)]
pub struct Parsed {
    /// (source_key, row)
    pub rows: Vec<(String, Row)>,
    pub rejected: usize,
    pub errors: Vec<String>,
    /// directives / defaults after merging `# key=value` header lines
    pub defaults: Defaults,
}

impl Parsed {
    fn reject(&mut self, msg: String) {
        self.rejected += 1;
        if self.errors.len() < 20 {
            self.errors.push(msg);
        }
    }
}

/// Extract leading `# key=value` directive lines.
fn directives(text: &str, d: &mut Defaults) {
    for line in text.lines() {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        let Some(rest) = t.strip_prefix('#') else { break };
        if let Some((k, v)) = rest.split_once('=') {
            let v = v.trim().to_string();
            match k.trim() {
                "source_key" => d.source_key = Some(v),
                "label_source" => d.label_source = Some(v),
                "chain" => d.chain = Some(v),
                "observed_at" => d.observed_at = Some(v),
                "agent" => d.agent = Some(v),
                _ => {}
            }
        }
    }
}

fn canonical_col(name: &str) -> Option<&'static str> {
    Some(match name.trim().to_ascii_lowercase().as_str() {
        "chain" | "network" | "blockchain" => "chain",
        "address" | "addr" => "address",
        "label" | "name" | "tag" => "label",
        "label_source" | "source" | "label_src" => "label_source",
        "category" | "type" | "label_type" => "category",
        "op" | "action" => "op",
        "observed_at" | "ts" | "timestamp" | "seen_at" => "observed_at",
        "source_key" => "source_key",
        _ => return None,
    })
}

fn make_row(
    get: &dyn Fn(&str) -> Option<String>,
    d: &Defaults,
) -> std::result::Result<(String, Row), String> {
    let pick = |k: &str, def: &Option<String>| get(k).filter(|s| !s.trim().is_empty()).or(def.clone());
    let source_key = pick("source_key", &d.source_key).ok_or("missing source_key")?;
    keys::validate_source_key(&source_key)?;
    let chain = norm::normalize_chain(&pick("chain", &d.chain).ok_or("missing chain")?);
    keys::validate_chain(&chain)?;
    let address = norm::normalize_address(&get("address").unwrap_or_default());
    if address.is_empty() {
        return Err("missing address".into());
    }
    let label = norm::clean_field(&get("label").unwrap_or_default());
    if label.is_empty() {
        return Err("missing label".into());
    }
    let label_source =
        norm::clean_field(&pick("label_source", &d.label_source).unwrap_or_else(|| source_key.clone()));
    let category = norm::clean_field(&get("category").unwrap_or_default());
    let op = norm::normalize_op(&get("op").unwrap_or_default())?.to_string();
    let observed_at = norm::clean_field(&pick("observed_at", &d.observed_at).unwrap_or_default());
    Ok((
        source_key,
        Row { chain, address, label, label_source, category, op, observed_at },
    ))
}

pub fn detect_format(data: &[u8], hint_ext: Option<&str>) -> Format {
    match hint_ext.map(|e| e.to_ascii_lowercase()) {
        Some(e) if e == "jsonl" || e == "ndjson" => return Format::Jsonl,
        Some(e) if e == "csv" => return Format::Csv,
        Some(e) if e == "tsv" => return Format::Tsv,
        _ => {}
    }
    let text = String::from_utf8_lossy(&data[..data.len().min(64 * 1024)]);
    for line in text.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        if t.starts_with('{') {
            return Format::Jsonl;
        }
        if !t.contains('\t') && t.contains(',') {
            return Format::Csv;
        }
        return Format::Tsv;
    }
    Format::Tsv
}

pub fn parse_input(data: &[u8], format: Format, defaults: &Defaults) -> Result<Parsed> {
    let text = std::str::from_utf8(data).context("input is not valid UTF-8")?;
    let mut d = defaults.clone();
    // CLI flags win over in-file directives, so apply directives only for unset fields.
    let mut fdir = Defaults::default();
    directives(text, &mut fdir);
    d.source_key = d.source_key.or(fdir.source_key);
    d.label_source = d.label_source.or(fdir.label_source);
    d.chain = d.chain.or(fdir.chain);
    d.observed_at = d.observed_at.or(fdir.observed_at);
    d.agent = d.agent.or(fdir.agent);

    let format = if format == Format::Auto { detect_format(data, None) } else { format };
    let mut p = Parsed { defaults: d.clone(), ..Default::default() };
    match format {
        Format::Jsonl => {
            for (i, line) in text.lines().enumerate() {
                let t = line.trim();
                if t.is_empty() || t.starts_with('#') {
                    continue;
                }
                let v: Value = match serde_json::from_str(t) {
                    Ok(v) => v,
                    Err(e) => {
                        p.reject(format!("line {}: bad json: {e}", i + 1));
                        continue;
                    }
                };
                let Some(obj) = v.as_object() else {
                    p.reject(format!("line {}: not an object", i + 1));
                    continue;
                };
                let mut m: BTreeMap<&'static str, String> = BTreeMap::new();
                for (k, v) in obj {
                    if let Some(c) = canonical_col(k) {
                        let s = match v {
                            Value::String(s) => s.clone(),
                            Value::Null => continue,
                            other => other.to_string(),
                        };
                        m.insert(c, s);
                    }
                }
                match make_row(&|k| m.get(k).cloned(), &d) {
                    Ok(r) => p.rows.push(r),
                    Err(e) => p.reject(format!("line {}: {e}", i + 1)),
                }
            }
        }
        Format::Tsv | Format::Csv | Format::Auto => {
            let mut b = csv::ReaderBuilder::new();
            b.has_headers(true).flexible(true).comment(Some(b'#'));
            if format == Format::Csv {
                b.delimiter(b',');
            } else {
                b.delimiter(b'\t').quoting(false);
            }
            let mut rdr = b.from_reader(data);
            let headers = rdr.headers().context("reading header row")?.clone();
            let cols: Vec<Option<&'static str>> = headers.iter().map(canonical_col).collect();
            if !cols.contains(&Some("address")) || !cols.contains(&Some("label")) {
                bail!(
                    "header row must contain at least 'address' and 'label' columns (got: {:?})",
                    headers.iter().collect::<Vec<_>>()
                );
            }
            for (i, rec) in rdr.records().enumerate() {
                let rec = match rec {
                    Ok(r) => r,
                    Err(e) => {
                        p.reject(format!("record {}: {e}", i + 1));
                        continue;
                    }
                };
                let get = |k: &str| -> Option<String> {
                    cols.iter()
                        .position(|c| *c == Some(k))
                        .and_then(|idx| rec.get(idx))
                        .map(|s| s.to_string())
                };
                match make_row(&get, &d) {
                    Ok(r) => p.rows.push(r),
                    Err(e) => p.reject(format!("record {}: {e}", i + 1)),
                }
            }
        }
    }
    Ok(p)
}

#[derive(Clone, Debug)]
pub struct Segment {
    pub chain: String,
    pub source_key: String,
    pub hash_hex: String,
    pub rows: usize,
    /// canonical uncompressed TSV
    pub tsv: Vec<u8>,
}

impl Segment {
    pub fn key(&self) -> String {
        keys::seg_key(&self.chain, &self.source_key, &self.hash_hex)
    }
    pub fn compressed(&self) -> Result<Vec<u8>> {
        Ok(zstd::encode_all(&self.tsv[..], 3)?)
    }
}

pub fn canonical_tsv(rows: &[Row]) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows.len() * 96 + HEADER.len() + 1);
    out.extend_from_slice(HEADER.as_bytes());
    out.push(b'\n');
    for r in rows {
        r.write_tsv(&mut out);
    }
    out
}

/// Build canonical segments for one source key: dedup + sort, group by chain, chunk.
pub fn build_segments(source_key: &str, mut rows: Vec<Row>, max_rows: usize) -> Vec<Segment> {
    let max_rows = max_rows.max(1);
    rows.sort();
    rows.dedup();
    let mut by_chain: BTreeMap<String, Vec<Row>> = BTreeMap::new();
    for r in rows {
        by_chain.entry(r.chain.clone()).or_default().push(r);
    }
    let mut segs = Vec::new();
    for (chain, rows) in by_chain {
        for chunk in rows.chunks(max_rows) {
            let tsv = canonical_tsv(chunk);
            let hash_hex = blake3::hash(&tsv).to_hex().to_string();
            segs.push(Segment {
                chain: chain.clone(),
                source_key: source_key.to_string(),
                hash_hex,
                rows: chunk.len(),
                tsv,
            });
        }
    }
    segs
}

/// Group parsed rows by source key and build segments for each.
pub fn segments_from_parsed(rows: Vec<(String, Row)>, max_rows: usize) -> Vec<Segment> {
    let mut by_src: BTreeMap<String, Vec<Row>> = BTreeMap::new();
    for (s, r) in rows {
        by_src.entry(s).or_default().push(r);
    }
    by_src
        .into_iter()
        .flat_map(|(s, rows)| build_segments(&s, rows, max_rows))
        .collect()
}

pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    let dec = zstd::stream::read::Decoder::new(data)?;
    let mut out = Vec::new();
    dec.take(MAX_SEGMENT_BYTES + 1).read_to_end(&mut out)?;
    if out.len() as u64 > MAX_SEGMENT_BYTES {
        bail!("segment exceeds {MAX_SEGMENT_BYTES} bytes decompressed");
    }
    Ok(out)
}

/// Parse a canonical segment TSV, verifying header and hash.
pub fn parse_segment(tsv: &[u8], expect_hash_hex: &str) -> Result<Vec<Row>> {
    let got = blake3::hash(tsv).to_hex();
    if got.as_str() != expect_hash_hex {
        bail!("segment hash mismatch: key says {expect_hash_hex}, content is {got}");
    }
    let text = std::str::from_utf8(tsv)?;
    let mut lines = text.split('\n');
    if lines.next() != Some(HEADER) {
        bail!("bad segment header");
    }
    let mut rows = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        if f.len() != 7 {
            return Err(anyhow!("bad segment row: {line:?}"));
        }
        rows.push(Row {
            chain: f[0].into(),
            address: f[1].into(),
            label: f[2].into(),
            label_source: f[3].into(),
            category: f[4].into(),
            op: f[5].into(),
            observed_at: f[6].into(),
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(src: &str) -> Defaults {
        Defaults { source_key: Some(src.into()), ..Default::default() }
    }

    const TSV_A: &str = "chain\taddress\tlabel\tcategory\n\
        ETH\t0xAbCdEf0123456789aBcDeF0123456789ABCDEF01\tBinance 14\texchange\n\
        btc\tBC1QAR0SRRR7XFKVY5L643LYDNW9RE59GTZZWF5MDQ\tsome wallet\t\n\
        eth\t0x00000000219ab540356cBB839Cbe05303d7705Fa\tBeacon Deposit\tstaking\n";

    #[test]
    fn same_data_different_order_and_case_same_hash() {
        let p1 = parse_input(TSV_A.as_bytes(), Format::Auto, &d("s1")).unwrap();
        // shuffled, duplicated, different casing and a JSONL encoding
        let jsonl = r#"{"chain":"eth","address":"0x00000000219AB540356CBB839CBE05303D7705FA","label":"Beacon Deposit","category":"staking"}
{"chain":"BTC","address":"bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq","label":"some wallet"}
{"chain":"eth","address":"0xabcdef0123456789abcdef0123456789abcdef01","label":"Binance 14","category":"exchange"}
{"chain":"eth","address":"0xabcdef0123456789abcdef0123456789abcdef01","label":"Binance 14","category":"exchange"}
"#;
        let p2 = parse_input(jsonl.as_bytes(), Format::Auto, &d("s1")).unwrap();
        assert_eq!(p1.rejected, 0);
        assert_eq!(p2.rejected, 0);
        let s1 = segments_from_parsed(p1.rows, DEFAULT_MAX_ROWS);
        let s2 = segments_from_parsed(p2.rows, DEFAULT_MAX_ROWS);
        assert_eq!(s1.len(), 2); // btc + eth
        let h1: Vec<_> = s1.iter().map(|s| s.key()).collect();
        let h2: Vec<_> = s2.iter().map(|s| s.key()).collect();
        assert_eq!(h1, h2);
        assert_eq!(s1[1].rows, 2);
        // canonical: sorted, normalized
        let txt = String::from_utf8(s1[1].tsv.clone()).unwrap();
        assert!(txt.starts_with(HEADER));
        assert!(txt.contains("0xabcdef0123456789abcdef0123456789abcdef01\tBinance 14\ts1\texchange\tadd\t"));
    }

    #[test]
    fn hash_is_stable_constant() {
        // Pin the hash so accidental format changes are caught (segment identity is a wire format!).
        let rows = vec![Row {
            chain: "eth".into(),
            address: "0x0000000000000000000000000000000000000000".into(),
            label: "Null Address".into(),
            label_source: "test".into(),
            category: "".into(),
            op: "add".into(),
            observed_at: "".into(),
        }];
        let s = build_segments("test", rows, 10);
        let expected = blake3::hash(
            b"chain\taddress\tlabel\tlabel_source\tcategory\top\tobserved_at\neth\t0x0000000000000000000000000000000000000000\tNull Address\ttest\t\tadd\t\n",
        )
        .to_hex()
        .to_string();
        assert_eq!(s[0].hash_hex, expected);
    }

    #[test]
    fn roundtrip_compress_and_verify() {
        let p = parse_input(TSV_A.as_bytes(), Format::Tsv, &d("s1")).unwrap();
        for s in segments_from_parsed(p.rows, DEFAULT_MAX_ROWS) {
            let c = s.compressed().unwrap();
            let back = decompress(&c).unwrap();
            assert_eq!(back, s.tsv);
            let rows = parse_segment(&back, &s.hash_hex).unwrap();
            assert_eq!(rows.len(), s.rows);
            assert!(parse_segment(&back, &"0".repeat(64)).is_err());
        }
    }

    #[test]
    fn chunking_deterministic() {
        let rows: Vec<Row> = (0..25)
            .map(|i| Row {
                chain: "eth".into(),
                address: format!("0x{:040x}", i),
                label: format!("l{i}"),
                label_source: "x".into(),
                category: "".into(),
                op: "add".into(),
                observed_at: "".into(),
            })
            .collect();
        let mut rev = rows.clone();
        rev.reverse();
        let a = build_segments("x", rows, 10);
        let b = build_segments("x", rev, 10);
        assert_eq!(a.len(), 3);
        assert_eq!(
            a.iter().map(|s| &s.hash_hex).collect::<Vec<_>>(),
            b.iter().map(|s| &s.hash_hex).collect::<Vec<_>>()
        );
    }

    #[test]
    fn directives_and_rejects() {
        let txt = "# source_key=etherscan-export-1\n# chain=eth\naddress\tlabel\top\n\
            0xAbCdEf0123456789aBcDeF0123456789ABCDEF01\tFoo\t\n\
            \tmissing addr\t\n\
            0xAbCdEf0123456789aBcDeF0123456789ABCDEF01\tFoo\tdel\n";
        let p = parse_input(txt.as_bytes(), Format::Auto, &Defaults::default()).unwrap();
        assert_eq!(p.rows.len(), 2);
        assert_eq!(p.rejected, 1);
        assert_eq!(p.rows[0].0, "etherscan-export-1");
        assert_eq!(p.rows[1].1.op, "del");
    }

    #[test]
    fn csv_quoted() {
        let txt = "chain,address,label\neth,0x0000000000000000000000000000000000000001,\"Foo, Inc\"\n";
        let p = parse_input(txt.as_bytes(), Format::Auto, &d("c")).unwrap();
        assert_eq!(p.rows[0].1.label, "Foo, Inc");
    }
}
