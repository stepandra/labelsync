//! Advisory work claims (leases) and the deterministic winner rule.
//!
//! Every author writes its own `claim/<source_key>/` entry. After entries have merged,
//! every node evaluates the same rule over the same set of records:
//! the **live** claim (not released, `lease_until > now`) with the minimal
//! `(claimed_at, author_id)` wins. A `done/` record for the source overrides all claims.
//! This is advisory: under a partition two agents may both believe they won; the
//! resulting duplicate work is harmless because segments are content-addressed and
//! SQLite deduplicates rows.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClaimRecord {
    pub agent: String,
    /// unix ms, agent clock. Kept stable across renewals.
    pub claimed_at: u64,
    /// unix ms
    pub lease_until: u64,
    #[serde(default)]
    pub source_url: Option<String>,
    #[serde(default)]
    pub source_hash: Option<String>,
    #[serde(default)]
    pub released: bool,
    /// lease length in ms used for renewals
    #[serde(default)]
    pub ttl_ms: u64,
}

impl ClaimRecord {
    pub fn is_live(&self, now_ms: u64) -> bool {
        !self.released && self.lease_until > now_ms
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoneRecord {
    pub agent: String,
    pub source_key: String,
    #[serde(default)]
    pub source_content_hash: Option<String>,
    pub segments: Vec<String>,
    pub finished_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SrcRecord {
    pub agent: String,
    pub source_key: String,
    pub finished_at: u64,
}

/// Pick the winning claim: live claim with minimal (claimed_at, author).
/// `claims` holds (author id bytes, record). Returns index into `claims`.
pub fn winner(claims: &[([u8; 32], ClaimRecord)], now_ms: u64) -> Option<usize> {
    claims
        .iter()
        .enumerate()
        .filter(|(_, (_, c))| c.is_live(now_ms))
        .min_by(|(_, (a1, c1)), (_, (a2, c2))| (c1.claimed_at, a1).cmp(&(c2.claimed_at, a2)))
        .map(|(i, _)| i)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Won,
    Lost,
    Skip,
    NotClaimed,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(claimed_at: u64, lease_until: u64) -> ClaimRecord {
        ClaimRecord {
            agent: "a".into(),
            claimed_at,
            lease_until,
            source_url: None,
            source_hash: None,
            released: false,
            ttl_ms: 1000,
        }
    }

    #[test]
    fn earliest_live_claim_wins() {
        let v = vec![([2u8; 32], c(200, 10_000)), ([1u8; 32], c(100, 10_000)), ([3u8; 32], c(150, 10_000))];
        assert_eq!(winner(&v, 500), Some(1));
    }

    #[test]
    fn tie_broken_by_author_id() {
        let v = vec![([9u8; 32], c(100, 10_000)), ([4u8; 32], c(100, 10_000))];
        assert_eq!(winner(&v, 500), Some(1));
        // order of input must not matter
        let v2 = vec![v[1].clone(), v[0].clone()];
        assert_eq!(v2[winner(&v2, 500).unwrap()].0, [4u8; 32]);
    }

    #[test]
    fn expired_and_released_ignored() {
        let mut released = c(50, 10_000);
        released.released = true;
        let v = vec![([1u8; 32], c(10, 400)), ([2u8; 32], released), ([3u8; 32], c(300, 10_000))];
        assert_eq!(winner(&v, 500), Some(2));
        assert_eq!(winner(&v, 20_000), None);
    }

    #[test]
    fn all_nodes_agree() {
        // Every permutation yields the same winner author.
        let base = [([5u8; 32], c(100, 9_000)), ([1u8; 32], c(100, 9_000)), ([0u8; 32], c(101, 9_000))];
        let perms = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
        for p in perms {
            let v: Vec<_> = p.iter().map(|&i| base[i].clone()).collect();
            assert_eq!(v[winner(&v, 0).unwrap()].0, [1u8; 32]);
        }
    }
}
