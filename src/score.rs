//! Per-source yield scoring and ranking for `labelsync next`. Deliberately simple.
//!
//! * new labels per minute = sum(rows_new_after_dedup) / sum(duration_s) * 60
//! * error rate            = errors per run
//! * expected yield        = new_per_min / (1 + error_rate)
//! * sources with no usable history get an exploration score of
//!   `max(known expected yield) * 1.25 + 1`, so they are tried before known sources.

use serde::Serialize;

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Agg {
    pub runs: u64,
    pub rows_total: f64,
    pub rows_new: f64,
    pub duration_s: f64,
    pub errors: f64,
    pub retries: f64,
    pub last_ts_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Stats {
    pub runs: u64,
    pub rows_new: f64,
    pub new_per_min: Option<f64>,
    pub error_rate: f64,
    pub last_run_age_s: Option<u64>,
}

pub fn stats(a: &Agg, now_ms: u64) -> Stats {
    Stats {
        runs: a.runs,
        rows_new: a.rows_new,
        new_per_min: (a.duration_s > 0.0).then(|| a.rows_new / a.duration_s * 60.0),
        error_rate: if a.runs > 0 { a.errors / a.runs as f64 } else { 0.0 },
        last_run_age_s: (a.runs > 0).then(|| now_ms.saturating_sub(a.last_ts_ms) / 1000),
    }
}

/// Expected yield of a source with history, or None if it has no usable history.
pub fn expected_yield(a: Option<&Agg>) -> Option<f64> {
    let a = a?;
    if a.runs == 0 || a.duration_s <= 0.0 {
        return None;
    }
    let s = stats(a, 0);
    Some(s.new_per_min? / (1.0 + s.error_rate))
}

/// Rank candidates, best first. Returns (source_key, score, explored).
pub fn rank(cands: &[(String, Option<Agg>)]) -> Vec<(String, f64, bool)> {
    let known_max = cands
        .iter()
        .filter_map(|(_, a)| expected_yield(a.as_ref()))
        .fold(0.0f64, f64::max);
    let explore = known_max * 1.25 + 1.0;
    let mut v: Vec<(String, f64, bool)> = cands
        .iter()
        .map(|(k, a)| match expected_yield(a.as_ref()) {
            Some(y) => (k.clone(), y, false),
            None => (k.clone(), explore, true),
        })
        .collect();
    v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.0.cmp(&b.0)));
    v.dedup_by(|a, b| a.0 == b.0);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agg(runs: u64, rows_new: f64, dur: f64, errors: f64) -> Option<Agg> {
        Some(Agg { runs, rows_new, duration_s: dur, errors, rows_total: rows_new, ..Default::default() })
    }

    #[test]
    fn stats_math() {
        let s = stats(&Agg { runs: 2, rows_new: 300.0, duration_s: 60.0, errors: 1.0, last_ts_ms: 1_000, ..Default::default() }, 61_000);
        assert_eq!(s.new_per_min, Some(300.0));
        assert_eq!(s.error_rate, 0.5);
        assert_eq!(s.last_run_age_s, Some(60));
        assert_eq!(stats(&Agg::default(), 5).new_per_min, None);
    }

    #[test]
    fn higher_yield_first_and_errors_penalize() {
        let r = rank(&[
            ("slow".into(), agg(1, 10.0, 60.0, 0.0)),
            ("fast".into(), agg(1, 100.0, 60.0, 0.0)),
            ("fast_but_flaky".into(), agg(1, 100.0, 60.0, 19.0)), // 100/min / 20 = 5/min
        ]);
        assert_eq!(r.iter().map(|x| x.0.as_str()).collect::<Vec<_>>(), vec!["fast", "slow", "fast_but_flaky"]);
    }

    #[test]
    fn unknown_sources_get_exploration_bonus() {
        let r = rank(&[("known".into(), agg(3, 1000.0, 60.0, 0.0)), ("new".into(), None), ("zero_dur".into(), agg(1, 5.0, 0.0, 0.0))]);
        assert!(r[0].2 && r[1].2, "{r:?}");
        assert_eq!(r[2].0, "known");
        assert!(r[0].1 > r[2].1);
    }

    #[test]
    fn deterministic_ties_and_empty() {
        let r = rank(&[("b".into(), None), ("a".into(), None)]);
        assert_eq!(r[0].0, "a");
        assert_eq!(r[0].1, 1.0);
        assert!(rank(&[]).is_empty());
    }
}
