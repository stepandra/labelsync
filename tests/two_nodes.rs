//! End-to-end test: two daemons on one machine, separate data dirs/sockets, syncing
//! over real iroh (N0 preset; tickets carry direct addresses so it also works without relays).

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;

const BIN: &str = env!("CARGO_BIN_EXE_labelsync");

struct Daemon {
    dir: PathBuf,
    child: Option<Child>,
    name: &'static str,
}

impl Daemon {
    fn start(root: &Path, name: &'static str, extra: &[&str]) -> Self {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let log = std::fs::File::create(root.join(format!("{name}.log"))).unwrap();
        let mut args = vec!["--agent", name, "daemon", "--ttl-secs", "600", "--sync-interval", "3"];
        args.extend_from_slice(extra);
        let child = Command::new(BIN)
            .env("LABELSYNC_DIR", &dir)
            .env("LABELSYNC_LOG", "labelsync=debug,iroh=warn,iroh_docs=error")
            .args(&args)
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        let d = Daemon { dir, child: Some(child), name };
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(30) {
            if d.try_cli(&["status"]).0 == Some(0) {
                return d;
            }
            thread::sleep(Duration::from_millis(200));
        }
        panic!("daemon {name} did not come up");
    }

    fn try_cli(&self, args: &[&str]) -> (Option<i32>, Value) {
        self.cli_stdin(args, None)
    }

    fn cli_stdin(&self, args: &[&str], stdin: Option<&str>) -> (Option<i32>, Value) {
        let mut cmd = Command::new(BIN);
        cmd.env("LABELSYNC_DIR", &self.dir).args(args).stdout(Stdio::piped()).stderr(Stdio::piped());
        cmd.stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() });
        let mut child = cmd.spawn().unwrap();
        if let Some(s) = stdin {
            use std::io::Write;
            child.stdin.take().unwrap().write_all(s.as_bytes()).unwrap();
        }
        let out = child.wait_with_output().unwrap();
        let v: Value = serde_json::from_slice(&out.stdout).unwrap_or(Value::Null);
        (out.status.code(), v)
    }

    fn cli(&self, args: &[&str]) -> Value {
        let (code, v) = self.try_cli(args);
        assert_eq!(v["ok"], Value::Bool(true), "{} {:?} failed: code={code:?} {v}", self.name, args);
        v
    }

    fn status(&self) -> Value {
        self.cli(&["status"])
    }

    fn labels(&self) -> (i64, i64, i64) {
        let s = self.status();
        (
            s["db"]["labels"].as_i64().unwrap(),
            s["db"]["active_labels"].as_i64().unwrap(),
            s["db"]["provenance_rows"].as_i64().unwrap(),
        )
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.try_cli(&["stop"]);
        if let Some(mut c) = self.child.take() {
            let t = Instant::now();
            while t.elapsed() < Duration::from_secs(5) {
                if let Ok(Some(_)) = c.try_wait() {
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn wait_until<F: FnMut() -> bool>(what: &str, secs: u64, f: F) -> Duration {
    wait_until_dbg(what, secs, f, String::new)
}

fn wait_until_dbg<F: FnMut() -> bool, D: Fn() -> String>(what: &str, secs: u64, mut f: F, dbg: D) -> Duration {
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(secs) {
        if f() {
            eprintln!("[ok] {what} after {:.1?}", t.elapsed());
            return t.elapsed();
        }
        thread::sleep(Duration::from_millis(300));
    }
    panic!("timeout waiting for: {what}\n{}", dbg());
}

fn integrity_ok(dir: &Path) -> bool {
    let c = rusqlite::Connection::open_with_flags(
        dir.join("labels.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let r: String = c.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    r == "ok"
}

const DATA1: &str = "chain\taddress\tlabel\tcategory\n\
eth\t0xAbCdEf0123456789aBcDeF0123456789ABCDEF01\tBinance 14\texchange\n\
eth\t0x00000000219ab540356cBB839Cbe05303d7705Fa\tBeacon Deposit Contract\tstaking\n\
btc\tBC1QAR0SRRR7XFKVY5L643LYDNW9RE59GTZZWF5MDQ\tsome wallet\t\n";

#[test]
fn two_daemons_end_to_end() {
    let root = PathBuf::from(format!("/tmp/lst-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let a = Daemon::start(&root, "hound-a", &[]);
    let b = Daemon::start(&root, "hound-b", &[]);

    // ---- share / join ----
    let ticket_file = root.join("ticket");
    let v = a.cli(&["share", "--out", ticket_file.to_str().unwrap()]);
    assert!(v.get("ticket").is_none(), "ticket must not be echoed when --out is used");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&ticket_file).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "ticket file must be 0600");
    }
    let j = b.cli(&["join", &format!("@{}", ticket_file.display())]);
    assert_eq!(j["namespace"], v["namespace"]);
    let alog = std::fs::read_to_string(root.join("hound-a.log")).unwrap();
    let ticket = std::fs::read_to_string(&ticket_file).unwrap();
    assert!(!alog.contains(ticket.trim()), "ticket leaked into daemon log");

    // ---- 1. append on A shows up in B's SQLite and export ----
    let (_, r) = a.cli_stdin(&["append", "--source-key", "etherscan-export-1"], Some(DATA1));
    assert_eq!(r["rows_accepted"], 3);
    assert_eq!(r["segments"].as_array().unwrap().len(), 2);
    wait_until("B materialized A's 3 labels", 60, || b.labels().0 == 3);
    let ex = b.cli(&["export"]);
    let eth = std::fs::read_to_string(b.dir.join("export/eth.tsv")).unwrap();
    assert!(eth.contains("0xabcdef0123456789abcdef0123456789abcdef01\tBinance 14\tetherscan-export-1\texchange\thound-a"), "{eth}");
    assert_eq!(eth.lines().count(), 3, "header + 2 eth rows");
    eprintln!("[ok] export on B: {}", ex["files"]);

    // ---- 2. inbox on B shows up on A ----
    let inbox_file = b.dir.join("inbox/.solscan-top.jsonl.tmp");
    std::fs::write(&inbox_file, "{\"chain\":\"sol\",\"address\":\"So11111111111111111111111111111111111111112\",\"label\":\"Wrapped SOL\",\"category\":\"token\"}\n").unwrap();
    std::fs::rename(&inbox_file, b.dir.join("inbox/solscan-top.jsonl")).unwrap();
    wait_until("A got B's inbox label", 60, || a.labels().0 == 4);
    assert!(std::fs::read_dir(b.dir.join("processed")).unwrap().count() >= 2, "file + result.json in processed/");

    // ---- 3. concurrent claim on the same source -> exactly one winner after sync ----
    let (ra, rb) = thread::scope(|s| {
        let ha = s.spawn(|| a.try_cli(&["claim", "arkham-page-7", "--source-url", "https://example.invalid/7"]));
        let hb = s.spawn(|| b.try_cli(&["claim", "arkham-page-7", "--source-url", "https://example.invalid/7"]));
        (ha.join().unwrap(), hb.join().unwrap())
    });
    eprintln!("[info] concurrent claim results: A={} B={}", ra.1["result"], rb.1["result"]);
    let last = std::cell::RefCell::new(String::new());
    wait_until_dbg("both nodes see both claims", 60, || {
        let sa = a.cli(&["status", "--source-key", "arkham-page-7"]);
        let sb = b.cli(&["status", "--source-key", "arkham-page-7"]);
        let n = |s: &Value| s["source"]["claims"].as_array().map(|x| x.len()).unwrap_or(0);
        // when one side saw the other's claim before writing, it lost without writing -> 1 claim
        // a node that saw the other's claim before writing loses without writing (1 claim);
        // a node that wrote first and then saw an earlier claim loses with its record kept (2).
        let expected = if ra.1["result"] == "won" && rb.1["result"] == "won" { 2 } else { n(&sa).max(1) };
        *last.borrow_mut() = format!("A-claim={} B-claim={} A={} B={}", ra.1, rb.1, sa["source"], sb["source"]);
        n(&sa) == expected && n(&sb) == expected && sa["source"]["winner"]["author"] == sb["source"]["winner"]["author"]
    }, || last.borrow().clone());
    let (ca, va) = a.try_cli(&["renew", "arkham-page-7"]);
    let (cb, vb) = b.try_cli(&["renew", "arkham-page-7"]);
    eprintln!("[info] renew after merge: A={} (exit {ca:?}) B={} (exit {cb:?})", va["result"], vb["result"]);
    let winners = [&va, &vb].iter().filter(|v| v["result"] == "won").count();
    assert_eq!(winners, 1, "exactly one winner after sync: A={va} B={vb}");
    assert!(ca == Some(0) || cb == Some(0));
    assert!(ca == Some(2) || cb == Some(2) || ca == Some(4) || cb == Some(4));

    // ---- 4. done on A makes B skip (by source_key and by raw-source hash) ----
    let raw = root.join("raw-source.csv");
    std::fs::write(&raw, "pretend this is the raw downloaded dataset\n").unwrap();
    let v = a.cli(&["claim", "dune-query-42"]);
    assert_eq!(v["result"], "won");
    a.cli_stdin(
        &["append", "--source-key", "dune-query-42", "--chain", "eth"],
        Some("address\tlabel\n0x1111111111111111111111111111111111111111\tDune Label\n"),
    );
    let d = a.cli(&["done", "dune-query-42", "--source-file", raw.to_str().unwrap()]);
    assert_eq!(d["done"]["segments"].as_array().unwrap().len(), 1);
    wait_until("B skips done source", 60, || {
        let (code, v) = b.try_cli(&["claim", "dune-query-42"]);
        code == Some(3) && v["reason"] == "done"
    });
    let (code, v) = b.try_cli(&["claim", "dune-mirror-url", "--source-file", raw.to_str().unwrap()]);
    assert_eq!(code, Some(3), "same raw file under another key must be skipped: {v}");
    assert_eq!(v["reason"], "source_seen");

    // ---- 5. partition: both claim + append the same data offline, then reconnect ----
    let before = a.labels();
    b.cli(&["sync", "pause"]);
    thread::sleep(Duration::from_secs(1));
    let ca = a.cli(&["claim", "oklink-dump-3"]);
    let cb = b.cli(&["claim", "oklink-dump-3"]);
    eprintln!("[info] partitioned claims: A={} B={}", ca["result"], cb["result"]);
    assert_eq!(ca["result"], "won");
    assert_eq!(cb["result"], "won", "B cannot see A's claim while partitioned");
    let shared = "chain\taddress\tlabel\n\
        tron\tTR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t\tTether USDT\n\
        eth\t0xdAC17F958D2ee523a2206206994597C13D831ec7\tTether USDT\n";
    a.cli_stdin(&["append", "--source-key", "oklink-dump-3"], Some(shared));
    // B ingests the same rows in different order/case plus one extra row
    let shared_b = "chain\taddress\tlabel\n\
        ETH\t0xdac17f958d2ee523a2206206994597c13d831ec7\tTether USDT\n\
        tron\tTR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t\tTether USDT\n\
        eth\t0x2222222222222222222222222222222222222222\tOnly B saw this\n";
    b.cli_stdin(&["append", "--source-key", "oklink-dump-3"], Some(shared_b));
    thread::sleep(Duration::from_secs(4));
    assert_eq!(b.labels().0, before.0 + 3, "B has its own 3 rows only (partition holds)");
    assert_eq!(a.labels().0, before.0 + 2, "A must not see B's rows during the partition");

    b.cli(&["sync", "resume"]);
    let expected = before.0 + 3;
    wait_until("partition healed, both converge", 90, || a.labels() == b.labels() && a.labels().0 == expected);
    let (la, act, prov) = a.labels();
    eprintln!("[info] after heal: labels={la} active={act} provenance={prov}");
    assert!(integrity_ok(&a.dir) && integrity_ok(&b.dir));
    // dedup: the two shared rows exist once each as labels, with provenance from both agents
    let c = rusqlite::Connection::open_with_flags(a.dir.join("labels.db"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let n: i64 = c
        .query_row("SELECT count(*) FROM labels WHERE address='0xdac17f958d2ee523a2206206994597c13d831ec7'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
    let agents: String = c
        .query_row("SELECT agents FROM active_labels WHERE address='0xdac17f958d2ee523a2206206994597c13d831ec7'", [], |r| r.get(0))
        .unwrap();
    assert!(agents.contains("hound-a") && agents.contains("hound-b"), "{agents}");
    // claim conflict from the partition resolves to exactly one winner
    wait_until("partition claims visible on both", 60, || {
        let n = |d: &Daemon| d.cli(&["status", "--source-key", "oklink-dump-3"])["source"]["claims"].as_array().map(|x| x.len()).unwrap_or(0);
        n(&a) == 2 && n(&b) == 2
    });
    let va = a.try_cli(&["renew", "oklink-dump-3"]).1;
    let vb = b.try_cli(&["renew", "oklink-dump-3"]).1;
    eprintln!("[info] post-partition renew: A={} B={}", va["result"], vb["result"]);
    assert_eq!([&va, &vb].iter().filter(|v| v["result"] == "won").count(), 1);

    // ---- 6. restart persistence ----
    drop(b);
    let b = Daemon::start(&root, "hound-b", &[]);
    assert_eq!(b.labels().0, expected, "B keeps its DB across restart");
    a.cli_stdin(&["append", "--source-key", "after-restart", "--chain", "eth"], Some("address\tlabel\n0x3333333333333333333333333333333333333333\tAfter restart\n"));
    wait_until("B resyncs after restart", 90, || b.labels().0 == expected + 1);

    drop(a);
    drop(b);
    let _ = std::fs::remove_dir_all(&root);
}
