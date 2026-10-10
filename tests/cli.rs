//! CLI contract tests: help, version, exit codes, stream separation, machine output.
//! Run the real binary as a subprocess; no daemon is started.

use std::process::{Command, Output, Stdio};
use std::io::Write;

use serde_json::Value;

fn bin() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_labelsync"));
    // isolate from the user's environment
    for k in ["LABELSYNC_DIR", "LABELSYNC_SOCKET", "LABELSYNC_AGENT", "LABELSYNC_OUTPUT", "LABELSYNC_TIMEOUT"] {
        c.env_remove(k);
    }
    c
}

fn run(args: &[&str]) -> Output {
    bin().args(args).stdin(Stdio::null()).output().unwrap()
}

fn run_stdin(args: &[&str], input: &str) -> Output {
    let mut ch = bin().args(args).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    ch.stdin.take().unwrap().write_all(input.as_bytes()).unwrap();
    ch.wait_with_output().unwrap()
}

fn s(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// stdout must be exactly one JSON value (one line).
fn one_json(o: &Output) -> Value {
    let out = s(&o.stdout);
    assert_eq!(out.lines().count(), 1, "stdout must be one line of JSON, got: {out:?}");
    let mut it = serde_json::Deserializer::from_str(&out).into_iter::<Value>();
    let v = it.next().expect("a JSON value").expect("valid JSON");
    assert!(it.next().is_none(), "exactly one JSON value");
    assert!(!out.contains('\x1b'));
    v
}

fn dead_dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// (path, has subcommands) for every visible command, from the capability inventory.
fn command_paths() -> Vec<Vec<String>> {
    let v = one_json(&run(&["capabilities"]));
    let mut out = vec![];
    for c in v["commands"].as_array().unwrap() {
        let name = c["name"].as_str().unwrap().to_string();
        out.push(vec![name.clone()]);
        for sc in c["subcommands"].as_array().unwrap() {
            out.push(vec![name.clone(), sc["name"].as_str().unwrap().to_string()]);
        }
    }
    out
}

#[test]
fn root_help_variants() {
    let paths = command_paths();
    for args in [vec!["--help"], vec!["-h"], vec!["help"]] {
        let o = run(&args);
        assert_eq!(o.status.code(), Some(0), "{args:?}");
        assert!(o.stderr.is_empty(), "{args:?}: help goes to stdout");
        let out = s(&o.stdout);
        for p in paths.iter().filter(|p| p.len() == 1) {
            assert!(out.contains(&format!("\n  {} ", p[0])), "root help lists `{}`", p[0]);
        }
        for needle in ["Examples:", "Exit codes:", "Global options:", "LABELSYNC_DIR", "Docs:"] {
            assert!(out.contains(needle), "root help has {needle}");
        }
        assert!(!out.contains("one version per agent"), "stale howto wording");
    }
}

#[test]
fn every_command_has_help_with_examples() {
    for p in command_paths() {
        let mut args: Vec<&str> = p.iter().map(|x| x.as_str()).collect();
        args.push("--help");
        let o = run(&args);
        assert_eq!(o.status.code(), Some(0), "{args:?}");
        let out = s(&o.stdout);
        assert!(out.contains("Usage: labelsync"), "{args:?}");
        assert!(out.contains("Examples:"), "{args:?} needs examples");
        assert!(!out.contains('\x1b'), "{args:?}: no ANSI when not a TTY");
        // `help <cmd>` is equivalent
        let mut h = vec!["help"];
        h.extend(p.iter().map(|x| x.as_str()));
        let o2 = run(&h);
        assert_eq!(o2.status.code(), Some(0), "{h:?}");
        assert!(s(&o2.stdout).contains("Examples:"), "{h:?}");
    }
}

#[test]
fn every_flag_is_documented() {
    let v = one_json(&run(&["capabilities"]));
    fn walk(c: &Value, path: &str) {
        for a in c["args"].as_array().unwrap() {
            let h = a["help"].as_str().unwrap_or("");
            assert!(!h.is_empty(), "{path} {} has no help text", a["name"]);
        }
        for sc in c["subcommands"].as_array().unwrap() {
            walk(sc, &format!("{path} {}", sc["name"].as_str().unwrap()));
        }
    }
    for c in v["commands"].as_array().unwrap() {
        walk(c, c["name"].as_str().unwrap());
    }
    for a in v["global_args"].as_array().unwrap() {
        assert!(a["help"].as_str().is_some_and(|h| !h.is_empty()), "global {} undocumented", a["name"]);
    }
}

#[test]
fn version_has_provenance() {
    for f in ["--version", "-V"] {
        let o = run(&[f]);
        assert_eq!(o.status.code(), Some(0));
        let out = s(&o.stdout);
        assert!(out.starts_with(&format!("labelsync {}", env!("CARGO_PKG_VERSION"))), "{out}");
        assert!(out.contains("commit ") && out.contains(std::env::consts::ARCH), "{out}");
    }
}

#[test]
fn usage_errors_exit_64_on_stderr_only() {
    // no args: concise help, but it's a usage error
    let o = run(&[]);
    assert_eq!(o.status.code(), Some(64));
    assert!(o.stdout.is_empty());
    assert!(s(&o.stderr).contains("Usage:"));
    // unknown command: suggestion, never executed
    let o = run(&["nex"]);
    assert_eq!(o.status.code(), Some(64));
    assert!(o.stdout.is_empty());
    assert!(s(&o.stderr).contains("a similar subcommand exists: 'next'"));
    // no prefix matching
    assert_eq!(run(&["stat"]).status.code(), Some(64));
    // bad enum value lists the allowed ones
    let o = run(&["sync", "stop"]);
    assert_eq!(o.status.code(), Some(64));
    assert!(s(&o.stderr).contains("pause, resume, now"));
    // missing required positional
    assert_eq!(run(&["claim"]).status.code(), Some(64));
    // nested group without subcommand
    assert_eq!(run(&["howto"]).status.code(), Some(64));
    // conflicting flags
    assert_eq!(run(&["claim", "x", "--source-hash", "ab", "--source-file", "f"]).status.code(), Some(64));
}

#[test]
fn daemon_unreachable_is_exit_1_with_json_and_hint() {
    let d = dead_dir();
    let dir = d.path().to_str().unwrap();
    let o = run(&["--data-dir", dir, "status"]);
    assert_eq!(o.status.code(), Some(1));
    let v = one_json(&o);
    assert_eq!(v["ok"], false);
    assert_eq!(v["code"], "daemon_unreachable");
    let err = s(&o.stderr);
    assert!(err.starts_with("labelsync: error: cannot connect"), "{err}");
    assert!(err.contains("hint: start it with `labelsync daemon"), "{err}");

    // --quiet drops the hint, keeps the error and status
    let o = run(&["--data-dir", dir, "--quiet", "status"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(s(&o.stderr).contains("error:") && !s(&o.stderr).contains("hint:"));

    // text mode: nothing on stdout on failure
    let o = run(&["--data-dir", dir, "--output", "text", "status"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(o.stdout.is_empty());

    // env var works the same as the flag
    let o = bin().env("LABELSYNC_DIR", dir).arg("status").stdin(Stdio::null()).output().unwrap();
    assert!(s(&o.stdout).contains(&format!("{dir}/labelsync.sock")));
}

#[test]
fn bad_input_is_usage_error() {
    let d = dead_dir();
    let o = run_stdin(&["--data-dir", d.path().to_str().unwrap(), "report", "x", "--json", "-"], "[1]");
    assert_eq!(o.status.code(), Some(64));
    assert_eq!(one_json(&o)["code"], "bad_input");
    let o = run_stdin(&["--data-dir", d.path().to_str().unwrap(), "report", "x", "--json", "-"], "{nope");
    assert_eq!(o.status.code(), Some(64));
}

#[test]
fn join_literal_ticket_warns() {
    let d = dead_dir();
    let o = run(&["--data-dir", d.path().to_str().unwrap(), "join", "docaaaa"]);
    assert!(s(&o.stderr).contains("warning: a ticket on the command line"));
    let o = run(&["--data-dir", d.path().to_str().unwrap(), "-q", "join", "docaaaa"]);
    assert!(!s(&o.stderr).contains("warning:"));
}

#[test]
fn append_dry_run_needs_no_daemon() {
    let input = "chain\taddress\tlabel\neth\t0xAbCdEf0000000000000000000000000000000001\tfoo\neth\t0x2\t\n";
    let o = run_stdin(&["--data-dir", "/nonexistent", "append", "--source-key", "s", "--dry-run"], input);
    assert_eq!(o.status.code(), Some(0), "{}", s(&o.stderr));
    let v = one_json(&o);
    assert_eq!(v["dry_run"], true);
    assert_eq!(v["rows_accepted"], 1);
    assert_eq!(v["rejected"], 1);
    let segs = v["segments"].as_array().unwrap();
    assert_eq!(segs.len(), 1);
    assert_eq!(segs[0]["chain"], "eth");
    assert_eq!(segs[0]["hash"].as_str().unwrap().len(), 64);
    assert!(!std::path::Path::new("/nonexistent").exists());
}

#[test]
fn no_color_and_dumb_terminal() {
    for (k, val) in [("NO_COLOR", "1"), ("TERM", "dumb")] {
        for args in [&["--help"][..], &["nex"][..], &["next", "--help"][..]] {
            let o = bin().env(k, val).args(args).output().unwrap();
            assert!(!s(&o.stdout).contains('\x1b') && !s(&o.stderr).contains('\x1b'), "{k} {args:?}");
        }
    }
}

#[test]
fn completions_man_capabilities_config() {
    for sh in ["bash", "zsh", "fish", "elvish", "powershell"] {
        let o = run(&["completions", sh]);
        assert_eq!(o.status.code(), Some(0), "{sh}");
        assert!(s(&o.stdout).contains("labelsync"), "{sh}");
    }
    let o = run(&["man"]);
    assert_eq!(o.status.code(), Some(0));
    assert!(s(&o.stdout).contains(".TH labelsync 1"));

    let v = one_json(&run(&["capabilities"]));
    assert_eq!(v["schema"], "labelsync.capabilities/v1");
    for c in ["0", "1", "2", "3", "4", "5", "64"] {
        assert!(v["exit_codes"][c].is_string(), "exit code {c} documented");
    }

    let o = bin().env("LABELSYNC_AGENT", "zz").args(["--timeout", "7", "config"]).output().unwrap();
    let v = one_json(&o);
    assert_eq!(v["agent"]["value"], "zz");
    assert_eq!(v["agent"]["from"], "env");
    assert_eq!(v["timeout_secs"]["from"], "flag");
    assert_eq!(v["daemon_running"], false);
}

#[test]
fn hash_command_json() {
    let d = dead_dir();
    let f = d.path().join("x");
    std::fs::write(&f, b"abc").unwrap();
    let v = one_json(&run(&["hash", f.to_str().unwrap()]));
    assert_eq!(v["blake3"], blake3::hash(b"abc").to_hex().to_string());
    let o = run(&["hash", "/nonexistent/file"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(s(&o.stderr).contains("cannot open /nonexistent/file"));
}
