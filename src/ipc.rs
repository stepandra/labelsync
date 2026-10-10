//! JSON-lines request protocol between the CLI and the daemon (one request per connection).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Request {
    Status {
        #[serde(default)]
        source_key: Option<String>,
    },
    Share {
        #[serde(default)]
        read_only: bool,
        /// all (default) | relay | direct | id
        #[serde(default)]
        addr: Option<String>,
    },
    Join {
        ticket: String,
        #[serde(default)]
        force: bool,
    },
    Append {
        path: String,
        #[serde(default)]
        delete_after: bool,
        #[serde(default)]
        agent: Option<String>,
        #[serde(default)]
        source_key: Option<String>,
        #[serde(default)]
        label_source: Option<String>,
        #[serde(default)]
        chain: Option<String>,
        #[serde(default)]
        observed_at: Option<String>,
        #[serde(default)]
        format: Option<String>,
    },
    Export {
        #[serde(default)]
        chain: Option<String>,
        #[serde(default)]
        out_dir: Option<String>,
    },
    Claim {
        source_key: String,
        #[serde(default)]
        agent: Option<String>,
        #[serde(default)]
        source_url: Option<String>,
        #[serde(default)]
        source_hash: Option<String>,
        #[serde(default)]
        ttl_secs: Option<u64>,
        #[serde(default)]
        auto_renew: bool,
    },
    Renew {
        source_key: String,
        #[serde(default)]
        agent: Option<String>,
    },
    Done {
        source_key: String,
        #[serde(default)]
        agent: Option<String>,
        #[serde(default)]
        source_hash: Option<String>,
    },
    Release {
        source_key: String,
        #[serde(default)]
        agent: Option<String>,
    },
    Sync {
        /// pause | resume | now
        action: String,
    },
    Shutdown,
}
