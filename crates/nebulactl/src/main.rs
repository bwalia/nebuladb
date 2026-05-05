//! `nebulactl` — operator + human-facing CLI for NebulaDB backup &
//! restore.
//!
//! The CLI is a thin shell over the `/admin/backup` and
//! `/admin/restore` REST endpoints. The HTTP server does the work; we
//! just format the request and decode the response. Three reasons to
//! ship this even though `curl` would suffice:
//!
//! 1. The operator's CronJob image needs *something* to call the API
//!    on a schedule. A static binary is cheaper than packaging curl
//!    + jq + a shell script.
//! 2. Humans benefit from typed flags + decent error messages.
//! 3. `--watch` mode polls status without re-implementing it in every
//!    operator/runbook.

use anyhow::{anyhow, bail, Result};
use clap::{Parser, Subcommand};
use serde_json::Value;

#[derive(Parser, Debug)]
#[command(
    name = "nebulactl",
    about = "Backup, restore, and inspect NebulaDB clusters",
    version,
)]
struct Cli {
    /// Base URL of the NebulaDB server. Defaults to NEBULA_URL env.
    #[arg(long, env = "NEBULA_URL", default_value = "http://localhost:8080")]
    url: String,
    /// Bearer token for the admin API. Defaults to NEBULA_TOKEN env.
    #[arg(long, env = "NEBULA_TOKEN")]
    token: Option<String>,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Backup operations.
    Backup {
        #[command(subcommand)]
        op: BackupOp,
    },
    /// Restore operations.
    Restore {
        #[command(subcommand)]
        op: RestoreOp,
    },
}

#[derive(Subcommand, Debug)]
enum BackupOp {
    /// Trigger a backup.
    Start {
        /// S3 bucket name (mutually exclusive with `--local`).
        #[arg(long)]
        s3_bucket: Option<String>,
        /// S3 key prefix.
        #[arg(long, default_value = "")]
        s3_prefix: String,
        /// S3 region.
        #[arg(long, default_value = "")]
        s3_region: String,
        /// Custom endpoint (MinIO, R2, etc).
        #[arg(long)]
        s3_endpoint: Option<String>,
        /// Local-fs target — exclusive with the s3 flags. For tests +
        /// mounted-volume backups.
        #[arg(long)]
        local: Option<String>,
        /// Cluster name baked into the manifest.
        #[arg(long, default_value = "")]
        cluster_name: String,
        /// Wait for the backup to finish before exiting (polls
        /// /admin/backup/:id every 2s).
        #[arg(long)]
        wait: bool,
    },
    /// Show a recent in-process job's status.
    Show {
        job_id: String,
    },
    /// List the in-process job ring (most recent 50).
    List,
    /// Read a backup's manifest from storage. Requires the same
    /// storage flags as `start` so we know where to look.
    Manifest {
        backup_id: String,
        #[arg(long)]
        s3_bucket: Option<String>,
        #[arg(long, default_value = "")]
        s3_prefix: String,
        #[arg(long, default_value = "")]
        s3_region: String,
        #[arg(long)]
        s3_endpoint: Option<String>,
        #[arg(long)]
        local: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum RestoreOp {
    /// Trigger a restore.
    Start {
        backup_id: String,
        /// Where the unpacked data should land. Caller is responsible
        /// for booting a server pointing at this dir afterwards.
        #[arg(long)]
        data_dir: String,
        // Same storage flags as backup start.
        #[arg(long)]
        s3_bucket: Option<String>,
        #[arg(long, default_value = "")]
        s3_prefix: String,
        #[arg(long, default_value = "")]
        s3_region: String,
        #[arg(long)]
        s3_endpoint: Option<String>,
        #[arg(long)]
        local: Option<String>,
        #[arg(long)]
        wait: bool,
    },
    Show {
        job_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = Client::new(cli.url, cli.token);
    match cli.command {
        Cmd::Backup { op } => run_backup(client, op).await,
        Cmd::Restore { op } => run_restore(client, op).await,
    }
}

// ---------------------------------------------------------------------------

struct Client {
    base: String,
    token: Option<String>,
    http: reqwest::Client,
}

impl Client {
    fn new(base: String, token: Option<String>) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            token,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("reqwest"),
        }
    }

    async fn post(&self, path: &str, body: &Value) -> Result<Value> {
        let mut req = self.http.post(format!("{}{path}", self.base)).json(body);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            bail!("HTTP {status}: {text}");
        }
        Ok(serde_json::from_str(&text)?)
    }

    async fn get(&self, path: &str) -> Result<Value> {
        let mut req = self.http.get(format!("{}{path}", self.base));
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            bail!("HTTP {status}: {text}");
        }
        Ok(serde_json::from_str(&text)?)
    }
}

/// Build the `storage` field shared by backup/restore/manifest
/// requests. Local target wins when both are provided — flag
/// validation catches the both-empty case.
fn storage_json(
    s3_bucket: Option<String>,
    s3_prefix: String,
    s3_region: String,
    s3_endpoint: Option<String>,
    local: Option<String>,
) -> Result<Value> {
    if let Some(path) = local {
        return Ok(serde_json::json!({"type": "local", "path": path}));
    }
    let bucket = s3_bucket.ok_or_else(|| anyhow!("set --s3-bucket or --local"))?;
    let mut v = serde_json::json!({
        "type": "s3",
        "bucket": bucket,
        "prefix": s3_prefix,
        "region": s3_region,
    });
    if let Some(ep) = s3_endpoint {
        v["endpoint"] = ep.into();
    }
    Ok(v)
}

async fn run_backup(client: Client, op: BackupOp) -> Result<()> {
    match op {
        BackupOp::Start {
            s3_bucket,
            s3_prefix,
            s3_region,
            s3_endpoint,
            local,
            cluster_name,
            wait,
        } => {
            let storage = storage_json(s3_bucket, s3_prefix, s3_region, s3_endpoint, local)?;
            let body = serde_json::json!({
                "cluster_name": cluster_name,
                "storage": storage,
            });
            let resp = client.post("/api/v1/admin/backup", &body).await?;
            let job_id = resp["backup_id"].as_str().unwrap_or("").to_string();
            println!("backup_id: {job_id}");
            println!("status_url: {}", resp["status_url"]);
            if wait {
                wait_for(&client, "backup", &job_id).await?;
            }
            Ok(())
        }
        BackupOp::Show { job_id } => {
            let resp = client
                .get(&format!("/api/v1/admin/backup/{job_id}"))
                .await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
            Ok(())
        }
        BackupOp::List => {
            let resp = client.get("/api/v1/admin/backups").await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
            Ok(())
        }
        BackupOp::Manifest {
            backup_id,
            s3_bucket,
            s3_prefix,
            s3_region,
            s3_endpoint,
            local,
        } => {
            let storage = storage_json(s3_bucket, s3_prefix, s3_region, s3_endpoint, local)?;
            let body = serde_json::json!({
                "backup_id": backup_id,
                "storage": storage,
            });
            let resp = client.post("/api/v1/admin/backup/manifest", &body).await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
            Ok(())
        }
    }
}

async fn run_restore(client: Client, op: RestoreOp) -> Result<()> {
    match op {
        RestoreOp::Start {
            backup_id,
            data_dir,
            s3_bucket,
            s3_prefix,
            s3_region,
            s3_endpoint,
            local,
            wait,
        } => {
            let storage = storage_json(s3_bucket, s3_prefix, s3_region, s3_endpoint, local)?;
            let body = serde_json::json!({
                "backup_id": backup_id,
                "data_dir": data_dir,
                "storage": storage,
            });
            let resp = client.post("/api/v1/admin/restore", &body).await?;
            let job_id = resp["restore_id"].as_str().unwrap_or("").to_string();
            println!("restore_id: {job_id}");
            println!("status_url: {}", resp["status_url"]);
            if wait {
                wait_for(&client, "restore", &job_id).await?;
            }
            Ok(())
        }
        RestoreOp::Show { job_id } => {
            let resp = client
                .get(&format!("/api/v1/admin/restore/{job_id}"))
                .await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
            Ok(())
        }
    }
}

/// Poll `/admin/{kind}/:id` every 2s until the phase is non-running.
/// Prints a one-line progress update on each tick — operators in a
/// terminal want feedback, scripts use the JSON output of `show`.
async fn wait_for(client: &Client, kind: &str, job_id: &str) -> Result<()> {
    let path = format!("/api/v1/admin/{kind}/{job_id}");
    loop {
        let resp = client.get(&path).await?;
        let phase = resp["phase"].as_str().unwrap_or("?");
        eprintln!("[{kind}] {job_id} -> {phase}");
        match phase {
            "completed" => return Ok(()),
            "failed" => {
                let err = resp["error"].as_str().unwrap_or("(no detail)");
                bail!("{kind} {job_id} failed: {err}");
            }
            _ => tokio::time::sleep(std::time::Duration::from_secs(2)).await,
        }
    }
}
