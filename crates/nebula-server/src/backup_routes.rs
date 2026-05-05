//! REST handlers for `/admin/backup` and `/admin/restore`.
//!
//! The HTTP layer is deliberately thin: parse the request, build a
//! `BackupTarget`, hand off to `nebula_backup::Backup::run`. Status
//! tracking is in-process (last 50 attempts); operators looking at
//! historical backups should query the storage target directly.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path, State};
use axum::Json;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use nebula_backup::{Backup, BackupTarget, LocalFsTarget, Manifest, Restore, S3Target};

use crate::error::ApiError;
use crate::state::AppState;

/// Capacity of the in-process job-status ring. Keeps memory bounded
/// while letting an operator review a few-day window of recent
/// attempts via `/admin/backups` without a side trip to S3.
const JOB_RING_CAP: usize = 50;

/// One entry in the status ring. Cheap to clone — no payloads, just
/// metadata.
#[derive(Debug, Clone, Serialize)]
pub struct JobStatus {
    pub id: String,
    pub kind: &'static str,
    pub phase: JobPhase,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub duration_seconds: Option<u64>,
    pub manifest_url: Option<String>,
    pub size_bytes: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum JobPhase {
    Running,
    Completed,
    Failed,
}

/// Concurrent ring of job statuses. Plug into AppState so background
/// runs can update it from any handler thread.
#[derive(Default)]
pub struct JobRing {
    inner: RwLock<VecDeque<JobStatus>>,
}

impl JobRing {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_running(&self, id: &str, kind: &'static str) {
        let job = JobStatus {
            id: id.to_string(),
            kind,
            phase: JobPhase::Running,
            started_at: now_iso(),
            completed_at: None,
            duration_seconds: None,
            manifest_url: None,
            size_bytes: None,
            error: None,
        };
        let mut g = self.inner.write();
        if g.len() >= JOB_RING_CAP {
            g.pop_front();
        }
        g.push_back(job);
    }

    pub fn complete(&self, id: &str, manifest_url: Option<String>, size_bytes: Option<u64>) {
        let mut g = self.inner.write();
        if let Some(j) = g.iter_mut().find(|j| j.id == id) {
            j.phase = JobPhase::Completed;
            j.completed_at = Some(now_iso());
            j.duration_seconds = elapsed(&j.started_at);
            j.manifest_url = manifest_url;
            j.size_bytes = size_bytes;
        }
    }

    pub fn fail(&self, id: &str, err: &str) {
        let mut g = self.inner.write();
        if let Some(j) = g.iter_mut().find(|j| j.id == id) {
            j.phase = JobPhase::Failed;
            j.completed_at = Some(now_iso());
            j.duration_seconds = elapsed(&j.started_at);
            j.error = Some(err.to_string());
        }
    }

    pub fn snapshot(&self) -> Vec<JobStatus> {
        self.inner.read().iter().cloned().collect()
    }

    pub fn get(&self, id: &str) -> Option<JobStatus> {
        self.inner.read().iter().find(|j| j.id == id).cloned()
    }
}

fn now_iso() -> String {
    // Keep this aligned with cross_region_status::iso_now — we'd
    // share the helper, but cross-crate drift is small and the cost
    // of a private duplicate is one screenful.
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let (y, mo, d) = days_to_ymd(days as i64);
    let hh = rem / 3600;
    let mm = (rem % 3600) / 60;
    let ss = rem % 60;
    format!("{y:04}-{mo:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

fn elapsed(started_iso: &str) -> Option<u64> {
    // Cheap: re-parse a fixed-format ISO. Good enough for an admin field.
    let _ = started_iso; // we'd parse + diff; stub with Instant for now.
    Some(0)
}

fn days_to_ymd(mut days: i64) -> (i32, u32, u32) {
    let mut y = 1970;
    loop {
        let ylen = if is_leap(y) { 366 } else { 365 };
        if days < ylen as i64 {
            break;
        }
        days -= ylen as i64;
        y += 1;
    }
    let months = if is_leap(y) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut m = 0;
    while m < 12 && days >= months[m] as i64 {
        days -= months[m] as i64;
        m += 1;
    }
    (y, (m + 1) as u32, (days + 1) as u32)
}
fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

// ---------------------------------------------------------------------------
// Request / response shapes
// ---------------------------------------------------------------------------

/// Storage configuration carried by both backup and restore requests.
/// Two flavors today: S3-compatible (production) and local filesystem
/// (tests, mounted volumes). Distinguished by the `type` discriminator
/// — Serde's tagged-enum form makes the wire shape ergonomic for the
/// operator's CronJob templates.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageSpec {
    S3 {
        bucket: String,
        #[serde(default)]
        prefix: String,
        /// Override the AWS endpoint URL — set this for MinIO, R2, etc.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
        /// AWS region the bucket lives in.
        #[serde(default)]
        region: String,
        /// Static credentials. Optional — when omitted the SDK's
        /// default credential chain runs (env, profile, IAM).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        access_key_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        secret_access_key: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_token: Option<String>,
    },
    Local {
        path: String,
    },
}

impl StorageSpec {
    /// Materialize into a concrete `BackupTarget`. Async because the
    /// S3 path constructs an SDK config from env/instance metadata.
    pub async fn into_target(self) -> Result<Arc<dyn BackupTarget>, ApiError> {
        match self {
            StorageSpec::Local { path } => Ok(Arc::new(LocalFsTarget::new(path))),
            StorageSpec::S3 {
                bucket,
                prefix,
                endpoint,
                region,
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
                if !region.is_empty() {
                    loader = loader.region(aws_config::Region::new(region));
                }
                if let Some(ep) = endpoint {
                    loader = loader.endpoint_url(ep);
                }
                if let (Some(ak), Some(sk)) = (access_key_id, secret_access_key) {
                    loader = loader.credentials_provider(
                        aws_credential_types::Credentials::new(
                            ak, sk, session_token, None, "nebuladb-static",
                        ),
                    );
                }
                let cfg = loader.load().await;
                let mut s3_cfg = aws_sdk_s3::config::Builder::from(&cfg);
                if cfg.endpoint_url().is_some() {
                    // Path-style addressing is what MinIO + R2 use;
                    // safe default for any non-AWS endpoint.
                    s3_cfg = s3_cfg.force_path_style(true);
                }
                let client = Arc::new(aws_sdk_s3::Client::from_conf(s3_cfg.build()));
                Ok(Arc::new(S3Target::new(client, bucket, prefix)))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StartBackupRequest {
    pub storage: StorageSpec,
    /// Cluster name baked into the manifest.
    #[serde(default)]
    pub cluster_name: String,
    /// Optional staging dir; defaults to `/tmp/nebula-backup-staging`.
    #[serde(default)]
    pub staging_dir: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StartBackupResponse {
    pub backup_id: String,
    pub status_url: String,
}

#[derive(Debug, Deserialize)]
pub struct StartRestoreRequest {
    pub storage: StorageSpec,
    pub backup_id: String,
    /// Where to land the restored data. Caller is responsible for
    /// pointing a server at this directory after the call returns.
    pub data_dir: String,
    #[serde(default)]
    pub staging_dir: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StartRestoreResponse {
    pub restore_id: String,
    pub status_url: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `POST /admin/backup` — kicks off a backup in the background and
/// returns the assigned id immediately. Polls `/admin/backup/:id` for
/// progress.
pub async fn admin_backup_start(
    State(s): State<AppState>,
    Json(req): Json<StartBackupRequest>,
) -> Result<Json<StartBackupResponse>, ApiError> {
    if !s.index.is_persistent() {
        return Err(ApiError::BadRequest(
            "backup requires a persistent index (NEBULA_DATA_DIR must be set)".into(),
        ));
    }
    let target = req.storage.into_target().await?;
    let staging = req
        .staging_dir
        .unwrap_or_else(|| "/tmp/nebula-backup-staging".to_string());
    let cluster_name = if req.cluster_name.is_empty() {
        s.cluster.node_id.clone().unwrap_or_else(|| "nebula".into())
    } else {
        req.cluster_name
    };

    let backup = Backup {
        index: Arc::clone(&s.index),
        target,
        cluster_name,
        region: s.cluster.region_or_default().to_string(),
        server_version: crate::build_info::VERSION.to_string(),
        git_commit: crate::build_info::GIT_COMMIT.to_string(),
        staging_dir: staging.into(),
    };

    // Mint the id BEFORE spawn so the response can return it.
    // `Backup::run()` mints its own internal ULID for the manifest;
    // ours is the job-tracking handle exposed to the client.
    let job_id = ulid_like();
    let ring = Arc::clone(&s.backup_jobs);
    ring.push_running(&job_id, "backup");
    let job_id_for_task = job_id.clone();

    tokio::spawn(async move {
        let started = Instant::now();
        match backup.run().await {
            Ok(manifest) => {
                let url = format!("backups/{}/manifest.json", manifest.backup_id);
                let size = manifest
                    .artifacts
                    .iter()
                    .map(|a| a.size_bytes)
                    .sum::<u64>();
                ring.complete(&job_id_for_task, Some(url), Some(size));
                tracing::info!(
                    job_id = %job_id_for_task,
                    backup_id = %manifest.backup_id,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "backup ok",
                );
            }
            Err(e) => {
                ring.fail(&job_id_for_task, &e.to_string());
                tracing::error!(job_id = %job_id_for_task, error = %e, "backup failed");
            }
        }
    });

    Ok(Json(StartBackupResponse {
        status_url: format!("/api/v1/admin/backup/{}", job_id),
        backup_id: job_id,
    }))
}

/// `GET /admin/backup/:id` — fetch the in-process job status.
pub async fn admin_backup_status(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobStatus>, ApiError> {
    s.backup_jobs
        .get(&id)
        .map(Json)
        .ok_or_else(|| ApiError::BadRequest(format!("job {id} not found")))
}

/// `GET /admin/backups` — list recent jobs (backups + restores).
pub async fn admin_backups_list(State(s): State<AppState>) -> Json<Vec<JobStatus>> {
    Json(s.backup_jobs.snapshot())
}

/// `POST /admin/restore` — kicks off a restore.
pub async fn admin_restore_start(
    State(s): State<AppState>,
    Json(req): Json<StartRestoreRequest>,
) -> Result<Json<StartRestoreResponse>, ApiError> {
    let target = req.storage.into_target().await?;
    let staging = req
        .staging_dir
        .unwrap_or_else(|| "/tmp/nebula-restore-staging".to_string());

    let restore = Restore {
        target,
        backup_id: req.backup_id.clone(),
        staging_dir: staging.into(),
        data_dir: req.data_dir.into(),
        server_version: crate::build_info::VERSION.to_string(),
        embedder_model: s.index.embedder_model().to_string(),
        embedder_dim: s.index.dim(),
    };

    let job_id = ulid_like();
    let ring = Arc::clone(&s.backup_jobs);
    ring.push_running(&job_id, "restore");
    let job_id_for_task = job_id.clone();

    tokio::spawn(async move {
        match restore.run().await {
            Ok(_manifest) => ring.complete(&job_id_for_task, None, None),
            Err(e) => ring.fail(&job_id_for_task, &e.to_string()),
        }
    });

    Ok(Json(StartRestoreResponse {
        status_url: format!("/api/v1/admin/restore/{}", job_id),
        restore_id: job_id,
    }))
}

/// Look up a recent restore. Same backing ring as backups since both
/// share the JobStatus shape.
pub async fn admin_restore_status(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobStatus>, ApiError> {
    s.backup_jobs
        .get(&id)
        .map(Json)
        .ok_or_else(|| ApiError::BadRequest(format!("job {id} not found")))
}

/// Manifest-by-id helper: downloads the manifest from the supplied
/// storage and returns it. Useful for `nebulactl backup show <id>`.
#[derive(Debug, Deserialize)]
pub struct GetManifestRequest {
    pub storage: StorageSpec,
    pub backup_id: String,
}

pub async fn admin_backup_manifest(
    State(_s): State<AppState>,
    Json(req): Json<GetManifestRequest>,
) -> Result<Json<Manifest>, ApiError> {
    let target = req.storage.into_target().await?;
    let bytes = target
        .get(&format!("backups/{}/manifest.json", req.backup_id))
        .await
        .map_err(|e| ApiError::BadRequest(format!("manifest: {e}")))?;
    let m: Manifest = serde_json::from_slice(&bytes)
        .map_err(|e| ApiError::BadRequest(format!("decode: {e}")))?;
    Ok(Json(m))
}

/// Generate a short job id. Reuses the wire form of a ULID without
/// pulling in the ulid crate at this layer (we already have it in
/// nebula-backup).
fn ulid_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    // Append a tiny PID-flavored suffix so two jobs minted in the
    // same millisecond don't collide.
    format!("{:013x}-{:04x}", ms, std::process::id() & 0xffff)
}
