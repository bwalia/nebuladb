//! First-class embed model / dimension migration.
//!
//! Flow (dim or model change):
//! 1. `POST /admin/embed/migrate` dumps every live doc's text to
//!    `{data_dir}/embed-migrate/docs.jsonl` and writes `pending.json`.
//! 2. Operator restarts with `NEBULA_OPENAI_*` + `NEBULA_EMBED_DIM`
//!    matching `pending.json` targets.
//! 3. Boot sees dim mismatch + matching pending → archives old
//!    snapshot/WAL, opens empty index at the new dim.
//! 4. [`spawn_drain_if_pending`] re-embeds from the jsonl via
//!    `upsert_text_bulk` until done, then clears pending + snapshots.
//!
//! Same-dim model swaps use the same path so there is one operator
//! story: dump → flip env → restart → drain.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use nebula_index::TextIndex;

const MIGRATE_DIR: &str = "embed-migrate";
const PENDING_FILE: &str = "pending.json";
const DOCS_FILE: &str = "docs.jsonl";
const DEFAULT_BATCH: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MigrateState {
    Idle,
    Dumping,
    AwaitingRestart,
    Draining,
    Done,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrateStatus {
    pub state: MigrateState,
    pub source_model: String,
    pub source_dim: usize,
    pub target_model: String,
    pub target_dim: usize,
    pub buckets: Vec<String>,
    pub dumped: u64,
    pub total: u64,
    pub drained: u64,
    pub error: Option<String>,
    pub resumable: bool,
    pub updated_at: String,
}

impl Default for MigrateStatus {
    fn default() -> Self {
        Self {
            state: MigrateState::Idle,
            source_model: String::new(),
            source_dim: 0,
            target_model: String::new(),
            target_dim: 0,
            buckets: Vec::new(),
            dumped: 0,
            total: 0,
            drained: 0,
            error: None,
            resumable: false,
            updated_at: iso_now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingFile {
    pub target_model: String,
    pub target_dim: usize,
    pub source_model: String,
    pub source_dim: usize,
    pub buckets: Vec<String>,
    pub dumped: u64,
    pub total: u64,
    pub drained: u64,
    pub state: MigrateState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DumpLine {
    bucket: String,
    id: String,
    text: String,
    #[serde(default)]
    metadata: serde_json::Value,
}

/// Shared handle for admin endpoints + background drain task.
#[derive(Clone)]
pub struct EmbedMigrateHub {
    inner: Arc<EmbedMigrateInner>,
}

struct EmbedMigrateInner {
    data_dir: Option<PathBuf>,
    status: Mutex<MigrateStatus>,
    cancel: AtomicBool,
    dump_lock: Mutex<()>,
}

impl EmbedMigrateHub {
    pub fn new(data_dir: Option<PathBuf>) -> Self {
        let hub = Self {
            inner: Arc::new(EmbedMigrateInner {
                data_dir,
                status: Mutex::new(MigrateStatus::default()),
                cancel: AtomicBool::new(false),
                dump_lock: Mutex::new(()),
            }),
        };
        if let Some(dir) = hub.migrate_dir() {
            if let Some(p) = read_pending(&dir) {
                let mut s = hub.inner.status.lock().expect("status");
                s.state = p.state.clone();
                s.source_model = p.source_model;
                s.source_dim = p.source_dim;
                s.target_model = p.target_model;
                s.target_dim = p.target_dim;
                s.buckets = p.buckets;
                s.dumped = p.dumped;
                s.total = p.total;
                s.drained = p.drained;
                s.resumable = matches!(
                    p.state,
                    MigrateState::AwaitingRestart | MigrateState::Draining | MigrateState::Dumping
                );
                s.updated_at = iso_now();
            }
        }
        hub
    }

    pub fn status(&self) -> MigrateStatus {
        self.inner.status.lock().expect("status").clone()
    }

    fn migrate_dir(&self) -> Option<PathBuf> {
        self.inner.data_dir.as_ref().map(|d| d.join(MIGRATE_DIR))
    }

    pub fn cancel(&self) {
        self.inner.cancel.store(true, Ordering::SeqCst);
        let mut s = self.inner.status.lock().expect("status");
        if matches!(
            s.state,
            MigrateState::Dumping | MigrateState::Draining | MigrateState::AwaitingRestart
        ) {
            s.state = MigrateState::Cancelled;
            s.updated_at = iso_now();
            s.resumable = false;
        }
    }

    /// Dump texts to disk and mark awaiting_restart. Does not change the
    /// live embedder — operator must restart with new env after this.
    pub fn start_dump(
        &self,
        index: &TextIndex,
        target_model: String,
        target_dim: usize,
        buckets: Option<Vec<String>>,
    ) -> Result<MigrateStatus, String> {
        let dir = self.migrate_dir().ok_or_else(|| {
            "embed migrate requires durable data_dir (NEBULA_DATA_DIR)".to_string()
        })?;
        let _guard = self
            .inner
            .dump_lock
            .try_lock()
            .map_err(|_| "embed migrate already running".to_string())?;

        let source_model = index.embedder_model().to_string();
        let source_dim = index.dim();
        if target_dim == 0 {
            return Err("target_dim must be > 0".into());
        }
        if target_model.is_empty() {
            return Err("target_model must be non-empty".into());
        }

        self.inner.cancel.store(false, Ordering::SeqCst);
        fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let docs_path = dir.join(DOCS_FILE);
        let _ = fs::remove_file(&docs_path);

        let bucket_list = match buckets {
            Some(b) if !b.is_empty() => b,
            _ => index.bucket_names(),
        };
        let mut total = 0u64;
        for b in &bucket_list {
            total += index.for_each_doc_in_bucket(b, |_| {}) as u64;
        }

        {
            let mut s = self.inner.status.lock().expect("status");
            *s = MigrateStatus {
                state: MigrateState::Dumping,
                source_model: source_model.clone(),
                source_dim,
                target_model: target_model.clone(),
                target_dim,
                buckets: bucket_list.clone(),
                dumped: 0,
                total,
                drained: 0,
                error: None,
                resumable: true,
                updated_at: iso_now(),
            };
        }

        let file = File::create(&docs_path).map_err(|e| e.to_string())?;
        let mut w = BufWriter::new(file);
        let mut dumped = 0u64;

        for bucket in &bucket_list {
            if self.inner.cancel.load(Ordering::SeqCst) {
                self.fail("cancelled during dump");
                return Err("cancelled".into());
            }
            index.for_each_doc_in_bucket(bucket, |doc| {
                if doc.text.is_empty() {
                    return;
                }
                let line = DumpLine {
                    bucket: doc.bucket.clone(),
                    id: doc.external_id.clone(),
                    text: doc.text.clone(),
                    metadata: doc.metadata.clone(),
                };
                if let Ok(js) = serde_json::to_string(&line) {
                    let _ = writeln!(w, "{js}");
                    dumped += 1;
                    if dumped % 10_000 == 0 {
                        let mut s = self.inner.status.lock().expect("status");
                        s.dumped = dumped;
                        s.updated_at = iso_now();
                    }
                }
            });
        }
        w.flush().map_err(|e| e.to_string())?;

        let pending = PendingFile {
            target_model: target_model.clone(),
            target_dim,
            source_model,
            source_dim,
            buckets: bucket_list,
            dumped,
            total,
            drained: 0,
            state: MigrateState::AwaitingRestart,
        };
        write_pending(&dir, &pending)?;
        {
            let mut s = self.inner.status.lock().expect("status");
            s.state = MigrateState::AwaitingRestart;
            s.dumped = dumped;
            s.total = total;
            s.updated_at = iso_now();
            s.resumable = true;
        }
        info!(
            dumped,
            total,
            target_model = %target_model,
            target_dim,
            "embed migrate dump complete; restart with matching embedder env"
        );
        Ok(self.status())
    }

    fn fail(&self, msg: &str) {
        warn!(error = %msg, "embed migrate failed");
        let mut s = self.inner.status.lock().expect("status");
        s.state = MigrateState::Failed;
        s.error = Some(msg.to_string());
        s.updated_at = iso_now();
        s.resumable = false;
    }

    fn set_draining(&self, drained: u64) {
        let mut s = self.inner.status.lock().expect("status");
        s.state = MigrateState::Draining;
        s.drained = drained;
        s.updated_at = iso_now();
        s.resumable = true;
    }

    fn set_done(&self, drained: u64) {
        let mut s = self.inner.status.lock().expect("status");
        s.state = MigrateState::Done;
        s.drained = drained;
        s.error = None;
        s.updated_at = iso_now();
        s.resumable = false;
    }
}

fn read_pending(dir: &Path) -> Option<PendingFile> {
    let s = fs::read_to_string(dir.join(PENDING_FILE)).ok()?;
    serde_json::from_str(&s).ok()
}

fn write_pending(dir: &Path, p: &PendingFile) -> Result<(), String> {
    let path = dir.join(PENDING_FILE);
    let tmp = dir.join(format!("{PENDING_FILE}.tmp"));
    let js = serde_json::to_vec_pretty(p).map_err(|e| e.to_string())?;
    fs::write(&tmp, js).map_err(|e| e.to_string())?;
    fs::rename(&tmp, &path).map_err(|e| e.to_string())
}

fn iso_now() -> String {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Compact sortable stamp; full RFC3339 not required for admin UI.
    format!("{t}")
}

/// If pending.json matches the live embedder, drain docs.jsonl into the
/// (empty) index. Spawned once from `main` after the server is up.
pub fn spawn_drain_if_pending(
    index: Arc<TextIndex>,
    hub: EmbedMigrateHub,
) -> Option<tokio::task::JoinHandle<()>> {
    let dir = hub.migrate_dir()?;
    let pending = read_pending(&dir)?;
    if pending.target_dim != index.dim() || pending.target_model != index.embedder_model() {
        info!(
            pending_model = %pending.target_model,
            pending_dim = pending.target_dim,
            live_model = %index.embedder_model(),
            live_dim = index.dim(),
            "embed migrate pending but embedder mismatch; waiting for dump/restart"
        );
        return None;
    }
    if !matches!(
        pending.state,
        MigrateState::AwaitingRestart | MigrateState::Draining
    ) {
        return None;
    }

    Some(tokio::spawn(async move {
        if let Err(e) = drain_loop(index, hub.clone(), dir).await {
            hub.fail(&e);
        }
    }))
}

type Row = (String, String, String, serde_json::Value);
// (bucket, id, text, metadata) — grouped by bucket for upsert_text_bulk

/// Drain tuning. The embedder is the bottleneck, so the drain keeps
/// `concurrency` batches of `batch` docs in flight at once.
#[derive(Debug, Clone, Copy)]
struct DrainConfig {
    batch: usize,
    concurrency: usize,
}

impl DrainConfig {
    fn from_env() -> Self {
        let read = |k: &str, d: usize| {
            std::env::var(k)
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .filter(|n| *n > 0)
                .unwrap_or(d)
        };
        Self {
            batch: read("NEBULA_EMBED_MIGRATE_BATCH", DEFAULT_BATCH),
            concurrency: read("NEBULA_EMBED_MIGRATE_CONCURRENCY", 1),
        }
    }
}

const FLUSH_ATTEMPTS: u32 = 6;

async fn drain_loop(
    index: Arc<TextIndex>,
    hub: EmbedMigrateHub,
    dir: PathBuf,
) -> Result<(), String> {
    drain_loop_with(index, hub, dir, DrainConfig::from_env()).await
}

async fn drain_loop_with(
    index: Arc<TextIndex>,
    hub: EmbedMigrateHub,
    dir: PathBuf,
    cfg: DrainConfig,
) -> Result<(), String> {
    let docs_path = dir.join(DOCS_FILE);
    let file = File::open(&docs_path).map_err(|e| format!("open dump: {e}"))?;
    let reader = BufReader::new(file);
    let mut pending = read_pending(&dir).ok_or_else(|| "pending.json missing".to_string())?;
    pending.state = MigrateState::Draining;
    write_pending(&dir, &pending)?;
    hub.set_draining(pending.drained);
    info!(
        drained = pending.drained,
        batch = cfg.batch,
        concurrency = cfg.concurrency,
        "embed migrate drain starting"
    );

    let mut skip = pending.drained;
    let mut drained = pending.drained;
    // One group = up to `concurrency` batches flushed together. `drained`
    // only advances after the whole group lands, so a crash resumes at a
    // group boundary and re-upserts at most one group (upserts are
    // idempotent by (bucket, id)).
    let group_cap = cfg.batch * cfg.concurrency;
    let mut group: Vec<Row> = Vec::with_capacity(group_cap);

    for line in reader.lines() {
        if hub.inner.cancel.load(Ordering::SeqCst) {
            return Err("cancelled during drain".into());
        }
        let line = line.map_err(|e| e.to_string())?;
        if line.is_empty() {
            continue;
        }
        if skip > 0 {
            skip -= 1;
            continue;
        }
        let row: DumpLine = serde_json::from_str(&line).map_err(|e| e.to_string())?;
        group.push((row.bucket, row.id, row.text, row.metadata));

        if group.len() >= group_cap {
            drained =
                flush_group(&index, &hub, &dir, &mut pending, drained, &mut group, cfg).await?;
        }
    }
    if !group.is_empty() {
        drained = flush_group(&index, &hub, &dir, &mut pending, drained, &mut group, cfg).await?;
    }

    // Snapshot so a crash after clearing pending does not lose the corpus.
    if let Err(e) = index.snapshot() {
        warn!(error = %e, "embed migrate: snapshot after drain failed (continuing)");
    }

    pending.state = MigrateState::Done;
    pending.drained = drained;
    write_pending(&dir, &pending)?;
    // Leave docs.jsonl for audit; clear the boot trigger by rewriting state Done
    // and renaming pending so a later restart does not re-archive.
    let _ = fs::rename(dir.join(PENDING_FILE), dir.join("pending.done.json"));
    hub.set_done(drained);
    info!(drained, "embed migrate drain complete");
    Ok(())
}

async fn flush_group(
    index: &Arc<TextIndex>,
    hub: &EmbedMigrateHub,
    dir: &Path,
    pending: &mut PendingFile,
    drained: u64,
    group: &mut Vec<Row>,
    cfg: DrainConfig,
) -> Result<u64, String> {
    let rows = std::mem::take(group);
    let n = rows.len() as u64;
    let mut tasks = Vec::with_capacity(cfg.concurrency);
    let mut it = rows.into_iter().peekable();
    while it.peek().is_some() {
        let batch: Vec<Row> = it.by_ref().take(cfg.batch).collect();
        let index = Arc::clone(index);
        // Spawned so each batch's HNSW insert (block_in_place) runs on
        // its own worker while the other batches wait on the embedder.
        tasks.push(tokio::spawn(
            async move { flush_batch(&index, batch).await },
        ));
    }
    for t in tasks {
        t.await.map_err(|e| format!("drain task: {e}"))??;
    }
    let drained = drained + n;
    pending.drained = drained;
    pending.state = MigrateState::Draining;
    write_pending(dir, pending)?;
    hub.set_draining(drained);
    Ok(drained)
}

/// Upsert one batch, retrying transient embedder failures (timeouts,
/// 5xx, an overloaded Ollama returning 400) with exponential backoff.
async fn flush_batch(index: &Arc<TextIndex>, batch: Vec<Row>) -> Result<(), String> {
    use std::collections::HashMap;
    let mut by_bucket: HashMap<String, Vec<(String, String, serde_json::Value)>> = HashMap::new();
    for (bucket, id, text, meta) in batch {
        by_bucket.entry(bucket).or_default().push((id, text, meta));
    }
    for (bucket, items) in by_bucket {
        let mut attempt = 0;
        loop {
            match index.upsert_text_bulk(&bucket, &items).await {
                Ok(_) => break,
                Err(e) if attempt + 1 < FLUSH_ATTEMPTS => {
                    attempt += 1;
                    let wait = std::time::Duration::from_millis(500u64 << attempt);
                    warn!(%bucket, attempt, error = %e, ?wait, "embed migrate: batch failed, retrying");
                    tokio::time::sleep(wait).await;
                }
                Err(e) => return Err(format!("upsert_text_bulk({bucket}): {e}")),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebula_embed::{Embedder, MockEmbedder};
    use nebula_vector::{HnswConfig, Metric};
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread")]
    async fn dump_writes_pending_and_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(8));
        let idx =
            TextIndex::open_persistent(emb, Metric::Cosine, HnswConfig::default(), dir.path())
                .unwrap();
        idx.upsert_text("b", "1", "hello", serde_json::json!({}))
            .await
            .unwrap();
        idx.upsert_text("b", "2", "world", serde_json::json!({}))
            .await
            .unwrap();

        let hub = EmbedMigrateHub::new(Some(dir.path().to_path_buf()));
        let st = hub
            .start_dump(&idx, "nomic-embed-text".into(), 768, None)
            .unwrap();
        assert_eq!(st.state, MigrateState::AwaitingRestart);
        assert_eq!(st.dumped, 2);
        assert_eq!(st.target_dim, 768);
        let pending = read_pending(&dir.path().join(MIGRATE_DIR)).unwrap();
        assert_eq!(pending.target_model, "nomic-embed-text");
        let lines = std::fs::read_to_string(dir.path().join(MIGRATE_DIR).join(DOCS_FILE)).unwrap();
        assert_eq!(lines.lines().count(), 2);
    }

    /// Dump 11 docs, then drain them into a fresh index with several
    /// batches in flight, resuming from a mid-dump checkpoint.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_drain_resumes_and_lands_every_doc() {
        let src_dir = tempfile::tempdir().unwrap();
        let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(8));
        let src = TextIndex::open_persistent(
            Arc::clone(&emb),
            Metric::Cosine,
            HnswConfig::default(),
            src_dir.path(),
        )
        .unwrap();
        for i in 0..11 {
            let bucket = if i % 2 == 0 { "a" } else { "b" };
            src.upsert_text(
                bucket,
                &i.to_string(),
                &format!("doc {i}"),
                serde_json::json!({ "i": i }),
            )
            .await
            .unwrap();
        }
        let src_hub = EmbedMigrateHub::new(Some(src_dir.path().to_path_buf()));
        let model = src.embedder_model().to_string();
        src_hub.start_dump(&src, model, 8, None).unwrap();

        let dst_dir = tempfile::tempdir().unwrap();
        let dst = Arc::new(
            TextIndex::open_persistent(emb, Metric::Cosine, HnswConfig::default(), dst_dir.path())
                .unwrap(),
        );
        let mig = src_dir.path().join(MIGRATE_DIR);
        // Pretend a previous run already landed the first 3 lines.
        let mut p = read_pending(&mig).unwrap();
        p.drained = 3;
        write_pending(&mig, &p).unwrap();

        let hub = EmbedMigrateHub::new(Some(src_dir.path().to_path_buf()));
        drain_loop_with(
            Arc::clone(&dst),
            hub.clone(),
            mig.clone(),
            DrainConfig {
                batch: 2,
                concurrency: 3,
            },
        )
        .await
        .unwrap();

        assert_eq!(hub.status().state, MigrateState::Done);
        assert_eq!(hub.status().drained, 11);
        let landed =
            dst.for_each_doc_in_bucket("a", |_| {}) + dst.for_each_doc_in_bucket("b", |_| {});
        assert_eq!(landed, 8, "lines 4..=11 re-embedded, first 3 skipped");
        assert!(mig.join("pending.done.json").exists());
    }
}
