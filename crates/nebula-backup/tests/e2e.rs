//! End-to-end backup + restore test against a real persistent index
//! and a LocalFsTarget. Drives the actual backup engine and the
//! actual restore flow; the only thing not exercised vs. production
//! is the S3 wire format (the Local target is the same trait).

use std::sync::Arc;

use nebula_backup::{Backup, BackupTarget, LocalFsTarget, Restore};
use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_vector::{HnswConfig, Metric};
use tempfile::tempdir;

fn embedder() -> Arc<dyn Embedder> {
    Arc::new(MockEmbedder::new(8))
}

#[tokio::test]
async fn backup_then_restore_round_trip() {
    // Source cluster: persistent index with two buckets and a few docs.
    let src_dir = tempdir().unwrap();
    let src = Arc::new(
        TextIndex::open_persistent(
            embedder(),
            Metric::Cosine,
            HnswConfig::default(),
            src_dir.path(),
        )
        .unwrap(),
    );
    src.upsert_text("docs", "1", "zero trust", serde_json::json!({"r": "eu"}))
        .await
        .unwrap();
    src.upsert_text("docs", "2", "kubernetes gitops", serde_json::json!({"r": "us"}))
        .await
        .unwrap();
    src.upsert_text(
        "logs",
        "ev-1",
        "production incident",
        serde_json::json!({"sev": "high"}),
    )
    .await
    .unwrap();

    // Storage target = local directory.
    let storage_dir = tempdir().unwrap();
    let target: Arc<dyn BackupTarget> = Arc::new(LocalFsTarget::new(storage_dir.path()));
    let staging = tempdir().unwrap();

    // Run a backup.
    let manifest = Backup {
        index: Arc::clone(&src),
        target: Arc::clone(&target),
        cluster_name: "nebula-test".into(),
        region: "us-east-1".into(),
        server_version: env!("CARGO_PKG_VERSION").into(),
        git_commit: "deadbeef".into(),
        staging_dir: staging.path().to_path_buf(),
    }
    .run()
    .await
    .unwrap();

    assert_eq!(manifest.cluster_name, "nebula-test");
    assert_eq!(manifest.embedder.dim, 8);
    assert_eq!(manifest.doc_count, 3);
    assert_eq!(manifest.bucket_count, 2);
    assert!(!manifest.artifacts.is_empty());
    assert_eq!(manifest.artifacts[0].name, "base.tar.zst");
    assert_eq!(manifest.artifacts[0].sha256.len(), 64);

    // Verify the manifest object actually landed at the expected key.
    let manifest_bytes = target
        .get(&format!("backups/{}/manifest.json", manifest.backup_id))
        .await
        .unwrap();
    assert!(!manifest_bytes.is_empty());

    // Drop the source index handle so reopen on the same dir later
    // doesn't trip the single-process WAL lock.
    drop(src);

    // Restore into a fresh data dir.
    let restore_dir = tempdir().unwrap();
    let restore_staging = tempdir().unwrap();
    let _restored_manifest = Restore {
        target: Arc::clone(&target),
        backup_id: manifest.backup_id.clone(),
        staging_dir: restore_staging.path().to_path_buf(),
        data_dir: restore_dir.path().to_path_buf(),
        server_version: env!("CARGO_PKG_VERSION").into(),
        embedder_model: "mock-8".into(),
        embedder_dim: 8,
    }
    .run()
    .await
    .unwrap();

    // Re-open the restored index and confirm every doc is back.
    let restored = TextIndex::open_persistent(
        embedder(),
        Metric::Cosine,
        HnswConfig::default(),
        restore_dir.path(),
    )
    .unwrap();
    assert_eq!(restored.len(), 3);
    assert!(restored.get("docs", "1").is_some());
    assert!(restored.get("docs", "2").is_some());
    assert!(restored.get("logs", "ev-1").is_some());
}

/// Restore must refuse a target whose embedder doesn't match the
/// manifest — silent acceptance would corrupt similarity scores.
#[tokio::test]
async fn restore_rejects_embedder_mismatch() {
    let src_dir = tempdir().unwrap();
    let src = Arc::new(
        TextIndex::open_persistent(
            embedder(),
            Metric::Cosine,
            HnswConfig::default(),
            src_dir.path(),
        )
        .unwrap(),
    );
    src.upsert_text("b", "x", "hi", serde_json::json!({}))
        .await
        .unwrap();

    let storage = tempdir().unwrap();
    let target: Arc<dyn BackupTarget> = Arc::new(LocalFsTarget::new(storage.path()));
    let staging = tempdir().unwrap();
    let m = Backup {
        index: src,
        target: Arc::clone(&target),
        cluster_name: "c".into(),
        region: String::new(),
        server_version: env!("CARGO_PKG_VERSION").into(),
        git_commit: String::new(),
        staging_dir: staging.path().to_path_buf(),
    }
    .run()
    .await
    .unwrap();

    // Now try to restore with a mismatched dim.
    let restore_dir = tempdir().unwrap();
    let restore_staging = tempdir().unwrap();
    let err = Restore {
        target,
        backup_id: m.backup_id,
        staging_dir: restore_staging.path().to_path_buf(),
        data_dir: restore_dir.path().to_path_buf(),
        server_version: env!("CARGO_PKG_VERSION").into(),
        embedder_model: "mock".into(),
        embedder_dim: 16,
    }
    .run()
    .await
    .unwrap_err();
    matches!(err, nebula_backup::Error::EmbedderMismatch { .. });
}

/// Restore into a non-empty data dir is rejected up front.
#[tokio::test]
async fn restore_refuses_non_empty_data_dir() {
    let src_dir = tempdir().unwrap();
    let src = Arc::new(
        TextIndex::open_persistent(
            embedder(),
            Metric::Cosine,
            HnswConfig::default(),
            src_dir.path(),
        )
        .unwrap(),
    );
    src.upsert_text("b", "x", "hi", serde_json::json!({}))
        .await
        .unwrap();

    let storage = tempdir().unwrap();
    let target: Arc<dyn BackupTarget> = Arc::new(LocalFsTarget::new(storage.path()));
    let staging = tempdir().unwrap();
    let m = Backup {
        index: src,
        target: Arc::clone(&target),
        cluster_name: "c".into(),
        region: String::new(),
        server_version: env!("CARGO_PKG_VERSION").into(),
        git_commit: String::new(),
        staging_dir: staging.path().to_path_buf(),
    }
    .run()
    .await
    .unwrap();

    let restore_dir = tempdir().unwrap();
    // Pre-populate the dir to look like an existing cluster.
    std::fs::create_dir_all(restore_dir.path().join("wal")).unwrap();
    std::fs::write(restore_dir.path().join("wal/0000000000.nwal"), b"NEBWAL01x").unwrap();

    let restore_staging = tempdir().unwrap();
    let err = Restore {
        target,
        backup_id: m.backup_id,
        staging_dir: restore_staging.path().to_path_buf(),
        data_dir: restore_dir.path().to_path_buf(),
        server_version: env!("CARGO_PKG_VERSION").into(),
        embedder_model: "mock-8".into(),
        embedder_dim: 8,
    }
    .run()
    .await
    .unwrap_err();
    matches!(err, nebula_backup::Error::DataDirNotEmpty { .. });
}
