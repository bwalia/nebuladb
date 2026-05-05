//! Backup/restore errors. Distinct from `nebula_core::NebulaError`
//! because failure modes here are storage-shaped (S3 quota, network,
//! checksum mismatch) rather than index-shaped.

use thiserror::Error;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    /// Object-storage error. Any AWS SDK failure or local-fs I/O
    /// surfaces here. We wrap the inner error as a string because
    /// `aws_sdk_s3::Error` is heavy and threading it through the
    /// public API would force every caller to depend on the SDK.
    #[error("storage: {0}")]
    Storage(String),

    /// Local filesystem I/O. Used for staging and unpack.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// Manifest decode / serialization failure. A partial or
    /// hand-edited manifest produces this — surface it loudly so the
    /// operator knows the backup is unrecoverable.
    #[error("manifest: {0}")]
    Manifest(String),

    /// SHA-256 mismatch during restore. Carries enough context for the
    /// operator to know which file failed.
    #[error("checksum mismatch on {artifact}: expected {expected}, got {got}")]
    Checksum {
        artifact: String,
        expected: String,
        got: String,
    },

    /// Restore-time validation: target cluster's binary is older than
    /// the backup's recorded version. Restore would corrupt state.
    #[error("version skew: backup is from {backup_version}, target runs {target_version}")]
    VersionSkew {
        backup_version: String,
        target_version: String,
    },

    /// Restore-time validation: target embedder differs from backup.
    /// Silent acceptance would corrupt similarity search.
    #[error("embedder mismatch: backup uses {backup} (dim {backup_dim}), target uses {target} (dim {target_dim})")]
    EmbedderMismatch {
        backup: String,
        backup_dim: usize,
        target: String,
        target_dim: usize,
    },

    /// Restore tried to land on a non-empty data dir. Silent overwrite
    /// is the kind of data loss we never want to inflict.
    #[error("target data dir is not empty: {path}")]
    DataDirNotEmpty { path: String },

    /// Catch-all for paths we don't have a typed variant for. Keep
    /// usage rare — typed variants are easier to react to.
    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Convert from a `nebula_index::IndexError`. Stringified — the
    /// index error type isn't stable across versions and we don't
    /// want backup callers branching on its variants.
    pub fn from_index(e: nebula_index::IndexError) -> Self {
        Error::Other(format!("index: {e}"))
    }

    /// Convert from a `nebula_wal::WalError`.
    pub fn from_wal(e: nebula_wal::WalError) -> Self {
        Error::Other(format!("wal: {e}"))
    }
}
