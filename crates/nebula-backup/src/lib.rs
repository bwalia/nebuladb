//! NebulaDB backup + restore.
//!
//! See `docs/design/0002-backup-restore.md` for the overall design.
//! This crate is intentionally protocol-shaped (storage targets +
//! manifests + bundle pack/unpack) and protocol-agnostic — the
//! orchestrator that ties snapshot, WAL enumeration, and upload
//! together is a thin layer that the server crate calls.
//!
//! Public surface:
//!
//! - [`Manifest`] — wire format for the per-backup inventory.
//! - [`BackupTarget`] — trait every storage backend implements.
//! - [`S3Target`], [`LocalFsTarget`] — the two implementations we ship.
//! - [`bundle::pack_tar_zstd`] / [`bundle::unpack_tar_zstd`] — bundle
//!   helpers used by the orchestrator (also useful in tests).

pub mod bundle;
pub mod engine;
pub mod error;
pub mod manifest;
pub mod target;

pub use engine::{Backup, Restore};
pub use error::{Error, Result};
pub use manifest::{Artifact, BackupKind, BucketState, EmbedderInfo, Manifest, WalRange};
pub use target::{
    BackupTarget, DownloadOutcome, LocalFsTarget, S3Target, StreamingUpload, UploadOutcome,
};
