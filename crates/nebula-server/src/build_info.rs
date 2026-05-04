//! Build-time identity: version + git sha.
//!
//! Exposed over `/healthz` (short form) and `/api/v1/admin/version` (full).
//! The operator reads this to detect that an upgrade has actually taken
//! effect inside the pod — relying on the image tag alone misses cases
//! where the tag was retagged in-place.
//!
//! # Optional env at compile time
//! - `NEBULADB_GIT_SHA` — short sha, set by CI. Falls back to `"unknown"`.
//! - `NEBULADB_BUILD_DATE` — ISO-8601 build date, set by CI. Falls back to `"unknown"`.

/// Cargo package version, e.g. `"0.1.0"`. Always present.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Git short sha when provided via env at build time, else `"unknown"`.
pub const GIT_COMMIT: &str = match option_env!("NEBULADB_GIT_SHA") {
    Some(v) => v,
    None => "unknown",
};

/// Build date when provided via env at build time, else `"unknown"`.
pub const BUILD_DATE: &str = match option_env!("NEBULADB_BUILD_DATE") {
    Some(v) => v,
    None => "unknown",
};

/// OS name the binary was compiled for (e.g. `"linux"`).
pub const OS: &str = std::env::consts::OS;

/// CPU architecture (e.g. `"x86_64"`, `"aarch64"`).
pub const ARCH: &str = std::env::consts::ARCH;
