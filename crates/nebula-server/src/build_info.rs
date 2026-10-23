//! Build-time identity: version + git sha + build date.
//!
//! Exposed over `/healthz` (short form) and `/api/v1/admin/version` (full).
//! The operator reads this to detect that an upgrade has actually taken
//! effect inside the pod — relying on the image tag alone misses cases
//! where the tag was retagged in-place.
//!
//! # Optional env at compile time (set by CI / Dockerfile ARGs)
//! - `NEBULADB_VERSION` — release tag without `v` (e.g. `0.1.54`). Falls
//!   back to `CARGO_PKG_VERSION` when unset.
//! - `NEBULADB_GIT_SHA` — short sha. Falls back to `"unknown"`.
//! - `NEBULADB_BUILD_DATE` — ISO-8601 build instant. Falls back to `"unknown"`.
//!
//! # Optional env at runtime (Helm)
//! - `NEBULA_DEPLOYED_AT` — when this chart render/upgrade ran (RFC3339).
//! - `NEBULA_ENV` — ring name (`int` / `prod` / …).
//! - `NEBULA_IMAGE_TAG` — container image tag (often the short sha).

/// Release version shown in the UI (e.g. `"0.1.54"`). Prefer the git tag
/// baked in by CI over the workspace Cargo version, which lags tags.
pub const VERSION: &str = {
    match option_env!("NEBULADB_VERSION") {
        Some(v) if !v.is_empty() => v,
        _ => env!("CARGO_PKG_VERSION"),
    }
};

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

fn runtime_env(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) => {
            let t = v.trim();
            if t.is_empty() || t.eq_ignore_ascii_case("unknown") {
                None
            } else {
                Some(t.to_string())
            }
        }
        Err(_) => None,
    }
}

/// Helm `NEBULA_DEPLOYED_AT` (RFC3339), if set on the pod.
pub fn deployed_at() -> Option<String> {
    runtime_env("NEBULA_DEPLOYED_AT")
}

/// Helm `NEBULA_ENV` (ring / environment name), if set.
pub fn deploy_env() -> Option<String> {
    runtime_env("NEBULA_ENV")
}

/// Helm `NEBULA_IMAGE_TAG`, if set.
pub fn image_tag() -> Option<String> {
    runtime_env("NEBULA_IMAGE_TAG")
}
