//! Embedding-space identity guard (design 0010 §5).
//!
//! Vectors from two different models — even at the same
//! dimensionality — live in incompatible spaces; mixing them corrupts
//! every similarity score with no error anywhere. This module stamps
//! `(model, dim)` into `<data_dir>/embedder.json` on first persistent
//! boot and refuses to start when a later boot's embedder disagrees.

use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct EmbedderIdentity {
    pub model: String,
    pub dim: usize,
}

const FILE_NAME: &str = "embedder.json";

/// Compare the configured embedder against the stamped identity.
/// First boot stamps; mismatch is a hard startup error unless
/// `override_mismatch` is set, which re-stamps the new identity
/// (an intentional migration where the operator wiped/rebuilt data).
pub fn check_or_stamp(
    data_dir: &Path,
    model: &str,
    dim: usize,
    override_mismatch: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(data_dir)?;
    let path = data_dir.join(FILE_NAME);
    let current = EmbedderIdentity {
        model: model.to_string(),
        dim,
    };
    match std::fs::read_to_string(&path) {
        Ok(raw) => {
            let stamped: EmbedderIdentity = serde_json::from_str(&raw)
                .map_err(|e| format!("corrupt {}: {e}", path.display()))?;
            if stamped == current {
                return Ok(());
            }
            if override_mismatch {
                tracing::warn!(
                    old_model = %stamped.model,
                    old_dim = stamped.dim,
                    new_model = %current.model,
                    new_dim = current.dim,
                    "embedder identity override: re-stamping — existing vectors are \
                     INVALID in the new space unless the corpus was re-ingested"
                );
                stamp(&path, &current)?;
                return Ok(());
            }
            Err(format!(
                "embedder mismatch: data dir was built with model '{}' (dim {}), but this \
                 boot is configured for '{}' (dim {}). Mixing embedding spaces silently \
                 corrupts similarity scores. Restore the original embedder config, or \
                 re-ingest into a fresh data dir, or set \
                 NEBULA_EMBEDDER_IDENTITY_OVERRIDE=1 to accept the new identity.",
                stamped.model, stamped.dim, current.model, current.dim
            )
            .into())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            stamp(&path, &current)?;
            Ok(())
        }
        Err(e) => Err(format!("cannot read {}: {e}", path.display()).into()),
    }
}

/// tmp + rename so a crash mid-write can't leave a torn stamp.
fn stamp(path: &Path, id: &EmbedderIdentity) -> std::io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(
        &tmp,
        serde_json::to_vec_pretty(id).expect("identity serializes"),
    )?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_boot_stamps_and_second_boot_matches() {
        let dir = tempfile::tempdir().unwrap();
        check_or_stamp(dir.path(), "text-embedding-3-small", 1536, false).unwrap();
        check_or_stamp(dir.path(), "text-embedding-3-small", 1536, false).unwrap();
    }

    #[test]
    fn model_change_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        check_or_stamp(dir.path(), "model-a", 768, false).unwrap();
        let err = check_or_stamp(dir.path(), "model-b", 768, false).unwrap_err();
        assert!(err.to_string().contains("embedder mismatch"));
    }

    #[test]
    fn dim_change_is_refused_even_for_same_model() {
        let dir = tempfile::tempdir().unwrap();
        check_or_stamp(dir.path(), "model-a", 768, false).unwrap();
        assert!(check_or_stamp(dir.path(), "model-a", 1536, false).is_err());
    }

    #[test]
    fn override_restamps() {
        let dir = tempfile::tempdir().unwrap();
        check_or_stamp(dir.path(), "model-a", 768, false).unwrap();
        check_or_stamp(dir.path(), "model-b", 768, true).unwrap();
        // New identity is now the stamp.
        check_or_stamp(dir.path(), "model-b", 768, false).unwrap();
        assert!(check_or_stamp(dir.path(), "model-a", 768, false).is_err());
    }
}
