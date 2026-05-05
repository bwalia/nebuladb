//! Tar+zstd bundle helpers. The backup pipeline does:
//!
//! ```text
//! snapshot()  + WAL segments  -> tar -> zstd -> StreamingUpload
//! ```
//!
//! Restore reverses it:
//!
//! ```text
//! StreamingDownload -> zstd-decode -> untar -> NEBULA_DATA_DIR
//! ```
//!
//! We deliberately materialize the bundle on local disk (under a
//! tempdir) before upload rather than streaming everything in one
//! pipeline — it's slower but it makes the sha256 + size in the
//! manifest trivial to compute (just hash the file once after it's
//! written) and lets us retry the upload on transient S3 failures
//! without re-running the snapshot.

use std::path::{Path, PathBuf};

use sha2::Digest;
use tracing::debug;

use crate::error::{Error, Result};

/// Pack `files` (each `(arcname, on-disk path)` pair) into a
/// `tar.zst` at `out_path`. Returns the resulting file's
/// (size_bytes, sha256_hex) so the manifest can be filled in directly.
///
/// `arcname` lets the caller present a clean directory layout in the
/// archive (e.g., `wal/0000000000.nwal`) regardless of where the file
/// lives on disk.
pub fn pack_tar_zstd(out_path: &Path, files: &[(String, PathBuf)]) -> Result<(u64, String)> {
    let out_file = std::fs::File::create(out_path)?;
    // zstd level 3 is the standard compromise — close to lz4 speed,
    // close to default-zstd ratio. Bumping to 6+ trades CPU for ~5%
    // size; not worth it on the backup hot path.
    let zstd_writer = zstd::Encoder::new(out_file, 3)
        .map_err(|e| Error::Other(format!("zstd init: {e}")))?
        .auto_finish();
    {
        let mut tar = tar::Builder::new(zstd_writer);
        for (arcname, path) in files {
            debug!(%arcname, ?path, "packing");
            let mut f = std::fs::File::open(path)?;
            tar.append_file(arcname, &mut f)?;
        }
        tar.finish()?;
    }
    // tar.finish() flushes; the zstd auto-finish writer flushes on
    // drop. Now hash + size the result.
    let mut hasher = sha2::Sha256::new();
    let mut f = std::fs::File::open(out_path)?;
    let mut buf = [0u8; 64 * 1024];
    let mut total: u64 = 0;
    loop {
        use std::io::Read;
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((total, hex::encode(hasher.finalize())))
}

/// Reverse of [`pack_tar_zstd`]: extract the archive at `archive_path`
/// into `dest_dir`. Creates the directory if it doesn't exist; refuses
/// to extract if the directory is non-empty (delegated to caller — we
/// don't enforce here so the orchestrator can decide).
pub fn unpack_tar_zstd(archive_path: &Path, dest_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dest_dir)?;
    let f = std::fs::File::open(archive_path)?;
    let zstd_reader =
        zstd::Decoder::new(f).map_err(|e| Error::Other(format!("zstd init: {e}")))?;
    let mut archive = tar::Archive::new(zstd_reader);
    // `unpack` honors entry paths; tar entries we control above use
    // safe relative names (`base/`, `wal/...`), so absolute paths or
    // `..` traversal aren't a concern. Document the assumption and
    // move on; a stricter mode is an `entry_safe_unpack` follow-up.
    archive.unpack(dest_dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn roundtrip_pack_then_unpack() {
        let stage = tempdir().unwrap();
        let src = stage.path().join("src");
        std::fs::create_dir(&src).unwrap();
        std::fs::write(src.join("hello.txt"), b"world").unwrap();
        std::fs::write(src.join("nested.bin"), b"binary").unwrap();

        let archive = stage.path().join("out.tar.zst");
        let (size, sha) = pack_tar_zstd(
            &archive,
            &[
                ("hello.txt".into(), src.join("hello.txt")),
                ("nested.bin".into(), src.join("nested.bin")),
            ],
        )
        .unwrap();
        assert!(size > 0);
        assert_eq!(sha.len(), 64);

        let dest = stage.path().join("dest");
        unpack_tar_zstd(&archive, &dest).unwrap();
        let hello = std::fs::read_to_string(dest.join("hello.txt")).unwrap();
        assert_eq!(hello, "world");
        let nested = std::fs::read(dest.join("nested.bin")).unwrap();
        assert_eq!(nested, b"binary");
    }
}
