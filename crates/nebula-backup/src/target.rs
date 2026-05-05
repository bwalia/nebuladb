//! Storage target abstraction.
//!
//! A [`BackupTarget`] is "somewhere a backup can be written to or
//! read from." We ship two implementations:
//!
//! - [`S3Target`]: production. Uses `aws-sdk-s3` so any S3-compatible
//!   endpoint works (AWS, MinIO, R2, B2, Wasabi, GCS via S3 interop).
//! - [`LocalFsTarget`]: tests + dev. Reads/writes a directory on the
//!   local filesystem; lets the integration tests round-trip a backup
//!   without spinning up MinIO.
//!
//! The trait is small on purpose — every method maps to one S3 API
//! call. Multipart upload is folded into `put_streaming` because S3
//! exposes its own multipart machinery and there's no benefit to
//! re-deriving it on top of `put_object` + range PUTs.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use crate::error::{Error, Result};

/// A storage target that backups can write to and restore from.
///
/// Methods are async because every real implementation does I/O.
/// Implementations are expected to be cheap to clone — most call
/// sites stash an Arc.
#[async_trait]
pub trait BackupTarget: Send + Sync {
    /// Write a blob in one shot. Used for small artifacts (the
    /// manifest, sha256 sidecar). Larger blobs go through
    /// [`put_streaming`].
    async fn put(&self, key: &str, body: Bytes) -> Result<()>;

    /// Stream-upload a blob. Implementation chooses its own chunking
    /// and concurrency; the caller writes into the returned writer
    /// and the operation finishes when the writer is dropped via
    /// `finish().await`.
    ///
    /// Returns the total bytes written + sha256 of the data, computed
    /// as the bytes flow through (so we don't have to read the object
    /// back to verify).
    async fn put_streaming(&self, key: &str) -> Result<Box<dyn StreamingUpload>>;

    /// Retrieve a blob in one shot.
    async fn get(&self, key: &str) -> Result<Bytes>;

    /// List object names under a prefix. Returns names *relative* to
    /// the target's base prefix — callers don't need to know whether
    /// the implementation is bucket-rooted or directory-rooted.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Delete a single object. Used by retention sweeps.
    async fn delete(&self, key: &str) -> Result<()>;

    /// Stream-download a blob to disk. Hands back the bytes-written
    /// + computed sha256 so the caller can validate against the
    /// manifest immediately after the body finishes.
    async fn download_to_path(&self, key: &str, path: &Path) -> Result<DownloadOutcome>;
}

/// Outcome of a [`BackupTarget::download_to_path`] call.
pub struct DownloadOutcome {
    pub bytes_written: u64,
    pub sha256_hex: String,
}

/// Active multi-part upload handle. The caller writes bytes through
/// `write_all`-style methods, then calls `finish().await` to commit.
/// Dropping without `finish` aborts the upload — implementations
/// must clean up partial state.
#[async_trait]
pub trait StreamingUpload: Send {
    /// Append bytes to the upload. Implementations may buffer and
    /// flush in chunks; callers don't need to chunk themselves.
    async fn write(&mut self, chunk: Bytes) -> Result<()>;

    /// Complete the upload. Returns total bytes + sha256 (hex).
    /// Calling write/finish after this is undefined.
    async fn finish(self: Box<Self>) -> Result<UploadOutcome>;

    /// Abort the upload, freeing any in-progress storage. Idempotent.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Bookkeeping a streaming upload returns when it commits.
pub struct UploadOutcome {
    pub bytes_written: u64,
    pub sha256_hex: String,
}

// =========================================================================
// LocalFsTarget — used by tests and the backup's own staging
// =========================================================================

/// Filesystem-backed target. The "bucket" is a directory; objects are
/// files relative to it. Useful both for tests and for users who want
/// to back up to a mounted volume (NFS, local SSD before sync).
pub struct LocalFsTarget {
    root: PathBuf,
}

impl LocalFsTarget {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }
}

#[async_trait]
impl BackupTarget for LocalFsTarget {
    async fn put(&self, key: &str, body: Bytes) -> Result<()> {
        let path = self.path(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(path, body.as_ref()).await?;
        Ok(())
    }

    async fn put_streaming(&self, key: &str) -> Result<Box<dyn StreamingUpload>> {
        let path = self.path(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::File::create(&path).await?;
        Ok(Box::new(LocalFsUpload {
            path,
            file: Some(file),
            hasher: sha2::Sha256::default(),
            bytes_written: 0,
        }))
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let path = self.path(key);
        let bytes = tokio::fs::read(path).await?;
        Ok(Bytes::from(bytes))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let dir = self.path(prefix);
        let mut out = Vec::new();
        let mut walker = match tokio::fs::read_dir(&dir).await {
            Ok(w) => w,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = walker.next_entry().await? {
            if let Some(name) = entry.file_name().to_str() {
                out.push(format!("{}/{}", prefix.trim_end_matches('/'), name));
            }
        }
        out.sort();
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.path(key);
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn download_to_path(&self, key: &str, dest: &Path) -> Result<DownloadOutcome> {
        use sha2::Digest;
        let bytes = tokio::fs::read(self.path(key)).await?;
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut hasher = sha2::Sha256::new();
        hasher.update(&bytes);
        let sha = hex::encode(hasher.finalize());
        let len = bytes.len() as u64;
        tokio::fs::write(dest, bytes).await?;
        Ok(DownloadOutcome {
            bytes_written: len,
            sha256_hex: sha,
        })
    }
}

struct LocalFsUpload {
    path: PathBuf,
    /// Some until finish/abort consumes it.
    file: Option<tokio::fs::File>,
    hasher: sha2::Sha256,
    bytes_written: u64,
}

#[async_trait]
impl StreamingUpload for LocalFsUpload {
    async fn write(&mut self, chunk: Bytes) -> Result<()> {
        use sha2::Digest;
        let f = self
            .file
            .as_mut()
            .ok_or_else(|| Error::Other("write after finish".into()))?;
        f.write_all(&chunk).await?;
        self.hasher.update(&chunk);
        self.bytes_written += chunk.len() as u64;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<UploadOutcome> {
        if let Some(mut f) = self.file.take() {
            f.flush().await?;
            f.sync_all().await?;
        }
        use sha2::Digest;
        Ok(UploadOutcome {
            bytes_written: self.bytes_written,
            sha256_hex: hex::encode(self.hasher.finalize()),
        })
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        // Drop the partial file so a retry doesn't see stale bytes.
        drop(self.file.take());
        let _ = tokio::fs::remove_file(&self.path).await;
        Ok(())
    }
}

// =========================================================================
// S3Target — production
// =========================================================================

/// S3-compatible target. Built once at config load; cheap to clone.
///
/// The `prefix` is folded into every key so a single bucket can host
/// backups for multiple clusters without collision.
#[derive(Clone)]
pub struct S3Target {
    client: Arc<S3Client>,
    bucket: String,
    prefix: String,
    /// Multipart part size in bytes. S3 mandates a minimum of 5 MiB
    /// for all parts except the last; we default to 16 MiB which is
    /// the AWS-recommended sweet spot for throughput vs. retry cost.
    part_size: usize,
}

impl S3Target {
    /// Construct an S3 target. The credentials chain is whatever
    /// `aws-config` resolves: env vars, `~/.aws/credentials`, IAM
    /// instance role. For MinIO/R2/etc. set `endpoint_url` on the
    /// SDK config before passing the client in.
    pub fn new(client: Arc<S3Client>, bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            prefix: prefix.into().trim_matches('/').to_string(),
            part_size: 16 * 1024 * 1024,
        }
    }

    pub fn with_part_size(mut self, bytes: usize) -> Self {
        // S3's lower bound is 5 MiB; below that the API rejects the
        // upload. Clamp here so a misconfigured caller doesn't fail at
        // upload time with a confusing error.
        self.part_size = bytes.max(5 * 1024 * 1024);
        self
    }

    fn key_for(&self, name: &str) -> String {
        if self.prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", self.prefix, name)
        }
    }
}

#[async_trait]
impl BackupTarget for S3Target {
    async fn put(&self, key: &str, body: Bytes) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(self.key_for(key))
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    async fn put_streaming(&self, key: &str) -> Result<Box<dyn StreamingUpload>> {
        let resolved_key = self.key_for(key);
        let create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&resolved_key)
            .send()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let upload_id = create
            .upload_id
            .ok_or_else(|| Error::Storage("S3 returned no upload_id".into()))?;
        Ok(Box::new(S3MultipartUpload {
            client: Arc::clone(&self.client),
            bucket: self.bucket.clone(),
            key: resolved_key,
            upload_id,
            part_size: self.part_size,
            buf: Vec::with_capacity(self.part_size),
            parts: Vec::new(),
            hasher: sha2::Sha256::default(),
            bytes_written: 0,
            done: false,
        }))
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key_for(key))
            .send()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .into_bytes();
        Ok(bytes)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let resolved = self.key_for(prefix);
        let mut continuation = None;
        let mut out = Vec::new();
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&resolved);
            if let Some(t) = &continuation {
                req = req.continuation_token(t);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            for o in resp.contents() {
                if let Some(k) = o.key() {
                    // Strip the target's own prefix so the caller
                    // sees relative names, matching LocalFsTarget.
                    let rel = match self.prefix.is_empty() {
                        true => k.to_string(),
                        false => k
                            .strip_prefix(&format!("{}/", self.prefix))
                            .unwrap_or(k)
                            .to_string(),
                    };
                    out.push(rel);
                }
            }
            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(|s| s.to_string());
                if continuation.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        out.sort();
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.key_for(key))
            .send()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    async fn download_to_path(&self, key: &str, dest: &Path) -> Result<DownloadOutcome> {
        use sha2::Digest;
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key_for(key))
            .send()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut file = tokio::fs::File::create(dest).await?;
        let mut hasher = sha2::Sha256::new();
        let mut total: u64 = 0;
        while let Some(chunk) = resp
            .body
            .try_next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            file.write_all(&chunk).await?;
            hasher.update(&chunk);
            total += chunk.len() as u64;
        }
        file.flush().await?;
        file.sync_all().await?;
        Ok(DownloadOutcome {
            bytes_written: total,
            sha256_hex: hex::encode(hasher.finalize()),
        })
    }
}

struct S3MultipartUpload {
    client: Arc<S3Client>,
    bucket: String,
    key: String,
    upload_id: String,
    part_size: usize,
    buf: Vec<u8>,
    parts: Vec<aws_sdk_s3::types::CompletedPart>,
    hasher: sha2::Sha256,
    bytes_written: u64,
    /// Set after finish/abort so accidental reuse fails fast.
    done: bool,
}

impl S3MultipartUpload {
    /// Flush whatever's in the buffer as one S3 part.
    async fn flush_part(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let part_number = (self.parts.len() as i32) + 1;
        let body = ByteStream::from(std::mem::take(&mut self.buf));
        let resp = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .part_number(part_number)
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let etag = resp
            .e_tag()
            .ok_or_else(|| Error::Storage("upload_part returned no ETag".into()))?
            .to_string();
        self.parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .e_tag(etag)
                .part_number(part_number)
                .build(),
        );
        Ok(())
    }
}

#[async_trait]
impl StreamingUpload for S3MultipartUpload {
    async fn write(&mut self, chunk: Bytes) -> Result<()> {
        use sha2::Digest;
        if self.done {
            return Err(Error::Other("write after finish".into()));
        }
        self.hasher.update(&chunk);
        self.bytes_written += chunk.len() as u64;
        self.buf.extend_from_slice(&chunk);
        // Flush whole parts as soon as we cross the threshold.
        // Keep the trailing remainder in the buffer for the next
        // write (or for finish to upload as the final part).
        while self.buf.len() >= self.part_size {
            let part: Vec<u8> = self.buf.drain(..self.part_size).collect();
            // Re-buffer the part bytes into self.buf temporarily so
            // flush_part has a single code path. (Kept simple — at
            // typical part sizes the extra copy is in the noise.)
            let saved = std::mem::replace(&mut self.buf, part);
            self.flush_part().await?;
            // Restore whatever was after the boundary.
            self.buf = saved;
        }
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<UploadOutcome> {
        use sha2::Digest;
        if self.done {
            return Err(Error::Other("double finish".into()));
        }
        self.done = true;
        self.flush_part().await?;
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(std::mem::take(&mut self.parts)))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(UploadOutcome {
            bytes_written: self.bytes_written,
            sha256_hex: hex::encode(self.hasher.finalize()),
        })
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        if self.done {
            return Ok(());
        }
        self.done = true;
        // Best effort — S3 lifecycle rules are the real guarantee
        // that abandoned multiparts go away.
        let _ = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .send()
            .await;
        Ok(())
    }
}
