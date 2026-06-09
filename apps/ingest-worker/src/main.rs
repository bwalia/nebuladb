//! NebulaDB ingestion worker (design 0008 §8).
//!
//! An out-of-process sidecar that turns files into indexed documents:
//! detect type → extract text → derive metadata → pick a chunking
//! strategy → `POST /api/v1/bucket/<bucket>/document`. Heavy/native
//! parsers (PDF/DOCX) live here behind the `office` feature, never in
//! `nebula-server`, keeping the DB binary lean (design 0008 §3/§8).
//!
//! The server does the actual chunking + embedding: we pass the
//! detected `doc_type` and the server selects the matching strategy via
//! `nebula_chunk::select_strategy`. That keeps chunking next to the
//! embedder and means the worker carries no embedding dependency.
//!
//! # Usage
//!
//! ```text
//! nebula-ingest-worker --base-url http://localhost:8080 --bucket docs \
//!     ./handbook.md ./vpn_guide.html ./src/main.rs
//!
//! # With a bearer token and a recursive directory walk:
//! nebula-ingest-worker -u http://db:8080 -b kb --token $TOKEN ./corpus/
//! ```

mod detect;
mod metadata;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::json;

#[derive(Parser, Debug)]
#[command(name = "nebula-ingest-worker", about = "Ingest files into NebulaDB")]
struct Args {
    /// NebulaDB base URL, e.g. http://localhost:8080.
    #[arg(short = 'u', long, env = "NEBULA_URL")]
    base_url: String,

    /// Target bucket.
    #[arg(short, long, env = "NEBULA_BUCKET")]
    bucket: String,

    /// Optional bearer token.
    #[arg(long, env = "NEBULA_TOKEN")]
    token: Option<String>,

    /// Max keywords to extract per document.
    #[arg(long, default_value_t = 12)]
    max_keywords: usize,

    /// Skip local metadata generation (summary/keywords); send only the
    /// detected doc_type.
    #[arg(long, default_value_t = false)]
    no_metadata: bool,

    /// Files or directories to ingest. Directories are walked
    /// recursively.
    #[arg(required = true)]
    paths: Vec<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();
    let client = reqwest::Client::builder()
        .build()
        .context("build http client")?;

    // Expand directories into a flat file list.
    let mut files = Vec::new();
    for p in &args.paths {
        collect_files(p, &mut files)?;
    }
    if files.is_empty() {
        anyhow::bail!("no files to ingest");
    }

    let mut ok = 0usize;
    let mut failed = 0usize;
    for path in &files {
        match ingest_one(&client, &args, path).await {
            Ok(chunks) => {
                ok += 1;
                tracing::info!(file = %path.display(), chunks, "ingested");
            }
            Err(e) => {
                failed += 1;
                tracing::warn!(file = %path.display(), error = %e, "skipped");
            }
        }
    }

    tracing::info!(ok, failed, total = files.len(), "ingest complete");
    if ok == 0 {
        anyhow::bail!("all {} file(s) failed to ingest", files.len());
    }
    Ok(())
}

/// Ingest a single file. Returns the number of chunks the server created.
async fn ingest_one(client: &reqwest::Client, args: &Args, path: &Path) -> Result<usize> {
    let extracted = detect::extract(path)
        .await
        .with_context(|| format!("extract {}", path.display()))?;

    let mut meta = if args.no_metadata {
        json!({ "doc_type": extracted.doc_type, "source": file_name(path) })
    } else {
        let mut m = metadata::build(&extracted.text, extracted.doc_type, args.max_keywords);
        m["source"] = json!(file_name(path));
        m
    };
    // Always record the original filename for attribution.
    if meta.get("source").is_none() {
        meta["source"] = json!(file_name(path));
    }

    let doc_id = doc_id_for(path);
    let url = format!(
        "{}/api/v1/bucket/{}/document",
        args.base_url.trim_end_matches('/'),
        args.bucket
    );
    let body = json!({
        "doc_id": doc_id,
        "text": extracted.text,
        "metadata": meta,
        "doc_type": extracted.doc_type,
    });

    let mut req = client.post(&url).json(&body);
    if let Some(token) = &args.token {
        req = req.bearer_auth(token);
    }
    let resp = req.send().await.context("send document")?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {text}");
    }
    let parsed: serde_json::Value = resp.json().await.context("parse response")?;
    Ok(parsed["chunks"].as_u64().unwrap_or(0) as usize)
}

/// Stable doc id from the file's name (without extension). Documents
/// with the same stem in different dirs collide intentionally — re-run
/// to update. Operators wanting path-unique ids can pre-rename.
fn doc_id_for(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("document")
        .to_string()
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string()
}

/// Recursively collect regular files under `path`. A file path is added
/// directly; a directory is walked one level at a time (no symlink
/// following) to avoid pulling in a walkdir dependency.
fn collect_files(path: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let meta = std::fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?;
    if meta.is_file() {
        out.push(path.to_path_buf());
        return Ok(());
    }
    if meta.is_dir() {
        for entry in std::fs::read_dir(path).with_context(|| format!("read dir {}", path.display()))? {
            let entry = entry?;
            let p = entry.path();
            let m = entry.file_type()?;
            if m.is_dir() {
                collect_files(&p, out)?;
            } else if m.is_file() {
                out.push(p);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_id_strips_extension() {
        assert_eq!(doc_id_for(Path::new("/x/handbook.md")), "handbook");
        assert_eq!(doc_id_for(Path::new("notes")), "notes");
    }

    #[test]
    fn collect_files_walks_directory() {
        let dir = std::env::temp_dir().join(format!("ingest-walk-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("a.txt"), "x").unwrap();
        std::fs::write(dir.join("sub/b.md"), "y").unwrap();
        let mut out = Vec::new();
        collect_files(&dir, &mut out).unwrap();
        assert_eq!(out.len(), 2);
        std::fs::remove_dir_all(&dir).ok();
    }
}
