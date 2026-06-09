//! File-type detection and text extraction (design 0008 §8).
//!
//! Runs **out of process** from the DB so heavy/native parsers never
//! link into `nebula-server`. Detection is by extension (cheap, good
//! enough for an ingest pipeline where the operator controls inputs);
//! extraction is per-kind.
//!
//! PDF/DOCX extraction is behind the `office` cargo feature. Without it,
//! those inputs return [`ExtractError::Unsupported`] so a lean build
//! degrades gracefully rather than failing to compile.

use std::path::Path;

use nebula_chunk::DocType;

#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("unsupported file type: {0} (rebuild with --features office for PDF/DOCX)")]
    Unsupported(String),
    // Only constructed by the `office`-gated PDF/DOCX extractors.
    #[cfg_attr(not(feature = "office"), allow(dead_code))]
    #[error("extraction failed: {0}")]
    Failed(String),
    #[error("file produced no extractable text")]
    Empty,
}

/// What we extracted: the plain text plus the chunking strategy the
/// server should apply (design 0008 §8 — the worker picks, the server
/// chunks where the embedder lives).
#[derive(Debug, Clone)]
pub struct Extracted {
    pub text: String,
    pub doc_type: DocType,
}

/// Lowercased file extension, or "" when there's none.
fn ext(path: &Path) -> String {
    path.extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase()
}

/// Map an extension to the chunking strategy kind. Unknown extensions
/// default to plain text — the safe, always-applicable choice.
pub fn doc_type_for(path: &Path) -> DocType {
    match ext(path).as_str() {
        "md" | "markdown" => DocType::Markdown,
        "html" | "htm" => DocType::Html,
        "rs" | "py" | "js" | "ts" | "go" | "java" | "c" | "h" | "cpp" | "hpp" | "rb" | "php"
        | "cs" | "swift" | "kt" | "scala" | "sh" => DocType::Code,
        "csv" | "tsv" | "jsonl" | "ndjson" => DocType::Structured,
        _ => DocType::Text,
    }
}

/// Read and extract a file into plain text + a chunking strategy.
pub async fn extract(path: &Path) -> Result<Extracted, ExtractError> {
    let e = ext(path);
    let doc_type = doc_type_for(path);

    let text = match e.as_str() {
        "pdf" => extract_pdf(path).await?,
        "docx" => extract_docx(path).await?,
        "html" | "htm" => {
            let raw = tokio::fs::read_to_string(path).await?;
            strip_html(&raw)
        }
        "json" => {
            // Pretty-print so structure survives as newlines the
            // chunkers can use; falls back to raw text if not valid JSON.
            let raw = tokio::fs::read_to_string(path).await?;
            match serde_json::from_str::<serde_json::Value>(&raw) {
                Ok(v) => serde_json::to_string_pretty(&v).unwrap_or(raw),
                Err(_) => raw,
            }
        }
        // Everything else (txt, md, code, csv, unknown) is read as UTF-8.
        _ => tokio::fs::read_to_string(path).await?,
    };

    if text.trim().is_empty() {
        return Err(ExtractError::Empty);
    }
    Ok(Extracted { text, doc_type })
}

/// Strip HTML tags into readable text. Deliberately minimal — drops tags
/// and collapses whitespace runs. A production deployment swaps in a
/// real HTML-to-text crate; this keeps the lean build dependency-free
/// and is adequate for the common "article body" case.
pub fn strip_html(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    let mut in_script_or_style = false;
    let lower = html.to_lowercase();
    let mut i = 0;
    let bytes = html.as_bytes();
    while i < bytes.len() {
        // Skip <script>...</script> and <style>...</style> bodies.
        if !in_tag && lower[i..].starts_with("<script") {
            in_script_or_style = true;
        }
        if !in_tag && lower[i..].starts_with("<style") {
            in_script_or_style = true;
        }
        let c = bytes[i] as char;
        if c == '<' {
            in_tag = true;
        } else if c == '>' {
            in_tag = false;
            if in_script_or_style
                && (lower[..=i].ends_with("</script>") || lower[..=i].ends_with("</style>"))
            {
                in_script_or_style = false;
            }
            // A closed tag acts as a soft separator.
            out.push(' ');
        } else if !in_tag && !in_script_or_style {
            out.push(c);
        }
        i += 1;
    }
    // Collapse whitespace.
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(feature = "office")]
async fn extract_pdf(path: &Path) -> Result<String, ExtractError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        pdf_extract::extract_text(&path).map_err(|e| ExtractError::Failed(e.to_string()))
    })
    .await
    .map_err(|e| ExtractError::Failed(e.to_string()))?
}

#[cfg(not(feature = "office"))]
async fn extract_pdf(_path: &Path) -> Result<String, ExtractError> {
    Err(ExtractError::Unsupported("pdf".into()))
}

#[cfg(feature = "office")]
async fn extract_docx(path: &Path) -> Result<String, ExtractError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let bytes = std::fs::read(&path).map_err(ExtractError::Io)?;
        let docx = docx_rs::read_docx(&bytes).map_err(|e| ExtractError::Failed(e.to_string()))?;
        // docx-rs exposes the document tree; we flatten paragraph text.
        let mut text = String::new();
        for child in docx.document.children {
            if let docx_rs::DocumentChild::Paragraph(p) = child {
                for run in p.children {
                    if let docx_rs::ParagraphChild::Run(r) = run {
                        for rc in r.children {
                            if let docx_rs::RunChild::Text(t) = rc {
                                text.push_str(&t.text);
                            }
                        }
                    }
                }
                text.push('\n');
            }
        }
        Ok(text)
    })
    .await
    .map_err(|e| ExtractError::Failed(e.to_string()))?
}

#[cfg(not(feature = "office"))]
async fn extract_docx(_path: &Path) -> Result<String, ExtractError> {
    Err(ExtractError::Unsupported("docx".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn detects_doc_types_by_extension() {
        assert_eq!(doc_type_for(&PathBuf::from("a.md")), DocType::Markdown);
        assert_eq!(doc_type_for(&PathBuf::from("a.HTML")), DocType::Html);
        assert_eq!(doc_type_for(&PathBuf::from("main.rs")), DocType::Code);
        assert_eq!(doc_type_for(&PathBuf::from("data.csv")), DocType::Structured);
        assert_eq!(doc_type_for(&PathBuf::from("notes.txt")), DocType::Text);
        // Unknown extension → text.
        assert_eq!(doc_type_for(&PathBuf::from("x.unknownext")), DocType::Text);
        assert_eq!(doc_type_for(&PathBuf::from("noext")), DocType::Text);
    }

    #[test]
    fn strip_html_removes_tags_and_collapses_ws() {
        let html = "<html><body><h1>Title</h1>\n<p>Hello   world</p></body></html>";
        let out = strip_html(html);
        assert!(out.contains("Title"));
        assert!(out.contains("Hello world"));
        assert!(!out.contains('<'));
        assert!(!out.contains('>'));
    }

    #[test]
    fn strip_html_drops_script_and_style_bodies() {
        let html = "<style>.x{color:red}</style><p>keep</p><script>evil()</script>";
        let out = strip_html(html);
        assert!(out.contains("keep"));
        assert!(!out.contains("color"));
        assert!(!out.contains("evil"));
    }

    #[tokio::test]
    async fn extract_reads_plain_text() {
        let dir = std::env::temp_dir().join(format!("ingest-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("note.md");
        std::fs::write(&p, "# Title\nbody text").unwrap();
        let out = extract(&p).await.unwrap();
        assert_eq!(out.doc_type, DocType::Markdown);
        assert!(out.text.contains("body text"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn extract_empty_file_errors() {
        let dir = std::env::temp_dir().join(format!("ingest-empty-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("empty.txt");
        std::fs::write(&p, "   \n  ").unwrap();
        let err = extract(&p).await.unwrap_err();
        assert!(matches!(err, ExtractError::Empty));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(not(feature = "office"))]
    #[tokio::test]
    async fn pdf_unsupported_without_office_feature() {
        let dir = std::env::temp_dir().join(format!("ingest-pdf-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("doc.pdf");
        std::fs::write(&p, "%PDF-1.4 fake").unwrap();
        let err = extract(&p).await.unwrap_err();
        assert!(matches!(err, ExtractError::Unsupported(_)));
        std::fs::remove_dir_all(&dir).ok();
    }
}
