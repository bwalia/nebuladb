//! Public-website lookup for company enrichment.
//!
//! Companies House does **not** publish websites. When RAG needs one we
//! research a likely official site, preferencing **Google Custom Search
//! JSON API** when configured (`NEBULA_GOOGLE_CSE_API_KEY` +
//! `NEBULA_GOOGLE_CSE_ID`), then falling back to DuckDuckGo HTML search
//! + a cheap liveness probe.
//!
//! For richer firmographics (domain, LinkedIn, headcount) operators can
//! also plug an external MCP such as CompanyEnrich or Apollo alongside
//! NebulaDB's Companies House tools.

use std::collections::HashSet;
use std::time::Duration;

use reqwest::Client;
use serde_json::Value;

/// Result of a website research attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebsiteHit {
    pub url: String,
    /// How it was found: `google_cse`, `web_search`, `fixture`, …
    pub source: String,
}

/// Google Custom Search credentials from the environment (if both set).
#[derive(Debug, Clone)]
pub struct GoogleCseConfig {
    pub api_key: String,
    pub cx: String,
}

impl GoogleCseConfig {
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("NEBULA_GOOGLE_CSE_API_KEY")
            .or_else(|_| std::env::var("GOOGLE_CSE_API_KEY"))
            .or_else(|_| std::env::var("GOOGLE_API_KEY"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;
        let cx = std::env::var("NEBULA_GOOGLE_CSE_ID")
            .or_else(|_| std::env::var("GOOGLE_CSE_ID"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;
        Some(Self { api_key, cx })
    }
}

fn junk_host(host: &str) -> bool {
    let h = host.to_ascii_lowercase();
    [
        "wikipedia.",
        "linkedin.",
        "facebook.",
        "twitter.",
        "x.com",
        "instagram.",
        "youtube.",
        "companies-house",
        "company-information.service.gov",
        "endole.",
        "creditsafe.",
        "duedil.",
        "beauhurst.",
        "yell.com",
        "thomsonlocal",
        "gumtree",
        "amazon.",
        "ebay.",
        "google.",
        "bing.",
        "duckduckgo.",
    ]
    .iter()
    .any(|j| h.contains(j))
}

fn percent_encode_query(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push_str("%20"),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let h = |c: u8| -> Option<u8> {
                match c {
                    b'0'..=b'9' => Some(c - b'0'),
                    b'a'..=b'f' => Some(c - b'a' + 10),
                    b'A'..=b'F' => Some(c - b'A' + 10),
                    _ => None,
                }
            };
            if let (Some(hi), Some(lo)) = (h(bytes[i + 1]), h(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        if bytes[i] == b'+' {
            out.push(b' ');
        } else {
            out.push(bytes[i]);
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn normalize_url(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let with_scheme = if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_string()
    } else {
        format!("https://{raw}")
    };
    let no_frag = with_scheme.split('#').next().unwrap_or(&with_scheme);
    Some(no_frag.trim_end_matches('/').to_string())
}

fn host_of(raw: &str) -> Option<String> {
    let u = normalize_url(raw)?;
    let rest = u
        .strip_prefix("https://")
        .or_else(|| u.strip_prefix("http://"))?;
    let host = rest.split('/').next()?.split(':').next()?;
    if host.is_empty() {
        None
    } else {
        Some(host.to_ascii_lowercase())
    }
}

async fn website_live(http: &Client, raw: &str) -> bool {
    let Some(u) = normalize_url(raw) else {
        return false;
    };
    for method in [reqwest::Method::HEAD, reqwest::Method::GET] {
        let Ok(req) = http
            .request(method, &u)
            .header("user-agent", "nebuladb-ch-enrich/1.0")
            .build()
        else {
            continue;
        };
        match http.execute(req).await {
            Ok(res) if (1..500).contains(&res.status().as_u16()) => return true,
            _ => continue,
        }
    }
    false
}

fn extract_hrefs(html: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut rest = html;
    while let Some(idx) = rest.to_ascii_lowercase().find("href=\"http") {
        let slice = &rest[idx + 6..];
        let Some(end) = slice.find('"') else {
            break;
        };
        let mut raw = slice[..end].to_string();
        if let Some(pos) = raw.find("uddg=") {
            let enc = &raw[pos + 5..];
            let enc = enc.split('&').next().unwrap_or(enc);
            raw = percent_decode(enc);
        }
        if let Some(n) = normalize_url(&raw) {
            if seen.insert(n.clone()) {
                out.push(n);
            }
        }
        rest = &slice[end + 1..];
        if out.len() >= 12 {
            break;
        }
    }
    out
}

async fn ddg_candidates(http: &Client, query: &str) -> Vec<String> {
    let u = format!(
        "https://html.duckduckgo.com/html/?q={}",
        percent_encode_query(query)
    );
    let Ok(res) = http
        .get(&u)
        .header("user-agent", "nebuladb-ch-enrich/1.0")
        .send()
        .await
    else {
        return Vec::new();
    };
    let Ok(html) = res.text().await else {
        return Vec::new();
    };
    extract_hrefs(&html)
}

/// Google Custom Search JSON API → result link URLs.
/// https://developers.google.com/custom-search/v1/overview
async fn google_cse_candidates(http: &Client, cfg: &GoogleCseConfig, query: &str) -> Vec<String> {
    let u = format!(
        "https://www.googleapis.com/customsearch/v1?key={}&cx={}&q={}&num=10",
        percent_encode_query(&cfg.api_key),
        percent_encode_query(&cfg.cx),
        percent_encode_query(query)
    );
    let Ok(res) = http
        .get(&u)
        .header("user-agent", "nebuladb-ch-enrich/1.0")
        .send()
        .await
    else {
        tracing::warn!("google CSE request failed to send");
        return Vec::new();
    };
    let status = res.status();
    let Ok(body) = res.text().await else {
        return Vec::new();
    };
    if !status.is_success() {
        tracing::warn!(%status, body = %body.chars().take(200).collect::<String>(), "google CSE non-2xx");
        return Vec::new();
    }
    let Ok(json): Result<Value, _> = serde_json::from_str(&body) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    if let Some(items) = json.get("items").and_then(|v| v.as_array()) {
        for item in items {
            if let Some(link) = item.get("link").and_then(|v| v.as_str()) {
                if let Some(n) = normalize_url(link) {
                    if seen.insert(n.clone()) {
                        out.push(n);
                    }
                }
            }
        }
    }
    out
}

async fn first_live_candidate(
    http: &Client,
    candidates: Vec<String>,
    seen_hosts: &mut HashSet<String>,
    source: &str,
) -> Option<WebsiteHit> {
    for cand in candidates {
        let Some(host) = host_of(&cand) else {
            continue;
        };
        if junk_host(&host) || !seen_hosts.insert(host) {
            continue;
        }
        if website_live(http, &cand).await {
            return Some(WebsiteHit {
                url: cand,
                source: source.into(),
            });
        }
    }
    None
}

fn research_queries(name: &str, locality: Option<&str>) -> [String; 3] {
    let loc = locality.unwrap_or("UK");
    [
        format!("\"{name}\" {loc} official website"),
        format!("\"{name}\" {loc} website"),
        format!("{name} UK company website"),
    ]
}

/// Research a likely public website for a UK company.
///
/// Prefer Google Custom Search when `NEBULA_GOOGLE_CSE_API_KEY` +
/// `NEBULA_GOOGLE_CSE_ID` are set; otherwise (or if Google returns nothing
/// usable) fall back to DuckDuckGo HTML. Returns `None` when nothing live
/// and non-junk is found.
pub async fn research_company_website(
    name: &str,
    locality: Option<&str>,
) -> Option<WebsiteHit> {
    let http = Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .ok()?;

    let queries = research_queries(name, locality);
    let mut seen_hosts = HashSet::new();

    if let Some(cfg) = GoogleCseConfig::from_env() {
        for q in &queries {
            let cands = google_cse_candidates(&http, &cfg, q).await;
            if let Some(hit) =
                first_live_candidate(&http, cands, &mut seen_hosts, "google_cse").await
            {
                return Some(hit);
            }
        }
    }

    for q in &queries {
        let cands = ddg_candidates(&http, q).await;
        if let Some(hit) = first_live_candidate(&http, cands, &mut seen_hosts, "web_search").await {
            return Some(hit);
        }
    }
    None
}

/// Whether website enrichment is enabled (default on). Set
/// `NEBULA_CH_ENRICH_WEBSITE=0|false|off` to skip the web lookup on ingest.
pub fn enrich_website_enabled() -> bool {
    match std::env::var("NEBULA_CH_ENRICH_WEBSITE") {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            !(v == "0" || v == "false" || v == "off" || v == "no")
        }
        Err(_) => true,
    }
}

/// True when Google Custom Search credentials are present.
pub fn google_cse_configured() -> bool {
    GoogleCseConfig::from_env().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn junk_filters_directory_sites() {
        assert!(junk_host("www.endole.co.uk"));
        assert!(junk_host(
            "find-and-update.company-information.service.gov.uk"
        ));
        assert!(!junk_host("workstation.co.uk"));
    }

    #[test]
    fn normalize_adds_https() {
        assert_eq!(
            normalize_url("workstation.co.uk").as_deref(),
            Some("https://workstation.co.uk")
        );
    }

    #[test]
    fn extract_uddg_links() {
        let html = r#"href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fworkstation.co.uk%2Fabout&amp;rut=x""#;
        let links = extract_hrefs(html);
        assert!(links.iter().any(|u| u.contains("workstation.co.uk")));
    }

    #[test]
    fn google_cse_from_env_needs_both() {
        // Ensure parsing doesn't panic when unset in the unit-test process.
        // Presence is env-dependent; we only assert the type round-trip shape.
        let _ = GoogleCseConfig::from_env();
    }
}
