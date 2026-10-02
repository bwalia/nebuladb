//! Public-website (+ email) lookup for company enrichment.
//!
//! Companies House does **not** publish websites or contact emails. When
//! RAG needs them we research via:
//!
//! 1. **Firecrawl** (preferred) — LLM-native search + scrape API when
//!    `NEBULA_FIRECRAWL_API_KEY` / `FIRECRAWL_API_KEY` is set.
//! 2. **Google Custom Search JSON API** when CSE key + cx are set.
//! 3. **DuckDuckGo HTML** scrape + liveness probe as last resort.
//!
//! Agents can also attach Firecrawl's hosted MCP
//! (`https://mcp.firecrawl.dev/v2/mcp`) for ad-hoc crawl beyond ingest.

use std::collections::HashSet;
use std::time::Duration;

use reqwest::Client;
use serde_json::{json, Value};

const FIRECRAWL_BASE: &str = "https://api.firecrawl.dev/v2";

/// Result of a website research attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebsiteHit {
    pub url: String,
    /// How it was found: `firecrawl`, `google_cse`, `web_search`, `fixture`, …
    pub source: String,
    /// Public contact email scraped from the site, if any.
    pub email: Option<String>,
}

impl WebsiteHit {
    fn with_url(url: String, source: &str) -> Self {
        Self {
            url,
            source: source.into(),
            email: None,
        }
    }
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

fn firecrawl_api_key() -> Option<String> {
    std::env::var("NEBULA_FIRECRAWL_API_KEY")
        .or_else(|_| std::env::var("FIRECRAWL_API_KEY"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
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
        "firecrawl.",
    ]
    .iter()
    .any(|j| h.contains(j))
}

fn free_mail_host(host: &str) -> bool {
    let h = host.to_ascii_lowercase();
    [
        "gmail.",
        "yahoo.",
        "hotmail.",
        "outlook.",
        "icloud.",
        "aol.",
        "mail.com",
        "protonmail.",
        "googlemail.",
        "live.com",
        "msn.com",
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

fn origin_of(raw: &str) -> Option<String> {
    let u = normalize_url(raw)?;
    let host = host_of(&u)?;
    let scheme = if u.starts_with("http://") {
        "http"
    } else {
        "https"
    };
    Some(format!("{scheme}://{host}"))
}

/// Pull plausible business emails from page text / markdown.
fn extract_emails(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'@' && i > 0 {
            let mut start = i;
            while start > 0 {
                let c = bytes[start - 1] as char;
                if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '%' || c == '+' || c == '-'
                {
                    start -= 1;
                } else {
                    break;
                }
            }
            let mut end = i + 1;
            while end < bytes.len() {
                let c = bytes[end] as char;
                if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                    end += 1;
                } else {
                    break;
                }
            }
            if start < i && end > i + 1 {
                let email = String::from_utf8_lossy(&bytes[start..end])
                    .to_ascii_lowercase()
                    .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '@' && c != '.' && c != '_' && c != '%' && c != '+' && c != '-')
                    .to_string();
                if email.contains('@') && email.contains('.') {
                    let domain = email.split('@').nth(1).unwrap_or("");
                    if !domain.is_empty()
                        && !free_mail_host(domain)
                        && !email.contains("example.")
                        && !email.ends_with(".png")
                        && !email.ends_with(".jpg")
                        && seen.insert(email.clone())
                    {
                        out.push(email);
                    }
                }
            }
            i = end;
            continue;
        }
        i += 1;
    }
    out
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

async fn firecrawl_search(http: &Client, api_key: &str, query: &str) -> Vec<String> {
    let body = json!({
        "query": query,
        "limit": 5,
        "country": "GB",
        "sources": [{"type": "web"}],
    });
    let Ok(res) = http
        .post(format!("{FIRECRAWL_BASE}/search"))
        .bearer_auth(api_key)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
    else {
        tracing::warn!("firecrawl search request failed to send");
        return Vec::new();
    };
    let status = res.status();
    let Ok(text) = res.text().await else {
        return Vec::new();
    };
    if !status.is_success() {
        tracing::warn!(%status, body = %text.chars().take(240).collect::<String>(), "firecrawl search non-2xx");
        return Vec::new();
    }
    let Ok(json): Result<Value, _> = serde_json::from_str(&text) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    // v2: data.web[].url ; tolerate legacy data[] shape
    let items = json
        .pointer("/data/web")
        .and_then(|v| v.as_array())
        .or_else(|| json.get("data").and_then(|v| v.as_array()));
    if let Some(items) = items {
        for item in items {
            if let Some(link) = item.get("url").and_then(|v| v.as_str()) {
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

async fn firecrawl_scrape_markdown(http: &Client, api_key: &str, url: &str) -> Option<String> {
    let body = json!({
        "url": url,
        "formats": ["markdown"],
        "onlyMainContent": true,
    });
    let Ok(res) = http
        .post(format!("{FIRECRAWL_BASE}/scrape"))
        .bearer_auth(api_key)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
    else {
        return None;
    };
    if !res.status().is_success() {
        return None;
    }
    let Ok(json): Result<Value, _> = res.json().await else {
        return None;
    };
    json.pointer("/data/markdown")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

async fn enrich_email_from_site(
    http: &Client,
    api_key: &str,
    site_url: &str,
) -> Option<String> {
    let mut texts = Vec::new();
    if let Some(md) = firecrawl_scrape_markdown(http, api_key, site_url).await {
        // Prefer a contact page link if the homepage mentions one.
        let contact_hint = md
            .lines()
            .find(|l| {
                let lo = l.to_ascii_lowercase();
                lo.contains("contact") && (lo.contains("http") || lo.contains("]("))
            })
            .and_then(|l| {
                // markdown link ](url)
                if let Some(i) = l.find("](http") {
                    let rest = &l[i + 2..];
                    let end = rest.find(')').unwrap_or(rest.len());
                    normalize_url(&rest[..end])
                } else {
                    None
                }
            });
        texts.push(md);
        if let Some(contact) = contact_hint {
            if let Some(md2) = firecrawl_scrape_markdown(http, api_key, &contact).await {
                texts.push(md2);
            }
        } else if let Some(origin) = origin_of(site_url) {
            for path in ["/contact", "/contact-us", "/about/contact"] {
                let u = format!("{origin}{path}");
                if let Some(md2) = firecrawl_scrape_markdown(http, api_key, &u).await {
                    texts.push(md2);
                    break;
                }
            }
        }
    }
    let combined = texts.join("\n");
    extract_emails(&combined).into_iter().next()
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
            return Some(WebsiteHit::with_url(cand, source));
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

/// Research a likely public website (and contact email) for a UK company.
///
/// Prefer Firecrawl when `NEBULA_FIRECRAWL_API_KEY` is set; else Google CSE;
/// else DuckDuckGo HTML. Returns `None` when nothing live and non-junk is found.
pub async fn research_company_website(
    name: &str,
    locality: Option<&str>,
) -> Option<WebsiteHit> {
    let http = Client::builder()
        .timeout(Duration::from_secs(45))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .ok()?;

    let queries = research_queries(name, locality);
    let mut seen_hosts = HashSet::new();

    if let Some(api_key) = firecrawl_api_key() {
        for q in &queries {
            let cands = firecrawl_search(&http, &api_key, q).await;
            for cand in cands {
                let Some(host) = host_of(&cand) else {
                    continue;
                };
                if junk_host(&host) || !seen_hosts.insert(host.clone()) {
                    continue;
                }
                // Firecrawl search already found it; skip separate HEAD when
                // scrape succeeds. Still accept URL if scrape fails but live.
                let email = enrich_email_from_site(&http, &api_key, &cand).await;
                if email.is_some() || website_live(&http, &cand).await {
                    return Some(WebsiteHit {
                        url: cand,
                        source: "firecrawl".into(),
                        email,
                    });
                }
            }
        }
    }

    if let Some(cfg) = GoogleCseConfig::from_env() {
        for q in &queries {
            let cands = google_cse_candidates(&http, &cfg, q).await;
            if let Some(mut hit) =
                first_live_candidate(&http, cands, &mut seen_hosts, "google_cse").await
            {
                if let Some(api_key) = firecrawl_api_key() {
                    hit.email = enrich_email_from_site(&http, &api_key, &hit.url).await;
                }
                return Some(hit);
            }
        }
    }

    for q in &queries {
        let cands = ddg_candidates(&http, q).await;
        if let Some(mut hit) =
            first_live_candidate(&http, cands, &mut seen_hosts, "web_search").await
        {
            if let Some(api_key) = firecrawl_api_key() {
                hit.email = enrich_email_from_site(&http, &api_key, &hit.url).await;
            }
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

/// True when Firecrawl API key is present.
pub fn firecrawl_configured() -> bool {
    firecrawl_api_key().is_some()
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
    fn extract_business_email_skips_gmail() {
        let text = "Contact us at info@workstation.co.uk or hello@gmail.com thanks";
        let emails = extract_emails(text);
        assert_eq!(emails, vec!["info@workstation.co.uk".to_string()]);
    }

    #[test]
    fn firecrawl_from_env_does_not_panic() {
        let _ = firecrawl_api_key();
        let _ = GoogleCseConfig::from_env();
    }
}
