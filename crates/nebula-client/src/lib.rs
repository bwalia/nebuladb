//! Region-aware client for NebulaDB.
//!
//! The SDK sits in front of one or more per-region NebulaDB endpoints
//! and steers writes to each bucket's home region automatically. Reads
//! hit the local-preferred region; writes that land on the wrong
//! region get a 421 with the correct `home_region` in the body, which
//! this client catches and retries transparently.
//!
//! # Basic usage
//!
//! ```ignore
//! use nebula_client::{Client, RegionEndpoint};
//!
//! let client = Client::builder()
//!     .region("us-east-1", "http://nebula-us-east-1:8080")
//!     .region("us-west-2", "http://nebula-us-west-2:8080")
//!     .prefer("us-east-1")
//!     .build();
//!
//! let resp = client
//!     .upsert_doc("catalog", "sku-001", "authoritative item", None)
//!     .await?;
//! ```
//!
//! # Home-region caching
//!
//! Each write triggers a lookup in the cache: bucket → home_region.
//! On miss we call `GET /admin/bucket/:b/home-region` against the
//! preferred region. Hits have a 30-second TTL; a `wrong_home_region`
//! response from the server invalidates the cache entry and retries
//! with the server-supplied region (so stale caches converge within
//! one request, not one TTL).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use reqwest::StatusCode;

const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(30);
const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(5);
/// HTTP 421 — the code REST middleware uses for `wrong_home_region`.
const WRONG_HOME_REGION_STATUS: u16 = 421;

/// One region's base URL.
#[derive(Clone, Debug)]
pub struct RegionEndpoint {
    pub name: String,
    pub base_url: String,
}

/// Builder for [`Client`]. Chainable; `build()` produces a ready client.
#[derive(Default)]
pub struct ClientBuilder {
    regions: Vec<RegionEndpoint>,
    prefer: Option<String>,
    bearer: Option<String>,
    http_timeout: Option<Duration>,
    cache_ttl: Option<Duration>,
}

impl ClientBuilder {
    /// Register a region's base URL.
    pub fn region(mut self, name: impl Into<String>, base_url: impl Into<String>) -> Self {
        self.regions.push(RegionEndpoint {
            name: name.into(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
        });
        self
    }

    /// Preferred region for reads and for initial home-region lookups.
    /// Defaults to the first region registered.
    pub fn prefer(mut self, region: impl Into<String>) -> Self {
        self.prefer = Some(region.into());
        self
    }

    /// Bearer token used for all requests (matches NEBULA_API_KEYS).
    pub fn bearer(mut self, token: impl Into<String>) -> Self {
        self.bearer = Some(token.into());
        self
    }

    pub fn http_timeout(mut self, d: Duration) -> Self {
        self.http_timeout = Some(d);
        self
    }

    pub fn cache_ttl(mut self, d: Duration) -> Self {
        self.cache_ttl = Some(d);
        self
    }

    pub fn build(self) -> Client {
        if self.regions.is_empty() {
            // A client with no regions is functionally useless, but
            // we don't panic here — the first request will surface a
            // NoRegions error.
        }
        let prefer = self
            .prefer
            .or_else(|| self.regions.first().map(|r| r.name.clone()));
        let by_name: HashMap<_, _> = self
            .regions
            .iter()
            .map(|r| (r.name.clone(), r.base_url.clone()))
            .collect();
        Client {
            regions: Arc::new(self.regions),
            by_name: Arc::new(by_name),
            prefer,
            bearer: self.bearer,
            http: reqwest::Client::builder()
                .timeout(self.http_timeout.unwrap_or(DEFAULT_HTTP_TIMEOUT))
                .build()
                .expect("reqwest client build"),
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl: self.cache_ttl.unwrap_or(DEFAULT_CACHE_TTL),
        }
    }
}

/// Region-aware NebulaDB client.
#[derive(Clone)]
pub struct Client {
    /// Ordered list of regions the client knows about. Kept for
    /// `regions()` observability and for a future "closest region"
    /// read path.
    #[allow(dead_code)]
    regions: Arc<Vec<RegionEndpoint>>,
    by_name: Arc<HashMap<String, String>>,
    prefer: Option<String>,
    bearer: Option<String>,
    http: reqwest::Client,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    cache_ttl: Duration,
}

#[derive(Clone, Debug)]
struct CacheEntry {
    home_region: String,
    expires_at: Instant,
}

/// Mirror of the JSON returned by `GET /admin/bucket/:b/home-region`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HomeRegion {
    pub bucket: String,
    #[serde(default)]
    pub home_region: Option<String>,
    #[serde(default)]
    pub home_epoch: u64,
    #[serde(default)]
    pub replicated_to: Vec<String>,
    #[serde(default)]
    pub node_region: String,
    pub has_home: bool,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Preferred base URL — first region listed, unless overridden via
    /// `ClientBuilder::prefer`.
    fn preferred_base_url(&self) -> Result<&str, Error> {
        let name = self.prefer.as_deref().ok_or(Error::NoRegions)?;
        self.by_name
            .get(name)
            .map(|s| s.as_str())
            .ok_or_else(|| Error::UnknownRegion(name.to_string()))
    }

    fn base_url_for(&self, region: &str) -> Result<&str, Error> {
        self.by_name
            .get(region)
            .map(|s| s.as_str())
            .ok_or_else(|| Error::UnknownRegion(region.to_string()))
    }

    /// Read the home region for a bucket. Checks the in-memory cache
    /// first (30s TTL) and falls back to an admin call on the
    /// preferred region.
    pub async fn home_region(&self, bucket: &str) -> Result<HomeRegion, Error> {
        if let Some(home) = self.cache_lookup(bucket) {
            let mut hr = HomeRegion::placeholder(bucket);
            hr.home_region = Some(home);
            hr.has_home = true;
            return Ok(hr);
        }
        self.refresh_home_region(bucket).await
    }

    fn cache_lookup(&self, bucket: &str) -> Option<String> {
        let g = self.cache.read();
        let e = g.get(bucket)?;
        if e.expires_at > Instant::now() {
            Some(e.home_region.clone())
        } else {
            None
        }
    }

    fn cache_store(&self, bucket: &str, home: &str) {
        let mut g = self.cache.write();
        g.insert(
            bucket.to_string(),
            CacheEntry {
                home_region: home.to_string(),
                expires_at: Instant::now() + self.cache_ttl,
            },
        );
    }

    fn cache_invalidate(&self, bucket: &str) {
        let mut g = self.cache.write();
        g.remove(bucket);
    }

    /// Force a fresh lookup, bypassing (and replacing) the cache.
    pub async fn refresh_home_region(&self, bucket: &str) -> Result<HomeRegion, Error> {
        let base = self.preferred_base_url()?;
        let url = format!("{base}/api/v1/admin/bucket/{bucket}/home-region");
        let mut req = self.http.get(&url);
        if let Some(tok) = &self.bearer {
            req = req.bearer_auth(tok);
        }
        let resp = req.send().await.map_err(Error::http)?;
        if !resp.status().is_success() {
            return Err(Error::from_status(resp.status(), resp.text().await.ok()));
        }
        let hr: HomeRegion = resp.json().await.map_err(Error::http)?;
        if let Some(home) = hr.home_region.clone() {
            self.cache_store(bucket, &home);
        } else {
            self.cache_invalidate(bucket);
        }
        Ok(hr)
    }

    /// Resolve the base URL for a write to `bucket`. Returns the
    /// preferred region's URL when the bucket has no home configured.
    async fn base_url_for_write(&self, bucket: &str) -> Result<String, Error> {
        if let Some(home) = self.cache_lookup(bucket) {
            return Ok(self.base_url_for(&home)?.to_string());
        }
        // Miss — look it up.
        let hr = self.refresh_home_region(bucket).await?;
        match hr.home_region {
            Some(home) => Ok(self.base_url_for(&home)?.to_string()),
            None => Ok(self.preferred_base_url()?.to_string()),
        }
    }

    /// Upsert a single document. Transparent retry on 421
    /// `wrong_home_region` — the server tells us the correct home,
    /// we update the cache, and retry once. A second 421 surfaces as
    /// `Error::WrongHomeRegion` (stuck in a loop, client needs to
    /// investigate).
    pub async fn upsert_doc(
        &self,
        bucket: &str,
        id: &str,
        text: &str,
        metadata: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, Error> {
        let body = serde_json::json!({
            "id": id,
            "text": text,
            "metadata": metadata.unwrap_or(serde_json::Value::Null),
        });
        let base = self.base_url_for_write(bucket).await?;
        let url = format!("{base}/api/v1/bucket/{bucket}/doc");
        match self.post_json(&url, &body).await {
            Err(Error::WrongHomeRegion(new_home)) => {
                // Server knows better — update cache and retry once.
                tracing::info!(bucket, new_home = %new_home, "home-region drift, re-routing");
                self.cache_store(bucket, &new_home);
                let base = self.base_url_for(&new_home)?.to_string();
                let url = format!("{base}/api/v1/bucket/{bucket}/doc");
                self.post_json(&url, &body).await
            }
            other => other,
        }
    }

    /// Generic search via the configured preferred region (reads are
    /// region-local by default).
    pub async fn ai_search(
        &self,
        query: &str,
        top_k: u32,
        bucket: Option<&str>,
    ) -> Result<serde_json::Value, Error> {
        let base = self.preferred_base_url()?;
        let url = format!("{base}/api/v1/ai/search");
        let mut body = serde_json::json!({"query": query, "top_k": top_k});
        if let Some(b) = bucket {
            body["bucket"] = serde_json::json!(b);
        }
        self.post_json(&url, &body).await
    }

    /// POST helper that decodes the server's `wrong_home_region` body
    /// into a typed [`Error::WrongHomeRegion`] so callers can react
    /// without parsing the response themselves.
    async fn post_json(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let mut req = self.http.post(url).json(body);
        if let Some(tok) = &self.bearer {
            req = req.bearer_auth(tok);
        }
        let resp = req.send().await.map_err(Error::http)?;
        let status = resp.status();
        let text = resp.text().await.map_err(Error::http)?;

        if status.as_u16() == WRONG_HOME_REGION_STATUS {
            // Try to extract the new home from the body for a quick retry.
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(h) = v.get("home_region").and_then(|x| x.as_str()) {
                    return Err(Error::WrongHomeRegion(h.to_string()));
                }
            }
            return Err(Error::Status {
                code: status,
                body: text,
            });
        }

        if !status.is_success() {
            return Err(Error::Status {
                code: status,
                body: text,
            });
        }
        serde_json::from_str(&text).map_err(|e| Error::Decode(e.to_string()))
    }
}

impl HomeRegion {
    /// Minimal constructor used by the cache-hit path. The other
    /// fields (epoch, node_region, replicated_to) can't be answered
    /// without a server round trip; we default them rather than
    /// inventing values.
    fn placeholder(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            home_region: None,
            home_epoch: 0,
            replicated_to: Vec::new(),
            node_region: String::new(),
            has_home: false,
        }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("no regions configured")]
    NoRegions,
    #[error("unknown region: {0}")]
    UnknownRegion(String),
    #[error("wrong home region: try {0}")]
    WrongHomeRegion(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("server returned {code}: {body}")]
    Status { code: StatusCode, body: String },
}

impl Error {
    fn http(e: reqwest::Error) -> Self {
        Error::Http(e.to_string())
    }
    fn from_status(code: StatusCode, body: Option<String>) -> Self {
        Error::Status {
            code,
            body: body.unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults_prefer_to_first_region() {
        let c = Client::builder()
            .region("us-east-1", "http://a")
            .region("us-west-2", "http://b")
            .build();
        assert_eq!(c.prefer.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn builder_respects_explicit_prefer() {
        let c = Client::builder()
            .region("us-east-1", "http://a")
            .region("us-west-2", "http://b")
            .prefer("us-west-2")
            .build();
        assert_eq!(c.prefer.as_deref(), Some("us-west-2"));
    }

    #[test]
    fn cache_store_and_lookup_roundtrip() {
        let c = Client::builder()
            .region("us-east-1", "http://a")
            .cache_ttl(Duration::from_secs(60))
            .build();
        c.cache_store("catalog", "us-east-1");
        assert_eq!(c.cache_lookup("catalog").as_deref(), Some("us-east-1"));
    }

    #[test]
    fn cache_expires_after_ttl() {
        let c = Client::builder()
            .region("us-east-1", "http://a")
            .cache_ttl(Duration::from_millis(0))
            .build();
        c.cache_store("catalog", "us-east-1");
        // Instant::now() moves forward a tick — zero-duration expiry
        // is always in the past.
        std::thread::sleep(Duration::from_millis(1));
        assert!(c.cache_lookup("catalog").is_none());
    }

    #[test]
    fn cache_invalidate_drops_entry() {
        let c = Client::builder()
            .region("us-east-1", "http://a")
            .build();
        c.cache_store("catalog", "us-east-1");
        c.cache_invalidate("catalog");
        assert!(c.cache_lookup("catalog").is_none());
    }

    #[test]
    fn unknown_region_errors_cleanly() {
        let c = Client::builder().region("us-east-1", "http://a").build();
        assert!(matches!(
            c.base_url_for("atlantis"),
            Err(Error::UnknownRegion(_))
        ));
    }

    #[test]
    fn no_regions_errors_cleanly() {
        let c = Client::builder().build();
        assert!(matches!(c.preferred_base_url(), Err(Error::NoRegions)));
    }
}
