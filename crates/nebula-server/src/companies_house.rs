//! UK Companies House API proxy for the showcase RAG demo.
//!
//! Auth stays on the server: `COMPANIES_HOUSE_API_KEY` or
//! `NEBULA_COMPANIES_HOUSE_API_KEY`. The frontend never sees the key.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::ApiError;
use crate::state::AppState;
use crate::website_enrich::{self, WebsiteHit};

const CH_BASE: &str = "https://api.company-information.service.gov.uk";

/// Shared HTTP client + optional API key for Companies House.
#[derive(Clone)]
pub struct CompaniesHouseClient {
    http: reqwest::Client,
    api_key: Option<String>,
}

impl CompaniesHouseClient {
    pub fn from_env() -> Self {
        let api_key = std::env::var("NEBULA_COMPANIES_HOUSE_API_KEY")
            .or_else(|_| std::env::var("COMPANIES_HOUSE_API_KEY"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        Self {
            http: reqwest::Client::new(),
            api_key,
        }
    }

    pub fn configured(&self) -> bool {
        self.api_key.is_some()
    }

    async fn get(&self, path: &str, query: &[(&str, &str)]) -> Result<Value, ApiError> {
        let key = self.api_key.as_ref().ok_or_else(|| {
            ApiError::BadRequest(
                "Companies House API key not configured. Set NEBULA_COMPANIES_HOUSE_API_KEY (or COMPANIES_HOUSE_API_KEY) on the server."
                    .into(),
            )
        })?;
        let url = format!("{CH_BASE}{path}");
        let resp = self
            .http
            .get(&url)
            .query(query)
            .basic_auth(key, Some(""))
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| ApiError::BadRequest(format!("companies house request failed: {e}")))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.as_u16() == 404 {
            return Err(ApiError::NotFound(format!("companies house: {path}")));
        }
        if !status.is_success() {
            return Err(ApiError::BadRequest(format!(
                "companies house HTTP {status}: {body}"
            )));
        }
        serde_json::from_str(&body)
            .map_err(|e| ApiError::BadRequest(format!("companies house decode error: {e}: {body}")))
    }
}

#[derive(Deserialize)]
pub struct SearchQuery {
    pub q: String,
    #[serde(default = "default_items")]
    pub items_per_page: u32,
    #[serde(default)]
    pub start_index: u32,
    /// When true and no API key, return a small fixture payload.
    #[serde(default)]
    pub fixture: bool,
}

fn default_items() -> u32 {
    20
}

fn fixture_search(q: &str) -> Value {
    json!({
        "fixture": true,
        "message": "No Companies House API key configured — showing fixture results. Set NEBULA_COMPANIES_HOUSE_API_KEY on the server for live data.",
        "items": [
            {
                "company_number": "00000001",
                "title": format!("{q} EXAMPLE LIMITED (fixture)"),
                "company_status": "active",
                "company_type": "ltd",
                "date_of_creation": "1900-01-01",
                "address_snippet": "1 Example Street, London, EC1A 1BB"
            },
            {
                "company_number": "00000002",
                "title": format!("{q} HOLDINGS PLC (fixture)"),
                "company_status": "active",
                "company_type": "plc",
                "date_of_creation": "1950-06-15",
                "address_snippet": "2 Demo Road, Manchester, M1 1AA"
            }
        ],
        "total_results": 2
    })
}

fn fixture_company(number: &str) -> Value {
    json!({
        "fixture": true,
        "company_number": number,
        "company_name": "EXAMPLE LIMITED (fixture)",
        "company_status": "active",
        "type": "ltd",
        "date_of_creation": "1900-01-01",
        "registered_office_address": {
            "address_line_1": "1 Example Street",
            "locality": "London",
            "postal_code": "EC1A 1BB",
            "country": "England"
        },
        "sic_codes": ["62012"],
        "officers": {"items": [{"name": "DOE, Jane", "officer_role": "director"}]},
        "persons_with_significant_control": {"items": [{"name": "DOE, Jane", "natures_of_control": ["ownership-of-shares-75-to-100-percent"]}]},
        "filing_history": {"items": [{"transaction_id": "fix-1", "description": "accounts-with-accounts-type-full", "date": "2024-01-01"}]}
    })
}

/// Flatten CH JSON into plain-text chunks suitable for RAG upsert / context.
pub fn company_to_rag_docs(company: &Value) -> Vec<(String, String, Value)> {
    let number = company
        .get("company_number")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let name = company
        .get("company_name")
        .or_else(|| company.get("title"))
        .and_then(|v| v.as_str())
        .unwrap_or(number);

    let mut docs = Vec::new();

    let profile = format!(
        "UK company {name} (number {number}). Status: {}. Type: {}. Incorporated: {}. \
         Registered office: {}. SIC codes: {}.",
        company
            .get("company_status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown"),
        company
            .get("type")
            .or_else(|| company.get("company_type"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown"),
        company
            .get("date_of_creation")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown"),
        company
            .get("registered_office_address")
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into()),
        company
            .get("sic_codes")
            .map(|s| s.to_string())
            .unwrap_or_else(|| "[]".into()),
    );
    docs.push((
        format!("{number}-profile"),
        profile,
        json!({"source": "companies_house", "kind": "profile", "company_number": number}),
    ));

    if let Some(items) = company
        .pointer("/officers/items")
        .and_then(|v| v.as_array())
    {
        let mut text = format!("Officers of {name} ({number}):\n");
        for o in items.iter().take(25) {
            text.push_str(&format!(
                "- {} ({})\n",
                o.get("name").and_then(|v| v.as_str()).unwrap_or("?"),
                o.get("officer_role")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?")
            ));
        }
        docs.push((
            format!("{number}-officers"),
            text,
            json!({"source": "companies_house", "kind": "officers", "company_number": number}),
        ));
    }

    if let Some(items) = company
        .pointer("/persons_with_significant_control/items")
        .and_then(|v| v.as_array())
    {
        let mut text = format!("Persons with significant control for {name} ({number}):\n");
        for p in items.iter().take(25) {
            text.push_str(&format!(
                "- {} controls={}\n",
                p.get("name").and_then(|v| v.as_str()).unwrap_or("?"),
                p.get("natures_of_control")
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "[]".into())
            ));
        }
        docs.push((
            format!("{number}-psc"),
            text,
            json!({"source": "companies_house", "kind": "psc", "company_number": number}),
        ));
    }

    if let Some(items) = company
        .pointer("/filing_history/items")
        .and_then(|v| v.as_array())
    {
        let mut text = format!("Recent filings for {name} ({number}):\n");
        for f in items.iter().take(15) {
            text.push_str(&format!(
                "- {} on {}: {}\n",
                f.get("transaction_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?"),
                f.get("date").and_then(|v| v.as_str()).unwrap_or("?"),
                f.get("description").and_then(|v| v.as_str()).unwrap_or("?")
            ));
        }
        docs.push((
            format!("{number}-filings"),
            text,
            json!({"source": "companies_house", "kind": "filings", "company_number": number}),
        ));
    }

    docs
}

/// Append secondary-source docs (website / email enrichment + source catalogue)
/// the LLM should consider before answering. Companies House does not
/// publish websites or emails — researched separately when provided.
pub fn append_enrichment_docs(
    company: &Value,
    website: Option<&WebsiteHit>,
) -> Vec<(String, String, Value)> {
    let number = company
        .get("company_number")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let name = company
        .get("company_name")
        .or_else(|| company.get("title"))
        .and_then(|v| v.as_str())
        .unwrap_or(number);

    let mut docs = Vec::new();

    let website_line = match website {
        Some(w) => format!(
            "Public website for {name} ({number}): {}. Found via {} (not from Companies House — \
             the UK register does not publish company websites).",
            w.url, w.source
        ),
        None => format!(
            "Website for {name} ({number}): not available on the Companies House register, and no \
             live official site was confirmed via web enrichment (Firecrawl / search). Do not invent \
             a URL; say it is unknown."
        ),
    };
    docs.push((
        format!("{number}-website"),
        website_line,
        json!({
            "source": website.map(|w| w.source.as_str()).unwrap_or("web_enrichment"),
            "kind": "website",
            "company_number": number,
            "url": website.map(|w| w.url.as_str()),
            "found": website.is_some(),
        }),
    ));

    if let Some(w) = website {
        if let Some(email) = w.email.as_deref() {
            docs.push((
                format!("{number}-contact"),
                format!(
                    "Public contact email for {name} ({number}): {email}. Scraped from {url} via \
                     {} (not from Companies House — the UK register does not publish emails).",
                    w.source,
                    url = w.url
                ),
                json!({
                    "source": w.source,
                    "kind": "email",
                    "company_number": number,
                    "email": email,
                    "url": w.url,
                    "found": true,
                }),
            ));
        }
    }

    let mut catalogue = format!(
        "Sources the assistant should use for {name} ({number}), in preference order:\n\
         1. companies_house / profile — status, type, incorporation date, registered office, SIC codes.\n\
         2. companies_house / officers — directors, secretaries, and other officers.\n\
         3. companies_house / psc — persons with significant control.\n\
         4. companies_house / filings — recent filing history descriptions.\n"
    );
    let mut n = 5u8;
    if let Some(w) = website {
        catalogue.push_str(&format!(
            "{n}. {src} / website — public site {url}. Use for website questions; NOT on Companies House.\n",
            src = w.source,
            url = w.url
        ));
        n += 1;
        if let Some(email) = w.email.as_deref() {
            catalogue.push_str(&format!(
                "{n}. {src} / email — contact {email} (from {url}). NOT on Companies House.\n",
                src = w.source,
                url = w.url
            ));
            n += 1;
        }
    } else {
        catalogue.push_str(
            "5. web_enrichment / website — not found. Companies House has no website field; say \
             unknown rather than guessing.\n",
        );
        n = 6;
    }
    let _ = n;
    catalogue.push_str(
        "Only answer from these sources. If a fact is missing, say so and name which source was checked.",
    );
    docs.push((
        format!("{number}-sources"),
        catalogue,
        json!({
            "source": "nebula",
            "kind": "sources_catalogue",
            "company_number": number,
        }),
    ));

    docs
}

fn company_locality(company: &Value) -> Option<&str> {
    company
        .pointer("/registered_office_address/locality")
        .and_then(|v| v.as_str())
        .or_else(|| company.get("address_snippet").and_then(|v| v.as_str()))
}

pub async fn ch_status(State(s): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "configured": s.companies_house.configured(),
        "fixture_available": true,
        "enrich_website": website_enrich::enrich_website_enabled(),
        "firecrawl": website_enrich::firecrawl_configured(),
        "google_cse": website_enrich::google_cse_configured(),
        "hint": "Set NEBULA_COMPANIES_HOUSE_API_KEY on nebula-server. For website+email enrichment set NEBULA_FIRECRAWL_API_KEY (https://www.firecrawl.dev/). Optional: NEBULA_GOOGLE_CSE_API_KEY + NEBULA_GOOGLE_CSE_ID."
    }))
}

pub async fn ch_search(
    State(s): State<AppState>,
    Query(q): Query<SearchQuery>,
) -> Result<impl IntoResponse, ApiError> {
    if q.q.trim().is_empty() {
        return Err(ApiError::BadRequest("q must be non-empty".into()));
    }
    if !s.companies_house.configured() {
        return Ok(Json(fixture_search(&q.q)));
    }
    let items = q.items_per_page.to_string();
    let start = q.start_index.to_string();
    let body = s
        .companies_house
        .get(
            "/search/companies",
            &[
                ("q", q.q.as_str()),
                ("items_per_page", items.as_str()),
                ("start_index", start.as_str()),
            ],
        )
        .await?;
    Ok(Json(body))
}

pub async fn ch_company(
    State(s): State<AppState>,
    Path(number): Path<String>,
    Query(opts): Query<CompanyOpts>,
) -> Result<impl IntoResponse, ApiError> {
    if !s.companies_house.configured() {
        return Ok(Json(fixture_company(&number)));
    }
    let mut profile = s
        .companies_house
        .get(&format!("/company/{number}"), &[])
        .await?;

    if opts.include_extras {
        let officers = s
            .companies_house
            .get(
                &format!("/company/{number}/officers"),
                &[("items_per_page", "35")],
            )
            .await
            .unwrap_or(json!({"items": []}));
        let psc = s
            .companies_house
            .get(
                &format!("/company/{number}/persons-with-significant-control"),
                &[("items_per_page", "35")],
            )
            .await
            .unwrap_or(json!({"items": []}));
        let filings = s
            .companies_house
            .get(
                &format!("/company/{number}/filing-history"),
                &[("items_per_page", "25")],
            )
            .await
            .unwrap_or(json!({"items": []}));
        if let Some(obj) = profile.as_object_mut() {
            obj.insert("officers".into(), officers);
            obj.insert("persons_with_significant_control".into(), psc);
            obj.insert("filing_history".into(), filings);
        }
    }
    Ok(Json(profile))
}

#[derive(Deserialize)]
pub struct CompanyOpts {
    #[serde(default = "default_true")]
    pub include_extras: bool,
    #[serde(default)]
    pub fixture: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize)]
pub struct IngestBody {
    /// When true, also upsert flattened docs into the given bucket for RAG.
    #[serde(default = "default_true")]
    pub upsert: bool,
    #[serde(default = "default_bucket")]
    pub bucket: String,
    /// Research a public website (CH does not publish one) and upsert it
    /// plus a sources catalogue for the LLM. Default true; disable with
    /// `false` or `NEBULA_CH_ENRICH_WEBSITE=0`.
    #[serde(default = "default_true")]
    pub enrich_website: bool,
}

fn default_bucket() -> String {
    "companies_house".into()
}

/// Fetch full company context and optionally upsert into NebulaDB for RAG.
pub async fn ch_ingest_for_rag(
    State(s): State<AppState>,
    Path(number): Path<String>,
    Json(body): Json<IngestBody>,
) -> Result<impl IntoResponse, ApiError> {
    let company = if s.companies_house.configured() {
        // Reuse ch_company logic
        let mut profile = s
            .companies_house
            .get(&format!("/company/{number}"), &[])
            .await?;
        let officers = s
            .companies_house
            .get(
                &format!("/company/{number}/officers"),
                &[("items_per_page", "35")],
            )
            .await
            .unwrap_or(json!({"items": []}));
        let psc = s
            .companies_house
            .get(
                &format!("/company/{number}/persons-with-significant-control"),
                &[("items_per_page", "35")],
            )
            .await
            .unwrap_or(json!({"items": []}));
        let filings = s
            .companies_house
            .get(
                &format!("/company/{number}/filing-history"),
                &[("items_per_page", "25")],
            )
            .await
            .unwrap_or(json!({"items": []}));
        if let Some(obj) = profile.as_object_mut() {
            obj.insert("officers".into(), officers);
            obj.insert("persons_with_significant_control".into(), psc);
            obj.insert("filing_history".into(), filings);
        }
        profile
    } else {
        fixture_company(&number)
    };

    let mut docs = company_to_rag_docs(&company);

    let do_enrich = body.enrich_website && website_enrich::enrich_website_enabled();
    let website = if do_enrich {
        let name = company
            .get("company_name")
            .or_else(|| company.get("title"))
            .and_then(|v| v.as_str())
            .unwrap_or(number.as_str());
        // Fixture shortcut for the showcase demo company when offline.
        if company
            .get("fixture")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
            && number == "11641870"
        {
            Some(WebsiteHit {
                url: "https://workstation.co.uk".into(),
                source: "fixture".into(),
                email: Some("info@workstation.co.uk".into()),
            })
        } else {
            website_enrich::research_company_website(name, company_locality(&company)).await
        }
    } else {
        None
    };
    docs.extend(append_enrichment_docs(&company, website.as_ref()));

    let mut upserted = Vec::new();
    if body.upsert {
        for (id, text, meta) in &docs {
            Arc::clone(&s.index)
                .upsert_text(&body.bucket, id, text, meta.clone())
                .await
                .map_err(ApiError::Index)?;
            upserted.push(id.clone());
        }
    }

    let sources: Vec<Value> = docs
        .iter()
        .map(|(id, _text, meta)| {
            json!({
                "id": id,
                "source": meta.get("source"),
                "kind": meta.get("kind"),
                "url": meta.get("url"),
                "email": meta.get("email"),
                "found": meta.get("found"),
            })
        })
        .collect();

    Ok(Json(json!({
        "company_number": number,
        "bucket": body.bucket,
        "docs": docs.iter().map(|(id, text, meta)| json!({
            "id": id,
            "text": text,
            "metadata": meta,
        })).collect::<Vec<_>>(),
        "upserted": upserted,
        "sources": sources,
        "website": website.as_ref().map(|w| json!({
            "url": w.url,
            "source": w.source,
            "email": w.email,
        })),
        "enrich_website": do_enrich,
        "fixture": company.get("fixture").and_then(|v| v.as_bool()).unwrap_or(false),
        "company": company,
    })))
}

#[derive(Deserialize)]
pub struct SyncAddressBody {
    /// Bucket holding the bulk-loaded register (`{number}-register` docs).
    #[serde(default = "default_bucket")]
    pub bucket: String,
}

/// The live registered office as one line, in the bulk loader's field
/// order (scripts/load_companies_house.py) so the two read alike.
fn live_address_line(company: &Value) -> Option<String> {
    let a = company.get("registered_office_address")?;
    let f = |k: &str| {
        a.get(k)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
    };
    let line1 = match (f("premises"), f("address_line_1")) {
        (Some(p), Some(l)) => Some(format!("{p} {l}")),
        (p, l) => p.or(l).map(str::to_string),
    };
    let parts: Vec<String> = [
        f("care_of").map(str::to_string),
        f("po_box").map(str::to_string),
        line1,
        f("address_line_2").map(str::to_string),
        f("locality").map(str::to_string),
        f("region").map(str::to_string),
        f("country").map(str::to_string),
        f("postal_code").map(str::to_string),
    ]
    .into_iter()
    .flatten()
    .collect();
    (!parts.is_empty()).then(|| parts.join(", "))
}

/// Upper-case letters and digits only, so "12 High St." and "12 HIGH ST" compare equal.
fn compact(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

/// The "Registered office: …." sentence of a register doc, as (start, end) byte offsets.
fn office_span(text: &str) -> Option<(usize, usize)> {
    const LABEL: &str = "Registered office: ";
    let start = text.find(LABEL)?;
    let rest = &text[start..];
    let end = [
        " Nature of business",
        " Accounts category",
        " Previous names",
    ]
    .iter()
    .filter_map(|m| rest.find(m))
    .min()
    .map(|i| start + i)
    .unwrap_or(text.len());
    Some((start, end))
}

/// Has the registered office moved? The bulk CSV and the live API format
/// addresses differently (case, premises split, country names), so this
/// compares the postcode plus the first address line rather than the
/// whole string, which would flag almost every company as changed.
fn address_changed(local_text: &str, local_postcode: &str, company: &Value) -> bool {
    let a = match company.get("registered_office_address") {
        Some(a) => a,
        None => return false,
    };
    let s = |k: &str| a.get(k).and_then(|v| v.as_str()).unwrap_or("");
    if compact(s("postal_code")) != compact(local_postcode) {
        return true;
    }
    let first_line = compact(&format!("{}{}", s("premises"), s("address_line_1")));
    let local_office = office_span(local_text)
        .map(|(b, e)| compact(&local_text[b..e]))
        .unwrap_or_default();
    !first_line.is_empty() && !local_office.contains(&first_line)
}

/// Swap the registered-office sentence for `addr`, or append one.
fn rewrite_office(text: &str, addr: &str) -> String {
    let sentence = format!("Registered office: {addr}.");
    match office_span(text) {
        Some((b, e)) => format!("{}{}{}", &text[..b], sentence, &text[e..]),
        None => format!("{} {sentence}", text.trim_end()),
    }
}

/// Compare a company's live registered office with the bulk-loaded copy
/// in the local register bucket, and rewrite the local doc when it moved.
/// The bulk CSV is a monthly snapshot, so addresses drift between loads.
pub async fn ch_sync_address(
    State(s): State<AppState>,
    Path(number): Path<String>,
    Json(body): Json<SyncAddressBody>,
) -> Result<impl IntoResponse, ApiError> {
    let company = if s.companies_house.configured() {
        s.companies_house
            .get(&format!("/company/{number}"), &[])
            .await?
    } else {
        fixture_company(&number)
    };
    let live = live_address_line(&company);
    let doc_id = format!("{number}-register");
    let Some(doc) = s.index.get(&body.bucket, &doc_id) else {
        return Ok(Json(json!({
            "status": "not_local",
            "bucket": body.bucket,
            "doc_id": doc_id,
            "live": live,
        })));
    };
    let local_postcode = doc
        .metadata
        .get("postcode")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let local = office_span(&doc.text).map(|(b, e)| {
        doc.text[b..e]
            .trim_start_matches("Registered office: ")
            .trim_end_matches('.')
            .to_string()
    });
    let Some(live_line) = live.clone() else {
        return Ok(Json(
            json!({"status": "unchanged", "doc_id": doc_id, "local": local, "live": live}),
        ));
    };
    if !address_changed(&doc.text, &local_postcode, &company) {
        return Ok(Json(
            json!({"status": "unchanged", "doc_id": doc_id, "local": local, "live": live}),
        ));
    }

    let addr = &company["registered_office_address"];
    let mut meta = doc.metadata.clone();
    if let Some(m) = meta.as_object_mut() {
        m.insert("previous_postcode".into(), json!(local_postcode));
        m.insert(
            "postcode".into(),
            addr.get("postal_code").cloned().unwrap_or(json!("")),
        );
        m.insert(
            "post_town".into(),
            addr.get("locality").cloned().unwrap_or(json!("")),
        );
        m.insert("address_source".into(), json!("companies_house_api"));
        m.insert("address_synced_at_ms".into(), json!(crate::audit::now_ms()));
    }
    let text = rewrite_office(&doc.text, &live_line);
    Arc::clone(&s.index)
        .upsert_text(&body.bucket, &doc_id, &text, meta)
        .await
        .map_err(ApiError::Index)?;
    Ok(Json(json!({
        "status": "updated",
        "bucket": body.bucket,
        "doc_id": doc_id,
        "local": local,
        "live": live,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCAL: &str = "ACME LTD (company number 01234567) is a Private Limited Company registered with UK Companies House. \
        Status: Active. Registered office: 12 HIGH STREET, LEEDS, ENGLAND, LS1 1AA. Nature of business (SIC): 62012.";

    fn live(premises: &str, line1: &str, postcode: &str) -> Value {
        json!({"registered_office_address": {
            "premises": premises, "address_line_1": line1, "locality": "Leeds",
            "country": "England", "postal_code": postcode
        }})
    }

    #[test]
    fn same_address_in_different_format_is_unchanged() {
        assert!(!address_changed(
            LOCAL,
            "LS1 1AA",
            &live("12", "High Street", "ls11aa")
        ));
    }

    #[test]
    fn new_postcode_or_street_is_a_change() {
        assert!(address_changed(
            LOCAL,
            "LS1 1AA",
            &live("12", "High Street", "LS2 9JT")
        ));
        assert!(address_changed(
            LOCAL,
            "LS1 1AA",
            &live("4", "Park Row", "LS1 1AA")
        ));
    }

    #[test]
    fn rewrite_replaces_only_the_office_sentence() {
        let out = rewrite_office(LOCAL, "4 Park Row, Leeds, England, LS1 5HD");
        assert!(out.contains("Status: Active. Registered office: 4 Park Row, Leeds, England, LS1 5HD. Nature of business"));
        assert!(!out.contains("HIGH STREET"));
    }

    #[test]
    fn live_line_joins_premises_and_street() {
        assert_eq!(
            live_address_line(&live("12", "High Street", "LS1 1AA")).as_deref(),
            Some("12 High Street, Leeds, England, LS1 1AA")
        );
    }
}
