//! Workstation AI Shop catalogue + stock levels for NebulaDB RAG.
//!
//! Source of truth for demo/int ingest is the shop seed catalogue
//! (`fixtures/shop_catalog.seed.json`, mirrored from workstation-website).
//! Live OpsAPI remains the transactional catalogue when reachable; NebulaDB
//! holds a searchable RAG mirror used for sales-agent grounding, stock
//! feasibility checks, and (optionally) chat-history analytics.
//!
//! Buckets (defaults):
//! - `shop_catalog` — categories, products, option groups, rules
//! - `shop_stock_levels` — per-SKU / per-option qty + lead time (SOT for stock)
//! - `shop_chats` — sales-agent conversation turns for later support analysis

use axum::{
    extract::{Query, State},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::ApiError;
use crate::state::AppState;

const SEED_JSON: &str = include_str!("../fixtures/shop_catalog.seed.json");

pub const DEFAULT_CATALOG_BUCKET: &str = "shop_catalog";
pub const DEFAULT_STOCK_BUCKET: &str = "shop_stock_levels";
pub const DEFAULT_CHATS_BUCKET: &str = "shop_chats";

fn shop_public_base() -> String {
    std::env::var("NEBULA_SHOP_PUBLIC_URL")
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "https://int-shop.workstation.co.uk".into())
}

fn load_seed_catalog() -> Result<Value, ApiError> {
    serde_json::from_str(SEED_JSON).map_err(|e| {
        ApiError::BadRequest(format!("embedded shop catalogue seed is invalid JSON: {e}"))
    })
}

fn gbp(minor: i64) -> String {
    let neg = minor < 0;
    let v = minor.unsigned_abs();
    let pounds = v / 100;
    let pence = v % 100;
    if neg {
        format!("-£{pounds}.{pence:02}")
    } else {
        format!("£{pounds}.{pence:02}")
    }
}

fn as_i64(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_u64().map(|u| u as i64))
        .or_else(|| v.as_f64().map(|f| f as i64))
}

fn as_bool(v: &Value) -> Option<bool> {
    v.as_bool().or_else(|| match v.as_str() {
        Some("true") | Some("1") | Some("yes") => Some(true),
        Some("false") | Some("0") | Some("no") => Some(false),
        _ => None,
    })
}

/// Build searchable RAG docs + stock SOT docs from a catalogue JSON object
/// shaped like `catalog.seed.json` (`categories` + `products`).
pub fn catalog_to_docs(catalog: &Value) -> (Vec<(String, String, Value)>, Vec<(String, String, Value)>) {
    let base = shop_public_base();
    let mut catalog_docs = Vec::new();
    let mut stock_docs = Vec::new();

    if let Some(cats) = catalog.get("categories").and_then(|v| v.as_array()) {
        for c in cats {
            let slug = c.get("slug").and_then(|v| v.as_str()).unwrap_or("unknown");
            let name = c.get("name").and_then(|v| v.as_str()).unwrap_or(slug);
            let desc = c
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let text = format!(
                "Shop category {name} (slug={slug}).\n{desc}\nBrowse: {base}/c/{slug}"
            );
            catalog_docs.push((
                format!("cat-{slug}"),
                text,
                json!({
                    "source": "shop",
                    "kind": "category",
                    "slug": slug,
                    "name": name,
                    "url": format!("{base}/c/{slug}"),
                }),
            ));
        }
    }

    let products = catalog
        .get("products")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    for p in &products {
        let slug = p.get("slug").and_then(|v| v.as_str()).unwrap_or("unknown");
        let sku = p.get("sku").and_then(|v| v.as_str()).unwrap_or(slug);
        let name = p.get("name").and_then(|v| v.as_str()).unwrap_or(slug);
        let brand = p.get("brand").and_then(|v| v.as_str()).unwrap_or("");
        let ptype = p
            .get("product_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let price_mode = p
            .get("price_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("fixed");
        let cat = p
            .get("category_slug")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let short = p
            .get("short_description")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let desc = p
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or(short);
        let tags = p
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|t| t.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let base_price = p
            .get("base_price_minor")
            .or_else(|| p.get("from_price_minor"))
            .and_then(as_i64)
            .unwrap_or(0);
        let price_verified = p
            .get("price_verified")
            .and_then(as_bool)
            .unwrap_or(false);

        let mut specs_lines = String::new();
        if let Some(obj) = p.get("specs").and_then(|v| v.as_object()) {
            for (k, v) in obj {
                let val = v
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| v.to_string());
                specs_lines.push_str(&format!("- {k}: {val}\n"));
            }
        }
        let mut attr_lines = String::new();
        if let Some(obj) = p.get("attributes").and_then(|v| v.as_object()) {
            for (k, v) in obj {
                attr_lines.push_str(&format!("- {k}={v}\n"));
            }
        }

        let stock_qty = p.get("stock_qty").and_then(as_i64).unwrap_or(0);
        let lead = p.get("lead_time_days").and_then(as_i64).unwrap_or(0);
        let allow_bo = p
            .get("allow_backorder")
            .and_then(as_bool)
            .unwrap_or(false);
        let product_text = format!(
            "Product {name}\n\
             SKU: {sku}\n\
             Slug: {slug}\n\
             Brand: {brand}\n\
             Type: {ptype}\n\
             Category: {cat}\n\
             Price mode: {price_mode}\n\
             From price (ex VAT): {price} (verified={price_verified})\n\
             Stock: qty_available={stock_qty} lead_time_days={lead} allow_backorder={allow_bo}\n\
             Tags: {tags}\n\
             URL: {base}/p/{slug}\n\n\
             Summary: {short}\n\n\
             Description:\n{desc}\n\n\
             Specs:\n{specs_lines}\n\
             Attributes:\n{attr_lines}\
             Stock qty above is mirrored from shop_stock_levels (SOT id stock-{sku}). \
             For configuration validity use the options and rules docs for this slug.",
            price = gbp(base_price),
        );
        catalog_docs.push((
            format!("prod-{slug}"),
            product_text,
            json!({
                "source": "shop",
                "kind": "product",
                "slug": slug,
                "sku": sku,
                "name": name,
                "brand": brand,
                "product_type": ptype,
                "category_slug": cat,
                "price_mode": price_mode,
                "base_price_minor": base_price,
                "price_verified": price_verified,
                "url": format!("{base}/p/{slug}"),
            }),
        ));

        // Option groups — critical for configurator / feasibility.
        if let Some(groups) = p.get("option_groups").and_then(|v| v.as_array()) {
            if !groups.is_empty() {
                let mut text = format!(
                    "Configuration options for {name} ({slug}). Use option codes exactly when pricing.\n"
                );
                for g in groups {
                    let gcode = g.get("code").and_then(|v| v.as_str()).unwrap_or("?");
                    let gname = g.get("name").and_then(|v| v.as_str()).unwrap_or(gcode);
                    let sel = g.get("selection").and_then(|v| v.as_str()).unwrap_or("single");
                    let req = g.get("required").and_then(as_bool).unwrap_or(false);
                    text.push_str(&format!(
                        "\nGroup {gcode} — {gname} (selection={sel}, required={req}):\n"
                    ));
                    if let Some(opts) = g.get("options").and_then(|v| v.as_array()) {
                        for o in opts {
                            let ocode = o.get("code").and_then(|v| v.as_str()).unwrap_or("?");
                            let oname = o.get("name").and_then(|v| v.as_str()).unwrap_or(ocode);
                            let delta = o
                                .get("price_delta_minor")
                                .and_then(as_i64)
                                .unwrap_or(0);
                            let max_qty = o.get("max_qty").and_then(as_i64).unwrap_or(1);
                            let is_default = o.get("is_default").and_then(as_bool).unwrap_or(false);
                            let comp = o
                                .get("component_product_sku")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            text.push_str(&format!(
                                "- option={ocode} name=\"{oname}\" delta={delta} ({gbp}) max_qty={max_qty} default={is_default} component_sku={comp}\n",
                                gbp = gbp(delta),
                            ));
                            if let Some(attrs) = o.get("attributes").and_then(|v| v.as_object()) {
                                for (k, v) in attrs {
                                    text.push_str(&format!("    attr {k}={v}\n"));
                                }
                            }
                            // Option-level stock SOT when present.
                            if o.get("stock_qty").is_some() {
                                stock_docs.push(stock_doc_for_option(sku, slug, gcode, o));
                            }
                        }
                    }
                }
                catalog_docs.push((
                    format!("prod-{slug}-options"),
                    text,
                    json!({
                        "source": "shop",
                        "kind": "options",
                        "slug": slug,
                        "sku": sku,
                    }),
                ));
            }
        }

        if let Some(rules) = p.get("rules").and_then(|v| v.as_array()) {
            if !rules.is_empty() {
                let mut text = format!(
                    "Compatibility / feasibility rules for {name} ({slug}). A configuration is invalid if any rule is violated.\n"
                );
                for (i, r) in rules.iter().enumerate() {
                    let kind = r.get("kind").and_then(|v| v.as_str()).unwrap_or("rule");
                    let msg = r.get("message").and_then(|v| v.as_str()).unwrap_or("");
                    text.push_str(&format!("{}. kind={kind}: {msg}\n", i + 1));
                    // Keep structured payload for agents.
                    text.push_str(&format!("   raw={}\n", r));
                }
                catalog_docs.push((
                    format!("prod-{slug}-rules"),
                    text,
                    json!({
                        "source": "shop",
                        "kind": "rules",
                        "slug": slug,
                        "sku": sku,
                        "rule_count": rules.len(),
                    }),
                ));
            }
        }

        // Product-level stock SOT.
        stock_docs.push(stock_doc_for_product(p));
    }

    // Sources catalogue for the LLM.
    catalog_docs.push((
        "shop-sources".into(),
        format!(
            "Sources for the Workstation AI Shop assistant (preference order):\n\
             1. shop / product — catalogue identity, specs, from-price, tags (bucket {DEFAULT_CATALOG_BUCKET}).\n\
             2. shop / options — option group codes, deltas, attributes for configurators.\n\
             3. shop / rules — compatibility constraints; treat violations as invalid builds.\n\
             4. shop_stock_levels / stock — authoritative qty_available and lead_time_days (SOT).\n\
             5. shop_chats / chat — prior sales conversations for support handoff (not for pricing).\n\
             Never invent prices or stock. Prefer OpsAPI price_configuration for money when live; \
             NebulaDB stock docs are the RAG SOT for availability. Shop URL base: {base}."
        ),
        json!({"source": "nebula", "kind": "sources_catalogue"}),
    ));

    (catalog_docs, stock_docs)
}

fn stock_doc_for_product(p: &Value) -> (String, String, Value) {
    let slug = p.get("slug").and_then(|v| v.as_str()).unwrap_or("unknown");
    let sku = p.get("sku").and_then(|v| v.as_str()).unwrap_or(slug);
    let name = p.get("name").and_then(|v| v.as_str()).unwrap_or(slug);
    let qty = p.get("stock_qty").and_then(as_i64).unwrap_or(0);
    let lead = p.get("lead_time_days").and_then(as_i64).unwrap_or(0);
    let allow_backorder = p
        .get("allow_backorder")
        .and_then(as_bool)
        .unwrap_or(false);
    let low = p
        .get("low_stock_threshold")
        .and_then(as_i64)
        .unwrap_or(0);
    let in_stock = qty > 0 || allow_backorder;
    let text = format!(
        "STOCK SOT product {name}\n\
         sku={sku} slug={slug}\n\
         qty_available={qty}\n\
         lead_time_days={lead}\n\
         in_stock={in_stock}\n\
         allow_backorder={allow_backorder}\n\
         low_stock_threshold={low}\n\
         This document is the source of truth for stock levels in NebulaDB. \
         Do not invent quantities. Feasibility checks must read qty_available."
    );
    (
        format!("stock-{sku}"),
        text,
        json!({
            "source": "shop",
            "kind": "stock",
            "level": "product",
            "sku": sku,
            "slug": slug,
            "name": name,
            "qty_available": qty,
            "lead_time_days": lead,
            "in_stock": in_stock,
            "allow_backorder": allow_backorder,
            "low_stock_threshold": low,
        }),
    )
}

fn stock_doc_for_option(
    product_sku: &str,
    product_slug: &str,
    group_code: &str,
    o: &Value,
) -> (String, String, Value) {
    let ocode = o.get("code").and_then(|v| v.as_str()).unwrap_or("?");
    let oname = o.get("name").and_then(|v| v.as_str()).unwrap_or(ocode);
    let qty = o.get("stock_qty").and_then(as_i64).unwrap_or(0);
    let lead = o.get("lead_time_days").and_then(as_i64).unwrap_or(0);
    let allow_backorder = o
        .get("allow_backorder")
        .and_then(as_bool)
        .unwrap_or(false);
    let in_stock = qty > 0 || allow_backorder;
    let id = format!("stock-{product_sku}-{group_code}-{ocode}");
    let text = format!(
        "STOCK SOT option {oname}\n\
         product_sku={product_sku} product_slug={product_slug}\n\
         group={group_code} option={ocode}\n\
         qty_available={qty}\n\
         lead_time_days={lead}\n\
         in_stock={in_stock}\n\
         allow_backorder={allow_backorder}\n\
         Source of truth for this option's stock in NebulaDB."
    );
    (
        id,
        text,
        json!({
            "source": "shop",
            "kind": "stock",
            "level": "option",
            "sku": product_sku,
            "slug": product_slug,
            "group": group_code,
            "option": ocode,
            "name": oname,
            "qty_available": qty,
            "lead_time_days": lead,
            "in_stock": in_stock,
            "allow_backorder": allow_backorder,
        }),
    )
}

#[derive(Deserialize)]
pub struct IngestBody {
    #[serde(default = "default_true")]
    pub upsert: bool,
    #[serde(default = "default_catalog_bucket")]
    pub catalog_bucket: String,
    #[serde(default = "default_stock_bucket")]
    pub stock_bucket: String,
    /// Optional full catalogue JSON. When omitted, the embedded seed is used.
    pub catalog: Option<Value>,
}

fn default_true() -> bool {
    true
}
fn default_catalog_bucket() -> String {
    DEFAULT_CATALOG_BUCKET.into()
}
fn default_stock_bucket() -> String {
    DEFAULT_STOCK_BUCKET.into()
}

pub async fn shop_status(State(_s): State<AppState>) -> impl axum::response::IntoResponse {
    let seed_ok = load_seed_catalog().is_ok();
    let (cats, prods) = if let Ok(c) = load_seed_catalog() {
        (
            c.get("categories")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0),
            c.get("products")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0),
        )
    } else {
        (0, 0)
    };
    Json(json!({
        "configured": seed_ok,
        "seed_embedded": seed_ok,
        "seed_categories": cats,
        "seed_products": prods,
        "catalog_bucket": DEFAULT_CATALOG_BUCKET,
        "stock_bucket": DEFAULT_STOCK_BUCKET,
        "chats_bucket": DEFAULT_CHATS_BUCKET,
        "shop_public_url": shop_public_base(),
        "note": "OpsAPI remains transactional SOT when live; NebulaDB holds searchable RAG + stock SOT mirror for the sales agent and feasibility checks.",
    }))
}

pub async fn shop_ingest_catalog(
    State(s): State<AppState>,
    Json(body): Json<IngestBody>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    let catalog = match body.catalog {
        Some(c) => c,
        None => load_seed_catalog()?,
    };
    let (catalog_docs, stock_docs) = catalog_to_docs(&catalog);
    let mut upserted = Vec::new();

    if body.upsert {
        for (id, text, meta) in &catalog_docs {
            s.index
                .clone()
                .upsert_text(&body.catalog_bucket, id, text, meta.clone())
                .await
                .map_err(ApiError::Index)?;
            upserted.push(format!("{}:{}", body.catalog_bucket, id));
        }
        for (id, text, meta) in &stock_docs {
            s.index
                .clone()
                .upsert_text(&body.stock_bucket, id, text, meta.clone())
                .await
                .map_err(ApiError::Index)?;
            upserted.push(format!("{}:{}", body.stock_bucket, id));
        }
    }

    Ok(Json(json!({
        "catalog_bucket": body.catalog_bucket,
        "stock_bucket": body.stock_bucket,
        "catalog_docs": catalog_docs.len(),
        "stock_docs": stock_docs.len(),
        "upserted": upserted,
        "shop_public_url": shop_public_base(),
        "sources": catalog_docs.iter().chain(stock_docs.iter()).map(|(id, _t, meta)| json!({
            "id": id,
            "source": meta.get("source"),
            "kind": meta.get("kind"),
            "slug": meta.get("slug"),
            "sku": meta.get("sku"),
            "qty_available": meta.get("qty_available"),
        })).collect::<Vec<_>>(),
    })))
}

#[derive(Deserialize)]
pub struct StockUpsertBody {
    #[serde(default = "default_stock_bucket")]
    pub bucket: String,
    pub sku: String,
    pub slug: Option<String>,
    pub name: Option<String>,
    pub qty_available: i64,
    #[serde(default)]
    pub lead_time_days: i64,
    #[serde(default)]
    pub allow_backorder: bool,
    pub low_stock_threshold: Option<i64>,
    /// When set, updates option-level stock instead of product-level.
    pub group: Option<String>,
    pub option: Option<String>,
}

pub async fn shop_upsert_stock(
    State(s): State<AppState>,
    Json(body): Json<StockUpsertBody>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    if body.sku.trim().is_empty() {
        return Err(ApiError::BadRequest("sku is required".into()));
    }
    let slug = body.slug.clone().unwrap_or_else(|| body.sku.clone());
    let name = body.name.clone().unwrap_or_else(|| slug.clone());
    let in_stock = body.qty_available > 0 || body.allow_backorder;
    let (id, text, meta) = if let (Some(group), Some(option)) = (&body.group, &body.option) {
        let id = format!("stock-{}-{}-{}", body.sku, group, option);
        let text = format!(
            "STOCK SOT option {name}\n\
             product_sku={} product_slug={slug}\n\
             group={group} option={option}\n\
             qty_available={}\n\
             lead_time_days={}\n\
             in_stock={in_stock}\n\
             allow_backorder={}\n\
             Source of truth for this option's stock in NebulaDB.",
            body.sku, body.qty_available, body.lead_time_days, body.allow_backorder
        );
        (
            id,
            text,
            json!({
                "source": "shop",
                "kind": "stock",
                "level": "option",
                "sku": body.sku,
                "slug": slug,
                "group": group,
                "option": option,
                "name": name,
                "qty_available": body.qty_available,
                "lead_time_days": body.lead_time_days,
                "in_stock": in_stock,
                "allow_backorder": body.allow_backorder,
            }),
        )
    } else {
        let id = format!("stock-{}", body.sku);
        let low = body.low_stock_threshold.unwrap_or(0);
        let text = format!(
            "STOCK SOT product {name}\n\
             sku={} slug={slug}\n\
             qty_available={}\n\
             lead_time_days={}\n\
             in_stock={in_stock}\n\
             allow_backorder={}\n\
             low_stock_threshold={low}\n\
             This document is the source of truth for stock levels in NebulaDB.",
            body.sku, body.qty_available, body.lead_time_days, body.allow_backorder
        );
        (
            id,
            text,
            json!({
                "source": "shop",
                "kind": "stock",
                "level": "product",
                "sku": body.sku,
                "slug": slug,
                "name": name,
                "qty_available": body.qty_available,
                "lead_time_days": body.lead_time_days,
                "in_stock": in_stock,
                "allow_backorder": body.allow_backorder,
                "low_stock_threshold": low,
            }),
        )
    };

    s.index
        .clone()
        .upsert_text(&body.bucket, &id, &text, meta.clone())
        .await
        .map_err(ApiError::Index)?;

    Ok(Json(json!({
        "bucket": body.bucket,
        "id": id,
        "stock": meta,
    })))
}

#[derive(Deserialize)]
pub struct FeasibilityBody {
    pub product_slug: String,
    #[serde(default = "default_qty")]
    pub qty: i64,
    /// Map of group_code -> [{option, qty}]
    #[serde(default)]
    pub selections: Value,
    #[serde(default = "default_stock_bucket")]
    pub stock_bucket: String,
}

fn default_qty() -> i64 {
    1
}

/// Lightweight feasibility check against NebulaDB stock SOT (+ seed rules text).
/// Full money pricing stays on OpsAPI; this answers "is this build stockable?".
pub async fn shop_feasibility(
    State(s): State<AppState>,
    Json(body): Json<FeasibilityBody>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    if body.product_slug.trim().is_empty() {
        return Err(ApiError::BadRequest("product_slug is required".into()));
    }
    let qty = body.qty.max(1);
    let catalog = load_seed_catalog()?;
    let product = catalog
        .get("products")
        .and_then(|v| v.as_array())
        .and_then(|arr| {
            arr.iter().find(|p| {
                p.get("slug").and_then(|v| v.as_str()) == Some(body.product_slug.as_str())
            })
        })
        .cloned()
        .ok_or_else(|| ApiError::NotFound(format!("product slug {}", body.product_slug)))?;

    let sku = product
        .get("sku")
        .and_then(|v| v.as_str())
        .unwrap_or(&body.product_slug)
        .to_string();

    // Prefer live stock doc from the stock bucket; fall back to seed.
    let stock_id = format!("stock-{sku}");
    let (qty_available, lead_time_days, allow_backorder) =
        if let Some(doc) = s.index.get(&body.stock_bucket, &stock_id) {
            let meta = &doc.metadata;
            (
                meta.get("qty_available").and_then(as_i64).unwrap_or(0),
                meta.get("lead_time_days").and_then(as_i64).unwrap_or(0),
                meta.get("allow_backorder")
                    .and_then(as_bool)
                    .unwrap_or(false),
            )
        } else {
            (
                product.get("stock_qty").and_then(as_i64).unwrap_or(0),
                product.get("lead_time_days").and_then(as_i64).unwrap_or(0),
                product
                    .get("allow_backorder")
                    .and_then(as_bool)
                    .unwrap_or(false),
            )
        };

    let mut shortages = Vec::new();
    if qty_available < qty && !allow_backorder {
        shortages.push(json!({
            "name": product.get("name").and_then(|v| v.as_str()).unwrap_or(&sku),
            "requested": qty,
            "available": qty_available,
            "sku": sku,
        }));
    }

    // Option stock checks from selections.
    if let Some(obj) = body.selections.as_object() {
        for (group, items) in obj {
            if let Some(arr) = items.as_array() {
                for item in arr {
                    let option = item.get("option").and_then(|v| v.as_str()).unwrap_or("");
                    let oqty = item.get("qty").and_then(as_i64).unwrap_or(1) * qty;
                    if option.is_empty() {
                        continue;
                    }
                    let oid = format!("stock-{sku}-{group}-{option}");
                    if let Some(doc) = s.index.get(&body.stock_bucket, &oid) {
                        let aq = doc
                            .metadata
                            .get("qty_available")
                            .and_then(as_i64)
                            .unwrap_or(0);
                        let ab = doc
                            .metadata
                            .get("allow_backorder")
                            .and_then(as_bool)
                            .unwrap_or(false);
                        if aq < oqty && !ab {
                            shortages.push(json!({
                                "name": doc.metadata.get("name").and_then(|v| v.as_str()).unwrap_or(option),
                                "requested": oqty,
                                "available": aq,
                                "sku": sku,
                                "group": group,
                                "option": option,
                            }));
                        }
                    }
                }
            }
        }
    }

    let rules = product
        .get("rules")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let in_stock = shortages.is_empty() && (qty_available > 0 || allow_backorder);
    let valid = shortages.is_empty(); // rule engine stays on OpsAPI; we only flag stock here

    Ok(Json(json!({
        "product_slug": body.product_slug,
        "sku": sku,
        "qty": qty,
        "valid": valid,
        "in_stock": in_stock,
        "qty_available": qty_available,
        "lead_time_days": lead_time_days,
        "allow_backorder": allow_backorder,
        "shortages": shortages,
        "rules_present": rules.as_array().map(|a| a.len()).unwrap_or(0),
        "note": "Stock feasibility from NebulaDB shop_stock_levels SOT. Compatibility rule evaluation and pricing remain on OpsAPI price_configuration when live.",
        "stock_bucket": body.stock_bucket,
        "stock_doc_id": stock_id,
    })))
}

#[derive(Deserialize)]
pub struct ChatIngestBody {
    #[serde(default = "default_chats_bucket")]
    pub bucket: String,
    pub session_id: String,
    /// Optional stable customer / cart token for support join.
    pub customer_ref: Option<String>,
    pub role: String,
    pub content: String,
    pub meta: Option<Value>,
}

fn default_chats_bucket() -> String {
    DEFAULT_CHATS_BUCKET.into()
}

pub async fn shop_ingest_chat(
    State(s): State<AppState>,
    Json(body): Json<ChatIngestBody>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    if body.session_id.trim().is_empty() || body.content.trim().is_empty() {
        return Err(ApiError::BadRequest(
            "session_id and content are required".into(),
        ));
    }
    let role = body.role.trim().to_ascii_lowercase();
    if !matches!(role.as_str(), "user" | "assistant" | "tool" | "system") {
        return Err(ApiError::BadRequest(
            "role must be user|assistant|tool|system".into(),
        ));
    }
    let ts = chrono_like_now();
    let id = format!("chat-{}-{}-{}", body.session_id, ts, role);
    let text = format!(
        "Shop sales chat turn\n\
         session_id={}\n\
         customer_ref={}\n\
         role={role}\n\
         at={ts}\n\n\
         {}",
        body.session_id,
        body.customer_ref.as_deref().unwrap_or(""),
        body.content.trim()
    );
    let meta = json!({
        "source": "shop",
        "kind": "chat",
        "session_id": body.session_id,
        "customer_ref": body.customer_ref,
        "role": role,
        "ts": ts,
        "extra": body.meta,
    });
    s.index
        .clone()
        .upsert_text(&body.bucket, &id, &text, meta.clone())
        .await
        .map_err(ApiError::Index)?;
    Ok(Json(json!({
        "bucket": body.bucket,
        "id": id,
        "session_id": body.session_id,
        "kind": "chat",
    })))
}

#[derive(Deserialize)]
pub struct ProductSearchQuery {
    pub q: String,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
    #[serde(default = "default_catalog_bucket")]
    pub bucket: String,
}

fn default_top_k() -> usize {
    8
}

/// Thin wrapper: hybrid search scoped to the shop catalogue bucket.
pub async fn shop_search_products(
    State(s): State<AppState>,
    Query(q): Query<ProductSearchQuery>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    if q.q.trim().is_empty() {
        return Err(ApiError::BadRequest("q is required".into()));
    }
    let weights = s.hybrid_weights.resolve(Some(q.bucket.as_str()));
    let hits = s
        .index
        .search_text_hybrid(&q.q, Some(q.bucket.as_str()), q.top_k.min(50), None, weights)
        .await
        .map_err(ApiError::Index)?;
    Ok(Json(json!({
        "bucket": q.bucket,
        "q": q.q,
        "hits": hits.iter().map(|h| json!({
            "id": h.id,
            "score": h.score,
            "text": h.text,
            "metadata": h.metadata,
        })).collect::<Vec<_>>(),
    })))
}

fn chrono_like_now() -> String {
    // Avoid pulling chrono if unused elsewhere — use unix millis.
    use std::time::{SystemTime, UNIX_EPOCH};
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("{ms}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_parses_and_emits_docs() {
        let catalog = load_seed_catalog().expect("seed");
        let (cats, stock) = catalog_to_docs(&catalog);
        assert!(cats.iter().any(|d| d.2.get("kind") == Some(&json!("product"))));
        assert!(cats.iter().any(|d| d.2.get("kind") == Some(&json!("options"))));
        assert!(stock.iter().any(|d| d.0.starts_with("stock-")));
        assert!(cats.iter().any(|d| d.0 == "shop-sources"));
        // Known product from int-shop
        assert!(cats.iter().any(|d| d.0 == "prod-rtx-pro-6000-blackwell-workstation"));
        assert!(stock
            .iter()
            .any(|d| d.0 == "stock-NV-RTXPRO6000-WE"));
    }

    #[test]
    fn gbp_format() {
        assert_eq!(gbp(1299900), "£12999.00");
        assert_eq!(gbp(0), "£0.00");
    }
}
