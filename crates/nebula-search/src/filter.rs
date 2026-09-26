//! Tiny OData-lite filter subset for Azure AI Search compatibility.
//!
//! Supported forms (translated onto NebulaDB metadata / tenant checks):
//! - `field eq 'value'` / `field eq value`
//! - `field ne 'value'`
//! - `search.in(field, 'a,b,c')`
//! - `A and B` (conjunction only)
//!
//! Anything else yields [`FilterError::Unsupported`] with a capability id.

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FilterError {
    #[error("unsupported filter (capability azure_search.filter_odata_full): {0}")]
    Unsupported(String),
    #[error("invalid filter: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterPred {
    Eq { field: String, value: String },
    Ne { field: String, value: String },
    In { field: String, values: Vec<String> },
}

/// Parsed filter: a conjunction of predicates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilterExpr {
    pub preds: Vec<FilterPred>,
}

impl FilterExpr {
    /// Return true when `metadata` (JSON object) satisfies every predicate.
    pub fn matches(&self, metadata: &serde_json::Value) -> bool {
        self.preds.iter().all(|p| match p {
            FilterPred::Eq { field, value } => {
                meta_str(metadata, field).as_deref() == Some(value.as_str())
            }
            FilterPred::Ne { field, value } => {
                meta_str(metadata, field).as_deref() != Some(value.as_str())
            }
            FilterPred::In { field, values } => meta_str(metadata, field)
                .map(|v| values.iter().any(|x| x == &v))
                .unwrap_or(false),
        })
    }
}

fn meta_str(metadata: &serde_json::Value, field: &str) -> Option<String> {
    metadata.get(field).and_then(|v| match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    })
}

/// Parse a restricted OData filter string.
pub fn parse_filter(input: &str) -> Result<FilterExpr, FilterError> {
    let s = input.trim();
    if s.is_empty() {
        return Ok(FilterExpr::default());
    }
    let lower = s.to_ascii_lowercase();
    for op in [" or ", " gt ", " ge ", " lt ", " le ", " not ", " any(", " all("] {
        if lower.contains(op) {
            return Err(FilterError::Unsupported(format!(
                "operator/construct `{op}` not in OData-lite subset"
            )));
        }
    }

    let mut preds = Vec::new();
    for part in split_and(s) {
        preds.push(parse_pred(part.trim())?);
    }
    Ok(FilterExpr { preds })
}

fn split_and(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = s;
    while let Some(idx) = find_and(rest) {
        out.push(&rest[..idx]);
        rest = &rest[idx + 5..];
    }
    out.push(rest);
    out
}

fn find_and(s: &str) -> Option<usize> {
    s.to_ascii_lowercase().find(" and ")
}

fn parse_pred(s: &str) -> Result<FilterPred, FilterError> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(FilterError::Invalid("empty predicate".into()));
    }

    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("search.in(") {
        return parse_search_in(trimmed);
    }

    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.len() < 3 {
        return Err(FilterError::Unsupported(format!(
            "cannot parse predicate `{trimmed}`"
        )));
    }
    let field = parts[0].to_string();
    let op = parts[1].to_ascii_lowercase();
    let raw_val = parts[2..].join(" ");
    let value = strip_quotes(&raw_val);
    match op.as_str() {
        "eq" => Ok(FilterPred::Eq { field, value }),
        "ne" => Ok(FilterPred::Ne { field, value }),
        _ => Err(FilterError::Unsupported(format!("operator `{op}`"))),
    }
}

fn parse_search_in(s: &str) -> Result<FilterPred, FilterError> {
    let open = s
        .find('(')
        .ok_or_else(|| FilterError::Invalid("search.in missing (".into()))?;
    let close = s
        .rfind(')')
        .ok_or_else(|| FilterError::Invalid("search.in missing )".into()))?;
    let inner = &s[open + 1..close];
    let mut args = split_csv_args(inner);
    if args.len() < 2 {
        return Err(FilterError::Invalid(
            "search.in needs field and list".into(),
        ));
    }
    let field = args.remove(0).trim().to_string();
    let list = strip_quotes(args[0].trim());
    let values: Vec<String> = list
        .split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect();
    Ok(FilterPred::In { field, values })
}

fn split_csv_args(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    for ch in s.chars() {
        match ch {
            '\'' if !in_quote => in_quote = true,
            '\'' if in_quote => in_quote = false,
            ',' if !in_quote => {
                out.push(std::mem::take(&mut cur));
            }
            _ => cur.push(ch),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn strip_quotes(s: &str) -> String {
    let t = s.trim();
    if (t.starts_with('\'') && t.ends_with('\'')) || (t.starts_with('"') && t.ends_with('"')) {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn eq_and_ne() {
        let f = parse_filter("tenant eq 'acme' and status ne 'archived'").unwrap();
        assert_eq!(f.preds.len(), 2);
        let meta = json!({"tenant": "acme", "status": "active"});
        assert!(f.matches(&meta));
        let meta2 = json!({"tenant": "acme", "status": "archived"});
        assert!(!f.matches(&meta2));
    }

    #[test]
    fn search_in() {
        let f = parse_filter("search.in(region, 'us,eu')").unwrap();
        assert!(f.matches(&json!({"region": "eu"})));
        assert!(!f.matches(&json!({"region": "ap"})));
    }

    #[test]
    fn rejects_or() {
        assert!(matches!(
            parse_filter("a eq 1 or b eq 2"),
            Err(FilterError::Unsupported(_))
        ));
    }
}
