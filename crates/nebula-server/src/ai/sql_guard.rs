//! Read-only SQL validation for AI NL→SQL and query_sql tool.

#[derive(Debug, thiserror::Error)]
pub enum SqlGuardError {
    #[error("only SELECT (read-only) statements are allowed")]
    NotReadOnly,
    #[error("table '{0}' is not in the allowlist")]
    TableNotAllowed(String),
    #[error("query rejected: {0}")]
    Rejected(&'static str),
}

/// Validate that `sql` is a single read-only SELECT.
/// Optional table allowlist (empty = any table permitted at this layer).
pub fn validate_readonly_sql(
    sql: &str,
    table_allowlist: &[String],
) -> Result<String, SqlGuardError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err(SqlGuardError::Rejected("empty sql"));
    }
    if trimmed.contains(';') {
        return Err(SqlGuardError::Rejected("multiple statements not allowed"));
    }
    let lower = trimmed.to_lowercase();
    if !lower.starts_with("select") && !lower.starts_with("with") {
        return Err(SqlGuardError::NotReadOnly);
    }
    for banned in [
        " insert ",
        " update ",
        " delete ",
        " drop ",
        " alter ",
        " truncate ",
        " create ",
        " grant ",
        " revoke ",
        " attach ",
        " copy ",
        " call ",
        " execute ",
        " into ",
    ] {
        if format!(" {lower} ").contains(banned) {
            return Err(SqlGuardError::NotReadOnly);
        }
    }

    if !table_allowlist.is_empty() {
        // Naive FROM extraction for allowlist demos.
        if let Some(pos) = lower.find(" from ") {
            let after = &lower[pos + 6..];
            let table = after
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches(|c| c == '"' || c == '`')
                .trim_end_matches(',');
            if !table.is_empty()
                && !table_allowlist
                    .iter()
                    .any(|t| t.eq_ignore_ascii_case(table))
            {
                return Err(SqlGuardError::TableNotAllowed(table.to_string()));
            }
        }
    }

    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_select() {
        assert!(validate_readonly_sql("SELECT 1", &[]).is_ok());
    }

    #[test]
    fn rejects_delete() {
        assert!(matches!(
            validate_readonly_sql("DELETE FROM t", &[]),
            Err(SqlGuardError::NotReadOnly)
        ));
    }

    #[test]
    fn enforces_allowlist() {
        let allow = vec!["products".into()];
        assert!(validate_readonly_sql("SELECT * FROM products", &allow).is_ok());
        assert!(matches!(
            validate_readonly_sql("SELECT * FROM secrets", &allow),
            Err(SqlGuardError::TableNotAllowed(_))
        ));
    }
}
