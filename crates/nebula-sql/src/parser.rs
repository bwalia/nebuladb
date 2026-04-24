//! Thin wrapper around `sqlparser-rs`.
//!
//! We use `GenericDialect` — not PostgreSQL — because NebulaDB's SQL
//! surface is an augmented subset, not a Postgres dialect clone. A
//! strict dialect would reject valid NebulaDB syntax (our function
//! calls) because it enforces its own list of known built-ins.

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::{Result, SqlError};

pub fn parse(sql: &str) -> Result<Statement> {
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| SqlError::Parse(e.to_string()))?;
    if stmts.len() != 1 {
        return Err(SqlError::Unsupported(format!(
            "expected exactly one statement, got {}",
            stmts.len()
        )));
    }
    Ok(stmts.pop().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_select() {
        let s = parse("SELECT * FROM docs WHERE id = 1").unwrap();
        assert!(matches!(s, Statement::Query(_)));
    }

    #[test]
    fn parses_semantic_match_as_function_call() {
        // We don't need sqlparser to know about our function — it just
        // needs to accept the syntactic form of a function call.
        let s = parse(
            "SELECT * FROM docs WHERE semantic_match(content, 'zero trust') LIMIT 10",
        )
        .unwrap();
        assert!(matches!(s, Statement::Query(_)));
    }

    #[test]
    fn rejects_multiple_statements() {
        let err = parse("SELECT 1; SELECT 2").unwrap_err();
        assert!(matches!(err, SqlError::Unsupported(_)));
    }
}
