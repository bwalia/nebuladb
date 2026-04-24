//! Parse + plan tests. These don't need an index; they verify that
//! SQL → Plan lowering is correct and consistent.

use nebula_sql::{parser, plan, SemanticClause, SqlError};

fn plan_of(sql: &str) -> nebula_sql::Plan {
    plan::build(parser::parse(sql).unwrap()).unwrap()
}

#[test]
fn plans_basic_semantic_match() {
    let p = plan_of("SELECT * FROM docs WHERE semantic_match(content, 'zero trust') LIMIT 5");
    assert_eq!(p.bucket, "docs");
    assert_eq!(p.limit, Some(5));
    match p.semantic {
        SemanticClause::Match { query, column } => {
            assert_eq!(query, "zero trust");
            assert_eq!(column, "content");
        }
        _ => panic!("expected semantic match"),
    }
    assert!(p.filters.is_empty());
}

#[test]
fn plans_semantic_match_with_residual_filter() {
    let p = plan_of(
        "SELECT id, score FROM docs \
         WHERE semantic_match(content, 'dns') AND region = 'eu-west' LIMIT 10",
    );
    assert_eq!(p.filters.len(), 1);
    assert!(matches!(p.filters[0], nebula_sql::plan::Filter::Eq { .. }));
}

#[test]
fn plans_vector_distance_with_json_literal() {
    let p = plan_of(
        "SELECT * FROM docs WHERE vector_distance(embedding, '[0.1, 0.2, 0.3]') LIMIT 3",
    );
    match p.semantic {
        SemanticClause::Distance { vector, .. } => {
            assert_eq!(vector, vec![0.1, 0.2, 0.3]);
        }
        _ => panic!("expected vector distance"),
    }
}

#[test]
fn rejects_or_in_where() {
    let err = plan::build(
        parser::parse(
            "SELECT * FROM docs WHERE semantic_match(content, 'x') OR region = 'eu'",
        )
        .unwrap(),
    )
    .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(_)));
}

#[test]
fn rejects_missing_semantic_clause() {
    let err = plan::build(
        parser::parse("SELECT * FROM docs WHERE region = 'eu'").unwrap(),
    )
    .unwrap_err();
    assert!(matches!(err, SqlError::InvalidPlan(_)));
}

#[test]
fn rejects_double_semantic_clause() {
    let err = plan::build(
        parser::parse(
            "SELECT * FROM docs \
             WHERE semantic_match(a, 'x') AND vector_distance(b, '[1]')",
        )
        .unwrap(),
    )
    .unwrap_err();
    assert!(matches!(err, SqlError::InvalidPlan(_)));
}

#[test]
fn plan_build_rejects_join_in_legacy_scan_path() {
    // The scan-only `plan::build` must still reject JOINs — joins are
    // handled at the top-level `plan_tree::build` layer.
    let err =
        plan::build(parser::parse("SELECT * FROM a JOIN b ON a.id = b.id").unwrap()).unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(_)));
}

#[test]
fn in_list_filter_parses() {
    let p = plan_of(
        "SELECT * FROM docs \
         WHERE semantic_match(content, 'x') AND region IN ('eu', 'us')",
    );
    match &p.filters[0] {
        nebula_sql::plan::Filter::In { values, .. } => assert_eq!(values.len(), 2),
        _ => panic!("expected In filter"),
    }
}

#[test]
fn order_by_score_is_explicit() {
    let p = plan_of(
        "SELECT * FROM docs WHERE semantic_match(content, 'x') ORDER BY score DESC LIMIT 3",
    );
    let ob = p.order_by.as_ref().unwrap();
    assert!(matches!(ob.key, nebula_sql::plan::OrderKey::Score));
    assert!(matches!(ob.dir, nebula_sql::plan::OrderDir::Desc));
}

#[test]
fn order_by_metadata_path() {
    let p = plan_of(
        "SELECT * FROM docs \
         WHERE semantic_match(content, 'x') ORDER BY region ASC",
    );
    match p.order_by.as_ref().unwrap().key {
        nebula_sql::plan::OrderKey::Metadata(ref p) => assert_eq!(p, &vec!["region".to_string()]),
        _ => panic!("expected Metadata order"),
    }
}

#[test]
fn top_k_defaults_when_no_limit() {
    let p = plan_of("SELECT * FROM docs WHERE semantic_match(content, 'x')");
    assert!(p.top_k() >= 10);
}
