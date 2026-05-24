//! SQL frontend — thin wrapper over `sqlparser-rs`.
//!
//! Parses a SQL string into sqlparser's AST (`Vec<Statement>`). Higher
//! layers convert the AST into our `LogicalPlan` IR and on to executor
//! trees.
//!
//! Uses `GenericDialect` — ANSI-ish, no vendor extensions. TPC-C
//! statements fit cleanly within this dialect.

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::common::{Error, Result};

/// Parse a SQL string into one or more statements.
///
/// Empty input yields an empty `Vec`. Syntax errors surface as
/// `Error::SqlParse` carrying sqlparser's diagnostic.
pub fn parse(sql: &str) -> Result<Vec<Statement>> {
    Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| Error::SqlParse(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_empty_string_to_empty_vec() {
        let stmts = parse("").unwrap();
        assert!(stmts.is_empty());
    }

    #[test]
    fn parses_simple_select() {
        let stmts = parse("SELECT a FROM t").unwrap();
        assert_eq!(stmts.len(), 1);
        // Just confirm the statement is a Query — the AST shape is
        // sqlparser's internal concern; later layers care about specifics.
        assert!(matches!(stmts[0], Statement::Query(_)));
    }

    #[test]
    fn parses_multiple_statements() {
        let stmts = parse("SELECT 1; SELECT 2; SELECT 3").unwrap();
        assert_eq!(stmts.len(), 3);
    }

    #[test]
    fn parses_each_dml_shape_tpcc_uses() {
        // Smoke test: every top-level statement type Phase 11 will plan.
        assert!(matches!(
            parse("CREATE TABLE t (id INT PRIMARY KEY)").unwrap()[0],
            Statement::CreateTable { .. }
        ));
        assert!(matches!(
            parse("INSERT INTO t VALUES (1)").unwrap()[0],
            Statement::Insert { .. }
        ));
        assert!(matches!(
            parse("UPDATE t SET a = 1 WHERE id = 2").unwrap()[0],
            Statement::Update { .. }
        ));
        assert!(matches!(
            parse("DELETE FROM t WHERE id = 1").unwrap()[0],
            Statement::Delete { .. }
        ));
    }

    #[test]
    fn syntax_error_surfaces_as_sql_parse() {
        match parse("SELEKT bogus FROM t") {
            Err(Error::SqlParse(_)) => {}
            other => panic!("expected SqlParse, got {:?}", other.err()),
        }
    }
}
