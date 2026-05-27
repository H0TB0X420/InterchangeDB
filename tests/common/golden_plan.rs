//! Deterministic pretty-printer for `PhysicalPlan` trees + a snapshot-style
//! assertion helper. Used by planner tests to lock down plan shapes before
//! Phase 14 (Selinger) starts mutating them.
//!
//! Why a separate walker (vs. reusing `render_explain` in
//! `src/sql/planner.rs`): `render_explain` is module-private and free to
//! evolve as the planner does. Keeping the test-side walker independent
//! insulates planner tests from cosmetic changes to operator `explain()`
//! strings.

use interchangedb::sql::PhysicalPlan;

/// Render a `PhysicalPlan` to a deterministic, diff-friendly string. Each
/// operator emits one line ending in `\n`; children are indented two spaces
/// deeper than their parent.
pub fn pretty_plan(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Executor(exec) => exec.explain(0),
        PhysicalPlan::CreateTable { name, .. } => format!("CreateTable({})\n", name),
        PhysicalPlan::BeginTxn => "BeginTxn\n".to_string(),
        PhysicalPlan::CommitTxn => "CommitTxn\n".to_string(),
        PhysicalPlan::AbortTxn => "AbortTxn\n".to_string(),
        PhysicalPlan::Explain(text) => text.clone(),
    }
}

/// Assert `pretty_plan(plan)` equals `expected`, after normalizing
/// whitespace on `expected`:
///
/// - Strips leading and trailing blank lines.
/// - Strips the minimum common leading indent from all non-blank lines.
///
/// Lets callers write the expected plan as a multi-line raw string literal:
///
/// ```ignore
/// assert_plan_matches(&p, r#"
///     Filter
///       SeqScan(t)
/// "#);
/// ```
pub fn assert_plan_matches(plan: &PhysicalPlan, expected: &str) {
    let actual = pretty_plan(plan);
    let expected_norm = normalize_indent(expected);
    assert_eq!(
        actual.trim_end_matches('\n'),
        expected_norm.trim_end_matches('\n'),
        "\n--- expected ---\n{}\n--- actual ---\n{}\n---",
        expected_norm,
        actual
    );
}

fn normalize_indent(s: &str) -> String {
    let lines: Vec<&str> = s.lines().collect();
    let first = lines.iter().position(|l| !l.trim().is_empty());
    let last = lines.iter().rposition(|l| !l.trim().is_empty());
    let (first, last) = match (first, last) {
        (Some(f), Some(l)) => (f, l),
        _ => return String::new(),
    };
    let body = &lines[first..=last];
    let min_indent = body
        .iter()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);
    body.iter()
        .map(|l| {
            if l.trim().is_empty() {
                String::new()
            } else {
                l[min_indent..].to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_strips_common_leading_indent() {
        let n = normalize_indent("\n    foo\n      bar\n    baz\n");
        assert_eq!(n, "foo\n  bar\nbaz");
    }

    #[test]
    fn normalize_preserves_inner_blank_lines() {
        let n = normalize_indent("    a\n\n    b\n");
        assert_eq!(n, "a\n\nb");
    }

    #[test]
    fn normalize_empty_input_returns_empty() {
        assert_eq!(normalize_indent(""), "");
        assert_eq!(normalize_indent("\n\n\n"), "");
    }

    #[test]
    fn pretty_renders_descriptor_plans() {
        assert_eq!(pretty_plan(&PhysicalPlan::BeginTxn), "BeginTxn\n");
        assert_eq!(pretty_plan(&PhysicalPlan::CommitTxn), "CommitTxn\n");
        assert_eq!(pretty_plan(&PhysicalPlan::AbortTxn), "AbortTxn\n");
    }

    #[test]
    fn pretty_renders_create_table_with_name_only() {
        let p = PhysicalPlan::CreateTable {
            name: "warehouse".to_string(),
            columns: vec![],
            primary_key: vec![],
        };
        assert_eq!(pretty_plan(&p), "CreateTable(warehouse)\n");
    }

    #[test]
    fn pretty_renders_explain_passthrough() {
        let p = PhysicalPlan::Explain("SeqScan(t)\n".to_string());
        assert_eq!(pretty_plan(&p), "SeqScan(t)\n");
    }

    #[test]
    fn assert_plan_matches_normalizes_expected_indent() {
        let p = PhysicalPlan::Explain("Filter\n  SeqScan(t)\n".to_string());
        assert_plan_matches(
            &p,
            r#"
                Filter
                  SeqScan(t)
            "#,
        );
    }
}
