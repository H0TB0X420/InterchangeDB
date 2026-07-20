-- TPC-H Q2 — Minimum Cost Supplier.
-- Spec validation substitution parameters: SIZE = 15, TYPE = 'BRASS',
-- REGION = 'EUROPE'. Correlated MIN scalar subquery.
-- Runs verbatim on this engine and unmodified in DuckDB.
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
-- FROM order note: the spec lists `part, supplier, partsupp, ...`; part and
-- supplier share no direct predicate, so textual left-deep lowering (this
-- engine's rule-based planner) would open with a cross join. Reordered so
-- consecutive tables share a join predicate — inner joins commute, results
-- identical; predicates/columns verbatim (harness textual-order accommodation).
FROM part, partsupp, supplier, nation, region
WHERE p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 15
  AND p_type LIKE '%BRASS'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE'
  AND ps_supplycost = (
      SELECT MIN(ps_supplycost)
      FROM partsupp, supplier, nation, region
      WHERE p_partkey = ps_partkey
        AND s_suppkey = ps_suppkey
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
        AND r_name = 'EUROPE'
  )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100;
