-- TPC-H Q8 — National Market Share.
-- Spec validation substitution parameters: NATION = 'BRAZIL',
-- REGION = 'AMERICA', TYPE = 'ECONOMY ANODIZED STEEL'. Spec derived-table form;
-- the market share is a post-aggregate division of two SUMs.
-- Runs verbatim on this engine and unmodified in DuckDB.
SELECT
    o_year,
    SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
FROM (
    SELECT
        EXTRACT(YEAR FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation
    -- FROM order note: the spec lists `part, supplier, lineitem, ...`; part
    -- and supplier only connect through lineitem, so textual left-deep
    -- lowering (used for derived-table subplans under every planner — the
    -- recorded H4a subplan-reorder lever) would open with a cross join.
    -- Reordered so consecutive tables share a join predicate — inner joins
    -- commute, results identical; predicates/columns verbatim (harness
    -- textual-order accommodation).
    FROM part, lineitem, supplier, orders, customer, nation n1, nation n2, region
    WHERE p_partkey = l_partkey
      AND s_suppkey = l_suppkey
      AND l_orderkey = o_orderkey
      AND o_custkey = c_custkey
      AND c_nationkey = n1.n_nationkey
      AND n1.n_regionkey = r_regionkey
      AND r_name = 'AMERICA'
      AND s_nationkey = n2.n_nationkey
      AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
      AND p_type = 'ECONOMY ANODIZED STEEL'
) AS all_nations
GROUP BY o_year
ORDER BY o_year;
