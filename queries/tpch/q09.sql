-- TPC-H Q9 — Product Type Profit Measure.
-- Spec validation substitution parameter: COLOR = 'green'. Spec derived-table
-- form; nation and o_year are plain columns of the derived table.
-- Runs verbatim on this engine and unmodified in DuckDB.
SELECT
    nation,
    o_year,
    SUM(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        EXTRACT(YEAR FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    -- FROM order note: the spec lists `part, supplier, lineitem, ...`; part
    -- and supplier only connect through lineitem, so textual left-deep
    -- lowering (used for derived-table subplans under every planner — the
    -- recorded H4a subplan-reorder lever) would open with a cross join
    -- (~2000×100 pairs probed against 60k lineitems = the observed grind).
    -- Reordered so consecutive tables share a join predicate — inner joins
    -- commute, results identical; predicates/columns verbatim (harness
    -- textual-order accommodation).
    FROM part, lineitem, supplier, partsupp, orders, nation
    WHERE s_suppkey = l_suppkey
      AND ps_suppkey = l_suppkey
      AND ps_partkey = l_partkey
      AND p_partkey = l_partkey
      AND o_orderkey = l_orderkey
      AND s_nationkey = n_nationkey
      AND p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation, o_year DESC;
