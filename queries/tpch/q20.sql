-- TPC-H Q20 — Potential Part Promotion.
-- Spec validation substitution parameters: COLOR = 'forest', DATE = '1994-01-01',
-- NATION = 'CANADA'. An uncorrelated IN whose inner query carries a nested
-- uncorrelated IN and a correlated scalar (0.5 * SUM over a date window).
-- Runs verbatim on this engine and unmodified in DuckDB.
SELECT
    s_name,
    s_address
FROM supplier, nation
WHERE s_suppkey IN (
        SELECT ps_suppkey
        FROM partsupp
        WHERE ps_partkey IN (
                SELECT p_partkey
                FROM part
                WHERE p_name LIKE 'forest%'
            )
          AND ps_availqty > (
                SELECT 0.5 * SUM(l_quantity)
                FROM lineitem
                WHERE l_partkey = ps_partkey
                  AND l_suppkey = ps_suppkey
                  AND l_shipdate >= DATE '1994-01-01'
                  AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            )
    )
  AND s_nationkey = n_nationkey
  AND n_name = 'CANADA'
ORDER BY s_name;
