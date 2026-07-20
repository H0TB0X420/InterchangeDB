-- TPC-H Q14 — Promotion Effect.
-- Spec validation substitution parameter: DATE = '1995-09-01'. Verbatim
-- left-associative `100.00 * SUM(...) / SUM(...)` (the H3.1 division fix).
-- Runs verbatim on this engine and unmodified in DuckDB.
SELECT
    100.00 * SUM(CASE
        WHEN p_type LIKE 'PROMO%'
        THEN l_extendedprice * (1 - l_discount)
        ELSE 0
    END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem, part
WHERE l_partkey = p_partkey
  AND l_shipdate >= DATE '1995-09-01'
  AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH;
