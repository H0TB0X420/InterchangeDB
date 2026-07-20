-- TPC-H Q4 — Order Priority Checking.
-- Spec validation substitution parameter: DATE = '1993-07-01'.
-- Correlated EXISTS over lineitem.
-- Runs verbatim on this engine and unmodified in DuckDB.
SELECT
    o_orderpriority,
    COUNT(*) AS order_count
FROM orders
WHERE o_orderdate >= DATE '1993-07-01'
  AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
  AND EXISTS (
      SELECT *
      FROM lineitem
      WHERE l_orderkey = o_orderkey
        AND l_commitdate < l_receiptdate
  )
GROUP BY o_orderpriority
ORDER BY o_orderpriority;
