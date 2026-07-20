-- TPC-H Q18 — Large Volume Customer.
-- Spec validation substitution parameter: QUANTITY = 300. IN over a
-- grouped-HAVING subquery (a statement constant set), a 3-way join, and a
-- 5-key GROUP BY.
-- Runs verbatim on this engine and unmodified in DuckDB.
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (
      SELECT l_orderkey
      FROM lineitem
      GROUP BY l_orderkey
      HAVING SUM(l_quantity) > 300
  )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100;
