-- TPC-H Q3 — Shipping Priority.
-- Spec validation substitution parameters: SEGMENT = 'BUILDING',
-- DATE = '1995-03-15'.
-- ORDER BY note: the spec orders by the output alias `revenue`; this engine
-- does not support ORDER BY on an output alias (recorded H4a limitation), so
-- the aggregate expression is repeated in ORDER BY. DuckDB accepts this form.
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY SUM(l_extendedprice * (1 - l_discount)) DESC, o_orderdate
LIMIT 10;
