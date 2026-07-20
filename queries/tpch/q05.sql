-- TPC-H Q5 — Local Supplier Volume.
-- Spec validation substitution parameters: REGION = 'ASIA', DATE = '1994-01-01'.
-- ORDER BY note: the spec orders by the output alias `revenue`; this engine
-- does not support ORDER BY on an output alias (recorded H4a limitation), so
-- the aggregate expression is repeated in ORDER BY. DuckDB accepts this form.
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY SUM(l_extendedprice * (1 - l_discount)) DESC;
