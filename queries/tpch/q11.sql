-- TPC-H Q11 — Important Stock Identification.
-- Spec validation substitution parameters: NATION = 'GERMANY',
-- FRACTION = 0.0001. HAVING compares each group against a scalar subquery.
-- ORDER BY note: the spec orders by the output alias `value`; this engine does
-- not support ORDER BY on an output alias (recorded H4a limitation), so the
-- aggregate expression is repeated in ORDER BY. DuckDB accepts this form.
SELECT
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) AS value
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING SUM(ps_supplycost * ps_availqty) > (
    SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
    FROM partsupp, supplier, nation
    WHERE ps_suppkey = s_suppkey
      AND s_nationkey = n_nationkey
      AND n_name = 'GERMANY'
)
ORDER BY SUM(ps_supplycost * ps_availqty) DESC;
