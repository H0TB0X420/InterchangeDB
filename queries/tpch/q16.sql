-- TPC-H Q16 — Parts/Supplier Relationship.
-- Spec validation substitution parameters: BRAND = 'Brand#45',
-- TYPE = 'MEDIUM POLISHED', SIZES = (49, 14, 23, 45, 19, 3, 36, 9).
-- NOT IN over a non-null key set, NOT LIKE, IN-list, COUNT(DISTINCT).
-- ORDER BY note: the spec orders by the output alias `supplier_cnt`; this
-- engine does not support ORDER BY on an output alias (recorded H4a
-- limitation), so the aggregate expression is repeated in ORDER BY. DuckDB
-- accepts this form.
SELECT
    p_brand,
    p_type,
    p_size,
    COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey
  AND p_brand <> 'Brand#45'
  AND p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps_suppkey NOT IN (
      SELECT s_suppkey
      FROM supplier
      WHERE s_comment LIKE '%Customer%Complaints%'
  )
GROUP BY p_brand, p_type, p_size
ORDER BY COUNT(DISTINCT ps_suppkey) DESC, p_brand, p_type, p_size;
