-- TPC-H Q15 — Top Supplier.
-- Spec validation substitution parameter: DATE = '1996-01-01'.
-- The spec defines a VIEW `revenue0`; this engine has no CREATE VIEW yet
-- (recorded gap), so the view body is inlined as a DERIVED TABLE at both of its
-- uses (the FROM relation and the MAX scalar subquery). DuckDB runs the inlined
-- form unmodified.
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM supplier,
     (SELECT l_suppkey AS supplier_no,
             SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
      FROM lineitem
      WHERE l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
      GROUP BY l_suppkey) revenue0
WHERE s_suppkey = supplier_no
  AND total_revenue = (
      SELECT MAX(total_revenue)
      FROM (SELECT l_suppkey AS supplier_no,
                   SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
            FROM lineitem
            WHERE l_shipdate >= DATE '1996-01-01'
              AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
            GROUP BY l_suppkey) revenue1
  )
ORDER BY s_suppkey;
