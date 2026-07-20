-- TPC-H Q6 — Forecasting Revenue Change.
-- Spec validation substitution parameters: DATE = '1994-01-01',
-- DISCOUNT = 0.06, QUANTITY = 24.
-- The spec writes the discount band as `0.06 - 0.01` .. `0.06 + 0.01`; those
-- decimal-literal sums are folded here to the constants 0.05 and 0.07 (this
-- engine's predicate RHS is a literal/column, not an arithmetic expression).
-- DuckDB produces identical results for the folded form.
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24;
