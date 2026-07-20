-- TPC-H Q19 — Discounted Revenue.
-- Spec validation substitution parameters:
--   (BRAND1, QUANTITY1) = ('Brand#12', 1)
--   (BRAND2, QUANTITY2) = ('Brand#23', 10)
--   (BRAND3, QUANTITY3) = ('Brand#34', 20)
-- The spec writes each quantity ceiling as `QUANTITY + 10`; those integer sums
-- are folded here to the constants 11 / 20 / 30 (this engine's predicate RHS is
-- a literal/column, not an arithmetic expression). DuckDB produces identical
-- results for the folded form.
-- Predicate note: the spec repeats `p_partkey = l_partkey`, the shipmode
-- IN-list, and the shipinstruct equality inside every OR branch; conjunct
-- extraction then sees only a disjunction — no top-level equi key — and the
-- join degrades to an NLJ cross product (the observed 120s+ grind). The common
-- conjuncts are factored OUT of the OR into top-level ANDs — semantically
-- identical by the distribution law, the standard engine accommodation, same
-- class as the FROM-order notes. Branch-specific predicates stay verbatim.
SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem, part
WHERE p_partkey = l_partkey
  AND l_shipmode IN ('AIR', 'AIR REG')
  AND l_shipinstruct = 'DELIVER IN PERSON'
  AND (
      (
          p_brand = 'Brand#12'
          AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
          AND l_quantity >= 1 AND l_quantity <= 11
          AND p_size BETWEEN 1 AND 5
      )
      OR
      (
          p_brand = 'Brand#23'
          AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
          AND l_quantity >= 10 AND l_quantity <= 20
          AND p_size BETWEEN 1 AND 10
      )
      OR
      (
          p_brand = 'Brand#34'
          AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
          AND l_quantity >= 20 AND l_quantity <= 30
          AND p_size BETWEEN 1 AND 15
      )
  );
