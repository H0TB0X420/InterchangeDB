-- TPC-H Q22 — Global Sales Opportunity.
-- Spec validation substitution parameters:
--   country codes I1..I7 = ('13', '31', '23', '29', '30', '18', '17').
-- Composes SUBSTRING, an uncorrelated scalar AVG subquery, and a correlated
-- NOT EXISTS, all inside the `custsale` derived table.
-- Runs verbatim on this engine and unmodified in DuckDB.
SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM (
    SELECT
        SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM customer
    WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
      AND c_acctbal > (
          SELECT AVG(c_acctbal)
          FROM customer
          WHERE c_acctbal > 0.00
            AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
      )
      AND NOT EXISTS (
          SELECT *
          FROM orders
          WHERE o_custkey = c_custkey
      )
) AS custsale
GROUP BY cntrycode
ORDER BY cntrycode;
