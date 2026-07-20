-- TPC-H Q13 — Customer Distribution.
-- Spec validation substitution parameters: WORD1 = 'special', WORD2 = 'requests'
-- (o_comment NOT LIKE '%special%requests%'). LEFT OUTER JOIN inside a derived
-- table, counted per customer.
-- ORDER BY note: the spec orders by the output alias `custdist`; this engine
-- does not support ORDER BY on an output alias (recorded H4a limitation), so
-- the aggregate expression COUNT(*) is repeated in ORDER BY. DuckDB accepts
-- this form.
SELECT
    c_count,
    COUNT(*) AS custdist
FROM (
    SELECT
        c_custkey,
        COUNT(o_orderkey)
    FROM customer LEFT OUTER JOIN orders
        ON c_custkey = o_custkey
        AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey
) AS c_orders (c_custkey, c_count)
GROUP BY c_count
ORDER BY COUNT(*) DESC, c_count DESC;
