# TPC-H query timings

Informational only (not a gate). Captured via `cargo run --release --bin
tpch -- --time` on the reference dev machine (2015 2-core MacBook, macOS),
seed 19920101, scale factor 0.01, config **rule-based / volcano**. These
are correctness-driven timings on a small machine, not a performance
benchmark — they exist to show which query shapes are cheap and which are
dominated by known levers.

| Query |     ms | rows | note |
|-------|-------:|-----:|------|
| Q1    |  192.5 |    4 | |
| Q2    |  194.1 |    8 | |
| Q3    |  226.2 |   10 | |
| Q4    | 100032 |    5 | correlated EXISTS, plan-per-row (→ H6) |
| Q5    |  304.7 |    5 | |
| Q6    |  145.2 |    1 | |
| Q7    |  507.6 |    4 | |
| Q8    |  256.5 |    2 | |
| Q9    | 7231.2 |  175 | largest join fan-out |
| Q10   |  226.7 |   20 | |
| Q11   |   77.9 |  160 | |
| Q12   |  215.1 |    2 | |
| Q13   |  106.8 |   24 | |
| Q14   |  174.4 |    1 | |
| Q15   |  277.4 |    1 | |
| Q16   |   18.0 |  268 | |
| Q17   | 9011.4 |    1 | correlated AVG, plan-per-row (→ H6) |
| Q18   |  382.2 |    1 | |
| Q19   |  169.4 |    1 | OR-factored (see queries/tpch/q19.sql) |
| Q20   | 27377  |    1 | nested correlated scalar (→ H6) |
| Q21   |      — |    — | FEASIBILITY: exceeds 120s guard (→ H6) |
| Q22   | 7612.2 |    7 | correlated NOT EXISTS |

The four expensive/open queries (Q4, Q17, Q20, Q21) are all dominated by
the correctness-first per-outer-row correlated apply from H4c. H6
(correlated plan caching) is the lever that collapses them; Q21 is the
only one that does not complete at all under the 120s guard. Everything
without a correlated subquery runs in well under a second except Q9's
join fan-out (~7s). Push generally beats volcano on the correlated
queries (measured ~2.4× on Q4 during the H5 sweep); this table is the
volcano column only.
