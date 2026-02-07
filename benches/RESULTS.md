# Benchmark Results

Single-threaded, real disk I/O. Run with `cargo bench`.

## BPM Bench (`bpm_bench.rs`)

Measures buffer pool manager page access throughput under eviction pressure (640 pages, pool=64).

| Benchmark | Throughput | Latency |
|-----------|-----------|---------|
| Sequential scan | 160K pages/sec | 4.0 ms / 640 pages |
| Random read-modify-write | 100 ops/sec | 1.0 s / 100 ops |

## B-Tree Bench (`btree_bench.rs`)

Measures B+Tree operations with realistic page sizes (10K keys, leaf_max=202, internal_max=290, pool=1024).

| Benchmark | Throughput | Latency |
|-----------|-----------|---------|
| Bulk insert (10K keys) | 6.8K inserts/sec | 1.48 s / 10K keys |
| Random lookup | 21K lookups/sec | 4.7 ms / 100 lookups |
| Mixed delete/insert/modify | 25K ops/sec | 4.0 ms / 100 ops |

## Eviction Policies (`eviction_policies.rs`)

Compares six eviction policies (FIFO, Clock, LRU, LRU-K, 2Q, ARC) across five workload patterns. Run with `cargo bench -- eviction` for throughput or `cargo bench -- summary` for hit rates.

## Notes

- BusTub equivalents use in-memory disk, so their numbers are much higher.
- Random BPM writes are bottlenecked by real disk I/O on eviction (fsync).
- Concurrent versions will be added after latch crabbing is implemented.
