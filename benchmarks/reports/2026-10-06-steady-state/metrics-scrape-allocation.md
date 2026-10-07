# Dense metrics scrape allocation

At 100 tenant clusters and 5,000 nodes, the production database-backed metrics collector allocates approximately **250.6 MB per complete scrape** of a dense retained-topology fixture (10 pools/cluster, 10 sets/pool). This is allocation traffic, not peak RSS. No scrape failed or was truncated in this experiment. The concern is recurring GC and memory pressure when scrapes overlap controller work.

Five repetitions per concurrency, 12 measured scrapes each (plus warmup), 1-second idle time per scraping worker:

| Scrape concurrency | Median per-scrape latency | Allocated bytes/scrape | End-of-run heap delta |
|---|---:|---:|---:|
| 1 | 463.1 ms | 250,606,768 | 129,258,152 |
| 2 | 647.9 ms | 250,597,876 | 216,856,376 |
| 4 | 718.9 ms | 250,592,564 | 529,603,112 |

Each response contains 100,100 samples and approximately 18.8 MB of text. All 180 measured scrapes returned the expected sample count and HTTP status. Each scrape executes seven database queries.

These component measurements exercise the production collector and PostgreSQL, not a deployed management-plane SLO. Go compilation ran concurrently during parts of this batch, so elapsed times are preliminary; allocation counts and complete-response checks are the stronger evidence. The retained topology deliberately stresses metrics cardinality; the full-worker fixture uses one current set per pool.

Reproduction with the benchmark harness changes in this working tree:
```sh
make -C controller perf-build
python3 -m benchmarks.cwe_perf.steady_state \
  --scenario benchmarks/scenarios/clusters-100-nodes-5000.json \
  --harness controller/bin/cwe-perf --db-dsn "$BENCHMARK_DB_DSN" \
  --results-dir /tmp/nke-scrapes --repetitions 5 \
  --case scrapes-c1 --case scrapes-c2 --case scrapes-c4
```

Underlying controller revision: a4b4f2a21cf3344ca19138ed4f5bbe099f430cb4; Go 1.27.1, darwin/arm64, PostgreSQL 17.4. Harness updates are currently uncommitted and `perf-build` is not available on main yet. Raw JSON and a report will be retained with the benchmark changes.

Follow-up: measure allocation and GC under realistic scrape cadence alongside steady controller work, then evaluate ways to reduce recurring collector allocation. This local record captures the finding; no production remediation has been made.
