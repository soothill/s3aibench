# PRD: S3 AI Workload Benchmark Tool

**Status:** Draft
**Author:** Darren Soothill
**Date:** 23 April 2026
**Target release:** TBD

---

## 1. Summary

A high-performance, single-binary Go tool (`s3aibench`) that benchmarks S3-compatible object storage endpoints against workload patterns representative of modern AI/ML infrastructure. Unlike generic S3 benchmarks (warp, s3-benchmark, cosbench), this tool models the specific I/O shapes produced by distributed training checkpointing, vector database (LanceDB-style) operations, metadata-heavy pipelines, and mixed small/large object access — all driven from a single process capable of saturating 100 GbE+ links.

The tool consumes a declarative test plan (YAML/TOML), runs for a user-defined duration with a user-defined thread count, and emits both human-readable text reports and machine-readable JSON for CI pipelines and downstream analysis.

## 2. Problem statement

Impossible Cloud needs defensible, reproducible performance data for AI/ML workloads. Existing S3 benchmarking tools have three limitations:

1. **Wrong workload shapes.** Tools like `warp` optimise for steady-state GET/PUT of uniform object sizes. Real AI workloads are bursty, mixed-size, and involve specific access patterns (range reads on Parquet/Lance files, multipart commits on checkpoints, manifest listing on MVCC tables) that stress different code paths in the storage backend.
2. **Weak metadata coverage.** LIST, HEAD, and tag operations are either absent or treated as an afterthought, despite dominating some ML pipelines (dataset enumeration, manifest resolution).
3. **Single-process scaling ceilings.** Python and Java benchmark runners hit GIL/GC walls before saturating a modern NIC. We need a Go binary that can push a single box to line rate.

A purpose-built benchmark closes these gaps and gives the storage, GPU-as-a-Service, and partnerships teams a common yardstick when talking to customers and validating platform changes.

## 3. Goals and non-goals

### 3.1 Goals

- Simulate AI-representative workloads: checkpointing, LanceDB-style MVCC, training data loading, metadata enumeration, and configurable mixes.
- Scale to ≥40 Gbps aggregate throughput from a single process on commodity hardware.
- Run for configurable durations (seconds to hours) with configurable concurrency.
- Accept declarative test plans describing file counts, size distributions, and operation mixes.
- Emit both a formatted text report and a JSON report in a single run.
- Work against any S3-compatible endpoint (Impossible Cloud, AWS S3, MinIO, Ceph RGW).

### 3.2 Non-goals

- Distributed/multi-node coordination. A single-process tool is the v1 scope; multi-host orchestration is a future consideration.
- GUI or web dashboard. Text and JSON only.
- Simulating compute-side ML logic (model forward passes, tokenisation). This is purely a storage benchmark.
- Replacing end-to-end training benchmarks such as MLPerf Storage — this tool is for targeted, reproducible micro-benchmarks.
- Functional/correctness testing of S3 APIs (there are existing conformance suites for that).

## 4. Target users

- **Storage platform engineers at Impossible Cloud** validating changes to the S3 data path, erasure coding, and placement logic.
- **GPU-as-a-Service team** establishing baseline performance guarantees for training and inference customers.
- **Solutions engineers** running reproducible demonstrations against customer workloads.
- **External customers and partners** running apples-to-apples comparisons between providers.

## 5. Workload definitions

Each workload is a named pattern the tool can execute. Workloads are composable: a test plan can run one, several sequentially, or several concurrently with weighted mixes.

### 5.1 Checkpointing

Models distributed-training checkpoint writes (PyTorch, DeepSpeed, Megatron).

- Multiple writer threads, each writing one large object per checkpoint cycle.
- Object sizes configurable; defaults cover `1 GiB`, `10 GiB`, `100 GiB`.
- Multipart upload with configurable part size (default `16 MiB`) and part-level concurrency (default `8`).
- Bursty cadence: write-burst, idle interval, repeat. Interval configurable.
- Optional "resume" mode: periodic reads of the most recent checkpoint to model restart/validation.
- Metrics: time-to-first-byte per upload, total upload wall-clock, part throughput, completion rate.

### 5.2 LanceDB / vector DB MVCC

Models the access pattern of LanceDB and similar columnar vector formats writing to S3 via the `object_store` crate.

- Periodic small writes to a manifest prefix (`_versions/*.manifest`) — typically `1–64 KiB`.
- Larger data-fragment writes to a data prefix — typically `1–256 MiB`, multipart.
- Range reads against existing data fragments with configurable range size distribution (`4 KiB` to `4 MiB`, skewed small).
- LIST operations against the manifest prefix with and without delimiter, pagination on.
- HEAD operations prior to range reads (simulating metadata validation).
- Configurable read:write ratio; default 80:20 read-heavy to model query-dominant workloads.
- Metrics: p50/p95/p99 range-read latency, manifest-write latency, LIST latency vs. prefix cardinality.

### 5.3 Training data loading

Models data-loader access patterns during training (WebDataset, Parquet, raw image shards).

- High-concurrency small-to-medium GETs against a pre-populated dataset.
- Size distribution configurable; default `64 KiB – 4 MiB`, log-normal.
- Access pattern selectable: `sequential`, `shuffled`, `zipfian` (hot-key skew).
- Optional range reads (for shard indexing).
- Metrics: sustained object-GET rate, tail latency (p99, p999), throughput per thread.

### 5.4 Metadata workload

Stresses metadata and listing paths independent of data throughput.

- Operations: `HEAD`, `LIST` (with and without prefix, with and without delimiter), `COPY`, `GetObjectTagging`, `PutObjectTagging`.
- Configurable prefix fanout and depth to model bucket layout.
- Pagination depth configurable (force N continuation tokens).
- Metrics: ops/sec per verb, latency distribution per verb, listing throughput in objects/sec.

### 5.5 Small-object I/O

- Uniform small objects (`1 KiB`, `4 KiB`, `64 KiB`, configurable).
- High-concurrency PUT and GET.
- Measures the endpoint's ability to sustain IOPS-bound traffic.

### 5.6 Large-object I/O

- Uniform large objects (`1 GiB`, `10 GiB`, `100 GiB`).
- Multipart PUT and multipart/range GET.
- Measures throughput ceiling.

### 5.7 Write-intensive mix

Composite scenario: checkpointing + LanceDB manifest writes + small PUT burst, running concurrently.

### 5.8 Read-intensive mix

Composite scenario: training data loading + LanceDB range reads + metadata LIST, running concurrently.

## 6. Configuration (test plan format)

Test plans are YAML files. TOML support is a stretch goal. Example:

```yaml
name: "ic-blackwell-training-baseline"
endpoint: "https://eu-central-1.storage.impossibleapi.net"
region: "eu-central-1"
bucket: "bench-scratch"

# Global defaults, overridable per workload
defaults:
  duration: "10m"
  threads: 256
  multipart_part_size: "16MiB"
  multipart_concurrency: 8
  warmup: "30s"

workloads:
  - name: "checkpoint-cycle"
    type: "checkpointing"
    weight: 1
    object_size: "10GiB"
    writers: 8
    burst_interval: "5m"
    retain_versions: 3

  - name: "lance-query"
    type: "lancedb"
    weight: 3
    dataset:
      fragments: 500
      fragment_size: "128MiB"
      manifest_count: 200
    read_write_ratio: "80:20"
    range_size_distribution:
      type: "lognormal"
      mean: "64KiB"
      sigma: 1.5

  - name: "dataloader"
    type: "training_data"
    weight: 2
    object_count: 1000000
    size_distribution:
      type: "lognormal"
      mean: "512KiB"
      sigma: 0.8
    access_pattern: "zipfian"
    zipfian_s: 0.99

output:
  text: "./reports/baseline.txt"
  json: "./reports/baseline.json"
  progress: true
```

### 6.1 Required parameters

- `endpoint`, `bucket`, and either AWS-standard env credentials or explicit `access_key`/`secret_key` fields (env preferred).
- At least one workload entry.

### 6.2 Optional parameters

- `tls_skip_verify`, `path_style`, `http2`, `connection_pool_size`.
- `prepopulate`: whether to create the dataset before measurement begins (default `true` for reads).
- `cleanup`: whether to delete created objects on exit (default `true`).
- `random_seed`: for reproducibility.

## 7. CLI

```
s3aibench run --plan plan.yaml [flags]
s3aibench validate --plan plan.yaml      # parse + dry-run, no S3 traffic
s3aibench prepopulate --plan plan.yaml   # just create the dataset, then exit
s3aibench cleanup --plan plan.yaml       # just delete objects created by a prior run
```

Flag overrides for common parameters (`--duration`, `--threads`, `--endpoint`, `--bucket`) take precedence over the plan file, to support CI matrix runs without editing YAML.

### 7.1 Environment variable overrides

Every plan field and CLI flag can also be set via environment variable. This is the primary mechanism for CI systems, container orchestrators (Kubernetes Jobs, GitHub Actions, Nomad), and secret stores to inject configuration without rewriting the plan file or constructing long flag strings.

**Precedence (highest to lowest):**

1. CLI flags
2. Environment variables
3. Plan file values
4. Built-in defaults

**Naming convention.** All variables prefixed `S3AIBENCH_`. Nested plan fields flatten with underscores; workload-scoped overrides take the workload name as a segment.

| Variable | Overrides |
|---|---|
| `S3AIBENCH_ENDPOINT` | `endpoint` |
| `S3AIBENCH_REGION` | `region` |
| `S3AIBENCH_BUCKET` | `bucket` |
| `S3AIBENCH_DURATION` | `defaults.duration` |
| `S3AIBENCH_THREADS` | `defaults.threads` |
| `S3AIBENCH_WARMUP` | `defaults.warmup` |
| `S3AIBENCH_MULTIPART_PART_SIZE` | `defaults.multipart_part_size` |
| `S3AIBENCH_MULTIPART_CONCURRENCY` | `defaults.multipart_concurrency` |
| `S3AIBENCH_PATH_STYLE` | `path_style` |
| `S3AIBENCH_TLS_SKIP_VERIFY` | `tls_skip_verify` |
| `S3AIBENCH_CONNECTION_POOL_SIZE` | `connection_pool_size` |
| `S3AIBENCH_OUTPUT_TEXT` | `output.text` |
| `S3AIBENCH_OUTPUT_JSON` | `output.json` |
| `S3AIBENCH_PROGRESS` | `output.progress` |
| `S3AIBENCH_RANDOM_SEED` | `random_seed` |
| `S3AIBENCH_LOG_LEVEL` | `--log-level` |
| `S3AIBENCH_PREFIX` | Run prefix (default `s3aibench/<run-id>/`) |
| `S3AIBENCH_WORKLOAD_<NAME>_THREADS` | Per-workload thread count |
| `S3AIBENCH_WORKLOAD_<NAME>_DURATION` | Per-workload duration |
| `S3AIBENCH_WORKLOAD_<NAME>_WEIGHT` | Per-workload mix weight |

Workload names in env vars are uppercased and have `-` replaced with `_`: a workload named `lance-query` becomes `S3AIBENCH_WORKLOAD_LANCE_QUERY_THREADS`.

**Credentials** follow AWS SDK convention (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_PROFILE`) — the tool does not introduce its own credential variables, so existing IAM tooling, `aws-vault`, and container credential providers work without modification.

**Validation.** On startup the tool logs every resolved value with its source (`flag`, `env`, `plan`, `default`) at `info` level, and the `config_hash` field in the JSON report is computed from the fully-resolved config so two runs with different env but the same effective configuration hash to the same value.

**Dry-run.** `s3aibench validate --plan plan.yaml` honours env var overrides and prints the resolved config, making it straightforward to debug CI pipelines where variables are injected from multiple layers.

## 8. Output

### 8.1 Text output

Human-readable, terminal-friendly, sections delimited by headings. Includes:

- Run metadata: plan name, endpoint, start/end time, total duration, tool version, git SHA.
- Per-workload summary: total ops, total bytes, aggregate throughput, error count and top error classes.
- Per-operation latency table: count, mean, p50, p90, p95, p99, p999, max, stddev.
- Throughput timeline: ASCII sparkline of throughput per 1-second bucket (optional flag).
- Error log summary: top 10 error signatures with counts.

### 8.2 JSON output

Machine-consumable, stable schema (versioned: `schema_version` field). Designed so another process can ingest it and feed dashboards, regression gates, or CI pass/fail checks.

Schema skeleton:

```json
{
  "schema_version": "1.0.0",
  "run": {
    "plan_name": "...",
    "tool_version": "...",
    "started_at": "2026-04-23T10:00:00Z",
    "ended_at": "2026-04-23T10:10:00Z",
    "endpoint": "...",
    "bucket": "...",
    "config_hash": "sha256:..."
  },
  "workloads": [
    {
      "name": "lance-query",
      "type": "lancedb",
      "operations": {
        "range_get": {
          "count": 123456,
          "errors": 2,
          "bytes": 8_589_934_592,
          "throughput_bps": 143_165_576,
          "latency_ns": {
            "p50": 3_200_000,
            "p95": 11_000_000,
            "p99": 22_000_000,
            "p999": 55_000_000,
            "max": 120_000_000,
            "histogram": { "buckets_ns": [...], "counts": [...] }
          }
        },
        "manifest_put": { ... },
        "list": { ... }
      }
    }
  ],
  "errors": [
    { "op": "range_get", "code": "SlowDown", "count": 2, "sample_message": "..." }
  ]
}
```

The JSON report is always written if `output.json` is set, regardless of whether the text report is enabled.

## 9. Non-functional requirements

### 9.1 Performance

- **Single-process throughput ≥ 40 Gbps** sustained on a host with a 100 GbE NIC and 32 cores, for large-object sequential GET. Stretch: 80 Gbps.
- **Single-process ops/sec ≥ 50,000** for small-object (`4 KiB`) GET against a low-latency endpoint.
- **Client-side overhead ≤ 5%** of observed latency at p99 — the tool must not be the bottleneck.

### 9.2 Implementation

- Written in Go (current stable release).
- AWS SDK for Go v2, with the ability to pin to a specific SDK version via build tag.
- HTTP client tuned for high concurrency: large `MaxConnsPerHost`, HTTP/2 off by default (most S3 endpoints are HTTP/1.1), TCP keepalive enabled, no connection pooling limits below the OS `ulimit`.
- Zero-allocation hot paths in the I/O loop where practical; `sync.Pool` for buffers.
- Latency captured with `hdrhistogram-go` (or equivalent) for accurate tail percentiles without memory blow-up.
- Single static binary, no runtime dependencies beyond libc.
- Cross-compiled for `linux/amd64`, `linux/arm64`, `darwin/arm64`.

### 9.3 Observability

- `--progress` flag prints live throughput and error counts every N seconds.
- `--pprof-addr` exposes `net/http/pprof` for profiling the tool itself.
- `--log-level` (`debug`|`info`|`warn`|`error`).

### 9.4 Safety

- Tool writes only under a configurable prefix (default `s3aibench/<run-id>/`) to avoid collisions with production data.
- Refuses to run against a bucket containing objects outside its prefix unless `--allow-shared-bucket` is set.
- Cleanup is best-effort but idempotent; `cleanup` subcommand can be re-run safely.

## 10. Success metrics

- Adopted by the IC storage team as the default tool for release regression testing within one quarter of GA.
- Runs reproducibly in CI with <2% run-to-run variance on fixed hardware for a reference workload.
- Used in at least one external customer benchmark report.

## 11. Risks and open questions

- **Client-side saturation.** At 100 GbE+, the tool itself can become the bottleneck. Mitigation: profile-guided tuning and explicit NIC affinity docs. Open: do we need multi-NIC binding in v1?
- **Workload realism drift.** LanceDB and checkpointing patterns will evolve. Mitigation: versioned workload definitions and a process for reviewing them against upstream libraries each quarter.
- **Credential handling.** Keeping keys out of plans. Resolved: credentials follow standard AWS SDK resolution (env vars, shared config, IMDS, container provider); the tool has no credential-specific variables of its own. Open: do we warn loudly or hard-refuse if `access_key`/`secret_key` are set inline in a plan file?
- **Comparability to MLPerf Storage.** Customers may ask how our numbers relate. Mitigation: publish a mapping doc; do not claim MLPerf compliance.

## 12. Milestones

| Milestone | Scope |
|---|---|
| M0 — Skeleton | CLI, plan parsing, S3 client, small/large object workloads, text + JSON output |
| M1 — AI workloads | Checkpointing, LanceDB, training data loader |
| M2 — Metadata + mixes | Metadata workload, read/write-intensive mixes, Zipfian access |
| M3 — Hardening | HDR histograms, pprof, progress UI, cross-compilation, docs |
| M4 — GA | CI integration, reference reports, external-facing README |

## 13. Appendix: reference workload sizing

Starting defaults based on observed patterns. These live in the repo as example plans (`plans/reference-*.yaml`), not hard-coded.

| Workload | Object size | Threads | Duration |
|---|---|---|---|
| Checkpointing (small model) | 1 GiB | 8 | 10 min |
| Checkpointing (70B model) | 140 GiB | 64 | 30 min |
| LanceDB query | 64 KiB – 4 MiB ranges | 256 | 10 min |
| Training dataloader | 512 KiB lognormal | 512 | 15 min |
| Metadata stress | n/a (1 KiB stub objects) | 128 | 5 min |
| Small-object IOPS | 4 KiB | 1024 | 5 min |
| Large-object throughput | 10 GiB | 32 | 10 min |
