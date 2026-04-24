# JSON report schema

`s3aibench run` emits a JSON report whose shape is declared in
[`pkg/reportschema`](../pkg/reportschema/report.go). The schema is stable at
`schema_version: "1.0.0"`. Breaking changes bump the major version.

## Top level

```json
{
  "schema_version": "1.0.0",
  "run":       { … run metadata },
  "workloads": [ { … per-workload } ],
  "errors":    [ { … aggregated errors } ]
}
```

## `run`

| Field          | Type     | Description |
|----------------|----------|-------------|
| `plan_name`    | string   | `name:` from the plan |
| `tool_version` | string   | `v0.1.0 (abcdef)` |
| `started_at`   | RFC3339  | Measurement-window start (after warmup) |
| `ended_at`    | RFC3339  | Measurement-window end |
| `endpoint`     | string   | Resolved S3 endpoint |
| `bucket`       | string   | Bucket name |
| `region`       | string   | Region (if set) |
| `config_hash`  | string   | `sha256:…` — deterministic fingerprint of the resolved config |
| `config_sources` | object | `{field → "flag"\|"env"\|"plan"\|"default"}` |

## `workloads[]`

Each entry reports one named workload. `operations` is keyed by op name
(`put`, `get`, `head`, `list`, `copy`, `range_get`, `multipart_complete`,
`manifest_put`, `checkpoint_put`, `get_tagging`, `put_tagging`, `delete`).

```json
{
  "name": "lance-query",
  "type": "lancedb",
  "operations": {
    "range_get": { … operation },
    "head":      { … operation },
    "list":      { … operation }
  }
}
```

## `operation`

| Field            | Type    | Units   |
|------------------|---------|---------|
| `count`          | int     | Ops |
| `errors`         | int     | Ops that returned a non-nil error |
| `bytes`          | int     | Cumulative bytes transferred |
| `throughput_bps` | float   | Bytes/sec over the measurement window |
| `latency_ns`     | object  | See below |

### `latency_ns`

| Field        | Type  | Units |
|--------------|-------|-------|
| `p50`, `p90`, `p95`, `p99`, `p999` | int | Nanoseconds |
| `max`        | int   | Nanoseconds |
| `mean`       | int   | Nanoseconds |
| `stddev`     | int   | Nanoseconds |
| `histogram`  | object | `{ buckets_ns: [n₁, …, n₁₆], counts: [c₁, …, c₁₆] }` |

The histogram is 16 percentile-spaced buckets over an HDR histogram with
3-significant-figure precision spanning 1 ns to 1 hour.

## `errors[]`

Top-level aggregated errors across all workloads.

| Field            | Type   | Description |
|------------------|--------|-------------|
| `op`             | string | Operation class |
| `code`           | string | SDK error code (`SlowDown`, `NoSuchKey`, `unknown`, …) |
| `count`          | int    | Occurrences |
| `sample_message` | string | First-seen error message |

## Stability

- Adding optional fields is *not* a breaking change.
- Removing, renaming, or changing the semantics of an existing field
  bumps `schema_version` to `2.0.0`.
- Downstream consumers should parse with tolerant unmarshalers
  (`json.Unmarshal` with `DisallowUnknownFields` off).
