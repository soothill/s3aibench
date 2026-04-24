# s3aibench

`s3aibench` is a single-binary Go benchmark for S3-compatible object storage,
focused on the workload shapes produced by modern AI/ML infrastructure:
distributed-training checkpoints, LanceDB/MVCC, training dataloaders,
metadata stress, and mixed scenarios.

- See [`s3-ai-benchmark-prd.md`](./s3-ai-benchmark-prd.md) for the full PRD.
- See [`docs/schema.md`](docs/schema.md) for the JSON report contract.
- See [`docs/tuning.md`](docs/tuning.md) for host tuning at ≥40 Gbps.
- See [`docs/comparison-mlperf.md`](docs/comparison-mlperf.md) for the
  intentional scope difference vs MLPerf Storage.

## Install

```
go install github.com/darrensoothill/s3aibench/cmd/s3aibench@latest
```

Or build from a clone:

```
make build           # ./s3aibench
make cross           # linux/{amd64,arm64} + darwin/arm64 under dist/
```

## Quickstart

```
cat > plan.yaml <<'YAML'
name: quickstart
endpoint: http://localhost:9000
region: us-east-1
bucket: bench-scratch
path_style: true
access_key: minio
secret_key: miniominio
defaults:
  duration: 30s
  threads: 32
workloads:
  - name: small
    type: smallobject
    object_size: 4KiB
output:
  text: ./out.txt
  json: ./out.json
  progress: true
YAML

s3aibench validate --plan plan.yaml
s3aibench run       --plan plan.yaml
```

## Subcommands

| Command        | Purpose |
|----------------|---------|
| `validate`     | Parse a plan + env + flags and print the resolved config + source per field. No S3 traffic. |
| `run`          | Execute the plan; emit text and/or JSON report. |
| `prepopulate`  | Create the dataset a plan needs, then exit. |
| `cleanup`      | Delete every object under the plan's run prefix (idempotent). |

Top-level flags:

- `--pprof-addr :6060` — bind `net/http/pprof` for profiling.
- Per-subcommand: `--endpoint`, `--bucket`, `--threads`, `--duration`,
  `--warmup`, `--multipart-part-size`, `--multipart-concurrency`,
  `--output-text`, `--output-json`, `--progress`, `--path-style`,
  `--tls-skip-verify`, `--http2`, `--connection-pool-size`,
  `--prepopulate`, `--cleanup`, `--random-seed`, `--prefix`,
  `--log-level`, `--allow-shared-bucket`.

## Environment variable overrides (PRD §7.1)

All plan fields can be overridden by `S3AIBENCH_*` env vars. Precedence
(high → low): **flag → env → plan → built-in default**. Common variables:

| Variable | Overrides |
|----------|-----------|
| `S3AIBENCH_ENDPOINT`           | `endpoint` |
| `S3AIBENCH_REGION`             | `region` |
| `S3AIBENCH_BUCKET`             | `bucket` |
| `S3AIBENCH_DURATION`           | `defaults.duration` |
| `S3AIBENCH_THREADS`            | `defaults.threads` |
| `S3AIBENCH_WARMUP`             | `defaults.warmup` |
| `S3AIBENCH_MULTIPART_PART_SIZE` | `defaults.multipart_part_size` |
| `S3AIBENCH_MULTIPART_CONCURRENCY` | `defaults.multipart_concurrency` |
| `S3AIBENCH_PATH_STYLE`         | `path_style` |
| `S3AIBENCH_TLS_SKIP_VERIFY`    | `tls_skip_verify` |
| `S3AIBENCH_HTTP2`              | `http2` |
| `S3AIBENCH_CONNECTION_POOL_SIZE` | `connection_pool_size` |
| `S3AIBENCH_OUTPUT_TEXT`        | `output.text` |
| `S3AIBENCH_OUTPUT_JSON`        | `output.json` |
| `S3AIBENCH_PROGRESS`           | `output.progress` |
| `S3AIBENCH_RANDOM_SEED`        | `random_seed` |
| `S3AIBENCH_LOG_LEVEL`          | `--log-level` |
| `S3AIBENCH_PREFIX`             | Run prefix root (default `s3aibench/`) |
| `S3AIBENCH_WORKLOAD_<NAME>_THREADS` | Per-workload threads |
| `S3AIBENCH_WORKLOAD_<NAME>_DURATION` | Per-workload duration |
| `S3AIBENCH_WORKLOAD_<NAME>_WEIGHT` | Per-workload mix weight |
| `S3AIBENCH_ALLOW_SHARED_BUCKET` | `--allow-shared-bucket` |

Workload names in env vars are uppercased and have `-` replaced by `_` —
`lance-query` becomes `S3AIBENCH_WORKLOAD_LANCE_QUERY_THREADS`.

**Credentials** follow the AWS SDK chain
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
`AWS_PROFILE`). Inline `access_key`/`secret_key` in a plan are accepted but
logged at `WARN`.

## Reference plans

Under `plans/`:

- `reference-small-object.yaml`, `reference-large-object.yaml` (M0)
- `reference-checkpoint-10gib.yaml`, `reference-lance-query.yaml`,
  `reference-dataloader.yaml` (M1)
- `reference-metadata.yaml`, `reference-write-intensive-mix.yaml`,
  `reference-read-intensive-mix.yaml` (M2)
- `perf-40gbps.yaml`, `perf-50kops.yaml` (M3 performance harness)

## Output

`run` always writes a text report (to `output.text` or stdout) and, when
`output.json` is set, a JSON report matching
[`pkg/reportschema`](./pkg/reportschema). The JSON schema is stable at
`schema_version: "1.0.0"`.

## Testing and quality

- **100 % line coverage** is enforced by CI via
  [`scripts/check-coverage.sh`](scripts/check-coverage.sh). `make coverage`
  is the single command that runs tests and fails the build on regression.
- CI runs `go vet`, `make coverage`, validates every reference plan, and
  runs a short MinIO-backed smoke run on every PR.
- Cross-compiled artefacts for `linux/{amd64,arm64}` and `darwin/arm64` are
  built on tag push via `.github/workflows/release.yml`.

## License

Apache 2.0. See [`LICENSE`](LICENSE).
