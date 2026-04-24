# Changelog

All notable changes to `s3aibench` are recorded here. The project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html); the JSON report
schema is versioned independently via `schema_version` (see
[`docs/schema.md`](docs/schema.md)).

## [Unreleased]

### Added

- **M0 skeleton.** Cobra CLI (`run`, `validate`, `prepopulate`, `cleanup`);
  YAML plan parser; config resolver with precedence and `config_hash`;
  AWS SDK v2 wrapper + in-memory fake; uniform smallobject / largeobject
  workloads; safety prefix guard; text + JSON reports.
- **M1 AI workloads.** Checkpointing (multipart burst writes with retention
  and optional resume reads), LanceDB (manifest PUT + data-fragment range
  GET with HEAD-before-GET and LIST), training dataloader
  (sequential / shuffled / zipfian access). Size samplers: uniform,
  uniform-range, lognormal, zipfian.
- **M2 metadata + mixes.** Metadata workload (HEAD, LIST w/ and w/o
  delimiter + pagination, COPY, Get/PutObjectTagging). Composite `mix`
  workload that runs nested workloads concurrently with weighted thread
  pools. Shared-bucket guard + `--allow-shared-bucket` flag.
- **M3 hardening.** HDR-histogram-backed metrics collector (1 ns – 1 h,
  3-sig-figure precision; 16-bucket percentile histogram in JSON).
  `sync.Pool`-backed `bufpool`. Live progress ticker (`output.progress`),
  ASCII sparkline helper, `--pprof-addr`, `--log-level`. Release workflow
  (`linux/{amd64,arm64}`, `darwin/arm64`). Tuning, schema, and perf-harness
  docs.
- **Tests.** 100 % line coverage enforced by CI; `make coverage` is the
  gate.

### Decided

- Inline `access_key`/`secret_key` in a plan: warn, never refuse. The SDK
  default credential chain (env / shared config / IMDS) is the preferred
  path.
- Multi-NIC binding: out of scope for v1. See
  [`docs/tuning.md`](docs/tuning.md).
