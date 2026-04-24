# Repository Guidelines

## Project Structure & Module Organization

`s3aibench` is a single-binary Go CLI. Entry points live in `cmd/s3aibench/`. Core implementation is under `internal/` by concern: `cli/` for command wiring, `plan/` for YAML parsing and validation, `runner/` for execution, `s3client/` for S3 access, and `workload/` for workload implementations such as `smallobj/`, `checkpoint/`, and `lancedb/`. Public report types live in `pkg/reportschema/`. Reference benchmark plans are in `plans/`, and supporting docs are in `docs/`.

## Build, Test, and Development Commands

- `make build` builds `./s3aibench` from `cmd/s3aibench`.
- `make test` runs `go test -race -count=1 ./...`.
- `make coverage` runs the full test suite, writes `cover.out`, and fails unless total coverage is exactly `100.0%`.
- `make vet` runs `go vet ./...`.
- `make cross` builds release binaries under `dist/` for Linux and Darwin targets.

For local validation, use the CLI directly:

```bash
./s3aibench validate --plan plans/reference-small-object.yaml
./s3aibench run --plan plans/reference-small-object.yaml
```

## Coding Style & Naming Conventions

Use standard Go formatting (`gofmt`) and idiomatic package layout. Keep package names lowercase, exported identifiers in `CamelCase`, and tests in `*_test.go`. Follow the existing pattern of keeping `main` thin and moving logic into testable helpers. When adding a workload, place it in `internal/workload/<name>/` and register it from `internal/cli/run.go`.

## Testing Guidelines

Every change must keep `make coverage` green; CI enforces 100% line coverage. Prefer table-driven tests where they improve clarity. Exercise workload and S3 behavior against `internal/s3client/fake` rather than real services. Add a reference plan in `plans/reference-<name>.yaml` for new workloads, and cover validation, error paths, and report output where applicable.

## Commit & Pull Request Guidelines

Keep commits small and reviewable. The current history is short and imperative (`Initial commit`, `Create s3-ai-benchmark-prd.md`); follow that style and reference PRD sections when useful. Do not commit generated artifacts such as `dist/`, `cover.out`, or `coverage.txt`. PRs should describe the user-visible change, note any plan or schema impact, and confirm `make vet` and `make coverage` pass.

## Security & Configuration Tips

Prefer AWS SDK credential resolution (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`, etc.) over inline secrets in plan files. Do not expose `--pprof-addr` outside trusted networks, and do not open public issues for vulnerabilities; follow `SECURITY.md`.
