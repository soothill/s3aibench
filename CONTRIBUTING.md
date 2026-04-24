# Contributing

## Local workflow

```
make build     # build the CLI
make test      # go test -race ./...
make coverage  # must stay at 100%
make cross     # linux/{amd64,arm64} + darwin/arm64 binaries
```

## Non-negotiable: 100 % test coverage

Every PR must keep `make coverage` green. The CI gate fails the build on any
regression. If you add code that cannot reasonably be tested (e.g., a `main`
wiring line), extract the testable parts into helpers and leave the
untestable shell minimal.

## Adding a workload

1. Create `internal/workload/<name>/<name>.go` implementing
   `workload.Workload`.
2. Provide a `Register()` function and call it from
   `internal/cli/run.go:registerWorkloads`.
3. Ship an example plan in `plans/reference-<name>.yaml`.
4. Add unit tests that exercise every branch against the
   `internal/s3client/fake` in-memory client. Integration tests against
   MinIO happen in CI automatically.

## JSON report schema

`schema_version: "1.0.0"` is a public contract. Breaking changes bump the
major version; see [`docs/schema.md`](docs/schema.md). Additive fields are
fine.

## Commits

- Small, reviewable commits.
- Reference the PRD section (`§5.2`, `§9.1`, …) when it's helpful context.
- No generated files in PRs (binaries, `dist/`, `cover.out`).
