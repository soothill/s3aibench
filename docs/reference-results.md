# Reference benchmark results

Reference result summaries are sanitized, human-readable evidence for PRD M4.
Raw generated JSON/text reports are build artifacts from the manual reference
benchmark workflow and should not be committed.

## Current status

| Target | Plan | Status | Evidence |
|---|---|---|---|
| Large-object throughput >= 40 Gbps | `plans/perf-40gbps.yaml` | Pending reference host run | Manual workflow artifact |
| Small-object GET >= 50,000 ops/sec | `plans/perf-50kops.yaml` | Pending reference host run | Manual workflow artifact |
| Fixed-hardware repeatability < 2% variance | `scripts/repeatability.py` | Pending reference host run | Manual workflow artifact |

## Publishing rules

- Run on hardware matching `bench/perf-harness.md`.
- Use AWS SDK credential-chain inputs, not inline plan credentials.
- Record `config_hash`, binary git SHA, endpoint class, host class, and date.
- Commit only this sanitized summary; keep raw `reports/` output as workflow
  artifacts.
