# Performance harness

This note pins down the reference hardware and methodology used to verify the
PRD §9.1 performance targets:

- **≥ 40 Gbps** sustained single-process throughput on large-object sequential GET
- **≥ 50,000 ops/sec** on 4 KiB GETs
- **≤ 5 %** client overhead at p99 vs a `curl --trace-time` baseline

## Reference hardware

| Component | Spec |
|-----------|------|
| CPU       | AMD EPYC 7643, 48c/96t (or comparable 32+ cores) |
| RAM       | 128 GiB DDR4-3200 |
| NIC       | Mellanox ConnectX-6 Dx 100 GbE (MTU 1500) |
| Kernel    | Linux 6.1+ with BBR congestion control |
| Go        | `go1.23+` |

Bind the process to the NIC's NUMA node (`numactl --cpunodebind=0 --membind=0`)
and run with `ulimit -n 1048576`. See [`docs/tuning.md`](../docs/tuning.md).

## Reference plans

- `plans/perf-40gbps.yaml` — 10 GiB objects, 32 threads, 16 MiB part size,
  8-way multipart concurrency. Targets the throughput ceiling.
- `plans/perf-50kops.yaml` — 4 KiB objects, 1024 threads. Targets the
  small-object IOPS ceiling.

## Methodology

1. Warm up the endpoint: run each plan once with `duration: 30s, warmup: 15s`
   and discard the report.
2. Run three back-to-back measurement cycles (`duration: 5m` each); the
   median cycle is the published number.
3. For client-overhead measurement: issue 100 identical GETs via `curl
   --trace-time` against the same object and compare p99 latency against
   the tool's recorded p99 for the same object in a single-thread run.

Report both the `config_hash` and the git SHA of the binary in every
published number.

## Reference evidence

Raw reference JSON/text outputs are uploaded by the manual reference benchmark
workflow. Sanitized, checked-in summaries live in
[`docs/reference-results.md`](../docs/reference-results.md); do not commit raw
generated reports under `reports/`.

| Plan              | Measured throughput | p99 client overhead |
|-------------------|---------------------|---------------------|
| `perf-40gbps.yaml`  | See reference summary | See reference summary |
| `perf-50kops.yaml`  | See reference summary | See reference summary |
