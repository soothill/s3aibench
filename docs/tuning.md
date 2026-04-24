# Tuning `s3aibench`

This document captures the knobs that matter when pushing a single box toward
its NIC line rate. It is deliberately short: most defaults are already tuned
for high-concurrency S3 traffic. Reach for these only if you cannot hit the
performance targets in `bench/perf-harness.md`.

## Kernel / process limits

```
ulimit -n 1048576          # high file descriptor ceiling
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_tw_reuse=1
sysctl -w net.ipv4.ip_local_port_range="10000 65535"
```

The tool may open thousands of concurrent TCP sockets; defaults on most
distros cap ephemeral ports and fd counts well below what's needed at
≥40 Gbps.

## NIC affinity

Pin the process (and its worker goroutines) to the NUMA node that owns the
100 GbE NIC:

```
numactl --cpunodebind=0 --membind=0 ./s3aibench run --plan plan.yaml
```

Verify with `ethtool -l <iface>` that RSS is enabled and the NIC has enough
hardware queues (ideally ≥ number of cores on the pinned NUMA node).

## Multi-NIC

**Not supported in v1.** Binding across multiple NICs requires kernel-level
socket steering or LACP, which is out of scope for a single-process
benchmark. If you need aggregate bandwidth beyond one NIC, run multiple
`s3aibench` processes bound to different interfaces and sum their JSON
reports externally.

## HTTP tuning

`s3aibench` already sets:
- `MaxIdleConnsPerHost: 4096` (overridable via `--connection-pool-size`)
- `MaxConnsPerHost: 0` (unlimited below ulimit)
- `KeepAlive: 30s`, `DisableCompression: true`
- `ForceAttemptHTTP2: false` (most S3 endpoints are HTTP/1.1)

For endpoints that benefit from HTTP/2 (very low per-connection RTT), add
`--http2` or set `http2: true` in the plan. In most Impossible Cloud and AWS
configurations this makes latency worse, not better.

## TLS

Leaving TLS on is the right default. `--tls-skip-verify` exists only for
lab environments with self-signed certs (e.g., MinIO in a test pod).

## Client-side bottlenecks

If `pprof` shows >5% of wall time in `crypto/tls` or `net/http` frame parsing,
you are hitting a client-side ceiling. Options:
1. Increase `--connection-pool-size` to expose more concurrent sockets.
2. Split the workload across multiple `s3aibench` processes on separate NUMA
   nodes.
3. File an issue — the tool aims for ≤5% client overhead at p99 per PRD §9.1.
