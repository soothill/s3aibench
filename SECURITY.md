# Security

## Reporting a vulnerability

Please email `security@impossiblecloud.com` with details. Do **not** open a
public GitHub issue. We aim to acknowledge reports within two business days
and to ship a fix or mitigation within 30 days for high-severity issues.

## Scope

`s3aibench` is a benchmark tool. Its runtime surface is:

- Reads a local YAML plan.
- Reads environment variables and CLI flags.
- Issues S3 API calls against a user-supplied endpoint with user-supplied
  credentials.
- Writes text and JSON report files to user-supplied paths.
- Optionally exposes `net/http/pprof` on a user-supplied address.

## Credentials

Credentials follow the AWS SDK v2 resolution chain
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
`AWS_PROFILE`, IMDS, and container credential providers). Inline
`access_key`/`secret_key` in a plan file are accepted but logged at `WARN`
— prefer the SDK chain.

## Safety guardrails

The tool refuses to run against a bucket that contains objects outside its
run prefix unless `--allow-shared-bucket` is set. Cleanup is idempotent and
scoped to the prefix.

## pprof

`--pprof-addr` binds the standard Go `net/http/pprof` handlers. Only expose
this on a trusted network; it provides heap and goroutine profiles of the
running process.
