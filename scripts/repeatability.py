#!/usr/bin/env python3
"""Run s3aibench plans repeatedly and summarize run-to-run variability."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


BYTES_PER_MIB = 1024 * 1024


@dataclass
class RunResult:
    plan: Path
    index: int
    returncode: int
    elapsed_seconds: float
    json_path: Path
    text_path: Path
    stdout_path: Path
    stderr_path: Path
    report: dict[str, Any] | None
    error: str


def main() -> int:
    args = parse_args()
    repo_root = Path.cwd()

    plans = [p.resolve() for p in args.plan] if args.plan else default_plans(repo_root)
    if not plans:
        print("no plans found; pass --plan or run from the repository root", file=sys.stderr)
        return 2

    if args.runs < 1:
        print("--runs must be at least 1", file=sys.stderr)
        return 2

    bench_bin = args.bin
    if args.build:
        run_build(repo_root)
    if not command_exists(bench_bin):
        print(f"{bench_bin} does not exist; run make build or pass --build", file=sys.stderr)
        return 2

    output_dir = args.output_dir or default_output_dir(repo_root)
    output_dir.mkdir(parents=True, exist_ok=True)

    extra_args = list(args.s3aibench_args)
    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]

    print(f"writing repeatability artifacts to {output_dir}")
    results = run_plans(
        bench_bin=bench_bin,
        plans=plans,
        runs=args.runs,
        output_dir=output_dir,
        prefix_root=args.prefix,
        extra_args=extra_args,
        fail_fast=args.fail_fast,
    )

    summary = render_summary(results, output_dir, args.runs, extra_args)
    summary_path = args.summary_file or output_dir / "summary.md"
    summary_path.write_text(summary, encoding="utf-8")
    print(summary)
    print(f"\nsummary written to {summary_path}")

    return 1 if any(r.returncode != 0 for r in results) else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run every benchmark plan multiple times and produce a Markdown "
            "repeatability/variability summary."
        )
    )
    parser.add_argument("--runs", type=int, default=3, help="times to run each plan")
    parser.add_argument(
        "--plan",
        action="append",
        type=Path,
        help="plan to run; repeat the flag to choose a subset (default: plans/*.yaml)",
    )
    parser.add_argument("--bin", default="./s3aibench", help="s3aibench binary path")
    parser.add_argument(
        "--build",
        action="store_true",
        help="run make build before executing benchmarks",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="directory for raw reports and the summary",
    )
    parser.add_argument(
        "--summary-file",
        type=Path,
        help="summary Markdown path (default: <output-dir>/summary.md)",
    )
    parser.add_argument(
        "--prefix",
        default="s3aibench-repeatability",
        help="S3 key prefix root passed to every run",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="stop after the first failed benchmark invocation",
    )
    parser.add_argument(
        "s3aibench_args",
        nargs=argparse.REMAINDER,
        help="arguments after -- are passed through to s3aibench run",
    )
    return parser.parse_args()


def default_plans(repo_root: Path) -> list[Path]:
    return sorted((repo_root / "plans").glob("*.yaml"))


def default_output_dir(repo_root: Path) -> Path:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return repo_root / "reports" / f"repeatability-{stamp}"


def run_build(repo_root: Path) -> None:
    proc = subprocess.run(["make", "build"], cwd=repo_root, text=True)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def command_exists(command: str) -> bool:
    path = Path(command)
    if path.exists():
        return True
    return shutil.which(command) is not None


def run_plans(
    *,
    bench_bin: str,
    plans: list[Path],
    runs: int,
    output_dir: Path,
    prefix_root: str,
    extra_args: list[str],
    fail_fast: bool,
) -> list[RunResult]:
    results: list[RunResult] = []
    total = len(plans) * runs
    ordinal = 0

    for plan in plans:
        plan_slug = plan.stem
        plan_dir = output_dir / plan_slug
        plan_dir.mkdir(parents=True, exist_ok=True)

        for index in range(1, runs + 1):
            ordinal += 1
            json_path = plan_dir / f"run-{index:02d}.json"
            text_path = plan_dir / f"run-{index:02d}.txt"
            stdout_path = plan_dir / f"run-{index:02d}.stdout.log"
            stderr_path = plan_dir / f"run-{index:02d}.stderr.log"

            cmd = [
                bench_bin,
                "run",
                "--plan",
                str(plan),
                "--output-json",
                str(json_path),
                "--output-text",
                str(text_path),
                "--progress=false",
                "--prefix",
                f"{prefix_root}/{plan_slug}/",
                *extra_args,
            ]
            print(f"[{ordinal}/{total}] {plan.name} run {index}: {shell_join(cmd)}")

            started = time.monotonic()
            try:
                proc = subprocess.run(cmd, text=True, capture_output=True)
            except OSError as exc:
                elapsed = time.monotonic() - started
                stdout_path.write_text("", encoding="utf-8")
                stderr_path.write_text(str(exc), encoding="utf-8")
                result = RunResult(
                    plan=plan,
                    index=index,
                    returncode=127,
                    elapsed_seconds=elapsed,
                    json_path=json_path,
                    text_path=text_path,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                    report=None,
                    error=str(exc),
                )
                results.append(result)
                print(f"failed: {exc}", file=sys.stderr)
                if fail_fast:
                    return results
                continue
            elapsed = time.monotonic() - started

            stdout_path.write_text(proc.stdout, encoding="utf-8")
            stderr_path.write_text(proc.stderr, encoding="utf-8")

            report: dict[str, Any] | None = None
            error = ""
            if proc.returncode == 0:
                try:
                    report = json.loads(json_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as exc:
                    error = f"could not parse {json_path}: {exc}"
                    proc_returncode = 1
                except TypeError as exc:
                    error = f"malformed report {json_path}: {exc}"
                    proc_returncode = 1
                else:
                    workloads = report.get("workloads")
                    if not isinstance(workloads, list) or not workloads:
                        error = f"report {json_path} does not contain workload results"
                        proc_returncode = 1
                    else:
                        proc_returncode = 0
            else:
                error = proc.stderr.strip().splitlines()[-1] if proc.stderr.strip() else "run failed"
                proc_returncode = proc.returncode

            result = RunResult(
                plan=plan,
                index=index,
                returncode=proc_returncode,
                elapsed_seconds=elapsed,
                json_path=json_path,
                text_path=text_path,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                report=report,
                error=error,
            )
            results.append(result)

            if proc_returncode != 0:
                print(f"failed: {error}", file=sys.stderr)
                if fail_fast:
                    return results

    return results


def render_summary(
    results: list[RunResult],
    output_dir: Path,
    requested_runs: int,
    extra_args: list[str],
) -> str:
    generated_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        "# s3aibench Repeatability Summary",
        "",
        f"- Generated: `{generated_at}`",
        f"- Output directory: `{output_dir}`",
        f"- Requested runs per plan: `{requested_runs}`",
        f"- Extra run args: `{shell_join(extra_args) if extra_args else '(none)'}`",
        "",
        "## Run Matrix",
        "",
        "| Plan | Success | Failed | Mean wall time | Config hashes |",
        "|---|---:|---:|---:|---|",
    ]

    for plan in sorted({r.plan for r in results}, key=lambda p: p.name):
        plan_results = [r for r in results if r.plan == plan]
        successes = [r for r in plan_results if r.returncode == 0 and r.report is not None]
        failures = [r for r in plan_results if r.returncode != 0]
        hashes = sorted(
            {
                str(r.report.get("run", {}).get("config_hash", ""))
                for r in successes
                if r.report is not None
            }
        )
        hash_cell = format_hashes(hashes)
        mean_wall = mean([r.elapsed_seconds for r in successes])
        lines.append(
            "| "
            + " | ".join(
                [
                    plan.name,
                    str(len(successes)),
                    str(len(failures)),
                    format_seconds(mean_wall),
                    hash_cell,
                ]
            )
            + " |"
        )

    metric_rows = collect_metric_rows(results)
    if metric_rows:
        lines.extend(
            [
                "",
                "## Variability",
                "",
                "| Plan | Workload | Op | Metric | N | Mean | StdDev | CV% | Min | Max | Band |",
                "|---|---|---|---|---:|---:|---:|---:|---:|---:|---|",
            ]
        )
        lines.extend(render_metric_rows(metric_rows))

    failures = [r for r in results if r.returncode != 0]
    if failures:
        lines.extend(
            [
                "",
                "## Failed Runs",
                "",
                "| Plan | Run | Exit | Stderr log | Last error |",
                "|---|---:|---:|---|---|",
            ]
        )
        for result in failures:
            lines.append(
                "| "
                + " | ".join(
                    [
                        result.plan.name,
                        str(result.index),
                        str(result.returncode),
                        f"`{result.stderr_path}`",
                        escape_table(result.error),
                    ]
                )
                + " |"
            )

    return "\n".join(lines)


def collect_metric_rows(results: list[RunResult]) -> dict[tuple[str, str, str, str], list[float]]:
    rows: dict[tuple[str, str, str, str], list[float]] = {}
    for result in results:
        if result.returncode != 0 or result.report is None:
            continue

        report = result.report
        plan_name = str(report.get("run", {}).get("plan_name") or result.plan.stem)
        duration = report_duration_seconds(report)
        if duration <= 0:
            duration = result.elapsed_seconds

        workloads = report.get("workloads") or []
        if not isinstance(workloads, list):
            continue

        for workload in workloads:
            workload_name = str(workload.get("name", "unknown"))
            operations = workload.get("operations", {})
            if not isinstance(operations, dict):
                continue

            for op_name, operation in operations.items():
                if not isinstance(operation, dict):
                    continue
                latency = operation.get("latency_ns", {})
                count = float(operation.get("count", 0) or 0)
                errors = float(operation.get("errors", 0) or 0)
                throughput_mib_s = float(operation.get("throughput_bps", 0) or 0) / BYTES_PER_MIB

                add_metric(rows, plan_name, workload_name, op_name, "ops/s", count / duration)
                add_metric(rows, plan_name, workload_name, op_name, "MiB/s", throughput_mib_s)
                add_metric(rows, plan_name, workload_name, op_name, "p50 ms", ns_to_ms(latency.get("p50", 0)))
                add_metric(rows, plan_name, workload_name, op_name, "p99 ms", ns_to_ms(latency.get("p99", 0)))
                add_metric(rows, plan_name, workload_name, op_name, "errors", errors)
    return rows


def add_metric(
    rows: dict[tuple[str, str, str, str], list[float]],
    plan: str,
    workload: str,
    op: str,
    metric: str,
    value: float,
) -> None:
    rows.setdefault((plan, workload, op, metric), []).append(value)


def render_metric_rows(rows: dict[tuple[str, str, str, str], list[float]]) -> list[str]:
    rendered: list[str] = []
    metric_order = {"ops/s": 0, "MiB/s": 1, "p50 ms": 2, "p99 ms": 3, "errors": 4}
    for key in sorted(rows, key=lambda k: (k[0], k[1], k[2], metric_order.get(k[3], 99), k[3])):
        plan, workload, op, metric = key
        values = rows[key]
        stats = summarize(values)
        rendered.append(
            "| "
            + " | ".join(
                [
                    escape_table(plan),
                    escape_table(workload),
                    escape_table(op),
                    metric,
                    str(len(values)),
                    format_metric(stats["mean"], metric),
                    format_metric(stats["stddev"], metric),
                    format_cv(stats["cv_pct"]),
                    format_metric(stats["min"], metric),
                    format_metric(stats["max"], metric),
                    variability_band(stats["cv_pct"]),
                ]
            )
            + " |"
        )
    return rendered


def summarize(values: list[float]) -> dict[str, float]:
    avg = mean(values)
    if len(values) > 1:
        variance = sum((value - avg) ** 2 for value in values) / (len(values) - 1)
        stddev = math.sqrt(variance)
    else:
        stddev = 0.0
    cv_pct = (stddev / avg * 100.0) if avg != 0 else 0.0
    return {
        "mean": avg,
        "stddev": stddev,
        "cv_pct": cv_pct,
        "min": min(values),
        "max": max(values),
    }


def report_duration_seconds(report: dict[str, Any]) -> float:
    run = report.get("run", {})
    try:
        start = parse_rfc3339(str(run.get("started_at", "")))
        end = parse_rfc3339(str(run.get("ended_at", "")))
    except ValueError:
        return 0.0
    return max((end - start).total_seconds(), 0.0)


def parse_rfc3339(value: str) -> dt.datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return dt.datetime.fromisoformat(value)


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def ns_to_ms(value: Any) -> float:
    return float(value or 0) / 1_000_000.0


def format_hashes(hashes: list[str]) -> str:
    if not hashes:
        return "(none)"
    if len(hashes) == 1:
        return f"`{hashes[0]}`"
    return f"{len(hashes)} distinct"


def format_seconds(value: float) -> str:
    return f"{value:.1f}s"


def format_cv(value: float) -> str:
    return f"{value:.2f}"


def format_metric(value: float, metric: str) -> str:
    if metric == "errors":
        return f"{value:.1f}"
    if metric in {"p50 ms", "p99 ms"}:
        return f"{value:.3f}"
    return f"{value:.2f}"


def variability_band(cv_pct: float) -> str:
    if cv_pct <= 5:
        return "high repeatability"
    if cv_pct <= 15:
        return "moderate variability"
    return "high variability"


def shell_join(parts: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in parts)


def escape_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


if __name__ == "__main__":
    raise SystemExit(main())
