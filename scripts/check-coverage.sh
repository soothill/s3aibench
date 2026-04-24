#!/usr/bin/env bash
# Fails unless the total coverage reported by `go tool cover -func` is 100.0%.
# Usage: check-coverage.sh <coverage-file>
set -euo pipefail

FILE="${1:-coverage.txt}"
if [[ ! -f "$FILE" ]]; then
  echo "coverage file not found: $FILE" >&2
  exit 2
fi

TOTAL="$(grep -E '^total:' "$FILE" | awk '{print $NF}' | tr -d '%')"
if [[ -z "$TOTAL" ]]; then
  echo "could not parse total coverage from $FILE" >&2
  exit 2
fi

if [[ "$TOTAL" != "100.0" ]]; then
  # Only print per-function gaps when the total is actually below 100%.
  # Zero-body functions report 0.0% but contribute 0 statements to the total,
  # so listing them when total==100% is noise.
  awk '
    $1 !~ /^total:/ && $NF ~ /%$/ {
      pct = $NF; sub(/%$/, "", pct) + 0
      if (pct + 0 < 100) { printf "  %-80s %s\n", $1, $NF }
    }
  ' "$FILE" | sort -u >&2
  echo "FAIL: total coverage is ${TOTAL}%, required 100.0%" >&2
  exit 1
fi
echo "OK: total coverage is 100.0%"
