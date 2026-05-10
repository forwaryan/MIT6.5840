#!/usr/bin/env bash
set -u

RUNS="${RUNS:-20}"
TIMEOUT="${TIMEOUT:-90s}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
TEST_DIR="$REPO_ROOT/src/shardkv"
RESULT_DIR="$REPO_ROOT/test-results/lab5a-$(date +%Y%m%d-%H%M%S)"

if [[ ! -d "$TEST_DIR" ]]; then
  echo "Cannot find shardkv test directory: $TEST_DIR" >&2
  exit 1
fi

mkdir -p "$RESULT_DIR"

failures=()

for ((i = 1; i <= RUNS; i++)); do
  log_path="$RESULT_DIR/run-$(printf "%02d" "$i").log"
  echo "[$i/$RUNS] Running Lab5A tests..."

  if (cd "$TEST_DIR" && go test -run '5A' -count=1 -v -timeout "$TIMEOUT") 2>&1 | tee "$log_path"; then
    echo "[$i/$RUNS] PASS"
  else
    echo "[$i/$RUNS] FAIL, log: $log_path"
    failures+=("$i")
  fi
done

echo
echo "Logs: $RESULT_DIR"
if (( ${#failures[@]} == 0 )); then
  echo "All $RUNS Lab5A runs passed."
  exit 0
fi

echo "Failed runs: ${failures[*]}"
exit 1
