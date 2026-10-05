#!/usr/bin/env bash
# Runs the standalone Micro benchmark (bench/, ch.epfl.coshim.bench.MicroBench) once per shim
# variant with the same parameters, appends every run to one CSV and prints a comparison.
#
#   scripts/bench-compare.sh [--reps N] [MicroBench options...]
#
# With no option it runs the "simple test" (MicroBench defaults: 50 threads, table size 10000,
# 2 branches, 2 reads + 2 rmw writes per branch, skew 0.9, 10 s warm-up + 30 s measured).
# Example: scripts/bench-compare.sh --reps 3 --skew 0.99 --threads 100
# Env: CSV=path (default bench-results/<timestamp>.csv), MVN_FLAGS (e.g. -o).
# Exit code 1 if any run printed a warning or failed its audit.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
reps=1
if [[ "${1:-}" == "--reps" ]]; then
    reps="$2"
    shift 2
fi
cd "$root"
mvn -q -B ${MVN_FLAGS:-} compile -pl bench -am

cp="bench/target/classes:shim-core/target/classes:store-api/target/classes"
csv="${CSV:-$root/bench-results/$(date +%Y%m%d-%H%M%S).csv}"
status=0
for rep in $(seq 1 "$reps"); do
    for shim in speculative nonspeculative; do
        java -cp "$cp" ch.epfl.coshim.bench.MicroBench "$@" --shim "$shim" --csv "$csv" --label "rep$rep" \
            || status=1
        echo
    done
done

echo "results: $csv"
awk -F, 'NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i }
         { printf "%-6s %-15s %10s %11s %9s %9s %9s %9s %15s %20s %9s\n",
                  $col["label"], $col["shim"], $col["tps"], $col["abort_rate"], $col["timed_out"], $col["mean_us"],
                  $col["p50_us"], $col["p99_us"],
                  $col["aborts_cascade"], $col["aborts_lock_timeout"], $col["audit_ok"] }' "$csv"
exit "$status"
