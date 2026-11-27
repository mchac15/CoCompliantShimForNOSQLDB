#!/usr/bin/env bash
# The final sweep: the adaptive fanout cap, the speculative shim (no cap) and the non-speculative one,
# over workload configurations that each change one parameter of the defaults (scripts/bench-defaults.env),
# at every thread count of THREADS_LIST.
#
#   scripts/bench-sweep.sh              # runs (or resumes) the sweep
#   DRY_RUN=1 scripts/bench-sweep.sh    # prints the commands and the expected duration, runs nothing
#   SMOKE=1 scripts/bench-sweep.sh      # every configuration once at 50 threads, 5 s + 10 s: checks the setup
#   ONLY="ops50 rtt30" scripts/bench-sweep.sh   # a subset of the configurations
#
# Per configuration NAME, in SWEEP_OUT/NAME/: adaptive/ (SPEC_MAX_FANOUT=auto, the long windows) and
# base/ (speculative + non-speculative, the short windows), each a scripts/bench-final.sh sweep with its
# summary. A finished thread count is skipped when the sweep is run again with the same SWEEP_OUT.
#
# The configurations (ops per branch; totals per txn). "one hot": branch 0 at skew 0.95, the others
# uniform (0.5). The totals stay near 20 where an exact read ratio needs it (4 and 6 branches: 24).
#   base        2 branches, all hot, 5 r + 5 w (20 ops, read ratio 0.5), round trip 10 ms on every message
#   b2-onehot   2 branches, one hot, 5 r + 5 w
#   b4-onehot   4 branches, one hot, 3 r + 3 w (24 ops)
#   b6-onehot   6 branches, one hot, 2 r + 2 w (24 ops)
#   b4-allhot   4 branches, all hot, 3 r + 3 w (24 ops)
#   ops50       2 branches, 13 r + 13 w (52 ops)
#   ops100      2 branches, 25 r + 25 w (100 ops)
#   rtt2        round trip 2 ms (client <-> shim and coordinator <-> shim alike)
#   rtt30       round trip 30 ms
#   ratio25     2 branches, 3 r + 9 w (24 ops, read ratio 0.25)
#   ratio75     2 branches, 9 r + 3 w (24 ops, read ratio 0.75)
#
# Env: SWEEP_OUT (bench-results/sweep-<timestamp>), ONLY, DRY_RUN, SMOKE, and the defaults' variables.
# Starts the MySQL servers once (stopping any running ones first) and stops them at the end.
set -uo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=bench-defaults.env
source "$root/scripts/bench-defaults.env"

# name | extra MicroBench options (after the defaults' workload, so they override it)
configs=(
    "base|"
    "b2-onehot|--branch-skews 0.95,0.5"
    "b4-onehot|--branches 4 --reads 3 --writes 3 --branch-skews 0.95,0.5,0.5,0.5"
    "b6-onehot|--branches 6 --reads 2 --writes 2 --branch-skews 0.95,0.5,0.5,0.5,0.5,0.5"
    "b4-allhot|--branches 4 --reads 3 --writes 3"
    "ops50|--reads 13 --writes 13"
    "ops100|--reads 25 --writes 25"
    "rtt2|--rtt-us 2000"
    "rtt30|--rtt-us 30000"
    "ratio25|--reads 3 --writes 9"
    "ratio75|--reads 9 --writes 3"
)

if [[ "${SMOKE:-0}" == 1 ]]; then
    THREADS_LIST=50
    ADAPTIVE_WARMUP_S=5 ADAPTIVE_MEASURE_S=10 BASE_WARMUP_S=5 BASE_MEASURE_S=10
fi
sweep_out="${SWEEP_OUT:-$root/bench-results/sweep-$(date +%Y%m%d-%H%M%S)$([[ "${SMOKE:-0}" == 1 ]] && echo -smoke)}"
read -r -a workload <<< "$BENCH_WORKLOAD"

selected=()
for c in "${configs[@]}"; do
    name="${c%%|*}"
    if [[ -z "${ONLY:-}" || " $ONLY " == *" $name "* ]]; then
        selected+=("$c")
    fi
done

# expected duration: the windows plus about 45 s of start-up and drain per run (measured on diascld21)
n_threads=$(wc -w <<< "$THREADS_LIST")
per_thread_s=$((ADAPTIVE_WARMUP_S + ADAPTIVE_MEASURE_S + 2 * (BASE_WARMUP_S + BASE_MEASURE_S) + 3 * 45))
total_min=$((${#selected[@]} * n_threads * per_thread_s / 60))
echo "sweep: ${#selected[@]} configurations x $n_threads thread counts ($THREADS_LIST) x 3 systems," \
    "about $((total_min / 60)) h $((total_min % 60)) min -> $sweep_out"
echo

run() {   # prints the command, runs it unless DRY_RUN
    echo "+ $*"
    [[ "${DRY_RUN:-0}" == 1 ]] || "$@"
}

if [[ "${DRY_RUN:-0}" != 1 ]]; then
    mkdir -p "$sweep_out"
    "$root/scripts/bench-dbs.sh" stop mysql || true   # fresh servers with the defaults' lock timeouts
    trap '"$root/scripts/bench-dbs.sh" stop mysql || true' EXIT
fi

status=0
for c in "${selected[@]}"; do
    name="${c%%|*}"
    read -r -a extra <<< "${c#*|}"
    echo "######## $name: ${c#*|}"
    run env OUT="$sweep_out/$name/adaptive" VARIANTS="speculative-mysql@wound-wait" KEEP_DBS=1 REUSE_DBS=1 \
        WARMUP_S="$ADAPTIVE_WARMUP_S" MEASURE_S="$ADAPTIVE_MEASURE_S" THREADS_LIST="$THREADS_LIST" \
        SPEC_MAX_FANOUT=auto SPEC_FANOUT_ARGS="$ADAPTIVE_FANOUT_ARGS" \
        "$root/scripts/bench-final.sh" "${workload[@]}" ${extra[@]+"${extra[@]}"} || status=1
    run env OUT="$sweep_out/$name/base" VARIANTS="speculative-mysql@wound-wait nonspeculative-mysql@wound-wait" \
        KEEP_DBS=1 REUSE_DBS=1 WARMUP_S="$BASE_WARMUP_S" MEASURE_S="$BASE_MEASURE_S" THREADS_LIST="$THREADS_LIST" \
        SPEC_MAX_FANOUT=none SPEC_FANOUT_ARGS= \
        "$root/scripts/bench-final.sh" "${workload[@]}" ${extra[@]+"${extra[@]}"} || status=1
    echo
done
echo "sweep done: $sweep_out (exit $status: 1 if a run warned or failed its audit)"
exit "$status"
