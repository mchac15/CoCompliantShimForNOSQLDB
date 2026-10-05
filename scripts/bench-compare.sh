#!/usr/bin/env bash
# Runs the Micro benchmark (bench/, ch.epfl.coshim.bench.MicroBench) through a real Seata TC, once
# per variant with the same options, appends every run to one CSV and prints a comparison.
#
#   scripts/bench-compare.sh [--reps N] [MicroBench options...]
#
# Variants:
#   speculative, nonspeculative, nocc  the shim: SHIMS shim nodes (CoShimNode, one database each, on
#                                      ports NODE_PORT, NODE_PORT+1, ...), started and stopped for
#                                      each run; each node's log, with its final commit/abort
#                                      counters, is kept next to the CSV
#   sonata-mysql, sonata-pg            Sonata: SHIMS MySQL / PostgreSQL servers (scripts/bench-dbs.sh:
#                                      Acta's settings, fsync off) behind Seata's stock XA proxy, with
#                                      sonata.enableGlobalSerializability (Sonata's other settings at
#                                      their defaults). Started once if not running, stopped at the end
#                                      if this script started them; tables are reset before each run
#
# Needs a running Seata TC (default 127.0.0.1:8091; pass --tc host:port otherwise), see README.md,
# and Docker for the Sonata variants.
#
# With no option it runs the "simple test" (50 threads, table size 10000, 2 branches, 2 reads +
# 2 rmw writes per branch, skew 0.9, 10 s warm-up + 30 s measured) on 2 participants.
# Examples: scripts/bench-compare.sh --reps 3 --skew 0.99 --threads 100
#           SHIMS=4 scripts/bench-compare.sh --parallel-branches true --txn-timeout-ms 500
#           VARIANTS="speculative sonata-mysql sonata-pg" scripts/bench-compare.sh
# Env: SHIMS (2), NODE_PORT (7000), LOCK_TIMEOUT_MS (1000), VARIANTS ("speculative nonspeculative"),
#      COSHIM_TOKEN (random), CSV (bench-results/<timestamp>.csv), MVN_FLAGS (e.g. -o), and
#      scripts/bench-dbs.sh's MYSQL_PORT, PG_PORT, ...
# Exit code 1 if any run printed a warning or failed its audit.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
reps=1
if [[ "${1:-}" == "--reps" ]]; then
    reps="$2"
    shift 2
fi
shims="${SHIMS:-2}"
base_port="${NODE_PORT:-7000}"
lock_timeout="${LOCK_TIMEOUT_MS:-1000}"
variants="${VARIANTS:-speculative nonspeculative}"
export COSHIM_TOKEN="${COSHIM_TOKEN:-bench-$RANDOM$RANDOM}"
cd "$root"

tc="127.0.0.1:8091"
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
    if [[ "${args[$i]}" == "--tc" ]]; then
        tc="${args[$((i + 1))]}"
    fi
done
if ! (exec 3<>"/dev/tcp/${tc%:*}/${tc#*:}") 2>/dev/null; then
    echo "no Seata TC at $tc: start one first (see README.md, Benchmark)" >&2
    exit 2
fi

cp_file="$(mktemp)"
trap 'rm -f "$cp_file"' EXIT
mvn -q -B ${MVN_FLAGS:-} compile dependency:build-classpath -pl bench -am -Dmdep.outputFile="$cp_file"
cp="bench/target/classes:$(cat "$cp_file")"

csv="${CSV:-$root/bench-results/$(date +%Y%m%d-%H%M%S).csv}"
logs="${csv%.csv}-nodes"
mkdir -p "$logs"
shim_list=""
for ((s = 0; s < shims; s++)); do
    shim_list+="${shim_list:+,}127.0.0.1:$((base_port + s))/s$s"
done

nodes=()
stop_nodes() {
    for pid in "${nodes[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${nodes[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    nodes=()
}
started_dbs=()
stop_dbs() {
    for kind in "${started_dbs[@]}"; do
        "$root/scripts/bench-dbs.sh" stop "$kind" || true
    done
}
trap 'stop_nodes; stop_dbs; rm -f "$cp_file"' EXIT

# the Sonata variants' databases: started once, before the first run
for variant in $variants; do
    case "$variant" in
        sonata-mysql) kind=mysql ;;
        sonata-pg) kind=postgres ;;
        *) continue ;;
    esac
    if ! docker container inspect "coshim-bench-$kind-0" > /dev/null 2>&1; then
        echo "starting $shims $kind server(s)"
        started_dbs+=("$kind")
        "$root/scripts/bench-dbs.sh" start "$kind" "$shims" > /dev/null
    fi
done

status=0
for rep in $(seq 1 "$reps"); do
    for variant in $variants; do
        case "$variant" in
            sonata-mysql | sonata-pg)
                kind=$([[ $variant == sonata-mysql ]] && echo mysql || echo postgres)
                java -cp "$cp" ch.epfl.coshim.bench.MicroBench --variant "$variant" \
                    --shims "$("$root/scripts/bench-dbs.sh" urls "$kind" "$shims")" --sonata true "$@" \
                    --csv "$csv" --label "rep$rep" || status=1
                echo
                continue
                ;;
        esac
        for ((s = 0; s < shims; s++)); do
            port=$((base_port + s))
            java -cp "$cp" ch.epfl.coshim.net.CoShimNode --port "$port" --shim "$variant" --databases "s$s" \
                --lock-timeout-ms "$lock_timeout" --stats-interval-s 0 > "$logs/rep$rep-$variant-s$s.log" 2>&1 &
            nodes+=($!)
            until (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; do
                sleep 0.2
            done
        done
        java -cp "$cp" ch.epfl.coshim.bench.MicroBench --variant "$variant" --shims "$shim_list" "$@" \
            --csv "$csv" --label "rep$rep" || status=1
        stop_nodes
        grep -h '^stats' "$logs"/rep"$rep"-"$variant"-s*.log | sed 's/^/  node /'
        echo
    done
done

echo "results: $csv (node logs: $logs)"
awk -F, 'NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i }
         { printf "%-6s %-15s %10s %11s %9s %8s %9s %9s %9s %9s  %s\n",
                  $col["label"], $col["variant"], $col["tps"], $col["abort_rate"], $col["timed_out"], $col["unknown"],
                  $col["mean_us"], $col["p50_us"], $col["p99_us"], $col["audit_ok"], $col["branch_failures"] }' "$csv"
exit "$status"
