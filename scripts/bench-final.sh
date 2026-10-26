#!/usr/bin/env bash
# The final micro benchmark: the speculative and the non-speculative shim (under both deadlock
# policies) and Sonata, all on the same MySQL / PostgreSQL servers, over every combination of thread
# count and skew, with the realistic workload, and a summary of the results.
#
#   scripts/bench-final.sh [MicroBench options...]     # runs the sweep (or resumes it), then summarizes
#   scripts/bench-final.sh --summarize DIR             # summarizes an existing sweep again
#
# Every (threads, skew) cell is one scripts/bench-compare.sh call with --reps REPS, so the variants
# of a cell run REPS times each, interleaved. A cell that finished is skipped when the sweep is run
# again with OUT=DIR (resume after an interruption).
#
# Timeouts are large on purpose: local deadlocks are handled by wait-die / wound-wait (the shim) and
# by the servers' deadlock detection (Sonata), so the timeouts only break cross-database deadlocks.
# Every lock wait gives up after LOCK_TIMEOUT_MS (the shim nodes, and the servers'
# innodb_lock_wait_timeout / lock_timeout), and the TC's global timeout TXN_TIMEOUT_MS is a backstop
# above it.
#
# Workload: MicroBench's defaults (10000 keys per table, 2 branches in order, rmw writes) with the
# realistic options --ops-mean 3 --think-mean-us 2000 --think-sd-us 1000; extra options are passed on.
#
# Output, in DIR = bench-results/final-<timestamp>/:
#   t<T>-skew<S>.csv, txns-t<T>-skew<S>.csv, t<T>-skew<S>-nodes/   the runs, their txns, node logs
#   summary.md, summary.csv   per threads, skew and variant: throughput (mean over reps, min-max);
#                             per-op latency p50 / p99 and per-txn execution p50 / p99 (committed txns,
#                             retries included, think time excluded, pooled over the reps); retries
#                             per txn p50 / p99 (pooled); abort rate; audits
#
# Defaults: threads 20 50 100 200, skews 0.9 0.99, 3 reps of 10 s warm-up + 30 s measured, 2
# participants, 10 variants: about 4 h.
# Env: THREADS_LIST ("20 50 100 200"), SKEWS ("0.9 0.99"), REPS (3), WARMUP_S (10), MEASURE_S (30),
#      LOCK_TIMEOUT_MS (30000), TXN_TIMEOUT_MS (60000), VARIANTS (below), OUT, KEEP_DBS (1: leave the
#      servers running), REUSE_DBS (1: use servers already running, whatever their lock timeouts),
#      and bench-compare.sh's SHIMS, NODE_PORT, ...
# Needs a running Seata TC (see README.md) and Docker. Exit code 1 if any run warned or failed its audit.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"

# Writes summary.md (to stdout) and summary.csv for the sweep in $1.
summarize() {
    python3 - "$1" <<'EOF'
import csv, glob, os, re, statistics, sys

out = sys.argv[1]
runs, txns = {}, {}
order = []
def cell_key(name):
    m = re.match(r"(?:txns-)?t(\d+)-skew([\d.]+)\.csv$", os.path.basename(name))
    return (int(m.group(1)), float(m.group(2))) if m else None

for f in sorted(glob.glob(os.path.join(out, "t[0-9]*-skew*.csv")), key=cell_key):   # not txns-*
    for r in csv.DictReader(open(f)):
        k = (int(r["threads"]), float(r["skew"]), r["variant"])
        if k not in runs:
            order.append(k)
        runs.setdefault(k, []).append(r)
for f in glob.glob(os.path.join(out, "txns-t*-skew*.csv")):
    t, _ = cell_key(f)
    for r in csv.DictReader(open(f)):
        txns.setdefault((t, float(r["skew"]), r["variant"]), []).append(r)
if not runs:
    sys.exit("no t*-skew*.csv in " + out)

def pct(values, p):
    # nearest rank, as BenchResult
    v = sorted(values)
    return v[min(len(v) - 1, len(v) * p // 100)] if v else -1

rows = []
for k in sorted(order, key=lambda k: (k[0], k[1], order.index(k))):
    rs, ts = runs[k], txns.get(k, [])
    tps = [float(r["tps"]) for r in rs]
    committed = [t for t in ts if t["outcome"] == "COMMITTED"]
    op = [int(t["op_latency_us"]) for t in committed]
    active = [int(t["active_us"]) for t in committed]
    retries = [int(t["attempts"]) - 1 for t in ts]
    rows.append(dict(threads=k[0], skew=k[1], variant=k[2], reps=len(rs),
        tps=statistics.mean(tps), tps_min=min(tps), tps_max=max(tps),
        op_p50_us=pct(op, 50), op_p99_us=pct(op, 99), txn_p50_us=pct(active, 50), txn_p99_us=pct(active, 99),
        retries_p50=pct(retries, 50), retries_p99=pct(retries, 99),
        abort_rate=statistics.mean(float(r["abort_rate"]) for r in rs),
        tc_timeouts=sum(int(r["timed_out"]) for r in rs), unknown=sum(int(r["unknown"]) for r in rs),
        unfinished=sum(t["outcome"] == "UNFINISHED" for t in ts),
        audits_ok=sum(r["audit_ok"] in ("true", "n/a") for r in rs)))

with open(os.path.join(out, "summary.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0]))
    w.writeheader()
    w.writerows(rows)

print("# Final micro benchmark: shim vs Sonata on the same SQL servers\n")
setup = os.path.join(out, "setup.txt")
if os.path.exists(setup):
    print("".join("    " + l for l in open(setup)))
print("Latencies: committed txns, from the first attempt's begin to the commit, retries included, think time")
print("excluded; per op = that divided by the txn's ops. Latency and retry percentiles are pooled over the")
print("reps; tps and abort rate are means over the reps.\n")
for (t, s) in sorted({(r["threads"], r["skew"]) for r in rows}):
    print(f"## {t} threads, skew {s}\n")
    print("| variant | reps | tps | tps min-max | op p50 us | op p99 us | txn p50 us | txn p99 us "
          "| retries p50 | retries p99 | abort rate | TC timeouts | unfinished | audits OK |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        if (r["threads"], r["skew"]) != (t, s):
            continue
        print(f"| {r['variant']} | {r['reps']} | {r['tps']:.1f} | {r['tps_min']:.1f}-{r['tps_max']:.1f} "
              f"| {r['op_p50_us']} | {r['op_p99_us']} | {r['txn_p50_us']} | {r['txn_p99_us']} "
              f"| {r['retries_p50']} | {r['retries_p99']} | {100 * r['abort_rate']:.2f}% | {r['tc_timeouts']} "
              f"| {r['unfinished']} | {r['audits_ok']}/{r['reps']} |")
    print()
print(f"Raw runs: {out}/t*-skew*.csv, txns: {out}/txns-t*-skew*.csv, node logs: {out}/t*-skew*-nodes/;")
print(f"this table as CSV: {out}/summary.csv.")
EOF
}

if [[ "${1:-}" == "--summarize" ]]; then
    summarize "${2:?usage: $0 --summarize DIR}"
    exit
fi

for arg in "$@"; do
    if [[ $arg =~ ^--?([A-Z_]+)= ]]; then
        echo "$arg: ${BASH_REMATCH[1]} is an environment variable, put it before the command:" \
            "${BASH_REMATCH[1]}=... $0 [MicroBench options]" >&2
        exit 2
    fi
done

threads_list="${THREADS_LIST:-20 50 100 200}"
skews="${SKEWS:-0.9 0.99}"
reps="${REPS:-3}"
lock_timeout="${LOCK_TIMEOUT_MS:-30000}"
txn_timeout="${TXN_TIMEOUT_MS:-60000}"
variants="${VARIANTS:-speculative-mysql@wait-die speculative-mysql@wound-wait nonspeculative-mysql@wait-die \
nonspeculative-mysql@wound-wait sonata-mysql speculative-pg@wait-die speculative-pg@wound-wait \
nonspeculative-pg@wait-die nonspeculative-pg@wound-wait sonata-pg}"
shims="${SHIMS:-2}"
out="${OUT:-$root/bench-results/final-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$out"

# the servers, started once with the sweep's lock timeout (in seconds, rounded up)
db_lock_timeout_s=$(((lock_timeout + 999) / 1000))
kinds=()
for variant in $variants; do
    case "${variant%@*}" in
        *-mysql) kind=mysql ;;
        *-pg) kind=postgres ;;
        *) continue ;;
    esac
    [[ " ${kinds[*]} " == *" $kind "* ]] || kinds+=("$kind")
done
started=()
stop_started() {
    if [[ "${KEEP_DBS:-0}" != 1 ]]; then
        for kind in "${started[@]}"; do
            "$root/scripts/bench-dbs.sh" stop "$kind" || true
        done
    fi
}
trap stop_started EXIT
for kind in "${kinds[@]}"; do
    if docker container inspect "coshim-bench-$kind-0" > /dev/null 2>&1; then
        if [[ "${REUSE_DBS:-0}" != 1 ]]; then
            echo "the $kind servers are already running, maybe with other lock timeouts: stop them" \
                "(scripts/bench-dbs.sh stop $kind) or set REUSE_DBS=1" >&2
            exit 2
        fi
        echo "reusing the running $kind server(s)"
    else
        echo "starting $shims $kind server(s), lock waits ${db_lock_timeout_s} s"
        MYSQL_LOCK_WAIT_TIMEOUT_S="$db_lock_timeout_s" PG_LOCK_TIMEOUT="${db_lock_timeout_s}s" \
            "$root/scripts/bench-dbs.sh" start "$kind" "$shims" > /dev/null
        started+=("$kind")
    fi
done

workload=(--ops-mean 3 --think-mean-us 2000 --think-sd-us 1000)
if [[ ! -f "$out/setup.txt" ]]; then
    {
        echo "date:      $(date '+%Y-%m-%d %H:%M')   commit: $(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo '?')"
        echo "variants:  $variants"
        echo "threads:   $threads_list   skews: $skews   reps: $reps   participants: $shims"
        echo "options:   --warmup-s ${WARMUP_S:-10} --measure-s ${MEASURE_S:-30} --txn-timeout-ms $txn_timeout ${workload[*]} $*"
        echo "timeouts:  lock waits $lock_timeout ms (shim nodes; servers ${db_lock_timeout_s} s), TC $txn_timeout ms"
        echo "servers:   scripts/bench-dbs.sh (MySQL ${MYSQL_IMAGE:-mysql:8.4}, PG ${POSTGRES_IMAGE:-postgres:16}, fsync off)"
    } > "$out/setup.txt"
fi
cat "$out/setup.txt"
echo

status=0
for threads in $threads_list; do
    for skew in $skews; do
        cell="t$threads-skew$skew"
        if [[ -f "$out/$cell.done" ]]; then
            echo "=== $threads threads, skew $skew: done, skipped"
            continue
        fi
        echo "=== $threads threads, skew $skew"
        rm -rf "$out/$cell.csv" "$out/txns-$cell.csv" "$out/$cell-nodes"   # a cell cut short restarts
        if CSV="$out/$cell.csv" VARIANTS="$variants" LOCK_TIMEOUT_MS="$lock_timeout" SHIMS="$shims" \
            "$root/scripts/bench-compare.sh" --reps "$reps" --threads "$threads" --skew "$skew" \
            --warmup-s "${WARMUP_S:-10}" --measure-s "${MEASURE_S:-30}" --txn-timeout-ms "$txn_timeout" \
            "${workload[@]}" "$@"; then
            touch "$out/$cell.done"
        else
            status=1
            [[ -f "$out/$cell.csv" ]] && touch "$out/$cell.done"   # ran, but a run warned: keep it
        fi
        echo
    done
done

summarize "$out" | tee "$out/summary.md"
echo
echo "summary: $out/summary.md"
exit "$status"
