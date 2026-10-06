#!/usr/bin/env bash
# Compares the shim with Sonata on the same SQL servers: the speculative shim whose store is MySQL /
# PostgreSQL (SqlKvStore, variants speculative-mysql / speculative-pg) against Sonata on those
# servers (sonata-mysql / sonata-pg), over a sweep of skews, and writes a summary of the results.
#
#   scripts/bench-sql-compare.sh [MicroBench options...]     # runs the sweep, then summarizes it
#   scripts/bench-sql-compare.sh --summarize DIR             # summarizes an existing sweep again
#
# Both sides use the same servers (scripts/bench-dbs.sh: Acta's settings, fsync off), the same
# tables, keys, statements and pool size; they differ only in who does the concurrency control (see
# README.md, Benchmark: speculative shim vs Sonata). The servers are started once, before the first
# run, and stopped at the end if this script started them. Each skew is one scripts/bench-compare.sh
# call with --reps REPS, so every variant runs REPS times per skew, interleaved.
#
# Output, in DIR = bench-results/sql-compare-<timestamp>/:
#   skew-<s>.csv, skew-<s>-nodes/   the runs (MicroBench CSV rows) and the shim nodes' logs
#   summary.md                      per skew and variant: throughput, abort rate, latencies over the
#                                   reps; the shim / Sonata ratios per server kind; the shim nodes'
#                                   abort causes. Also printed at the end.
#
# Defaults: skews 0.5 0.9 0.99, 3 reps, 10 threads, 10 s warm-up + 30 s measured, the rest as
# MicroBench's defaults (10000 keys, 2 branches, 2 reads + 2 rmw writes); about 30 min.
# Example: SKEWS="0.9 0.99" REPS=5 LOCK_TIMEOUT_MS=100 scripts/bench-sql-compare.sh --threads 50
# Env: SKEWS ("0.5 0.9 0.99"), REPS (3), THREADS (10), MEASURE_S (30), WARMUP_S (10),
#      VARIANTS ("speculative-mysql sonata-mysql speculative-pg sonata-pg"; add e.g.
#      nonspeculative-mysql), OUT (the output directory), KEEP_DBS (1: leave the servers running),
#      and bench-compare.sh's SHIMS, LOCK_TIMEOUT_MS, NODE_PORT, ...
# Needs a running Seata TC (see README.md) and Docker. Exit code 1 if any run warned or failed its audit.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"

# Prints summary.md for the sweep in $1 (skew-*.csv and skew-*-nodes/).
summarize() {
    local dir="$1"
    local csvs=()
    mapfile -t csvs < <(printf '%s\n' "$dir"/skew-*.csv | sort -V)   # by skew: 0.5, 0.9, 0.99
    if [[ ! -f "${csvs[0]}" ]]; then
        echo "no skew-*.csv in $dir" >&2
        return 1
    fi
    echo "# Speculative shim vs Sonata on the same SQL servers"
    echo
    [[ -f "$dir/setup.txt" ]] && sed 's/^/    /' "$dir/setup.txt" && echo

    echo "## Per skew and variant (mean over reps; tps also min-max)"
    echo
    echo "| skew | variant | reps | tps | tps min-max | abort rate | TC timeouts | mean us | p50 us | p99 us | audits OK | failed branches (sum) |"
    echo "|---|---|---|---|---|---|---|---|---|---|---|---|"
    awk -F, '
        FNR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
        {
            k = $col["skew"] SUBSEP $col["variant"]
            if (!(k in n)) order[++keys] = k
            n[k]++; tps[k] += $col["tps"]; abort[k] += $col["abort_rate"]; to[k] += $col["timed_out"]
            mean[k] += $col["mean_us"]; p50[k] += $col["p50_us"]; p99[k] += $col["p99_us"]
            if (!(k in lo) || $col["tps"] + 0 < lo[k]) lo[k] = $col["tps"] + 0
            if (!(k in hi) || $col["tps"] + 0 > hi[k]) hi[k] = $col["tps"] + 0
            ok[k] += ($col["audit_ok"] == "true" || $col["audit_ok"] == "n/a")
            split($col["branch_failures"], parts, ";")
            for (p in parts) if (split(parts[p], kv, "=") == 2) { fail[k, kv[1]] += kv[2]; causes[kv[1]] }
        }
        END {
            for (j = 1; j <= keys; j++) {
                k = order[j]; split(k, sv, SUBSEP)
                f = ""
                for (c in causes) if ((k, c) in fail) f = f (f == "" ? "" : ", ") c "=" fail[k, c]
                printf "| %s | %s | %d | %.1f | %.1f-%.1f | %.2f%% | %.1f | %.0f | %.0f | %.0f | %d/%d | %s |\n",
                    sv[1], sv[2], n[k], tps[k] / n[k], lo[k], hi[k], 100 * abort[k] / n[k], to[k] / n[k],
                    mean[k] / n[k], p50[k] / n[k], p99[k] / n[k], ok[k], n[k], f
            }
        }' "${csvs[@]}"
    echo

    echo "## Shim / Sonata on the same servers (ratio of the means; tps > 1: the shim is faster, p99 < 1: the shim's tail is shorter)"
    echo
    echo "| skew | shim variant | vs | tps ratio | p99 ratio | abort rate shim / Sonata |"
    echo "|---|---|---|---|---|---|"
    awk -F, '
        FNR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
        {
            k = $col["skew"] SUBSEP $col["variant"]
            if (!(k in n)) order[++keys] = k
            n[k]++; tps[k] += $col["tps"]; p99[k] += $col["p99_us"]; abort[k] += $col["abort_rate"]
        }
        END {
            for (j = 1; j <= keys; j++) {
                k = order[j]; split(k, sv, SUBSEP)
                if (sv[2] ~ /^sonata-/ || sv[2] !~ /-(mysql|pg)$/) continue
                db = sv[2]; sub(/^.*-/, "", db)
                s = sv[1] SUBSEP "sonata-" db
                if (!(s in n)) continue
                printf "| %s | %s | sonata-%s | %s | %s | %.2f%% / %.2f%% |\n", sv[1], sv[2], db,
                    (tps[s] > 0 ? sprintf("%.2f", (tps[k] / n[k]) / (tps[s] / n[s])) : "-"),
                    (p99[s] > 0 ? sprintf("%.2f", (p99[k] / n[k]) / (p99[s] / n[s])) : "-"),
                    100 * abort[k] / n[k], 100 * abort[s] / n[s]
            }
        }' "${csvs[@]}"
    echo

    echo "## Shim nodes: aborts by cause (sum over reps and nodes, warm-up and audit included)"
    echo
    local any=0 csv nodes skew
    for csv in "${csvs[@]}"; do
        nodes="${csv%.csv}-nodes"
        skew="${csv##*/skew-}"
        skew="${skew%.csv}"
        [[ -d $nodes ]] || continue
        # files rep<r>-<variant>-s<i>.log, last line "stats s<i>: commits=.. aborts=.. lock_timeout=.. ..."
        for log in "$nodes"/*.log; do
            [[ -f $log ]] || continue
            any=1
            variant="${log##*/}"
            variant="${variant#rep*-}"
            variant="${variant%-s*.log}"
            grep -h '^stats' "$log" | tail -1 | sed "s/^stats [^:]*://; s/^/$skew $variant/"
        done
    done | awk '
        {
            k = $1 " | " $2
            if (!(k in seen)) { seen[k]; order[++keys] = k }
            for (i = 3; i <= NF; i++) if (split($i, kv, "=") == 2) {
                if (!(kv[1] in known)) { known[kv[1]]; names[++m] = kv[1] }
                v[k, kv[1]] += kv[2]
            }
        }
        END {
            if (keys == 0) exit
            printf "| skew | variant |"; for (i = 1; i <= m; i++) printf " %s |", names[i]; print ""
            printf "|---|---|";        for (i = 1; i <= m; i++) printf "---|"; print ""
            for (j = 1; j <= keys; j++) {
                printf "| %s |", order[j]
                for (i = 1; i <= m; i++) printf " %d |", v[order[j], names[i]]
                print ""
            }
        }'
    echo
    echo "Raw runs: $dir/skew-*.csv; node logs: $dir/skew-*-nodes/."
}

if [[ "${1:-}" == "--summarize" ]]; then
    summarize "${2:?usage: $0 --summarize DIR}"
    exit
fi

skews="${SKEWS:-0.5 0.9 0.99}"
reps="${REPS:-3}"
variants="${VARIANTS:-speculative-mysql sonata-mysql speculative-pg sonata-pg}"
shims="${SHIMS:-2}"
out="${OUT:-$root/bench-results/sql-compare-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$out"

# the servers every variant needs, started once here so bench-compare.sh reuses them for every skew
# (it would otherwise start and stop them, and refill Sonata's dummy table, per call)
kinds=()
for variant in $variants; do
    case "$variant" in
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
        echo "reusing the running $kind server(s)"
    else
        echo "starting $shims $kind server(s)"
        "$root/scripts/bench-dbs.sh" start "$kind" "$shims" > /dev/null
        started+=("$kind")
    fi
done

{
    echo "date:      $(date '+%Y-%m-%d %H:%M')   commit: $(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo '?')"
    echo "variants:  $variants"
    echo "skews:     $skews   reps: $reps   participants: $shims"
    echo "options:   --threads ${THREADS:-10} --warmup-s ${WARMUP_S:-10} --measure-s ${MEASURE_S:-30} $*"
    echo "shim:      lock timeout ${LOCK_TIMEOUT_MS:-1000} ms"
    echo "servers:   scripts/bench-dbs.sh (MySQL ${MYSQL_IMAGE:-mysql:8.4}, PG ${POSTGRES_IMAGE:-postgres:16}, fsync off)"
} > "$out/setup.txt"
cat "$out/setup.txt"
echo

status=0
for skew in $skews; do
    echo "=== skew $skew"
    CSV="$out/skew-$skew.csv" VARIANTS="$variants" \
        "$root/scripts/bench-compare.sh" --reps "$reps" --skew "$skew" \
        --threads "${THREADS:-10}" --warmup-s "${WARMUP_S:-10}" --measure-s "${MEASURE_S:-30}" "$@" || status=1
    echo
done

summarize "$out" | tee "$out/summary.md"
echo
echo "summary: $out/summary.md"
exit "$status"
