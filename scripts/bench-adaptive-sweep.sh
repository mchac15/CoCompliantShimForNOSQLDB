#!/usr/bin/env bash
# Sweep of the adaptive speculation cap against the speculative and the non-speculative shim, on the
# pilot's workload (scripts/bench-adaptive-pilot.sh) over every (threads, skew) cell: one
# scripts/bench-final.sh sweep, so the variants of a cell run interleaved, rep by rep.
#
#   scripts/bench-adaptive-sweep.sh [MicroBench options...]   # runs (or resumes, with OUT=DIR), then summarizes
#   scripts/bench-adaptive-sweep.sh --summarize DIR
#
# Output, in DIR = bench-results/adaptive-sweep-<timestamp>/: bench-final.sh's files (summary.md,
# summary.csv, the runs and node logs), and adaptive.md: per skew, each variant's peak throughput (best
# thread count) and its gain over the two baselines' peaks; then per cell the tps of every variant and
# how the adaptive lock waits were granted (summed over nodes and reps).
#
# Env: THREADS_LIST ("50 75 100 200 300 400 500 600 700 800 900 1000"), SKEWS ("0.9 0.95 0.99"),
#      REPS (3), ADAPTIVE_VARIANTS ("adaptive-th0.9-tau2000 adaptive-th0.97-tau2000": see
#      scripts/bench-compare.sh), ADAPTIVE_ARGS (the cap's other options, "--spec-prior-mean 0.05
#      --spec-prior-weight 3"), PREPARE_DEADLOCK (wait-die), OUT, and bench-final.sh's.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"

summarize() {
    python3 - "$1" <<'EOF'
import csv, glob, os, sys
from collections import defaultdict

out = sys.argv[1]
if not os.path.exists(os.path.join(out, "summary.csv")):
    sys.exit(f"no summary.csv in {out}: no cell ran")
rows = list(csv.DictReader(open(os.path.join(out, "summary.csv"))))
GRANTS = ("adaptive_speculated", "adaptive_deferred", "adaptive_forced", "adaptive_abandoned")

def counters(t, s, variant):
    acc = dict.fromkeys(GRANTS + ("cascade",), 0)
    for log in glob.glob(os.path.join(out, f"t{t}-skew{s}-nodes", f"rep*-{variant}-s*.log")):
        lines = [l for l in open(log, errors="replace") if l.startswith("stats ")]
        if lines:
            fields = dict(kv.split("=", 1) for kv in lines[-1].split() if "=" in kv)
            for k in acc:
                acc[k] += int(fields.get(k, 0))
    return acc

base = lambda v: v.split("-mysql")[0].split("-pg")[0]
print(f"# Adaptive speculation sweep: {out}\n")
setup = os.path.join(out, "setup.txt")
if os.path.exists(setup):
    print("    " + open(setup).read().replace("\n", "\n    ").rstrip())
extra = os.path.join(out, "adaptive-setup.txt")
print("    " + (open(extra).read().strip() if os.path.exists(extra) else "prepare deadlock: ?") + "\n")

# peaks: max tps over the thread counts, per skew and variant
by_skew = defaultdict(dict)
for r in rows:
    v, tps = r["variant"], float(r["tps"])
    if tps > by_skew[r["skew"]].get(v, (-1, 0))[0]:
        by_skew[r["skew"]][v] = (tps, int(r["threads"]))
threads_all = sorted({int(r["threads"]) for r in rows})
print("## Peak throughput (best thread count; * = at the edge of the thread range)\n")
print("| skew | variant | peak tps | at threads | vs nonspeculative peak | vs speculative peak |")
print("|---|---|---|---|---|---|")
for s in sorted(by_skew):
    peaks = by_skew[s]
    for v, (tps, t) in sorted(peaks.items(), key=lambda kv: -kv[1][0]):
        edge = "*" if t in (threads_all[0], threads_all[-1]) else ""
        # the baseline on the same database, same policy: swap the system part of the name
        ref = lambda b: peaks.get(v.replace(base(v), b, 1), (0,))[0]
        gain = lambda b: f"{100 * (tps / ref(b) - 1):+.1f}%" if ref(b) else ""
        print(f"| {s} | {v} | {tps:.1f} | {t}{edge} | {gain('nonspeculative')} | {gain('speculative')} |")
print()

print("## Per cell\n")
print("Adaptive grants summed over nodes and reps (warm-up included): spec(ulated at once), def(erred, then "
      "granted), forced (a writer ahead was younger), aband(oned: deferred, then the wait ended in an "
      "abort); def % = (def + aband) / all.\n")
print("| threads | skew | variant | tps | abort rate | txn p99 us | cascade aborts | spec | def | forced | aband | def % |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|")
for r in sorted(rows, key=lambda r: (float(r["skew"]), int(r["threads"]), r["variant"])):
    a = counters(r["threads"], r["skew"], r["variant"])
    n = sum(a[k] for k in GRANTS)
    tail = (f"| {a['adaptive_speculated']} | {a['adaptive_deferred']} | {a['adaptive_forced']} "
            f"| {a['adaptive_abandoned']} | {100 * (a['adaptive_deferred'] + a['adaptive_abandoned']) / n:.1f}% |"
            if r["variant"].startswith("adaptive") and n else "| | | | | |")
    print(f"| {r['threads']} | {r['skew']} | {r['variant']} | {float(r['tps']):.1f} "
          f"| {100 * float(r['abort_rate']):.2f}% | {r['txn_p99_us']} | {a['cascade']} " + tail)
EOF
}

if [[ "${1:-}" == "--summarize" ]]; then
    summarize "${2:?usage: $0 --summarize DIR}"
    exit
fi

adaptive_variants="${ADAPTIVE_VARIANTS:-adaptive-th0.9-tau2000 adaptive-th0.97-tau2000}"
variants="nonspeculative-mysql@wound-wait speculative-mysql@wound-wait"
for v in $adaptive_variants; do
    variants+=" $v-mysql@wound-wait"
done
out="${OUT:-$root/bench-results/adaptive-sweep-$(date +%Y%m%d-%H%M%S)}"
export PREPARE_DEADLOCK="${PREPARE_DEADLOCK:-wait-die}"   # read by bench-compare.sh for the nodes
export ADAPTIVE_ARGS="${ADAPTIVE_ARGS:---spec-prior-mean 0.05 --spec-prior-weight 3}"
mkdir -p "$out"
echo "prepare deadlock: $PREPARE_DEADLOCK   adaptive args: $ADAPTIVE_ARGS" > "$out/adaptive-setup.txt"

workload=(--ops-mean 0 --reads 5 --writes 5 --branches 4 --rmw true --get-for-update true
    --pipelined-branches true --think-mean-us 10000 --think-sd-us 5000)
status=0
OUT="$out" VARIANTS="$variants" THREADS_LIST="${THREADS_LIST:-50 75 100 200 300 400 500 600 700 800 900 1000}" \
    SKEWS="${SKEWS:-0.9 0.95 0.99}" REPS="${REPS:-3}" \
    "$root/scripts/bench-final.sh" "${workload[@]}" "$@" || status=1

summarize "$out" | tee "$out/adaptive.md"
echo
echo "adaptive: $out/adaptive.md"
exit "$status"
