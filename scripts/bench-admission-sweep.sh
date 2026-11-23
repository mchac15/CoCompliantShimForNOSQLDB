#!/usr/bin/env bash
# Sweep of speculation admission (CoShimNode --shim admission: each txn speculates or not, decided at
# its start from the recent share of speculating txns that cascades aborted) against the speculative
# and the non-speculative shim, over every (threads, skew) cell: one scripts/bench-final.sh sweep, so
# the variants of a cell run interleaved, rep by rep.
#
#   scripts/bench-admission-sweep.sh [MicroBench options...]   # runs (or resumes, with OUT=DIR), then summarizes
#   scripts/bench-admission-sweep.sh --summarize DIR
#
# Workload (MySQL): 2 branches, pipelined, each 5 reads + 5 rmw writes (get-for-update), 100000 keys
# per table, think time N(10 ms, 5 ms) before every op but the first and before the commit; the shim
# nodes run wound-wait in lock() and wait-die in prepare() (cross-branch deadlocks). This is where the
# speculative shim peaks above the non-speculative one, then collapses under load (cascades). Extra
# options override it.
#
# Output, in DIR = bench-results/admission-sweep-<timestamp>/: bench-final.sh's files (summary.md,
# summary.csv, the runs and node logs), and admission.md: per skew, each variant's peak throughput
# (best thread count) and its gain over the two baselines' peaks; then per cell the tps of every
# variant and, for the admission ones, the nodes' final admission state (summed over nodes and reps:
# txns admitted speculative / as probes / non-speculative, forced grants, on/off switches, and the
# cascade share of speculating branches by number of dependencies k).
# Plots: scripts/bench-admission-plot.py DIR.
#
# Env: THREADS_LIST ("50 100 200 400 600 800 1000"), SKEWS ("0.9 0.95 0.99"), REPS (2),
#      ADMISSION_VARIANTS ("admission-auto admission-r0.15": see scripts/bench-compare.sh for -w / -reach),
#      ADMISSION_ARGS (the other admission options, default CoShimNode's), PREPARE_DEADLOCK (wait-die),
#      OUT, and bench-final.sh's.
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
SUMS = ("admitted_speculative", "admitted_nonspeculative", "forced_grants")
MEANS = ("p", "r", "r_star", "dependents_mean", "fanout_mean", "e_ratio")

def last_line(log, prefix):
    lines = [l for l in open(log, errors="replace") if l.startswith(prefix)]
    return dict(kv.split("=", 1) for kv in lines[-1].split() if "=" in kv) if lines else None

def admission(t, s, variant):
    acc = dict.fromkeys(SUMS, 0)
    k = defaultdict(lambda: [0, 0])
    means, cascades = defaultdict(list), 0
    for log in glob.glob(os.path.join(out, f"t{t}-skew{s}-nodes", f"rep*-{variant}-s*.log")):
        st = last_line(log, "stats ")
        if st:
            cascades += int(st.get("cascade", 0))
        f = last_line(log, "admission ")
        if not f:
            continue
        for key in SUMS:
            acc[key] += int(f.get(key, 0))
        for key in MEANS:
            if key in f:
                means[key].append(float(f[key]))
        for key in ("k1", "k2", "k3plus"):
            n, c = f.get(key, "0/0").split("/")
            k[key][0] += int(n)
            k[key][1] += int(c)
    return acc, k, means, cascades

base = lambda v: v.split("-mysql")[0].split("-pg")[0]
print(f"# Speculation admission sweep: {out}\n")
for name in ("setup.txt", "admission-setup.txt"):
    f = os.path.join(out, name)
    if os.path.exists(f):
        print("    " + open(f).read().replace("\n", "\n    ").rstrip())
print()

by_skew = defaultdict(dict)
for r in rows:
    v, tps = r["variant"], float(r["tps"])
    if tps > by_skew[r["skew"]].get(v, (-1, 0))[0]:
        by_skew[r["skew"]][v] = (tps, int(r["threads"]))
threads_all = sorted({int(r["threads"]) for r in rows})
print("## Peak throughput (best thread count; * = at the edge of the thread range)\n")
print("| skew | variant | peak tps | at threads | vs nonspeculative peak | vs speculative peak |")
print("|---|---|---|---|---|---|")
for s in sorted(by_skew, key=float):
    peaks = by_skew[s]
    for v, (tps, t) in sorted(peaks.items(), key=lambda kv: -kv[1][0]):
        edge = "*" if t in (threads_all[0], threads_all[-1]) else ""
        ref = lambda b: peaks.get(v.replace(base(v), b, 1), (0,))[0]
        gain = lambda b: f"{100 * (tps / ref(b) - 1):+.1f}%" if ref(b) else ""
        print(f"| {s} | {v} | {tps:.1f} | {t}{edge} | {gain('nonspeculative')} | {gain('speculative')} |")
print()

print("## Per cell\n")
print("Admission counters at the end of the runs, summed over nodes and reps (warm-up included): spec / "
      "non-spec = txns admitted speculative / non-speculative; forced = grants a non-speculative txn took "
      "speculatively (deadlock guard); p, r, r* = the share admitted speculative, the cascade share and the "
      "threshold as of the last outcome (mean over nodes and reps: not the values during the run); deps / "
      "fan-out = mean reach of the commits / cascades; E s/n = ratio mode's E_spec / E_nonspec as of the last "
      "outcome; k1 / k2 / k3+ = cascade share of the speculating branches that depended on 1 / 2 / 3+ "
      "uncommitted txns.\n")
print("| threads | skew | variant | tps | abort rate | txn p99 us | cascade aborts | spec | non-spec | forced "
      "| p | r | r* | deps | fan-out | E s/n | k1 | k2 | k3+ |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
for r in sorted(rows, key=lambda r: (float(r["skew"]), int(r["threads"]), r["variant"])):
    a, k, means, cascades = admission(r["threads"], r["skew"], r["variant"])
    share = lambda key: f"{100 * k[key][1] / k[key][0]:.1f}% of {k[key][0]}" if k[key][0] else ""
    mean = lambda key, fmt: fmt.format(sum(means[key]) / len(means[key])) if means[key] else ""
    tail = (f"| {a['admitted_speculative']} | {a['admitted_nonspeculative']} | {a['forced_grants']} "
            f"| {mean('p', '{:.2f}')} | {mean('r', '{:.3f}')} | {mean('r_star', '{:.3f}')} "
            f"| {mean('dependents_mean', '{:.2f}')} | {mean('fanout_mean', '{:.2f}')} | {mean('e_ratio', '{:.2f}')} "
            f"| {share('k1')} | {share('k2')} | {share('k3plus')} |"
            if means else "| | | | | | | | | | | | |")
    print(f"| {r['threads']} | {r['skew']} | {r['variant']} | {float(r['tps']):.1f} "
          f"| {100 * float(r['abort_rate']):.2f}% | {r['txn_p99_us']} | {cascades} " + tail)
EOF
}

if [[ "${1:-}" == "--summarize" ]]; then
    summarize "${2:?usage: $0 --summarize DIR}"
    exit
fi

admission_variants="${ADMISSION_VARIANTS:-admission-auto admission-r0.15}"
variants="nonspeculative-mysql@wound-wait speculative-mysql@wound-wait"
for v in $admission_variants; do
    variants+=" $v-mysql@wound-wait"
done
out="${OUT:-$root/bench-results/admission-sweep-$(date +%Y%m%d-%H%M%S)}"
export PREPARE_DEADLOCK="${PREPARE_DEADLOCK:-wait-die}"   # read by bench-compare.sh for the nodes
export ADMISSION_ARGS="${ADMISSION_ARGS:-}"
mkdir -p "$out"
echo "prepare deadlock: $PREPARE_DEADLOCK   admission args: ${ADMISSION_ARGS:-(defaults)}" > "$out/admission-setup.txt"

workload=(--ops-mean 0 --reads 5 --writes 5 --branches 2 --table-size 100000 --rmw true --get-for-update true
    --pipelined-branches true --think-mean-us 10000 --think-sd-us 5000)
status=0
OUT="$out" VARIANTS="$variants" THREADS_LIST="${THREADS_LIST:-50 100 200 400 600 800 1000}" \
    SKEWS="${SKEWS:-0.9 0.95 0.99}" REPS="${REPS:-2}" \
    "$root/scripts/bench-final.sh" "${workload[@]}" "$@" || status=1

summarize "$out" | tee "$out/admission.md"
echo
echo "admission: $out/admission.md"
exit "$status"
