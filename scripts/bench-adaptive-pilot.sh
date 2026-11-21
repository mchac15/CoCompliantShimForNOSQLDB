#!/usr/bin/env bash
# Pilot of the adaptive speculation cap: one (threads, skew) cell, the speculative and the
# non-speculative shim, then the adaptive one over a grid of theta x tau, to pick the cap's
# parameters before a full sweep. Every run is a scripts/bench-final.sh call (same timeouts, same
# servers), into its own folder; pilot.md compares them.
#
#   scripts/bench-adaptive-pilot.sh [MicroBench options...]   # extra options override the workload
#   scripts/bench-adaptive-pilot.sh --summarize DIR
#
# Workload: 4 branches, pipelined, 5 reads + 5 rmw writes each (10 ops), get-for-update on, think time
# N(10 ms, 5 ms) before every op but the first and before the commit, on MySQL. The shim nodes run
# prepare() wait-die (--prepare-deadlock wait-die): without it, two branches that two shims order
# oppositely deadlock in prepare() until the TC's timeout.
#
# Output, in DIR = bench-results/adaptive-pilot-<timestamp>/:
#   baseline/                     bench-final.sh's folder of the speculative and non-speculative runs
#   adaptive-th<X>-tau<T>ms/      one per cap configuration
#   pilot.md                      per configuration: tps, abort rate, latencies, cascade aborts, net
#                                 saved, and how the adaptive lock waits were granted (speculated /
#                                 deferred / forced, summed over nodes and reps)
#
# Env: THREADS (200), SKEW (0.95), REPS (1), THETAS ("0.3 0.5 0.7 0.9"), TAUS_MS ("500 2000"),
#      PRIOR_MEAN (0.05), PRIOR_WEIGHT (3), PREPARE_DEADLOCK (wait-die | none, default wait-die), OUT,
#      and bench-final.sh's WARMUP_S, MEASURE_S,
#      LOCK_TIMEOUT_MS, TXN_TIMEOUT_MS, SHIMS, ... The MySQL servers are started once if not running
#      (and stopped at the end then); servers already running are reused as they are.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"

# Writes pilot.md (to stdout) for the pilot in $1.
summarize() {
    python3 - "$1" <<'EOF'
import csv, glob, os, sys

out = sys.argv[1]
STATS = ("commits", "aborts", "cascade", "adaptive_speculated", "adaptive_deferred", "adaptive_forced")
rows = []
for d in sorted(glob.glob(os.path.join(out, "*", "summary.csv"))):
    config = os.path.basename(os.path.dirname(d))
    for r in csv.DictReader(open(d)):
        # the nodes' last stats line: the final counters, summed over nodes and reps
        acc = dict.fromkeys(STATS, 0)
        cell = f"t{r['threads']}-skew{r['skew']}-nodes"
        for log in glob.glob(os.path.join(os.path.dirname(d), cell, f"rep*-{r['variant']}-s*.log")):
            lines = [l for l in open(log, errors="replace") if l.startswith("stats ")]
            if lines:
                fields = dict(kv.split("=", 1) for kv in lines[-1].split() if "=" in kv)
                for k in STATS:
                    acc[k] += int(fields.get(k, 0))
        rows.append((config, r, acc))
if not rows:
    sys.exit("no */summary.csv in " + out)

print(f"# Adaptive speculation pilot: {out}\n")
setup = glob.glob(os.path.join(out, "baseline", "setup.txt")) or glob.glob(os.path.join(out, "*", "setup.txt"))
if setup:
    print("    " + open(setup[0]).read().replace("\n", "\n    ").rstrip())
pd = os.path.join(out, "prepare-deadlock.txt")
print("    " + (open(pd).read().strip() if os.path.exists(pd) else "prepare deadlock: ?") + "\n")
print("Node counters summed over nodes and reps (warm-up included): cascade = aborts by cascade; "
      "adaptive = how the adaptive lock waits that had uncommitted writers ahead were granted: "
      "spec(ulated at once), def(erred: waited for writers to commit first), forced (a writer ahead was "
      "younger); def % = deferred share of those.\n")
print("| config | variant | threads | skew | tps | tps min-max | abort rate | op p99 us | txn p99 us "
      "| cascade aborts | net saved us | adaptive spec | adaptive def | adaptive forced | def % |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
best = {}
for config, r, a in rows:
    granted = a["adaptive_speculated"] + a["adaptive_deferred"] + a["adaptive_forced"]
    adaptive = r["variant"].startswith("adaptive")
    print(f"| {config} | {r['variant']} | {r['threads']} | {r['skew']} | {float(r['tps']):.1f} "
          f"| {r['tps_min']}-{r['tps_max']} | {100 * float(r['abort_rate']):.2f}% | {r['op_p99_us']} "
          f"| {r['txn_p99_us']} | {a['cascade']} | {r['net_saved_us']} "
          + (f"| {a['adaptive_speculated']} | {a['adaptive_deferred']} | {a['adaptive_forced']} "
             f"| {100 * a['adaptive_deferred'] / granted:.1f}% |" if adaptive and granted else "| | | | |"))
    key = (r["threads"], r["skew"])
    if float(r["tps"]) > best.get(key, ("", "", -1))[2]:
        best[key] = (config, r["variant"], float(r["tps"]))
print()
for (t, s), (config, variant, tps) in sorted(best.items()):
    print(f"best at {t} threads, skew {s}: {config} {variant}, {tps:.1f} tps")
EOF
}

if [[ "${1:-}" == "--summarize" ]]; then
    summarize "${2:?usage: $0 --summarize DIR}"
    exit
fi

threads="${THREADS:-200}"
skew="${SKEW:-0.95}"
reps="${REPS:-1}"
thetas="${THETAS:-0.3 0.5 0.7 0.9}"
taus_ms="${TAUS_MS:-500 2000}"
prior_mean="${PRIOR_MEAN:-0.05}"
prior_weight="${PRIOR_WEIGHT:-3}"
out="${OUT:-$root/bench-results/adaptive-pilot-$(date +%Y%m%d-%H%M%S)}"
export PREPARE_DEADLOCK="${PREPARE_DEADLOCK:-wait-die}"   # read by bench-compare.sh for the nodes
mkdir -p "$out"
echo "prepare deadlock: $PREPARE_DEADLOCK" > "$out/prepare-deadlock.txt"

workload=(--ops-mean 0 --reads 5 --writes 5 --branches 4 --rmw true --get-for-update true
    --pipelined-branches true --think-mean-us 10000 --think-sd-us 5000)

if ! docker container inspect coshim-bench-mysql-0 > /dev/null 2>&1; then
    # the first bench-final.sh call starts them (with its lock timeout); stop them at the end
    trap '"$root/scripts/bench-dbs.sh" stop mysql || true' EXIT
fi

# one bench-final.sh run of a single cell: $1 = folder, $2 = variants, the rest = env assignments
run() {
    local dir="$1" variants="$2"
    shift 2
    echo "##### $dir: $variants $*"
    env OUT="$out/$dir" VARIANTS="$variants" THREADS_LIST="$threads" SKEWS="$skew" REPS="$reps" \
        KEEP_DBS=1 REUSE_DBS=1 "$@" "$root/scripts/bench-final.sh" "${workload[@]}" ${extra[@]+"${extra[@]}"} || status=1
    echo
}

extra=("$@")
status=0
run baseline "speculative-mysql@wound-wait nonspeculative-mysql@wound-wait"
for tau in $taus_ms; do
    for theta in $thetas; do
        run "adaptive-th$theta-tau${tau}ms" "adaptive-mysql@wound-wait" \
            ADAPTIVE_ARGS="--spec-theta $theta --spec-tau-ms $tau --spec-prior-mean $prior_mean --spec-prior-weight $prior_weight"
    done
done

summarize "$out" | tee "$out/pilot.md"
echo
echo "pilot: $out/pilot.md"
exit "$status"
