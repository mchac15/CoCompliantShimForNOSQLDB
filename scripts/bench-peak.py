#!/usr/bin/env python3
"""Finds the peak throughput of the speculative and the non-speculative shim, and how they get there.

    scripts/bench-peak.py [run] [options] [-- MicroBench options...]   # searches (resumes with --out DIR)
    scripts/bench-peak.py summarize DIR                                # peak.md, peak.csv, points.csv
    scripts/bench-peak.py plot DIR                                     # peak-curves.png, timeseries-<db>.png,
                                                                       # budget-<db>.png
    scripts/bench-peak.py stalls DIR                                   # stalls.csv: throughput dips vs GC, system

For every server kind x skew x system (speculative / non-speculative shim, wound-wait, on MySQL /
PostgreSQL; no Sonata), it searches the thread count with the highest mean throughput:

  1. seeds: --seeds (50 100 200) threads;
  2. while the best point is the lowest (highest) one measured, halve (double) it, within
     [--min-threads, --max-threads];
  3. then probe the middle of each side of the best point whose gap to its measured neighbor is
     more than --tol x best threads (0.25: 25 threads around 100), until neither is.
With --threads T...: no search, exactly those thread counts for every config (same points for both
systems, to compare them at equal load).

Every point is --reps (3) runs of scripts/bench-compare.sh (one variant; all of them with the workload
of scripts/bench-final.sh: --ops-mean 3, 2 ms think time, random op order, lock waits 30 s on the
nodes and the servers, TC 60 s), and the search goes by their mean throughput, each run's being the
median of its per-second samples in the measurement window: a global stall does not decide a peak (the
raw mean is kept as tps_raw). The searches advance round-robin
(speculative and non-speculative of one config next to each other in time). A finished point is
never measured again: rerun with --out DIR to resume after an interruption.

Each run also records its throughput over time (MicroBench --sample-ms: commits and aborts every
second, warm-up and drain included, in timeseries-t<T>.csv) and the shim nodes log their counters
every --stats-interval-s (1) s (stats / speculation lines in t<T>-nodes/). Per-commit figures use the
counters at the last second the workload was active, since the audit's read commits come after it.

Diagnostics of the stalls: GC and safepoint logs of the client and the nodes (t<T>-nodes/*.gc), and in
diag/ vmstat and the TC's GC counters (jstat), all on the wall clock; `stalls` lines up each dip with them.

Output, in DIR = bench-results/peak-<timestamp>/:
  <db>-skew<s>-<system>/t<T>.csv, txns-t<T>.csv, timeseries-t<T>.csv, t<T>-nodes/   one point
  points.csv     every point: threads, tps mean / min / max / CV, abort rate, op latency, and per branch
                 commit: saved, aborts by cause, and the time budget (below)
  peak.csv, peak.md   per config: the peak (threads, tps), whether it is bracketed by measured
                 points on both sides, the runner-up; speculative vs non-speculative at their peaks
  peak-curves.png, timeseries-<db>.png   (with matplotlib) throughput against threads, and over time
                 at each peak
  budget-<db>.png   where the time of a branch commit goes, speculative vs non-speculative, per thread
                 count: a committed branch's active time (work and think time), lock wait and prepare
                 wait, plus the lifetime of the aborted branches it took, by abort cause. Post-vote wait
                 is in peak.md only: a prepared branch waits while its thread runs the next branch, so it
                 is not extra thread time

Needs a running Seata TC (see README.md) and Docker; plot needs matplotlib.
Example: scripts/bench-peak.py --skews 0.9 0.95 --reps 3
"""

import argparse
import csv
import glob
import os
import re
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SYSTEMS = ("speculative", "nonspeculative")
DB_KINDS = {"mysql": "mysql", "pg": "postgres"}
WORKLOAD = ["--ops-mean", "3", "--think-mean-us", "2000", "--think-sd-us", "1000"]
SECONDS_PER_RUN_OVERHEAD = 45   # node start, drain, audit, JVM: measured on diascld21


def variant(db, system):
    return f"{system}-{db}@wound-wait"


def config_dir(out, db, skew, system):
    return out / f"{db}-skew{skew:g}-{system}"


# ---- one point ----

def read_point(d, threads):
    """The runs of one point: list of dicts (rows of t<T>.csv), [] if none."""
    f = d / f"t{threads}.csv"
    return list(csv.DictReader(open(f))) if f.exists() else []


def robust_tps(d, threads):
    """Per run: the median of its per-second samples in the measurement window, or its raw tps without samples."""
    f = d / f"timeseries-t{threads}.csv"
    reps = {}
    if f.exists():
        for r in csv.DictReader(open(f)):
            if r["phase"] == "measure":
                reps.setdefault(r["label"], []).append(float(r["tps"]))
    # the last sample may cover a partial interval
    medians = [statistics.median(v[:-1] or v) for v in reps.values()]
    return medians or [float(r["tps"]) for r in read_point(d, threads)]


def measure(args, out, db, skew, system, threads):
    """Runs one point unless it is done; returns its mean (robust) tps."""
    d = config_dir(out, db, skew, system)
    d.mkdir(parents=True, exist_ok=True)
    done = d / f"t{threads}.done"
    if not done.exists():
        # a point cut short starts over
        for name in (f"t{threads}.csv", f"txns-t{threads}.csv", f"timeseries-t{threads}.csv"):
            (d / name).unlink(missing_ok=True)
        shutil.rmtree(d / f"t{threads}-nodes", ignore_errors=True)
        print(f"=== {db} skew {skew:g} {system}: {threads} threads x {args.reps}", flush=True)
        env = dict(os.environ, CSV=str(d / f"t{threads}.csv"), VARIANTS=variant(db, system),
                   LOCK_TIMEOUT_MS=str(args.lock_timeout_ms), SHIMS="2",
                   STATS_INTERVAL_S=str(args.stats_interval_s), GC_LOGS="1")
        cmd = [str(ROOT / "scripts" / "bench-compare.sh"), "--reps", str(args.reps), "--tc", args.tc,
               "--threads", str(threads),
               "--skew", f"{skew:g}", "--warmup-s", f"{args.warmup_s:g}", "--measure-s", f"{args.measure_s:g}",
               "--txn-timeout-ms", str(args.txn_timeout_ms), *WORKLOAD, "--sample-ms", str(args.sample_ms),
               *args.microbench]
        rc = subprocess.run(cmd, env=env, cwd=ROOT).returncode
        rows = read_point(d, threads)
        if len(rows) < args.reps:
            sys.exit(f"{db} skew {skew:g} {system} at {threads} threads: {len(rows)}/{args.reps} runs "
                     f"completed (exit {rc}); fix the cause and rerun with --out {out} to resume")
        if rc != 0:
            print(f"  warning: a run of this point warned or failed its audit (see the log above)", flush=True)
        done.touch()
    return statistics.mean(robust_tps(d, threads))


# ---- the search ----

def search(seeds, lo, hi, tol):
    """Generator: yields the thread counts to measure next, receives {threads: mean tps}."""
    measured = {}
    pending = sorted(set(seeds))
    while pending:
        measured.update((yield pending))
        xs = sorted(measured)
        best = max(xs, key=lambda x: measured[x])
        i = xs.index(best)
        pending = []
        if i == 0 and best > lo:
            pending.append(max(lo, best // 2))
        elif i == len(xs) - 1 and best < hi:
            pending.append(min(hi, best * 2))
        else:
            gap = max(2, tol * best)
            if i > 0 and best - xs[i - 1] > gap:
                pending.append(round((xs[i - 1] + best) / 2))
            if i < len(xs) - 1 and xs[i + 1] - best > gap:
                pending.append(round((best + xs[i + 1]) / 2))
        pending = sorted(p for p in set(pending) if p not in measured)


def grid(threads):
    """The search's interface for a fixed set of thread counts."""
    yield sorted(set(threads))


# ---- diagnostics of the stalls ----

def start_diagnostics(out):
    """vmstat and the TC's GC counters, every second, each line prefixed with the wall clock."""
    d = out / "diag"
    d.mkdir(exist_ok=True)

    def stamped(cmd, path):
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        except OSError as e:
            print(f"diagnostics: {cmd[0]} is not available ({e})")
            return None

        def pump():
            with open(path, "a") as f:
                for line in proc.stdout:
                    f.write(f"{time.time():.3f} {line}")
                    f.flush()

        threading.Thread(target=pump, daemon=True).start()
        return proc

    procs = [stamped(["vmstat", "1"], d / "vmstat.log")]
    pids = subprocess.run(["pgrep", "-f", "seata-server"], capture_output=True, text=True).stdout.split()
    if pids:
        procs.append(stamped(["jstat", "-gcutil", pids[0], "1000"], d / "tc-gc.log"))
    else:
        print("diagnostics: no seata-server process found, no TC GC counters")
    return [p for p in procs if p]


# ---- servers ----

def tc_up(tc):
    host, port = tc.rsplit(":", 1)
    try:
        with socket.create_connection((host, int(port)), timeout=2):
            return True
    except OSError:
        return False


def running(kind):
    return subprocess.run(["docker", "container", "inspect", f"coshim-bench-{kind}-0"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def start_servers(args):
    started = []
    lock_s = (args.lock_timeout_ms + 999) // 1000
    for kind in sorted({DB_KINDS[db] for db in args.dbs}):
        if running(kind):
            if not args.reuse_dbs:
                sys.exit(f"the {kind} servers are already running, maybe with other lock timeouts: stop them "
                         f"(scripts/bench-dbs.sh stop {kind}) or pass --reuse-dbs")
            print(f"reusing the running {kind} server(s)")
            continue
        print(f"starting 2 {kind} server(s), lock waits {lock_s} s", flush=True)
        env = dict(os.environ, MYSQL_LOCK_WAIT_TIMEOUT_S=str(lock_s), PG_LOCK_TIMEOUT=f"{lock_s}s")
        subprocess.run([str(ROOT / "scripts" / "bench-dbs.sh"), "start", kind, "2"], env=env, check=True,
                       stdout=subprocess.DEVNULL)
        started.append(kind)
    return started


def run(args):
    out = Path(args.out) if args.out else ROOT / "bench-results" / f"peak-{datetime.now():%Y%m%d-%H%M%S}"
    out.mkdir(parents=True, exist_ok=True)
    configs = [(db, skew, system) for db in args.dbs for skew in args.skews for system in args.systems]
    per_run = args.warmup_s + args.measure_s + SECONDS_PER_RUN_OVERHEAD
    print(f"{len(configs)} configs; the seeds alone are {len(configs) * len(args.seeds) * args.reps} runs "
          f"(~{len(configs) * len(args.seeds) * args.reps * per_run / 3600:.1f} h); a search usually adds 2-4 "
          f"points per config")
    if not tc_up(args.tc):
        sys.exit(f"no Seata TC at {args.tc}: start one first (see README.md)")
    setup = out / "setup.txt"
    if not setup.exists():
        commit = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"], capture_output=True,
                                text=True).stdout.strip() or "?"
        setup.write_text(
            f"date:      {datetime.now():%Y-%m-%d %H:%M}   commit: {commit}\n"
            f"configs:   dbs {' '.join(args.dbs)}, skews {' '.join(f'{s:g}' for s in args.skews)}, "
            f"systems {' '.join(args.systems)} (wound-wait)\n"
            f"search:    seeds {' '.join(map(str, args.seeds))}, threads in [{args.min_threads}, "
            f"{args.max_threads}], tol {args.tol:g}, {args.reps} reps per point\n"
            f"runs:      --warmup-s {args.warmup_s:g} --measure-s {args.measure_s:g} --txn-timeout-ms "
            f"{args.txn_timeout_ms} {' '.join(WORKLOAD)} --sample-ms {args.sample_ms} "
            f"{' '.join(args.microbench)}\n"
            f"timeouts:  lock waits {args.lock_timeout_ms} ms (nodes and servers), TC {args.txn_timeout_ms} ms; "
            f"node stats every {args.stats_interval_s} s\n")
    print(setup.read_text())

    started = start_servers(args)
    diagnostics = start_diagnostics(out)
    try:
        gens = {c: grid(args.threads) if args.threads else search(args.seeds, args.min_threads, args.max_threads, args.tol)
                for c in configs}
        batches = {c: next(g) for c, g in gens.items()}
        while batches:
            for c in list(batches):
                results = {t: measure(args, out, *c, t) for t in batches[c]}
                try:
                    batches[c] = gens[c].send(results)
                except StopIteration:
                    del batches[c]
                    print(f"--- {c[0]} skew {c[1]:g} {c[2]}: search done", flush=True)
    finally:
        for p in diagnostics:
            p.terminate()
        if not args.keep_dbs:
            for kind in started:
                subprocess.run([str(ROOT / "scripts" / "bench-dbs.sh"), "stop", kind])
    summarize(out)
    stalls(out)
    plot(out, quiet=True)


# ---- summary ----

def counters_of(line):
    return {k: int(v) for k, v in (kv.split("=", 1) for kv in line.split() if "=" in kv) if re.fullmatch(r"-?\d+", v)}


def node_counters(nodes_dir, var):
    """
    The shim nodes' counters at the last logged second the workload was active, summed over nodes and reps.
    The audit that follows only commits (reads), never aborts, and would swell every per-commit figure:
    up to 2/3 of the final commits at skew 0.99.
    """
    total = {}
    for log in glob.glob(str(nodes_dir / f"rep*-{var}-s*.log")):
        stats, spec = [], []
        for line in open(log, errors="replace"):
            if line.startswith("stats"):
                stats.append(counters_of(line))
            elif line.startswith("speculation"):
                spec.append(counters_of(line))
        if not stats:
            continue
        i = len(stats) - 1
        while i > 0 and stats[i].get("aborts") == stats[i - 1].get("aborts"):
            i -= 1
        snapshot = dict(stats[i])
        if i < len(spec):
            # from the speculation line only its totals (it repeats commits=)
            snapshot.update({k: v for k, v in spec[i].items() if k.endswith("_total")})
        for k, v in snapshot.items():
            total[k] = total.get(k, 0) + v
    return total


BUDGET = [("active_us", "active"), ("lock_wait_us", "lock wait"), ("prepare_wait_us", "prepare wait"),
          ("post_vote_us", "post-vote wait"), ("lost_wounded_us", "lost: wounded"),
          ("lost_cascade_us", "lost: cascade"), ("lost_coordinator_us", "lost: coordinator"),
          ("lost_other_us", "lost: other")]


def budget(n):
    """The time budget per branch commit, in µs, from the counters of one point; {} without them."""
    commits = n.get("commits", 0)
    if not commits or "committed_lifetime_us_total" not in n:
        return {}
    lifetime, lock, prep, post = (n[f"committed_{k}_us_total"] for k in ("lifetime", "lock_wait", "prepare_wait",
                                                                         "post_vote"))
    named = ("lost_wounded_us_total", "lost_cascade_us_total", "lost_coordinator_us_total")
    other = sum(v for k, v in n.items() if k.startswith("lost_") and k.endswith("_us_total") and k not in named)
    return {k: round(v / commits) for k, v in dict(
        active_us=lifetime - lock - prep - post, lock_wait_us=lock, prepare_wait_us=prep, post_vote_us=post,
        lost_wounded_us=n[named[0]], lost_cascade_us=n[named[1]], lost_coordinator_us=n[named[2]],
        lost_other_us=other).items()}


def load_points(out):
    points = []
    for d in sorted(out.glob("*-skew*-*")):
        m = re.fullmatch(r"(\w+)-skew([\d.]+)-(\w+)", d.name)
        if not m or not d.is_dir():
            continue
        db, skew, system = m.group(1), float(m.group(2)), m.group(3)
        for done in d.glob("t*.done"):
            t = int(done.stem[1:])
            rows = read_point(d, t)
            tps = robust_tps(d, t)
            mean = statistics.mean(tps)
            n = node_counters(d / f"t{t}-nodes", variant(db, system))
            commits = n.get("commits", 0)
            per = (lambda k: round(n[k] / commits) if commits and k in n else "")
            points.append(dict(
                db=db, skew=skew, system=system, threads=t, reps=len(rows), tps=round(mean, 1),
                tps_raw=round(statistics.mean(float(r["tps"]) for r in rows), 1),
                thread_us=round(1e6 * t / (mean * int(rows[0]["branches"]))) if mean and "branches" in rows[0] else "",
                tps_min=min(tps), tps_max=max(tps),
                tps_cv=round(100 * statistics.stdev(tps) / mean, 2) if len(tps) > 1 and mean else 0,
                abort_rate=round(statistics.mean(float(r["abort_rate"]) for r in rows), 4),
                op_p50_us=round(statistics.mean(float(r["op_p50_us"]) for r in rows)),
                op_p99_us=round(statistics.mean(float(r["op_p99_us"]) for r in rows)),
                saved_gross_us=per("saved_us_total"), saved_net_us=per("saved_net_us_total"),
                cascade_lost_us=per("cascade_lost_us_total"),
                cascades_per_100=round(100 * n["cascade"] / commits, 1) if commits and "cascade" in n else "",
                wounded_per_100=round(100 * n["wounded"] / commits, 1) if commits and "wounded" in n else "",
                coordinator_per_100=round(100 * n["coordinator"] / commits, 1) if commits and "coordinator" in n else "",
                upgrade_per_100=round(100 * n["upgrade_conflict"] / commits, 1)
                if commits and "upgrade_conflict" in n else "",
                **{k: budget(n).get(k, "") for k, _ in BUDGET}))
    return sorted(points, key=lambda p: (p["db"], p["skew"], p["system"], p["threads"]))


def peaks(points):
    by = {}
    for p in points:
        by.setdefault((p["db"], p["skew"], p["system"]), []).append(p)
    out = {}
    for k, ps in by.items():
        ps = sorted(ps, key=lambda p: p["threads"])
        best = max(ps, key=lambda p: p["tps"])
        i = ps.index(best)
        rest = [p for p in ps if p is not best]
        runner = max(rest, key=lambda p: p["tps"]) if rest else None
        out[k] = dict(best, bracketed=0 < i < len(ps) - 1, points=len(ps),
                      runner_up=f"{runner['threads']}: {runner['tps']}" if runner else "",
                      # the runner-up's mean is inside the peak's rep range: the top is flat
                      flat=bool(runner and runner["tps"] >= best["tps_min"]),
                      curve=" ".join(f"{p['threads']}:{p['tps']:.0f}" for p in ps))
    return out


def summarize(out):
    out = Path(out)
    points = load_points(out)
    if not points:
        sys.exit(f"no finished point in {out}")
    with open(out / "points.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(points[0]))
        w.writeheader()
        w.writerows(points)
    pk = peaks(points)
    with open(out / "peak.csv", "w", newline="") as f:
        rows = [pk[k] for k in sorted(pk)]
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    lines = ["# Peak throughput: speculative vs non-speculative shim (wound-wait)", ""]
    setup = out / "setup.txt"
    if setup.exists():
        lines += ["    " + l for l in setup.read_text().splitlines()] + [""]
    lines += ["tps of a run = the median of its per-second samples in the measurement window, so that a global stall",
              "does not count (see stalls.csv); per-commit figures stop at the end of the workload (no audit).",
              "Peak = the thread count with the highest mean tps over the reps. bracketed = measured points on both",
              "sides of it (else it sits at the edge of the search range); flat = the runner-up's mean is inside the",
              "peak's rep min-max (the top of the curve is flat, the exact thread count does not matter).", "",
              "## Peaks", "",
              "| db | skew | system | peak threads | peak tps | rep min-max | CV | bracketed | flat | runner-up | curve (threads:tps) |",
              "|---|---|---|---|---|---|---|---|---|---|---|"]
    for k in sorted(pk):
        p = pk[k]
        lines.append(f"| {p['db']} | {p['skew']:g} | {p['system']} | {p['threads']} | {p['tps']:.1f} "
                     f"| {p['tps_min']:.0f}-{p['tps_max']:.0f} | {p['tps_cv']}% | {'yes' if p['bracketed'] else 'NO'} "
                     f"| {'yes' if p['flat'] else 'no'} | {p['runner_up']} | {p['curve']} |")
    lines += ["", "## Speculative vs non-speculative, each at its own peak", "",
              "Per branch commit on the shims: saved net = lock wait speculation avoided minus the wait moved into "
              "prepare; lost = work of the txns cascade-aborted; cascades / wounds per 100 commits.", "",
              "| db | skew | spec peak (threads) | non-spec peak (threads) | gain | abort rate spec / non-spec "
              "| op p99 us spec / non-spec | saved net us | lost us | cascades /100 | wounds /100 spec / non-spec |",
              "|---|---|---|---|---|---|---|---|---|---|---|"]
    for db, skew in sorted({(k[0], k[1]) for k in pk}):
        s, n = pk.get((db, skew, "speculative")), pk.get((db, skew, "nonspeculative"))
        if not s or not n:
            continue
        lines.append(
            f"| {db} | {skew:g} | {s['tps']:.1f} ({s['threads']}) | {n['tps']:.1f} ({n['threads']}) "
            f"| {100 * (s['tps'] / n['tps'] - 1):+.1f}% | {100 * s['abort_rate']:.1f}% / {100 * n['abort_rate']:.1f}% "
            f"| {s['op_p99_us']} / {n['op_p99_us']} | {s['saved_net_us']} | {s['cascade_lost_us']} "
            f"| {s['cascades_per_100']} | {s['wounded_per_100']} / {n['wounded_per_100']} |")
    both = sorted({(p["db"], p["skew"], p["threads"]) for p in points if p["active_us"] != ""})
    if both:
        lines += ["", "## Time budget per branch commit, at equal thread counts", "",
                  "In ms. thread = threads / branch commits per second: all the thread time one branch commit costs. "
                  "active = work and think time of a committed branch; lost = lifetime of the aborted branches, per "
                  "commit, by cause. post-vote wait = voted YES, waiting for the global decision: it overlaps the "
                  "thread's next branch, so it is not added to the others. thread - the others = time outside the "
                  "shim's branches (TC, client, connections), roughly: an aborted branch's lifetime can also overlap.",
                  "", "| db | skew | threads | system | tps | gain | thread | " + " | ".join(n for _, n in BUDGET) + " |",
                  "|---|---|---|---|---|---|---|" + "---|" * len(BUDGET)]
        by_key = {(p["db"], p["skew"], p["threads"], p["system"]): p for p in points}
        for db, skew, t in both:
            s, n = by_key.get((db, skew, t, "speculative")), by_key.get((db, skew, t, "nonspeculative"))
            for system, p in (("spec", s), ("non-spec", n)):
                if not p or p["active_us"] == "":
                    continue
                gain = f"{100 * (s['tps'] / n['tps'] - 1):+.1f}%" if s and n and system == "spec" else ""
                thread = f"{p['thread_us'] / 1000:.1f}" if p["thread_us"] != "" else ""
                lines.append(f"| {db} | {skew:g} | {t} | {system} | {p['tps']:.0f} | {gain} | {thread} | "
                             + " | ".join(f"{p[k] / 1000:.1f}" for k, _ in BUDGET) + " |")
    lines += ["", f"Every point: {out}/points.csv; runs: <db>-skew<s>-<system>/t<T>.csv, throughput over time: "
              "timeseries-t<T>.csv, node logs (counters every second): t<T>-nodes/."]
    text = "\n".join(lines) + "\n"
    (out / "peak.md").write_text(text)
    print(text)


# ---- plots ----

COLORS = {"speculative": ("#2a78d6", "o", "Speculative shim"), "nonspeculative": ("#eb6834", "s", "Non-speculative shim")}
DB_NAMES = {"mysql": "MySQL", "pg": "PostgreSQL"}
INK, INK_MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def style(ax):
    ax.set_facecolor(SURFACE)
    ax.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.tick_params(length=0)


def plot(out, quiet=False):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.lines import Line2D
        from matplotlib.ticker import FuncFormatter, NullFormatter
    except ImportError:
        msg = "matplotlib is missing: python -m venv .venv && .venv/bin/pip install matplotlib"
        if quiet:
            print(msg)
            return
        sys.exit(msg)
    out = Path(out)
    points = load_points(out)
    if not points:
        sys.exit(f"no finished point in {out}")
    pk = peaks(points)
    dbs = [db for db in DB_NAMES if any(p["db"] == db for p in points)]
    skews = sorted({p["skew"] for p in points})
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK_MUTED,
                         "xtick.color": INK_MUTED, "ytick.color": INK_MUTED, "text.color": INK})
    handles = [Line2D([], [], color=c, marker=m, linewidth=2, markersize=6, markeredgecolor=SURFACE, label=n)
               for c, m, n in COLORS.values()]
    handles.append(Line2D([], [], color=INK_MUTED, marker="*", linestyle="none", markersize=12, label="peak"))

    budget_plots(out, points, dbs, skews, plt)

    # throughput against threads, one panel per db x skew
    fig, axes = plt.subplots(len(dbs), len(skews), figsize=(max(4.4 * len(skews), 13), 3.8 * len(dbs)), squeeze=False,
                             facecolor=SURFACE)
    for i, db in enumerate(dbs):
        for j, skew in enumerate(skews):
            ax = axes[i][j]
            style(ax)
            for system, (color, marker, _) in COLORS.items():
                ps = sorted((p for p in points if (p["db"], p["skew"], p["system"]) == (db, skew, system)),
                            key=lambda p: p["threads"])
                if not ps:
                    continue
                xs = [p["threads"] for p in ps]
                ax.plot(xs, [p["tps"] for p in ps], color=color, marker=marker, linewidth=2, markersize=6,
                        markeredgecolor=SURFACE, markeredgewidth=1.5, zorder=3)
                ax.vlines(xs, [p["tps_min"] for p in ps], [p["tps_max"] for p in ps], color=color, linewidth=1.2,
                          alpha=0.7, zorder=2)
                best = pk[(db, skew, system)]
                ax.plot([best["threads"]], [best["tps"]], marker="*", markersize=16, color=color,
                        markeredgecolor=INK, markeredgewidth=0.8, zorder=4)
                # the peak's thread count: speculative above-left of its star, non-speculative below-right
                above = system == "speculative"
                ax.annotate(f"{best['threads']}", (best["threads"], best["tps"]), textcoords="offset points",
                            xytext=(-10, 10) if above else (10, -14), ha="right" if above else "left", color=INK,
                            fontsize=9)
            ax.set_xscale("log")
            ts = sorted({p["threads"] for p in points if (p["db"], p["skew"]) == (db, skew)})
            ax.set_xticks(ts)
            ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
            ax.xaxis.set_minor_formatter(NullFormatter())
            ax.tick_params(axis="x", labelrotation=45, labelsize=8)
            ax.set_ylim(0, max(p["tps_max"] for p in points if (p["db"], p["skew"]) == (db, skew)) * 1.15)
            ax.set_title(f"{DB_NAMES[db]}, skew {skew:g}", color=INK, fontsize=11, fontweight="bold")
            if j == 0:
                ax.set_ylabel("Throughput (txn/s)")
            if i == len(dbs) - 1:
                ax.set_xlabel("Threads")
    fig.legend(handles=handles, loc="upper center", ncol=len(handles), frameon=False, bbox_to_anchor=(0.5, 1.0))
    # the header (legend, then this line) takes a fixed height, whatever the number of rows
    height = fig.get_figheight()
    fig.text(0.5, 1 - 0.42 / height, "Mean over reps, whisker = rep min-max; star = peak, with its thread count",
             ha="center", va="top", color=INK_MUTED, fontsize=9)
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.7 / height))
    fig.savefig(out / "peak-curves.png", dpi=150, facecolor=SURFACE)
    plt.close(fig)
    print(out / "peak-curves.png")

    # throughput over time at each peak: one figure per db, rows skews, columns systems
    for db in dbs:
        fig, axes = plt.subplots(len(skews), len(SYSTEMS), figsize=(7 * len(SYSTEMS), 2.6 * len(skews)),
                                 squeeze=False, sharex=True, facecolor=SURFACE)
        for i, skew in enumerate(skews):
            row_max = 0
            for j, system in enumerate(SYSTEMS):
                ax = axes[i][j]
                style(ax)
                best = pk.get((db, skew, system))
                if not best:
                    continue
                color = COLORS[system][0]
                f = config_dir(out, db, skew, system) / f"timeseries-t{best['threads']}.csv"
                reps = {}
                for r in (csv.DictReader(open(f)) if f.exists() else []):
                    reps.setdefault(r["label"], []).append(r)
                warm_end = None
                for k, (label, rs) in enumerate(sorted(reps.items())):
                    ts = [float(r["t_s"]) for r in rs]
                    tps = [float(r["tps"]) for r in rs]
                    row_max = max([row_max] + tps)
                    ax.plot(ts, tps, color=color, linewidth=1.2, alpha=[1, 0.65, 0.4][k % 3], label=label)
                    warm = [float(r["t_s"]) for r in rs if r["phase"] == "warmup"]
                    meas = [float(r["t_s"]) for r in rs if r["phase"] == "measure"]
                    if warm and meas:
                        warm_end, meas_end = max(warm), max(meas)
                if warm_end is not None:
                    for x in (warm_end, meas_end):
                        ax.axvline(x, color=INK_MUTED, linewidth=1, linestyle=(0, (4, 2)))
                ax.set_title(f"{COLORS[system][2]}, skew {skew:g}: {best['threads']} threads, "
                             f"{best['tps']:.0f} txn/s", color=INK, fontsize=10, fontweight="bold")
                if j == 0:
                    ax.set_ylabel("txn/s (per s)")
                if i == len(skews) - 1:
                    ax.set_xlabel("Time since start (s); dashed: end of warm-up, end of measurement")
            for j in range(len(SYSTEMS)):
                axes[i][j].set_ylim(0, row_max * 1.1 or 1)
        fig.suptitle(f"{DB_NAMES[db]}: throughput over time at each peak (one line per rep)", color=INK,
                     fontsize=12, fontweight="bold")
        fig.tight_layout(rect=(0, 0, 1, 0.97))
        target = out / f"timeseries-{db}.png"
        fig.savefig(target, dpi=150, facecolor=SURFACE)
        plt.close(fig)
        print(target)


# reference categorical slots in their fixed order (validated: adjacent pairs clear the CVD and normal-vision
# floors); the legend, the S / N labels and the budget table in peak.md carry the identity as well
BUDGET_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6"]
STACKED = [b for b in BUDGET if b[0] != "post_vote_us"]


def budget_plots(out, points, dbs, skews, plt):
    """Stacked bars per thread count: speculative (S) next to non-speculative (N), one figure per server kind."""
    if not any(p["active_us"] != "" for p in points):
        return
    by_key = {(p["db"], p["skew"], p["threads"], p["system"]): p for p in points}
    for db in dbs:
        fig, axes = plt.subplots(len(skews), 1, figsize=(13, 3.6 * len(skews)), squeeze=False, facecolor=SURFACE)
        for ax, skew in zip((row[0] for row in axes), skews):
            style(ax)
            ax.grid(False)
            ax.grid(True, axis="y", color=GRID, linewidth=0.8)
            threads = sorted({k[2] for k in by_key if k[:2] == (db, skew) and by_key[k]["active_us"] != ""})
            for x, t in enumerate(threads):
                for offset, system, tag in ((-0.2, "speculative", "S"), (0.2, "nonspeculative", "N")):
                    p = by_key.get((db, skew, t, system))
                    if not p or p["active_us"] == "":
                        continue
                    bottom = 0
                    for (k, name), color in zip(STACKED, BUDGET_COLORS):
                        h = p[k] / 1000
                        ax.bar(x + offset, h, bottom=bottom, width=0.36, color=color, edgecolor=SURFACE, linewidth=1,
                               label=name if x == 0 and tag == "S" else None, zorder=3)
                        bottom += h
                    ax.text(x + offset, bottom, f"{tag} {p['tps']:.0f}", ha="center", va="bottom", fontsize=8, color=INK)
            ax.set_xticks(range(len(threads)), [f"{t} threads" for t in threads])
            ax.set_ylabel("ms per branch commit")
            ax.margins(y=0.12)
            ax.set_title(f"{DB_NAMES[db]}, skew {skew:g}", color=INK, fontsize=11, fontweight="bold")
        handles, labels = axes[0][0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", ncol=len(STACKED), frameon=False, bbox_to_anchor=(0.5, 1.0),
                   fontsize=9)
        height = fig.get_figheight()
        fig.text(0.5, 1 - 0.32 / height, "S = speculative, N = non-speculative, number = txn/s; active = work and think "
                 "time of the committed branch, lost = aborted branches' lifetime per commit; post-vote wait not stacked", ha="center", va="top",
                 color=INK_MUTED, fontsize=9)
        fig.tight_layout(rect=(0, 0, 1, 1 - 0.6 / height))
        target = out / f"budget-{db}.png"
        fig.savefig(target, dpi=150, facecolor=SURFACE)
        plt.close(fig)
        print(target)


# ---- stalls: what happened during the throughput dips ----

# the JVM prints decimals in the machine's locale: 4,772ms
GC_LINE = re.compile(r"^\[([^\]]+)\].*\bPause\b.* (\d+(?:[.,]\d+)?)ms$")
SAFEPOINT_LINE = re.compile(r"^\[([^\]]+)\].*\bSafepoint\b.*Total: (\d+) ns")


def jvm_pauses(path):
    """[(end wall time s, seconds)] of the GC pauses and safepoints of one -Xlog file."""
    out = []
    for line in open(path, errors="replace"):
        m = GC_LINE.match(line)
        scale = 1e-3
        if not m:
            m, scale = SAFEPOINT_LINE.match(line), 1e-9
        if m:
            try:
                out.append((datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp(), float(m.group(2).replace(",", ".")) * scale))
            except ValueError:
                pass
    return out


def stamped_rows(path):
    """[(wall time s, columns)] of a diag/ file."""
    rows = []
    if path.exists():
        for line in open(path, errors="replace"):
            parts = line.split()
            try:
                rows.append((float(parts[0]), parts[1:]))
            except (ValueError, IndexError):
                pass
    return rows


def stalls(out):
    """stalls.csv: every sample below 60 % of its run's median, with what the JVMs and the machine did meanwhile."""
    out = Path(out)
    vm = stamped_rows(out / "diag" / "vmstat.log")
    tc = stamped_rows(out / "diag" / "tc-gc.log")
    rows = []
    runs = 0
    for f in sorted(out.glob("*-skew*-*/timeseries-t*.csv")):
        reps = {}
        for r in csv.DictReader(open(f)):
            reps.setdefault(r["label"], []).append(r)
        t = re.search(r"timeseries-t(\d+)", f.name).group(1)
        nodes = f.parent / f"t{t}-nodes"
        for label, rs in reps.items():
            window = [r for r in rs if r["phase"] == "measure"][:-1]
            if not window or "wall_ms" not in window[0]:
                continue
            runs += 1
            med = statistics.median(float(r["tps"]) for r in window)
            pauses = None
            for r in window:
                if float(r["tps"]) >= 0.6 * med:
                    continue
                if pauses is None:   # parsed once per run, only for the runs with a dip
                    pauses = {re.sub(r"^.*@wound-wait-|\.gc$", "", p): jvm_pauses(p)
                              for p in glob.glob(str(nodes / f"{label}-*.gc"))}
                end = int(r["wall_ms"]) / 1000
                start = end - int(r["interval_ms"]) / 1000
                worst = {jvm: max((s for e, s in ps if start <= e <= end + 1), default=0.0) for jvm, ps in pauses.items()}
                vms = [c for w, c in vm if start <= w <= end + 1 and len(c) >= 16 and c[0].isdigit()]
                # the header's last column is a name; G1 prints "-" in the first columns
                tcs = [c for w, c in tc if start - 1 <= w <= end + 1 and len(c) >= 11 and c[-1][0].isdigit()]
                rows.append(dict(
                    config=f.parent.name, threads=t, label=label, t_s=r["t_s"], tps=r["tps"], median_tps=round(med),
                    client_pause_s=round(worst.get("client", 0), 3), s0_pause_s=round(worst.get("s0", 0), 3),
                    s1_pause_s=round(worst.get("s1", 0), 3),
                    vm_max_blocked=max((int(c[1]) for c in vms), default=""),
                    vm_max_wa=max((int(c[15]) for c in vms), default=""), vm_min_idle=min((int(c[14]) for c in vms), default=""),
                    vm_max_bo=max((int(c[9]) for c in vms), default=""),
                    tc_gc_time_delta_s=round(float(tcs[-1][-1]) - float(tcs[0][-1]), 3) if len(tcs) > 1 else ""))
    print(f"stalls: {len(rows)} samples below 60 % of the median, in {runs} runs with samples")
    if rows:
        with open(out / "stalls.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
        print(f"  with a client GC / safepoint pause >= 0.2 s: {sum(r['client_pause_s'] >= 0.2 for r in rows)}; "
              f"in a node: {sum(max(r['s0_pause_s'], r['s1_pause_s']) >= 0.2 for r in rows)}; "
              f"see {out / 'stalls.csv'}")


def main():
    argv = sys.argv[1:]
    if argv and argv[0] in ("summarize", "plot", "stalls"):
        if len(argv) != 2:
            sys.exit(__doc__)
        {"summarize": summarize, "plot": plot, "stalls": stalls}[argv[0]](argv[1])
        return
    if argv and argv[0] == "run":
        argv = argv[1:]
    microbench = []
    if "--" in argv:
        i = argv.index("--")
        argv, microbench = argv[:i], argv[i + 1:]
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", help="output directory (an existing one resumes the search)")
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--dbs", nargs="+", default=["mysql", "pg"], choices=list(DB_KINDS))
    p.add_argument("--skews", nargs="+", type=float, default=[0.8, 0.9, 0.95, 0.99])
    p.add_argument("--systems", nargs="+", default=list(SYSTEMS), choices=list(SYSTEMS))
    p.add_argument("--seeds", nargs="+", type=int, default=[50, 100, 200])
    p.add_argument("--threads", nargs="+", type=int, help="no search: exactly these thread counts for every config")
    p.add_argument("--min-threads", type=int, default=10)
    p.add_argument("--max-threads", type=int, default=400)
    p.add_argument("--tol", type=float, default=0.25,
                   help="stop refining once both neighbors of the best point are within tol x its threads")
    p.add_argument("--warmup-s", type=float, default=10)
    p.add_argument("--measure-s", type=float, default=30)
    p.add_argument("--lock-timeout-ms", type=int, default=30000)
    p.add_argument("--txn-timeout-ms", type=int, default=60000)
    p.add_argument("--sample-ms", type=int, default=1000)
    p.add_argument("--stats-interval-s", type=int, default=1)
    p.add_argument("--tc", default="127.0.0.1:8091")
    p.add_argument("--reuse-dbs", action="store_true", help="use servers already running, whatever their timeouts")
    p.add_argument("--keep-dbs", action="store_true", help="leave the servers running at the end")
    args = p.parse_args(argv)
    args.microbench = microbench
    run(args)


if __name__ == "__main__":
    main()
