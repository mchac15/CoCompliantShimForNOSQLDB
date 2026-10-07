#!/usr/bin/env python3
"""Sweeps the shim's lock timeout against Sonata on the same SQL servers, and plots it.

    scripts/bench-lock-timeout-sweep.py [run] [options] [-- MicroBench options...]
    scripts/bench-lock-timeout-sweep.py plot DIR            # re-plots an existing sweep

For each lock timeout (default 1 5 10 20 50 100 ms) it runs scripts/bench-sql-compare.sh with
LOCK_TIMEOUT_MS set, the shim variants only, over the skews (default 0.5 0.99), once each, at a fixed
thread count (50) and table size (10000). The lock timeout is an option of the shim nodes only:
Sonata's lock waits are the servers' (innodb_lock_wait_timeout / lock_timeout, scripts/bench-dbs.sh),
so Sonata runs once per skew and is drawn as a flat reference line (--sonata-each-timeout reruns it
at every timeout instead). The servers are started once and stopped at the end if this script
started them.

Output, in DIR = bench-results/lock-timeout-<timestamp>/:
  lt-<T>ms/        bench-sql-compare.sh's output for the shim variants at lock timeout T
  sonata/          bench-sql-compare.sh's output for the Sonata variants (once, or lt-<T>ms/ holds them)
  skew-<s>.png     tps and abort rate against the lock timeout at skew s (with --reps > 1: mean, min-max band)
  overview.png     all skews in one figure (columns: skews; rows: tps, abort rate)
  sweep.csv        every run, with a lock_timeout_ms column (empty for Sonata run once)

A sweep that stops halfway can be resumed with --out DIR: the timeouts (and Sonata) with a
summary.md and every skew's CSV are skipped.

Needs a running Seata TC, Docker, and matplotlib (python -m venv .venv && .venv/bin/pip install
matplotlib). Example: scripts/bench-lock-timeout-sweep.py --reps 3 -- --warmup-s 5
"""

import argparse
import csv
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# categorical slots 1-2 of the reference palette (validated all-pairs): color = server kind,
# line style = who does the concurrency control
DB_COLORS = {"mysql": "#2a78d6", "pg": "#eb6834"}
DB_NAMES = {"mysql": "MySQL", "pg": "PostgreSQL"}
SYSTEM_STYLES = {
    "speculative": dict(linestyle="-", marker="o", name="shim"),
    "nonspeculative": dict(linestyle=":", marker="s", name="non-speculative shim"),
    "nocc": dict(linestyle="-.", marker="^", name="no-CC shim"),
    "sonata": dict(linestyle="--", marker=None, name="Sonata"),
}
INK, INK_MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"


def split_variant(variant):
    """'speculative-mysql' -> ('speculative', 'mysql')."""
    system, _, db = variant.rpartition("-")
    return system, db


def run_compare(out, variants, skews, lock_timeout, args, microbench_args):
    env = dict(os.environ,
               OUT=str(out), VARIANTS=" ".join(variants), SKEWS=" ".join(skews),
               REPS=str(args.reps), THREADS=str(args.threads), WARMUP_S=str(args.warmup_s),
               MEASURE_S=str(args.measure_s), KEEP_DBS="1")
    if lock_timeout is not None:
        env["LOCK_TIMEOUT_MS"] = str(lock_timeout)
    cmd = [str(ROOT / "scripts/bench-sql-compare.sh"),
           "--table-size", str(args.table_size), *microbench_args]
    print(f"\n##### {out.name}: {' '.join(variants)}"
          + (f", lock timeout {lock_timeout} ms" if lock_timeout is not None else ""), flush=True)
    return subprocess.run(cmd, env=env).returncode


def db_kinds(variants):
    kinds = []
    for v in variants:
        kind = {"mysql": "mysql", "pg": "postgres"}.get(split_variant(v)[1])
        if kind and kind not in kinds:
            kinds.append(kind)
    return kinds


def running(kind):
    return subprocess.run(["docker", "container", "inspect", f"coshim-bench-{kind}-0"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def run(args, microbench_args):
    out = Path(args.out) if args.out else ROOT / f"bench-results/lock-timeout-{datetime.now():%Y%m%d-%H%M%S}"
    out.mkdir(parents=True, exist_ok=True)
    variants = args.variants.split()
    shim_variants = [v for v in variants if not v.startswith("sonata-")]
    sonata_variants = [v for v in variants if v.startswith("sonata-")]
    # bench-sql-compare.sh runs with KEEP_DBS=1, so the servers outlive each call; stop at the end
    # the ones that were not running before
    to_stop = [k for k in db_kinds(variants) if not running(k)]
    status = 0
    try:
        jobs = []
        for t in args.lock_timeouts:
            vs = variants if args.sonata_each_timeout else shim_variants
            if vs:
                jobs.append((out / f"lt-{t}ms", vs, t))
        if sonata_variants and not args.sonata_each_timeout:
            jobs.append((out / "sonata", sonata_variants, None))
        for i, (d, vs, t) in enumerate(jobs, 1):
            if (d / "summary.md").exists() and all((d / f"skew-{s}.csv").exists() for s in args.skews):
                print(f"skipping {d.name}: already done")
                continue
            print(f"\n[{i}/{len(jobs)}]", end="")
            status |= run_compare(d, vs, args.skews, t, args, microbench_args)
            if not any(d.glob("skew-*.csv")):   # not one run: the setup is broken (servers, TC), stop here
                print(f"\n{d.name} produced no run, stopping the sweep (see the errors above); "
                      f"fix it and resume with --out {out}", file=sys.stderr)
                return 1
    finally:
        if not args.keep_dbs:
            for kind in to_stop:
                subprocess.run([str(ROOT / "scripts/bench-dbs.sh"), "stop", kind])
    plot(out)
    return 1 if status else 0


def load(out):
    """Every run of the sweep in out, as dicts with a lock_timeout_ms key (None: Sonata run once)."""
    rows = []
    for d in sorted(out.iterdir()):
        if d.name.startswith("lt-") and d.name.endswith("ms"):
            lock_timeout = float(d.name[3:-2])
        elif d.name == "sonata":
            lock_timeout = None
        else:
            continue
        for f in d.glob("skew-*.csv"):
            with f.open() as fh:
                for row in csv.DictReader(fh):
                    row["lock_timeout_ms"] = lock_timeout
                    rows.append(row)
    return rows


def aggregate(rows, key):
    """{(skew, variant): {lock_timeout: (mean, min, max)}} of float(row[key]) over the reps."""
    groups = {}
    for r in rows:
        groups.setdefault((r["skew"], r["variant"]), {}).setdefault(r["lock_timeout_ms"], []).append(float(r[key]))
    return {k: {t: (sum(v) / len(v), min(v), max(v)) for t, v in by_t.items()} for k, by_t in groups.items()}


METRICS = [("tps", "Throughput (committed txn/s)", 1.0), ("abort_rate", "Abort rate (%)", 100.0)]


def draw(ax, skew, metric, label, scale, aggs, variants, timeouts, direct_labels):
    data = aggs[metric]
    for variant in variants:
        series = data.get((skew, variant))
        if not series:
            continue
        system, db = split_variant(variant)
        style = SYSTEM_STYLES.get(system, dict(linestyle="-", marker="o", name=system))
        color = DB_COLORS.get(db, INK_MUTED)
        name = f"{style['name']} on {DB_NAMES.get(db, db)}"
        if None in series and len(series) == 1:   # Sonata, run once: flat across the timeouts
            mean, lo, hi = (x * scale for x in series[None])
            xs = [timeouts[0], timeouts[-1]]
            ys, los, his = [mean] * 2, [lo] * 2, [hi] * 2
        else:
            xs = sorted(t for t in series if t is not None)
            ys = [series[t][0] * scale for t in xs]
            los = [series[t][1] * scale for t in xs]
            his = [series[t][2] * scale for t in xs]
        ax.plot(xs, ys, color=color, linestyle=style["linestyle"], linewidth=2,
                marker=style["marker"], markersize=6, markeredgecolor="white", markeredgewidth=1.5,
                label=name, zorder=3)
        if any(h > l for l, h in zip(los, his)):
            ax.fill_between(xs, los, his, color=color, alpha=0.12, linewidth=0, zorder=2)
        if direct_labels:
            ax.annotate(name, (xs[-1], ys[-1]), xytext=(6, 0), textcoords="offset points",
                        va="center", fontsize=8, color=INK_MUTED)
    ax.set_xscale("log")
    ax.set_xticks(timeouts)
    ax.set_xticklabels([f"{t:g}" for t in timeouts])
    ax.minorticks_off()
    ax.set_xlabel("Shim lock timeout (ms)", color=INK_MUTED)
    ax.set_ylabel(label, color=INK_MUTED)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK_MUTED, labelsize=9)


def setup_line(out):
    """The run options (threads, table size, ...) from one of the sweep's setup.txt, for the titles."""
    for f in sorted(out.glob("*/setup.txt")):
        for line in f.read_text().splitlines():
            if line.startswith("options:"):
                return line.split(":", 1)[1].strip()
    return ""


def plot(out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = load(out)
    if not rows:
        print(f"no runs in {out} (lt-*ms/skew-*.csv, sonata/skew-*.csv)", file=sys.stderr)
        return 1
    with (out / "sweep.csv").open("w", newline="") as fh:
        fields = ["lock_timeout_ms"] + [k for k in rows[0] if k != "lock_timeout_ms"]
        w = csv.DictWriter(fh, fields, extrasaction="ignore")
        w.writeheader()
        w.writerows({**r, "lock_timeout_ms": "" if r["lock_timeout_ms"] is None else f"{r['lock_timeout_ms']:g}"}
                    for r in rows)

    skews = sorted({r["skew"] for r in rows}, key=float)
    timeouts = sorted({r["lock_timeout_ms"] for r in rows if r["lock_timeout_ms"] is not None})
    if not timeouts:
        print("no shim runs to plot against the lock timeout", file=sys.stderr)
        return 1
    # shim variants first, then Sonata; within each, MySQL before PostgreSQL
    order = list(SYSTEM_STYLES)
    variants = sorted({r["variant"] for r in rows},
                      key=lambda v: (split_variant(v)[0] == "sonata", order.index(split_variant(v)[0])
                                     if split_variant(v)[0] in order else 99, split_variant(v)[1]))
    aggs = {m: aggregate(rows, m) for m, _, _ in METRICS}
    reps = max(len([r for r in rows if r["skew"] == s and r["variant"] == v and r["lock_timeout_ms"] == t])
               for s in skews for v in variants for t in timeouts + [None]) or 1
    options = setup_line(out)
    note = ("one run per point" if reps == 1 else f"mean over {reps} reps, band: min-max") \
        + (f"   |   {options}" if options else "")
    plt.rcParams.update({"font.size": 10, "axes.titlesize": 11, "figure.facecolor": "white"})

    def legend(fig, axes):
        entries = {}   # every series of every panel, once (a variant may be missing at some skew)
        for ax in axes:
            for h, l in zip(*ax.get_legend_handles_labels()):
                entries.setdefault(l, h)
        handles, labels = list(entries.values()), list(entries)
        fig.legend(handles, labels, loc="lower center", ncol=min(len(labels), 4), frameon=False,
                   fontsize=9, bbox_to_anchor=(0.5, 0.0))

    for skew in skews:
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.6))
        for ax, (m, label, scale) in zip(axes, METRICS):
            draw(ax, skew, m, label, scale, aggs, variants, timeouts, direct_labels=False)
            ax.set_title(label.split(" (")[0], color=INK, loc="left")
        fig.suptitle(f"Shim vs Sonata against the shim's lock timeout, skew {skew}", color=INK, x=0.01,
                     ha="left", fontweight="bold")
        fig.text(0.01, 0.905, note, fontsize=8, color=INK_MUTED, ha="left")
        legend(fig, axes)
        fig.tight_layout(rect=(0, 0.08, 1, 0.9))
        fig.savefig(out / f"skew-{skew}.png", dpi=150)
        plt.close(fig)

    fig, axes = plt.subplots(len(METRICS), len(skews), figsize=(5 * len(skews), 8), squeeze=False)
    for col, skew in enumerate(skews):
        for row, (m, label, scale) in enumerate(METRICS):
            ax = axes[row][col]
            draw(ax, skew, m, label, scale, aggs, variants, timeouts, direct_labels=False)
            if row == 0:
                ax.set_title(f"skew {skew}", color=INK)
            if col > 0:
                ax.set_ylabel("")
    fig.suptitle("Shim vs Sonata against the shim's lock timeout", color=INK, x=0.01, ha="left",
                 fontweight="bold")
    fig.text(0.01, 0.935, note, fontsize=8, color=INK_MUTED, ha="left")
    legend(fig, axes.flat)
    fig.tight_layout(rect=(0, 0.05, 1, 0.93))
    fig.savefig(out / "overview.png", dpi=150)
    plt.close(fig)

    print(f"plots: {', '.join(str(out / f'skew-{s}.png') for s in skews)}, {out / 'overview.png'}")
    print(f"runs:  {out / 'sweep.csv'}")
    return 0


def main():
    argv = sys.argv[1:]
    if argv[:1] == ["plot"]:
        if len(argv) != 2:
            sys.exit(f"usage: {sys.argv[0]} plot DIR")
        sys.exit(plot(Path(argv[1])))
    if argv[:1] == ["run"]:
        argv = argv[1:]
    microbench_args = []
    if "--" in argv:
        i = argv.index("--")
        argv, microbench_args = argv[:i], argv[i + 1:]

    p = argparse.ArgumentParser(description=__doc__.split("\n")[0],
                                epilog="Arguments after -- go to MicroBench (through bench-sql-compare.sh).")
    p.add_argument("--lock-timeouts", type=int, nargs="+", default=[1, 5, 10, 20, 50, 100], metavar="MS")
    p.add_argument("--skews", nargs="+", default=["0.5", "0.99"])
    p.add_argument("--threads", type=int, default=50)
    p.add_argument("--table-size", type=int, default=10000)
    p.add_argument("--reps", type=int, default=1)
    p.add_argument("--warmup-s", type=int, default=10)
    p.add_argument("--measure-s", type=int, default=30)
    p.add_argument("--variants", default="speculative-mysql nonspeculative-mysql sonata-mysql "
                                     "speculative-pg nonspeculative-pg sonata-pg",
                   help="bench-compare.sh variants (default: %(default)s)")
    p.add_argument("--sonata-each-timeout", action="store_true",
                   help="rerun Sonata at every lock timeout instead of once per skew")
    p.add_argument("--keep-dbs", action="store_true", help="leave the servers running at the end")
    p.add_argument("--out", help="output directory (an existing one resumes the sweep)")
    args = p.parse_args(argv)
    args.lock_timeouts = sorted(args.lock_timeouts)
    sys.exit(run(args, microbench_args))


if __name__ == "__main__":
    main()
