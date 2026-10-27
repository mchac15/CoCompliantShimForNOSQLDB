#!/usr/bin/env python3
"""Plots a scripts/bench-final.sh sweep from its summary.csv.

    scripts/bench-final-plot.py DIR        # writes DIR/final.png

One figure, small multiples: columns are server kind x skew, rows are the metrics (throughput,
per-op latency p50 / p99, retries per txn p50 / p99, and for the shims the net time speculation saved
per branch commit), x is the thread count. Color is the system
(speculative shim, non-speculative shim, Sonata), with a marker per system; line style is the
deadlock policy (solid wound-wait, dashed wait-die; Sonata has none). The y axes are logarithmic
(the values span several orders of magnitude between skews), but the net saved one: it can be negative.

Needs matplotlib (python -m venv .venv && .venv/bin/pip install matplotlib).
"""

import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter  # noqa: E402

# categorical slots 1-3 of the reference palette (validated: CVD and normal-vision separation pass;
# slot 3 is below 3:1 on the surface, so every system also has its own marker and a legend)
SYSTEMS = {
    "speculative": dict(color="#2a78d6", marker="o", name="Speculative shim"),
    "nonspeculative": dict(color="#eb6834", marker="s", name="Non-speculative shim"),
    "sonata": dict(color="#1baf7a", marker="^", name="Sonata"),
}
POLICIES = {"wound-wait": "-", "wait-die": (0, (4, 2)), None: "-"}
DB_NAMES = {"mysql": "MySQL", "pg": "PostgreSQL"}
INK, INK_MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"

# (column, label, divisor, scale): "log", "count" (symlog from 0) or "linear"
METRICS = [
    ("tps", "Throughput (txn/s)", 1, "log"),
    ("op_p50_us", "Per-op latency p50 (ms)", 1000, "log"),
    ("op_p99_us", "Per-op latency p99 (ms)", 1000, "log"),
    ("retries_p50", "Retries per txn p50", 1, "count"),
    ("retries_p99", "Retries per txn p99", 1, "count"),
    ("net_saved_us", "Net saved by speculation\nper branch commit (ms)", 1000, "linear"),
]


def value(r, metric, scale):
    """The metric of a summary row, or None (missing: Sonata has no shim stats, an old summary.csv)."""
    v = r.get(metric, "")
    return None if v in ("", None) or float(v) < 0 and metric != "net_saved_us" else float(v) / scale


def split_variant(variant):
    """'speculative-pg@wound-wait' -> ('speculative', 'pg', 'wound-wait'); 'sonata-pg' -> (.., None)."""
    base, _, policy = variant.partition("@")
    system, _, db = base.rpartition("-")
    return system, db, policy or None


def load(path):
    rows = []
    for r in csv.DictReader(open(path)):
        system, db, policy = split_variant(r["variant"])
        rows.append(dict(r, system=system, db=db, policy=policy, threads=int(r["threads"]),
                         skew=float(r["skew"])))
    return rows


def plot(out):
    rows = load(out / "summary.csv")
    dbs = [d for d in DB_NAMES if any(r["db"] == d for r in rows)]
    skews = sorted({r["skew"] for r in rows})
    threads = sorted({r["threads"] for r in rows})
    columns = [(db, skew) for db in dbs for skew in skews]
    metrics = [m for m in METRICS if any(value(r, m[0], m[2]) is not None for r in rows)]

    plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK_MUTED,
                         "xtick.color": INK_MUTED, "ytick.color": INK_MUTED, "text.color": INK})
    fig, axes = plt.subplots(len(metrics), len(columns), figsize=(max(4 * len(columns), 16), 2.9 * len(metrics)),
                             sharex=True, squeeze=False, facecolor=SURFACE)
    for row, (metric, label, scale, kind) in enumerate(metrics):
        values = [v for v in (value(r, metric, scale) for r in rows) if v is not None]
        positive = [v for v in values if v > 0]
        lo, hi = (min(positive), max(positive)) if positive else (1, 10)
        for col, (db, skew) in enumerate(columns):
            ax = axes[row][col]
            ax.set_facecolor(SURFACE)
            for system, style in SYSTEMS.items():
                for policy in ("wound-wait", "wait-die", None):
                    pts = sorted((r["threads"], value(r, metric, scale)) for r in rows
                                 if r["db"] == db and r["skew"] == skew and r["system"] == system
                                 and r["policy"] == policy and value(r, metric, scale) is not None)
                    if not pts:
                        continue
                    ax.plot([p[0] for p in pts], [p[1] for p in pts], color=style["color"],
                            linestyle=POLICIES[policy], linewidth=2, marker=style["marker"], markersize=6,
                            markeredgecolor=SURFACE, markeredgewidth=1.5, zorder=3)
            ax.set_xscale("log")
            ax.set_xticks(threads)
            ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
            ax.xaxis.set_minor_formatter(NullFormatter())
            if kind == "linear":
                ax.axhline(0, color=INK_MUTED, linewidth=1, zorder=2)
                span = max(abs(min(values, default=0)), abs(max(values, default=0))) or 1
                ax.set_ylim(min(0, min(values, default=0)) - 0.1 * span, max(0, max(values, default=0)) + 0.1 * span)
            elif kind == "count":
                # retry counts include 0: linear up to 1, logarithmic above
                ax.set_yscale("symlog", linthresh=1)
                ax.set_ylim(0, hi * 1.6)
            else:
                ax.set_yscale("log")
                ax.set_ylim(lo / 1.6, hi * 1.6)
                # narrow ranges hold no full decade: label 1, 2, 5 within each decade then
                ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0,) if hi / lo >= 100 else (1.0, 2.0, 5.0)))
            ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
            ax.yaxis.set_minor_formatter(NullFormatter())
            ax.grid(True, which="major", color=GRID, linewidth=0.8)
            ax.set_axisbelow(True)
            for side in ("top", "right"):
                ax.spines[side].set_visible(False)
            ax.tick_params(length=0)
            if row == 0:
                ax.set_title(f"{DB_NAMES[db]}, skew {skew:g}", color=INK, fontsize=11, fontweight="bold")
            if col == 0:
                ax.set_ylabel(label)
            if row == len(metrics) - 1:
                ax.set_xlabel("Threads")

    handles = [Line2D([], [], color=s["color"], marker=s["marker"], linewidth=2, markersize=6,
                      markeredgecolor=SURFACE, label=s["name"]) for s in SYSTEMS.values()]
    handles += [Line2D([], [], color=INK_MUTED, linestyle=POLICIES["wound-wait"], linewidth=2,
                       label="wound-wait (shim)"),
                Line2D([], [], color=INK_MUTED, linestyle=POLICIES["wait-die"], linewidth=2,
                       label="wait-die (shim)")]
    fig.legend(handles=handles, loc="upper center", ncol=len(handles), frameon=False,
               bbox_to_anchor=(0.5, 1.0))
    fig.text(0.5, 0.975, "Latency: committed txns, whole execution with retries, think time excluded, per op; "
             "percentiles pooled over reps, throughput = mean over reps", ha="center", color=INK_MUTED,
             fontsize=9)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    target = out / "final.png"
    fig.savefig(target, dpi=150, facecolor=SURFACE)
    print(target)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    plot(Path(sys.argv[1]))
