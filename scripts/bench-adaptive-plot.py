#!/usr/bin/env python3
"""Latency-throughput plot of a scripts/bench-adaptive-sweep.sh sweep, from its summary.csv.

    scripts/bench-adaptive-plot.py DIR        # writes DIR/latency-throughput.png and DIR/throughput-threads.png

Small multiples: columns are skews, rows are the committed txns' latency p50 and p99 (first attempt's
begin to commit, retries included, think time excluded; seconds, log scale), x is the throughput. Each
variant is one line through its thread counts in increasing order, the lowest and highest thread count
labelled at its ends. Color is the system (speculative, non-speculative, adaptive); the adaptive cap
configurations share their color and differ by marker and dash. A label that would overlap one
already drawn in its panel is left out. throughput-threads.png: the same lines as throughput against thread count,
one panel per skew (where a system collapses under load).

Needs matplotlib (python -m venv .venv && .venv/bin/pip install matplotlib).
"""

import csv
import re
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

# categorical slots 1-3 of the reference palette (validated all-pairs; slot 3 is below 3:1 on the
# surface, so every line also has its own marker, a legend and an end label)
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK_MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
ADAPTIVE_STYLES = [("^", "-"), ("D", (0, (4, 2))), ("v", (0, (1, 1.5)))]
ROWS = [("txn_p50_us", "Txn latency p50 (s)"), ("txn_p99_us", "Txn latency p99 (s)")]


def style(variant, adaptive_order):
    system = variant.split("@")[0].rsplit("-", 1)[0]   # adaptive-th0.9-tau2000-mysql@... -> adaptive-th0.9-tau2000
    if system == "speculative":
        return dict(color=BLUE, marker="o", linestyle="-", name="Speculative")
    if system == "nonspeculative":
        return dict(color=ORANGE, marker="s", linestyle="-", name="Non-speculative")
    m = re.fullmatch(r"adaptive-th([0-9.]+)(?:-tau(\d+))?", system)
    marker, dash = ADAPTIVE_STYLES[adaptive_order.index(variant) % len(ADAPTIVE_STYLES)]
    name = f"Adaptive θ={m.group(1)}" + (f", τ={int(m.group(2)) / 1000:g} s" if m.group(2) else "") if m else system
    return dict(color=AQUA, marker=marker, linestyle=dash, name=name)


def plot(out):
    rows = list(csv.DictReader(open(out / "summary.csv")))
    skews = sorted({r["skew"] for r in rows}, key=float)
    variants = list(dict.fromkeys(r["variant"] for r in rows))
    # baselines first, then the cap configurations
    variants.sort(key=lambda v: (not v.startswith("speculative"), not v.startswith("nonspeculative"), v))
    adaptive = [v for v in variants if v.startswith("adaptive")]
    styles = {v: style(v, adaptive) for v in variants}

    plt.rcParams.update({"font.size": 9, "axes.edgecolor": GRID, "axes.labelcolor": INK_MUTED,
                         "xtick.color": INK_MUTED, "ytick.color": INK_MUTED, "text.color": INK})
    fig, axes = plt.subplots(len(ROWS), len(skews), figsize=(4.2 * len(skews), 3.4 * len(ROWS)),
                             sharey="row", squeeze=False, facecolor=SURFACE)
    candidates = []   # (axes, thread count, x, y, ha): placed once the layout is final
    for col, skew in enumerate(skews):
        for row, (metric, label) in enumerate(ROWS):
            ax = axes[row][col]
            ax.set_facecolor(SURFACE)
            ax.set_yscale("log")
            ax.grid(True, color=GRID, linewidth=0.6)
            ax.set_axisbelow(True)
            for spine in ("top", "right"):
                ax.spines[spine].set_visible(False)
            for v in variants:
                pts = sorted((int(r["threads"]), float(r["tps"]), float(r[metric]) / 1e6)
                             for r in rows if r["skew"] == skew and r["variant"] == v and float(r[metric]) > 0)
                if not pts:
                    continue
                s = styles[v]
                ax.plot([p[1] for p in pts], [p[2] for p in pts], color=s["color"], linestyle=s["linestyle"],
                        linewidth=2, marker=s["marker"], markersize=6, markeredgecolor=SURFACE,
                        markeredgewidth=1, label=s["name"])
                # thread counts at both ends of the line, in muted ink
                for (t, x, y), ha in ((pts[0], "right"), (pts[-1], "left")):
                    candidates.append((ax, t, x, y, ha))
            ax.margins(x=0.12)
            if row == 0:
                ax.set_title(f"skew {skew}", color=INK, fontsize=10)
            if row == len(ROWS) - 1:
                ax.set_xlabel("Throughput (committed txn/s)")
            if col == 0:
                ax.set_ylabel(label)
    handles = [Line2D([], [], color=styles[v]["color"], linestyle=styles[v]["linestyle"], linewidth=2,
                      marker=styles[v]["marker"], markersize=6, markeredgecolor=SURFACE, label=styles[v]["name"])
               for v in variants]
    fig.legend(handles=handles, loc="upper center", ncol=len(handles), frameon=False,
               bbox_to_anchor=(0.5, 1.0), fontsize=9)
    fig.text(0.5, 0.005, "Each line runs through the thread counts in increasing order; its end points are labelled "
             "with their thread count. MySQL, 4 pipelined branches × (5 reads + 5 rmw writes), get-for-update, "
             "10 ms think time, wound-wait + prepare wait-die; mean of 3 reps (tps), latencies pooled.",
             ha="center", va="bottom", fontsize=7, color=INK_MUTED, wrap=True)
    fig.tight_layout(rect=(0, 0.04, 1, 0.94))
    # thread count labels at both ends of each line, in muted ink, minus those that would overlap one
    # already drawn in the same panel (in display coordinates, now that limits and layout are final)
    placed = {}
    for ax, t, x, y, ha in candidates:
        px, py = ax.transData.transform((x, y))
        px += -14 if ha == "right" else 14
        if any(abs(px - qx) < 30 and abs(py - qy) < 14 for qx, qy in placed.get(ax, [])):
            continue
        placed.setdefault(ax, []).append((px, py))
        ax.annotate(f"{t}", (x, y), xytext=(-5 if ha == "right" else 5, 0), textcoords="offset points",
                    ha=ha, va="center", fontsize=7, color=INK_MUTED)
    target = out / "latency-throughput.png"
    fig.savefig(target, dpi=150, facecolor=SURFACE)
    print(target)

    fig, axes = plt.subplots(1, len(skews), figsize=(4.2 * len(skews), 3.4), squeeze=False, facecolor=SURFACE)
    for col, skew in enumerate(skews):
        ax = axes[0][col]
        ax.set_facecolor(SURFACE)
        ax.grid(True, color=GRID, linewidth=0.6)
        ax.set_axisbelow(True)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        for v in variants:
            pts = sorted((int(r["threads"]), float(r["tps"])) for r in rows if r["skew"] == skew and r["variant"] == v)
            s = styles[v]
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=s["color"], linestyle=s["linestyle"], linewidth=2,
                    marker=s["marker"], markersize=6, markeredgecolor=SURFACE, markeredgewidth=1)
        ax.set_ylim(bottom=0)
        ax.set_title(f"skew {skew}", color=INK, fontsize=10)
        ax.set_xlabel("Threads")
        if col == 0:
            ax.set_ylabel("Throughput (committed txn/s)")
    fig.legend(handles=handles, loc="upper center", ncol=len(handles), frameon=False,
               bbox_to_anchor=(0.5, 1.0), fontsize=9)
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    target = out / "throughput-threads.png"
    fig.savefig(target, dpi=150, facecolor=SURFACE)
    print(target)


if __name__ == "__main__":
    plot(Path(sys.argv[1] if len(sys.argv) > 1 else "."))
