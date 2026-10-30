#!/usr/bin/env python3
"""Peak throughput of a scripts/bench-final.sh sweep, from its summary.csv.

    scripts/bench-final-peak.py DIR        # writes DIR/peak.csv and DIR/peak.png, prints the table

For every server kind, skew and system: the highest mean throughput over the thread counts, and the
thread count it was reached at (each system at its own best client count). The figure shows the
peaks as grouped bars per skew, labeled with the speculative vs non-speculative gain and, under each
bar, the thread count of the peak. Peaks at the edge of the thread range, or a coarse thread grid,
mean the true peak may lie between two measured counts. Needs matplotlib.
"""

import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

SYSTEMS = [("speculative", "Speculative shim", "#2a78d6"), ("nonspeculative", "Non-speculative shim", "#eb6834"),
           ("sonata", "Sonata", "#1baf7a")]
DB_NAMES = {"mysql": "MySQL", "pg": "PostgreSQL"}
INK, INK_MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def peaks(path):
    """{(db, skew, system): (tps, threads, tps_min, tps_max)} at the thread count of the highest tps."""
    runs = {}
    for r in csv.DictReader(open(path)):
        system, _, db = r["variant"].split("@")[0].rpartition("-")
        runs.setdefault((db, float(r["skew"]), system), []).append(
            (float(r["tps"]), int(r["threads"]), float(r["tps_min"]), float(r["tps_max"])))
    return {k: max(v) for k, v in runs.items()}, sorted({t for v in runs.values() for _, t, _, _ in v})


def main(out):
    p, threads = peaks(out / "summary.csv")
    dbs = [db for db in DB_NAMES if any(k[0] == db for k in p)]
    skews = sorted({k[1] for k in p})

    with open(out / "peak.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["db", "skew", "system", "peak_tps", "threads", "tps_min", "tps_max", "gain_vs_nonspeculative"])
        for db in dbs:
            for s in skews:
                ns = p[(db, s, "nonspeculative")][0]
                for system, _, _ in SYSTEMS:
                    tps, t, lo, hi = p[(db, s, system)]
                    w.writerow([db, s, system, f"{tps:.1f}", t, f"{lo:.1f}", f"{hi:.1f}", f"{tps / ns - 1:+.4f}"])
                    print(f"{DB_NAMES[db]:10s} skew {s:<4g} {system:15s} {tps:8.1f} tps at {t:3d} threads "
                          f"({lo:.0f}-{hi:.0f}), {100 * (tps / ns - 1):+5.1f}% vs non-speculative")

    plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK_MUTED,
                         "xtick.color": INK_MUTED, "ytick.color": INK_MUTED, "text.color": INK})
    fig, axes = plt.subplots(1, len(dbs), figsize=(7.5 * len(dbs), 5.2), squeeze=False, facecolor=SURFACE)
    width = 0.27
    top = max(v[3] for v in p.values())
    for ax, db in zip(axes[0], dbs):
        ax.set_facecolor(SURFACE)
        for i, (system, name, color) in enumerate(SYSTEMS):
            xs = [g + (i - 1) * width for g in range(len(skews))]
            vals = [p[(db, s, system)] for s in skews]
            ax.bar(xs, [v[0] for v in vals], width=width - 0.04, color=color, label=name, zorder=3)
            # rep min-max of the peak cell
            ax.vlines(xs, [v[2] for v in vals], [v[3] for v in vals], color=INK_MUTED, linewidth=1.2, zorder=4)
            for x, v in zip(xs, vals):
                ax.text(x, -0.012 * top, f"{v[1]}", ha="center", va="top", color=INK_MUTED, fontsize=8)
        for g, s in enumerate(skews):
            sp, ns = p[(db, s, "speculative")][0], p[(db, s, "nonspeculative")][0]
            ax.text(g - width / 2, max(p[(db, s, "speculative")][3], p[(db, s, "nonspeculative")][3]) + 0.02 * top,
                    f"{100 * (sp / ns - 1):+.1f}%", ha="center", va="bottom", color=INK, fontsize=9,
                    fontweight="bold")
        ax.set_xticks(range(len(skews)), [f"skew {s:g}" for s in skews])
        ax.tick_params(axis="x", pad=14, length=0)
        ax.tick_params(axis="y", length=0)
        ax.set_ylim(0, top * 1.12)
        ax.set_ylabel("Peak throughput (txn/s)")
        ax.set_title(DB_NAMES[db], color=INK, fontsize=12, fontweight="bold")
        ax.grid(True, axis="y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(SYSTEMS), frameon=False, bbox_to_anchor=(0.5, 1.0))
    fig.text(0.5, 0.905, f"Highest mean throughput over {', '.join(map(str, threads))} threads; "
             "number under a bar = threads at the peak, whisker = rep min-max; "
             "% = speculative vs non-speculative peak", ha="center", color=INK_MUTED, fontsize=9)
    fig.tight_layout(rect=(0, 0, 1, 0.89))
    target = out / "peak.png"
    fig.savefig(target, dpi=150, facecolor=SURFACE)
    print(target)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(Path(sys.argv[1]))
