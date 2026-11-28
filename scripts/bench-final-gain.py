#!/usr/bin/env python3
"""Where speculation pays off: heatmaps of a scripts/bench-final.sh sweep, from its summary.csv.

    scripts/bench-final-gain.py DIR        # writes DIR/gain.png

Per server kind, threads x skew: the speculative shim's throughput relative to the non-speculative
one (%), and the extra abort rate speculation causes (percentage points: cascades on top of the
wounds both shims have). Diverging blue (speculation better) / red (worse) around a gray zero, each
cell labeled with its value. Needs matplotlib.
"""

import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm  # noqa: E402

# the reference diverging pair: blue (good for speculation) <-> red, neutral gray midpoint
GAIN = LinearSegmentedColormap.from_list("gain", ["#e34948", "#f0efec", "#2a78d6"])
ABORTS = LinearSegmentedColormap.from_list("aborts", ["#2a78d6", "#f0efec", "#e34948"])
DB_NAMES = {"mysql": "MySQL", "pg": "PostgreSQL"}
INK, INK_MUTED, SURFACE = "#0b0b0b", "#52514e", "#fcfcfb"


def load(path):
    out = {}
    for r in csv.DictReader(open(path)):
        system, _, db = r["variant"].split("@")[0].rpartition("-")
        out[(int(r["threads"]), float(r["skew"]), db, system)] = r
    return out


def main(out):
    d = load(out / "summary.csv")
    threads = sorted({k[0] for k in d})
    skews = sorted({k[1] for k in d})
    dbs = [db for db in DB_NAMES if any(k[2] == db for k in d)]

    def grid(db, f):
        return [[f(d[(t, s, db, "speculative")], d[(t, s, db, "nonspeculative")]) for s in skews] for t in threads]

    rows = [
        ("Throughput, speculative vs non-speculative", "%+.0f%%", GAIN,
         lambda sp, ns: 100 * (float(sp["tps"]) / float(ns["tps"]) - 1)),
        ("Extra abort rate caused by speculation", "%+.1f pp", ABORTS,
         lambda sp, ns: 100 * (float(sp["abort_rate"]) - float(ns["abort_rate"]))),
    ]
    plt.rcParams.update({"font.size": 10, "text.color": INK, "axes.labelcolor": INK_MUTED,
                         "xtick.color": INK_MUTED, "ytick.color": INK_MUTED})
    fig, axes = plt.subplots(len(rows), len(dbs), figsize=(5.2 * len(dbs), 4.2 * len(rows)), squeeze=False,
                             facecolor=SURFACE)
    for i, (title, fmt, cmap, f) in enumerate(rows):
        grids = {db: grid(db, f) for db in dbs}
        span = max(abs(v) for g in grids.values() for row in g for v in row) or 1
        norm = TwoSlopeNorm(vcenter=0, vmin=-span, vmax=span)   # same scale for both server kinds
        for j, db in enumerate(dbs):
            ax = axes[i][j]
            ax.imshow(grids[db], cmap=cmap, norm=norm, aspect="auto", origin="lower")
            for y, row in enumerate(grids[db]):
                for x, v in enumerate(row):
                    ax.text(x, y, fmt % v, ha="center", va="center", color=INK, fontsize=10,
                            fontweight="bold" if abs(v) >= span / 2 else "normal")
            ax.set_xticks(range(len(skews)), [f"{s:g}" for s in skews])
            ax.set_yticks(range(len(threads)), [str(t) for t in threads])
            ax.set_xlabel("Skew")
            ax.set_ylabel("Threads")
            ax.set_title(f"{title}\n{DB_NAMES[db]}", color=INK, fontsize=11, fontweight="bold")
            for side in ax.spines.values():
                side.set_visible(False)
            ax.tick_params(length=0)
            # white gaps between cells
            ax.set_xticks([x - 0.5 for x in range(1, len(skews))], minor=True)
            ax.set_yticks([y - 0.5 for y in range(1, len(threads))], minor=True)
            ax.grid(which="minor", color=SURFACE, linewidth=3)
            ax.tick_params(which="minor", length=0)
    fig.text(0.5, 0.005, "Wound-wait, think time 2 ms, 3 reps; blue = better for speculation, red = worse",
             ha="center", color=INK_MUTED, fontsize=9)
    fig.tight_layout(rect=(0, 0.02, 1, 1))
    target = out / "gain.png"
    fig.savefig(target, dpi=150, facecolor=SURFACE)
    print(target)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(Path(sys.argv[1]))
