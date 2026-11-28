#!/usr/bin/env python3
"""Does speculation gain more the higher the skew? Trend test of a bench-peak.py sweep over many skews.

    scripts/bench-skew-trend.py DIR        # DIR of bench-peak.py --threads ... --skews ...; writes trend.md,
                                           # trend.csv, gain-heatmap.png, gain-vs-skew.png

Per server kind and thread count, the gain of the speculative shim over the non-speculative one
(mean tps of each over its reps, a run's tps being the median of its per-second samples) at every
skew, then:

  - slope: least-squares fit of gain (percentage points) on skew, reported per +0.01 of skew;
  - p: permutation test of that slope, 10000 shuffles of the skews (two-sided: how often a random
    order gives a slope at least as steep); no assumption on the distribution of the gains;
  - noise: the gain's own uncertainty at one skew, from the reps (half the range of the gains over
    every pairing of a speculative rep with a non-speculative rep, averaged over the skews), against
    the scatter of the gains around the fitted line. If the gains were random around a constant, the
    slope would be about 0 and p large; a real tendency gives a slope well beyond the noise and a small
    p, with the same sign at neighboring thread counts and on both server kinds.

Needs matplotlib for the figures (the tables are written without it).
"""

import csv
import random
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import importlib.util  # noqa: E402

_spec = importlib.util.spec_from_file_location("bench_peak", Path(__file__).resolve().parent / "bench-peak.py")
bench_peak = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench_peak)

DB_NAMES = {"mysql": "MySQL", "pg": "PostgreSQL"}
INK, INK_MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
DB_COLORS = {"mysql": ("#2a78d6", "o"), "pg": ("#eb6834", "s")}
PERMUTATIONS = 10000


def rep_tps(out, db, skew, system, threads):
    d = bench_peak.config_dir(out, db, skew, system)
    if not (d / f"t{threads}.done").exists():
        return []
    return bench_peak.robust_tps(d, threads)


def slope(xs, ys):
    mx, my = statistics.mean(xs), statistics.mean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx if sxx else 0.0


def permutation_p(xs, ys, rng):
    observed = abs(slope(xs, ys))
    ys = list(ys)
    hits = 0
    for _ in range(PERMUTATIONS):
        rng.shuffle(ys)
        hits += abs(slope(xs, ys)) >= observed - 1e-12
    return (hits + 1) / (PERMUTATIONS + 1)


def main(out):
    out = Path(out)
    points = bench_peak.load_points(out)
    dbs = [db for db in DB_NAMES if any(p["db"] == db for p in points)]
    skews = sorted({p["skew"] for p in points})
    threads = sorted({p["threads"] for p in points})

    # gain per (db, threads, skew): mean over reps of each system; half-range over every rep pairing
    gain = {}
    for db in dbs:
        for t in threads:
            for s in skews:
                a, b = rep_tps(out, db, s, "speculative", t), rep_tps(out, db, s, "nonspeculative", t)
                if a and b:
                    pairs = [100 * (x / y - 1) for x in a for y in b]
                    gain[(db, t, s)] = (100 * (statistics.mean(a) / statistics.mean(b) - 1),
                                        (max(pairs) - min(pairs)) / 2, min(pairs), max(pairs))

    rng = random.Random(1)
    trends = []
    for db in dbs:
        for t in threads:
            xs = [s for s in skews if (db, t, s) in gain]
            if len(xs) < 4:
                continue
            ys = [gain[(db, t, s)][0] for s in xs]
            b = slope(xs, ys)
            a = statistics.mean(ys) - b * statistics.mean(xs)
            resid = [y - (a + b * x) for x, y in zip(xs, ys)]
            trends.append(dict(db=db, threads=t, skews=len(xs), slope_pp_per_001=round(b / 100, 2),
                               gain_at_first=round(a + b * xs[0], 1), gain_at_last=round(a + b * xs[-1], 1),
                               p=round(permutation_p(xs, ys, rng), 4),
                               scatter_pp=round(statistics.stdev(resid), 2) if len(resid) > 2 else "",
                               rep_noise_pp=round(statistics.mean(gain[(db, t, s)][1] for s in xs), 2)))

    with open(out / "trend.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(trends[0]))
        w.writeheader()
        w.writerows(trends)

    lines = ["# Speculation's gain against skew", "",
             "Gain = speculative / non-speculative throughput - 1, at equal thread count (mean over reps; a run's",
             "tps = median of its per-second samples). Missing cells were not measured.", ""]
    for db in dbs:
        lines += [f"## {DB_NAMES[db]}: gain (%) by threads x skew", "",
                  "| threads | " + " | ".join(f"{s:g}" for s in skews) + " |",
                  "|---|" + "---|" * len(skews)]
        for t in threads:
            cells = [f"{gain[(db, t, s)][0]:+.1f}" if (db, t, s) in gain else "-" for s in skews]
            lines.append(f"| {t} | " + " | ".join(cells) + " |")
        lines.append("")
    lines += ["## Trend per thread count", "",
              "slope = fitted change of the gain per +0.01 of skew (percentage points); p = permutation test of the "
              "slope (10000 shuffles); scatter = std of the gains around the fitted line; rep noise = the gain's "
              "uncertainty at one skew from the reps (mean half-range). A real tendency: |slope| x 9 steps well "
              "beyond the noise, small p, consistent across thread counts and server kinds.", "",
              "| db | threads | skews | slope pp / 0.01 | fitted gain at 0.90 -> 0.99 | p | scatter pp | rep noise pp |",
              "|---|---|---|---|---|---|---|---|"]
    for r in trends:
        lines.append(f"| {r['db']} | {r['threads']} | {r['skews']} | {r['slope_pp_per_001']:+.2f} "
                     f"| {r['gain_at_first']:+.1f} -> {r['gain_at_last']:+.1f} | {r['p']} | {r['scatter_pp']} "
                     f"| {r['rep_noise_pp']} |")
    text = "\n".join(lines) + "\n"
    (out / "trend.md").write_text(text)
    print(text)

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm
    except ImportError:
        print("matplotlib is missing: no figures")
        return
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK_MUTED,
                         "xtick.color": INK_MUTED, "ytick.color": INK_MUTED, "text.color": INK})

    # heatmap: threads x skew, diverging blue (speculation better) / red (worse) around a gray zero
    cmap = LinearSegmentedColormap.from_list("gain", ["#e34948", "#f0efec", "#2a78d6"])
    span = max(abs(v[0]) for v in gain.values()) or 1
    norm = TwoSlopeNorm(vcenter=0, vmin=-span, vmax=span)
    fig, axes = plt.subplots(1, len(dbs), figsize=(6.4 * len(dbs), 5.2), squeeze=False, facecolor=SURFACE)
    for ax, db in zip(axes[0], dbs):
        grid = [[gain[(db, t, s)][0] if (db, t, s) in gain else float("nan") for s in skews] for t in threads]
        ax.imshow(grid, cmap=cmap, norm=norm, aspect="auto", origin="lower")
        for y, t in enumerate(threads):
            for x, s in enumerate(skews):
                v = gain.get((db, t, s))
                ax.text(x, y, f"{v[0]:+.0f}" if v else "n/m", ha="center", va="center",
                        color=INK if v else INK_MUTED, fontsize=8)
        ax.set_xticks(range(len(skews)), [f"{s:g}" for s in skews])
        ax.set_yticks(range(len(threads)), [str(t) for t in threads])
        ax.set_xticks([x - 0.5 for x in range(1, len(skews))], minor=True)
        ax.set_yticks([y - 0.5 for y in range(1, len(threads))], minor=True)
        ax.grid(which="minor", color=SURFACE, linewidth=2)
        ax.tick_params(which="both", length=0)
        for side in ax.spines.values():
            side.set_visible(False)
        ax.set_xlabel("Skew")
        ax.set_ylabel("Threads")
        ax.set_title(f"{DB_NAMES[db]}: speculative vs non-speculative throughput (%)", color=INK, fontsize=11,
                     fontweight="bold")
    fig.text(0.5, 0.01, "Blue = speculation faster, red = slower; n/m = not measured", ha="center",
             color=INK_MUTED, fontsize=9)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(out / "gain-heatmap.png", dpi=150, facecolor=SURFACE)
    plt.close(fig)
    print(out / "gain-heatmap.png")

    # gain against skew, one panel per thread count, both server kinds, with the fitted lines
    cols = 4
    rows = (len(threads) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(4.3 * cols, 3.3 * rows), squeeze=False, sharex=True, sharey=True,
                             facecolor=SURFACE)
    lo = min(v[2] for v in gain.values())
    hi = max(v[3] for v in gain.values())
    for k, t in enumerate(threads):
        ax = axes[k // cols][k % cols]
        ax.set_facecolor(SURFACE)
        ax.grid(True, color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.tick_params(length=0)
        ax.axhline(0, color=INK_MUTED, linewidth=1, zorder=2)
        notes = []
        for db in dbs:
            color, marker = DB_COLORS[db]
            xs = [s for s in skews if (db, t, s) in gain]
            if not xs:
                continue
            g = [gain[(db, t, s)] for s in xs]
            ax.vlines(xs, [v[2] for v in g], [v[3] for v in g], color=color, linewidth=1.2, alpha=0.6, zorder=3)
            ax.plot(xs, [v[0] for v in g], color=color, marker=marker, linewidth=2, markersize=5,
                    markeredgecolor=SURFACE, markeredgewidth=1.2, zorder=4, label=DB_NAMES[db] if k == 0 else None)
            tr = next((r for r in trends if (r["db"], r["threads"]) == (db, t)), None)
            if tr:
                b = tr["slope_pp_per_001"] * 100
                a = statistics.mean(v[0] for v in g) - b * statistics.mean(xs)
                ax.plot([xs[0], xs[-1]], [a + b * xs[0], a + b * xs[-1]], color=color, linewidth=1.2,
                        linestyle=(0, (4, 2)), zorder=3)
                notes.append(f"{DB_NAMES[db]}: {tr['slope_pp_per_001']:+.1f} pp/0.01, p={tr['p']:.3g}")
        ax.set_ylim(lo - 3, hi + 3)
        ax.set_title(f"{t} threads", color=INK, fontsize=11, fontweight="bold")
        ax.text(0.02, 0.98, "\n".join(notes), transform=ax.transAxes, ha="left", va="top", fontsize=8, color=INK)
        if k % cols == 0:
            ax.set_ylabel("Gain of speculation (%)")
        if k // cols == rows - 1:
            ax.set_xlabel("Skew")
    for k in range(len(threads), rows * cols):
        axes[k // cols][k % cols].set_visible(False)
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(dbs), frameon=False, bbox_to_anchor=(0.5, 1.0))
    height = fig.get_figheight()
    fig.text(0.5, 1 - 0.38 / height, "Whisker = range over every pairing of the reps; dashed = least-squares fit "
             "(slope per +0.01 of skew, permutation p)", ha="center", va="top", color=INK_MUTED, fontsize=9)
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.65 / height))
    fig.savefig(out / "gain-vs-skew.png", dpi=150, facecolor=SURFACE)
    plt.close(fig)
    print(out / "gain-vs-skew.png")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
