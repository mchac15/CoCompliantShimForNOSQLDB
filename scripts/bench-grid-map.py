#!/usr/bin/env python3
"""Where speculation wins, over a skew x branch-count grid: RESULTS_DIR/s<skew>/b<branches>/{nonspec,spec-*}, each
a bench-peak.py --out run (the speculative curve is the best spec-* config at each thread count).

    scripts/bench-grid-map.py RESULTS_DIR [--out DIR] [--at-threads 25]

Writes grid-map.png: two heatmaps, speculation's gain peak against peak (each system at its best thread count) and
at --at-threads, each cell labeled with both throughputs; and grid-map.md, the same as a table, with the thread
count of each peak (↑: the largest one run, the system may peak higher).
"""
import argparse
import csv
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm

INK, MUTED, SURFACE = "#1f1f1e", "#52514e", "#fcfcfb"
DIVERGING = LinearSegmentedColormap.from_list("gain", ["#e34948", "#f0efec", "#2a78d6"])


def curve(cfg_dir):
    return {int(r["threads"]): float(r["tps"]) for r in csv.DictReader(open(cfg_dir / "points.csv"))}


def load(results):
    cells = {}
    for sdir in results.glob("s*"):
        skew = float(sdir.name[1:])
        for bdir in sdir.glob("b*"):
            b = int(bdir.name[1:])
            if not (bdir / "nonspec" / "points.csv").exists():
                continue
            non = curve(bdir / "nonspec")
            spec = {}
            for c in bdir.glob("spec-*"):
                if (c / "points.csv").exists():
                    for t, v in curve(c).items():
                        spec[t] = max(spec.get(t, 0), v)
            if spec:
                cells[(skew, b)] = (non, spec)
    return cells


def peak(cur):
    t = max(cur, key=cur.get)
    return t, cur[t], t == max(cur)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", type=Path)
    ap.add_argument("--out", type=Path)
    ap.add_argument("--at-threads", type=int, default=25)
    args = ap.parse_args()
    out = args.out or args.results
    cells = load(args.results)
    skews = sorted({s for s, _ in cells})
    branches = sorted({b for _, b in cells})
    plt.rcParams.update({"figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
                         "text.color": INK, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
                         "font.size": 9})
    fig, axes = plt.subplots(1, 2, figsize=(12, 1.1 * len(skews) + 2))
    lines = ["| skew | branches | non-spec peak | spec peak | gain peak vs peak | "
             f"non-spec @ {args.at_threads} | spec @ {args.at_threads} | gain @ {args.at_threads} |", "|---" * 8 + "|"]
    grids = []
    for mode in ("peak", "at"):
        m, txt = [], []
        for s in skews:
            row, trow = [], []
            for b in branches:
                if (s, b) not in cells:
                    row.append(float("nan"))
                    trow.append("")
                    continue
                non, spec = cells[(s, b)]
                if mode == "peak":
                    (tn, vn, en), (ts, vs, es) = peak(non), peak(spec)
                    label = f"{vs:.0f}@{ts}{'↑' if es else ''}\nvs {vn:.0f}@{tn}{'↑' if en else ''}"
                else:
                    t = args.at_threads
                    vn, vs = non.get(t, float("nan")), spec.get(t, float("nan"))
                    label = f"{vs:.0f} vs {vn:.0f}"
                g = 100 * (vs / vn - 1)
                row.append(g)
                trow.append(f"{g:+.0f}%\n{label}")
            m.append(row)
            txt.append(trow)
        grids.append((m, txt))
    lim = max(abs(v) for m, _ in grids for row in m for v in row if v == v)
    norm = TwoSlopeNorm(vmin=-lim, vcenter=0, vmax=lim)
    titles = ["speculation's gain, each system at its peak", f"speculation's gain at {args.at_threads} threads"]
    for ax, (m, txt), title in zip(axes, grids, titles):
        im = ax.imshow(m, cmap=DIVERGING, norm=norm, aspect="auto", origin="lower")
        for i in range(len(skews)):
            for j in range(len(branches)):
                ax.text(j, i, txt[i][j], ha="center", va="center", fontsize=8, color=INK)
        ax.set_xticks(range(len(branches)), [str(b) for b in branches])
        ax.set_yticks(range(len(skews)), [f"{s:g}" for s in skews])
        ax.set_xlabel("branches per txn")
        ax.set_ylabel("skew")
        ax.set_title(title, color=INK)
    cb = fig.colorbar(im, ax=axes, shrink=0.85, pad=0.02)
    cb.set_label("speculative vs non-speculative", color=MUTED)
    fig.text(0.45, -0.03, "cell: gain, then spec tps @ threads vs non-spec tps @ threads (↑: peak at the largest "
             "thread count run)", ha="center", color=MUTED, fontsize=8)
    fig.savefig(out / "grid-map.png", dpi=150, bbox_inches="tight")
    for s in skews:
        for b in branches:
            if (s, b) not in cells:
                continue
            non, spec = cells[(s, b)]
            (tn, vn, en), (ts, vs, es) = peak(non), peak(spec)
            t = args.at_threads
            lines.append(f"| {s:g} | {b} | {vn:.0f} @ {tn}{' ↑' if en else ''} | {vs:.0f} @ {ts}{' ↑' if es else ''} | "
                         f"{100 * (vs / vn - 1):+.0f}% | {non.get(t, float('nan')):.0f} | {spec.get(t, float('nan')):.0f} | "
                         f"{100 * (spec.get(t, float('nan')) / non.get(t, float('nan')) - 1):+.0f}% |")
    (out / "grid-map.md").write_text("# Speculation over skew x branches\n\n" + "\n".join(lines) + "\n")
    print(out / "grid-map.png")


if __name__ == "__main__":
    main()
