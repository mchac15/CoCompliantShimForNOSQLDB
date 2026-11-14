#!/usr/bin/env python3
"""Latency against throughput: each point a thread count, the latency of the committed txns from their creation
(first attempt's begin) to their commit, retries included, median and p99.

    scripts/bench-latency-throughput.py OUT_PNG --skew 0.95 PANEL/SERIES=CONFIG_DIR [...]

CONFIG_DIR is a bench-peak.py --out run of one system; PANEL groups series into one column (e.g. a randomness
level), SERIES names a curve (e.g. speculative). Top row: median latency; bottom row: p99. Also writes OUT_PNG
with .md: every point as a table.
"""
import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

INK, MUTED, SURFACE, GRID = "#1f1f1e", "#52514e", "#fcfcfb", "#e1e0d9"
SERIES = [("#2a78d6", "o"), ("#eb6834", "s"), ("#1baf7a", "^"), ("#eda100", "D")]


def pct(xs, q):
    return xs[min(int(q * len(xs)), len(xs) - 1)]


def load(cfg_dir, skew):
    pdir = next(p for p in cfg_dir.iterdir() if p.is_dir() and p.name.startswith(("mysql-", "pg-")))
    pts = []
    for r in csv.DictReader(open(cfg_dir / "points.csv")):
        if abs(float(r["skew"]) - skew) > 1e-9:
            continue
        t = int(r["threads"])
        lat = sorted(int(x["latency_us"]) / 1000 for x in csv.DictReader(open(pdir / f"txns-t{t}.csv"))
                     if x["outcome"] == "COMMITTED")
        pts.append(dict(threads=t, tps=float(r["tps"]), p50=pct(lat, .5), p99=pct(lat, .99)))
    return sorted(pts, key=lambda p: p["threads"])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("out", type=Path)
    ap.add_argument("--skew", type=float, required=True)
    ap.add_argument("runs", nargs="+", help="PANEL/SERIES=CONFIG_DIR")
    args = ap.parse_args()
    panels, series = [], []
    data = {}
    for r in args.runs:
        key, d = r.split("=", 1)
        panel, name = key.split("/", 1)
        panels += [panel] if panel not in panels else []
        series += [name] if name not in series else []
        data[(panel, name)] = load(Path(d), args.skew)
    look = {s: dict(color=c, marker=m) for s, (c, m) in zip(series, SERIES)}
    plt.rcParams.update({"figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
                         "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
                         "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
                         "axes.spines.top": False, "axes.spines.right": False, "font.size": 9, "lines.linewidth": 2})
    fig, axes = plt.subplots(2, len(panels), figsize=(3.6 * len(panels), 6.4), squeeze=False, sharex=True,
                             sharey="row")
    for j, panel in enumerate(panels):
        for i, (y, title) in enumerate((("p50", "median latency"), ("p99", "p99 latency"))):
            ax = axes[i][j]
            for s in series:
                pts = data.get((panel, s), [])
                if not pts:
                    continue
                ax.plot([p["tps"] for p in pts], [p[y] for p in pts], label=s, **look[s])
                for p in pts:
                    ax.annotate(str(p["threads"]), (p["tps"], p[y]), textcoords="offset points", xytext=(4, 4),
                                fontsize=7, color=MUTED)
            ax.set_ylim(bottom=0)
            ax.set_title(f"{panel}: {title}", color=INK, fontsize=9)
            if j == 0:
                ax.set_ylabel(f"{title}, ms (creation → commit)")
            if i == 1:
                ax.set_xlabel("committed txns / s")
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False, bbox_to_anchor=(0.5, 1.02))
    fig.text(0.5, -0.02, "each point: one thread count (labeled); latency over the committed txns, retries included",
             ha="center", color=MUTED, fontsize=8)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(args.out, dpi=150, bbox_inches="tight")
    lines = ["| panel | series | threads | tps | p50 ms | p99 ms |", "|---|---|---|---|---|---|"]
    for (panel, s), pts in data.items():
        for p in pts:
            lines.append(f"| {panel} | {s} | {p['threads']} | {p['tps']:.0f} | {p['p50']:.0f} | {p['p99']:.0f} |")
    args.out.with_suffix(".md").write_text(f"# Latency vs throughput, skew {args.skew:g}\n\n" + "\n".join(lines) + "\n")
    print(args.out)


if __name__ == "__main__":
    main()
