#!/usr/bin/env python3
"""Speculation's gain against the delay between a txn's operations: RESULTS_DIR/d<delay µs>/{nonspec,spec-*}, each a
bench-peak.py --out run (the speculative curve is the best spec-* config at each thread count).

    scripts/bench-delay-gain.py RESULTS_DIR [--out DIR]

Writes delay-gain.png: left, speculation's gain against the delay, peak against peak and at every thread count both
systems ran; right, each system's peak throughput against the delay. And delay-gain.md, the same as a table.
"""
import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

INK, MUTED, SURFACE, GRID = "#1f1f1e", "#52514e", "#fcfcfb", "#e1e0d9"
SERIES = [("#2a78d6", "o"), ("#eb6834", "s"), ("#1baf7a", "^"), ("#eda100", "D"), ("#e87ba4", "v")]


def curve(cfg_dir):
    return {int(r["threads"]): float(r["tps"]) for r in csv.DictReader(open(cfg_dir / "points.csv"))}


def peak(cur):
    t = max(cur, key=cur.get)
    return t, cur[t]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", type=Path)
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()
    out = args.out or args.results
    rows = []
    for d in sorted(args.results.glob("d*"), key=lambda p: int(p.name[1:])):
        if not (d / "nonspec" / "points.csv").exists():
            continue
        non = curve(d / "nonspec")
        spec = {}
        for c in d.glob("spec-*"):
            for t, v in curve(c).items():
                spec[t] = max(spec.get(t, 0), v)
        rows.append((int(d.name[1:]) / 1000, non, spec))
    threads = sorted(set.intersection(*[set(n) & set(s) for _, n, s in rows]))
    delays = [ms for ms, _, _ in rows]
    plt.rcParams.update({"figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
                         "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
                         "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
                         "axes.spines.top": False, "axes.spines.right": False, "font.size": 9, "lines.linewidth": 2})
    fig, (a, b) = plt.subplots(1, 2, figsize=(12.5, 4.4))
    series = [("peak vs peak", [100 * (peak(s)[1] / peak(n)[1] - 1) for _, n, s in rows])]
    series += [(f"at {t} threads", [100 * (s[t] / n[t] - 1) for _, n, s in rows]) for t in threads]
    for (label, ys), (color, marker) in zip(series, SERIES):
        a.plot(delays, ys, label=label, color=color, marker=marker, linewidth=2.6 if label == "peak vs peak" else 1.6)
    a.axhline(0, color=MUTED, linewidth=0.9)
    a.set_xticks(delays)
    a.set_xlabel("mean delay between a txn's operations, ms")
    a.set_ylabel("speculative vs non-speculative")
    a.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter())
    a.set_title("speculation's gain against the delay", color=INK)
    a.legend(frameon=False, fontsize=8)
    for (label, idx), (color, marker) in zip((("non-speculative", 1), ("speculative", 2)), [(MUTED, "x"), SERIES[0]]):
        pts = [peak(r[idx]) for r in rows]
        b.plot(delays, [v for _, v in pts], label=label, color=color, marker=marker,
               linestyle="--" if idx == 1 else "-")
        for ms, (t, v) in zip(delays, pts):
            b.annotate(f"@{t}", (ms, v), textcoords="offset points", xytext=(4, 4), fontsize=7, color=MUTED)
    b.set_yscale("log")
    b.set_xticks(delays)
    b.set_xlabel("mean delay between a txn's operations, ms")
    b.set_ylabel("peak committed txns / s (log)")
    b.set_title("peak throughput (@ its thread count)", color=INK)
    b.legend(frameon=False, fontsize=8)
    fig.tight_layout()
    fig.savefig(out / "delay-gain.png", dpi=150, bbox_inches="tight")
    lines = ["| delay ms | non-spec peak | spec peak | gain peak vs peak | "
             + " | ".join(f"gain @ {t} thr" for t in threads) + " |", "|" + "---|" * (4 + len(threads))]
    for (ms, n, s), i in zip(rows, range(len(rows))):
        (tn, vn), (ts, vs) = peak(n), peak(s)
        lines.append(f"| {ms:g} | {vn:.0f} @ {tn} | {vs:.0f} @ {ts} | {series[0][1][i]:+.0f}% | "
                     + " | ".join(f"{ys[i]:+.0f}%" for _, ys in series[1:]) + " |")
    (out / "delay-gain.md").write_text("# Speculation's gain against the delay between operations\n\n"
                                       + "\n".join(lines) + "\n")
    print(out / "delay-gain.png")


if __name__ == "__main__":
    main()
