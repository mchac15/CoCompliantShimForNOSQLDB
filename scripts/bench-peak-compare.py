#!/usr/bin/env python3
"""Compares non-speculative and speculative peak throughput across workloads (e.g. operations per txn and key
space): each workload is a directory of bench-peak.py --out runs, nonspec and spec-* (the speculative peak is the
best of them), at one skew.

    scripts/bench-peak-compare.py OUT_PNG --skew 0.95 LABEL=DIR [LABEL=DIR ...]

Left: each workload's peak tps per system (@ its thread count; ↑: the peak is the largest thread count run, so the
system may peak higher). Right: speculation's gain over non-speculative, peak against peak and at every thread count
both systems ran. Also writes OUT_PNG with .md: the same numbers as a table.
"""
import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

INK, MUTED, SURFACE, GRID = "#1f1f1e", "#52514e", "#fcfcfb", "#e1e0d9"
NONSPEC, SPEC = "#8a8984", "#2a78d6"
GAINS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]   # peak vs peak, then one per shared thread count


def curve(cfg_dir, skew):
    """{threads: tps} of one bench-peak run at that skew."""
    return {int(r["threads"]): float(r["tps"]) for r in csv.DictReader(open(cfg_dir / "points.csv"))
            if abs(float(r["skew"]) - skew) < 1e-9}


def load(d, skew):
    non = curve(d / "nonspec", skew)
    spec = {}
    for c in sorted(d.glob("spec-*")):
        for t, v in curve(c, skew).items():
            spec[t] = max(spec.get(t, 0), v)   # the best speculative config at each thread count
    return non, spec


def peak(cur):
    t = max(cur, key=cur.get)
    return t, cur[t], t == max(cur)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("out", type=Path)
    ap.add_argument("--skew", type=float, required=True)
    ap.add_argument("workloads", nargs="+", help="LABEL=DIR")
    args = ap.parse_args()
    rows = []
    for w in args.workloads:
        label, d = w.split("=", 1)
        non, spec = load(Path(d), args.skew)
        rows.append((label, non, spec))
    shared = sorted(set.intersection(*[set(n) & set(s) for _, n, s in rows]))

    plt.rcParams.update({"figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
                         "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
                         "text.color": INK, "axes.spines.top": False, "axes.spines.right": False, "font.size": 9})
    fig, (a, b) = plt.subplots(1, 2, figsize=(13, 4.4), gridspec_kw={"width_ratios": [1.15, 1]})
    xs = range(len(rows))
    for ax in (a, b):
        ax.grid(True, axis="y", color=GRID, linewidth=0.6, zorder=0)
        ax.set_xticks(list(xs), [label for label, *_ in rows])
    lines = ["| workload | non-spec peak | spec peak | gain (peak vs peak) | "
             + " | ".join(f"gain @ {t} thr" for t in shared) + " |", "|" + "---|" * (4 + len(shared))]
    for i, (label, non, spec) in enumerate(rows):
        for dx, cur, color, name in ((-0.2, non, NONSPEC, "non-speculative"), (0.2, spec, SPEC, "speculative")):
            t, v, edge = peak(cur)
            a.bar(i + dx, v, width=0.38, color=color, label=name if i == 0 else None, zorder=3)
            a.text(i + dx, v, f"{v:.0f}\n@{t}{' ↑' if edge else ''}", ha="center", va="bottom", fontsize=7.5,
                   color=INK)
        (tn, vn, en), (ts, vs, es) = peak(non), peak(spec)
        gains = [100 * (vs / vn - 1)] + [100 * (spec[t] / non[t] - 1) for t in shared]
        names = ["peak vs peak"] + [f"at {t} threads" for t in shared]
        width = 0.8 / len(gains)
        for k, (g, name) in enumerate(zip(gains, names)):
            x = i + (k - (len(gains) - 1) / 2) * width
            b.bar(x, g, width=width * 0.9, color=GAINS[k % len(GAINS)], label=name if i == 0 else None, zorder=3)
            b.text(x, g, f"{g:+.0f}%", ha="center", va="bottom" if g >= 0 else "top", fontsize=7.5, color=INK)
        lines.append(f"| {label.replace(chr(10), ' ')} | {vn:.0f} @ {tn}{' (edge)' if en else ''} | {vs:.0f} @ {ts}{' (edge)' if es else ''} | "
                     + " | ".join(f"{g:+.0f}%" for g in gains) + " |")
    a.set_ylabel("peak committed txns / s")
    a.set_title(f"peak throughput, skew {args.skew:g}", color=INK)
    a.margins(y=0.18)
    a.legend(frameon=False, loc="upper right")
    b.axhline(0, color=MUTED, linewidth=0.8)
    b.set_ylabel("speculative vs non-speculative")
    b.set_title("speculation's gain", color=INK)
    b.margins(y=0.15)
    b.legend(frameon=False, loc="upper left", fontsize=8)
    fig.text(0.5, -0.03, "speculative = best speculative config at each thread count; ↑ = peak at the largest thread "
             "count run (may peak higher)", ha="center", color=MUTED, fontsize=8)
    fig.tight_layout()
    fig.savefig(args.out, dpi=150, bbox_inches="tight")
    args.out.with_suffix(".md").write_text(f"# Peak throughput by workload, skew {args.skew:g}\n\n" + "\n".join(lines) + "\n")
    print(args.out)


if __name__ == "__main__":
    main()
