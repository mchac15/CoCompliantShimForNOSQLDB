#!/usr/bin/env python3
"""Plots a --retry-age comparison: bench-peak.py outputs named <system>-<fresh|first> under RESULTS_DIR.

    scripts/bench-retry-age-plot.py RESULTS_DIR [--out DIR]

Writes retry-age.png (throughput, attempts per committed txn p50 / p99, wounds and prepare wait-die NO votes per
100 branch commits, creation-to-commit latency p50 / p99, all vs threads) and retry-age.md (the same as a table).
"""
import argparse
import csv
import re
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

INK, MUTED, SURFACE, GRID = "#1f1f1e", "#52514e", "#fcfcfb", "#e1e0d9"
SYSTEMS = {"speculative": "#2a78d6", "nonspeculative": "#eb6834"}
AGES = {"fresh": dict(linestyle="-", marker="o"), "first": dict(linestyle="--", marker="s")}


def last(log, prefix):
    line = ""
    for l in open(log, errors="replace"):
        if l.startswith(prefix + " "):
            line = l
    return {k: int(v) for k, v in re.findall(r"(\w+)=(-?\d+)\b", line)}


def pct(xs, q):
    return xs[min(int(q * len(xs)), len(xs) - 1)]


def load(results):
    out = {}
    for sys in SYSTEMS:
        for age in AGES:
            cfg = results / f"{sys}-{age}"
            if not (cfg / "points.csv").exists():
                continue
            d = next(cfg.glob("mysql-*"))
            pts = []
            for p in csv.DictReader(open(cfg / "points.csv")):
                t = int(p["threads"])
                txns = [r for r in csv.DictReader(open(d / f"txns-t{t}.csv")) if r["outcome"] == "COMMITTED"]
                att = sorted(int(r["attempts"]) for r in txns)
                lat = sorted(int(r["latency_us"]) / 1000 for r in txns)
                st = {}
                for log in (d / f"t{t}-nodes").glob("*-s[0-9].log"):
                    for k, v in last(log, "stats").items():
                        st[k] = st.get(k, 0) + v
                c = max(st.get("commits", 0), 1)
                pts.append(dict(threads=t, tps=float(p["tps"]), att_mean=statistics.mean(att),
                                att_p50=pct(att, .5), att_p99=pct(att, .99), att_max=att[-1],
                                lat_p50=pct(lat, .5), lat_p99=pct(lat, .99), lat_mean=statistics.mean(lat),
                                wounded=100 * st.get("wounded", 0) / c, cascade=100 * st.get("cascade", 0) / c,
                                coordinator=100 * st.get("coordinator", 0) / c,
                                pwd=100 * st.get("prepare_wait_die", 0) / c))
            out[(sys, age)] = sorted(pts, key=lambda p: p["threads"])
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", type=Path)
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()
    out = args.out or args.results
    data = load(args.results)
    plt.rcParams.update({"figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
                         "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
                         "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
                         "axes.spines.top": False, "axes.spines.right": False, "font.size": 9, "lines.linewidth": 2})
    panels = [("tps", None, "committed txns / s"), ("att_p50", "att_p99", "attempts per committed txn\n(p50 thin, p99 bold)"),
              ("wounded", None, "wounds per 100 branch commits"), ("pwd", None, "prepare wait-die NO\nper 100 branch commits"),
              ("lat_p50", "lat_p99", "creation → commit, ms\n(p50 thin, p99 bold)")]
    fig, axes = plt.subplots(1, len(panels), figsize=(3.6 * len(panels), 3.4))
    for ax, (y, y2, title) in zip(axes, panels):
        for (sys, age), pts in data.items():
            xs = [p["threads"] for p in pts]
            kw = dict(color=SYSTEMS[sys], **AGES[age])
            ax.plot(xs, [p[y] for p in pts], label=f"{sys}, {age}", linewidth=1.2 if y2 else 2, **kw)
            if y2:
                ax.plot(xs, [p[y2] for p in pts], linewidth=2.4, **kw)
        ax.set_title(title, color=INK, fontsize=9)
        ax.set_xlabel("client threads")
        ax.set_ylim(bottom=0)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=4, frameon=False, bbox_to_anchor=(0.5, 1.04))
    fig.text(0.5, -0.04, "fresh: every attempt is a new, youngest txn; first: a retry keeps its first attempt's age "
             "(wound-wait in lock(), wait-die in prepare)", ha="center", color=MUTED, fontsize=8)
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(out / "retry-age.png", dpi=150, bbox_inches="tight")
    cols = ["tps", "att_mean", "att_p50", "att_p99", "att_max", "lat_mean", "lat_p50", "lat_p99", "wounded", "cascade",
            "coordinator", "pwd"]
    lines = ["| config | threads | " + " | ".join(cols) + " |", "|" + "---|" * (len(cols) + 2)]
    for (sys, age), pts in data.items():
        for p in pts:
            lines.append(f"| {sys}, {age} | {p['threads']} | " + " | ".join(f"{p[c]:.0f}" if c != "att_mean"
                                                                          else f"{p[c]:.1f}" for c in cols) + " |")
    (out / "retry-age.md").write_text("# --retry-age fresh vs first\n\nlatencies in ms (creation to commit); aborts "
                                      "per 100 branch commits\n\n" + "\n".join(lines) + "\n")
    print(out / "retry-age.png")


if __name__ == "__main__":
    main()
