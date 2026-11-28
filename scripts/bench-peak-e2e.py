#!/usr/bin/env python3
"""Where a committed txn's time goes, from its creation (first attempt's begin) to its commit, at one point per
system: the earlier failed attempts (split by abort cause in the proportions of the shims' lost time) and the
final, committed attempt (lock waits of its branches, which run one after the other, prepare wait and 2PC
decision wait, which they share, and execution: work + think time + the rest).

    scripts/bench-peak-e2e.py OUT_PNG LABEL=POINT_DIR:THREADS [LABEL=POINT_DIR:THREADS ...]

POINT_DIR is a bench-peak config directory (e.g. .../nonspec/mysql-skew0.99-nonspeculative). Also writes OUT_PNG
with .md: the same numbers as a table.
"""
import csv
import re
import statistics
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

INK, MUTED, SURFACE, GRID = "#1f1f1e", "#52514e", "#fcfcfb", "#e1e0d9"
LOST = [("wounded", "retries: wounded", "#eda100"), ("cascade", "retries: cascade", "#e87ba4"),
        ("coordinator", "retries: coordinator", "#008300"),
        ("prepare_wait_die", "retries: prepare wait-die", "#6250d6"),
        ("upgrade_conflict", "retries: upgrade conflict", "#8a8984"), ("lock_timeout", "retries: lock timeout", "#b5b4ad")]
FINAL = [("exec", "final attempt: execution (work + think)", "#2a78d6", ""),
         ("lock", "final attempt: lock wait", "#eb6834", ""),
         ("prep", "final attempt: prepare wait", "#1baf7a", ""),
         ("post", "final attempt: 2PC decision wait", "#e34948", "..")]


def last(log, prefix):
    line = ""
    for l in open(log, errors="replace"):
        if l.startswith(prefix + " "):
            line = l
    return {k: int(v) for k, v in re.findall(r"(\w+)=(-?\d+)\b", line)}


def point(d, t):
    txns = [r for r in csv.DictReader(open(d / f"txns-t{t}.csv")) if r["outcome"] == "COMMITTED"]
    run = list(csv.DictReader(open(d / f"t{t}.csv")))[0]
    e2e = statistics.mean(int(r["latency_us"]) for r in txns) / 1000
    final = float(run["mean_us"]) / 1000
    lock = prep = post = 0.0
    lost = dict.fromkeys((k for k, *_ in LOST), 0.0)
    logs = sorted((d / f"t{t}-nodes").glob("*-s[0-9].log"))
    for log in logs:
        st, sp = last(log, "stats"), last(log, "speculation")
        n = st["commits"]
        lock += sp["committed_lock_wait_us_total"] / n / 1000          # the branches lock one after the other
        prep = max(prep, sp["committed_prepare_wait_us_total"] / n / 1000)   # but prepare and wait together
        post = max(post, sp["committed_post_vote_us_total"] / n / 1000)
        for k in lost:
            lost[k] += sp.get(f"lost_{k}_us_total", 0) / n / 1000
    retries = e2e - final
    total_lost = sum(lost.values()) or 1
    out = {f"lost_{k}": retries * v / total_lost for k, v in lost.items()}
    out.update(exec=max(final - lock - prep - post, 0), lock=lock, prep=prep, post=post)
    out.update(e2e=e2e, final=final, tps=float(run["tps"]), threads=t,
               attempts=statistics.mean(int(r["attempts"]) for r in txns),
               p50=statistics.median(int(r["latency_us"]) for r in txns) / 1000)
    return out


def main():
    out = Path(sys.argv[1])
    points = []
    for arg in sys.argv[2:]:
        label, rest = arg.split("=", 1)
        d, t = rest.rsplit(":", 1)
        points.append((label, point(Path(d), int(t))))
    span = max(p["e2e"] for _, p in points)
    plt.rcParams.update({"figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
                         "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
                         "text.color": INK, "font.size": 9})
    fig, ax = plt.subplots(figsize=(11, 1.1 * len(points) + 1.8))
    ax.grid(True, axis="x", color=GRID, linewidth=0.6)
    ax.spines[["top", "right"]].set_visible(False)
    segments = [(f"lost_{k}", name, color, "//") for k, name, color in LOST] + FINAL
    for y, (label, p) in enumerate(points):
        left = 0
        for k, name, color, hatch in segments:
            w = p[k]
            if w <= 0:
                continue
            ax.barh(y, w, left=left, height=0.6, color=color, hatch=hatch, edgecolor=SURFACE, linewidth=2,
                    label=name, zorder=3)
            if w >= 0.04 * span:
                ax.text(left + w / 2, y, f"{w:.0f}", ha="center", va="center", fontsize=8, color=INK, zorder=4,
                        bbox=dict(boxstyle="round,pad=0.15", facecolor=SURFACE, edgecolor="none", alpha=0.85))
            left += w
        ax.text(left + 0.01 * span, y, f"{p['e2e']:.0f} ms mean (p50 {p['p50']:.0f}) · {p['attempts']:.1f} attempts "
                f"· {p['tps']:.0f} tps @ {p['threads']} thr", va="center", fontsize=8, color=MUTED)
        ax.axvline(0, color=GRID)
        ax.plot([p["e2e"] - p["final"]] * 2, [y - 0.38, y + 0.38], color=INK, linewidth=1.2, zorder=5)
    ax.set_yticks(range(len(points)), [label for label, _ in points])
    ax.invert_yaxis()
    ax.set_xlim(0, 1.45 * span)
    ax.set_xlabel("ms from the txn's creation to its commit (mean over committed txns)")
    handles, labels = ax.get_legend_handles_labels()
    seen = {}
    for h, l in zip(handles, labels):
        seen.setdefault(l, h)
    fig.legend(seen.values(), seen.keys(), loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.55, 1.0),
               fontsize=8)
    fig.text(0.55, -0.02, "black tick: start of the final, committed attempt. Retries split by cause in the proportions "
             "of the shims' lost time; final attempt from the shims' budget of its branches", ha="center", color=MUTED,
             fontsize=8)
    fig.tight_layout(rect=(0, 0, 1, 0.8))
    fig.savefig(out, dpi=150, bbox_inches="tight")
    cols = ["e2e", "p50", "attempts", "final"] + [k for k, *_ in segments]
    lines = ["| point | " + " | ".join(cols) + " |", "|" + "---|" * (len(cols) + 1)]
    for label, p in points:
        lines.append(f"| {label} | " + " | ".join(f"{p[c]:.1f}" for c in cols) + " |")
    out.with_suffix(".md").write_text("# Creation to commit (ms, mean per committed txn)\n\n" + "\n".join(lines) + "\n")
    print(out)


if __name__ == "__main__":
    main()
