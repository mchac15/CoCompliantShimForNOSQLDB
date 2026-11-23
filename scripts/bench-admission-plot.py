#!/usr/bin/env python3
"""Latency-throughput plot of a scripts/bench-admission-sweep.sh sweep (or any bench-final.sh sweep of the
speculative and non-speculative shims against admission / adaptive variants), from its summary.csv.

    scripts/bench-admission-plot.py DIR [--all]   # writes DIR/latency-throughput.png, DIR/throughput-threads.png
                                              # and, with more than two cap configurations, DIR/peak-gain.png;
                                              # with at most 6 cells, DIR/tps-by-variant.png

Small multiples: columns are skews, rows are the committed txns' latency p50 and p99 (first attempt's
begin to commit, retries included, think time excluded; seconds, log scale), x is the throughput. Each
variant is one line through its thread counts in increasing order, the lowest and highest thread count
labelled at its ends. Color is the system (speculative, non-speculative, the candidates: admission
or the per-key adaptive cap); the candidate
configurations share their color and differ by marker and dash. A label that would overlap one
already drawn in its panel is left out. throughput-threads.png: the same lines as throughput against thread count,
one panel per skew (where a system collapses under load). tps-by-variant.png (small sweeps, e.g. a
pilot over a few cells): one panel per (threads, skew) cell, every variant's throughput as a
horizontal bar, value at its end; the baselines in their colors, the candidates in theirs.

With more than two cap configurations (and without --all, which shows them all), those two figures
show two of them: the best mean peak gain
(over the skews), and of the others the highest mean throughput at the highest thread count (the
one that resists a collapse best); peak-gain.png has them all: per skew, a theta x tau grid of each configuration's
peak throughput (best thread count) relative to the better of the two baselines' peaks (diverging:
blue better, red worse, gray no change; blank: not run).

Needs matplotlib (python -m venv .venv && .venv/bin/pip install matplotlib).
"""

import csv
import math
import re
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

# a log axis in plain numbers (0.2, 1, 6) rather than powers of ten
PLAIN = FuncFormatter(lambda y, _: f"{y:g}")
# minor ticks: only the 2x and 5x ones are labelled, so they never crowd
PLAIN_MINOR = FuncFormatter(
    lambda y, _: f"{y:g}" if y > 0 and round(y / 10 ** math.floor(math.log10(y))) in (2, 5) else "")

# categorical slots 1-3 of the reference palette (validated all-pairs; slot 3 is below 3:1 on the
# surface, so every line also has its own marker, a legend and an end label)
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK_MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
# diverging pair of the reference palette: blue <-> red, gray midpoint
DIVERGING = LinearSegmentedColormap.from_list("gain", ["#e34948", "#f0efec", "#2a78d6"])
ADAPTIVE_STYLES = [("^", "-"), ("D", (0, (4, 2))), ("v", (0, (1, 1.5)))]
ROWS = [("txn_p50_us", "Txn latency p50 (s)"), ("txn_p99_us", "Txn latency p99 (s)")]


def cap(variant):
    """adaptive-th0.95-tau500-mysql@wound-wait -> match of (theta, tau ms or None); None if not one."""
    return re.fullmatch(r"adaptive-th([0-9.]+)(?:-tau(\d+))?", variant.split("@")[0].rsplit("-", 1)[0])


def style(variant, adaptive_order):
    system = variant.split("@")[0].rsplit("-", 1)[0]   # adaptive-th0.9-tau2000-mysql@... -> adaptive-th0.9-tau2000
    if system == "speculative":
        return dict(color=BLUE, marker="o", linestyle="-", name="Speculative")
    if system == "nonspeculative":
        return dict(color=ORANGE, marker="s", linestyle="-", name="Non-speculative")
    m = cap(variant)
    marker, dash = ADAPTIVE_STYLES[adaptive_order.index(variant) % len(ADAPTIVE_STYLES)]
    a = re.fullmatch(
        r"admission-(auto|ratio|r([0-9.]+))(?:-tau(\d+))?(?:-w([0-9.]+))?(?:-reach(\d+))?(?:-eta([0-9.]+))?", system)
    if a:
        name = ("Admission r*=saved/(saved+lost)" if a.group(1) == "auto"
                else "Admission ratio (E_spec/E_nonspec)" if a.group(1) == "ratio"
                else f"Admission r*={a.group(2)}") \
            + (f", η={a.group(6)}" if a.group(6) else "") \
            + (f", τ={int(a.group(3)) / 1000:g} s" if a.group(3) else "") \
            + ("" if a.group(4) is None else ", switch" if float(a.group(4)) == 0 else f", ramp ±{a.group(4)}") \
            + ("" if a.group(5) is None else ", no reach" if a.group(5) == "0" else f", reach ≤{a.group(5)}")
    elif re.fullmatch(r"spec-f(\d+)", system):
        name = f"Speculative, fanout ≤ {system[len('spec-f'):]}"
    elif m:
        name = f"Adaptive θ={m.group(1)}" + (f", τ={int(m.group(2)) / 1000:g} s" if m.group(2) else "")
    else:
        name = system
    return dict(color=AQUA, marker=marker, linestyle=dash, name=name)


def plot(out, show_all=False):
    rows = list(csv.DictReader(open(out / "summary.csv")))
    skews = sorted({r["skew"] for r in rows}, key=float)
    variants = list(dict.fromkeys(r["variant"] for r in rows))
    # baselines first, then the cap configurations
    variants.sort(key=lambda v: (not v.startswith("speculative"), not v.startswith("nonspeculative"), v))
    # the candidates: every variant but the two baselines
    adaptive = [v for v in variants if not v.startswith(("speculative", "nonspeculative"))]
    peak = {}
    for r in rows:
        k = (r["skew"], r["variant"])
        peak[k] = max(peak.get(k, 0), float(r["tps"]))
    baselines = [v for v in variants if v not in adaptive]

    def gain(v, skew):
        best = max((peak.get((skew, b), 0) for b in baselines), default=0)
        return peak[(skew, v)] / best - 1 if best and (skew, v) in peak else None

    def mean_gain(v):
        gs = [g for g in (gain(v, s) for s in skews) if g is not None]
        return sum(gs) / len(gs) if gs else -1

    def tps_at_max_threads(v):
        top = max(int(r["threads"]) for r in rows)
        ts = [float(r["tps"]) for r in rows if r["variant"] == v and int(r["threads"]) == top]
        return sum(ts) / len(ts) if ts else -1

    cells = sorted({(int(r["threads"]), r["skew"]) for r in rows}, key=lambda c: (float(c[1]), c[0]))
    if len(cells) <= 6:
        plot_bars(out, rows, cells, variants, {v: style(v, adaptive) for v in variants})
    if len(adaptive) > 2 and not show_all:
        if all(cap(v) for v in adaptive):   # a theta x tau grid of the per-key cap
            plot_gains(out, adaptive, skews, gain)
        # the trade-off: the best one at the peak, and of the others the best one under the highest load
        best_peak = max(adaptive, key=mean_gain)
        shown = [best_peak, max((v for v in adaptive if v != best_peak), key=tps_at_max_threads)]
        variants = baselines + [v for v in adaptive if v in shown]
        adaptive = [v for v in adaptive if v in shown]
    styles = {v: style(v, adaptive) for v in variants}

    plt.rcParams.update({"font.size": 9, "axes.edgecolor": GRID, "axes.labelcolor": INK_MUTED,
                         "xtick.color": INK_MUTED, "ytick.color": INK_MUTED, "text.color": INK})
    fig, axes = plt.subplots(len(ROWS), len(skews), figsize=(max(4.2 * len(skews), 9), 3.4 * len(ROWS)),
                             sharey="row", squeeze=False, facecolor=SURFACE)
    candidates = []   # (axes, thread count, x, y, ha): placed once the layout is final
    for col, skew in enumerate(skews):
        for row, (metric, label) in enumerate(ROWS):
            ax = axes[row][col]
            ax.set_facecolor(SURFACE)
            ax.set_yscale("log")
            ax.yaxis.set_major_formatter(PLAIN)
            ax.yaxis.set_minor_formatter(PLAIN_MINOR)
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
    fig.legend(handles=handles, loc="upper center", ncol=len(handles) if len(skews) > 1 else (len(handles) + 1) // 2, frameon=False,
               bbox_to_anchor=(0.5, 1.0), fontsize=9)
    fig.text(0.5, 0.005, "Each line runs through the thread counts in increasing order; its end points are labelled "
             f"with their thread count. {workload(out)}tps: mean over the reps; latencies pooled.",
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

    fig, axes = plt.subplots(1, len(skews), figsize=(max(4.2 * len(skews), 9), 3.4), squeeze=False, facecolor=SURFACE)
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
    fig.legend(handles=handles, loc="upper center", ncol=len(handles) if len(skews) > 1 else (len(handles) + 1) // 2, frameon=False,
               bbox_to_anchor=(0.5, 1.0), fontsize=9)
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    target = out / "throughput-threads.png"
    fig.savefig(target, dpi=150, facecolor=SURFACE)
    print(target)


def plot_bars(out, rows, cells, variants, styles):
    tps = {(int(r["threads"]), r["skew"], r["variant"]): float(r["tps"]) for r in rows}
    fig, axes = plt.subplots(1, len(cells), figsize=(4.6 * len(cells), 0.42 * len(variants) + 1.6),
                             squeeze=False, sharey=True, facecolor=SURFACE)
    for col, (t, skew) in enumerate(cells):
        ax = axes[0][col]
        ax.set_facecolor(SURFACE)
        # each panel to its own scale: the cells' throughputs differ by up to 10x between skews
        top = max((tps[(t, skew, v)] for v in variants if (t, skew, v) in tps), default=1)
        for y, v in enumerate(variants):
            x = tps.get((t, skew, v))
            if x is None:
                continue
            ax.barh(y, x, height=0.62, color=styles[v]["color"])
            ax.text(x + top * 0.01, y, f"{x:.0f}", va="center", fontsize=8, color=INK)
        ax.set_yticks(range(len(variants)), [styles[v]["name"] for v in variants])
        ax.invert_yaxis()
        ax.set_xlim(0, top * 1.12)
        ax.grid(True, axis="x", color=GRID, linewidth=0.6)
        ax.set_axisbelow(True)
        ax.tick_params(axis="y", length=0)
        for spine in ("top", "right", "left"):
            ax.spines[spine].set_visible(False)
        ax.set_title(f"{t} threads, skew {skew}", color=INK, fontsize=10)
        ax.set_xlabel("Throughput (committed txn/s)")
    fig.text(0.5, 0.005, workload(out) + "tps: mean over the reps.", ha="center", va="bottom", fontsize=7,
             color=INK_MUTED, wrap=True)
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    target = out / "tps-by-variant.png"
    fig.savefig(target, dpi=150, facecolor=SURFACE)
    print(target)


def workload(out):
    """The run's options from setup.txt, each flag once (the last value wins, as in MicroBench)."""
    setup = out / "setup.txt"
    if not setup.exists():
        return ""
    lines = setup.read_text().splitlines()
    opts = next((l.split(":", 1)[1].split() for l in lines if l.startswith("options:")), [])
    flags = {}
    for i, o in enumerate(opts):
        if o.startswith("--") and i + 1 < len(opts):
            flags[o] = opts[i + 1]
    flags.setdefault("--table-size", "10000")
    keep = ("--branches", "--table-size", "--reads", "--writes", "--think-mean-us", "--pipelined-branches",
            "--get-for-update")
    reps = next((l.split("reps:")[1].split()[0] for l in lines if "reps:" in l), "?")
    return "Workload: " + " ".join(f"{k} {flags[k]}" for k in keep if k in flags) + f"; {reps} reps. "


def plot_gains(out, adaptive, skews, gain):
    thetas = sorted({cap(v).group(1) for v in adaptive}, key=float)
    taus = sorted({cap(v).group(2) or "" for v in adaptive}, key=lambda t: float(t) if t else 0)
    gains = {(s, v): gain(v, s) for s in skews for v in adaptive}
    vmax = max([abs(g) for g in gains.values() if g is not None] + [0.01])
    fig, axes = plt.subplots(1, len(skews), figsize=(1.2 * len(taus) * len(skews) + 2, 0.55 * len(thetas) + 1.8),
                             squeeze=False, facecolor=SURFACE)
    for col, skew in enumerate(skews):
        ax = axes[0][col]
        ax.set_facecolor(SURFACE)
        for v in adaptive:
            m = cap(v)
            x, y, g = taus.index(m.group(2) or ""), thetas.index(m.group(1)), gains[(skew, v)]
            if g is None:
                continue
            ax.add_patch(plt.Rectangle((x - 0.47, y - 0.45), 0.94, 0.9, color=DIVERGING(0.5 + g / (2 * vmax))))
            ax.text(x, y, f"{100 * g:+.1f}%", ha="center", va="center", fontsize=8,
                    color=SURFACE if abs(g) > 0.6 * vmax else INK)
        ax.set_xlim(-0.5, len(taus) - 0.5)
        ax.set_ylim(-0.5, len(thetas) - 0.5)
        ax.set_xticks(range(len(taus)), [f"{int(t) / 1000:g} s" if t else "default" for t in taus])
        ax.set_yticks(range(len(thetas)), [f"θ {t}" for t in thetas])
        ax.tick_params(length=0)
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.set_title(f"skew {skew}", color=INK, fontsize=10)
        ax.set_xlabel("τ")
    fig.suptitle("Adaptive peak throughput vs the better baseline's peak (best thread count each)",
                 fontsize=10, color=INK)
    fig.tight_layout()
    target = out / "peak-gain.png"
    fig.savefig(target, dpi=150, facecolor=SURFACE)
    print(target)


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--all"]
    plot(Path(args[0] if args else "."), show_all="--all" in sys.argv[1:])
