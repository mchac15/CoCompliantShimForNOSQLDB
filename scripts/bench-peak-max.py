#!/usr/bin/env python3
"""Peak throughput per configuration of a bench-peak.py sweep: the best thread count of each shim.

    scripts/bench-peak-max.py DIR        # writes DIR/peak-max.png and DIR/peak-max.md, prints the table

For every server kind and skew, each shim's highest mean throughput over the measured thread counts
(a run's tps = the median of its per-second samples), the thread count it was reached at, and the
speculative vs non-speculative gain at those peaks. A peak at the lowest or the highest measured thread
count is flagged (edge): the true peak may lie outside the measured range. The peak is only as precise
as the thread grid.

Figure, one column per server kind: peak throughput of both shims against skew (whisker = rep min-max of
the peak point), the gain at the peaks, and the thread count of each peak. Needs matplotlib.
"""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location("bench_peak", Path(__file__).resolve().parent / "bench-peak.py")
bench_peak = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench_peak)

DB_NAMES = {"mysql": "MySQL", "pg": "PostgreSQL"}
SYSTEMS = {"speculative": ("#2a78d6", "o", "Speculative shim"), "nonspeculative": ("#eb6834", "s", "Non-speculative shim")}
GAIN_COLORS = ("#2a78d6", "#e34948")   # speculation faster / slower: the reference diverging poles
INK, INK_MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def main(out):
    out = Path(out)
    points = bench_peak.load_points(out)
    pk = bench_peak.peaks(points)
    dbs = [db for db in DB_NAMES if any(p["db"] == db for p in points)]
    skews = sorted({p["skew"] for p in points})
    measured = sorted({p["threads"] for p in points})
    edges = (measured[0], measured[-1])

    lines = ["# Peak throughput per configuration", "",
             f"Each shim's highest mean tps over the measured thread counts ({', '.join(map(str, measured))}); "
             "edge = the peak is at the lowest or highest one, the true peak may lie outside.", "",
             "| db | skew | speculative peak (threads) | rep min-max | non-speculative peak (threads) | rep min-max | gain |",
             "|---|---|---|---|---|---|---|"]
    for db in dbs:
        for s in skews:
            a, b = pk.get((db, s, "speculative")), pk.get((db, s, "nonspeculative"))
            if not a or not b:
                continue
            flag = lambda p: " edge" if p["threads"] in edges else ""
            lines.append(f"| {db} | {s:g} | {a['tps']:.0f} ({a['threads']}{flag(a)}) | {a['tps_min']:.0f}-{a['tps_max']:.0f} "
                         f"| {b['tps']:.0f} ({b['threads']}{flag(b)}) | {b['tps_min']:.0f}-{b['tps_max']:.0f} "
                         f"| {bench_peak.fmt_gain(a['tps'], b['tps'])} |")
    text = "\n".join(lines) + "\n"
    (out / "peak-max.md").write_text(text)
    print(text)

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK_MUTED,
                         "xtick.color": INK_MUTED, "ytick.color": INK_MUTED, "text.color": INK})
    fig, axes = plt.subplots(3, len(dbs), figsize=(7.2 * len(dbs), 10), squeeze=False, sharex=True,
                             gridspec_kw={"height_ratios": [3, 2, 1.6]}, facecolor=SURFACE)
    # a gain is undefined where the non-speculative shim committed nothing
    gain_span = max([abs(100 * (pk[(db, s, "speculative")]["tps"] / pk[(db, s, "nonspeculative")]["tps"] - 1))
                     for db in dbs for s in skews if (db, s, "speculative") in pk and (db, s, "nonspeculative") in pk
                     and pk[(db, s, "nonspeculative")]["tps"] > 0], default=1) or 1
    for j, db in enumerate(dbs):
        tps_ax, gain_ax, thr_ax = axes[0][j], axes[1][j], axes[2][j]
        for ax in (tps_ax, gain_ax, thr_ax):
            ax.set_facecolor(SURFACE)
            ax.grid(True, color=GRID, linewidth=0.8)
            ax.set_axisbelow(True)
            for side in ("top", "right"):
                ax.spines[side].set_visible(False)
            ax.tick_params(length=0)
        for k, (system, (color, marker, name)) in enumerate(SYSTEMS.items()):
            ss = [s for s in skews if (db, s, system) in pk]
            ps = [pk[(db, s, system)] for s in ss]
            tps_ax.plot(ss, [p["tps"] for p in ps], color=color, marker=marker, linewidth=2, markersize=6,
                        markeredgecolor=SURFACE, markeredgewidth=1.2, zorder=3)
            tps_ax.vlines(ss, [p["tps_min"] for p in ps], [p["tps_max"] for p in ps], color=color, linewidth=1.2,
                          alpha=0.6, zorder=2)
            # thread count of the peak; the two shims side by side at each skew, hollow at the edge of the range
            off = (-0.0012, 0.0012)[k]
            for s, p in zip(ss, ps):
                edge = p["threads"] in edges
                thr_ax.plot([s + off], [p["threads"]], marker=marker, markersize=7, linestyle="none",
                            color=SURFACE if edge else color, markeredgecolor=color, markeredgewidth=1.5, zorder=3)
        gs = [s for s in skews if (db, s, "speculative") in pk and (db, s, "nonspeculative") in pk
              and pk[(db, s, "nonspeculative")]["tps"] > 0]
        gains = [100 * (pk[(db, s, "speculative")]["tps"] / pk[(db, s, "nonspeculative")]["tps"] - 1) for s in gs]
        gain_ax.bar(gs, gains, width=0.0065, color=[GAIN_COLORS[g < 0] for g in gains], edgecolor=SURFACE,
                    linewidth=1, zorder=3)
        for s, g in zip(gs, gains):
            gain_ax.text(s, g + (0.06 if g >= 0 else -0.06) * gain_span, f"{g:+.0f}%", ha="center",
                         va="bottom" if g >= 0 else "top", fontsize=8, color=INK)
        gain_ax.axhline(0, color=INK_MUTED, linewidth=1, zorder=2)
        gain_ax.set_ylim(-gain_span * 1.35, gain_span * 1.35)
        tps_ax.set_ylim(bottom=0)
        thr_ax.set_yticks(measured)
        thr_ax.set_ylim(measured[0] - 15, measured[-1] + 15)
        thr_ax.set_xticks(skews, [f"{s:g}" for s in skews])
        thr_ax.set_xlabel("Skew")
        tps_ax.set_title(DB_NAMES[db], color=INK, fontsize=12, fontweight="bold")
        if j == 0:
            tps_ax.set_ylabel("Peak throughput (txn/s)")
            gain_ax.set_ylabel("Gain at the peaks (%)")
            thr_ax.set_ylabel("Threads at the peak")
    handles = [Line2D([], [], color=c, marker=m, linewidth=2, markersize=6, markeredgecolor=SURFACE, label=n)
               for c, m, n in SYSTEMS.values()]
    handles.append(Line2D([], [], color=INK_MUTED, marker="o", linestyle="none", markerfacecolor=SURFACE,
                          markersize=7, label="peak at the edge of the range"))
    fig.legend(handles=handles, loc="upper center", ncol=len(handles), frameon=False, bbox_to_anchor=(0.5, 1.0))
    height = fig.get_figheight()
    fig.text(0.5, 1 - 0.38 / height, f"Each shim at its own best thread count ({measured[0]}-{measured[-1]} measured); "
             "whisker = rep min-max of the peak point; gain = speculative / non-speculative peak - 1",
             ha="center", va="top", color=INK_MUTED, fontsize=9)
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.65 / height))
    fig.savefig(out / "peak-max.png", dpi=150, facecolor=SURFACE)
    plt.close(fig)
    print(out / "peak-max.png")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
