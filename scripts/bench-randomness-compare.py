#!/usr/bin/env python3
"""Peak throughput against the op-order randomness: one bench-peak.py sweep per randomness value.

    scripts/bench-randomness-compare.py OUT.png [--db mysql] R=DIR R=DIR ...   # e.g. 0=bench-results/rand-0

For every skew present in every sweep: each shim's peak throughput (best thread count, a run's tps the
median of its per-second samples) against the randomness R of MicroBench --randomness (0: every branch
locks in ascending key order; 1: random order), with the thread count of each peak, and the speculative
vs non-speculative gain at those peaks. Writes OUT.png and OUT.md. Needs matplotlib.
"""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location("bench_peak", Path(__file__).resolve().parent / "bench-peak.py")
bench_peak = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench_peak)

SYSTEMS = {"speculative": ("#2a78d6", "o", "Speculative shim"),
           "nonspeculative": ("#eb6834", "s", "Non-speculative shim")}
GAIN_COLORS = ("#2a78d6", "#e34948")   # speculation faster / slower: the reference diverging poles
INK, INK_MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def main(argv):
    db = "mysql"
    if "--db" in argv:
        i = argv.index("--db")
        db = argv[i + 1]
        argv = argv[:i] + argv[i + 2:]
    if len(argv) < 2:
        sys.exit(__doc__)
    target = Path(argv[0])
    sweeps = {}
    for arg in argv[1:]:
        r, _, d = arg.partition("=")
        points = [p for p in bench_peak.load_points(Path(d)) if p["db"] == db]
        sweeps[float(r)] = {k: v for k, v in bench_peak.peaks(points).items()}
    rs = sorted(sweeps)
    skews = sorted(set.intersection(*({k[1] for k in pk} for pk in sweeps.values())))

    lines = [f"# {db}: peak throughput against op-order randomness", "",
             "Peak = best thread count of each shim; @ = its thread count; gain = speculative / non-speculative peak - 1.",
             ""]
    for s in skews:
        lines += [f"## skew {s:g}", "", "| randomness | speculative peak | non-speculative peak | gain |", "|---|---|---|---|"]
        for r in rs:
            a, b = sweeps[r].get((db, s, "speculative")), sweeps[r].get((db, s, "nonspeculative"))
            if a and b:
                lines.append(f"| {r:g} | {a['tps']:.0f} @{a['threads']} | {b['tps']:.0f} @{b['threads']} "
                             f"| {bench_peak.fmt_gain(a['tps'], b['tps'])} |")
        lines.append("")
    target.with_suffix(".md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK_MUTED,
                         "xtick.color": INK_MUTED, "ytick.color": INK_MUTED, "text.color": INK})
    fig, axes = plt.subplots(2, len(skews), figsize=(7 * len(skews), 7.5), squeeze=False, sharex=True,
                             gridspec_kw={"height_ratios": [3, 2]}, facecolor=SURFACE)
    for j, s in enumerate(skews):
        tps_ax, gain_ax = axes[0][j], axes[1][j]
        for ax in (tps_ax, gain_ax):
            ax.set_facecolor(SURFACE)
            ax.grid(True, color=GRID, linewidth=0.8)
            ax.set_axisbelow(True)
            for side in ("top", "right"):
                ax.spines[side].set_visible(False)
            ax.tick_params(length=0)
        for k, (system, (color, marker, _)) in enumerate(SYSTEMS.items()):
            xs = [r for r in rs if (db, s, system) in sweeps[r]]
            ps = [sweeps[r][(db, s, system)] for r in xs]
            tps_ax.plot(xs, [p["tps"] for p in ps], color=color, marker=marker, linewidth=2, markersize=6,
                        markeredgecolor=SURFACE, markeredgewidth=1.2, zorder=3)
            for x, p in zip(xs, ps):
                tps_ax.annotate(f"@{p['threads']}", (x, p["tps"]), textcoords="offset points",
                                xytext=(0, 8) if k == 0 else (0, -14), ha="center", fontsize=8, color=INK_MUTED)
        gs = [r for r in rs if (db, s, "speculative") in sweeps[r] and (db, s, "nonspeculative") in sweeps[r]
              and sweeps[r][(db, s, "nonspeculative")]["tps"] > 0]
        gains = [100 * (sweeps[r][(db, s, "speculative")]["tps"] / sweeps[r][(db, s, "nonspeculative")]["tps"] - 1)
                 for r in gs]
        gain_ax.bar(gs, gains, width=0.08, color=[GAIN_COLORS[g < 0] for g in gains], edgecolor=SURFACE,
                    linewidth=1, zorder=3)
        span = max([abs(g) for g in gains], default=1) or 1
        for r, g in zip(gs, gains):
            gain_ax.text(r, g + (0.06 if g >= 0 else -0.06) * span, f"{g:+.1f}%", ha="center",
                         va="bottom" if g >= 0 else "top", fontsize=8, color=INK)
        gain_ax.axhline(0, color=INK_MUTED, linewidth=1, zorder=2)
        gain_ax.set_ylim(-span * 1.4, span * 1.4)
        tps_ax.set_ylim(0, max(sweeps[r][(db, s, sy)]["tps"] for r in rs for sy in SYSTEMS
                               if (db, s, sy) in sweeps[r]) * 1.18)
        gain_ax.set_xticks(rs, [f"{r:g}" for r in rs])
        gain_ax.set_xlabel("Op-order randomness (0 = ascending key order, 1 = random)")
        tps_ax.set_title(f"skew {s:g}", color=INK, fontsize=12, fontweight="bold")
        if j == 0:
            tps_ax.set_ylabel("Peak throughput (txn/s)")
            gain_ax.set_ylabel("Gain at the peaks (%)")
    handles = [Line2D([], [], color=c, marker=m, linewidth=2, markersize=6, markeredgecolor=SURFACE, label=n)
               for c, m, n in SYSTEMS.values()]
    fig.legend(handles=handles, loc="upper center", ncol=len(handles), frameon=False, bbox_to_anchor=(0.5, 1.0))
    height = fig.get_figheight()
    fig.text(0.5, 1 - 0.38 / height, "Each shim at its own best thread count (@ = threads); "
             f"numbers in {target.with_suffix('.md').name}", ha="center", va="top", color=INK_MUTED, fontsize=9)
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.65 / height))
    fig.savefig(target, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    print(target)


if __name__ == "__main__":
    main(sys.argv[1:])
