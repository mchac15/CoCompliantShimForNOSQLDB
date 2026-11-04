#!/usr/bin/env python3
"""Compares several bench-peak.py sweeps (e.g. key distributions, table sizes) at their common skews.

    scripts/bench-dist-compare.py OUT.png [--db mysql] LABEL=DIR LABEL=DIR ...

One row per skew present in every sweep: throughput of both shims against threads (solid speculative,
dashed non-speculative, one color per sweep), and the speculative vs non-speculative gain at equal thread
count. A run's tps is the median of its per-second samples (sweeps without samples: the raw tps). Writes
OUT.png and OUT.md (the numbers). Needs matplotlib.
"""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location("bench_peak", Path(__file__).resolve().parent / "bench-peak.py")
bench_peak = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench_peak)

# reference categorical slots 1-3 in their fixed order (validated: CVD and normal-vision separation pass;
# slot 3 is below 3:1 on the surface, so each sweep also has its own marker and the numbers go to OUT.md)
COLORS = [("#2a78d6", "o"), ("#eb6834", "s"), ("#1baf7a", "^")]
DB_NAMES = {"mysql": "MySQL", "pg": "PostgreSQL"}
INK, INK_MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def main(argv):
    db = "mysql"
    if "--db" in argv:
        i = argv.index("--db")
        db = argv[i + 1]
        argv = argv[:i] + argv[i + 2:]
    if len(argv) < 2 or len(argv) - 1 > len(COLORS):
        sys.exit(__doc__)
    target = Path(argv[0])
    sweeps = []
    for arg in argv[1:]:
        label, _, d = arg.partition("=")
        points = [p for p in bench_peak.load_points(Path(d)) if p["db"] == db]
        sweeps.append((label, {(p["skew"], p["system"], p["threads"]): p for p in points}))
    skews = sorted(set.intersection(*({k[0] for k in pts} for _, pts in sweeps)))
    if not skews:
        sys.exit("no skew is in every sweep")

    lines = [f"# {DB_NAMES[db]}: " + " vs ".join(label for label, _ in sweeps), ""]
    for s in skews:
        lines += [f"## skew {s:g}", "",
                  "| threads | " + " | ".join(f"{label}: spec / non-spec (gain)" for label, _ in sweeps) + " |",
                  "|---|" + "---|" * len(sweeps)]
        threads = sorted({k[2] for _, pts in sweeps for k in pts if k[0] == s})
        for t in threads:
            cells = []
            for _, pts in sweeps:
                a, b = pts.get((s, "speculative", t)), pts.get((s, "nonspeculative", t))
                if a and b:
                    cells.append(f"{a['tps']:.0f} / {b['tps']:.0f} ({100 * (a['tps'] / b['tps'] - 1):+.1f}%)")
                else:
                    cells.append(f"{a['tps']:.0f} / -" if a else (f"- / {b['tps']:.0f}" if b else "-"))
            lines.append(f"| {t} | " + " | ".join(cells) + " |")
        lines.append("")
    md = target.with_suffix(".md")
    md.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK_MUTED,
                         "xtick.color": INK_MUTED, "ytick.color": INK_MUTED, "text.color": INK})
    fig, axes = plt.subplots(len(skews), 2, figsize=(14, 4.3 * len(skews)), squeeze=False, facecolor=SURFACE)
    for i, s in enumerate(skews):
        tps_ax, gain_ax = axes[i]
        for ax in (tps_ax, gain_ax):
            ax.set_facecolor(SURFACE)
            ax.grid(True, color=GRID, linewidth=0.8)
            ax.set_axisbelow(True)
            for side in ("top", "right"):
                ax.spines[side].set_visible(False)
            ax.tick_params(length=0)
        for (label, pts), (color, marker) in zip(sweeps, COLORS):
            for system, style in (("speculative", "-"), ("nonspeculative", (0, (4, 2)))):
                ps = sorted((p for k, p in pts.items() if k[0] == s and k[1] == system), key=lambda p: p["threads"])
                if ps:
                    tps_ax.plot([p["threads"] for p in ps], [p["tps"] for p in ps], color=color, linestyle=style,
                                marker=marker, linewidth=2, markersize=5, markeredgecolor=SURFACE,
                                markeredgewidth=1.2, zorder=3)
            ts = sorted(t for (sk, sy, t) in pts if sk == s and sy == "speculative" and (s, "nonspeculative", t) in pts)
            gains = [100 * (pts[(s, "speculative", t)]["tps"] / pts[(s, "nonspeculative", t)]["tps"] - 1) for t in ts]
            gain_ax.plot(ts, gains, color=color, marker=marker, linewidth=2, markersize=5, markeredgecolor=SURFACE,
                         markeredgewidth=1.2, zorder=3)
        gain_ax.axhline(0, color=INK_MUTED, linewidth=1, zorder=2)
        tps_ax.set_ylim(bottom=0)
        tps_ax.set_title(f"Throughput, skew {s:g}", color=INK, fontsize=11, fontweight="bold")
        gain_ax.set_title(f"Gain of speculation at equal threads, skew {s:g}", color=INK, fontsize=11, fontweight="bold")
        tps_ax.set_ylabel("txn/s")
        gain_ax.set_ylabel("speculative / non-speculative - 1 (%)")
        if i == len(skews) - 1:
            tps_ax.set_xlabel("Threads")
            gain_ax.set_xlabel("Threads")
    handles = [Line2D([], [], color=c, marker=m, linewidth=2, markersize=6, markeredgecolor=SURFACE, label=label)
               for (label, _), (c, m) in zip(sweeps, COLORS)]
    handles += [Line2D([], [], color=INK_MUTED, linewidth=2, label="speculative"),
                Line2D([], [], color=INK_MUTED, linewidth=2, linestyle=(0, (4, 2)), label="non-speculative")]
    fig.legend(handles=handles, loc="upper center", ncol=len(handles), frameon=False, bbox_to_anchor=(0.5, 1.0))
    height = fig.get_figheight()
    fig.text(0.5, 1 - 0.38 / height, f"{DB_NAMES[db]}; mean over reps, a run's tps = median of its per-second samples; "
             f"numbers in {md.name}", ha="center", va="top", color=INK_MUTED, fontsize=9)
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.65 / height))
    fig.savefig(target, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    print(target)


if __name__ == "__main__":
    main(sys.argv[1:])
