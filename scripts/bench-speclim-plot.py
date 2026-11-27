#!/usr/bin/env python3
"""Plots a speculation-limits sweep: one bench-peak.py --out per config, named nonspec, spec-dnone-fnone and
spec-d<D>-f<F> (the depth / fanout caps of CoShimNode --spec-max-depth / --spec-max-fanout).

    scripts/bench-speclim-plot.py RESULTS_DIR [--out DIR]

Writes into --out (default RESULTS_DIR):
  speclim-throughput.png  tps vs threads, one panel per fanout cap, a line per depth cap, plus the
                          non-speculative baseline and uncapped speculation
  speclim-gain.png        gain over the baseline per (depth, fanout), one heatmap per thread count
  speclim-aborts.png      cascades / wounds / coordinator aborts per 100 commits vs threads
  speclim-limiter.png     what the caps did (node logs): lock calls refused per commit by each cap, and the
                          share of cascade marks that hit txns still blocked in lock()
  speclim-budget.png      where a committed branch's time goes (bench-peak's budget, plus the post-vote wait),
                          each config at its peak-throughput thread count; speclim-budget.md the same as a table
  speclim.md              every point as a table

A point is "stalled" when its throughput was 0 for at least 5 consecutive seconds of the measurement window
(lock_timeout breaking a deadlock): the plots show its mean tps over the window, as a hollow marker.
"""
import argparse
import csv
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm

# reference palette (dataviz skill): categorical slots 1-3, neutral ink, surface, hairline grid
# one (color, marker) per depth cap (or fanout cap), in order: categorical slots 1-3
SERIES = [("#2a78d6", "o"), ("#eb6834", "s"), ("#1baf7a", "^")]
INK, MUTED, SURFACE, GRID = "#1f1f1e", "#52514e", "#fcfcfb", "#e1e0d9"
DIVERGING = LinearSegmentedColormap.from_list("gain", ["#e34948", "#f0efec", "#2a78d6"])
STALL_S = 5
# bench-peak.py's time budget per branch commit (µs), its colors, plus the post-vote wait it leaves out;
# hatched: lost time (aborted branches' lifetime per commit), dotted: post-vote (secondary cue for its neighbor)
BUDGET = [("active_us", "active (work + think)", "#2a78d6", ""), ("lock_wait_us", "lock wait", "#eb6834", ""),
          ("prepare_wait_us", "prepare wait", "#1baf7a", ""),
          ("post_vote_us", "post-vote wait (2PC decision)", "#e34948", ".."),
          ("lost_wounded_us", "lost: wounded", "#eda100", "//"), ("lost_cascade_us", "lost: cascade", "#e87ba4", "//"),
          ("lost_coordinator_us", "lost: coordinator", "#008300", "//"), ("lost_other_us", "lost: other (upgrade conflicts, …)", "#6250d6", "//")]

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
    "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 9, "lines.linewidth": 2,
    "lines.markersize": 6,
})


def config_name(d):
    m = re.fullmatch(r"spec-d(\w+)-f(\w+)", d.name)
    return ("spec", m.group(1), m.group(2)) if m else ("nonspec", None, None) if d.name == "nonspec" else None


def final_line(log, prefix):
    """The last '<prefix> sN: k=v ...' line of a node log, as a dict of ints."""
    last = None
    for line in open(log, errors="replace"):
        if line.startswith(prefix + " "):
            last = line
    return {k: int(v) for k, v in re.findall(r"(\w+)=(-?\d+)\b", last or "")}


def node_counters(point_dir, threads):
    """Sums over the nodes of the final stats and speculation counters of one point."""
    out = {}
    for log in sorted((point_dir / f"t{threads}-nodes").glob("*-s[0-9].log")):
        for prefix in ("stats", "speculation"):
            for k, v in final_line(log, prefix).items():
                out[k] = out.get(k, 0) + v
    return out


def stalled(point_dir, threads):
    f = point_dir / f"timeseries-t{threads}.csv"
    if not f.exists():
        return False
    run = best = 0
    for r in csv.DictReader(open(f)):
        if r["phase"] == "measure":
            run = run + 1 if float(r["tps"]) == 0 else 0
            best = max(best, run)
    return best >= STALL_S


def load(results):
    """{(kind, depth, fanout): [point dict, ...]} sorted by threads."""
    configs = {}
    for d in sorted(results.iterdir()):
        key = config_name(d) if d.is_dir() else None
        if key is None or not (d / "points.csv").exists():
            continue
        point_dir = next(p for p in d.iterdir() if p.is_dir() and p.name.startswith(("mysql-", "pg-")))
        rows = []
        for r in csv.DictReader(open(d / "points.csv")):
            t = int(r["threads"])
            p = {k: float(r[k]) for k in ("tps", "tps_raw", "abort_rate", "cascades_per_100", "wounded_per_100",
                                           "coordinator_per_100", "upgrade_per_100", "op_p50_us")}
            p.update({k: float(r[k] or 0) for k, *_ in BUDGET})
            p["threads"] = t
            p["stalled"] = stalled(point_dir, t)
            p["tps_plot"] = p["tps_raw"] if p["stalled"] else p["tps"]
            c = node_counters(point_dir, t)
            commits = max(c.get("commits", 0) // 2, 1)   # "commits" is in both lines of each node
            p["refused_depth_pc"] = c.get("spec_refused_depth_total", 0) / commits
            p["refused_fanout_pc"] = c.get("spec_refused_fanout_total", 0) / commits
            p["refused_wait_ms_pc"] = c.get("spec_refused_wait_us_total", 0) / 1000 / commits
            marked = c.get("cascade_marked_total", 0)
            # met while still blocked in lock(): spared since the granted flag, marked anyway before it
            blocked = c.get("cascade_spared_total", c.get("cascade_marked_waiting_total", 0))
            met = marked + c.get("cascade_spared_total", 0)
            p["marked_waiting_share"] = blocked / met if met else 0
            p["lock_timeouts"] = c.get("lock_timeout", 0)
            rows.append(p)
        configs[key] = sorted(rows, key=lambda p: p["threads"])
    return configs


def grid(configs):
    depths = sorted({k[1] for k in configs if k[0] == "spec" and k[1] != "none"}, key=int)
    fanouts = sorted({k[2] for k in configs if k[0] == "spec" and k[2] != "none"}, key=int)
    return depths, fanouts


def styles(values):
    """{value: dict(color, marker)} in the fixed categorical order."""
    return {v: dict(color=c, marker=m) for v, (c, m) in zip(values, SERIES)}


def plot_line(ax, pts, y, **kw):
    xs = [p["threads"] for p in pts]
    ax.plot(xs, [p[y] for p in pts], **kw)
    hollow = [p for p in pts if p["stalled"]]
    if hollow:
        ax.plot([p["threads"] for p in hollow], [p[y] for p in hollow], linestyle="none",
                marker=kw.get("marker", "o"), markersize=9, markerfacecolor=SURFACE,
                markeredgecolor=kw.get("color", INK), markeredgewidth=2)


def references(ax, configs, y):
    if ("nonspec", None, None) in configs:
        plot_line(ax, configs[("nonspec", None, None)], y, color=MUTED, linestyle="--", marker="x",
                  label="non-speculative")
    if ("spec", "none", "none") in configs:
        plot_line(ax, configs[("spec", "none", "none")], y, color=MUTED, linestyle=":", marker="D",
                  markersize=4, label="speculative, uncapped")


def throughput(configs, out):
    depths, fanouts = grid(configs)
    style = styles(depths)
    fig, axes = plt.subplots(1, len(fanouts), figsize=(4 * len(fanouts), 3.6), sharey=True, squeeze=False)
    axes = axes[0]
    for ax, f in zip(axes, fanouts):
        references(ax, configs, "tps_plot")
        for d in depths:
            plot_line(ax, configs[("spec", d, f)], "tps_plot", label=f"depth ≤ {d}", **style[d])
        ax.set_title(f"fanout ≤ {f}", color=INK)
        ax.set_xlabel("client threads")
    axes[0].set_ylabel("committed txns / s")
    axes[0].set_ylim(bottom=0)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False, bbox_to_anchor=(0.5, 1.02))
    fig.text(0.5, -0.04, "hollow marker: stalled point (≥ 5 s at 0 tps until lock_timeout), its mean tps over the window",
             ha="center", color=MUTED, fontsize=8)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(out / "speclim-throughput.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def gain(configs, out):
    depths, fanouts = grid(configs)
    if ("nonspec", None, None) not in configs:
        return
    base = {p["threads"]: p["tps_plot"] for p in configs[("nonspec", None, None)]}
    uncapped = {p["threads"]: p["tps_plot"] for p in configs.get(("spec", "none", "none"), [])}
    cells = {}
    for d in depths:
        for f in fanouts:
            for p in configs[("spec", d, f)]:
                if p["threads"] in base:
                    cells[(d, f, p["threads"])] = (100 * (p["tps_plot"] / base[p["threads"]] - 1), p["stalled"])
    # the thread counts both the baseline and every capped config ran
    threads = sorted(t for t in base if all((d, f, t) in cells for d in depths for f in fanouts))
    # the color scale follows the points that ran normally: a stall would wash out the rest
    lim = max(abs(v) for v, s in cells.values() if not s)
    norm = TwoSlopeNorm(vmin=-lim, vcenter=0, vmax=lim)
    fig, axes = plt.subplots(1, len(threads), figsize=(2.6 * len(threads), 3.2), squeeze=False)
    axes = axes[0]
    for ax, t in zip(axes, threads):
        m = [[float("nan") if cells[(d, f, t)][1] else cells[(d, f, t)][0] for f in fanouts] for d in depths]
        im = ax.imshow(m, cmap=DIVERGING, norm=norm, origin="lower")
        for i, d in enumerate(depths):
            for j, f in enumerate(fanouts):
                v, s = cells[(d, f, t)]
                if s:
                    ax.add_patch(plt.Rectangle((j - 0.5, i - 0.5), 1, 1, facecolor=GRID, edgecolor=MUTED,
                                               hatch="//", linewidth=0))
                ax.text(j, i, f"{v:+.0f}%" + ("\nstall" if s else ""), ha="center", va="center", fontsize=8, color=INK)
        ax.set_xticks(range(len(fanouts)), fanouts)
        ax.set_yticks(range(len(depths)), depths)
        ax.set_xlabel("fanout cap")
        ax.grid(False)
        u = f"uncapped {100 * (uncapped[t] / base[t] - 1):+.0f}%" if t in uncapped else ""
        ax.set_title(f"{t} threads\n{u}", color=INK, fontsize=9)
    axes[0].set_ylabel("depth cap")
    cb = fig.colorbar(im, ax=axes, shrink=0.8, pad=0.02)
    cb.set_label("throughput vs non-speculative", color=MUTED)
    fig.savefig(out / "speclim-gain.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def aborts(configs, out):
    depths, fanouts = grid(configs)
    d = max(depths, key=int)
    causes = [(y, title) for y, title in (("cascades_per_100", "cascade aborts"), ("wounded_per_100", "wounds"),
                                           ("coordinator_per_100", "coordinator aborts"),
                                           ("upgrade_per_100", "upgrade conflicts"))
              if any(p[y] > 0 for pts in configs.values() for p in pts)]
    if not causes:
        return
    style = styles(fanouts)
    fig, axes = plt.subplots(1, len(causes), figsize=(4 * len(causes), 3.4), squeeze=False)
    axes = axes[0]
    for ax, (y, title) in zip(axes, causes):
        references(ax, configs, y)
        for f in fanouts:
            plot_line(ax, configs[("spec", d, f)], y, label=f"fanout ≤ {f}", **style[f])
        ax.set_title(f"{title} per 100 commits", color=INK)
        ax.set_xlabel("client threads")
        ax.set_ylim(bottom=0)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False, bbox_to_anchor=(0.5, 1.03))
    fig.text(0.5, -0.04, f"capped lines: depth ≤ {d} (the other depth caps differ little, see speclim.md)",
             ha="center", color=MUTED, fontsize=8)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(out / "speclim-aborts.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def limiter(configs, out):
    depths, fanouts = grid(configs)
    rows = [("refused_fanout_pc", "refused by the fanout cap\nper commit"),
            ("refused_depth_pc", "refused by the depth cap\nper commit"),
            ("marked_waiting_share", "txns a cascade met while\nstill blocked in lock()")]
    style = styles(depths)
    fig, axes = plt.subplots(len(rows), len(fanouts), figsize=(4 * len(fanouts), 2.6 * len(rows)), sharex=True,
                             sharey="row", squeeze=False)
    for i, (y, label) in enumerate(rows):
        for j, f in enumerate(fanouts):
            ax = axes[i][j]
            if y == "marked_waiting_share" and ("spec", "none", "none") in configs:
                plot_line(ax, configs[("spec", "none", "none")], y, color=MUTED, linestyle=":", marker="D",
                          markersize=4, label="speculative, uncapped")
            for d in depths:
                plot_line(ax, configs[("spec", d, f)], y, label=f"depth ≤ {d}", **style[d])
            if i == 0:
                ax.set_title(f"fanout ≤ {f}", color=INK)
            if j == 0:
                ax.set_ylabel(label)
            if i == len(rows) - 1:
                ax.set_xlabel("client threads")
    for (y, _), row in zip(rows, axes):
        # after every panel of the row is drawn, so the shared scale fits them all
        top = max(p[y] for k, pts in configs.items() if k[0] == "spec" for p in pts)
        row[0].set_ylim(0, 1.08 * top or 1)
    for ax in axes[-1]:
        ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    handles, labels = axes[-1][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False, bbox_to_anchor=(0.5, 1.01))
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(out / "speclim-limiter.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def peak(pts):
    """The point of highest throughput among those that did not stall."""
    return max((p for p in pts if not p["stalled"]), key=lambda p: p["tps_plot"])


def config_label(k):
    if k[0] == "nonspec":
        return "non-speculative"
    return "speculative, uncapped" if k[1] == "none" else f"depth ≤ {k[1]}, fanout ≤ {k[2]}"


def config_order(k):
    return (k[0] != "nonspec", k[1] != "none", int(k[1]) if k[1] not in (None, "none") else 0,
            int(k[2]) if k[2] not in (None, "none") else 0)


def budget(configs, out):
    """Where the time goes per committed branch, at each config's peak throughput."""
    keys = sorted(configs, key=config_order)
    peaks = {k: peak(configs[k]) for k in keys}
    totals = {k: sum(peaks[k][c] for c, *_ in BUDGET) / 1000 for k in keys}
    span = max(totals.values())
    fig, ax = plt.subplots(figsize=(11, 0.5 * len(keys) + 1.6))
    ax.grid(False)
    ax.grid(True, axis="x")
    for y, k in enumerate(keys):
        left = 0
        for c, name, color, hatch in BUDGET:
            w = peaks[k][c] / 1000
            ax.barh(y, w, left=left, height=0.62, color=color, hatch=hatch, edgecolor=SURFACE, linewidth=2,
                    label=name if y == 0 else None, zorder=3)
            if w >= 0.045 * span:
                ax.text(left + w / 2, y, f"{w:.0f}", ha="center", va="center", fontsize=7.5, color=INK, zorder=4,
                        bbox=dict(boxstyle="round,pad=0.15", facecolor=SURFACE, edgecolor="none", alpha=0.85))
            left += w
        ax.text(left + 0.01 * span, y, f"{left:.0f} ms · {peaks[k]['tps_plot']:.0f} tps @ {peaks[k]['threads']} thr",
                va="center", fontsize=8, color=MUTED)
    ax.set_yticks(range(len(keys)), [config_label(k) for k in keys])
    ax.invert_yaxis()
    ax.set_xlim(0, 1.3 * span)
    ax.set_xlabel("ms per committed branch (lost = lifetime of the aborted branches, per commit)")
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=4, frameon=False, bbox_to_anchor=(0.55, 1.0), fontsize=8)
    fig.text(0.55, -0.01, "each config at its peak-throughput thread count (stalled points excluded); "
             "values in ms, segments under ~4% of the widest bar unlabeled (see speclim-budget.md)",
             ha="center", color=MUTED, fontsize=8)
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    fig.savefig(out / "speclim-budget.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    lines = ["| config | peak threads | tps | " + " | ".join(name for _, name, *_ in BUDGET) + " | total |",
             "|" + "---|" * (len(BUDGET) + 4)]
    for k in keys:
        p = peaks[k]
        lines.append(f"| {config_label(k)} | {p['threads']} | {p['tps_plot']:.0f} | "
                     + " | ".join(f"{p[c] / 1000:.1f}" for c, *_ in BUDGET) + f" | {totals[k]:.1f} |")
    (out / "speclim-budget.md").write_text(
        "# Time budget at peak throughput\n\nms per committed branch, each config at its peak-throughput thread count "
        "(stalled points excluded); lost = lifetime of the aborted branches, per commit.\n\n" + "\n".join(lines) + "\n")


def table(configs, out):
    def name(k):
        return "non-speculative" if k[0] == "nonspec" else f"spec d≤{k[1]} f≤{k[2]}".replace("≤none", " none")

    lines = ["| config | threads | tps | stalled | abort rate | cascades/100 | wounds/100 | coord/100 | upgrade/100 | "
             "refused fanout/commit | refused depth/commit | refused wait ms/commit | marks on blocked | "
             "lock timeouts | op p50 ms |", "|" + "---|" * 15]
    order = sorted(configs, key=lambda k: (k[0] != "nonspec", k[1] != "none", int(k[1]) if k[1] not in (None, "none") else 0,
                                           int(k[2]) if k[2] not in (None, "none") else 0))
    for k in order:
        for p in configs[k]:
            lines.append(f"| {name(k)} | {p['threads']} | {p['tps_plot']:.0f} | {'yes' if p['stalled'] else ''} | "
                         f"{p['abort_rate']:.2f} | {p['cascades_per_100']:.0f} | {p['wounded_per_100']:.0f} | "
                         f"{p['coordinator_per_100']:.0f} | {p['upgrade_per_100']:.0f} | {p['refused_fanout_pc']:.2f} | {p['refused_depth_pc']:.2f} | "
                         f"{p['refused_wait_ms_pc']:.1f} | {100 * p['marked_waiting_share']:.0f}% | {p['lock_timeouts']} | "
                         f"{p['op_p50_us'] / 1000:.1f} |")
    (out / "speclim.md").write_text(
        "# Speculation limits sweep\n\ntps: robust (median per-second) tps, or the window mean for a stalled point.\n\n"
        + "\n".join(lines) + "\n")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("results", type=Path)
    p.add_argument("--out", type=Path)
    args = p.parse_args()
    out = args.out or args.results
    out.mkdir(parents=True, exist_ok=True)
    configs = load(args.results)
    throughput(configs, out)
    gain(configs, out)
    aborts(configs, out)
    limiter(configs, out)
    budget(configs, out)
    table(configs, out)
    print(f"wrote speclim-{{throughput,gain,aborts,limiter,budget}}.png and speclim{{,-budget}}.md to {out}")


if __name__ == "__main__":
    main()
