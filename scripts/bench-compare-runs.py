#!/usr/bin/env python3
"""Two or more bench-peak.py runs of the same configs side by side (e.g. get-for-update off vs on).

    scripts/bench-compare-runs.py OUT_PREFIX LABEL=DIR LABEL=DIR [LABEL=DIR ...] [--db mysql] [--skew 0.99]

For both systems (speculative, non-speculative) at every thread count the runs share, from each run's
points (client) and shim node logs (bench-peak.py's counters, cut before the audit):
  OUT_PREFIX-throughput.png  throughput and median transaction latency (retries included) per thread count;
  OUT_PREFIX-aborts.png      shim aborts per 100 branch commits by cause, and the client's abort rate;
  OUT_PREFIX-budget.png      where a committed branch's time goes (active, lock wait, prepare wait, post-vote
                             wait) plus the time the aborted branches lost, per branch commit;
  OUT_PREFIX.md              the numbers.
Every run but the last is drawn dashed / dotted / dash-dotted and hatched (/// ... xx), the last one solid.
Needs matplotlib.
"""

import csv
import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location("bench_peak", Path(__file__).resolve().parent / "bench-peak.py")
bench_peak = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench_peak)

SYSTEMS = {"speculative": ("#2a78d6", "o", "Speculative"), "nonspeculative": ("#eb6834", "s", "Non-speculative")}
CAUSES = [("upgrade_conflict", "#2a78d6", "Upgrade conflict"), ("lock_timeout", "#6250d6", "Lock timeout"),
          ("prepare_wait_die", "#e87ba4", "Prepare wait-die (vote NO)"), ("wounded", "#008300", "Wounded (wound-wait)"),
          ("coordinator", "#eb6834", "Coordinator abort"), ("cascade", "#1baf7a", "Cascade"),
          (None, "#eda100", "Other")]
NAMED = tuple(c for c, _, _ in CAUSES if c)
BUDGET = [("active_us", "#1baf7a", "Active (work + think)"), ("lock_wait_us", "#6250d6", "Lock wait"),
          ("prepare_wait_us", "#2a78d6", "Prepare wait"), ("post_vote_us", "#eda100", "Post-vote wait"),
          ("lost_us", "#e34948", "Lost to aborts")]
# per run, all but the last: line style, hatch, and their name in the subtitle; the last run is solid
EARLIER = [("--", "///", "dashed, ///"), (":", "...", "dotted, ..."), ("-.", "xx", "dash-dotted, xx")]
INK, INK_MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def load(run_dir, db, skew):
    """system -> threads -> {tps, p50_ms, client_abort, aborts{cause}, commits, budget{}}"""
    out = {}
    # throughput as bench-peak.py reports it (median of a run's per-second samples), when points.csv has it
    peak_tps = {}
    if (run_dir / "points.csv").exists():
        for r in csv.DictReader(open(run_dir / "points.csv")):
            if r["db"] == db and r["skew"] == skew:
                peak_tps[(r["system"], int(r["threads"]))] = float(r["tps"])
    for system in SYSTEMS:
        cfg = run_dir / f"{db}-skew{skew}-{system}"
        if not cfg.is_dir():
            continue
        for nodes in sorted(cfg.glob("t*-nodes")):
            t = int(nodes.name[1:].split("-")[0])
            point = cfg / f"t{t}.csv"
            if not point.exists():
                continue
            rows = list(csv.DictReader(open(point)))
            if not rows:
                continue
            tps = peak_tps.get((system, t), sum(float(r["tps"]) for r in rows) / len(rows))
            p50 = sum(float(r["p50_us"]) for r in rows) / len(rows) / 1000
            client_abort = sum(float(r["abort_rate"]) for r in rows) / len(rows)
            var = rows[0]["variant"]
            n = bench_peak.node_counters(nodes, var)
            commits = n.get("commits", 0)
            aborts = {c: n.get(c, 0) for c in ("lock_timeout", "deadlock_prevention", "wounded", "upgrade_conflict",
                                               "cascade", "prepare_no", "coordinator", "prepare_wait_die")}
            b = bench_peak.budget(n)
            budget = {k: b.get(k, 0) for k in ("active_us", "lock_wait_us", "prepare_wait_us", "post_vote_us")}
            budget["lost_us"] = sum(v for k, v in b.items() if k.startswith("lost_"))
            out.setdefault(system, {})[t] = dict(tps=tps, p50_ms=p50, client_abort=client_abort, aborts=aborts,
                                                 commits=commits, budget=budget)
    return out


def style(ax, title, ylabel, xlabel="Client threads"):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=10.5, color=INK, pad=8)
    ax.set_ylabel(ylabel, fontsize=9, color=INK_MUTED)
    ax.set_xlabel(xlabel, fontsize=9, color=INK_MUTED)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK_MUTED, labelsize=8.5)


def figure(plt, ncols, title, subtitle, width=13, height=5.2, bottom=0.2):
    fig, axes = plt.subplots(1, ncols, figsize=(width, height), facecolor=SURFACE)
    fig.subplots_adjust(wspace=0.22, top=0.78, bottom=bottom, left=0.06, right=0.98)
    fig.suptitle(title, x=0.06, ha="left", fontsize=13, color=INK)
    fig.text(0.06, 0.88, subtitle, fontsize=9.5, color=INK_MUTED)
    return fig, axes


def lines(ax, runs, labels, threads, value):
    for system, (color, marker, name) in SYSTEMS.items():
        for k, (run, label) in enumerate(zip(runs, labels)):
            pts = [(t, value(run[system][t])) for t in threads if t in run.get(system, {})]
            if not pts:
                continue
            xs, ys = zip(*pts)
            last = k == len(runs) - 1
            ax.plot(xs, ys, color=color, linewidth=2, linestyle="-" if last else EARLIER[k][0], marker=marker,
                    markersize=7 if last else 6, markerfacecolor=color if last else SURFACE, markeredgecolor=color,
                    markeredgewidth=1.6, label=f"{name}, {label}")
    ax.set_xticks(threads)


def paired_stacks(ax, runs, labels, threads, system, parts, scale=1.0, totals=True):
    """Per thread count one stacked bar per run, all but the last hatched; parts: [(color, label, value(point))]."""
    import numpy as np
    x = np.arange(len(threads))
    w = 0.8 / len(runs) - 0.04
    for k, (run, label) in enumerate(zip(runs, labels)):
        last = k == len(runs) - 1
        pos = x + (k - (len(runs) - 1) / 2) * (w + 0.04)
        bottom = np.zeros(len(threads))
        for color, name, value in parts:
            vals = np.array([value(run[system][t]) * scale if t in run.get(system, {}) else 0 for t in threads])
            ax.bar(pos, vals, w, bottom=bottom, color=color, edgecolor=SURFACE, linewidth=1.2,
                   hatch=None if last else EARLIER[k][1], alpha=1.0 if last else 0.55,
                   label=name if last else None)
            bottom += vals
        tag = label if len(label) <= 6 else label[:3] + "."   # under a bar: the long labels overlap
        for p, b in zip(pos, bottom):
            if len(runs) > 1:
                ax.text(p, -0.02, tag, transform=ax.get_xaxis_transform(), ha="center", va="top",
                        fontsize=7 if len(runs) == 2 else 6.5, color=INK_MUTED)
            if totals:
                ax.text(p, b, f"{b:.1f}" if 0 < b < 10 else f"{b:.0f}", ha="center", va="bottom", fontsize=7.5,
                        color=INK_MUTED)
    ax.set_xticks(x, [f"\n{t}" for t in threads])


def share_scale(a1, a2):
    """One y scale for both panels, so they compare at a glance, unless one would flatten the other (> 4x)."""
    t1, t2 = a1.get_ylim()[1], a2.get_ylim()[1]
    if max(t1, t2) <= 4 * max(1e-9, min(t1, t2)):
        a1.set_ylim(0, max(t1, t2))
        a2.set_ylim(0, max(t1, t2))
    else:
        for ax in (a1, a2):
            ax.set_ylabel(ax.get_ylabel() + " (own scale)")


def plot(runs, labels, threads, out, skew, db):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    what = f"{db}, skew {skew}: " + "".join(f"{label} ({EARLIER[k][2]}), " for k, label in enumerate(labels[:-1])) \
        + (f"{labels[-1]} (solid)" if len(labels) > 1 else labels[0])
    vs = " vs ".join(labels)

    # throughput and latency
    fig, (a1, a2) = figure(plt, 2, f"Throughput: {vs}", what, height=6, bottom=0.27)
    lines(a1, runs, labels, threads, lambda p: p["tps"])
    style(a1, "Committed transactions per second", "Throughput (txn/s)")
    a1.set_ylim(bottom=0)
    # below the panels: with several runs the lines fill the plot area
    a1.legend(frameon=False, fontsize=8.5, loc="upper left", bbox_to_anchor=(0, -0.16), ncol=len(runs))
    lines(a2, runs, labels, threads, lambda p: p["p50_ms"])
    style(a2, "Median transaction latency (retries included)", "Latency p50 (ms)")
    p50s = [p["p50_ms"] for run in runs for sys_ in run.values() for p in sys_.values() if p["p50_ms"] > 0]
    if p50s and max(p50s) > 20 * min(p50s):
        a2.set_yscale("log")   # e.g. a run stuck on the lock timeout next to ones in the 100 ms
        a2.set_ylabel("Latency p50 (ms, log scale)")
    else:
        a2.set_ylim(bottom=0)
    fig.savefig(f"{out}-throughput.png", dpi=150, facecolor=SURFACE)

    # aborts by cause, same scale for both systems
    fig, (a1, a2, a3) = figure(plt, 3, f"Aborts: {vs}", what, width=20, height=6, bottom=0.3)
    def per100(cause):
        def f(p):
            c = max(1, p["commits"])
            if cause is None:
                return 100 * sum(v for k, v in p["aborts"].items()
                                 if k not in NAMED) / c
            return 100 * p["aborts"][cause] / c
        return f
    parts = [(color, name, per100(cause)) for cause, color, name in CAUSES]
    for ax, system in ((a1, "speculative"), (a2, "nonspeculative")):
        paired_stacks(ax, runs, labels, threads, system, parts)
        style(ax, f"{SYSTEMS[system][2]} shim: aborts per 100 branch commits", "Aborts per 100 commits", "")
    share_scale(a1, a2)
    a1.legend(frameon=False, fontsize=8.5, loc="upper left", bbox_to_anchor=(0, -0.16), ncol=3)
    lines(a3, runs, labels, threads, lambda p: 100 * p["client_abort"])
    style(a3, "Client: failed attempts", "Attempts that aborted (%)")
    a3.set_ylim(bottom=0)
    a3.legend(frameon=False, fontsize=8, loc="center right")
    fig.text(0.06, 0.03, "Shim counters of both nodes, cut at the last second before the audit."
             + "".join(f" {EARLIER[k][1]}: {label};" for k, label in enumerate(labels[:-1]))
             + (f" solid: {labels[-1]}." if len(labels) > 1 else ""), fontsize=8, color=INK_MUTED)
    fig.savefig(f"{out}-aborts.png", dpi=150, facecolor=SURFACE)

    # time budget per branch commit
    fig, (a1, a2) = figure(plt, 2, f"Where the time goes: {vs}", what, width=15, height=6, bottom=0.27)
    parts = [(color, name, (lambda key: lambda p: p["budget"][key] / 1000)(key)) for key, color, name in BUDGET]
    for ax, system in ((a1, "speculative"), (a2, "nonspeculative")):
        paired_stacks(ax, runs, labels, threads, system, parts)
        style(ax, f"{SYSTEMS[system][2]} shim: ms per branch commit", "ms per committed branch", "")
    share_scale(a1, a2)
    a1.legend(*a2.get_legend_handles_labels(), frameon=False, fontsize=8.5, loc="upper left",
              bbox_to_anchor=(0, -0.16), ncol=len(BUDGET))
    fig.text(0.06, 0.03, "Committed branches' lifetime split in active, lock wait, prepare wait and post-vote wait; "
             "plus the lifetime of the aborted branches, all divided by the branch commits.", fontsize=8,
             color=INK_MUTED)
    fig.savefig(f"{out}-budget.png", dpi=150, facecolor=SURFACE)


def table(runs, labels, threads, out):
    lines_ = [f"# {' vs '.join(labels)}", "",
              "| system | threads | tps | p50 ms | client abort % | shim aborts / 100 commits (upgrade, lock timeout, "
              "prepare wait-die, wounded, coordinator, cascade) | lock wait ms | prepare wait ms | lost ms |", "|---|---|---|---|---|---|---|---|---|"]
    for system in SYSTEMS:
        for t in threads:
            for run, label in zip(runs, labels):
                p = run.get(system, {}).get(t)
                if p is None:
                    continue
                c = max(1, p["commits"])
                a = p["aborts"]
                b = p["budget"]
                lines_.append(
                    f"| {system} ({label}) | {t} | {p['tps']:.0f} | {p['p50_ms']:.0f} | {100 * p['client_abort']:.1f} | "
                    f"{100 * sum(a.values()) / c:.1f} ({', '.join(f'{100 * a[k] / c:.1f}' for k in NAMED)}) | "
                    f"{b['lock_wait_us'] / 1000:.0f} | "
                    f"{b['prepare_wait_us'] / 1000:.0f} | {b['lost_us'] / 1000:.0f} |")
    Path(f"{out}.md").write_text("\n".join(lines_) + "\n")


def main(argv):
    db, skew = "mysql", "0.99"
    for flag in ("--db", "--skew"):
        if flag in argv:
            i = argv.index(flag)
            if flag == "--db":
                db = argv[i + 1]
            else:
                skew = argv[i + 1]
            del argv[i:i + 2]
    if not 3 <= len(argv) <= 2 + 1 + len(EARLIER) or not all("=" in a for a in argv[2:]):
        sys.exit(__doc__)
    out = argv[1]
    labels, runs = [], []
    for a in argv[2:]:
        label, d = a.split("=", 1)
        labels.append(label)
        runs.append(load(Path(d), db, skew))
    threads = sorted(set.intersection(*(set(t for s in r.values() for t in s) for r in runs)))   # one run: its own
    if not threads:
        sys.exit("the two runs share no thread count")
    table(runs, labels, threads, out)
    plot(runs, labels, threads, out, skew, db)
    print(Path(f"{out}.md").read_text())


if __name__ == "__main__":
    main(sys.argv)
