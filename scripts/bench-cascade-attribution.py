#!/usr/bin/env python3
"""Where the cascading aborts come from, from the cascade attribution counters of the shim nodes.

    scripts/bench-cascade-attribution.py DIR [OUT_PREFIX]   # DIR: a bench-peak.py --out folder

For every config (skew) and thread count of the speculative shim, sums both nodes' counters at the last
second the workload was active (as bench-peak.py), and the client's branch failure causes (txns-t*.csv).
Writes OUT_PREFIX.png (default DIR/cascade-attribution) and OUT_PREFIX.md:
  - aborts per 100 commits by shim cause (upgrade conflict, coordinator, cascade, other);
  - cascade victims per root abort, by the root's cause (cascade_root_<cause>_victims_total);
  - the client's failed attempts by cause (prepare NO = xa_rollback, a cascade victim that had executed);
  - per depth bucket: share of the branches that aborted, and the mean lifetime an abort lost.
Needs matplotlib.
"""

import csv
import glob
import re
import sys
from pathlib import Path

CAUSES = ("lock_timeout", "deadlock_prevention", "wounded", "upgrade_conflict", "cascade", "prepare_no",
          "coordinator", "prepare_wait_die")
# fixed categorical slots (reference palette order), one per abort cause wherever it appears
CAUSE_STYLE = {"upgrade_conflict": ("#2a78d6", "Upgrade conflict"),
               "coordinator": ("#eb6834", "Coordinator abort"),
               "cascade": ("#1baf7a", "Cascade"),
               "other": ("#eda100", "Other")}
INK, INK_MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
# sequential blue ramp, light -> dark, for the thread counts (a magnitude)
MIN_BUCKET = 50   # depth buckets with fewer branches (aborts, for the lost time) are noise: left out
RAMP = ["#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#104281"]


def parse_line(line):
    """key=value tokens; a value is followed by the bare tokens up to the next key (histograms)."""
    out, key = {}, None
    for tok in line.split()[2:]:   # drop "speculation s0:" / "stats s0:"
        if "=" in tok:
            key, val = tok.split("=", 1)
            out[key] = [val] if val else []
        elif key is not None:
            out[key].append(tok)
    return out


def hist(vals, n):
    """'d:a/b/c' (or 'd+:...') tokens -> {d: (a, b, c)} with n numbers each."""
    h = {}
    for v in vals:
        m = re.fullmatch(r"(\d+)\+?:([\d/]+)", v)
        if m:
            nums = [int(x) for x in m.group(2).split("/")]
            if len(nums) == n:
                h[int(m.group(1))] = nums
    return h


def node_snapshot(log):
    """The stats and speculation lines at the last second whose abort count still changed."""
    stats, spec = [], []
    for line in open(log, errors="replace"):
        if line.startswith("stats"):
            stats.append(parse_line(line))
        elif line.startswith("speculation"):
            spec.append(parse_line(line))
    if not stats:
        return None
    i = len(stats) - 1
    while i > 0 and stats[i].get("aborts") == stats[i - 1].get("aborts"):
        i -= 1
    return stats[i], spec[min(i, len(spec) - 1)] if spec else {}


def point(nodes_dir):
    tot = {"commits": 0, "aborts": {c: 0 for c in CAUSES}, "victims_by_root": {}, "depth": {}, "depth_lost": {},
           "marked": 0, "marked_executed": 0, "marked_waiting": 0}
    logs = glob.glob(str(nodes_dir / "rep*-s*.log"))
    for log in logs:
        snap = node_snapshot(log)
        if snap is None:
            continue
        st, sp = snap
        tot["commits"] += int(st["commits"][0])
        for c in CAUSES:
            tot["aborts"][c] += int(st.get(c, ["0"])[0])
        for c in CAUSES:   # by name: a pattern would also match cascade_root_<c>_tree_victims_total
            v = int(sp.get(f"cascade_root_{c}_victims_total", ["0"])[0])
            tot["victims_by_root"][c] = tot["victims_by_root"].get(c, 0) + v
        for name, k in (("marked", "cascade_marked_total"), ("marked_executed", "cascade_marked_executed_total"),
                        ("marked_waiting", "cascade_marked_waiting_total")):
            tot[name] += int(sp.get(k, ["0"])[0])
        for d, (c, a, _) in hist(sp.get("depth", []), 3).items():
            pc, pa = tot["depth"].get(d, (0, 0))
            tot["depth"][d] = (pc + c, pa + a)
        for d, (a, _, mean) in hist(sp.get("depth_lost", []), 3).items():
            pa, pl = tot["depth_lost"].get(d, (0, 0))
            tot["depth_lost"][d] = (pa + a, pl + a * mean)
    return tot if logs else None


def client_causes(txns_csv):
    causes = {}
    with open(txns_csv) as f:
        for row in csv.DictReader(f):
            for c in filter(None, row["causes"].split(";")):
                causes[c] = causes.get(c, 0) + 1
    return causes


def load(root):
    data = {}   # skew -> threads -> point
    for cfg in sorted(root.glob("*-skew*-speculative")):
        skew = float(re.search(r"skew([\d.]+)-", cfg.name).group(1))
        for nodes in sorted(cfg.glob("t*-nodes")):
            t = int(nodes.name[1:].split("-")[0])
            p = point(nodes)
            if p is None:
                continue
            txns = cfg / f"txns-t{t}.csv"
            p["client"] = client_causes(txns) if txns.exists() else {}
            data.setdefault(skew, {})[t] = p
    return data


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


def stacked(ax, threads, series, total_label=True):
    """series: [(key, values)] stacked bars with a 2px-ish surface gap between segments."""
    import numpy as np
    x = np.arange(len(threads))
    bottom = np.zeros(len(threads))
    for key, vals in series:
        color, label = CAUSE_STYLE[key]
        ax.bar(x, vals, 0.62, bottom=bottom, color=color, label=label, edgecolor=SURFACE, linewidth=1.2)
        bottom += np.array(vals)
    if total_label:
        for xi, b in zip(x, bottom):
            ax.text(xi, b, f"{b:.0f}", ha="center", va="bottom", fontsize=8, color=INK_MUTED)
    ax.set_xticks(x, [str(t) for t in threads])


def plot(data, out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    skews = sorted(data)
    fig, axes = plt.subplots(3, 2, figsize=(12, 12.5), facecolor=SURFACE)
    fig.subplots_adjust(hspace=0.55, wspace=0.25, top=0.9, bottom=0.06, left=0.07, right=0.98)
    fig.suptitle("Where the cascading aborts come from: ordered access, no deadlock prevention, MySQL",
                 x=0.07, ha="left", fontsize=13, color=INK)
    fig.text(0.07, 0.935, "Speculative shim, both nodes summed. Every cascade victim is counted under the cause of "
             "the abort at the root of its tree.", fontsize=9.5, color=INK_MUTED)

    # row 1: aborts per 100 commits by shim cause, one panel per skew (same y scale: compare skews)
    ymax = 0
    for skew in skews:
        for p in data[skew].values():
            ymax = max(ymax, 100 * sum(p["aborts"].values()) / max(1, p["commits"]))
    for ax, skew in zip(axes[0], skews):
        threads = sorted(data[skew])
        per = lambda key: [100 * (data[skew][t]["aborts"][key] if key != "other" else
                                  sum(v for c, v in data[skew][t]["aborts"].items()
                                      if c not in ("upgrade_conflict", "coordinator", "cascade")))
                           / max(1, data[skew][t]["commits"]) for t in threads]
        stacked(ax, threads, [(k, per(k)) for k in ("upgrade_conflict", "coordinator", "cascade", "other")])
        ax.set_ylim(0, ymax * 1.12)   # one scale for both skews: the near-empty skew 0.9 panel is the finding
        style(ax, f"Shim aborts per 100 branch commits by cause, skew {skew}", "Aborts per 100 commits")
    axes[0][0].legend(frameon=False, fontsize=8.5, loc="upper left")

    # row 2 left: cascade victims per root abort, by the root's cause (only causes that rooted anything)
    ax = axes[1][0]
    for i, skew in enumerate(skews):
        threads = sorted(data[skew])
        ratio = [data[skew][t]["victims_by_root"].get("coordinator", 0)
                 / max(1, data[skew][t]["aborts"]["coordinator"]) for t in threads]
        color = ("#6da7ec", "#184f95")[i % 2]
        ax.plot(threads, ratio, color=color, linewidth=2, marker="o", markersize=7,
                markeredgecolor=SURFACE, markeredgewidth=2, label=f"skew {skew}")
        ax.annotate(f"skew {skew}", (threads[-1], ratio[-1]), xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=8.5, color=INK_MUTED)
    style(ax, "Cascade victims per coordinator abort", "Victims per root abort")
    ax.legend(frameon=False, fontsize=8.5, loc="upper left")

    # row 2 right: client failed attempts by cause, skew of most failures, as a share
    skew = max(skews)
    ax = axes[1][1]
    threads = sorted(data[skew])
    groups = {"upgrade_conflict": ("shim_upgrade_conflict",), "cascade": ("xa_rollback", "shim_cascade")}
    totals = [sum(data[skew][t]["client"].values()) or 1 for t in threads]
    series = [(k, [100 * sum(data[skew][t]["client"].get(c, 0) for c in cs) / tot
                   for t, tot in zip(threads, totals)]) for k, cs in groups.items()]
    other = [100 - sum(s[1][i] for s in series) for i in range(len(threads))]
    stacked(ax, threads, series + [("other", other)], total_label=False)
    ax.set_ylim(0, 100)
    style(ax, f"Client: failed attempts by cause, skew {skew}", "Share of failed attempts (%)")
    ax.legend(frameon=False, fontsize=8.5, loc="upper center", bbox_to_anchor=(0.5, -0.17), ncol=3, labels=[
        "Upgrade conflict", "Cascade (prepare NO or while running)", "Other"])

    # row 3: per depth bucket, abort share and mean lost lifetime, one line per thread count (skew max)
    for ax, kind in zip(axes[2], ("share", "lost")):
        for i, t in enumerate(threads):
            p = data[skew][t]
            src = p["depth"] if kind == "share" else p["depth_lost"]
            ds = sorted(d for d in src if (sum(src[d]) if kind == "share" else src[d][0]) >= MIN_BUCKET)
            if kind == "share":
                ys = [100 * src[d][1] / (src[d][0] + src[d][1]) for d in ds]
            else:
                ys = [src[d][1] / src[d][0] / 1000 for d in ds]
            ax.plot(ds, ys, color=RAMP[min(i + 1, len(RAMP) - 1)], linewidth=2, marker="o", markersize=6,
                    markeredgecolor=SURFACE, markeredgewidth=1.5, label=f"{t} threads")
        ax.set_xticks(range(0, 11), [str(d) if d < 10 else "10+" for d in range(11)])
        if kind == "share":
            style(ax, f"Abort share by speculation depth, skew {skew}", "Branches that aborted (%)",
                  "Depth (uncommitted nodes ahead at a grant)")
            ax.legend(frameon=False, fontsize=8.5, loc="upper left")
        else:
            style(ax, f"Lifetime lost per abort by depth, skew {skew}", "Mean lost per abort (ms)",
                  "Depth (uncommitted nodes ahead at a grant)")
    fig.text(0.07, 0.015, f"Depth buckets with fewer than {MIN_BUCKET} branches are left out. Prepare NO (xa_rollback) is a "
             "cascade victim that had already executed: the shims' prepare_no is 0.", fontsize=8, color=INK_MUTED)
    fig.savefig(out.with_suffix(".png"), dpi=150, facecolor=SURFACE)


def table(data, out):
    lines = ["# Cascade attribution (speculative shim, both nodes summed)", ""]
    for skew in sorted(data):
        lines += [f"## skew {skew}", "",
                  "| threads | commits | upgrade conflict | coordinator | cascade | victims by root cause | "
                  "victims / coordinator abort | marked executed | marked while blocked | client causes |",
                  "|---|---|---|---|---|---|---|---|---|---|"]
        for t in sorted(data[skew]):
            p = data[skew][t]
            a = p["aborts"]
            roots = ", ".join(f"{k} {v}" for k, v in sorted(p["victims_by_root"].items()) if v) or "-"
            client = ", ".join(f"{k} {v}" for k, v in sorted(p["client"].items(), key=lambda kv: -kv[1]))
            lines.append(f"| {t} | {p['commits']} | {a['upgrade_conflict']} | {a['coordinator']} | {a['cascade']} | "
                         f"{roots} | {p['victims_by_root'].get('coordinator', 0) / max(1, a['coordinator']):.1f} | "
                         f"{p['marked_executed']}/{p['marked']} | {p['marked_waiting']}/{p['marked']} | {client} |")
        lines.append("")
    out.with_suffix(".md").write_text("\n".join(lines))


def main(argv):
    if len(argv) < 2:
        sys.exit(__doc__)
    root = Path(argv[1])
    out = Path(argv[2]) if len(argv) > 2 else root / "cascade-attribution"
    data = load(root)
    if not data:
        sys.exit(f"no speculative points under {root}")
    table(data, out)
    plot(data, out)
    print(out.with_suffix(".md").read_text())


if __name__ == "__main__":
    main(sys.argv)
