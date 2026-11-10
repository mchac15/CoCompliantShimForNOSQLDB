#!/usr/bin/env python3
"""Plots a deadlock-policy x speculation-limits sweep over several skews: RESULTS_DIR/<policy>/<config>, each a
bench-peak.py --out run over every skew, config nonspec or spec-d<D>-f<F>.

    scripts/bench-sweep-plot.py RESULTS_DIR [--out DIR]

Writes into --out (default RESULTS_DIR):
  sweep-throughput.png  tps vs threads, a row per skew, a column per policy, a line per config
  sweep-peaks.png       each config's peak tps per skew and policy, labeled with its gain over the
                        non-speculative peak of the same policy
  sweep.md              every config's peak per skew and policy: tps, threads, gain, abort rate, aborts by cause
                        per 100 branch commits, wound victims blocked / running, stalled points

A point is stalled when its throughput was 0 for at least 5 consecutive seconds of the measurement window: the
plots show its mean tps over the window, as a hollow marker, and peaks ignore it.
"""
import argparse
import csv
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

INK, MUTED, SURFACE, GRID = "#1f1f1e", "#52514e", "#fcfcfb", "#e1e0d9"
# categorical slots 1-4 of the reference palette, a marker each (secondary encoding)
SERIES = [("#2a78d6", "o"), ("#eb6834", "s"), ("#1baf7a", "^"), ("#eda100", "D")]
STALL_S = 5


def last(log, prefix):
    line = ""
    for l in open(log, errors="replace"):
        if l.startswith(prefix + " "):
            line = l
    return {k: int(v) for k, v in re.findall(r"(\w+)=(-?\d+)\b", line)}


def stalled(path):
    run = best = 0
    for r in csv.DictReader(open(path)):
        if r["phase"] == "measure":
            run = run + 1 if float(r["tps"]) == 0 else 0
            best = max(best, run)
    return best >= STALL_S


def label(cfg):
    if cfg == "nonspec":
        return "non-speculative"
    m = re.fullmatch(r"spec-d(\w+)-f(\w+)", cfg)
    return f"spec d≤{m.group(1)} f≤{m.group(2)}"


def order(cfg):
    """Non-speculative, then the capped configs by depth and fanout, then uncapped speculation."""
    m = re.fullmatch(r"spec-d(\w+)-f(\w+)", cfg)
    cap = lambda v: int(v) if v.isdigit() else 10**9
    return (0, 0, 0) if cfg == "nonspec" else (1, cap(m.group(1)), cap(m.group(2)))


def load(results):
    """{policy: {config: {skew: [point, ...]}}}"""
    data = {}
    for pol_dir in sorted(p for p in results.iterdir() if p.is_dir()):
        for cfg_dir in sorted(p for p in pol_dir.iterdir() if (p / "points.csv").exists()):
            for r in csv.DictReader(open(cfg_dir / "points.csv")):
                skew, t = float(r["skew"]), int(r["threads"])
                d = cfg_dir / f"mysql-skew{skew:g}-{r['system']}"
                st, sp = {}, {}
                for log in (d / f"t{t}-nodes").glob("*-s[0-9].log"):
                    for k, v in last(log, "stats").items():
                        st[k] = st.get(k, 0) + v
                    for k, v in last(log, "speculation").items():
                        sp[k] = sp.get(k, 0) + v
                c = max(st.get("commits", 0), 1)
                s = stalled(d / f"timeseries-t{t}.csv")
                p = dict(threads=t, stalled=s, tps=float(r["tps_raw"]) if s else float(r["tps"]),
                         abort_rate=float(r["abort_rate"]),
                         **{k: 100 * st.get(k, 0) / c for k in ("wounded", "cascade", "coordinator",
                                                                "prepare_wait_die", "lock_timeout")},
                         wb=sp.get("wounded_blocked_total", 0), wr=sp.get("wounded_running_total", 0))
                data.setdefault(pol_dir.name, {}).setdefault(cfg_dir.name, {}).setdefault(skew, []).append(p)
    for cfgs in data.values():
        for skews in cfgs.values():
            for pts in skews.values():
                pts.sort(key=lambda p: p["threads"])
    return data


def peak(pts):
    ok = [p for p in pts if not p["stalled"]]
    return max(ok, key=lambda p: p["tps"]) if ok else None


def style():
    plt.rcParams.update({"figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
                         "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
                         "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
                         "axes.spines.top": False, "axes.spines.right": False, "font.size": 9, "lines.linewidth": 2})


def throughput(data, out):
    pols = sorted(data)
    cfgs = sorted({c for p in data.values() for c in p}, key=order)
    specs = [c for c in cfgs if c != "nonspec"]
    look = {c: dict(color=col, marker=m) for c, (col, m) in zip(specs, SERIES)}
    look["nonspec"] = dict(color=MUTED, marker="x", linestyle="--")
    skews = sorted({s for p in data.values() for c in p.values() for s in c})
    fig, axes = plt.subplots(len(skews), len(pols), figsize=(5.2 * len(pols), 3.0 * len(skews)), squeeze=False,
                             sharex=True)
    for i, skew in enumerate(skews):
        for j, pol in enumerate(pols):
            ax = axes[i][j]
            for cfg in cfgs:
                pts = data[pol].get(cfg, {}).get(skew, [])
                if not pts:
                    continue
                ax.plot([p["threads"] for p in pts], [p["tps"] for p in pts], label=label(cfg), **look[cfg])
                hollow = [p for p in pts if p["stalled"]]
                if hollow:
                    ax.plot([p["threads"] for p in hollow], [p["tps"] for p in hollow], linestyle="none",
                            marker=look[cfg]["marker"], markersize=10, markerfacecolor=SURFACE,
                            markeredgecolor=look[cfg]["color"], markeredgewidth=2)
            ax.set_ylim(bottom=0)
            ax.set_title(f"skew {skew:g}, {pol}", color=INK, fontsize=10)
            if j == 0:
                ax.set_ylabel("committed txns / s")
            if i == len(skews) - 1:
                ax.set_xlabel("client threads")
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False, bbox_to_anchor=(0.5, 1.0))
    fig.text(0.5, -0.01, "hollow marker: stalled point (≥ 5 s at 0 tps), its mean tps over the window",
             ha="center", color=MUTED, fontsize=8)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(out / "sweep-throughput.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def peaks(data, out):
    pols = sorted(data)
    cfgs = sorted({c for p in data.values() for c in p}, key=order)
    specs = [c for c in cfgs if c != "nonspec"]
    colors = {c: col for c, (col, _) in zip(specs, SERIES)}
    colors["nonspec"] = MUTED
    skews = sorted({s for p in data.values() for c in p.values() for s in c})
    fig, axes = plt.subplots(1, len(pols), figsize=(6.5 * len(pols), 3.8), squeeze=False, sharey=True)
    width = 0.8 / len(cfgs)
    for ax, pol in zip(axes[0], pols):
        ax.grid(False)
        ax.grid(True, axis="y", color=GRID, linewidth=0.6)
        for k, skew in enumerate(skews):
            base = peak(data[pol].get("nonspec", {}).get(skew, []))
            for n, cfg in enumerate(cfgs):
                p = peak(data[pol].get(cfg, {}).get(skew, []))
                if not p:
                    continue
                x = k + (n - (len(cfgs) - 1) / 2) * width
                ax.bar(x, p["tps"], width=width * 0.9, color=colors[cfg], label=label(cfg) if k == 0 else None,
                       zorder=3)
                text = f"{p['tps']:.0f}\n@{p['threads']}"
                if cfg != "nonspec" and base:
                    text += f"\n{100 * (p['tps'] / base['tps'] - 1):+.0f}%"
                ax.text(x, p["tps"], text, ha="center", va="bottom", fontsize=6.5, color=INK)
        ax.set_xticks(range(len(skews)), [f"skew {s:g}" for s in skews])
        ax.set_title(pol, color=INK, fontsize=10)
        ax.margins(y=0.2)
    axes[0][0].set_ylabel("peak committed txns / s")
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False, bbox_to_anchor=(0.5, 1.02))
    fig.text(0.5, -0.02, "each bar: the config's best non-stalled thread count (@); % = gain over the "
             "non-speculative peak of the same policy and skew", ha="center", color=MUTED, fontsize=8)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(out / "sweep-peaks.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def table(data, out):
    lines = ["| policy | skew | config | peak tps @ threads | gain vs non-spec | abort rate | wounded | cascade | coord | "
             "prepare wait-die | wound victims blocked / running (all points) | stalled points |",
             "|" + "---|" * 12]
    for pol in sorted(data):
        skews = sorted({s for c in data[pol].values() for s in c})
        for skew in skews:
            base = peak(data[pol].get("nonspec", {}).get(skew, []))
            for cfg in sorted(data[pol], key=order):
                pts = data[pol][cfg].get(skew, [])
                p = peak(pts)
                if not p:
                    lines.append(f"| {pol} | {skew:g} | {label(cfg)} | all stalled | | | | | | | | {len(pts)} |")
                    continue
                gain = f"{100 * (p['tps'] / base['tps'] - 1):+.0f}%" if base and cfg != "nonspec" else ""
                wb, wr = sum(x["wb"] for x in pts), sum(x["wr"] for x in pts)
                lines.append(f"| {pol} | {skew:g} | {label(cfg)} | {p['tps']:.0f} @ {p['threads']} | {gain} | "
                             f"{p['abort_rate']:.2f} | {p['wounded']:.0f} | {p['cascade']:.0f} | {p['coordinator']:.0f} | "
                             f"{p['prepare_wait_die']:.0f} | {wb} / {wr} | {sum(x['stalled'] for x in pts)} |")
    (out / "sweep.md").write_text("# Sweep: peaks per policy, skew and config\n\nAborts per 100 branch commits at the "
                                  "peak point.\n\n" + "\n".join(lines) + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", type=Path)
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()
    out = args.out or args.results
    style()
    data = load(args.results)
    throughput(data, out)
    peaks(data, out)
    table(data, out)
    print(f"wrote sweep-throughput.png, sweep-peaks.png and sweep.md to {out}")


if __name__ == "__main__":
    main()
