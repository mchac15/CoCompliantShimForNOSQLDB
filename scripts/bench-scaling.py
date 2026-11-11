#!/usr/bin/env python3
"""How every term of a run scales with the thread count: what grows faster than throughput and caps it.

    scripts/bench-scaling.py OUT_PNG --skew 0.95 LABEL=CONFIG_DIR [LABEL=CONFIG_DIR ...]

CONFIG_DIR is a bench-peak.py --out run (one system). Per thread count: throughput; the txns in flight
(throughput x time per committed txn, Little's law); the time per committed txn split into earlier failed
attempts (by abort cause, in the proportions of the shims' lost time) and the final attempt (execution, lock wait,
prepare wait, 2PC decision wait); aborts per 100 branch commits, primary (wounded, prepare wait-die NO: they start
a failure) and secondary (coordinator: the sibling branch failed; cascade: a txn read from an aborted one); the
amplification (secondary per primary abort, cascade victims per coordinator abort); the useful share (final
attempt / time per txn). Writes OUT_PNG and OUT_PNG with .md (every number as a table).
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
SERIES = [("#2a78d6", "o"), ("#eb6834", "s"), ("#1baf7a", "^"), ("#eda100", "D")]
CAUSES = ["wounded", "prepare_wait_die", "coordinator", "cascade"]


def last(log, prefix):
    line = ""
    for l in open(log, errors="replace"):
        if l.startswith(prefix + " "):
            line = l
    return {k: int(v) for k, v in re.findall(r"(\w+)=(-?\d+)\b", line)}


def point(pdir, t, tps):
    run = list(csv.DictReader(open(pdir / f"t{t}.csv")))[0]
    txns = [r for r in csv.DictReader(open(pdir / f"txns-t{t}.csv")) if r["outcome"] == "COMMITTED"]
    e2e = statistics.mean(int(r["latency_us"]) for r in txns) / 1000
    final = float(run["mean_us"]) / 1000
    st, sp = {}, {}
    logs = sorted((pdir / f"t{t}-nodes").glob("*-s[0-9].log"))
    for log in logs:
        for k, v in last(log, "stats").items():
            st[k] = st.get(k, 0) + v
        for k, v in last(log, "speculation").items():
            sp[k] = sp.get(k, 0) + v
    c = max(st.get("commits", 0), 1)
    per = {k: 100 * st.get(k, 0) / c for k in CAUSES}
    lost = {k: sp.get(f"lost_{k}_us_total", 0) for k in CAUSES}
    retries = e2e - final
    tot = sum(lost.values()) or 1
    # the final attempt from the shims' budget of a committed branch: the branches lock one after the other,
    # they wait in prepare and for the decision together
    lock = prep = post = 0.0
    for log in logs:
        s1, p1 = last(log, "stats"), last(log, "speculation")
        n = max(s1.get("commits", 0), 1)
        lock += p1.get("committed_lock_wait_us_total", 0) / n / 1000
        prep = max(prep, p1.get("committed_prepare_wait_us_total", 0) / n / 1000)
        post = max(post, p1.get("committed_post_vote_us_total", 0) / n / 1000)
    primary = per["wounded"] + per["prepare_wait_die"]
    coord_n = st.get("coordinator", 0)
    victims = sp.get("cascade_root_coordinator_victims_total", 0) + sp.get("cascade_root_prepare_wait_die_victims_total", 0)
    return dict(
        threads=t, tps=tps, e2e=e2e, inflight=tps * e2e / 1000, final=final, retries=retries,
        attempts=statistics.mean(int(r["attempts"]) for r in txns),
        **{f"ms_{k}": retries * lost[k] / tot for k in CAUSES},
        exec=max(final - lock - prep - post, 0), lock=lock, prep=prep, post=post,
        **{f"per_{k}": per[k] for k in CAUSES}, primary=primary,
        secondary_per_primary=(per["coordinator"] + per["cascade"]) / primary if primary else 0,
        coord_per_primary=per["coordinator"] / primary if primary else 0,
        victims_per_root=victims / max(coord_n + st.get("prepare_wait_die", 0), 1),
        useful=final / e2e, speculated=sp.get("speculated_commits", 0) / max(sp.get("commits", 1), 1))


def load(cfg_dir, skew):
    pdir = next(p for p in cfg_dir.iterdir() if p.is_dir() and p.name.startswith(("mysql-", "pg-")))
    pts = [point(pdir, int(r["threads"]), float(r["tps"])) for r in csv.DictReader(open(cfg_dir / "points.csv"))
           if abs(float(r["skew"]) - skew) < 1e-9]
    return sorted(pts, key=lambda p: p["threads"])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("out", type=Path)
    ap.add_argument("--skew", type=float, required=True)
    ap.add_argument("runs", nargs="+", help="LABEL=CONFIG_DIR")
    args = ap.parse_args()
    runs = [(lab, load(Path(d), args.skew)) for lab, d in (r.split("=", 1) for r in args.runs)]
    plt.rcParams.update({"figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
                         "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
                         "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
                         "axes.spines.top": False, "axes.spines.right": False, "font.size": 9, "lines.linewidth": 2})
    panels = [("tps", "committed txns / s"), ("inflight", "txns in flight (tps × time per txn)"),
              ("e2e", "time per committed txn, ms"), ("final", "final attempt, ms"),
              ("attempts", "attempts per committed txn"), ("primary", "primary aborts per 100 commits\n(wounded + prepare wait-die)"),
              ("secondary_per_primary", "secondary per primary abort\n((coordinator + cascade) / primary)"),
              ("useful", "useful share\n(final attempt / time per txn)")]
    fig, axes = plt.subplots(2, 4, figsize=(16, 7.2))
    for ax, (y, title) in zip(axes.flat, panels):
        for (lab, pts), (color, marker) in zip(runs, SERIES):
            ax.plot([p["threads"] for p in pts], [p[y] for p in pts], color=color, marker=marker, label=lab)
        ax.set_title(title, color=INK, fontsize=9)
        ax.set_ylim(bottom=0)
        ax.set_xlabel("client threads")
    axes.flat[-1].yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False, bbox_to_anchor=(0.5, 1.02))
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(args.out, dpi=150, bbox_inches="tight")
    cols = ["tps", "inflight", "e2e", "attempts", "final", "exec", "lock", "prep", "post", "retries", "ms_wounded",
            "ms_prepare_wait_die", "ms_coordinator", "ms_cascade", "per_wounded", "per_prepare_wait_die",
            "per_coordinator", "per_cascade", "secondary_per_primary", "coord_per_primary", "victims_per_root",
            "useful", "speculated"]
    lines = ["| run | threads | " + " | ".join(cols) + " |", "|" + "---|" * (len(cols) + 2)]
    for lab, pts in runs:
        for p in pts:
            lines.append(f"| {lab} | {p['threads']} | " + " | ".join(
                f"{p[c]:.2f}" if c in ("attempts", "secondary_per_primary", "coord_per_primary", "victims_per_root",
                                       "useful", "speculated") else f"{p[c]:.0f}" for c in cols) + " |")
    args.out.with_suffix(".md").write_text(
        f"# Scaling, skew {args.skew:g}\n\nms per committed txn; aborts per 100 branch commits; inflight = tps x "
        "time per txn.\n\n" + "\n".join(lines) + "\n")
    print(args.out)


if __name__ == "__main__":
    main()
