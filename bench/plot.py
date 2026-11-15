#!/usr/bin/env python3
"""Charts and Markdown tables from bench/run.py results (requires matplotlib).

    python bench/plot.py --out figures bench/results/single.json [bench/results/two-nodes.json:2n]

A ``:label`` suffix marks a result file whose targets are shown as
"<target> (<label>)", for example the same volumes spread over two nodes.
"""

import argparse
import json
from pathlib import Path
import statistics

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Categorical palette (fixed order, validated for colour-vision deficiencies);
# colours follow the target, never its rank.
PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
NAMES = {"nfs": "NFS (/home)", "tmpfs": "tmpfs (/dev/shm)", "dagonfs-cs": "DAGonFS client-server",
         "dagonfs-p2p": "DAGonFS peer-to-peer"}
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"


def load(specs):
    targets = {}
    meta = []
    for spec in specs:
        path, _, label = spec.partition(":")
        data = json.loads(Path(path).read_text())
        meta.append((data.get("nodes"), data.get("ranks"), label))
        for key, value in data["targets"].items():
            name = NAMES.get(key, key) + (f", {label}" if label else "")
            targets[name] = value
    return targets, meta


def style(ax, ylabel):
    ax.set_ylabel(ylabel, color=MUTED)
    ax.tick_params(colors=MUTED, length=0)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def grouped_bars(ax, groups, series, values, log=False, fmt="{:.0f}"):
    """values[s][g]; one colour per series, 2 px gaps between adjacent bars."""
    n = len(series)
    width = 0.8 / n
    for s, name in enumerate(series):
        xs = [g + (s - (n - 1) / 2) * width for g in range(len(groups))]
        bars = ax.bar(xs, values[s], width * 0.92, color=PALETTE[s], label=name, edgecolor="white", linewidth=1)
        for bar, value in zip(bars, values[s]):
            ax.annotate(fmt.format(value), (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        ha="center", va="bottom", fontsize=6.5, color=MUTED, rotation=90,
                        xytext=(0, 2), textcoords="offset points")
    ax.set_xticks(range(len(groups)), groups)
    if log:
        ax.set_yscale("log")
        # Headroom for the rotated value labels above the tallest bar.
        ax.set_ylim(top=max(max(v) for v in values) * 6)


def figure_metadata(targets, out):
    ops = ["create", "stat", "unlink", "mkdir", "rmdir"]
    names = list(targets)
    values = [[targets[t]["fs"]["metadata"][op]["median"] for op in ops] for t in names]
    fig, ax = plt.subplots(figsize=(9, 4.2))
    grouped_bars(ax, ops, names, values, log=True, fmt="{:,.0f}")
    style(ax, "operations per second (log scale, median)")
    ax.set_title("Metadata operations (median of 5 runs)", loc="left", color=INK, fontsize=11)
    ax.legend(frameon=False, fontsize=8, ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.08))
    fig.tight_layout()
    fig.savefig(out / "bench-metadata.png", dpi=160)
    plt.close(fig)


def figure_small(targets, out):
    names = list(targets)
    values = [[targets[t]["fs"]["small"][k]["median"] for k in ("write", "read")] for t in names]
    fig, ax = plt.subplots(figsize=(7, 4))
    grouped_bars(ax, ["write (open, write, close)", "read (open, read, close)"], names, values, log=True,
                 fmt="{:,.0f}")
    style(ax, "files per second (log scale, median)")
    ax.set_title("Small files, 4 KiB each (median of 5 runs)", loc="left", color=INK, fontsize=11)
    ax.legend(frameon=False, fontsize=8, ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.08))
    fig.tight_layout()
    fig.savefig(out / "bench-small-files.png", dpi=160)
    plt.close(fig)


def figure_throughput(targets, out):
    fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharey=True)
    for ax, key, title in zip(axes, ("write_MiBps", "read_MiBps"), ("Sequential write + fsync", "Sequential read")):
        for s, name in enumerate(targets):
            rows = targets[name]["fs"]["large"]
            sizes = [r["size"] >> 20 for r in rows]
            ax.plot(sizes, [r[key]["median"] for r in rows], color=PALETTE[s], linewidth=2, marker="o",
                    markersize=5, label=name)
        ax.set_xscale("log", base=2)
        ax.set_xticks(sizes, [f"{s} MiB" for s in sizes])
        ax.set_title(title, loc="left", color=INK, fontsize=11)
        style(ax, "MiB/s (median)" if ax is axes[0] else "")
    axes[0].legend(frameon=False, fontsize=8)
    fig.tight_layout()
    fig.savefig(out / "bench-throughput.png", dpi=160)
    plt.close(fig)


def figure_workflow(targets, out):
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.2))
    names = list(targets)
    titles = {key: "DAGonStar dataflow, " + workflow_label(targets[names[0]][key])
              for key in ("workflow_large", "workflow_small")}
    for ax, key in zip(axes, ("workflow_large", "workflow_small")):
        med = [statistics.median(targets[t][key]["seconds"]) for t in names]
        low = [m - min(targets[t][key]["seconds"]) for m, t in zip(med, names)]
        high = [max(targets[t][key]["seconds"]) - m for m, t in zip(med, names)]
        bars = ax.bar(range(len(names)), med, 0.7, color=PALETTE[:len(names)], edgecolor="white", linewidth=1,
                      yerr=[low, high], ecolor=MUTED, capsize=3)
        base = med[names.index(NAMES["nfs"])] if NAMES["nfs"] in names else None
        for bar, value, name, up in zip(bars, med, names, high):
            faster = base and value and name != NAMES["nfs"]
            label = f"{value:.1f} s" + (f"\n{base / value:.2f}× vs NFS" if faster else "")
            # Above the whisker, so that the label never overlaps it.
            ax.annotate(label, (bar.get_x() + bar.get_width() / 2, value + up), ha="center", va="bottom",
                        fontsize=7.5, color=MUTED, xytext=(0, 3), textcoords="offset points")
        ax.set_xticks(range(len(names)), [n.replace(" ", "\n", 1) for n in names], fontsize=7.5)
        ax.set_title(titles[key], loc="left", color=INK, fontsize=11)
        ax.set_ylim(0, max(m + h for m, h in zip(med, high)) * 1.3)
        style(ax, "wall time, s (median; whiskers: min–max)" if ax is axes[0] else "")
    fig.tight_layout()
    fig.savefig(out / "bench-workflow.png", dpi=160)
    plt.close(fig)


def workflow_label(run):
    if run["kind"] == "large":
        return f"{run['width']} × {run['size_mib']} MiB"
    return f"{run['files']} × 8 KiB files, {run['width']} tasks"


def tables(targets):
    names = list(targets)
    lines = ["| Metric | " + " | ".join(names) + " |", "| --- |" + " ---: |" * len(names)]

    def row(label, values, fmt):
        lines.append(f"| {label} | " + " | ".join(fmt.format(v) for v in values) + " |")

    for op in ("create", "stat", "unlink", "mkdir", "rmdir"):
        row(f"{op} (ops/s)", [targets[t]["fs"]["metadata"][op]["median"] for t in names], "{:,.0f}")
    for kind in ("write", "read"):
        row(f"4 KiB files, {kind} (files/s)", [targets[t]["fs"]["small"][kind]["median"] for t in names], "{:,.0f}")
    for index, entry in enumerate(targets[names[0]]["fs"]["large"]):
        size = entry["size"] >> 20
        for kind in ("write", "read"):
            row(f"{size} MiB {kind} (MiB/s)",
                [targets[t]["fs"]["large"][index][f"{kind}_MiBps"]["median"] for t in names], "{:,.0f}")
    for key in ("workflow_large", "workflow_small"):
        label = f"DAGonStar, {workflow_label(targets[names[0]][key])} (s)"
        row(label, [statistics.median(targets[t][key]["seconds"]) for t in names], "{:.2f}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("results", nargs="+", help="result files, optionally suffixed with :label")
    parser.add_argument("--out", type=Path, default=Path("figures"))
    args = parser.parse_args()
    targets, meta = load(args.results)
    args.out.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.size": 9, "font.family": "DejaVu Sans"})
    figure_metadata(targets, args.out)
    figure_small(targets, args.out)
    figure_throughput(targets, args.out)
    figure_workflow(targets, args.out)
    print(tables(targets))
    for nodes, ranks, label in meta:
        print(f"<!-- {label or 'main'}: nodes {nodes}, {ranks} ranks -->")


if __name__ == "__main__":
    main()
