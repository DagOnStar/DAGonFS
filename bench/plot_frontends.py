#!/usr/bin/env python3
"""Plot frontend probe results (bench/frontend_probe.py JSON files).

    python3 bench/plot_frontends.py OUTPUT.png RESULT.json[=LABEL] [...]

One panel per operation (pwrite, pread); one group per result file (host);
bars are medians, whiskers span the minimum and maximum of the repeats.
"""
import json
import statistics
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Fixed categorical order: a mode keeps its colour whichever modes a host has.
MODES = [("native", "Native"), ("preload", "LD_PRELOAD"), ("fuse", "FUSE3 client-server"),
         ("fuse-p2p", "FUSE3 peer-to-peer"), ("local", "Local file (reference)")]
COLOURS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#8a8a85"]


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    output, results = Path(sys.argv[1]), []
    for argument in sys.argv[2:]:
        path, _, label = argument.partition("=")
        result = json.loads(Path(path).read_text())
        # Early result files used these names for the FUSE and reference modes.
        for old, new in (("fuse3", "fuse"), ("fuse3_p2p", "fuse-p2p"), ("local_tempfile", "local")):
            if old in result:
                result[new] = result.pop(old)
        result.setdefault("size", 4096)
        result.setdefault("ops", 100)
        result.setdefault("ranks", 2)
        result["label"] = label or result["host"]
        results.append(result)
    present = [i for i, (key, _) in enumerate(MODES) if any(key in r for r in results)]
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.2), sharey=True)
    width = 0.8 / len(present)
    for ax, operation in zip(axes, ("pwrite", "pread")):
        for slot, index in enumerate(present):
            key, label = MODES[index]
            xs, medians, low, high = [], [], [], []
            for group, result in enumerate(results):
                if key not in result:
                    continue
                values = result[key][operation]
                median = statistics.median(values)
                xs.append(group + (slot - (len(present) - 1) / 2) * width)
                medians.append(median)
                low.append(median - min(values))
                high.append(max(values) - median)
            ax.bar(xs, medians, width=width * 0.9, color=COLOURS[index], label=label,
                   yerr=[low, high], error_kw={"elinewidth": 1, "capsize": 2, "ecolor": "#555"})
        ax.set_xticks(range(len(results)), [r["label"] for r in results])
        ax.set_yscale("log")
        ax.set_ylim(bottom=1e4)
        ax.set_title(f"{results[0]['size'] // 1024} KiB {operation}", fontsize=11)
        ax.grid(axis="y", color="#ddd", linewidth=0.6)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
    axes[0].set_ylabel("Operations per second (log scale, median, min–max)")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=len(labels), frameon=False, fontsize=9)
    fig.suptitle(f"DAGonFS frontends: {results[0]['ops']} sequential positional operations per run, "
                 f"{results[0]['ranks']} MPI ranks, 5 runs", fontsize=11)
    fig.tight_layout(rect=(0, 0.08, 1, 0.95))
    fig.savefig(output, dpi=170)


if __name__ == "__main__":
    main()
