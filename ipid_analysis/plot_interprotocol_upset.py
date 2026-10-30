"""UpSet-style campaign cohorts, stacked by observed interprotocol outcome.

Only selected measurement targets are represented, not all original protocol
responders. Pair cohorts exclude same-strategy triple targets. Membership
indicates the measurement cohort, not inferred counter sharing.
"""

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
from matplotlib.transforms import blended_transform_factory
import numpy as np

from ipid_analysis.artifacts import save_figure
from ipid_analysis.interprotocol import GROUPS
from ipid_analysis.interprotocol_outcomes import STRATEGIES, outcome_plot_style
from ipid_analysis.paper_figures import (
    COMPACT_PAPER_PDF_PADDING_INCHES,
    COMPACT_PAPER_STROKE_WIDTH,
    configure_compact_validation_style,
)


def format_size(value):
    """Compact counts with decimal points and no thousands separators."""
    value = int(value)
    if value <= 10000:
        return str(value)
    scale, suffix = (1000000, "M") if value >= 1000000 else (1000, "k")
    return f"{value / scale:.2f}".rstrip("0").rstrip(".") + suffix


def plot_campaign_upset(summary_path: Path, output_dir: Path):
    """Render normalized outcomes with absolute intersection and set sizes."""
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    configure_compact_validation_style()
    output_dir.mkdir(parents=True, exist_ok=True)
    groups = list(GROUPS)
    protocols = ("icmp", "tcp", "udp")
    membership = np.array([[p in g.split("-") for g in groups] for p in protocols])
    outputs = []
    for strategy in STRATEGIES:
        categories, colors = outcome_plot_style(strategy)
        values = np.array(
            [
                [
                    summary["groups"].get(g, {}).get("counts", {}).get(f"{strategy}:{o}", 0)
                    for g in groups
                ]
                for o, _ in categories
            ]
        )
        totals = values.sum(axis=0)
        sizes = membership @ totals
        percentages = np.divide(
            100.0 * values,
            totals,
            out=np.zeros_like(values, dtype=float),
            where=totals > 0,
        )
        fig = plt.figure(figsize=(5.0, 2.9))
        grid = fig.add_gridspec(
            2,
            1,
            height_ratios=(2.1, 0.85),
            left=0.34,
            right=0.88,
            bottom=0.16,
            top=0.83 if len(categories) > 3 else 0.89,
            hspace=0.27,
        )
        bars = fig.add_subplot(grid[0, 0])
        matrix = fig.add_subplot(grid[1, 0], sharex=bars)
        x = np.arange(len(groups)) * 0.66
        bottom = np.zeros(len(groups))
        for (_, label), heights, color in zip(categories, percentages, colors, strict=True):
            bars.bar(
                x,
                heights,
                bottom=bottom,
                width=0.46,
                color=color,
                edgecolor="white",
                linewidth=COMPACT_PAPER_STROKE_WIDTH,
                label=label,
            )
            bottom += heights
        size_y = (bars.get_position().y0 + matrix.get_position().y1) / 2
        size_transform = blended_transform_factory(bars.transData, fig.transFigure)
        for i, n in enumerate(totals):
            fig.text(
                x[i],
                size_y,
                f"({format_size(n)})",
                transform=size_transform,
                ha="center",
                va="center",
            )
        bars.set_ylim(0, 100)
        bars.set_ylabel("Percentage [%]")
        bars.set_yticks(np.arange(0, 101, 20))
        bars.yaxis.set_minor_locator(MultipleLocator(10))
        bars.tick_params(axis="y", which="major", length=5, width=COMPACT_PAPER_STROKE_WIDTH)
        bars.tick_params(axis="y", which="minor", length=2.8, width=COMPACT_PAPER_STROKE_WIDTH)
        bars.set_axisbelow(True)
        bars.grid(
            axis="y", which="major", color="#BDBDBD", linestyle="--", linewidth=0.5, alpha=0.7
        )
        bars.tick_params(axis="x", bottom=False, labelbottom=False)
        for spine in bars.spines.values():
            spine.set_visible(True)
            spine.set_color("black")
            spine.set_linewidth(COMPACT_PAPER_STROKE_WIDTH)

        for row in range(3):
            if row % 2 == 0:
                matrix.axhspan(row - 0.45, row + 0.45, color="#E6E6E6", zorder=0)
            matrix.scatter(x, np.full(len(x), row), color="#B8B8B8", s=19, zorder=2)
        for col, position in enumerate(x):
            active = np.flatnonzero(membership[:, col])
            matrix.plot(
                [position, position],
                [active.min(), active.max()],
                color="#333333",
                linewidth=1.1,
                zorder=3,
            )
            matrix.scatter(np.full(len(active), position), active, color="#333333", s=23, zorder=4)
        matrix.set_xlim(x[0] - 0.30, x[-1] + 0.30)
        matrix.set_ylim(2.6, -0.6)
        matrix.set_yticks(
            range(3), [f"{p.upper()} ({format_size(n)})" for p, n in zip(protocols, sizes)]
        )
        matrix.set_ylabel("Protocol\n(#IP Addr.)")
        matrix.tick_params(axis="both", length=0)
        matrix.tick_params(axis="x", labelbottom=False)
        matrix.set_xlabel("Protocol Intersection (#IP Addr.)", labelpad=3)
        for spine in matrix.spines.values():
            spine.set_visible(False)

        handles, labels = bars.get_legend_handles_labels()
        fig.legend(
            handles,
            labels,
            frameon=False,
            loc="upper center",
            bbox_to_anchor=(0.5, 0.99),
            ncol=min(3, len(categories)),
            columnspacing=0.65,
            handlelength=1.0,
            handletextpad=0.3,
            borderaxespad=0,
            borderpad=0,
            labelspacing=0.3,
        )
        stem = "interprotocol-upset-" + strategy.lower().replace("_", "-")
        pdf = output_dir / f"{stem}.pdf"
        save_figure(fig, pdf, bbox_inches="tight", pad_inches=COMPACT_PAPER_PDF_PADDING_INCHES)
        plt.close(fig)
        outputs.append(pdf)
    return outputs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=Path)
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args()
    plot_campaign_upset(args.summary, args.output_dir)
