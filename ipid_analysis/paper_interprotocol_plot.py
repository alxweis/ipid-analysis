"""Three EU UpSets, from existing outcomes only, in the common 3.33in/7pt style."""

from pathlib import Path

from paper_values.review import COUNTERS, GROUPS, validate_summary


def draw(kind, summary, output: Path):
    from matplotlib.patches import Rectangle
    import matplotlib.pyplot as plt
    from matplotlib.text import Text
    from matplotlib.transforms import blended_transform_factory
    import numpy as np

    from ipid_analysis.interprotocol_outcomes import outcome_plot_style
    from ipid_analysis.paper_plot_style import COLORS, FONT_SIZE, WIDTH, _font
    from ipid_analysis.plot_interprotocol_upset import format_size

    strategy = kind.removeprefix("interprotocol-upset-").upper().replace("-", "_")
    if strategy not in COUNTERS:
        raise ValueError("paper UpSets support the three selected counter strategies")
    validate_summary(summary)
    font, _ = _font()
    categories, colors = outcome_plot_style(strategy)
    colors[-1] = COLORS["NOT_ENOUGH_SAMPLES"]
    counts = np.array(
        [
            [
                summary["groups"].get(g, {}).get("counts", {}).get(strategy + ":" + o, 0)
                for g in GROUPS
            ]
            for o, _ in categories
        ],
        dtype=np.int64,
    )
    totals = counts.sum(axis=0)
    values = np.divide(
        100.0 * counts, totals, out=np.zeros_like(counts, dtype=float), where=totals > 0
    )
    membership = np.array([[p in g.split("-") for g in GROUPS] for p in ("icmp", "tcp", "udp")])
    sizes = membership @ totals
    with plt.rc_context(
        {
            "font.family": font.get_name(),
            "font.size": FONT_SIZE,
            "axes.labelsize": FONT_SIZE,
            "xtick.labelsize": FONT_SIZE,
            "ytick.labelsize": FONT_SIZE,
            "axes.linewidth": 0.45,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    ):
        fig = plt.figure(figsize=(WIDTH, 2.2))
        grid = fig.add_gridspec(
            2,
            1,
            height_ratios=(2.1, 0.85),
            left=0.33,
            right=0.995,
            bottom=0.15,
            top=0.72,
            hspace=0.30,
        )
        bars = fig.add_subplot(grid[0, 0])
        matrix = fig.add_subplot(grid[1, 0], sharex=bars)
        x = np.arange(len(GROUPS))
        bottom = np.zeros(len(GROUPS))
        for heights, color in zip(values, colors):
            bars.bar(
                x, heights, bottom=bottom, width=0.7, color=color, edgecolor="white", linewidth=0.3
            )
            bottom += heights
        bars.set(ylim=(0, 100), ylabel="Percentage [%]", yticks=np.arange(0, 101, 20))
        bars.set_axisbelow(True)
        bars.grid(axis="y", color="#D9D9D9", linewidth=0.4, linestyle="--")
        bars.tick_params(axis="y", length=2, pad=1)
        bars.tick_params(axis="x", bottom=False, labelbottom=False)
        size_y = (bars.get_position().y0 + matrix.get_position().y1) / 2
        transform = blended_transform_factory(bars.transData, fig.transFigure)
        for i, n in enumerate(totals):
            fig.text(
                x[i],
                size_y,
                "(" + format_size(n) + ")",
                transform=transform,
                ha="center",
                va="center",
                fontproperties=font,
            )
        for row in range(3):
            if row % 2 == 0:
                matrix.axhspan(row - 0.45, row + 0.45, color="#EEEEEE", zorder=0)
            matrix.scatter(x, np.full(4, row), color="#C0C0C0", s=10)
        for i in range(4):
            active = np.flatnonzero(membership[:, i])
            matrix.plot([i, i], [active.min(), active.max()], color="#333333", linewidth=0.8)
            matrix.scatter(np.full(len(active), i), active, color="#333333", s=13, zorder=3)
        matrix.set(
            xlim=(-0.5, 3.5),
            ylim=(2.6, -0.6),
            yticks=range(3),
            yticklabels=[
                p.upper() + " (" + format_size(n) + ")"
                for p, n in zip(("icmp", "tcp", "udp"), sizes)
            ],
            xlabel="Protocol Intersection (#IP Addr.)",
            ylabel="Protocol\n(#IP Addr.)",
        )
        matrix.tick_params(axis="both", length=0, pad=1)
        matrix.tick_params(axis="x", labelbottom=False)
        for spine in matrix.spines.values():
            spine.set_visible(False)
        # Fixed 4pt squares, common font, two columns; never shrink text to fit.
        for i, ((_, label), color) in enumerate(zip(categories, colors)):
            col, row = i % 2, i // 2
            px, py = 3 + col * 119, 153 - row * 10
            fig.add_artist(
                Rectangle(
                    (px / (WIDTH * 72), (py - 2) / (2.2 * 72)),
                    4 / (WIDTH * 72),
                    4 / (2.2 * 72),
                    transform=fig.transFigure,
                    facecolor=color,
                    edgecolor="none",
                )
            )
            fig.text(
                (px + 6) / (WIDTH * 72), py / (2.2 * 72), label, va="center", fontproperties=font
            )
        # Family lookup can otherwise select the original CFF face again.
        # Use the common converted/embedded face for EVERY text object.
        for text in fig.findobj(Text):
            text.set_fontproperties(font)
        fig.savefig(
            output,
            format="pdf",
            dpi=300,
            metadata={
                "Title": "EU interprotocol " + strategy,
                "Subject": "Existing selected protocol cohorts; original outcome denominators",
                "Creator": "ipid-analysis",
            },
        )
        plt.close(fig)
