"""Approved two-column EU counter UpSet, from the existing small summary only."""

from decimal import ROUND_HALF_UP, Decimal
from io import BytesIO
from pathlib import Path

from paper_values.review import GROUPS, validate_summary

KIND = "interprotocol-upset-combined"
PANELS = (
    ("PER_DESTINATION", "Per-Destination"),
    ("SINGLE", "Single"),
    ("PER_BUCKET", "Per-Bucket"),
)
LABELS = {
    "PROTOCOL_SHARED": "Inter-protocol Counter",
    "PARTIAL_PROTOCOL_SHARED": "Partial Inter-protocol Counter",
    "PROTOCOL_ISOLATED": "Per-protocol Counter",
    "STRATEGY_CHANGED": "Selection Strategy changed",
    "NOT_ENOUGH_SAMPLES": "Not Enough Samples",
}
WIDTH, HEIGHT = 7.0, 2.2
AXIS_WIDTH, X_TICK_LENGTH = 0.45, 2.5


def segment_label(count, total):
    """Threshold exact counts before rounding; a displayed 10 need not qualify."""
    if not total or count * 10 < total:
        return None
    return str(
        (Decimal(int(count)) * 100 / Decimal(int(total))).quantize(
            Decimal(1), rounding=ROUND_HALF_UP
        )
    )


def draw(kind, summary, output: Path):
    import matplotlib.pyplot as plt

    from ipid_analysis.paper_plot_style import FONT_SIZE, _font

    if kind != KIND:
        raise ValueError("unsupported combined interprotocol plot")
    validate_summary(summary)
    font, _ = _font()
    with plt.rc_context(
        {
            "font.family": font.get_name(),
            "font.size": FONT_SIZE,
            "axes.labelsize": FONT_SIZE,
            "xtick.labelsize": FONT_SIZE,
            "ytick.labelsize": FONT_SIZE,
            "axes.linewidth": AXIS_WIDTH,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    ):
        fig = plt.figure(figsize=(WIDTH, HEIGHT))
        try:
            _draw(fig, summary, font)
            _save(fig, output)
        finally:
            plt.close(fig)


def _draw(fig, summary, font):
    from matplotlib.patches import Rectangle
    from matplotlib.text import Text
    from matplotlib.textpath import TextPath
    import numpy as np

    from ipid_analysis.interprotocol_outcomes import outcome_plot_style
    from ipid_analysis.paper_heatmap_style import format_population
    from ipid_analysis.paper_plot_style import COLORS

    wp, hp = WIDTH * 72, HEIGHT * 72
    membership = np.array(
        [[protocol in group.split("-") for group in GROUPS] for protocol in ("icmp", "tcp", "udp")]
    )
    titles, matrices, bar_axes, count_labels = [], [], [], []
    for panel, (strategy, title) in enumerate(PANELS):
        categories, colors = outcome_plot_style(strategy)
        categories = [(key, LABELS[key]) for key, _ in categories]
        colors[-1] = COLORS["NOT_ENOUGH_SAMPLES"]
        counts = np.array(
            [
                [
                    summary["groups"].get(group, {}).get("counts", {}).get(strategy + ":" + key, 0)
                    for group in GROUPS
                ]
                for key, _ in categories
            ],
            dtype=np.int64,
        )
        totals = counts.sum(axis=0)
        values = np.divide(
            100.0 * counts, totals, out=np.zeros_like(counts, dtype=float), where=totals > 0
        )
        sizes = membership @ totals
        left, frame_width = 63 + panel * 160, 110
        bars = fig.add_axes([left / wp, 60 / hp, frame_width / wp, 64 / hp])
        bar_axes.append(bars)
        matrix = fig.add_axes([left / wp, 19 / hp, frame_width / wp, 25 / hp], sharex=bars)
        matrices.append(matrix)
        x = np.arange(4)
        bottom = np.zeros(4)
        for segment, color, count_values in zip(values, colors, counts):
            bars.bar(
                x,
                segment,
                bottom=bottom,
                width=0.7,
                facecolor=color,
                edgecolor="none",
                linewidth=0,
            )
            for i, percent in enumerate(segment):
                label = segment_label(count_values[i], totals[i])
                if label is not None:
                    bars.text(
                        i,
                        bottom[i] + percent / 2,
                        label,
                        color="black",
                        ha="center",
                        va="center",
                        fontproperties=font,
                        zorder=5,
                    )
            bottom += segment
        # One 0.2pt separator per nonempty internal boundary; no double strokes.
        for i in range(4):
            boundaries = np.unique(np.cumsum(values[:, i])[:-1])
            boundaries = boundaries[(boundaries > 1e-9) & (boundaries < 100 - 1e-9)]
            bars.hlines(boundaries, i - 0.35, i + 0.35, color="#222222", linewidth=0.2, zorder=4)
        bars.set(xlim=(-0.5, 3.5), ylim=(0, 100), yticks=np.arange(0, 101, 20))
        bars.set_axisbelow(True)
        bars.grid(axis="y", color="#BDBDBD", linewidth=0.35, linestyle="--", alpha=0.7)
        bars.set_xticks(x)
        bars.tick_params(
            axis="x",
            bottom=True,
            top=False,
            labelbottom=False,
            length=X_TICK_LENGTH,
            width=AXIS_WIDTH,
            direction="out",
        )
        bars.tick_params(axis="y", length=2, width=AXIS_WIDTH, pad=1, labelleft=panel == 0)
        if panel == 0:
            bars.set_ylabel("Percentage [%]", labelpad=3)
        titles.append(
            fig.text(
                (left + frame_width / 2) / wp,
                131 / hp,
                title,
                ha="center",
                va="center",
                fontproperties=font,
            )
        )
        count_labels.append(
            [
                fig.text(
                    (left + (i + 0.5) * frame_width / 4) / wp,
                    52 / hp,
                    "(" + format_population(int(n)) + ")",
                    ha="center",
                    va="top",
                    fontproperties=font,
                )
                for i, n in enumerate(totals)
            ]
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
            ylim=(2.6, -0.6),
            yticks=range(3),
            yticklabels=[
                p.upper() + " (" + format_population(int(n)) + ")"
                for p, n in zip(("icmp", "tcp", "udp"), sizes)
            ],
        )
        matrix.tick_params(axis="both", length=0, pad=1)
        matrix.tick_params(axis="x", labelbottom=False)
        for spine in matrix.spines.values():
            spine.set_visible(False)

    protocol_label = fig.text(
        12 / wp,
        31.5 / hp,
        "Protocol\n(#IP Addr.)",
        rotation=90,
        ha="center",
        va="center",
        fontproperties=font,
    )
    xlabel = fig.text(
        0.5,
        6.5 / hp,
        "Protocol Intersection (#IP Addr.)",
        ha="center",
        va="top",
        fontproperties=font,
    )
    # One centered row, 4pt squares, 1.2pt swatch/label gaps, 11pt between entries.
    widths = [TextPath((0, 0), label, prop=font).get_extents().width for _, label in categories]
    item_widths = [4 + 1.2 + width for width in widths]
    total_width = sum(item_widths) + 11 * (len(item_widths) - 1)
    if total_width >= wp - 4:
        raise ValueError("combined UpSet legend does not fit at 7pt")
    cursor = (wp - total_width) / 2
    legend_texts, legend_patches = [], []
    for (_, label), color, item_width in zip(categories, colors, item_widths):
        patch = Rectangle(
            (cursor / wp, 148 / hp),
            4 / wp,
            4 / hp,
            transform=fig.transFigure,
            facecolor=color,
            edgecolor="#222222",
            linewidth=0.3,
        )
        fig.add_artist(patch)
        legend_patches.append(patch)
        legend_texts.append(
            fig.text((cursor + 5.2) / wp, 150 / hp, label, va="center", fontproperties=font)
        )
        cursor += item_width + 11
    for text in fig.findobj(Text):
        text.set_fontproperties(font)
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    for patch, text in zip(legend_patches, legend_texts):
        box = text.get_window_extent(renderer)
        patch.set_y((box.y0 + box.y1) / 2 / fig.bbox.height - patch.get_height() / 2)
    for bars, labels, matrix in zip(bar_axes, count_labels, matrices):
        bar_bottom = bars.get_window_extent(renderer).y0
        for text in labels:
            text.set_y((bar_bottom - (X_TICK_LENGTH + 2) * fig.dpi / 72) / fig.bbox.height)
        count_bottom = min(text.get_window_extent(renderer).y0 for text in labels)
        position = matrix.get_position()
        matrix.set_position(
            [
                position.x0,
                (count_bottom - 2 * fig.dpi / 72) / fig.bbox.height - position.height,
                position.width,
                position.height,
            ]
        )
    protocol_label.set_y(
        sum(ax.get_position().y0 + ax.get_position().height / 2 for ax in matrices) / len(matrices)
    )
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    matrix_bottom = min(ax.get_window_extent(renderer).y0 for ax in matrices)
    xlabel.set_y((matrix_bottom - 4 * fig.dpi / 72) / fig.bbox.height)
    title_top = max(text.get_window_extent(renderer).y1 for text in titles)
    legend_bottom = min(
        artist.get_window_extent(renderer).y0 for artist in (*legend_texts, *legend_patches)
    )
    shift = (title_top + 4 * fig.dpi / 72 - legend_bottom) / fig.bbox.height
    for text in legend_texts:
        text.set_y(text.get_position()[1] + shift)
    for patch in legend_patches:
        patch.set_y(patch.get_y() + shift)
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    for text in fig.findobj(Text):
        if not text.get_visible() or not text.get_text().strip():
            continue
        box = text.get_window_extent(renderer)
        if (
            box.x0 < -0.5
            or box.y0 < -0.5
            or box.x1 > fig.bbox.width + 0.5
            or box.y1 > fig.bbox.height + 0.5
        ):
            raise ValueError(f"combined UpSet label would be clipped: {text.get_text()}")
    bounds = [text.get_window_extent(renderer) for text in legend_texts]
    if any(a.x1 >= b.x0 for a, b in zip(bounds, bounds[1:])):
        raise ValueError("combined UpSet legend labels overlap")


def _save(fig, output):
    """Crop vertical whitespace only; retain the full 7in vector PDF and font size."""
    from matplotlib.transforms import Bbox
    from PIL import Image, ImageChops

    dpi = 300
    # Inspect a small in-memory preview, like the shared paper bars' ink-row crop.
    # This is only a page-bound calculation; the exported PDF remains vector.
    with BytesIO() as buffer:
        fig.savefig(buffer, format="png", dpi=dpi)
        buffer.seek(0)
        with Image.open(buffer) as preview:
            rgb = preview.convert("RGB")
            bounds = ImageChops.difference(rgb, Image.new("RGB", rgb.size, "white")).getbbox()
            low = max(0, (rgb.height - bounds[3] - 1) / dpi)
            high = min(HEIGHT, (rgb.height - bounds[1] + 1) / dpi)
    fig.savefig(
        output,
        format="pdf",
        dpi=dpi,
        bbox_inches=Bbox.from_extents(0, low, WIDTH, high),
        pad_inches=0,
        metadata={
            "Title": "EU interprotocol counter strategies",
            "Subject": "Per-Destination, Single, Per-Bucket; original cohort denominators",
            "Creator": "ipid-analysis",
        },
    )
