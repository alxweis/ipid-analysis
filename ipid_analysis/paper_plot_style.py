"""Opt-in single-column IMC design; existing plotting defaults stay untouched."""

import atexit
from functools import lru_cache
from pathlib import Path
import tempfile

import matplotlib

matplotlib.use("Agg")

from matplotlib import font_manager  # noqa: E402
from matplotlib.patches import Polygon, Rectangle  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.text import Text  # noqa: E402
from matplotlib.textpath import TextPath  # noqa: E402
from matplotlib.transforms import Bbox  # noqa: E402
import numpy as np  # noqa: E402

from ipid_analysis.paper_figures import linux_libertine_font_properties  # noqa: E402
from ipid_analysis.runtime import scratch_root  # noqa: E402
from ipid_analysis.strategies import (  # noqa: E402
    PAPER_STRATEGY_ORDER,
    STRATEGY_COLORS,
    STRATEGY_PRETTY,
)

STYLE_VERSION = "imc-single-column-v1"
WIDTH, HEIGHT, FONT_SIZE = 3.33, 1.1715, 7.0
COLORS = {
    **STRATEGY_COLORS,
    "CONSTANT": "#94AECB",
    "UNCLASSIFIED": "#D0C8C4",
    "NOT_ENOUGH_SAMPLES": "#BE9D8B",
}
ORDER = tuple(PAPER_STRATEGY_ORDER)
MODES = ("RT-based", "Fixed-Interval")
ROLES = ("Transit-Observed", "No Transit Evidence")


def measure_heatmap_cell(fig, font, text="100.0"):
    """Measure annotation width with 5px side padding; row height is always 10pt."""
    from ipid_analysis.paper_heatmap_style import CELL_HEIGHT_POINTS

    probe = fig.text(0.5, 0.5, text, ha="center", va="center", fontproperties=font)
    try:
        fig.canvas.draw()
        box = probe.get_window_extent(fig.canvas.get_renderer())
        return box.width + 10, CELL_HEIGHT_POINTS * fig.dpi / 72
    finally:
        probe.remove()


@lru_cache(maxsize=1)
def _font():
    """Reuse font discovery; convert CFF outlines once per worker for valid Type42 PDFs."""
    from fontTools.fontBuilder import FontBuilder
    from fontTools.pens.cu2quPen import Cu2QuPen
    from fontTools.pens.ttGlyphPen import TTGlyphPen
    from fontTools.ttLib import TTFont

    properties = linux_libertine_font_properties("R", size=FONT_SIZE)
    font = TTFont(properties.get_file())
    if "CFF " not in font:
        font.close()
        return properties, None
    temporary = tempfile.TemporaryDirectory(prefix="paper-font-", dir=scratch_root())
    atexit.register(temporary.cleanup)
    glyph_set, glyphs = font.getGlyphSet(), {}
    for name in font.getGlyphOrder():
        pen = TTGlyphPen(None)
        glyph_set[name].draw(Cu2QuPen(pen, max_err=0.5, reverse_direction=True))
        glyphs[name] = pen.glyph()
    del font["CFF "]
    font.sfntVersion = "\x00\x01\x00\x00"
    builder = FontBuilder(font=font)
    builder.isTTF = True
    builder.setupGlyf(glyphs)
    builder.setupMaxp()
    builder.setupPost()
    path = Path(temporary.name) / "LinuxLibertine.ttf"
    font.save(path)
    font.close()
    return font_manager.FontProperties(fname=path, size=FONT_SIZE), temporary


def _legend(fig, font, *, itdk, entries=None):
    # Reserve the same five columns in both figures, even when the last entry
    # is absent because the ITDK report combines insufficient samples with Unclassified.
    strategy_legend = entries is None
    if strategy_legend:
        entries = [(key, STRATEGY_PRETTY[key], COLORS[key]) for key in ORDER]
    if not entries:
        return []
    widths = [TextPath((0, 0), label, prop=font).get_extents().x1 for _, label, _ in entries]
    handle, gap = 4.0, 1.2
    height = fig.get_size_inches()[1]
    column_count = min(5, len(entries))
    row_count = (len(entries) + column_count - 1) // column_count
    columns = [
        handle + gap + max(widths[i : i + row_count]) for i in range(0, len(entries), row_count)
    ]
    x = 72 / 300
    between = (WIDTH * 72 - 2 * x - sum(columns)) / max(1, len(columns) - 1)
    if between <= 0:
        raise ValueError("Linux Libertine legend does not fit the 3.33-inch page")
    texts = []
    for column in range(len(columns)):
        for row in range(row_count):
            index = column * row_count + row
            if index >= len(entries):
                continue
            key, label, color = entries[index]
            if strategy_legend and itdk and key == "NOT_ENOUGH_SAMPLES":
                continue
            y = height * 72 - 5.5 - row * 9
            fig.add_artist(
                Rectangle(
                    (x / (WIDTH * 72), (y - 2) / (height * 72)),
                    4 / (WIDTH * 72),
                    4 / (height * 72),
                    transform=fig.transFigure,
                    facecolor=color,
                    edgecolor="none",
                )
            )
            texts.append(
                fig.text(
                    (x + handle + gap) / (WIDTH * 72),
                    y / (height * 72),
                    label,
                    ha="left",
                    va="center",
                    fontproperties=font,
                )
            )
        x += columns[column] + between
    return texts


def draw(kind: str, percentages, output: Path) -> None:
    """Render normalized rows; no analysis, joins, or source-file writes."""
    font, _ = _font()
    os_roles = kind == "itdk-role-by-os"
    itdk = kind in ("itdk-role-by-strategy", "itdk-role-by-os")
    entries = None
    if os_roles:
        from ipid_analysis.plot_itdk_os import _group_color, _group_label

        entries = [(key, _group_label(key), _group_color(key)) for key in percentages.groups]
        percentages = percentages.percentages
    connection = kind == "measurement-type-by-strategy-with-connection"
    tcp_flags = kind == "tcp-flags-by-strategy"
    rows = ROLES if itdk else MODES
    labels = ("Router", "All Other") if itdk else MODES
    height = HEIGHT
    if connection:
        from ipid_analysis.plot_strategy_refinement import MODE_LABELS, MODES_WITH_CONNECTION

        rows = MODES_WITH_CONNECTION
        labels = tuple(MODE_LABELS.get(row, row) for row in rows)
        height = 1.4025
    elif tcp_flags:
        from ipid_analysis.plot_tcp_flags_strategy import FLAG_GROUPS

        rows = labels = FLAG_GROUPS
        height = 1.4025
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
        fig = plt.figure(
            figsize=(WIDTH, height), dpi=300 if connection or tcp_flags or os_roles else 240
        )
        try:
            _draw(
                fig,
                font,
                itdk,
                rows,
                labels,
                percentages,
                output,
                connection=connection,
                tcp_flags=tcp_flags,
                os_entries=entries,
            )
        finally:
            plt.close(fig)


def _draw(
    fig,
    font,
    itdk,
    rows,
    labels,
    percentages,
    output,
    *,
    connection=False,
    tcp_flags=False,
    os_entries=None,
):
    os_roles = os_entries is not None
    order = tuple(key for key, _, _ in os_entries) if os_roles else ORDER
    colors = {key: color for key, _, color in os_entries} if os_roles else COLORS
    height = fig.get_size_inches()[1]
    three_rows = connection or tcp_flags
    left = 1.01 if connection else 0.80 if tcp_flags else 0.59 if itdk else 0.77
    axes_height = 0.6325 if three_rows else 0.4015
    centers = (1.68, 0.84, 0) if three_rows else (0.84, 0)
    ax = fig.add_axes(
        [left / WIDTH, 0.43 / height, (WIDTH - left - 0.1) / WIDTH, axes_height / height]
    )
    ax.set(xlim=(0, 100), ylim=(-0.31, 1.99 if three_rows else 1.15))
    ax.set_yticks(centers, labels)
    ax.set_xticks(range(0, 101, 20))
    ax.set_xticks([n for n in range(5, 100, 5) if n % 20], minor=True)
    ax.set_xlabel(
        "OS Distribution [%]" if os_roles else "IP-ID Selection Strategy [%]", labelpad=1
    )
    ax.tick_params(axis="x", which="major", length=2.5, width=0.45, pad=1)
    ax.tick_params(axis="x", which="minor", length=1.4, width=0.35)
    ax.tick_params(axis="y", length=2.5, width=0.45, pad=1.5)
    ax.grid(axis="x", color="#BDBDBD", linestyle="--", linewidth=0.35, alpha=0.7)
    ax.set_axisbelow(True)
    title = fig.text(
        0.075 / WIDTH,
        0.5,
        "TCP Flags" if tcp_flags else "Device Type" if itdk else "Measurement Type",
        rotation=90,
        rotation_mode="anchor",
        ha="center",
        va="center",
        fontproperties=font,
    )
    if not itdk and not tcp_flags:
        upper, lower = (1.47, 1.05) if connection else (0.63, 0.21)
        rt = percentages[MODES[0]]
        begin = sum(rt.get(key, 0) for key in ORDER[: ORDER.index("UNCLASSIFIED")])
        end = begin + rt.get("UNCLASSIFIED", 0)
        if end > begin:
            ax.add_patch(
                Polygon(
                    [(begin, upper), (end, upper), (100, lower), (0, lower)],
                    closed=True,
                    facecolor="#BDBDBD",
                    edgecolor="none",
                    alpha=0.21,
                    zorder=0,
                )
            )
            for x1, x2 in [(begin, 0), (end, 100)]:
                ax.plot(
                    [x1, x2],
                    [upper, lower],
                    color="#A6A6A6",
                    linestyle="--",
                    linewidth=0.55,
                    zorder=1,
                )
    segments = []
    for y, row in zip(centers, rows):
        x = 0.0
        for key in order:
            value = percentages[row].get(key, 0)
            if value:
                ax.barh(
                    y, value, left=x, height=0.42, color=colors[key], edgecolor="none", zorder=2
                )
                segments.append((x, y, value))
            x += value
    legends = _legend(fig, font, itdk=itdk, entries=os_entries)
    for text in fig.findobj(match=Text):
        text.set_fontproperties(font)
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    margin = fig.dpi / 300
    title.set_x(
        title.get_position()[0] + (margin - title.get_window_extent(renderer).x0) / fig.bbox.width
    )
    right = max(text.get_window_extent(renderer).x1 for text in ax.get_xticklabels())
    frame = ax.get_position()
    ax.set_position(
        [
            frame.x0,
            frame.y0,
            frame.width + (fig.bbox.width - margin - right) / fig.bbox.width,
            frame.height,
        ]
    )
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    bottom = (
        ax.bbox.y0
        if itdk or three_rows
        else min(ax.bbox.y0, ax.xaxis.label.get_window_extent(renderer).y0)
    )
    title.set_y((bottom + ax.bbox.y1) / (2 * fig.bbox.height))
    if connection:
        title.set_y(title.get_position()[1] - 2 / (72 * height))
    for x, y, value in segments:
        if value < 1:
            continue
        label = f"{value:.0f}"
        # Keep the agreed >=1% threshold even for narrow one-digit segments.
        ax.text(
            x + value / 2,
            y,
            label,
            ha="center",
            va="center",
            fontproperties=font,
            color="black",
            zorder=3,
        )
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    for text in fig.findobj(match=Text):
        if not text.get_visible() or not text.get_text():
            continue
        box = text.get_window_extent(renderer)
        right = box.x1
        if text in legends:
            right = (
                text.get_transform().transform(text.get_position())[0]
                + TextPath((0, 0), text.get_text(), prop=font).get_extents().x1 * fig.dpi / 72
            )
        if min(box.x0, box.y0) < -0.5 or right > fig.bbox.x1 + 0.5 or box.y1 > fig.bbox.y1 + 0.5:
            raise ValueError(f"paper label would be clipped: {text.get_text()}")
    if max(title.get_window_extent(renderer).x1, 0) >= min(
        t.get_window_extent(renderer).x0 for t in ax.get_yticklabels()
    ):
        raise ValueError("paper y-axis label overlaps tick labels")
    rgb = np.asarray(fig.canvas.buffer_rgba())[:, :, :3]
    ink_rows = np.flatnonzero(np.any(rgb < 250, axis=(1, 2)))
    correction = 0 if itdk else 3 / 300
    bottom = max(0, (rgb.shape[0] - ink_rows[-1] - 1) / fig.dpi - 1 / 300 + correction)
    top = min(height, (rgb.shape[0] - ink_rows[0]) / fig.dpi + 2 / 300)
    fig.savefig(
        output,
        format="pdf",
        bbox_inches=Bbox.from_extents(0, bottom, WIDTH, top),
        pad_inches=0,
        metadata={
            "Title": "Device type by operating system"
            if os_roles
            else "TCP flags by strategy"
            if tcp_flags
            else "Device type by strategy"
            if itdk
            else "Measurement type by strategy",
            "Subject": f"{STYLE_VERSION}; Linux Libertine 7 pt; 3.33 inches",
            "Creator": "ipid-analysis paper-ready renderer",
        },
    )
