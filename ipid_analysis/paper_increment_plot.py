"""Approved compact ECDF design, reading only the persisted increment histogram."""

import math
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

MAX_HISTOGRAM_ROWS = 8 * 65535
MAX_HISTOGRAM_BYTES = 128 << 20
HEIGHT, AXES_HEIGHT = 1.44, 0.80
XTICK_SUPERSCRIPTS = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹")


def aggregate_path(
    source,
    report,
    *,
    processed_root=None,
    phase=("comparison", "rt-based-base_fixed-interval-base"),
    mode="no-connection",
):
    """Use the report's exact aggregate; never fall back to per-IP increments."""
    recorded = report.get("aggregate")
    if not isinstance(recorded, str) or not recorded:
        raise ValueError("increment report is missing its aggregate histogram path")
    path = Path(recorded)
    if not path.is_absolute() or path.name != source.with_suffix(".pq").name:
        raise ValueError("expected the absolute path of the matching increment histogram")
    if processed_root is not None:
        parts = path.parts
        if parts.count("continental") != 1:
            raise ValueError("--processed-root requires a continental histogram path")
        relative = parts[parts.index("continental") :]
        suffix = (mode, *phase)
        if (
            len(relative) != 6 + len(phase)
            or ".." in relative
            or relative[-len(suffix) - 1 : -1] != suffix
        ):
            raise ValueError("unexpected continental histogram layout")
        path = Path(processed_root).joinpath(*relative)
    return path


def load_histogram(path, report):
    """Validate a bounded columnar aggregate, preserving its stored ECDF values."""
    from ipid_analysis.paper_figures import INCREMENT_SCHEMA, INCREMENT_STRATEGIES, MODES

    if path.stat().st_size > MAX_HISTOGRAM_BYTES:
        raise ValueError("expected a small increment histogram (at most 128 MiB)")
    summaries = report.get("series")
    if report.get("percentile") != 0.999 or not isinstance(summaries, dict):
        raise ValueError("expected the existing p99.9 increment histogram report")
    allowed = {f"{mode}:{strategy}" for mode in MODES for strategy in INCREMENT_STRATEGIES}
    if set(summaries) - allowed:
        raise ValueError("unknown increment histogram series")
    with pq.ParquetFile(path) as source:
        if source.metadata.num_rows > MAX_HISTOGRAM_ROWS:
            raise ValueError("expected a small one-row-per-increment histogram")
        for field in INCREMENT_SCHEMA:
            actual = source.schema_arrow.field(field.name)
            if (pa.types.is_integer(field.type) and not pa.types.is_integer(actual.type)) or (
                pa.types.is_floating(field.type) and not pa.types.is_floating(actual.type)
            ):
                raise ValueError(f"unexpected histogram type for {field.name}")
        table = source.read(columns=INCREMENT_SCHEMA.names)
    if any(column.null_count for column in table.columns):
        raise ValueError("null values in increment histogram")
    if not pc.all(pc.is_in(table["MODE"], value_set=pa.array(MODES))).as_py() and len(table):
        raise ValueError("unknown probing mode in increment histogram")
    if not pc.all(
        pc.is_in(table["IPID_SELECTION_STRATEGY"], value_set=pa.array(INCREMENT_STRATEGIES))
    ).as_py() and len(table):
        raise ValueError("unknown strategy in increment histogram")
    result, found = [], set()
    for strategy in INCREMENT_STRATEGIES:
        for mode in MODES:
            group = table.filter(
                pc.and_(
                    pc.equal(table["MODE"], mode),
                    pc.equal(table["IPID_SELECTION_STRATEGY"], strategy),
                )
            )
            if not len(group):
                continue
            key = f"{mode}:{strategy}"
            found.add(key)
            summary = summaries.get(key, {})
            values = [
                summary.get(name)
                for name in ("total_count", "retained_count", "clipped_count", "cutoff_increment")
            ]
            if any(type(value) is not int or value < 0 for value in values):
                raise ValueError(f"invalid increment summary for {key}")
            total, retained, clipped, cutoff = values
            if (
                not 0 < retained <= total
                or total - retained != clipped
                or not 1 <= cutoff <= 65535
            ):
                raise ValueError(f"invalid increment population for {key}")
            group = group.sort_by("INCREMENT")
            arrays = {
                name: group[name].to_numpy(zero_copy_only=False)
                for name in INCREMENT_SCHEMA.names[2:]
            }
            increments, counts = arrays["INCREMENT"], arrays["COUNT"]
            cumulative, percentages = arrays["CUMULATIVE_COUNT"], arrays["CUMULATIVE_PERCENTAGE"]
            if (
                np.any(increments < 1)
                or np.any(increments > 65535)
                or np.any(np.diff(increments) <= 0)
                or np.any(counts <= 0)
                or not np.array_equal(np.cumsum(counts, dtype=np.int64), cumulative)
                or np.any(np.diff(cumulative) <= 0)
                or cumulative[-1] != retained
                or increments[-1] != cutoff
                or not np.all(arrays["TOTAL_COUNT"] == total)
                or not np.all(arrays["RETAINED_COUNT"] == retained)
                or not np.all(arrays["CLIPPED_COUNT"] == clipped)
                or not np.all(np.isfinite(percentages))
                or not np.allclose(
                    percentages, 100.0 * cumulative / retained, rtol=1e-10, atol=1e-8
                )
            ):
                raise ValueError(f"inconsistent increment histogram for {key}")
            # Same zero baseline as paper_figures._ecdf_step_coordinates, using
            # columnar arrays instead of allocating one Python dictionary per bin.
            result.append(
                {
                    "strategy": strategy,
                    "mode": mode,
                    "increments": np.r_[increments[0], increments],
                    "percentages": np.r_[0.0, percentages],
                }
            )
    if found != set(summaries):
        raise ValueError("increment histogram series disagree with the report")
    return result


def _legend(fig, font, strategies, colors, pretty, modes, styles, width):
    from matplotlib.lines import Line2D
    from matplotlib.patches import Rectangle
    from matplotlib.textpath import TextPath

    entries = [(pretty[key], colors[key], None) for key in strategies]
    entries += [(mode, "#777777", styles[mode]) for mode in modes]
    handles, gap, between = [4, 4, 4, 4, 10, 10], 1.6, 7.0
    widths = [TextPath((0, 0), label, prop=font).get_extents().x1 for label, _, _ in entries]
    columns = [
        max(handles[i] + gap + widths[i], handles[i + 1] + gap + widths[i + 1]) for i in (0, 2, 4)
    ]
    x = (width * 72 - sum(columns) - 2 * between) / 2
    if x <= 0:
        raise ValueError("increment legend does not fit the paper page")
    texts = []
    for column in range(3):
        for row in range(2):
            index = column * 2 + row
            label, color, style = entries[index]
            y = HEIGHT * 72 - 5.5 - 9 * row
            if style:
                fig.add_artist(
                    Line2D(
                        [x / (width * 72), (x + handles[index]) / (width * 72)],
                        [y / (HEIGHT * 72)] * 2,
                        transform=fig.transFigure,
                        color=color,
                        linestyle=style,
                        linewidth=1.0,
                    )
                )
            else:
                fig.add_artist(
                    Rectangle(
                        (x / (width * 72), (y - 2) / (HEIGHT * 72)),
                        4 / (width * 72),
                        4 / (HEIGHT * 72),
                        transform=fig.transFigure,
                        facecolor=color,
                        edgecolor="none",
                    )
                )
            texts.append(
                fig.text(
                    (x + handles[index] + gap) / (width * 72),
                    y / (HEIGHT * 72),
                    label,
                    ha="left",
                    va="center",
                    fontproperties=font,
                )
            )
        x += columns[column] + between
    return texts


def draw(kind, series, output):
    import matplotlib.pyplot as plt
    from matplotlib.text import Text
    from matplotlib.textpath import TextPath
    from matplotlib.ticker import FixedLocator, LogLocator, MultipleLocator, NullFormatter
    from matplotlib.transforms import Bbox

    from ipid_analysis.paper_figures import INCREMENT_STRATEGIES, MODE_LINESTYLES
    from ipid_analysis.paper_plot_style import COLORS, FONT_SIZE, MODES, WIDTH, _font
    from ipid_analysis.strategies import STRATEGY_PRETTY

    font, _ = _font()
    linewidths = dict(zip(INCREMENT_STRATEGIES, (1.0, 1.6, 0.8, 1.0)))
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
        fig = plt.figure(figsize=(WIDTH, HEIGHT), dpi=300)
        try:
            ax = fig.add_axes(
                [
                    0.38 / WIDTH,
                    0.31 / HEIGHT,
                    (WIDTH - 0.38 - 2 / 300) / WIDTH,
                    AXES_HEIGHT / HEIGHT,
                ]
            )
            ax.set_xscale("log")
            ax.set_ylim(0, 103)
            ax.set_xlabel("IP-ID Increment", labelpad=1)
            ax.set_ylabel("Cumulative Percentage [%]", labelpad=2)
            for item in series:
                ax.step(
                    item["increments"],
                    item["percentages"],
                    where="post",
                    color=COLORS[item["strategy"]],
                    linestyle=MODE_LINESTYLES[item["mode"]],
                    linewidth=linewidths[item["strategy"]],
                    solid_capstyle="butt",
                )
            right = max(1.05, ax.get_xlim()[1]) if series else 10**1.05
            ax.set_xlim(0.85, right)
            powers = range(math.floor(math.log10(right)) + 1)
            ax.xaxis.set_major_locator(FixedLocator([10**power for power in powers]))
            ax.set_xticklabels(
                ["10" + str(power).translate(XTICK_SUPERSCRIPTS) for power in powers]
            )
            ax.xaxis.set_minor_locator(LogLocator(base=10, subs=range(2, 10)))
            ax.xaxis.set_minor_formatter(NullFormatter())
            ax.yaxis.set_major_locator(MultipleLocator(20))
            ax.yaxis.set_minor_locator(MultipleLocator(10))
            ax.tick_params(which="major", length=2.5, width=0.45, pad=1)
            ax.tick_params(which="minor", length=1.4, width=0.35)
            ax.grid(which="major", color="#BDBDBD", linestyle="--", linewidth=0.35, alpha=0.7)
            ax.grid(which="minor", color="#D9D9D9", linestyle=":", linewidth=0.25, alpha=0.55)
            ax.set_axisbelow(True)
            legends = _legend(
                fig,
                font,
                INCREMENT_STRATEGIES,
                COLORS,
                STRATEGY_PRETTY,
                MODES,
                MODE_LINESTYLES,
                WIDTH,
            )
            for text in fig.findobj(match=Text):
                text.set_fontproperties(font)
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            shift = (2 - ax.yaxis.label.get_window_extent(renderer).x0) / fig.bbox.width
            frame = ax.get_position()
            ax.set_position([frame.x0 + shift, frame.y0, frame.width - shift, frame.height])
            # Near a decade boundary (e.g. p99.9=65535), the original 5% log
            # margin fits at 7.16 inches but can clip a tick in a 3.33-inch PDF.
            # Expand only the right limit until its visible labels fit.
            for _ in range(4):
                fig.canvas.draw()
                renderer = fig.canvas.get_renderer()
                if max(text.get_window_extent(renderer).x1 for text in ax.get_xticklabels()) <= (
                    fig.bbox.width - 2
                ):
                    break
                ax.set_xlim(0.85, ax.get_xlim()[1] * 1.15)
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            for text in fig.findobj(match=Text):
                if text.get_visible() and text.get_text():
                    box = text.get_window_extent(renderer)
                    edge = (
                        text.get_transform().transform(text.get_position())[0]
                        + (
                            TextPath((0, 0), text.get_text(), prop=font).get_extents().x1
                            * fig.dpi
                            / 72
                        )
                        if text in legends
                        else box.x1
                    )
                    if box.x0 < -0.5 or edge > fig.bbox.width + 0.5:
                        raise ValueError(f"increment label would be clipped: {text.get_text()}")
            rgb = np.asarray(fig.canvas.buffer_rgba())[:, :, :3]
            rows = np.flatnonzero(np.any(rgb < 250, axis=(1, 2)))
            bottom = max(0, (rgb.shape[0] - rows[-1] - 1) / fig.dpi - 2 / 300)
            top = min(HEIGHT, (rgb.shape[0] - rows[0]) / fig.dpi + 2 / 300)
            fig.savefig(
                output,
                format="pdf",
                bbox_inches=Bbox.from_extents(0, bottom, WIDTH, top),
                pad_inches=0,
                metadata={
                    "Title": "IP-ID increment distributions",
                    "Subject": "Linux Libertine 7 pt; 3.33 inches",
                },
            )
        finally:
            plt.close(fig)
