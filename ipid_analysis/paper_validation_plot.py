"""Approved single-column validation layouts; render persisted aggregates only."""

from matplotlib import font_manager
from matplotlib.lines import Line2D
from matplotlib.patches import Polygon
import matplotlib.pyplot as plt
from matplotlib.text import Text
from matplotlib.textpath import TextPath
from matplotlib.ticker import LogLocator, NullFormatter
from matplotlib.transforms import Bbox
import numpy as np

from ipid_analysis.classifier_validation import FIXED_STRATEGIES as CLASSES
from ipid_analysis.paper_figures import PERCENTAGE_CMAP, draw_percentage_colorbar_axis
from ipid_analysis.paper_heatmap_style import CELL_HEIGHT_POINTS, format_percentage
from ipid_analysis.paper_increment_plot import AXES_HEIGHT as CDF_AXES_HEIGHT
from ipid_analysis.paper_increment_plot import HEIGHT as CDF_HEIGHT
from ipid_analysis.paper_plot_style import COLORS, FONT_SIZE, WIDTH, _font, measure_heatmap_cell
from ipid_analysis.plot_random_structure_score_cdf import _positive_ecdf_coordinates
from ipid_analysis.strategies import STRATEGY_PRETTY

STYLE = {
    "font.size": 7,
    "axes.labelsize": 7,
    "xtick.labelsize": 7,
    "ytick.labelsize": 7,
    "axes.titlesize": 7,
    "axes.linewidth": 0.45,
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
}
SUPERSCRIPT = str.maketrans("-0123456789", "⁻⁰¹²³⁴⁵⁶⁷⁸⁹")
OS_GROUP_GAP_PT = 0.20 * 72
CONNECTOR_FILL = "#BDBDBD"
CONNECTOR_FILL_ALPHA = 0.21


def finalize(fig, path, title, extra=None):
    FONT = _font()[0]
    for t in fig.findobj(match=Text):
        t.set_fontproperties(FONT)
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    for t in fig.findobj(match=Text):
        if not t.get_visible() or not t.get_text():
            continue
        b = t.get_window_extent(renderer)
        if (
            b.x0 < -0.75
            or b.x1 > fig.bbox.width + 0.75
            or b.y0 < -0.75
            or b.y1 > fig.bbox.height + 0.75
        ):
            raise ValueError(f"Clipped label: {t.get_text()} {b}")
    rgb = np.asarray(fig.canvas.buffer_rgba())[:, :, :3]
    ink = np.flatnonzero(np.any(rgb < 250, axis=(1, 2)))
    low = max(0, (len(rgb) - ink[-1] - 1) / fig.dpi - 1 / 300)
    high = min(fig.get_size_inches()[1], (len(rgb) - ink[0]) / fig.dpi + 2 / 300)
    # Keep the font descent inside the PDF even when the label has no
    # descending glyphs. Snap outward to the existing 300-dpi crop grid.
    if (extra or {}).get("x_label"):
        label = next(t for t in fig.texts if t.get_text() == extra["x_label"])
        face = font_manager.get_font(FONT.get_file())
        baseline = fig.transFigure.transform(label.get_position())[1] / fig.dpi
        descent = face.descender / face.units_per_EM * FONT_SIZE / 72
        limit = np.floor((baseline + descent - 1 / 300) * fig.dpi) / fig.dpi
        low = max(0, min(low, limit))
    fig.savefig(
        path,
        bbox_inches=Bbox.from_extents(0, low, WIDTH, high),
        pad_inches=0,
        metadata={
            "Title": title,
            "Subject": "Linux Libertine 7pt; 3.33 inches; persisted validation aggregates",
        },
    )
    geometry = {
        "file": path.name,
        "width_pt": 72 * WIDTH,
        "height_pt": 72 * (high - low),
        "font_pt": 7,
        **(extra or {}),
    }
    plt.close(fig)
    return geometry


def heatmap(items, name):
    FONT = _font()[0]
    rows, cols = 8, 9
    height = 2.05 if len(items) == 1 else 3.39
    with plt.rc_context(STYLE):
        fig = plt.figure(figsize=(WIDTH, height), dpi=300)
        cw, ch = measure_heatmap_cell(fig, FONT)
        # 9 rather than 7 detected classes: fit annotations at the same physical
        # font and row height, reducing horizontal padding only if necessary.
        cw_pt = min(cw * 72 / 300, 15.9)
        matrix_w = cols * cw_pt / 72
        matrix_h = rows * CELL_HEIGHT_POINTS / 72
        bottom = 0.64
        gap = 0.23
        axes = []
        for index, (title, data) in enumerate(items):
            y = bottom + (len(items) - 1 - index) * (matrix_h + gap)
            ax = fig.add_axes([0.79 / WIDTH, y / height, matrix_w / WIDTH, matrix_h / height])
            values, counts = np.array(data["percentages"]), np.array(data["counts"])
            assert values.shape == counts.shape == (8, 9)
            assert np.all(counts.sum(axis=1) == np.array(data["supports"]))
            assert np.allclose(values.sum(axis=1), 100)
            ax.imshow(
                values, cmap=PERCENTAGE_CMAP, vmin=0, vmax=100, interpolation="none", aspect="auto"
            )
            ax.set_yticks(range(rows), [STRATEGY_PRETTY[k] for k in CLASSES])
            ax.set_xticks(
                range(cols),
                [STRATEGY_PRETTY[k] for k in (*CLASSES, "UNCLASSIFIED")]
                if index == len(items) - 1
                else [],
            )
            ax.tick_params(length=2.5, width=0.45, pad=1.5)
            if index == len(items) - 1:
                plt.setp(ax.get_xticklabels(), rotation=30, ha="right", rotation_mode="anchor")
                ax.set_xlabel("Detected IP-ID Selection Strategy", labelpad=2)
            ax.set_title(title, pad=3)
            for i in range(rows):
                for j in range(cols):
                    ax.text(
                        j,
                        i,
                        format_percentage(values[i, j], int(counts[i, j])),
                        ha="center",
                        va="center",
                        fontproperties=FONT,
                        color="white" if values[i, j] >= 50 else "#222222",
                    )
            axes.append(ax)
        total_h = len(items) * matrix_h + (len(items) - 1) * gap
        cax = fig.add_axes(
            [
                (0.79 + matrix_w + 4 / 72) / WIDTH,
                (bottom + (total_h - 70 / 72) / 2) / height,
                3.6 / 72 / WIDTH,
                70 / 72 / height,
            ]
        )
        draw_percentage_colorbar_axis(
            cax, label="Percentage [%]", stroke_width=0.45, tick_length=2.5, tick_pad=1.5
        )
        cax.yaxis.labelpad = 0
        ylabel = fig.text(
            0.07 / WIDTH,
            (bottom + total_h / 2) / height,
            "Generating IP-ID Strategy",
            rotation=90,
            ha="center",
            va="center",
            fontproperties=FONT,
        )
        for t in fig.findobj(match=Text):
            t.set_fontproperties(FONT)
        # Centre the axes, row labels, common title and fixed colorbar as a unit.
        fig.canvas.draw()
        rr = fig.canvas.get_renderer()
        boxes = [a.get_tightbbox(rr) for a in (*axes, cax)]
        left, right = min(b.x0 for b in boxes), max(b.x1 for b in boxes)
        yw = ylabel.get_window_extent(rr).width
        needed = right - left + yw + 2 * fig.dpi / 72
        if needed > fig.bbox.width - 2:
            reduce = (needed - (fig.bbox.width - 2)) / fig.dpi
            matrix_w -= reduce
            for ax in axes:
                b = ax.get_position()
                ax.set_position([b.x0, b.y0, matrix_w / WIDTH, b.height])
            b = cax.get_position()
            cax.set_position([b.x0 - reduce / WIDTH, b.y0, b.width, b.height])
            fig.canvas.draw()
            rr = fig.canvas.get_renderer()
            boxes = [a.get_tightbbox(rr) for a in (*axes, cax)]
            left, right = min(b.x0 for b in boxes), max(b.x1 for b in boxes)
            needed = right - left + yw + 2 * fig.dpi / 72
        shift = ((fig.bbox.width - needed) / 2 + yw + 2 * fig.dpi / 72 - left) / fig.bbox.width
        for a in (*axes, cax):
            b = a.get_position()
            a.set_position([b.x0 + shift, b.y0, b.width, b.height])
        ylabel.set_x(((fig.bbox.width - needed) / 2 + yw / 2) / fig.bbox.width)
        assert abs(axes[0].bbox.height * 72 / fig.dpi / 8 - 10) < 1e-6
        prefix = "Fixed-interval Mass"
        return finalize(
            fig,
            name,
            prefix + " confusion matrix",
            extra={
                "cell_height_pt": 10,
                "cell_width_pt": matrix_w * 72 / 9,
                "colorbar_height_pt": 70,
                "colorbar_gap_pt": 4,
            },
        )


def cdf_legend(fig):
    """Centred two-row legend using the approved increment-plot spacing."""
    FONT = _font()[0]
    handle, gap, between = 8.0, 1.6, 7.0
    widths = [TextPath((0, 0), STRATEGY_PRETTY[k], prop=FONT).get_extents().x1 for k in CLASSES]
    columns = [handle + gap + max(widths[i : i + 2]) for i in range(0, len(CLASSES), 2)]
    left = (WIDTH * 72 - sum(columns) - between * (len(columns) - 1)) / 2
    assert left > 0
    for column, w in enumerate(columns):
        for row in range(2):
            k = CLASSES[column * 2 + row]
            y = CDF_HEIGHT * 72 - 5.5 - row * 9
            fig.add_artist(
                Line2D(
                    [
                        left / (WIDTH * 72),
                        (left + handle / 2) / (WIDTH * 72),
                        (left + handle) / (WIDTH * 72),
                    ],
                    [y / (CDF_HEIGHT * 72)] * 3,
                    transform=fig.transFigure,
                    color=COLORS[k],
                    linestyle="-",
                    linewidth=1.0,
                    marker="o",
                    markevery=[1],
                    markersize=3.0,
                    markeredgecolor="white",
                    markeredgewidth=72 / 300,
                )
            )
            fig.text(
                (left + handle + gap) / (WIDTH * 72),
                y / (CDF_HEIGHT * 72),
                STRATEGY_PRETTY[k],
                ha="left",
                va="center",
                fontproperties=FONT,
            )
        left += w + between


def regular_curve_markers(ax, line, spacing_pt=36, offset_pt=9):
    """Equally spaced identifiers along the displayed step path, in points."""
    points = ax.transData.transform(line.get_path().vertices)
    points = points[np.r_[True, np.linalg.norm(np.diff(points, axis=0), axis=1) > 1e-9]]
    if len(points) < 2:
        return np.empty((0, 2)), np.empty(0)
    distances = np.r_[0, np.cumsum(np.linalg.norm(np.diff(points, axis=0), axis=1))]
    scale = ax.figure.dpi / 72
    positions = np.arange(offset_pt * scale, distances[-1], spacing_pt * scale)
    xy = np.column_stack([np.interp(positions, distances, points[:, i]) for i in range(2)])
    # Leave complete circles inside the frame; omit clipped circles on the
    # zero baseline without moving any remaining marker off the step path.
    margin = (3 / 2 + 72 / 300 / 2) * scale
    visible = (
        (xy[:, 0] >= ax.bbox.x0 + margin)
        & (xy[:, 0] <= ax.bbox.x1 - margin)
        & (xy[:, 1] >= ax.bbox.y0 + margin)
        & (xy[:, 1] <= ax.bbox.y1 - margin)
    )
    return ax.transData.inverted().transform(xy[visible]), positions[visible] / scale


def cdf(scores, name, *, nist=False, condition="ideal"):
    FONT = _font()[0]
    height = CDF_HEIGHT
    with plt.rc_context(STYLE):
        fig = plt.figure(figsize=(WIDTH, height), dpi=300)
        left = 0.38
        bottom = 0.31
        main_left = 0.77
        main_right = WIDTH - 2 / 300
        frame_top = bottom + CDF_AXES_HEIGHT
        if nist:
            main_left = left
        ax = fig.add_axes(
            [
                main_left / WIDTH,
                bottom / height,
                (main_right - main_left) / WIDTH,
                (frame_top - bottom) / height,
            ]
        )
        # The categorical left panel retains exact zero and all sub-1e-6 mass.
        tiny = (
            None
            if nist
            else fig.add_axes(
                [left / WIDTH, bottom / height, 0.27 / WIDTH, (frame_top - bottom) / height],
                sharey=ax,
            )
        )
        lower = (
            10.0 ** np.floor(np.log10(min(float(s.min()) for s in scores.values())))
            if nist
            else 1e-6
        )
        fractions = {
            k: 100 * np.count_nonzero(scores[k] < lower) / len(scores[k]) for k in CLASSES
        }
        subminimum = [k for k in CLASSES if fractions[k] > 0]
        complete_subminimum = [k for k in subminimum if fractions[k] == 100]
        # A categorical offset changes only marker placement, never the score
        # coordinates or ECDF heights. Only exact 100% markers are separated;
        # all others stay on the shared line. 2.2pt spacing overlaps 3pt circles.
        offsets = {
            k: (i - (len(complete_subminimum) - 1) / 2) * 2.2 / (0.27 * 72)
            for i, k in enumerate(complete_subminimum)
        }
        line_widths = {
            "SINGLE": 1.0,
            "PER_DESTINATION": 1.6,
            "PER_CONNECTION": 0.8,
            "PER_BUCKET": 1.0,
        }
        main_curves = []
        for i, k in enumerate(CLASSES):
            values = scores[k]
            frac = fractions[k]
            x, y = _positive_ecdf_coordinates(values, lower)
            if len(x):
                # Extend the true ECDF to the upper endpoint and retain its
                # nonzero starting height due to the categorical left tail.
                xx = np.r_[lower, x, 1.20]
                yy = np.r_[frac, y, 100]
                (line,) = ax.step(
                    xx,
                    yy,
                    where="post",
                    color=COLORS[k],
                    lw=line_widths.get(k, 1.0),
                    ls="-",
                    solid_capstyle="butt",
                )
                main_curves.append((k, line))
            if frac and tiny is not None:
                tiny.vlines(0, 0, frac, color=COLORS[k], lw=1.0)
                tiny.plot(
                    offsets.get(k, 0),
                    frac,
                    ls="none",
                    marker="o",
                    ms=3.0,
                    color=COLORS[k],
                    markeredgewidth=72 / 300,
                    markeredgecolor="white",
                    zorder=3,
                )
        threshold = 0.0005 if nist else 4.0383019016847365e-5
        threshold_color = "#4B4B4B"
        ax.axvline(threshold, color=threshold_color, ls="--", lw=0.65, zorder=1)
        ax.text(
            threshold * 1.16,
            7,
            "Threshold",
            rotation=90,
            va="bottom",
            fontproperties=FONT,
            color=threshold_color,
            zorder=20,
        )
        ax.set_xscale("log")
        ax.set_xlim(lower, 1.20)
        exponents = tuple(range(int(np.log10(lower)), 1))
        ax.set_xticks(
            [10.0**e for e in exponents], ["10" + str(e).translate(SUPERSCRIPT) for e in exponents]
        )
        ax.xaxis.set_minor_locator(LogLocator(base=10, subs=range(2, 10)))
        ax.xaxis.set_minor_formatter(NullFormatter())
        ax.set_ylim(0, 103)
        if tiny is not None:
            tiny.set_ylim(0, 103)
            tiny.set_xlim(-0.5, 0.5)
            tiny.set_xticks([0], ["<10" + "-6".translate(SUPERSCRIPT)])
            tiny.set_ylabel("Cumulative Percentage [%]", labelpad=2)
            ax.tick_params(axis="y", left=False, labelleft=False)
        else:
            ax.set_ylabel("Cumulative Percentage [%]", labelpad=2)
        for a in (ax,) if tiny is None else (ax, tiny):
            a.set_yticks(range(0, 101, 20))
            a.set_yticks(range(10, 100, 20), minor=True)
            a.tick_params(which="major", length=2.5, width=0.45, pad=1)
            a.tick_params(which="minor", length=1.4, width=0.35)
            a.grid(which="major", color="#BDBDBD", ls="-", lw=0.35, alpha=0.7)
            a.grid(which="minor", color="#D9D9D9", ls="-", lw=0.25, alpha=0.55)
            a.set_axisbelow(True)
        if tiny is not None:
            ax.tick_params(axis="y", which="both", left=False, labelleft=False)
        if tiny is not None:
            ax.spines["left"].set_visible(False)
            tiny.spines["right"].set_visible(False)
        xlabel = fig.text(
            (left + main_right) / 2 / WIDTH,
            0.055 / height,
            "Randomness Test Score",
            ha="center",
            fontproperties=FONT,
        )
        cdf_legend(fig)
        for t in fig.findobj(match=Text):
            t.set_fontproperties(FONT)
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        yaxis = tiny if tiny is not None else ax
        shift = (2 - yaxis.yaxis.label.get_window_extent(renderer).x0) / fig.bbox.width
        if tiny is not None:
            b = tiny.get_position()
            tiny.set_position([b.x0 + shift, b.y0, b.width, b.height])
        b = ax.get_position()
        ax.set_position([b.x0 + shift, b.y0, b.width - shift, b.height])
        # Keep modest headroom beyond a score of one. Reserve room for the
        # last tick label outside the frame instead of extending the x range.
        for _ in range(4):
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            overflow = max(t.get_window_extent(renderer).x1 for t in ax.get_xticklabels()) - (
                fig.bbox.width - 2
            )
            if overflow <= 0.01:
                break
            b = ax.get_position()
            ax.set_position([b.x0, b.y0, b.width - overflow / fig.bbox.width, b.height])
        first = tiny if tiny is not None else ax
        xlabel.set_x((first.bbox.x0 + ax.bbox.x1) / (2 * fig.bbox.width))
        marker_positions = {}
        for k, line in main_curves:
            xy, positions = regular_curve_markers(ax, line)
            marker_positions[k] = positions.tolist()
            if len(xy):
                ax.plot(
                    xy[:, 0],
                    xy[:, 1],
                    ls="none",
                    marker="o",
                    ms=3,
                    markeredgewidth=72 / 300,
                    markeredgecolor="white",
                    color=COLORS[k],
                    zorder=3,
                )
        # Fixed physical dimensions keep every slash 4pt wide and 2pt high,
        # independent of the widths of the categorical and logarithmic axes.
        if tiny is not None:
            dx = 2 / (WIDTH * 72)
            dy = 1 / (height * 72)
            for a, x in ((tiny, 1), (ax, 0)):
                box = a.get_position()
                for y in (box.y0, box.y1):
                    cx = box.x0 + x * box.width
                    fig.add_artist(
                        Line2D(
                            [cx - dx, cx + dx],
                            [y - dy, y + dy],
                            transform=fig.transFigure,
                            color="#222",
                            lw=0.45,
                            clip_on=False,
                            zorder=10,
                        )
                    )
        return finalize(
            fig,
            name,
            ("Adapted NIST baseline" if nist else "Production RANDOM score") + ": " + condition,
            extra={
                "x_label": "Randomness Test Score",
                "threshold_linestyle": "--",
                "threshold_color": threshold_color,
                "curve_linestyle": "-",
                "marker": "o",
                "marker_size_pt": 3.0,
                "marker_edge_color": "white",
                "marker_edge_width_pt": 72 / 300,
                "design_reference": "n-rt-b_fi-b_increment-distributions__paper.pdf",
                "axes_height_pt": CDF_AXES_HEIGHT * 72,
                "subminimum_line_x": 0,
                "x_max": ax.get_xlim()[1],
                "curve_marker_spacing_pt": 36,
                "curve_marker_offset_pt": 9,
                "curve_marker_arclengths_pt": marker_positions,
                "break_slash_width_pt": None if tiny is None else 4,
                "break_slash_height_pt": None if tiny is None else 2,
                "broken_axis_gap_pt": None
                if tiny is None
                else (ax.get_position().x0 - tiny.get_position().x1) * WIDTH * 72,
                "subminimum_marker_spacing_pt": 2.2,
                "subminimum_markers": [
                    {"strategy": k, "percentage": fractions[k], "x": offsets.get(k, 0)}
                    for k in subminimum
                ],
            },
        )


def pipeline(data, output):
    FONT = _font()[0]
    condition = "ideal"
    rt = np.asarray(data["e2e_rt"]["counts"], dtype=int)
    # A single bottom tick-label row must map to the same columns in both
    # matrices. Base cannot emit Multi/Random; retain their exact-zero columns
    # so its Unclassified column aligns with FI Unclassified.
    rt_columns = [*CLASSES, "UNCLASSIFIED"]
    assert not rt[:, 6:8].any()
    original_support = np.asarray(data["e2e_rt"]["supports"])
    retained = np.array([data["e2e_routing"][k]["retained_rt_counts"] for k in CLASSES])
    fi = np.asarray(data["e2e"][condition]["counts"]) - retained
    forwarded = np.asarray([data["e2e_routing"][k]["forwarded_to_fi"] for k in CLASSES])
    assert (fi >= 0).all() and np.array_equal(fi.sum(axis=1), forwarded)
    selected = np.flatnonzero(forwarded > 0)
    fi = fi[selected]
    support = forwarded[selected]
    matrix_width = 1.99
    left = 0.80
    fi_base = 0.84
    stage_gap_pt = OS_GROUP_GAP_PT
    top_base = fi_base + len(selected) * 10 / 72 + stage_gap_pt / 72
    height = top_base + 80 / 72 + 0.17
    with plt.rc_context(STYLE):
        fig = plt.figure(figsize=(WIDTH, height), dpi=300)
        axes = []
        for values, totals, columns, labels, base in (
            (rt, original_support, rt_columns, [STRATEGY_PRETTY[k] for k in CLASSES], top_base),
            (
                fi,
                support,
                [*CLASSES, "UNCLASSIFIED"],
                [STRATEGY_PRETTY[CLASSES[i]] for i in selected],
                fi_base,
            ),
        ):
            v = 100 * values / totals[:, None]
            mh = len(labels) * 10 / 72
            ax = fig.add_axes([left / WIDTH, base / height, matrix_width / WIDTH, mh / height])
            ax.imshow(
                v, cmap=PERCENTAGE_CMAP, vmin=0, vmax=100, aspect="auto", interpolation="none"
            )
            ax.set_xticks(
                range(len(columns)),
                [STRATEGY_PRETTY[k] for k in columns],
                rotation=30,
                ha="right",
                rotation_mode="anchor",
            )
            ax.set_yticks(range(len(labels)), labels)
            ax.tick_params(length=2.5, width=0.45, pad=1.5)
            for i in range(len(labels)):
                for j in range(len(columns)):
                    ax.text(
                        j,
                        i,
                        format_percentage(v[i, j], int(values[i, j])),
                        ha="center",
                        va="center",
                        fontproperties=FONT,
                        color="white" if v[i, j] >= 50 else "#222222",
                    )
            axes.append(ax)
        for ax, heading in zip(axes, ("RT-based", "Fixed-Interval")):
            ax.set_title(heading, loc="center", fontsize=FONT_SIZE, pad=3, y=1)
        axes[0].tick_params(axis="x", bottom=False, labelbottom=False)
        axes[-1].set_xlabel("Detected IP-ID Selection Strategy", labelpad=2)
        total_h = top_base + 80 / 72 - fi_base
        cax = fig.add_axes(
            [
                (left + matrix_width + 4 / 72) / WIDTH,
                (fi_base + (total_h - 70 / 72) / 2) / height,
                3.6 / 72 / WIDTH,
                70 / 72 / height,
            ]
        )
        draw_percentage_colorbar_axis(
            cax, label="Percentage [%]", stroke_width=0.45, tick_length=2.5, tick_pad=1.5
        )
        cax.yaxis.labelpad = 0
        ylab = fig.text(
            0.07 / WIDTH,
            (fi_base + total_h / 2) / height,
            "Generating IP-ID Strategy",
            rotation=90,
            ha="center",
            va="center",
            fontproperties=FONT,
        )
        for t in fig.findobj(match=Text):
            t.set_fontproperties(FONT)
        fig.canvas.draw()
        r = fig.canvas.get_renderer()
        boxes = [a.get_tightbbox(r) for a in (*axes, cax)]
        xmin, xmax = min(b.x0 for b in boxes), max(b.x1 for b in boxes)
        yw = ylab.get_window_extent(r).width
        required = xmax - xmin + yw + 2 * fig.dpi / 72
        shift = ((fig.bbox.width - required) / 2 + yw + 2 * fig.dpi / 72 - xmin) / fig.bbox.width
        for a in (*axes, cax):
            b = a.get_position()
            a.set_position([b.x0 + shift, b.y0, b.width, b.height])
        ylab.set_x(((fig.bbox.width - required) / 2 + yw / 2) / fig.bbox.width)
        b = axes[0].get_position()
        bf = axes[1].get_position()
        # Keep the agreed physical matrix gap. Centre the FI heading within
        # it, masking the connector behind the text to retain legibility.
        fi_heading = axes[1].title
        fi_heading.set_bbox({"facecolor": "white", "edgecolor": "none", "pad": 0.25})
        fig.canvas.draw()
        title_box = fi_heading.get_window_extent(fig.canvas.get_renderer())
        gap_center = (axes[0].bbox.y0 + axes[1].bbox.y1) / 2
        fi_heading.set_y(
            1 + (gap_center - (title_box.y0 + title_box.y1) / 2) / axes[1].bbox.height
        )
        fig.canvas.draw()
        title_box = fi_heading.get_window_extent(fig.canvas.get_renderer())
        assert axes[1].bbox.y1 < title_box.y0 < title_box.y1 < axes[0].bbox.y0
        # Expand the lower edge of the RT-Unclassified column to the entire
        # upper edge of FI, using the approved measurement-type connector.
        # Draw behind the axes so the black matrix frames remain intact.
        fig.add_artist(
            Polygon(
                [(b.x0 + b.width * 8 / 9, b.y0), (b.x1, b.y0), (bf.x1, bf.y1), (bf.x0, bf.y1)],
                closed=True,
                transform=fig.transFigure,
                facecolor=CONNECTOR_FILL,
                edgecolor="none",
                alpha=CONNECTOR_FILL_ALPHA,
                zorder=-2,
            )
        )
        for upper_x, lower_x in ((b.x0 + b.width * 8 / 9, bf.x0), (b.x1, bf.x1)):
            fig.add_artist(
                Line2D(
                    [upper_x, lower_x],
                    [b.y0, bf.y1],
                    transform=fig.transFigure,
                    color="#A6A6A6",
                    linestyle="--",
                    linewidth=0.55,
                    zorder=-1,
                )
            )
        # Accuracy remains available in the metrics JSON, outside the figure.
        return finalize(
            fig,
            output,
            "Two-stage RT-to-FI routing: " + condition,
            extra={
                "cell_height_pt": 10,
                "colorbar_height_pt": 70,
                "colorbar_gap_pt": 4,
                "rt_rows": 8,
                "fi_rows": len(selected),
                "rt_observations": 16,
                "fi_new_observations": 100,
                "stage_gap_pt": stage_gap_pt,
                "connector_color": "#A6A6A6",
                "connector_width_pt": 0.55,
                "connector_fill": CONNECTOR_FILL,
                "connector_fill_alpha": CONNECTOR_FILL_ALPHA,
                "stage_headings": ["RT-based", "Fixed-Interval"],
            },
        )


def _draw(figure, payload, output):
    """Return display metadata; numerical values and cached inputs stay unchanged."""
    if figure.startswith("mass-4x25-classifier-confusion-"):
        return heatmap([(panel["title"], panel["metrics"]) for panel in payload["panels"]], output)
    if figure == "e2e-classifier-confusion-ideal":
        return pipeline(payload, output)
    return cdf(
        payload["scores"], output, nist=payload["method"] == "nist", condition=payload["condition"]
    )


def draw(figure, payload, output):
    """Close private Matplotlib figures even when rendering fails."""
    before = set(plt.get_fignums())
    try:
        return _draw(figure, payload, output)
    finally:
        for number in set(plt.get_fignums()) - before:
            plt.close(number)
