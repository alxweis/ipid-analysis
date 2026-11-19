"""Approved compact strategy heatmap from its existing 49-cell aggregate."""

import math

import numpy as np
import pyarrow.parquet as pq

MAX_AGGREGATE_BYTES = 1 << 20
HEIGHT = 2.50
X_LABEL = "Detected IP-ID Selection Strategy using Fixed-Interval probing"
Y_LABEL = "Detected IP-ID Selection\nStrategy using RT-based\nprobing"


def load_matrix(path, report):
    """Validate original counts/percentages without joining or renormalizing."""
    from ipid_analysis.paper_figures import INTERSECTION_SCHEMA, INTERSECTION_STRATEGIES

    strategies = INTERSECTION_STRATEGIES
    if report.get("figure") != "strategy-intersection" or report.get("strategies") != list(
        strategies
    ):
        raise ValueError("expected the original seven-strategy intersection report")
    if path.stat().st_size > MAX_AGGREGATE_BYTES:
        raise ValueError("expected a compact 49-cell strategy aggregate (at most 1 MiB)")
    with pq.ParquetFile(path) as source:
        if source.metadata.num_rows != 49 or not source.schema_arrow.equals(
            INTERSECTION_SCHEMA, check_metadata=False
        ):
            raise ValueError("expected the original 49-cell strategy intersection schema")
        rows = source.read().to_pylist()
    indices = {strategy: index for index, strategy in enumerate(strategies)}
    percentages = np.zeros((7, 7), dtype=float)
    counts = np.zeros((7, 7), dtype=np.int64)
    totals, seen = {}, set()
    for row in rows:
        rt, fixed = row["RT_BASED_STRATEGY"], row["FIXED_INTERVAL_STRATEGY"]
        if rt not in indices or fixed not in indices or (rt, fixed) in seen:
            raise ValueError("unknown/duplicate strategy intersection cell")
        seen.add((rt, fixed))
        count, total, share = row["COUNT"], row["RT_BASED_TOTAL"], row["PERCENTAGE"]
        if (
            type(count) is not int
            or type(total) is not int
            or not 0 <= count <= total
            or not isinstance(share, float)
            or not math.isfinite(share)
            or not 0 <= share <= 100
        ):
            raise ValueError("invalid intersection count/percentage")
        if rt in totals and totals[rt] != total:
            raise ValueError("inconsistent RT-based population in intersection row")
        totals[rt] = total
        if not math.isclose(
            share, 100 * count / total if total else 0, rel_tol=1e-9, abs_tol=1e-8
        ):
            raise ValueError("intersection count and percentage disagree")
        i, j = indices[rt], indices[fixed]
        percentages[i, j], counts[i, j] = share, count
    for strategy, i in indices.items():
        if sum(int(v) for v in counts[i]) != totals[strategy]:
            raise ValueError("intersection counts disagree with the RT-based row total")
    for key in ("included_ip_count", "intersection_ip_count", "excluded_strategy_ip_count"):
        if type(report.get(key)) is not int or report[key] < 0:
            raise ValueError(f"invalid intersection report population: {key}")
    if (
        sum(totals.values()) != report["included_ip_count"]
        or report["included_ip_count"] + report["excluded_strategy_ip_count"]
        != report["intersection_ip_count"]
    ):
        raise ValueError("intersection aggregate and report populations disagree")
    return {"percentages": percentages, "counts": counts}


def draw(kind, data, output):
    import matplotlib.pyplot as plt
    from matplotlib.text import Text
    from matplotlib.transforms import Bbox

    from ipid_analysis.paper_figures import (
        INTERSECTION_STRATEGIES,
        PERCENTAGE_CMAP,
        draw_percentage_colorbar_axis,
    )
    from ipid_analysis.paper_plot_style import FONT_SIZE, WIDTH, _font
    from ipid_analysis.strategies import STRATEGY_PRETTY

    font, _ = _font()
    labels = [STRATEGY_PRETTY[key] for key in INTERSECTION_STRATEGIES]
    values, counts = data["percentages"], data["counts"]
    with plt.rc_context(
        {
            "font.family": font.get_name(),
            "font.size": FONT_SIZE,
            "axes.labelsize": FONT_SIZE,
            "xtick.labelsize": FONT_SIZE,
            "ytick.labelsize": FONT_SIZE,
            "axes.linewidth": 0.45,
            "pdf.fonttype": 42,
        }
    ):
        fig = plt.figure(figsize=(WIDTH, HEIGHT), dpi=300)
        try:
            bottom = 0.88
            ax = fig.add_axes([0.83 / WIDTH, bottom / HEIGHT, 1.96 / WIDTH, 1.50 / HEIGHT])
            ax.imshow(
                values, cmap=PERCENTAGE_CMAP, vmin=0, vmax=100, aspect="auto", interpolation="none"
            )
            ax.set_xticks(range(7), labels, rotation=30, ha="right", rotation_mode="anchor")
            ax.set_yticks(range(7), labels)
            ax.set_xlabel(X_LABEL, labelpad=2)
            ax.set_ylabel(Y_LABEL, labelpad=2)
            ax.tick_params(axis="both", which="major", length=2.5, width=0.45, pad=1.5)
            for i in range(7):
                for j in range(7):
                    text = "-" if counts[i, j] == 0 else f"{values[i, j]:.1f}"
                    ax.text(
                        j,
                        i,
                        text,
                        ha="center",
                        va="center",
                        fontproperties=font,
                        color="white" if values[i, j] >= 50 else "#222222",
                    )
            cax = fig.add_axes([2.89 / WIDTH, bottom / HEIGHT, 0.05 / WIDTH, 1.50 / HEIGHT])
            draw_percentage_colorbar_axis(
                cax, label="Percentage [%]", stroke_width=0.45, tick_length=2.5, tick_pad=1.5
            )
            cax.yaxis.labelpad = 2
            for text in fig.findobj(match=Text):
                text.set_fontproperties(font)
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            shift = (1 - ax.yaxis.label.get_window_extent(renderer).x0) / fig.bbox.width
            box = ax.get_position()
            ax.set_position([box.x0 + shift, box.y0, box.width - shift, box.height])
            # Always measure 100.0, even when no input cell happens to contain it.
            probe = fig.text(0.5, 0.5, "100.0", ha="center", va="center", fontproperties=font)
            fig.canvas.draw()
            box = probe.get_window_extent(fig.canvas.get_renderer())
            cell_width, cell_height = box.width + 10, box.height + 10
            probe.remove()
            box = ax.get_position()
            ax.set_position(
                [
                    box.x0,
                    box.y0,
                    7 * cell_width / fig.bbox.width,
                    7 * cell_height / fig.bbox.height,
                ]
            )
            box = cax.get_position()
            cax.set_position(
                [
                    ax.get_position().x1 + 0.10 / WIDTH,
                    box.y0,
                    box.width,
                    7 * cell_height / fig.bbox.height,
                ]
            )
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            right = max(
                t.get_window_extent(renderer).x1 for t in [*cax.get_yticklabels(), cax.yaxis.label]
            )
            left = ax.yaxis.label.get_window_extent(renderer).x0
            shift = (fig.bbox.width - right - left) / (2 * fig.bbox.width)
            for axis in (ax, cax):
                box = axis.get_position()
                axis.set_position([box.x0 + shift, box.y0, box.width, box.height])
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            box = ax.xaxis.label.get_window_extent(renderer)
            adjust = max(0, 1 - box.x0) - max(0, box.x1 - (fig.bbox.width - 1))
            ax.xaxis.label.set_x(ax.xaxis.label.get_position()[0] + adjust / ax.bbox.width)
            gap = min(
                4.0, max(0.0, (ax.yaxis.label.get_window_extent(renderer).x0 - 1) * 72 / fig.dpi)
            )
            ax.yaxis.labelpad += gap
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            for text in fig.findobj(match=Text):
                if not text.get_visible() or not text.get_text():
                    continue
                box = text.get_window_extent(renderer)
                if (
                    min(box.x0, box.y0) < -0.5
                    or box.x1 > fig.bbox.width + 0.5
                    or box.y1 > fig.bbox.height + 0.5
                ):
                    raise ValueError(f"paper label would be clipped: {text.get_text()}")
            rgb = np.asarray(fig.canvas.buffer_rgba())[:, :, :3]
            ink = np.flatnonzero(np.any(rgb < 250, axis=(1, 2)))
            low = max(0, (len(rgb) - ink[-1] - 1) / fig.dpi - 1 / 300)
            high = min(HEIGHT, (len(rgb) - ink[0]) / fig.dpi + 2 / 300)
            fig.savefig(
                output,
                format="pdf",
                bbox_inches=Bbox.from_extents(0, low, WIDTH, high),
                pad_inches=0,
                metadata={
                    "Title": "Strategy intersection",
                    "Subject": "Linux Libertine 7 pt; 3.33 inches; original row-normalized aggregate percentages",
                },
            )
        finally:
            plt.close(fig)
