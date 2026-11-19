"""Approved OS-group heatmaps from the existing compact strategy aggregate."""

import math

import numpy as np
import pyarrow.parquet as pq

from ipid_analysis.plot_os_group_strategy import AGGREGATE_SCHEMA, GROUP_INFO, HEATMAP_STRATEGIES

MAX_AGGREGATE_BYTES = 4 << 20
SELECTION = (
    (
        "General-Purpose OS",
        ("ubuntu", "debian", "rhel", "centos", "fedora", "freebsd", "openbsd", "windows"),
    ),
    (
        "Network OS",
        (
            "mikrotik",
            "f5",
            "cisco",
            "sonicwall",
            "zte",
            "hpe-network",
            "check-point",
            "opnsense",
            "huawei",
            "draytek",
            "zyxel",
            "aruba",
        ),
    ),
)
Y_LABEL = "Operating-System Group (#IP Addr.)"
BASE_OMITTED = ("MULTI", "RANDOM", "NOT_ENOUGH_SAMPLES")
BASE_STRATEGIES = tuple(key for key in HEATMAP_STRATEGIES if key not in BASE_OMITTED)
# Physical measurements from the approved drafts; no PDF is read at runtime.
BASE_CELL_POINTS = (11.9145, 9.1275)
BASE_COLORBAR_POINTS = (3.6, 63.8925)
BASE_COLORBAR_GAP_POINTS = 5.04


def load_data(path, report, *, connection_base=False):
    """Validate the complete small table, then select rows without renormalizing."""
    if report.get("figure") != "operating-system-group-by-strategy":
        raise ValueError("expected the OS-group strategy report")
    if connection_base and tuple(
        report.get(k) for k in ("protocol", "connection_mode", "target")
    ) != ("tcp", "connection", "tcp.ipid.connection.rt-based.base"):
        raise ValueError("expected the connection-oriented TCP RT-base OS-group report")
    if path.stat().st_size > MAX_AGGREGATE_BYTES:
        raise ValueError("expected a compact OS-group aggregate (at most 4 MiB)")
    with pq.ParquetFile(path) as source:
        if not 0 < source.metadata.num_rows <= len(GROUP_INFO) * len(HEATMAP_STRATEGIES):
            raise ValueError("unexpected OS-group aggregate row count")
        if not source.schema_arrow.equals(AGGREGATE_SCHEMA, check_metadata=False):
            raise ValueError("unexpected OS-group aggregate schema")
        rows = source.read().to_pylist()
    groups = report.get("groups")
    if not isinstance(groups, dict) or not groups or set(groups) - set(GROUP_INFO):
        raise ValueError("unknown or missing OS groups in report")
    indices = {strategy: i for i, strategy in enumerate(HEATMAP_STRATEGIES)}
    seen, totals, counts, percentages = set(), {}, {}, {}
    for row in rows:
        group, strategy = row["OS_GROUP"], row["IPID_SELECTION_STRATEGY"]
        if group not in groups or strategy not in indices or (group, strategy) in seen:
            raise ValueError("unknown/duplicate OS-group strategy cell")
        seen.add((group, strategy))
        section, label = GROUP_INFO[group]
        count, total, share = row["COUNT"], row["OS_GROUP_TOTAL"], row["PERCENTAGE"]
        if (row["OS_SECTION"], row["OS_GROUP_LABEL"]) != (section, label):
            raise ValueError("OS-group labels/sections disagree with canonical taxonomy")
        if (
            type(count) is not int
            or type(total) is not int
            or not 0 <= count <= total
            or total <= 0
            or not isinstance(share, float)
            or not math.isfinite(share)
            or not 0 <= share <= 100
        ):
            raise ValueError("invalid OS-group counts/population/percentage")
        if group in totals and totals[group] != total:
            raise ValueError("inconsistent OS-group total")
        if not math.isclose(share, 100 * count / total, rel_tol=1e-9, abs_tol=1e-8):
            raise ValueError("OS-group count and percentage disagree")
        if (
            not isinstance(groups[group], dict)
            or type(groups[group].get("ip_count")) is not int
            or groups[group] != {"section": section, "label": label, "ip_count": total}
        ):
            raise ValueError("OS-group report and aggregate disagree")
        totals[group] = total
        counts.setdefault(group, np.zeros(10, dtype=np.int64))[indices[strategy]] = count
        percentages.setdefault(group, np.zeros(10))[indices[strategy]] = share
    if set(totals) != set(groups) or len(seen) != 10 * len(groups):
        raise ValueError("missing OS-group strategy cells")
    if any(sum(int(v) for v in counts[g]) != totals[g] for g in groups):
        raise ValueError("OS-group cells disagree with their population")
    expected = {
        "os_group_count": len(groups),
        "matched_grouped_ip_count": sum(totals.values()),
        "not_enough_samples_ip_count": sum(int(c[-1]) for c in counts.values()),
    }
    if any(type(report.get(k)) is not int or report[k] != value for k, value in expected.items()):
        raise ValueError("OS-group report totals disagree with aggregate")
    if connection_base:
        omitted = [indices[key] for key in BASE_OMITTED]
        if any(counts[g][omitted].any() or percentages[g][omitted].any() for g in groups):
            raise ValueError("BASE-only omitted strategy columns must be exactly zero")
    # Absence from the validated complete table means zero observed IPs, not
    # permission to drop/reorder the user's fixed selection.
    result = {
        "sections": [
            {
                "title": title,
                "groups": keys,
                "totals": [totals.get(g, 0) for g in keys],
                "counts": np.asarray([counts.get(g, np.zeros(10, dtype=np.int64)) for g in keys]),
                "percentages": np.asarray([percentages.get(g, np.zeros(10)) for g in keys]),
            }
            for title, keys in SELECTION
        ]
    }
    if connection_base:
        result.update(connection_base=True, strategies=BASE_STRATEGIES)
        columns = [indices[key] for key in BASE_STRATEGIES]
        for section in result["sections"]:
            for field in ("counts", "percentages"):
                section[field] = section[field][:, columns]
    return result


def draw(kind, data, output):
    return _draw(data, output)


def _draw(data, output, *, wrap_width=None):
    import matplotlib.pyplot as plt
    from matplotlib.text import Text
    from matplotlib.transforms import Bbox

    from ipid_analysis.paper_figures import PERCENTAGE_CMAP, draw_percentage_colorbar_axis
    from ipid_analysis.paper_plot_style import FONT_SIZE, WIDTH, _font, measure_heatmap_cell
    from ipid_analysis.plot_os_group_strategy import _format_ip_count
    from ipid_analysis.strategies import STRATEGY_PRETTY

    DPI, HEIGHT = 300, 4.6
    font, _ = _font()
    connection_base = data.get("connection_base", False)
    strategies = data.get("strategies", HEATMAP_STRATEGIES)
    column_count = len(strategies)
    LABELS = [STRATEGY_PRETTY[key] for key in strategies]

    def population(total):
        if connection_base and total > 100_000:
            return f"{total / 1000:.0f}k"
        return _format_ip_count(total)

    def annotation(count, share):
        if count == 0:
            return "-"
        text = f"{share:.1f}"
        # The exact endpoint fits a fixed 7-pt cell as 100, with no information
        # lost relative to its 100.0 annotation. Other cells keep one decimal.
        return "100" if connection_base and text == "100.0" else text

    cmap = PERCENTAGE_CMAP
    selected = [
        (
            section["title"],
            [
                (
                    GROUP_INFO[key],
                    shares,
                    [annotation(count, share) for count, share in zip(counts, shares)],
                )
                for key, counts, shares in zip(
                    section["groups"], section["counts"], section["percentages"]
                )
            ],
        )
        for section in data["sections"]
    ]
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
        fig = plt.figure(figsize=(WIDTH, HEIGHT), dpi=DPI)
        try:
            probe = fig.text(0.1, 0.1, "", fontproperties=font)
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            annotation_sizes = []
            for label in {"99.9"} | {
                label for _, rows in selected for _, _, labels in rows for label in labels
            }:
                probe.set_text(label)
                annotation_sizes.append(probe.get_window_extent(renderer).size)
            cell_width_px, cell_height_px = np.max(annotation_sizes, axis=0) + 10
            if connection_base:
                cell_width_px, cell_height_px = np.asarray(BASE_CELL_POINTS) * DPI / 72
            labels_by_section = [
                [
                    f"{GROUP_INFO[key][1]} ({population(total)})"
                    for key, total in zip(section["groups"], section["totals"])
                ]
                for section in data["sections"]
            ]

            def width(text):
                probe.set_text(text)
                return probe.get_window_extent(renderer).width

            if wrap_width is not None:
                for labels in labels_by_section:
                    for i, label in enumerate(labels):
                        lines = [""]
                        for word in label.split():
                            candidate = (lines[-1] + " " + word).strip()
                            if lines[-1] and width(candidate) > wrap_width:
                                lines.append(word)
                            else:
                                lines[-1] = candidate
                        labels[i] = "\n".join(lines)
                        probe.set_text(labels[i])
                        cell_height_px = max(
                            cell_height_px, probe.get_window_extent(renderer).height + 10
                        )
            max_label_width = max(width(label) for labels in labels_by_section for label in labels)
            probe.remove()
            _, reference_cell_height = measure_heatmap_cell(fig, font)
            colorbar_width, colorbar_height = 0.05, 7 * reference_cell_height / DPI
            colorbar_gap = 0.02
            if connection_base:
                colorbar_width, colorbar_height = np.asarray(BASE_COLORBAR_POINTS) / 72
                colorbar_gap = BASE_COLORBAR_GAP_POINTS / 72
            matrix_width = column_count * cell_width_px / DPI
            row_height = cell_height_px / DPI
            left, bottom, section_gap = 1.04, 0.9, 0.20
            network_height, general_height = 12 * row_height, 8 * row_height
            height_needed = bottom + network_height + section_gap + general_height + 0.24
            HEIGHT = max(HEIGHT, height_needed)
            fig.set_size_inches(WIDTH, HEIGHT)
            frames = (
                (bottom + network_height + section_gap, general_height),
                (bottom, network_height),
            )
            axes = []
            for (title, rows), row_labels, (base, height) in zip(
                selected, labels_by_section, frames
            ):
                ax = fig.add_axes(
                    [left / WIDTH, base / HEIGHT, matrix_width / WIDTH, height / HEIGHT]
                )
                values = np.asarray([row[1] for row in rows])
                ax.imshow(values, cmap=cmap, vmin=0, vmax=100, interpolation="none", aspect="auto")
                ax.set_yticks(range(len(rows)), row_labels)
                ax.set_xticks(range(column_count), LABELS if title == "Network OS" else [])
                ax.tick_params(axis="both", length=2.5, width=0.45, pad=1.5)
                if title == "Network OS":
                    plt.setp(ax.get_xticklabels(), rotation=30, ha="right", rotation_mode="anchor")
                    ax.set_xlabel("IP-ID Selection Strategy", labelpad=2)
                else:
                    ax.tick_params(axis="x", bottom=True, labelbottom=False)
                ax.set_title(title, loc="center", fontsize=FONT_SIZE, pad=3)
                for i, (_, percentages, annotations) in enumerate(rows):
                    for j, label in enumerate(annotations):
                        ax.text(
                            j,
                            i,
                            label,
                            ha="center",
                            va="center",
                            fontproperties=font,
                            color="white" if percentages[j] >= 50 else "#222222",
                        )
                axes.append(ax)
            total_height = network_height + section_gap + general_height
            colorbar_bottom = bottom + (total_height - colorbar_height) / 2
            cax = fig.add_axes(
                [
                    (left + matrix_width + colorbar_gap) / WIDTH,
                    colorbar_bottom / HEIGHT,
                    colorbar_width / WIDTH,
                    colorbar_height / HEIGHT,
                ]
            )
            draw_percentage_colorbar_axis(
                cax, label="Percentage [%]", stroke_width=0.45, tick_length=2.5, tick_pad=1.5
            )
            cax.yaxis.labelpad = 2
            ylabel = fig.text(
                0.07 / WIDTH,
                (bottom + total_height / 2) / HEIGHT,
                Y_LABEL,
                rotation=90,
                ha="center",
                va="center",
                fontproperties=font,
            )
            for text in fig.findobj(match=Text):
                text.set_fontproperties(font)
            # Fit the two heatmaps, label column and common colorbar as one unit;
            # retain the 3.33-inch canvas and exact physical font/cell sizes.
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            bounds = [a.get_tightbbox(renderer) for a in (*axes, cax)]
            min_x, max_x = min(b.x0 for b in bounds), max(b.x1 for b in bounds)
            title_width = ylabel.get_window_extent(renderer).width
            gap = 2 * DPI / 72
            required_width = max_x - min_x + title_width + gap
            # Make space for the requested inline populations without shrinking
            # the 7-pt text, changing row heights, or widening the paper column.
            if not connection_base and required_width > fig.bbox.width - 2:
                reduction = required_width - (fig.bbox.width - 2)
                cell_width_px -= reduction / 10
                minimum_width = np.max(annotation_sizes, axis=0)[0] + 1
                if cell_width_px < minimum_width:
                    allowance = max_label_width - 10 * (minimum_width - cell_width_px) - 2
                    if allowance <= 0 or (wrap_width is not None and allowance >= wrap_width - 1):
                        raise ValueError("OS labels cannot fit the paper column at 7 pt")
                    return _draw(data, output, wrap_width=allowance)
                matrix_width = 10 * cell_width_px / DPI
                for ax in axes:
                    box = ax.get_position()
                    ax.set_position([box.x0, box.y0, matrix_width / WIDTH, box.height])
                box = cax.get_position()
                cax.set_position(
                    [box.x0 - reduction / fig.bbox.width, box.y0, box.width, box.height]
                )
                fig.canvas.draw()
                renderer = fig.canvas.get_renderer()
                bounds = [a.get_tightbbox(renderer) for a in (*axes, cax)]
                min_x, max_x = min(b.x0 for b in bounds), max(b.x1 for b in bounds)
                required_width = max_x - min_x + title_width + gap
            if required_width > fig.bbox.width - 2 + 0.01:
                raise ValueError("OS labels cannot fit the fixed paper cells at 7 pt")
            offset = (fig.bbox.width - required_width) / 2 + title_width + gap - min_x
            for ax in (*axes, cax):
                box = ax.get_position()
                ax.set_position([box.x0 + offset / fig.bbox.width, box.y0, box.width, box.height])
            ylabel.set_x(
                ((fig.bbox.width - required_width) / 2 + title_width / 2) / fig.bbox.width
            )
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            for ax in axes:
                title_box = ax.title.get_window_extent(renderer)
                assert (
                    abs((title_box.x0 + title_box.x1) / 2 - (ax.bbox.x0 + ax.bbox.x1) / 2) < 0.001
                )
            for text in fig.findobj(match=Text):
                if text.get_visible() and text.get_text():
                    b = text.get_window_extent(renderer)
                    if (
                        min(b.x0, b.y0) < -0.5
                        or b.x1 > fig.bbox.width + 0.5
                        or b.y1 > fig.bbox.height + 0.5
                    ):
                        raise ValueError(f"paper label would be clipped: {text.get_text()}")
            assert sum(len(a.texts) for a in axes) == 20 * column_count
            image = np.asarray(fig.canvas.buffer_rgba())[:, :, :3]
            ys = np.flatnonzero(np.any(image < 250, axis=2).any(axis=1))
            crop_bottom = max(0, (len(image) - ys[-1] - 1) / DPI - 1 / DPI)
            crop_top = min(HEIGHT, (len(image) - ys[0]) / DPI + 2 / DPI)
            fig.savefig(
                output,
                format="pdf",
                bbox_inches=Bbox.from_extents(0, crop_bottom, WIDTH, crop_top),
                pad_inches=0,
                metadata={
                    "Title": "Operating-system groups by strategy",
                    "Subject": "3.33 inches; Linux Libertine 7 pt; original aggregate percentages",
                },
            )
        finally:
            plt.close(fig)
