"""Approved p99.9 split violins; explicit preparation, compact read-only rendering."""

from decimal import ROUND_HALF_UP, Decimal
from functools import lru_cache
import hashlib
import inspect
import json
import os
from pathlib import Path
import tempfile

import numpy as np
import pyarrow.parquet as pq

KIND = "probing-intervals-by-continent"
QUANTILE_POINTS, UPPER_QUANTILE = 256, 0.999
HEIGHT, AXES_HEIGHT = 1.74, 0.88
MAX_AGGREGATE_BYTES = 1 << 20


@lru_cache(maxsize=1)
def _anycast_calculation_digest():
    from ipid_analysis.anycast_probing import filtered_continent_map
    from ipid_analysis.paper_figures import aggregate_probing_intervals

    return hashlib.sha256(
        (
            inspect.getsource(aggregate_probing_intervals)
            + inspect.getsource(filtered_continent_map)
        ).encode()
    ).hexdigest()


def _remap(path, processed_root, mode):
    if processed_root is None:
        return path
    parts = path.parts
    if parts.count("continental") != 1:
        raise ValueError("--processed-root requires a continental probing-interval path")
    relative = parts[parts.index("continental") :]
    if len(relative) not in (7, 8) or ".." in relative or relative[4] != mode:
        raise ValueError("unexpected continental probing-interval layout")
    return Path(processed_root).joinpath(*relative)


def _context(source, report):
    """Keep connection quantiles separate from the stateless comparison."""
    if source.name in (
        f"c-rt-b_fi-b_{KIND}.json",
        f"c-rt-b_fi-b_{KIND}__full.json",
    ):
        target = "tcp.ipid.connection.rt-based.base+tcp.ipid.connection.fixed-interval.base"
        if tuple(
            report.get(key) for key in ("figure", "protocol", "connection_mode", "target")
        ) != (
            KIND,
            "tcp",
            "connection",
            target,
        ):
            raise ValueError("expected a connection-oriented TCP RT-base/FI-base interval report")
        return "c", "connection"
    return "n", "no-connection"


def paths(source, report, processed_root=None, *, anycast=None):
    """Resolve separate paper aggregates without changing the original report."""
    from ipid_analysis.paper_increment_plot import aggregate_path

    prefix, mode = _context(source, report)
    anchor = source
    if source.stem.endswith("__full"):
        if anycast is None or processed_root is not None:
            raise ValueError(
                "full follow-up intervals require explicit anycast selection and original stored paths"
            )
        anchor = source.with_name(source.stem.removesuffix("__full") + source.suffix)
    original = aggregate_path(anchor, report, processed_root=processed_root, mode=mode)
    if prefix == "c" and original.parts[-4:-1] != (
        mode,
        "comparison",
        "rt-based-base_fixed-interval-base",
    ):
        raise ValueError("expected the connection RT-base/FI-base comparison aggregate")
    aggregate = original.with_name(
        original.stem + ("__paper__anycast" if anycast else "__paper") + ".pq"
    )
    if anycast is not None:
        from ipid_analysis.artifacts import file_digest

        identity = file_digest(Path(anycast))[:16] + "-" + _anycast_calculation_digest()[:12]
        aggregate = aggregate.parent / "anycast" / identity / aggregate.name
    return aggregate, aggregate.with_suffix(".json")


def prepare(job, *, anycast=None):
    """One-off opt-in aggregation from stored intervals; never replace an artifact."""
    from ipid_analysis.artifacts import (
        assert_complete,
        cache_current,
        completion_path,
        file_digest,
        fingerprint,
        publish_new_file,
    )
    from ipid_analysis.paper_figures import aggregate_probing_intervals
    from ipid_analysis.runtime import exclusive_lock, scratch_root, validate_storage

    if os.path.lexists(job.destination) and anycast is None:
        return "skipped-existing"
    assert_complete(job.source)
    if job.source.stat().st_size > 4 << 20:
        raise ValueError("expected a small probing-interval report")
    report = json.loads(job.source.read_text())
    if report.get("figure") != KIND:
        raise ValueError("expected the original probing-interval report")
    prefix, mode = _context(job.source, report)
    aggregate, metadata = paths(job.source, report, job.processed_root, anycast=anycast)
    census, excluded = None, None
    if anycast is not None:
        from ipid_analysis.anycast_census import load_dataset

        census, excluded = load_dataset(anycast)
    source_paths = {}
    for key in ("rt_based", "fixed_interval", "continent_map"):
        recorded = Path(report["sources"][key])
        if not recorded.is_absolute():
            raise ValueError("expected absolute original interval/map paths")
        source_paths[key] = _remap(recorded, job.processed_root, mode)
    anchor = job.source
    if anchor.stem.endswith("__full"):
        anchor = anchor.with_name(anchor.stem.removesuffix("__full") + anchor.suffix)
    original, _ = paths(anchor, report, job.processed_root)
    phase = original.parents[2]
    expected = {
        "rt_based": phase / f"rt-based-base/{prefix}-rt-b_probing-intervals.pq",
        "fixed_interval": phase / f"fixed-interval-base/{prefix}-fi-b_probing-intervals.pq",
        "continent_map": original.with_name(f"{prefix}-rt-b_fi-b_ip-continents.pq"),
    }
    if source_paths != expected:
        raise ValueError("probing-interval sources do not belong to the original comparison")
    inputs = [job.source, *source_paths.values()]
    if anycast is not None:
        inputs.extend((Path(anycast), excluded))
    parameters = {
        "operation": "paper-probing-quantiles-v1",
        "quantile_points": QUANTILE_POINTS,
        "upper_quantile": UPPER_QUANTILE,
        "quartiles": [0.25, 0.5, 0.75],
    }
    if census is not None:
        parameters.update(
            operation="paper-anycast-probing-quantiles-v1",
            census_sha256=file_digest(Path(anycast)),
            prefixes_sha256=file_digest(excluded),
            calculation_sha256=_anycast_calculation_digest(),
        )
    validate_storage()
    with exclusive_lock("paper-probing-prepare:" + str(aggregate.resolve())):
        before = [fingerprint(path) for path in inputs]
        if any(
            os.path.lexists(p) or os.path.lexists(completion_path(p))
            for p in (aggregate, metadata)
        ):
            if (
                aggregate.is_file()
                and metadata.is_file()
                and cache_current(aggregate, inputs, parameters)
                and cache_current(metadata, [aggregate], parameters)
            ):
                load_series(aggregate, metadata, job.source, anycast=anycast)
                return "skipped-current"
            raise FileExistsError("existing/incomplete paper quantile aggregate left untouched")
        with tempfile.TemporaryDirectory(prefix="paper-probing-", dir=scratch_root()) as directory:
            temporary = Path(directory) / "quantiles.pq"
            stats = aggregate_probing_intervals(
                source_paths["rt_based"],
                source_paths["fixed_interval"],
                source_paths["continent_map"],
                temporary,
                quantile_points=QUANTILE_POINTS,
                upper_quantile=UPPER_QUANTILE,
                **({"exclude_prefixes": excluded} if excluded is not None else {}),
            )
            meta = {
                "figure": KIND,
                **stats,
                "source_report_sha256": file_digest(job.source),
                "quartile_probabilities": [0.25, 0.5, 0.75],
                "quartiles_population": "all finite nonnegative per-IP interval medians in each continent/mode; not the KDE",
                "methodology": report.get("methodology", {})
                | {"display_limit": "99.9th percentile separately per continent and probing mode"},
            }
            if census is not None:
                meta["anycast_census"] = {
                    "manifest_sha256": file_digest(Path(anycast)),
                    "prefixes_sha256": file_digest(excluded),
                    "filter": census["filter"],
                    "revision": census["revision"],
                    "dates": census["dates"],
                    "missing_dates": census["missing_dates"],
                    "complete_month": census["complete_month"],
                    "prefix_count": census["prefix_count"],
                    "calculation_sha256": _anycast_calculation_digest(),
                    "action": "exclude selected-period union of detected IPv4 /24s",
                    "period_start": census.get("period_start", census["dates"][0]),
                    "period_end": census.get("period_end", census["dates"][-1]),
                    "complete_period": census.get("complete_period", census["complete_month"]),
                    "expected_snapshots": census.get("expected_snapshots"),
                    "confidence": "high-confidence",
                    "snapshots": census["snapshots"],
                }
            temporary_meta = Path(directory) / "quantiles.json"
            temporary_meta.write_text(json.dumps(meta, indent=2) + "\n")
            load_series(temporary, temporary_meta, job.source, anycast=anycast)
            if [fingerprint(path) for path in inputs] != before:
                raise ValueError("source changed during preparation; refusing publication")
            publish_new_file(temporary, aggregate, inputs=inputs, parameters=parameters)
            publish_new_file(temporary_meta, metadata, inputs=[aggregate], parameters=parameters)
    return "created"


def load_series(aggregate, metadata, source, *, anycast=None):
    """Read only a bounded prepared aggregate; refuse p99.5 instead of extrapolating."""
    from ipid_analysis.artifacts import assert_complete
    from ipid_analysis.paper_figures import CONTINENT_NAMES, MODES, PAPER_INTERVAL_SCHEMA

    if not aggregate.is_file() or not metadata.is_file():
        raise ValueError(
            "p99.9/256 paper quantiles missing; run paper-plots with --prepare-probing-intervals once"
        )
    for path in (aggregate, metadata):
        assert_complete(path)
        if path.stat().st_size > MAX_AGGREGATE_BYTES:
            raise ValueError(
                "expected a compact probing-interval aggregate/metadata (at most 1 MiB)"
            )
    meta = json.loads(metadata.read_text())
    if anycast is not None:
        from ipid_analysis.anycast_census import load_dataset
        from ipid_analysis.artifacts import file_digest

        census, excluded = load_dataset(anycast)
        actual = meta.get("anycast_census", {})
        if (
            actual.get("manifest_sha256") != file_digest(Path(anycast))
            or actual.get("prefixes_sha256") != file_digest(excluded)
            or actual.get("dates") != census["dates"]
            or actual.get("calculation_sha256") != _anycast_calculation_digest()
        ):
            raise ValueError(
                "anycast quantiles belong to a different census; existing files left untouched"
            )
    elif "anycast_census" in meta:
        raise ValueError("filtered quantiles cannot replace the original unfiltered paper plot")
    if (
        meta.get("figure") != KIND
        or meta.get("upper_quantile") != UPPER_QUANTILE
        or meta.get("quantile_points") != QUANTILE_POINTS
        or meta.get("quartile_probabilities") != [0.25, 0.5, 0.75]
        or meta.get("source_report_sha256") != hashlib.sha256(source.read_bytes()).hexdigest()
    ):
        raise ValueError("expected matching p99.9/256 report and population quartiles")
    with pq.ParquetFile(aggregate) as file:
        if not 0 < file.metadata.num_rows <= 14:
            raise ValueError("expected at most fourteen continent/mode groups")
        if not file.schema_arrow.equals(PAPER_INTERVAL_SCHEMA, check_metadata=False):
            raise ValueError("unexpected paper probing-interval schema")
        rows = file.read().to_pylist()
    seen, populations = set(), {}
    for row in rows:
        code, mode = row["CONTINENT_CODE"], row["MODE"]
        if code not in CONTINENT_NAMES or mode not in MODES or (code, mode) in seen:
            raise ValueError("unknown/duplicate continent or probing mode")
        seen.add((code, mode))
        count, total = row["IP_COUNT_MODE"], row["IP_COUNT_UNION"]
        if not isinstance(count, int) or not isinstance(total, int) or not 0 < count <= total:
            raise ValueError("invalid continent population")
        if code in populations and populations[code] != total:
            raise ValueError("continent union counts disagree")
        populations[code] = total
        values = np.asarray(row["QUANTILES_MS"], dtype=float)
        quartiles = np.asarray([row[key] for key in ("Q1_MS", "MEDIAN_MS", "Q3_MS")], dtype=float)
        if (
            values.shape != (256,)
            or not np.all(np.isfinite(values))
            or values[0] < 0
            or np.any(np.diff(values) < 0)
            or not np.isfinite(row["CUTOFF_MS"])
            or row["CUTOFF_MS"] != values[-1]
            or not np.all(np.isfinite(quartiles))
            or np.any(np.diff(quartiles) < 0)
            or quartiles[0] < values[0]
            or quartiles[-1] > values[-1]
        ):
            raise ValueError("invalid quantiles, cutoff or population quartiles")
    if meta.get("groups") != len(rows) or meta.get("located_ip_count_union") != sum(
        populations.values()
    ):
        raise ValueError("quantile aggregate populations disagree with metadata")
    return rows


def _place_legend(fig, ax, legend):
    """Prefer no violin intersections, otherwise minimize covered density area."""
    # Collections expose their contour through get_paths(), unlike individual patches.
    paths = [
        path.transformed(body.get_transform())
        for body in ax.collections
        for path in body.get_paths()
    ]
    locations = (
        "upper center",
        "upper right",
        "upper left",
        "center left",
        "center right",
        "center",
        "lower left",
        "lower right",
        "lower center",
    )
    placements = [(loc, None) for loc in locations]
    placements += [
        ("upper center", float(x))
        for x in sorted(np.linspace(0.2, 0.8, 31), key=lambda x: abs(x - 0.5))
    ]
    choices = []
    for preference, (location, anchor) in enumerate(placements):
        legend.set_loc(location)
        legend.set_bbox_to_anchor(None if anchor is None else (anchor, 1))
        # Only legend layout changes; drawing all violin polygons for every candidate is unnecessary.
        box = legend.get_window_extent(fig.canvas.get_renderer()).padded(2)
        intersections = sum(p.intersects_bbox(box, filled=True) for p in paths)
        xx, yy = np.meshgrid(np.linspace(box.x0, box.x1, 96), np.linspace(box.y0, box.y1, 48))
        points = np.column_stack((xx.ravel(), yy.ravel()))
        covered = np.zeros(len(points), dtype=bool)
        for path in paths:
            covered |= path.contains_points(points)
        choices.append(
            (
                intersections != 0,
                float(covered.mean()),
                intersections,
                preference,
                location,
                anchor,
            )
        )
    best = min(choices, key=lambda c: c[:4])
    legend.set_loc(best[4])
    legend.set_bbox_to_anchor(None if best[5] is None else (best[5], 1))
    return best[2]


def _anycast_count(value):
    """Format integer populations without stripping significant integer zeros."""
    if value < 1_000:
        return str(value)
    if value >= 1_000_000:
        scale, suffix, precision = 1_000_000, "M", "0.1"
    else:
        scale, suffix = 1_000, "k"
        precision = "0.1" if value < 100_000 else "1"
    rounded = (Decimal(value) / scale).quantize(Decimal(precision), rounding=ROUND_HALF_UP)
    text = format(rounded, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text + suffix


def draw(kind, rows, output):
    from matplotlib.legend_handler import HandlerPatch
    from matplotlib.patches import Patch, Rectangle
    import matplotlib.pyplot as plt
    from matplotlib.text import Text
    from matplotlib.ticker import AutoMinorLocator, MaxNLocator
    from matplotlib.transforms import Bbox

    from ipid_analysis.paper_figures import (
        CONTINENT_NAMES,
        CONTINENT_ORDER,
        MODES,
        _abbreviate_count,
    )
    from ipid_analysis.paper_plot_style import COLORS, FONT_SIZE, WIDTH, _font

    class SquareHandler(HandlerPatch):
        def create_artists(self, legend, handle, xd, yd, width, height, fontsize, trans):
            return [
                Rectangle(
                    (width / 2 - xd - 2, height / 2 - yd - 2),
                    4,
                    4,
                    facecolor=handle.get_facecolor(),
                    edgecolor="none",
                    transform=trans,
                )
            ]

    anycast = kind.endswith("-anycast")
    format_count = _anycast_count if anycast else _abbreviate_count
    font, _ = _font()
    colors = dict(zip(MODES, (COLORS["CONSTANT"], COLORS["SINGLE"])))
    codes = [code for code in CONTINENT_ORDER if any(r["CONTINENT_CODE"] == code for r in rows)]
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
            ax = fig.add_axes(
                [0.43 / WIDTH, 0.58 / HEIGHT, (WIDTH - 0.48) / WIDTH, AXES_HEIGHT / HEIGHT]
            )
            for row in rows:
                center = codes.index(row["CONTINENT_CODE"])
                side = row["MODE"] == MODES[0]
                body = ax.violinplot(
                    [row["QUANTILES_MS"]],
                    positions=[center],
                    widths=0.82,
                    showmeans=False,
                    showmedians=False,
                    showextrema=False,
                )["bodies"][0]
                vertices = body.get_paths()[0].vertices
                vertices[:, 0] = (
                    np.minimum(vertices[:, 0], center)
                    if side
                    else np.maximum(vertices[:, 0], center)
                )
                body.set(
                    facecolor=colors[row["MODE"]],
                    edgecolor=colors[row["MODE"]],
                    linewidth=0.35,
                    alpha=0.92,
                    zorder=2,
                )
                yy = np.unique(vertices[:, 1])
                xx = np.asarray(
                    [
                        vertices[vertices[:, 1] == y, 0].min()
                        if side
                        else vertices[vertices[:, 1] == y, 0].max()
                        for y in yy
                    ]
                )
                quartiles = ("Q1_MS", "MEDIAN_MS", "Q3_MS") if side or not anycast else ()
                for key in quartiles:
                    value = row[key]
                    edge = np.interp(value, yy, xx)
                    start, end = sorted((center, edge))
                    inset = 0.04 * (end - start)
                    ax.plot(
                        [start + inset, end - inset],
                        [value] * 2,
                        color="#444444",
                        linewidth=0.55 if key == "MEDIAN_MS" else 0.4,
                        linestyle="-" if key == "MEDIAN_MS" else (0, (1.5, 1.2)),
                        solid_capstyle="butt",
                        dash_capstyle="butt",
                        zorder=3,
                    )
            names = {"NA": "N. America", "SA": "S. America"}
            labels = [
                names.get(code, CONTINENT_NAMES[code])
                + "\n("
                + format_count(
                    next(r["IP_COUNT_UNION"] for r in rows if r["CONTINENT_CODE"] == code)
                )
                + ")"
                for code in codes
            ]
            ax.set(
                xlim=(-0.46, len(codes) - 0.54),
                xlabel="Continent (#IP Addr.)",
                ylabel="Probing Interval [ms]",
            )
            ax.set_ylim(bottom=0)
            ax.set_xticks(range(len(codes)), labels)
            ax.xaxis.labelpad = ax.yaxis.labelpad = 2
            ax.yaxis.set_major_locator(MaxNLocator(nbins=5, steps=[1, 2, 5, 10]))
            ax.yaxis.set_minor_locator(AutoMinorLocator(2))
            ax.tick_params(axis="both", which="major", length=2.5, width=0.45, pad=1.5)
            ax.tick_params(axis="y", which="minor", length=1.4, width=0.35)
            ax.grid(
                axis="y", which="major", color="#BDBDBD", linestyle="--", linewidth=0.35, alpha=0.7
            )
            ax.grid(
                axis="y", which="minor", color="#D9D9D9", linestyle=":", linewidth=0.3, alpha=0.7
            )
            ax.set_axisbelow(True)
            legend = ax.legend(
                handles=[Patch(facecolor=colors[m], label=m) for m in MODES],
                loc="upper center",
                prop=font,
                frameon=True,
                fancybox=True,
                facecolor="white",
                edgecolor="#BDBDBD",
                framealpha=0.9,
                borderpad=0.3,
                borderaxespad=0.4,
                labelspacing=0.2,
                handlelength=4 / FONT_SIZE,
                handletextpad=1.2 / FONT_SIZE,
                handler_map={Patch: SquareHandler()},
            )
            legend.get_frame().set_linewidth(0.45)
            for text in fig.findobj(match=Text):
                text.set_fontproperties(font)
            fig.canvas.draw()
            box = ax.yaxis.label.get_window_extent(fig.canvas.get_renderer())
            delta = (1 - box.x0) / fig.bbox.width
            frame = ax.get_position()
            ax.set_position([frame.x0 + delta, frame.y0, frame.width - delta, frame.height])
            fig.canvas.draw()
            _place_legend(fig, ax, legend)
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            ticks = [t.get_window_extent(renderer) for t in ax.get_xticklabels()]
            if any(a.x1 + 1 >= b.x0 for a, b in zip(ticks, ticks[1:])):
                raise ValueError("continent labels overlap")
            for text in fig.findobj(match=Text):
                if not text.get_visible() or not text.get_text():
                    continue
                box = text.get_window_extent(renderer)
                if text in ax.get_yticklabels() and (box.y1 < ax.bbox.y0 or box.y0 > ax.bbox.y1):
                    continue
                if (
                    min(box.x0, box.y0) < -0.5
                    or box.x1 > fig.bbox.width + 0.5
                    or box.y1 > fig.bbox.height + 0.5
                ):
                    raise ValueError(f"paper label would be clipped: {text.get_text()}")
            rgb = np.asarray(fig.canvas.buffer_rgba())[:, :, :3]
            ink = np.flatnonzero(np.any(rgb < 250, axis=(1, 2)))
            bottom = max(0, (len(rgb) - ink[-1] - 1) / fig.dpi - 1 / 300)
            top = min(HEIGHT, (len(rgb) - ink[0]) / fig.dpi + 2 / 300)
            fig.savefig(
                output,
                format="pdf",
                bbox_inches=Bbox.from_extents(0, bottom, WIDTH, top),
                pad_inches=0,
                metadata={
                    "Title": "Probing intervals by continent"
                    + (" (detected anycast /24s excluded)" if kind.endswith("-anycast") else ""),
                    "Subject": "p99.9; 256 quantiles; "
                    + ("RT-based " if anycast else "")
                    + "population Q1/median/Q3; Linux Libertine 7 pt; 3.33 inches"
                    + (
                        "; high-confidence monthly anycast-prefix union excluded"
                        if kind.endswith("-anycast")
                        else ""
                    ),
                },
            )
        finally:
            plt.close(fig)
