"""Paper plot for the RT-based to fixed-interval strategy refinement.

The stateless fixed-interval mass measurement targets exactly the addresses
classified as ``UNCLASSIFIED`` by the preceding RT-based base measurement.  The
plot renders both populations as horizontal stacked bars and visually connects
the RT ``UNCLASSIFIED`` segment to the full intended fixed-interval population.
Targets without a stored follow-up result are shown as ``NOT_ENOUGH_SAMPLES``.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb
from loguru import logger
import matplotlib
import pyarrow as pa
import pyarrow.parquet as pq
import typer

matplotlib.use("Agg")

from matplotlib.patches import Patch, Polygon  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import MultipleLocator  # noqa: E402

from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR  # noqa: E402
from ipid_analysis.coverage import coverage_for_measurement  # noqa: E402
from ipid_analysis.manifest import IpidMeasurement, resolve  # noqa: E402
from ipid_analysis.paper_figures import configure_paper_style  # noqa: E402
from ipid_analysis.strategies import (  # noqa: E402
    DEFAULT_MANIFEST,
    STRATEGY_COLORS,
    STRATEGY_NAMES,
    STRATEGY_PRETTY,
)
from ipid_analysis.strategy_merge import (  # noqa: E402
    StrategyMerge,
    load_manifest,
    resolve_strategy_merge,
)

app = typer.Typer()

KIND = "measurement-type-by-strategy"
KIND_WITH_CONNECTION = "measurement-type-by-strategy-with-connection"
RT_MODE = "RT-based"
FIXED_MODE = "Fixed-Interval"
CONNECTION_MODE = "RT-based & Connection-oriented"
MODES = (RT_MODE, FIXED_MODE)
MODES_WITH_CONNECTION = (RT_MODE, FIXED_MODE, CONNECTION_MODE)
MODE_LABELS = {CONNECTION_MODE: "RT-based &\nConnection-oriented"}

# Paper ordering follows the visual grouping used throughout the manuscript:
# direct/simple behaviours, scoped counters, then the mass-only strategies.
PLOT_STRATEGY_ORDER = (
    "REFLECTION",
    "CONSTANT",
    "SINGLE",
    "PER_DESTINATION",
    "PER_CONNECTION",
    "PER_BUCKET",
    "MULTI",
    "RANDOM",
    "UNCLASSIFIED",
    "NOT_ENOUGH_SAMPLES",
)

OUTPUT_SCHEMA = pa.schema(
    [
        ("MEASUREMENT_TYPE", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.string()),
        ("COUNT", pa.int64()),
        ("TOTAL", pa.int64()),
        ("PERCENTAGE", pa.float64()),
    ]
)


def _write_table(table: pa.Table, output_path: Path, compression: str | None) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    pq.write_table(table, temporary, compression=compression)
    temporary.replace(output_path)
    return output_path


def _write_json(output_path: Path, value: dict) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(output_path)
    return output_path


def _save_figure(fig, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_path,
        bbox_inches="tight",
        pad_inches=0.02,
        metadata={
            "Title": "Measurement type by IP-ID selection strategy",
            "Subject": "RT-based classification and fixed-interval refinement",
            "Creator": "ipid-analysis",
        },
    )
    plt.close(fig)
    return output_path


def aggregate_measurement_type_strategies(
    rt_path: Path,
    fixed_path: Path,
    output_path: Path,
    *,
    compression: str | None = "zstd",
    threads: int = 0,
) -> dict:
    """Validate the refinement population and write per-bar strategy shares."""
    for path in (rt_path, fixed_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    con = duckdb.connect(config={"threads": threads} if threads else {})
    try:
        population = con.execute(
            """
            SELECT
                (SELECT count(*) FROM read_parquet($rt)) AS rt_rows,
                (SELECT count(DISTINCT IP_ADDR) FROM read_parquet($rt)) AS rt_ips,
                (SELECT count(*) FROM read_parquet($rt)
                 WHERE CAST(IPID_SELECTION_STRATEGY AS VARCHAR) = 'UNCLASSIFIED')
                    AS rt_unclassified,
                (SELECT count(*) FROM read_parquet($fixed)) AS fixed_rows,
                (SELECT count(DISTINCT IP_ADDR) FROM read_parquet($fixed)) AS fixed_ips,
                (SELECT count(*)
                 FROM read_parquet($fixed) AS f
                 LEFT JOIN read_parquet($rt) AS r USING (IP_ADDR)
                 WHERE r.IP_ADDR IS NULL
                    OR CAST(r.IPID_SELECTION_STRATEGY AS VARCHAR) <> 'UNCLASSIFIED')
                    AS invalid_fixed_rows
            """,
            {"rt": str(rt_path), "fixed": str(fixed_path)},
        ).fetchone()
        rt_rows, rt_ips, rt_unclassified, fixed_rows, fixed_ips, invalid_fixed_rows = map(
            int, population
        )

        if rt_rows == 0:
            raise ValueError(f"{rt_path}: RT-based strategy result is empty")
        if rt_rows != rt_ips:
            raise ValueError(f"{rt_path}: duplicate IP addresses in RT-based strategy result")
        if fixed_rows != fixed_ips:
            raise ValueError(
                f"{fixed_path}: duplicate IP addresses in fixed-interval strategy result"
            )
        if invalid_fixed_rows:
            raise ValueError(
                f"{fixed_path}: {invalid_fixed_rows} fixed-interval IP address(es) were not "
                "UNCLASSIFIED in the RT-based result"
            )
        if fixed_ips > rt_unclassified:
            raise ValueError(
                f"{fixed_path}: fixed-interval population exceeds the RT UNCLASSIFIED population"
            )
        if rt_unclassified == 0:
            raise ValueError(f"{rt_path}: no RT-based UNCLASSIFIED population to refine")

        rows = con.execute(
            """
            SELECT MEASUREMENT_TYPE, STRATEGY, count(*)::BIGINT AS N
            FROM (
                SELECT 'RT-based' AS MEASUREMENT_TYPE,
                       CAST(IPID_SELECTION_STRATEGY AS VARCHAR) AS STRATEGY
                FROM read_parquet($rt)
                UNION ALL
                SELECT 'Fixed-Interval' AS MEASUREMENT_TYPE,
                       CAST(IPID_SELECTION_STRATEGY AS VARCHAR) AS STRATEGY
                FROM read_parquet($fixed)
            )
            GROUP BY MEASUREMENT_TYPE, STRATEGY
            """,
            {"rt": str(rt_path), "fixed": str(fixed_path)},
        ).fetchall()
    finally:
        con.close()

    known = set(STRATEGY_NAMES)
    unknown = sorted({str(strategy) for _, strategy, _ in rows} - known)
    if unknown:
        raise ValueError(f"unknown IP-ID strategies in refinement input: {unknown}")

    counts = {(str(mode), str(strategy)): int(count) for mode, strategy, count in rows}
    missing = rt_unclassified - fixed_ips
    counts[(FIXED_MODE, "NOT_ENOUGH_SAMPLES")] = (
        counts.get((FIXED_MODE, "NOT_ENOUGH_SAMPLES"), 0) + missing
    )
    totals = {RT_MODE: rt_rows, FIXED_MODE: rt_unclassified}
    output_rows = []
    bars = {}
    for mode in MODES:
        mode_counts = {strategy: counts.get((mode, strategy), 0) for strategy in STRATEGY_NAMES}
        mode_percentages = {
            strategy: 100.0 * count / totals[mode] for strategy, count in mode_counts.items()
        }
        bars[mode] = {
            "total": totals[mode],
            "counts": mode_counts,
            "percentages": mode_percentages,
        }
        for strategy in STRATEGY_NAMES:
            count = mode_counts[strategy]
            if count == 0:
                continue
            output_rows.append(
                {
                    "MEASUREMENT_TYPE": mode,
                    "IPID_SELECTION_STRATEGY": strategy,
                    "COUNT": count,
                    "TOTAL": totals[mode],
                    "PERCENTAGE": mode_percentages[strategy],
                }
            )

    table = pa.Table.from_pylist(output_rows, schema=OUTPUT_SCHEMA)
    _write_table(table, output_path, compression)

    return {
        "rt_based_ip_count": rt_ips,
        "rt_based_unclassified_ip_count": rt_unclassified,
        "fixed_interval_target_ip_count": rt_unclassified,
        "fixed_interval_result_ip_count": fixed_ips,
        "fixed_interval_missing_result_ip_count": missing,
        "not_enough_samples_count": bars[FIXED_MODE]["counts"]["NOT_ENOUGH_SAMPLES"],
        "fixed_interval_result_coverage_percent": (
            100.0 * fixed_ips / rt_unclassified if rt_unclassified else 0.0
        ),
        "bars": bars,
    }


def aggregate_measurement_type_strategies_with_connection(
    rt_path: Path,
    fixed_path: Path,
    connection_path: Path,
    output_path: Path,
    *,
    compression: str | None = "zstd",
    threads: int = 0,
) -> dict:
    """Add the TCP RT-based connection-oriented distribution to the refinement data."""
    if not connection_path.is_file():
        raise FileNotFoundError(connection_path)

    con = duckdb.connect(config={"threads": threads} if threads else {})
    try:
        connection_rows, connection_ips = map(
            int,
            con.execute(
                """
                SELECT count(*), count(DISTINCT IP_ADDR)
                FROM read_parquet($connection)
                """,
                {"connection": str(connection_path)},
            ).fetchone(),
        )
        rows = con.execute(
            """
            SELECT CAST(IPID_SELECTION_STRATEGY AS VARCHAR), count(*)::BIGINT
            FROM read_parquet($connection)
            GROUP BY IPID_SELECTION_STRATEGY
            """,
            {"connection": str(connection_path)},
        ).fetchall()
    finally:
        con.close()

    if connection_rows == 0:
        raise ValueError(f"{connection_path}: connection-oriented strategy result is empty")
    if connection_rows != connection_ips:
        raise ValueError(
            f"{connection_path}: duplicate IP addresses in connection-oriented strategy result"
        )

    known = set(STRATEGY_NAMES)
    unknown = sorted({str(strategy) for strategy, _ in rows} - known)
    if unknown:
        raise ValueError(f"unknown IP-ID strategies in connection-oriented input: {unknown}")

    stats = aggregate_measurement_type_strategies(
        rt_path,
        fixed_path,
        output_path,
        compression=compression,
        threads=threads,
    )

    connection_counts = {strategy: 0 for strategy in STRATEGY_NAMES} | {
        str(strategy): int(count) for strategy, count in rows
    }
    connection_percentages = {
        strategy: 100.0 * count / connection_rows for strategy, count in connection_counts.items()
    }
    connection_output_rows = [
        {
            "MEASUREMENT_TYPE": CONNECTION_MODE,
            "IPID_SELECTION_STRATEGY": strategy,
            "COUNT": count,
            "TOTAL": connection_rows,
            "PERCENTAGE": connection_percentages[strategy],
        }
        for strategy, count in connection_counts.items()
        if count
    ]
    table = pa.concat_tables(
        [
            pq.read_table(output_path, schema=OUTPUT_SCHEMA),
            pa.Table.from_pylist(connection_output_rows, schema=OUTPUT_SCHEMA),
        ]
    )
    _write_table(table, output_path, compression)

    stats["connection_oriented_ip_count"] = connection_ips
    stats["bars"][CONNECTION_MODE] = {
        "total": connection_rows,
        "counts": connection_counts,
        "percentages": connection_percentages,
    }
    return stats


def _label_percentage(value: float) -> str:
    if value < 1.0:
        return f"{value:.1f}"
    return f"{value:.0f}"


def plot_measurement_type_by_strategy(
    aggregate_path: Path,
    output_path: Path,
    *,
    modes: tuple[str, ...] = MODES,
) -> Path:
    """Render stacked strategy bars and the RT-unclassified refinement guides."""
    rows = pq.read_table(aggregate_path).to_pylist()
    percentages = {
        (row["MEASUREMENT_TYPE"], row["IPID_SELECTION_STRATEGY"]): float(row["PERCENTAGE"])
        for row in rows
    }
    represented = [
        strategy
        for strategy in PLOT_STRATEGY_ORDER
        if any(percentages.get((mode, strategy), 0.0) > 0 for mode in modes)
    ]
    if not represented:
        raise ValueError(f"{aggregate_path}: no strategy shares to plot")

    rt_unclassified_left = sum(
        percentages.get((RT_MODE, strategy), 0.0)
        for strategy in PLOT_STRATEGY_ORDER
        if strategy != "UNCLASSIFIED"
        and PLOT_STRATEGY_ORDER.index(strategy) < PLOT_STRATEGY_ORDER.index("UNCLASSIFIED")
    )
    rt_unclassified_width = percentages.get((RT_MODE, "UNCLASSIFIED"), 0.0)
    rt_unclassified_right = rt_unclassified_left + rt_unclassified_width

    configure_paper_style()
    figure_height = 2.45 if len(modes) == 2 else 2.90
    fig, ax = plt.subplots(figsize=(7.16, figure_height))
    bar_height = 0.38
    y_positions = {mode: float(len(modes) - index - 1) for index, mode in enumerate(modes)}

    if rt_unclassified_width > 0:
        rt_bottom = y_positions[RT_MODE] - bar_height / 2
        fixed_top = y_positions[FIXED_MODE] + bar_height / 2
        polygon = Polygon(
            [
                (rt_unclassified_left, rt_bottom),
                (rt_unclassified_right, rt_bottom),
                (100.0, fixed_top),
                (0.0, fixed_top),
            ],
            closed=True,
            facecolor="#BDBDBD",
            edgecolor="none",
            alpha=0.14,
            zorder=0,
        )
        ax.add_patch(polygon)
        connector_style = {
            "color": "#A6A6A6",
            "linestyle": "--",
            "linewidth": 0.85,
            "zorder": 1,
        }
        ax.plot([rt_unclassified_left, 0.0], [rt_bottom, fixed_top], **connector_style)
        ax.plot([rt_unclassified_right, 100.0], [rt_bottom, fixed_top], **connector_style)

    for mode in modes:
        left = 0.0
        for strategy in PLOT_STRATEGY_ORDER:
            value = percentages.get((mode, strategy), 0.0)
            if value <= 0:
                continue
            ax.barh(
                y_positions[mode],
                value,
                left=left,
                height=bar_height,
                color=STRATEGY_COLORS.get(strategy, "#8C8C8C"),
                edgecolor="none",
                zorder=2,
            )
            if value >= 1.5:
                ax.text(
                    left + value / 2,
                    y_positions[mode],
                    _label_percentage(value),
                    ha="center",
                    va="center",
                    fontsize=7,
                    color="#111111",
                    zorder=3,
                )
            left += value

    ax.set_xlim(0, 100)
    ax.set_ylim(-0.52, len(modes) - 0.48)
    ax.set_yticks(
        [y_positions[mode] for mode in modes],
        [MODE_LABELS.get(mode, mode) for mode in modes],
    )
    ax.set_xlabel("IP-ID Selection Strategy [%]")
    ax.set_ylabel("Measurement Type")
    ax.xaxis.set_major_locator(MultipleLocator(20))
    ax.xaxis.set_minor_locator(MultipleLocator(5))
    ax.tick_params(axis="x", which="major", length=5, width=0.8)
    ax.tick_params(axis="x", which="minor", length=2.8, width=0.65)
    ax.grid(axis="x", which="major", color="#BDBDBD", linestyle="--", linewidth=0.5, alpha=0.7)
    ax.set_axisbelow(True)

    handles = [
        Patch(
            facecolor=STRATEGY_COLORS.get(strategy, "#8C8C8C"),
            edgecolor="none",
            label=STRATEGY_PRETTY.get(strategy, strategy),
        )
        for strategy in represented
    ]
    ax.legend(
        handles=handles,
        ncol=min(5, len(handles)),
        loc="lower center",
        bbox_to_anchor=(0.5, 1.035),
        frameon=False,
        borderaxespad=0,
        columnspacing=1.25,
        handlelength=1.45,
        handletextpad=0.4,
    )
    left = 0.16 if len(modes) == 2 else 0.25
    bottom = 0.24 if len(modes) == 2 else 0.20
    fig.subplots_adjust(left=left, right=0.995, bottom=bottom, top=0.68)
    return _save_figure(fig, output_path)


def _validate_connection_measurement(
    merge: StrategyMerge,
    connection: IpidMeasurement,
) -> None:
    if merge.protocol != "tcp":
        raise ValueError("connection-oriented strategy refinement is only supported for TCP")
    if (
        connection.protocol != "tcp"
        or connection.connection_mode != "connection"
        or connection.interval != "rt-based"
        or connection.scale != "base"
    ):
        raise ValueError(
            "connection-oriented strategy refinement requires tcp.ipid.connection.rt-based.base"
        )
    if connection.zmap_id != merge.zmap_id:
        raise ValueError("all strategy refinement targets must belong to the same zmap campaign")


def render(
    merge: StrategyMerge,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    raw_root: Path = RAW_DATA_DIR,
    compression: str | None = "zstd",
    threads: int = 0,
) -> tuple[Path, Path, Path]:
    """Create aggregate Parquet, paper PDF, and metadata JSON."""
    if merge.base.interval != "rt-based" or merge.mass.interval != "fixed-interval":
        raise ValueError(
            "strategy refinement requires an RT-based base target followed by a "
            "fixed-interval mass target"
        )

    rt_path = merge.base.artifact_path(processed_root, "strategies")
    fixed_path = merge.mass.artifact_path(processed_root, "strategies")
    aggregate_path = merge.artifact_path(processed_root, KIND)
    pdf_path = merge.artifact_path(figures_root, KIND, "pdf")
    json_path = merge.artifact_path(figures_root, KIND, "json")

    stats = aggregate_measurement_type_strategies(
        rt_path,
        fixed_path,
        aggregate_path,
        compression=compression,
        threads=threads,
    )
    plot_measurement_type_by_strategy(aggregate_path, pdf_path)
    _write_json(
        json_path,
        {
            "target": merge.target,
            "protocol": merge.protocol,
            "connection_mode": merge.connection_mode,
            "zmap_id": merge.zmap_id,
            "measurements": {
                "rt_based_base": merge.base.measurement_id,
                "fixed_interval_mass": merge.mass.measurement_id,
            },
            "sources": {"rt_based": str(rt_path), "fixed_interval": str(fixed_path)},
            "aggregate": str(aggregate_path),
            "ipid_measurement_coverage": coverage_for_measurement(
                merge.base,
                processed_root=processed_root,
                raw_root=raw_root,
            ),
            "figure": KIND,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "methodology": {
                "rt_based_normalization": "all rows in the RT-based base strategy result",
                "fixed_interval_normalization": (
                    "all intended fixed-interval targets; missing result rows are "
                    "NOT_ENOUGH_SAMPLES"
                ),
                "fixed_interval_target_population": (
                    "IP addresses classified UNCLASSIFIED by the RT-based base measurement"
                ),
                "connector": (
                    "RT-based UNCLASSIFIED segment expands to the fixed-interval result bar"
                ),
            },
            **stats,
        },
    )
    return pdf_path, json_path, aggregate_path


def render_with_connection(
    merge: StrategyMerge,
    connection: IpidMeasurement,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    raw_root: Path = RAW_DATA_DIR,
    compression: str | None = "zstd",
    threads: int = 0,
) -> tuple[Path, Path, Path]:
    """Create the TCP refinement plot with an additional connection-oriented bar."""
    if merge.base.interval != "rt-based" or merge.mass.interval != "fixed-interval":
        raise ValueError(
            "strategy refinement requires an RT-based base target followed by a "
            "fixed-interval mass target"
        )
    _validate_connection_measurement(merge, connection)

    rt_path = merge.base.artifact_path(processed_root, "strategies")
    fixed_path = merge.mass.artifact_path(processed_root, "strategies")
    connection_path = connection.artifact_path(processed_root, "strategies")
    aggregate_path = merge.artifact_path(processed_root, KIND_WITH_CONNECTION)
    pdf_path = merge.artifact_path(figures_root, KIND_WITH_CONNECTION, "pdf")
    json_path = merge.artifact_path(figures_root, KIND_WITH_CONNECTION, "json")

    stats = aggregate_measurement_type_strategies_with_connection(
        rt_path,
        fixed_path,
        connection_path,
        aggregate_path,
        compression=compression,
        threads=threads,
    )
    plot_measurement_type_by_strategy(
        aggregate_path,
        pdf_path,
        modes=MODES_WITH_CONNECTION,
    )
    _write_json(
        json_path,
        {
            "target": f"{merge.target}+{connection.target}",
            "protocol": merge.protocol,
            "zmap_id": merge.zmap_id,
            "measurements": {
                "rt_based_base": merge.base.measurement_id,
                "fixed_interval_mass": merge.mass.measurement_id,
                "rt_based_connection_oriented": connection.measurement_id,
            },
            "sources": {
                "rt_based": str(rt_path),
                "fixed_interval": str(fixed_path),
                "rt_based_connection_oriented": str(connection_path),
            },
            "aggregate": str(aggregate_path),
            "ipid_measurement_coverage": coverage_for_measurement(
                merge.base,
                processed_root=processed_root,
                raw_root=raw_root,
            ),
            "figure": KIND_WITH_CONNECTION,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "methodology": {
                "rt_based_normalization": "all rows in the RT-based base strategy result",
                "fixed_interval_normalization": (
                    "all intended fixed-interval targets; missing result rows are "
                    "NOT_ENOUGH_SAMPLES"
                ),
                "fixed_interval_target_population": (
                    "IP addresses classified UNCLASSIFIED by the RT-based base measurement"
                ),
                "connection_oriented_normalization": (
                    "all rows in the RT-based connection-oriented base strategy result"
                ),
                "connector": (
                    "RT-based UNCLASSIFIED segment expands to the fixed-interval result bar"
                ),
            },
            **stats,
        },
    )
    return pdf_path, json_path, aggregate_path


@app.command()
def main(
    base_target: str = typer.Argument(..., help="RT-based base measurement target"),
    mass_target: str = typer.Argument(..., help="fixed-interval mass measurement target"),
    connection_target: str | None = typer.Argument(
        None,
        help="optional TCP RT-based connection-oriented base measurement target",
    ),
    manifest: Path = typer.Option(DEFAULT_MANIFEST, help="measurement manifest JSON"),
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
    threads: int = typer.Option(0, min=0, help="DuckDB threads; 0 uses all cores"),
) -> None:
    try:
        manifest_data = load_manifest(manifest)
        merge = resolve_strategy_merge(manifest_data, base_target, mass_target)
        if connection_target is None:
            outputs = render(
                merge,
                compression=None if compression == "none" else compression,
                threads=threads,
            )
        else:
            connection = resolve(manifest_data, connection_target)
            if connection is None:
                raise ValueError(f"{connection_target}: not present in manifest")
            outputs = render_with_connection(
                merge,
                connection,
                compression=None if compression == "none" else compression,
                threads=threads,
            )
    except (FileNotFoundError, ValueError) as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1) from exc
    logger.success(f"[{merge.target}] strategy-refinement figure -> {outputs[0]}")


if __name__ == "__main__":
    app()
