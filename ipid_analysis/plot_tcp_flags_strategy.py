"""Paper plot of merged TCP IP-ID strategies by ZMap reply classification."""

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

from matplotlib.patches import Patch  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import MultipleLocator  # noqa: E402

from ipid_analysis.config import (  # noqa: E402
    FIGURES_DIR,
    PROCESSED_DATA_DIR,
    RAW_DATA_DIR,
)
from ipid_analysis.paper_figures import configure_paper_style  # noqa: E402
from ipid_analysis.plot_strategy_refinement import PLOT_STRATEGY_ORDER  # noqa: E402
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

KIND = "tcp-flags-by-strategy"
ALL_FLAGS = "SYN-ACK/RST"
SYNACK_FLAGS = "SYN-ACK"
RST_FLAGS = "RST"
FLAG_GROUPS = (ALL_FLAGS, SYNACK_FLAGS, RST_FLAGS)
ZMAP_INPUT_NAME = "zmap.pq"

OUTPUT_SCHEMA = pa.schema(
    [
        ("TCP_FLAGS", pa.string()),
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
            "Title": "TCP flags by IP-ID selection strategy",
            "Subject": "Merged RT-based and fixed-interval TCP strategy classification",
            "Creator": "ipid-analysis",
        },
    )
    plt.close(fig)
    return output_path


def aggregate_tcp_flags_strategies(
    merged_path: Path,
    zmap_path: Path,
    output_path: Path,
    *,
    compression: str | None = "zstd",
    threads: int = 0,
) -> dict:
    """Join merged strategies to TCP ZMap replies and aggregate three distributions."""
    for path in (merged_path, zmap_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    con = duckdb.connect(config={"threads": threads} if threads else {})
    try:
        population = con.execute(
            """
            WITH merged AS (
                SELECT IP_ADDR,
                       CAST(IPID_SELECTION_STRATEGY AS VARCHAR) AS STRATEGY
                FROM read_parquet($merged)
            ),
            zmap AS (
                SELECT IP_ADDR, lower(trim(CAST(REPLY_TYPE AS VARCHAR))) AS REPLY_TYPE
                FROM read_parquet($zmap)
            )
            SELECT
                (SELECT count(*) FROM merged),
                (SELECT count(DISTINCT IP_ADDR) FROM merged),
                (SELECT count(*) FROM zmap),
                (SELECT count(DISTINCT IP_ADDR) FROM zmap),
                count(*) FILTER (WHERE zmap.IP_ADDR IS NULL),
                count(*) FILTER (
                    WHERE zmap.IP_ADDR IS NOT NULL
                      AND (
                          zmap.REPLY_TYPE IS NULL
                          OR zmap.REPLY_TYPE NOT IN ('synack', 'rst')
                      )
                ),
                count(*) FILTER (WHERE zmap.REPLY_TYPE = 'synack'),
                count(*) FILTER (WHERE zmap.REPLY_TYPE = 'rst')
            FROM merged
            LEFT JOIN zmap USING (IP_ADDR)
            """,
            {"merged": str(merged_path), "zmap": str(zmap_path)},
        ).fetchone()
        (
            merged_rows,
            merged_ips,
            zmap_rows,
            zmap_ips,
            missing_zmap_rows,
            unsupported_reply_rows,
            synack_rows,
            rst_rows,
        ) = map(int, population)

        rows = con.execute(
            """
            WITH joined AS (
                SELECT CAST(m.IPID_SELECTION_STRATEGY AS VARCHAR) AS STRATEGY,
                       lower(trim(CAST(z.REPLY_TYPE AS VARCHAR))) AS REPLY_TYPE
                FROM read_parquet($merged) AS m
                INNER JOIN read_parquet($zmap) AS z USING (IP_ADDR)
                WHERE lower(trim(CAST(z.REPLY_TYPE AS VARCHAR))) IN ('synack', 'rst')
            ), expanded AS (
                SELECT 'SYN-ACK/RST' AS TCP_FLAGS, STRATEGY FROM joined
                UNION ALL
                SELECT CASE REPLY_TYPE
                           WHEN 'synack' THEN 'SYN-ACK'
                           ELSE 'RST'
                       END AS TCP_FLAGS,
                       STRATEGY
                FROM joined
            )
            SELECT TCP_FLAGS, STRATEGY, count(*)::BIGINT
            FROM expanded
            GROUP BY TCP_FLAGS, STRATEGY
            """,
            {"merged": str(merged_path), "zmap": str(zmap_path)},
        ).fetchall()
    finally:
        con.close()

    if merged_rows == 0:
        raise ValueError(f"{merged_path}: merged strategy result is empty")
    if merged_rows != merged_ips:
        raise ValueError(f"{merged_path}: duplicate IP addresses in merged strategy result")
    if zmap_rows != zmap_ips:
        raise ValueError(f"{zmap_path}: duplicate IP addresses in ZMap result")
    if missing_zmap_rows:
        raise ValueError(
            f"{merged_path}: {missing_zmap_rows} merged IP address(es) are missing from ZMap"
        )
    if unsupported_reply_rows:
        raise ValueError(
            f"{zmap_path}: {unsupported_reply_rows} merged IP address(es) do not have "
            "a synack or rst classification"
        )
    if synack_rows == 0 or rst_rows == 0:
        raise ValueError(
            f"{zmap_path}: TCP flags plot requires both synack and rst classifications"
        )

    known = set(STRATEGY_NAMES)
    unknown = sorted({str(strategy) for _, strategy, _ in rows} - known)
    if unknown:
        raise ValueError(f"unknown IP-ID strategies in TCP flags input: {unknown}")

    totals = {
        ALL_FLAGS: synack_rows + rst_rows,
        SYNACK_FLAGS: synack_rows,
        RST_FLAGS: rst_rows,
    }
    counts = {(str(flags), str(strategy)): int(count) for flags, strategy, count in rows}
    output_rows = []
    bars = {}
    for flags in FLAG_GROUPS:
        flag_counts = {strategy: counts.get((flags, strategy), 0) for strategy in STRATEGY_NAMES}
        percentages = {
            strategy: 100.0 * count / totals[flags] for strategy, count in flag_counts.items()
        }
        bars[flags] = {
            "total": totals[flags],
            "counts": flag_counts,
            "percentages": percentages,
        }
        for strategy, count in flag_counts.items():
            if count:
                output_rows.append(
                    {
                        "TCP_FLAGS": flags,
                        "IPID_SELECTION_STRATEGY": strategy,
                        "COUNT": count,
                        "TOTAL": totals[flags],
                        "PERCENTAGE": percentages[strategy],
                    }
                )

    _write_table(pa.Table.from_pylist(output_rows, schema=OUTPUT_SCHEMA), output_path, compression)
    return {
        "merged_ip_count": merged_ips,
        "zmap_ip_count": zmap_ips,
        "matched_ip_count": synack_rows + rst_rows,
        "synack_ip_count": synack_rows,
        "rst_ip_count": rst_rows,
        "bars": bars,
    }


def _label_percentage(value: float) -> str:
    if value < 1.0:
        return f"{value:.1f}"
    return f"{value:.0f}"


def plot_tcp_flags_by_strategy(aggregate_path: Path, output_path: Path) -> Path:
    """Render the three independently normalized TCP reply-classification bars."""
    rows = pq.read_table(aggregate_path).to_pylist()
    percentages = {
        (row["TCP_FLAGS"], row["IPID_SELECTION_STRATEGY"]): float(row["PERCENTAGE"])
        for row in rows
    }
    represented = [
        strategy
        for strategy in PLOT_STRATEGY_ORDER
        if any(percentages.get((flags, strategy), 0.0) > 0 for flags in FLAG_GROUPS)
    ]
    if not represented:
        raise ValueError(f"{aggregate_path}: no strategy shares to plot")

    configure_paper_style()
    fig, ax = plt.subplots(figsize=(7.16, 2.75))
    bar_height = 0.42
    y_positions = {
        flags: float(len(FLAG_GROUPS) - index - 1) for index, flags in enumerate(FLAG_GROUPS)
    }

    for flags in FLAG_GROUPS:
        left = 0.0
        for strategy in PLOT_STRATEGY_ORDER:
            value = percentages.get((flags, strategy), 0.0)
            if value <= 0:
                continue
            ax.barh(
                y_positions[flags],
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
                    y_positions[flags],
                    _label_percentage(value),
                    ha="center",
                    va="center",
                    fontsize=7,
                    color="#111111",
                    zorder=3,
                )
            left += value

    ax.set_xlim(0, 100)
    ax.set_ylim(-0.52, len(FLAG_GROUPS) - 0.48)
    ax.set_yticks([y_positions[flags] for flags in FLAG_GROUPS], FLAG_GROUPS)
    ax.set_xlabel("IP-ID Selection Strategy [%]")
    ax.set_ylabel("TCP Flags")
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
    fig.subplots_adjust(left=0.18, right=0.995, bottom=0.21, top=0.68)
    return _save_figure(fig, output_path)


def render(
    merge: StrategyMerge,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    raw_root: Path = RAW_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    compression: str | None = "zstd",
    threads: int = 0,
) -> tuple[Path, Path, Path]:
    """Create aggregate Parquet, paper PDF, and metadata JSON."""
    if (
        merge.protocol != "tcp"
        or merge.connection_mode != "no-connection"
        or merge.base.interval != "rt-based"
        or merge.mass.interval != "fixed-interval"
    ):
        raise ValueError(
            "TCP flags plot requires tcp.ipid.no-connection.rt-based.base followed by "
            "tcp.ipid.no-connection.fixed-interval.mass"
        )

    merged_path = merge.artifact_path(processed_root, "strategies")
    zmap_path = raw_root / "zmap" / merge.zmap_id / ZMAP_INPUT_NAME
    aggregate_path = merge.artifact_path(processed_root, KIND)
    pdf_path = merge.artifact_path(figures_root, KIND, "pdf")
    json_path = merge.artifact_path(figures_root, KIND, "json")

    stats = aggregate_tcp_flags_strategies(
        merged_path,
        zmap_path,
        aggregate_path,
        compression=compression,
        threads=threads,
    )
    plot_tcp_flags_by_strategy(aggregate_path, pdf_path)
    _write_json(
        json_path,
        {
            "target": merge.target,
            "protocol": merge.protocol,
            "zmap_id": merge.zmap_id,
            "measurements": {
                "rt_based_base": merge.base.measurement_id,
                "fixed_interval_mass": merge.mass.measurement_id,
            },
            "sources": {
                "merged_strategies": str(merged_path),
                "zmap": str(zmap_path),
            },
            "aggregate": str(aggregate_path),
            "figure": KIND,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "methodology": {
                "strategy_input": "merged RT-based base and fixed-interval mass classification",
                "tcp_flag_source": "ZMap REPLY_TYPE classification joined by IP_ADDR",
                "reply_type_mapping": {"synack": SYNACK_FLAGS, "rst": RST_FLAGS},
                "all_bar": "union of joined synack and rst rows",
                "normalization": "each TCP flag bar is normalized independently to 100%",
            },
            **stats,
        },
    )
    return pdf_path, json_path, aggregate_path


@app.command()
def main(
    base_target: str = typer.Argument(..., help="TCP RT-based base measurement target"),
    mass_target: str = typer.Argument(..., help="TCP fixed-interval mass measurement target"),
    manifest: Path = typer.Option(DEFAULT_MANIFEST, help="measurement manifest JSON"),
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
    threads: int = typer.Option(0, min=0, help="DuckDB threads; 0 uses all cores"),
) -> None:
    try:
        merge = resolve_strategy_merge(load_manifest(manifest), base_target, mass_target)
        outputs = render(
            merge,
            compression=None if compression == "none" else compression,
            threads=threads,
        )
    except (FileNotFoundError, ValueError) as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1) from exc
    logger.success(f"[{merge.target}] TCP-flags strategy figure -> {outputs[0]}")


if __name__ == "__main__":
    app()
