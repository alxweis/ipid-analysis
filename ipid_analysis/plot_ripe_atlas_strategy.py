"""RIPE Atlas observed-role by IP-ID strategy analysis."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")

from matplotlib.patches import Patch
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator

from ipid_analysis.caida_itdk import ITDKDataset
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.paper_figures import configure_paper_style
from ipid_analysis.ripe_atlas import RipeAtlasDataset
from ipid_analysis.strategies import (
    PAPER_STRATEGY_ORDER,
    STRATEGY_COLORS,
    STRATEGY_PRETTY,
)
from ipid_analysis.strategy_merge import StrategyMerge

TRANSIT_ROLE = "Transit-Observed"
DESTINATION_ROLE = "Destination-Only"
ROLE_ORDER = (TRANSIT_ROLE, DESTINATION_ROLE)
NON_STRATEGIES = frozenset({"UNCLASSIFIED", "NOT_ENOUGH_SAMPLES"})
PLOT_STRATEGIES = tuple(name for name in PAPER_STRATEGY_ORDER if name != "NOT_ENOUGH_SAMPLES")


@dataclass(frozen=True)
class RipeAtlasAnalysisOutputs:
    joined: Path
    distribution: Path
    role_pdf: Path
    role_json: Path
    caida_agreement: Path | None


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _source_metadata(source: IpidMeasurement | StrategyMerge) -> dict:
    metadata = {
        "target": source.target,
        "protocol": source.protocol,
        "connection_mode": source.connection_mode,
        "zmap_id": source.zmap_id,
    }
    if isinstance(source, StrategyMerge):
        metadata.update(
            {
                "base_target": source.base.target,
                "base_measurement_id": source.base.measurement_id,
                "mass_target": source.mass.target,
                "mass_measurement_id": source.mass.measurement_id,
            }
        )
    else:
        metadata.update(
            {
                "measurement_id": source.measurement_id,
                "interval": source.interval,
                "scale": source.scale,
            }
        )
    return metadata


def _write_query(con: duckdb.DuckDBPyConnection, query: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_suffix(output.suffix + ".part")
    partial.unlink(missing_ok=True)
    con.execute(f"COPY ({query}) TO '{_sql_path(partial)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    partial.replace(output)


def _artifact_paths(
    source: IpidMeasurement | StrategyMerge,
    *,
    processed_root: Path,
    figures_root: Path,
    with_caida: bool,
) -> RipeAtlasAnalysisOutputs:
    return RipeAtlasAnalysisOutputs(
        joined=source.artifact_path(processed_root, "ripe-atlas-strategy-join"),
        distribution=source.artifact_path(processed_root, "ripe-atlas-role-strategy-distribution"),
        role_pdf=source.artifact_path(figures_root, "ripe-atlas-role-by-strategy", "pdf"),
        role_json=source.artifact_path(figures_root, "ripe-atlas-role-by-strategy", "json"),
        caida_agreement=(
            source.artifact_path(processed_root, "caida-ripe-role-agreement")
            if with_caida
            else None
        ),
    )


def _role_plot_data(rows: list[tuple[str, str, int]]) -> dict[str, dict[str, float]]:
    counts = {role: {name: 0 for name in PLOT_STRATEGIES} for role in ROLE_ORDER}
    for role, strategy, count in rows:
        display_strategy = "UNCLASSIFIED" if strategy in NON_STRATEGIES else strategy
        if role in counts and display_strategy in counts[role]:
            counts[role][display_strategy] += int(count)
    return {
        role: {
            strategy: (
                100.0 * count / sum(counts[role].values()) if sum(counts[role].values()) else 0.0
            )
            for strategy, count in counts[role].items()
        }
        for role in ROLE_ORDER
    }


def _label_percentage(value: float) -> str:
    return f"{value:.1f}" if value < 1.0 else f"{value:.0f}"


def _plot_roles(percentages: dict[str, dict[str, float]], output: Path) -> None:
    configure_paper_style()
    fig, ax = plt.subplots(figsize=(7.16, 2.45))
    y_positions = {
        role: float(len(ROLE_ORDER) - index - 1) for index, role in enumerate(ROLE_ORDER)
    }
    for role in ROLE_ORDER:
        left = 0.0
        for strategy in PLOT_STRATEGIES:
            value = percentages[role].get(strategy, 0.0)
            if value <= 0:
                continue
            ax.barh(
                y_positions[role],
                value,
                left=left,
                height=0.38,
                color=STRATEGY_COLORS[strategy],
                edgecolor="none",
                zorder=2,
            )
            if value >= 1.5:
                ax.text(
                    left + value / 2.0,
                    y_positions[role],
                    _label_percentage(value),
                    ha="center",
                    va="center",
                    fontsize=9,
                    color="#111111",
                    zorder=3,
                )
            left += value

    ax.set_xlim(0, 100)
    ax.set_ylim(-0.52, len(ROLE_ORDER) - 0.48)
    ax.set_yticks(
        [y_positions[role] for role in ROLE_ORDER],
        ["Transit-Observed", "Destination-\nOnly"],
    )
    ax.set_xlabel("IP-ID Selection Strategy [%]")
    ax.set_ylabel("Observed Network Role")
    ax.xaxis.set_major_locator(MultipleLocator(20))
    ax.xaxis.set_minor_locator(MultipleLocator(5))
    ax.tick_params(axis="x", which="major", length=5, width=0.8)
    ax.tick_params(axis="x", which="minor", length=2.8, width=0.65)
    ax.grid(
        axis="x",
        which="major",
        color="#BDBDBD",
        linestyle="--",
        linewidth=0.5,
        alpha=0.7,
    )
    ax.set_axisbelow(True)
    handles = [
        Patch(
            facecolor=STRATEGY_COLORS[strategy],
            edgecolor="none",
            label=STRATEGY_PRETTY[strategy],
        )
        for strategy in PLOT_STRATEGIES
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
    fig.subplots_adjust(left=0.20, right=0.995, bottom=0.24, top=0.68)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output,
        bbox_inches="tight",
        pad_inches=0.02,
        metadata={
            "Title": "Observed network role by IP-ID selection strategy",
            "Subject": "RIPE Atlas traceroute roles and IP-ID strategy",
            "Creator": "ipid-analysis",
        },
    )
    plt.close(fig)


def render_ripe_atlas_analysis(
    source: IpidMeasurement | StrategyMerge,
    dataset: RipeAtlasDataset,
    *,
    itdk: ITDKDataset | None = None,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    threads: int = 0,
) -> RipeAtlasAnalysisOutputs:
    """Join one strategy result to RIPE roles and render paper artifacts."""
    strategies = source.artifact_path(processed_root, "strategies")
    if not strategies.is_file():
        raise FileNotFoundError(strategies)
    if not dataset.roles_path.is_file():
        raise FileNotFoundError(dataset.roles_path)
    if itdk is not None and not itdk.interfaces_path.is_file():
        raise FileNotFoundError(itdk.interfaces_path)

    outputs = _artifact_paths(
        source,
        processed_root=processed_root,
        figures_root=figures_root,
        with_caida=itdk is not None,
    )
    con = duckdb.connect(config={"threads": threads} if threads else {})
    joined_path = _sql_path(outputs.joined)
    try:
        _write_query(
            con,
            f"""
            SELECT
                s.IP_ADDR,
                CAST(s.IPID_SELECTION_STRATEGY AS VARCHAR) AS IPID_SELECTION_STRATEGY,
                r.T,
                r.D,
                r.TRACE_COUNT,
                r.PROBE_COUNT,
                r.MEASUREMENT_COUNT,
                r.PROTOCOLS,
                r.IP_ADDR IS NOT NULL AS RIPE_MATCH,
                CASE
                    WHEN r.IP_ADDR IS NULL THEN NULL
                    WHEN r.T THEN '{TRANSIT_ROLE}'
                    ELSE '{DESTINATION_ROLE}'
                END AS OBSERVED_ROLE
            FROM read_parquet('{_sql_path(strategies)}') AS s
            LEFT JOIN read_parquet('{_sql_path(dataset.roles_path)}') AS r USING (IP_ADDR)
            """,
            outputs.joined,
        )
        _write_query(
            con,
            f"""
            SELECT
                OBSERVED_ROLE,
                IPID_SELECTION_STRATEGY,
                count(*)::BIGINT AS COUNT,
                100.0 * count(*) / sum(count(*)) OVER (PARTITION BY OBSERVED_ROLE)
                    AS PERCENTAGE
            FROM read_parquet('{joined_path}')
            WHERE RIPE_MATCH
            GROUP BY OBSERVED_ROLE, IPID_SELECTION_STRATEGY
            ORDER BY OBSERVED_ROLE, IPID_SELECTION_STRATEGY
            """,
            outputs.distribution,
        )
        role_rows = con.execute(
            f"""
            SELECT OBSERVED_ROLE, IPID_SELECTION_STRATEGY, COUNT
            FROM read_parquet('{_sql_path(outputs.distribution)}')
            """
        ).fetchall()
        coverage = con.execute(
            f"""
            SELECT
                count(*)::BIGINT,
                count(*) FILTER (WHERE RIPE_MATCH)::BIGINT,
                count(*) FILTER (WHERE NOT RIPE_MATCH)::BIGINT,
                count(*) FILTER (WHERE RIPE_MATCH AND T AND D)::BIGINT,
                count(*) FILTER (WHERE RIPE_MATCH AND T AND NOT D)::BIGINT,
                count(*) FILTER (WHERE RIPE_MATCH AND NOT T AND D)::BIGINT
            FROM read_parquet('{joined_path}')
            """
        ).fetchone()
        if not coverage[1]:
            raise ValueError(f"{source.target}: no measured IP address matched RIPE Atlas")

        agreement_rows = []
        if itdk is not None and outputs.caida_agreement is not None:
            _write_query(
                con,
                f"""
                WITH dual_matches AS (
                    SELECT j.IP_ADDR, j.T AS RIPE_T, i.T AS ITDK_T
                    FROM read_parquet('{joined_path}') AS j
                    JOIN read_parquet('{_sql_path(itdk.interfaces_path)}') AS i USING (IP_ADDR)
                    WHERE j.RIPE_MATCH
                ), categorized AS (
                    SELECT
                        CASE
                            WHEN RIPE_T AND ITDK_T THEN 'Both Transit-Observed'
                            WHEN NOT RIPE_T AND ITDK_T THEN 'CAIDA Only Transit-Observed'
                            WHEN RIPE_T AND NOT ITDK_T THEN 'RIPE Only Transit-Observed'
                            ELSE 'Neither Transit-Observed'
                        END AS CATEGORY
                    FROM dual_matches
                )
                SELECT
                    CATEGORY,
                    count(*)::BIGINT AS COUNT,
                    100.0 * count(*) / sum(count(*)) OVER () AS PERCENTAGE
                FROM categorized
                GROUP BY CATEGORY
                ORDER BY COUNT DESC, CATEGORY
                """,
                outputs.caida_agreement,
            )
            agreement_rows = con.execute(
                f"""
                SELECT CATEGORY, COUNT, PERCENTAGE
                FROM read_parquet('{_sql_path(outputs.caida_agreement)}')
                ORDER BY COUNT DESC, CATEGORY
                """
            ).fetchall()
    finally:
        con.close()

    percentages = _role_plot_data(role_rows)
    _plot_roles(percentages, outputs.role_pdf)
    total, matched, unmatched, t1_d1, t1_d0, t0_d1 = map(int, coverage)
    report = {
        **_source_metadata(source),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ripe_atlas": {
            "source": dataset.source,
            "window_start": dataset.window.start.isoformat(),
            "window_end": dataset.window.end.isoformat(),
            "roles": str(dataset.roles_path),
            "metadata": str(dataset.metadata_path),
        },
        "role_definition": {
            TRANSIT_ROLE: "RIPE observation with T=true; D arbitrary",
            DESTINATION_ROLE: "RIPE observation with T=false and D=true",
        },
        "coverage": {
            "total": total,
            "matched": matched,
            "unmatched": unmatched,
            "match_percentage": 100.0 * matched / total if total else 0.0,
            "t1_d1": t1_d1,
            "t1_d0": t1_d0,
            "t0_d1": t0_d1,
        },
        "plot_percentages": percentages,
        "raw_distribution": [
            {"role": role, "strategy": strategy, "count": int(count)}
            for role, strategy, count in role_rows
        ],
        "caida_ripe_agreement": [
            {"category": category, "count": int(count), "percentage": float(percentage)}
            for category, count, percentage in agreement_rows
        ],
        "outputs": {
            key: (None if value is None else str(value)) for key, value in asdict(outputs).items()
        },
    }
    outputs.role_json.parent.mkdir(parents=True, exist_ok=True)
    outputs.role_json.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return outputs
