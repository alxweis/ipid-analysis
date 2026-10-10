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
import numpy as np

from ipid_analysis.caida_itdk import ITDKDataset
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.network_role_utils import (
    NETWORK_ROLE_ANALYSIS_VERSION,
    log_cache_reuse,
    output_cache_is_current,
    role_count_label,
)
from ipid_analysis.paper_figures import configure_paper_style
from ipid_analysis.ripe_atlas import RipeAtlasDataset
from ipid_analysis.strategies import (
    PAPER_STRATEGY_ORDER,
    STRATEGY_COLORS,
    STRATEGY_PRETTY,
)
from ipid_analysis.strategy_merge import StrategyMerge

TRANSIT_ROLE = "Transit-Observed"
NO_TRANSIT_ROLE = "No Transit Evidence"
# Backward-compatible symbol for downstream imports; figures use the common label.
DESTINATION_ROLE = NO_TRANSIT_ROLE
ROLE_ORDER = (TRANSIT_ROLE, NO_TRANSIT_ROLE)
NON_STRATEGIES = frozenset({"UNCLASSIFIED", "NOT_ENOUGH_SAMPLES"})
PLOT_STRATEGIES = tuple(name for name in PAPER_STRATEGY_ORDER if name != "NOT_ENOUGH_SAMPLES")


@dataclass(frozen=True)
class RipeAtlasAnalysisOutputs:
    joined: Path
    distribution: Path
    role_pdf: Path
    role_json: Path
    caida_agreement: Path | None
    caida_agreement_pdf: Path | None
    caida_intersection: Path | None
    caida_intersection_pdf: Path | None
    caida_intersection_json: Path | None


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
        caida_agreement_pdf=(
            source.artifact_path(figures_root, "caida-ripe-role-agreement", "pdf")
            if with_caida
            else None
        ),
        caida_intersection=(
            source.artifact_path(processed_root, "caida-ripe-intersection-by-strategy")
            if with_caida
            else None
        ),
        caida_intersection_pdf=(
            source.artifact_path(figures_root, "caida-ripe-intersection-role-by-strategy", "pdf")
            if with_caida
            else None
        ),
        caida_intersection_json=(
            source.artifact_path(figures_root, "caida-ripe-intersection-role-by-strategy", "json")
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


def _plot_roles(
    percentages: dict[str, dict[str, float]],
    role_totals: dict[str, int],
    output: Path,
) -> None:
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
        [
            role_count_label("Transit-Observed", role_totals.get(TRANSIT_ROLE, 0)),
            role_count_label("No Transit Evidence", role_totals.get(NO_TRANSIT_ROLE, 0)),
        ],
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


def _plot_agreement(rows: list[tuple[str, int, float]], output: Path) -> None:
    categories = {
        str(category): (int(count), float(percentage)) for category, count, percentage in rows
    }
    cells = (
        ("Neither Transit-Observed", "CAIDA Only Transit-Observed"),
        ("RIPE Only Transit-Observed", "Both Transit-Observed"),
    )
    values = np.asarray([[categories.get(name, (0, 0.0))[1] for name in row] for row in cells])
    configure_paper_style()
    fig, ax = plt.subplots(figsize=(4.15, 3.15))
    image = ax.imshow(values, cmap="Blues", vmin=0, vmax=max(100.0, float(values.max())))
    for y, row in enumerate(cells):
        for x, name in enumerate(row):
            count, percentage = categories.get(name, (0, 0.0))
            ax.text(
                x,
                y,
                f"{percentage:.1f}%\n(n={count:,})",
                ha="center",
                va="center",
                color="white" if percentage >= 50.0 else "#111111",
                fontsize=9,
            )
    ax.set_xticks([0, 1], ["No Transit\nEvidence", "Transit-Observed"])
    ax.set_yticks([0, 1], ["No Transit\nEvidence", "Transit-Observed"])
    ax.set_xlabel("CAIDA ITDK")
    ax.set_ylabel("RIPE Atlas")
    for spine in ax.spines.values():
        spine.set_visible(False)
    colorbar = fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
    colorbar.set_label("Common addresses [%]")
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)


def _plot_intersection(
    rows: list[tuple[str, str, str, int]],
    output: Path,
) -> tuple[dict[str, dict[str, float]], dict[str, int]]:
    labels = (
        ("CAIDA ITDK", TRANSIT_ROLE),
        ("CAIDA ITDK", NO_TRANSIT_ROLE),
        ("RIPE Atlas", TRANSIT_ROLE),
        ("RIPE Atlas", NO_TRANSIT_ROLE),
    )
    counts = {label: {strategy: 0 for strategy in PLOT_STRATEGIES} for label in labels}
    for source_name, role, strategy, count in rows:
        label = (str(source_name), str(role))
        display = "UNCLASSIFIED" if strategy in NON_STRATEGIES else str(strategy)
        if label in counts and display in counts[label]:
            counts[label][display] += int(count)
    totals = {f"{source}|{role}": sum(counts[(source, role)].values()) for source, role in labels}
    percentages = {
        f"{source}|{role}": {
            strategy: (
                100.0 * value / sum(counts[(source, role)].values())
                if sum(counts[(source, role)].values())
                else 0.0
            )
            for strategy, value in counts[(source, role)].items()
        }
        for source, role in labels
    }
    configure_paper_style()
    fig, ax = plt.subplots(figsize=(7.16, 3.15))
    y_positions = {label: float(len(labels) - index - 1) for index, label in enumerate(labels)}
    for source_name, role in labels:
        key = f"{source_name}|{role}"
        left = 0.0
        for strategy in PLOT_STRATEGIES:
            value = percentages[key][strategy]
            if value <= 0:
                continue
            ax.barh(
                y_positions[(source_name, role)],
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
                    y_positions[(source_name, role)],
                    _label_percentage(value),
                    ha="center",
                    va="center",
                    fontsize=9,
                )
            left += value
    ax.set_xlim(0, 100)
    ax.set_yticks(
        [y_positions[label] for label in labels],
        [
            role_count_label(f"{source_name}: {role}", totals[f"{source_name}|{role}"])
            for source_name, role in labels
        ],
    )
    ax.set_xlabel("IP-ID Selection Strategy [%]")
    ax.set_ylabel("Data Source and Observed Role")
    ax.xaxis.set_major_locator(MultipleLocator(20))
    ax.xaxis.set_minor_locator(MultipleLocator(5))
    ax.grid(axis="x", which="major", color="#BDBDBD", linestyle="--", linewidth=0.5)
    ax.set_axisbelow(True)
    handles = [
        Patch(facecolor=STRATEGY_COLORS[name], edgecolor="none", label=STRATEGY_PRETTY[name])
        for name in PLOT_STRATEGIES
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
    fig.subplots_adjust(left=0.29, right=0.995, bottom=0.19, top=0.75)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    return percentages, totals


def render_ripe_atlas_analysis(
    source: IpidMeasurement | StrategyMerge,
    dataset: RipeAtlasDataset,
    *,
    itdk: ITDKDataset | None = None,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    threads: int = 0,
) -> RipeAtlasAnalysisOutputs:
    """Join one strategy result to RIPE roles and render paper artifacts.

    The persisted join is deliberately matched-only. Coverage is computed from
    the input row count, so hundreds of millions of unmatched measurement rows
    are never copied into a second Parquet file.
    """
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
    cache_inputs = [strategies, dataset.roles_path]
    if itdk is not None:
        cache_inputs.append(itdk.interfaces_path)
    if output_cache_is_current(
        metadata_path=outputs.role_json,
        outputs=tuple(asdict(outputs).values()),
        inputs=cache_inputs,
    ):
        log_cache_reuse(source.target, outputs.role_json)
        return outputs
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
                TRUE AS RIPE_MATCH,
                CASE
                    WHEN r.T THEN '{TRANSIT_ROLE}'
                    ELSE '{NO_TRANSIT_ROLE}'
                END AS OBSERVED_ROLE
            FROM read_parquet('{_sql_path(strategies)}') AS s
            INNER JOIN read_parquet('{_sql_path(dataset.roles_path)}') AS r USING (IP_ADDR)
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
        match_counts = con.execute(
            f"""
            SELECT
                count(*)::BIGINT,
                count(*) FILTER (WHERE T AND D)::BIGINT,
                count(*) FILTER (WHERE T AND NOT D)::BIGINT,
                count(*) FILTER (WHERE NOT T AND D)::BIGINT
            FROM read_parquet('{joined_path}')
            """
        ).fetchone()
        total = int(
            con.execute(
                f"SELECT count(*)::BIGINT FROM read_parquet('{_sql_path(strategies)}')"
            ).fetchone()[0]
        )
        matched, t1_d1, t1_d0, t0_d1 = map(int, match_counts)
        coverage = (total, matched, total - matched, t1_d1, t1_d0, t0_d1)
        if not matched:
            raise ValueError(f"{source.target}: no measured IP address matched RIPE Atlas")

        agreement_rows = []
        intersection_rows = []
        if (
            itdk is not None
            and outputs.caida_agreement is not None
            and outputs.caida_intersection is not None
        ):
            _write_query(
                con,
                f"""
                WITH dual_matches AS (
                    SELECT j.IP_ADDR, j.T AS RIPE_T, i.T AS ITDK_T
                    FROM read_parquet('{joined_path}') AS j
                    JOIN read_parquet('{_sql_path(itdk.interfaces_path)}') AS i USING (IP_ADDR)
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
            _write_query(
                con,
                f"""
                WITH dual_matches AS (
                    SELECT
                        j.IP_ADDR,
                        j.IPID_SELECTION_STRATEGY,
                        j.T AS RIPE_T,
                        i.T AS ITDK_T
                    FROM read_parquet('{joined_path}') AS j
                    INNER JOIN read_parquet('{_sql_path(itdk.interfaces_path)}') AS i
                        USING (IP_ADDR)
                ), expanded AS (
                    SELECT
                        'CAIDA ITDK' AS SOURCE,
                        CASE WHEN ITDK_T THEN '{TRANSIT_ROLE}'
                             ELSE '{NO_TRANSIT_ROLE}' END AS OBSERVED_ROLE,
                        IPID_SELECTION_STRATEGY
                    FROM dual_matches
                    UNION ALL
                    SELECT
                        'RIPE Atlas' AS SOURCE,
                        CASE WHEN RIPE_T THEN '{TRANSIT_ROLE}'
                             ELSE '{NO_TRANSIT_ROLE}' END AS OBSERVED_ROLE,
                        IPID_SELECTION_STRATEGY
                    FROM dual_matches
                )
                SELECT
                    SOURCE,
                    OBSERVED_ROLE,
                    IPID_SELECTION_STRATEGY,
                    count(*)::BIGINT AS COUNT
                FROM expanded
                GROUP BY SOURCE, OBSERVED_ROLE, IPID_SELECTION_STRATEGY
                ORDER BY SOURCE, OBSERVED_ROLE, IPID_SELECTION_STRATEGY
                """,
                outputs.caida_intersection,
            )
            intersection_rows = con.execute(
                f"""
                SELECT SOURCE, OBSERVED_ROLE, IPID_SELECTION_STRATEGY, COUNT
                FROM read_parquet('{_sql_path(outputs.caida_intersection)}')
                ORDER BY SOURCE, OBSERVED_ROLE, IPID_SELECTION_STRATEGY
                """
            ).fetchall()
    finally:
        con.close()

    percentages = _role_plot_data(role_rows)
    role_totals = {
        role: sum(int(count) for row_role, _, count in role_rows if row_role == role)
        for role in ROLE_ORDER
    }
    _plot_roles(percentages, role_totals, outputs.role_pdf)
    intersection_percentages: dict[str, dict[str, float]] = {}
    intersection_totals: dict[str, int] = {}
    if outputs.caida_agreement_pdf is not None:
        _plot_agreement(agreement_rows, outputs.caida_agreement_pdf)
    if outputs.caida_intersection_pdf is not None:
        intersection_percentages, intersection_totals = _plot_intersection(
            intersection_rows, outputs.caida_intersection_pdf
        )
    if outputs.caida_intersection_json is not None:
        outputs.caida_intersection_json.parent.mkdir(parents=True, exist_ok=True)
        outputs.caida_intersection_json.write_text(
            json.dumps(
                {
                    **_source_metadata(source),
                    "analysis_cache_version": NETWORK_ROLE_ANALYSIS_VERSION,
                    "population": "addresses matched by both CAIDA ITDK and RIPE Atlas",
                    "plot_percentages": intersection_percentages,
                    "plot_role_totals": intersection_totals,
                    "raw_distribution": [
                        {
                            "source": str(source_name),
                            "role": str(role),
                            "strategy": str(strategy),
                            "count": int(count),
                        }
                        for source_name, role, strategy, count in intersection_rows
                    ],
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    total, matched, unmatched, t1_d1, t1_d0, t0_d1 = map(int, coverage)
    report = {
        **_source_metadata(source),
        "analysis_cache_version": NETWORK_ROLE_ANALYSIS_VERSION,
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
            NO_TRANSIT_ROLE: (
                "RIPE observation with T=false; every included RIPE address has T or D, "
                "therefore this group has D=true"
            ),
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
        "plot_role_totals": role_totals,
        "raw_distribution": [
            {"role": role, "strategy": strategy, "count": int(count)}
            for role, strategy, count in role_rows
        ],
        "caida_ripe_agreement": [
            {"category": category, "count": int(count), "percentage": float(percentage)}
            for category, count, percentage in agreement_rows
        ],
        "artifact_semantics": {
            "joined": "matched-only measured addresses; unmatched addresses are not materialized",
            "caida_intersection": "addresses matched by both CAIDA ITDK and RIPE Atlas",
        },
        "outputs": {
            key: (None if value is None else str(value)) for key, value in asdict(outputs).items()
        },
    }
    outputs.role_json.parent.mkdir(parents=True, exist_ok=True)
    outputs.role_json.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return outputs
