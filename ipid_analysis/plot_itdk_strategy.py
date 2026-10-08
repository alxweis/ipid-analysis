"""CAIDA ITDK role and cross-interface IP-ID strategy analyses."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import MultipleLocator  # noqa: E402

from ipid_analysis.caida_itdk import ITDKDataset  # noqa: E402
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR  # noqa: E402
from ipid_analysis.manifest import IpidMeasurement  # noqa: E402
from ipid_analysis.paper_figures import configure_paper_style  # noqa: E402
from ipid_analysis.strategies import (  # noqa: E402
    STRATEGY_COLORS,
    STRATEGY_NAMES,
    STRATEGY_PRETTY,
)
from ipid_analysis.strategy_merge import StrategyMerge  # noqa: E402

TRANSIT_ROLE = "Transit-Observed"
NO_TRANSIT_ROLE = "No Transit Evidence"
ROLE_ORDER = (TRANSIT_ROLE, NO_TRANSIT_ROLE)
NON_STRATEGIES = frozenset({"UNCLASSIFIED", "NOT_ENOUGH_SAMPLES"})
PLOT_STRATEGIES = tuple(name for name in STRATEGY_NAMES if name != "NOT_ENOUGH_SAMPLES")


@dataclass(frozen=True)
class ITDKAnalysisOutputs:
    joined: Path
    distribution: Path
    role_pdf: Path
    role_json: Path
    node_consistency: Path
    combinations: Path
    combinations_pdf: Path
    consistency_json: Path


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
        metadata["measurement_id"] = source.measurement_id
        metadata["interval"] = source.interval
        metadata["scale"] = source.scale
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
) -> ITDKAnalysisOutputs:
    return ITDKAnalysisOutputs(
        joined=source.artifact_path(processed_root, "itdk-strategy-join"),
        distribution=source.artifact_path(processed_root, "itdk-role-strategy-distribution"),
        role_pdf=source.artifact_path(figures_root, "itdk-role-by-strategy", "pdf"),
        role_json=source.artifact_path(figures_root, "itdk-role-by-strategy", "json"),
        node_consistency=source.artifact_path(processed_root, "itdk-node-consistency"),
        combinations=source.artifact_path(processed_root, "itdk-discordant-strategy-combinations"),
        combinations_pdf=source.artifact_path(
            figures_root, "itdk-discordant-strategy-combinations", "pdf"
        ),
        consistency_json=source.artifact_path(figures_root, "itdk-consistency-summary", "json"),
    )


def _role_plot_data(rows: list[tuple[str, str, int]]) -> dict[str, dict[str, float]]:
    counts = {role: {name: 0 for name in PLOT_STRATEGIES} for role in ROLE_ORDER}
    for role, strategy, count in rows:
        display_strategy = "UNCLASSIFIED" if strategy in NON_STRATEGIES else strategy
        if role in counts and display_strategy in counts[role]:
            counts[role][display_strategy] += int(count)
    percentages: dict[str, dict[str, float]] = {}
    for role in ROLE_ORDER:
        total = sum(counts[role].values())
        percentages[role] = {
            name: (100.0 * count / total if total else 0.0) for name, count in counts[role].items()
        }
    return percentages


def _plot_roles(percentages: dict[str, dict[str, float]], output: Path) -> None:
    configure_paper_style()
    fig, ax = plt.subplots(figsize=(7.0, 1.75))
    plot_roles = (NO_TRANSIT_ROLE, TRANSIT_ROLE)
    y_positions = range(len(plot_roles))
    used = [
        name
        for name in PLOT_STRATEGIES
        if any(percentages[role].get(name, 0.0) > 0 for role in plot_roles)
    ]
    for y, role in zip(y_positions, plot_roles):
        left = 0.0
        for strategy in used:
            value = percentages[role].get(strategy, 0.0)
            ax.barh(
                y,
                value,
                left=left,
                height=0.52,
                color=STRATEGY_COLORS[strategy],
                edgecolor="none",
                label=STRATEGY_PRETTY[strategy] if y == 0 else None,
            )
            if value >= 2.0:
                ax.text(
                    left + value / 2.0,
                    y,
                    f"{value:.0f}",
                    ha="center",
                    va="center",
                    fontsize=9,
                )
            left += value
    ax.set_xlim(0, 100)
    ax.set_yticks(list(y_positions))
    ax.set_yticklabels(["No Transit\nEvidence", "Transit-Observed"])
    ax.set_xlabel("IP-ID Selection Strategy [%]")
    ax.set_ylabel("Observed Network Role")
    ax.xaxis.set_minor_locator(MultipleLocator(5))
    ax.grid(axis="x", linestyle="--", linewidth=0.45, alpha=0.5)
    ax.set_axisbelow(True)
    if used:
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(
            handles,
            labels,
            loc="lower center",
            bbox_to_anchor=(0.5, 1.02),
            ncol=min(5, len(used)),
            frameon=False,
            handlelength=1.1,
            columnspacing=0.9,
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, bbox_inches="tight", pad_inches=0.025)
    plt.close(fig)


def _pretty_combination(value: str) -> str:
    return " + ".join(STRATEGY_PRETTY.get(part, part) for part in value.split(" + "))


def _plot_combinations(rows: list[tuple[str, int, float]], output: Path) -> None:
    configure_paper_style()
    top = rows[:10]
    if len(rows) > 10:
        top.append(
            (
                "Other",
                sum(int(row[1]) for row in rows[10:]),
                sum(float(row[2]) for row in rows[10:]),
            )
        )
    height = max(2.0, 0.35 * max(len(top), 1) + 0.65)
    fig, ax = plt.subplots(figsize=(6.0, height))
    if top:
        labels = [_pretty_combination(str(row[0])) for row in reversed(top)]
        percentages = [float(row[2]) for row in reversed(top)]
        bars = ax.barh(range(len(top)), percentages, color="#799ABE", height=0.65)
        ax.set_yticks(range(len(top)))
        ax.set_yticklabels(labels)
        ax.set_xlabel("Discordant Nodes [%]")
        ax.set_xlim(0, max(percentages) * 1.18 if percentages else 1)
        for bar, value in zip(bars, percentages):
            ax.text(
                bar.get_width(),
                bar.get_y() + bar.get_height() / 2,
                f" {value:.1f}",
                va="center",
                fontsize=9,
            )
    else:
        ax.text(0.5, 0.5, "No discordant multi-interface nodes", ha="center", va="center")
        ax.set_axis_off()
    ax.spines[["top", "right"]].set_visible(False)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, bbox_inches="tight", pad_inches=0.025)
    plt.close(fig)


def render_itdk_analysis(
    source: IpidMeasurement | StrategyMerge,
    dataset: ITDKDataset,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    threads: int = 0,
) -> ITDKAnalysisOutputs:
    """Join one strategy result to ITDK and render all role/node artifacts."""
    strategies = source.artifact_path(processed_root, "strategies")
    if not strategies.is_file():
        raise FileNotFoundError(strategies)
    if not dataset.interfaces_path.is_file():
        raise FileNotFoundError(dataset.interfaces_path)
    outputs = _artifact_paths(
        source,
        processed_root=processed_root,
        figures_root=figures_root,
    )
    con = duckdb.connect(config={"threads": threads} if threads else {})
    strategy_path = _sql_path(strategies)
    interface_path = _sql_path(dataset.interfaces_path)
    joined_path = _sql_path(outputs.joined)
    node_path = _sql_path(outputs.node_consistency)
    try:
        _write_query(
            con,
            f"""
            SELECT
                s.IP_ADDR,
                CAST(s.IPID_SELECTION_STRATEGY AS VARCHAR) AS IPID_SELECTION_STRATEGY,
                i.NODE_ID,
                i.T,
                i.D,
                i.IP_ADDR IS NOT NULL AS ITDK_MATCH,
                CASE
                    WHEN i.IP_ADDR IS NULL THEN NULL
                    WHEN i.T THEN '{TRANSIT_ROLE}'
                    ELSE '{NO_TRANSIT_ROLE}'
                END AS OBSERVED_ROLE
            FROM read_parquet('{strategy_path}') AS s
            LEFT JOIN read_parquet('{interface_path}') AS i USING (IP_ADDR)
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
            WHERE ITDK_MATCH
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
                count(*)::BIGINT AS total,
                count(*) FILTER (WHERE ITDK_MATCH)::BIGINT AS matched,
                count(*) FILTER (WHERE NOT ITDK_MATCH)::BIGINT AS unmatched,
                count(*) FILTER (WHERE ITDK_MATCH AND T AND D)::BIGINT AS t1_d1,
                count(*) FILTER (WHERE ITDK_MATCH AND T AND NOT D)::BIGINT AS t1_d0,
                count(*) FILTER (WHERE ITDK_MATCH AND NOT T AND D)::BIGINT AS t0_d1,
                count(*) FILTER (WHERE ITDK_MATCH AND NOT T AND NOT D)::BIGINT AS t0_d0
            FROM read_parquet('{joined_path}')
            """
        ).fetchone()
        if not coverage[1]:
            raise ValueError(f"{source.target}: no measured IP address matched CAIDA ITDK")

        _write_query(
            con,
            f"""
            WITH classified AS (
                SELECT NODE_ID, IP_ADDR, IPID_SELECTION_STRATEGY, T
                FROM read_parquet('{joined_path}')
                WHERE ITDK_MATCH
                  AND NODE_ID IS NOT NULL
                  AND IPID_SELECTION_STRATEGY NOT IN ('UNCLASSIFIED', 'NOT_ENOUGH_SAMPLES')
            ), strategy_counts AS (
                SELECT
                    NODE_ID,
                    IPID_SELECTION_STRATEGY,
                    count(DISTINCT IP_ADDR)::BIGINT AS INTERFACES,
                    bool_or(T) AS STRATEGY_HAS_TRANSIT
                FROM classified
                GROUP BY NODE_ID, IPID_SELECTION_STRATEGY
            )
            SELECT
                NODE_ID,
                sum(INTERFACES)::BIGINT AS CLASSIFIED_INTERFACES,
                count(*)::BIGINT AS STRATEGY_COUNT,
                bool_or(STRATEGY_HAS_TRANSIT) AS HAS_TRANSIT_EVIDENCE,
                array_to_string(
                    list_sort(list(IPID_SELECTION_STRATEGY)), ' + '
                ) AS STRATEGY_SET,
                sum(INTERFACES * (INTERFACES - 1) / 2)::DOUBLE
                    / (sum(INTERFACES) * (sum(INTERFACES) - 1) / 2)
                    AS PAIRWISE_AGREEMENT
            FROM strategy_counts
            GROUP BY NODE_ID
            HAVING sum(INTERFACES) >= 2
            ORDER BY NODE_ID
            """,
            outputs.node_consistency,
        )
        _write_query(
            con,
            f"""
            WITH discordant AS (
                SELECT STRATEGY_SET
                FROM read_parquet('{node_path}')
                WHERE STRATEGY_COUNT > 1
            ), combinations AS (
                SELECT STRATEGY_SET, count(*)::BIGINT AS NODES
                FROM discordant
                GROUP BY STRATEGY_SET
            )
            SELECT
                STRATEGY_SET,
                NODES,
                100.0 * NODES / sum(NODES) OVER () AS PERCENTAGE
            FROM combinations
            ORDER BY NODES DESC, STRATEGY_SET
            """,
            outputs.combinations,
        )

        node_counts = con.execute(
            f"""
            SELECT
                count(*)::BIGINT AS eligible,
                count(*) FILTER (WHERE STRATEGY_COUNT = 1)::BIGINT AS strict,
                avg(PAIRWISE_AGREEMENT)::DOUBLE AS mean_pairwise,
                count(*) FILTER (WHERE HAS_TRANSIT_EVIDENCE)::BIGINT AS transit_eligible,
                count(*) FILTER (
                    WHERE HAS_TRANSIT_EVIDENCE AND STRATEGY_COUNT = 1
                )::BIGINT AS transit_strict
            FROM read_parquet('{node_path}')
            """
        ).fetchone()
        matched_multi_nodes = con.execute(
            f"""
            SELECT count(*)::BIGINT
            FROM (
                SELECT NODE_ID
                FROM read_parquet('{joined_path}')
                WHERE ITDK_MATCH AND NODE_ID IS NOT NULL
                GROUP BY NODE_ID
                HAVING count(DISTINCT IP_ADDR) >= 2
            )
            """
        ).fetchone()[0]
        combination_rows = con.execute(
            f"""
            SELECT STRATEGY_SET, NODES, PERCENTAGE
            FROM read_parquet('{_sql_path(outputs.combinations)}')
            ORDER BY NODES DESC, STRATEGY_SET
            """
        ).fetchall()
    finally:
        con.close()

    percentages = _role_plot_data(role_rows)
    _plot_roles(percentages, outputs.role_pdf)
    _plot_combinations(combination_rows, outputs.combinations_pdf)

    total, matched, unmatched, t1_d1, t1_d0, t0_d1, t0_d0 = map(int, coverage)
    role_report = {
        **_source_metadata(source),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "itdk": {
            "release": dataset.release,
            "topology": dataset.topology,
            "interfaces": str(dataset.interfaces_path),
            "metadata": str(dataset.metadata_path),
        },
        "role_definition": {
            TRANSIT_ROLE: "ITDK match with T=true; D arbitrary",
            NO_TRANSIT_ROLE: "ITDK match with T=false; D arbitrary",
        },
        "coverage": {
            "total": total,
            "matched": matched,
            "unmatched": unmatched,
            "match_percentage": 100.0 * matched / total if total else 0.0,
            "t1_d1": t1_d1,
            "t1_d0": t1_d0,
            "t0_d1": t0_d1,
            "t0_d0": t0_d0,
        },
        "plot_percentages": percentages,
        "raw_distribution": [
            {"role": role, "strategy": strategy, "count": int(count)}
            for role, strategy, count in role_rows
        ],
    }
    outputs.role_json.parent.mkdir(parents=True, exist_ok=True)
    outputs.role_json.write_text(json.dumps(role_report, indent=2) + "\n", encoding="utf-8")

    eligible, strict, mean_pairwise, transit_eligible, transit_strict = node_counts
    eligible = int(eligible or 0)
    strict = int(strict or 0)
    transit_eligible = int(transit_eligible or 0)
    transit_strict = int(transit_strict or 0)
    consistency_report = {
        **_source_metadata(source),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "itdk_release": dataset.release,
        "itdk_topology": dataset.topology,
        "matched_multi_interface_nodes": int(matched_multi_nodes),
        "eligible_nodes_with_two_classified_interfaces": eligible,
        "excluded_for_classification_coverage": max(int(matched_multi_nodes) - eligible, 0),
        "strict_agreement_nodes": strict,
        "strict_agreement_percentage": 100.0 * strict / eligible if eligible else 0.0,
        "mean_node_pairwise_agreement": (
            float(mean_pairwise)
            if mean_pairwise is not None and math.isfinite(mean_pairwise)
            else 0.0
        ),
        "transit_evidenced_eligible_nodes": transit_eligible,
        "transit_evidenced_strict_nodes": transit_strict,
        "transit_evidenced_strict_percentage": (
            100.0 * transit_strict / transit_eligible if transit_eligible else 0.0
        ),
        "discordant_nodes": eligible - strict,
        "outputs": {key: str(value) for key, value in asdict(outputs).items()},
    }
    outputs.consistency_json.parent.mkdir(parents=True, exist_ok=True)
    outputs.consistency_json.write_text(
        json.dumps(consistency_report, indent=2) + "\n", encoding="utf-8"
    )
    return outputs
