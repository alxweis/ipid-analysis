"""CAIDA ITDK observed-role distributions for resolved OS groups."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")

from matplotlib.patches import Patch  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import MultipleLocator  # noqa: E402

from ipid_analysis.caida_itdk import ITDKDataset  # noqa: E402
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR  # noqa: E402
from ipid_analysis.manifest import IpidMeasurement  # noqa: E402
from ipid_analysis.paper_figures import configure_paper_style  # noqa: E402
from ipid_analysis.plot_itdk_strategy import (  # noqa: E402
    NO_TRANSIT_ROLE,
    ROLE_ORDER,
    TRANSIT_ROLE,
)
from ipid_analysis.plot_os_group_strategy import (  # noqa: E402
    GROUP_INFO,
    OS_GROUP_INPUT_NAME,
    OS_INPUT_NAME,
    PAPER_OS_GROUP_COLORS,
    PAPER_OS_GROUP_ORDER,
    PAPER_OS_OTHER_COLOR,
    write_os_groups,
)
from ipid_analysis.strategy_merge import StrategyMerge  # noqa: E402

KIND = "itdk-role-by-os"
MAX_LEGEND_GROUPS = 10
MAX_NAMED_GROUPS = MAX_LEGEND_GROUPS - 1
OTHER_GROUP = "__other__"
OTHER_LABEL = "Other"


@dataclass(frozen=True)
class ITDKOSOutputs:
    joined: Path
    distribution: Path
    role_pdf: Path
    role_json: Path


@dataclass(frozen=True)
class OSRolePlotData:
    groups: tuple[str, ...]
    percentages: dict[str, dict[str, float]]
    counts: dict[str, dict[str, int]]
    role_totals: dict[str, int]


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _write_query(con: duckdb.DuckDBPyConnection, query: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_suffix(output.suffix + ".part")
    partial.unlink(missing_ok=True)
    con.execute(f"COPY ({query}) TO '{_sql_path(partial)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    partial.replace(output)


def _write_json(output: Path, value: dict) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_suffix(output.suffix + ".part")
    partial.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    partial.replace(output)


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


def _artifact_paths(
    source: IpidMeasurement | StrategyMerge,
    *,
    processed_root: Path,
    figures_root: Path,
) -> ITDKOSOutputs:
    return ITDKOSOutputs(
        joined=source.artifact_path(processed_root, "itdk-os-role-join"),
        distribution=source.artifact_path(processed_root, "itdk-role-os-distribution"),
        role_pdf=source.artifact_path(figures_root, KIND, "pdf"),
        role_json=source.artifact_path(figures_root, KIND, "json"),
    )


def _paper_order_index(group: str) -> int:
    try:
        return PAPER_OS_GROUP_ORDER.index(group)
    except ValueError:
        return len(PAPER_OS_GROUP_ORDER)


def _select_groups(
    counts: dict[str, dict[str, int]],
    role_totals: dict[str, int],
) -> tuple[str, ...]:
    represented = {
        group for role in ROLE_ORDER for group, count in counts[role].items() if count > 0
    }
    if len(represented) <= MAX_LEGEND_GROUPS:
        return tuple(sorted(represented, key=_paper_order_index))

    def mean_role_percentage(group: str) -> float:
        shares = [
            counts[role].get(group, 0) / role_totals[role]
            for role in ROLE_ORDER
            if role_totals[role]
        ]
        return sum(shares) / len(shares) if shares else 0.0

    strongest = sorted(
        represented,
        key=lambda group: (-mean_role_percentage(group), _paper_order_index(group)),
    )[:MAX_NAMED_GROUPS]
    return (*sorted(strongest, key=_paper_order_index), OTHER_GROUP)


def _role_plot_data(rows: list[tuple[str, str, int]]) -> OSRolePlotData:
    counts = {role: {} for role in ROLE_ORDER}
    for role, group, count in rows:
        if role not in counts or group not in GROUP_INFO:
            continue
        counts[role][group] = counts[role].get(group, 0) + int(count)
    role_totals = {role: sum(counts[role].values()) for role in ROLE_ORDER}
    groups = _select_groups(counts, role_totals)
    percentages = {role: {} for role in ROLE_ORDER}
    plot_counts = {role: {} for role in ROLE_ORDER}
    named = set(groups) - {OTHER_GROUP}
    for role in ROLE_ORDER:
        for group in groups:
            if group == OTHER_GROUP:
                count = sum(value for name, value in counts[role].items() if name not in named)
            else:
                count = counts[role].get(group, 0)
            plot_counts[role][group] = count
            percentages[role][group] = (
                100.0 * count / role_totals[role] if role_totals[role] else 0.0
            )
    return OSRolePlotData(
        groups=groups,
        percentages=percentages,
        counts=plot_counts,
        role_totals=role_totals,
    )


def _group_label(group: str) -> str:
    return OTHER_LABEL if group == OTHER_GROUP else GROUP_INFO[group][1]


def _group_color(group: str) -> str:
    return PAPER_OS_OTHER_COLOR if group == OTHER_GROUP else PAPER_OS_GROUP_COLORS[group]


def _label_percentage(value: float) -> str:
    if value < 1.0:
        return f"{value:.1f}"
    return f"{value:.0f}"


def _plot_roles(data: OSRolePlotData, output: Path) -> None:
    configure_paper_style()
    fig, ax = plt.subplots(figsize=(7.16, 2.45))
    plot_roles = (TRANSIT_ROLE, NO_TRANSIT_ROLE)
    y_positions = {
        role: float(len(plot_roles) - index - 1) for index, role in enumerate(plot_roles)
    }
    for role in plot_roles:
        left = 0.0
        for group in data.groups:
            value = data.percentages[role].get(group, 0.0)
            if value <= 0:
                continue
            ax.barh(
                y_positions[role],
                value,
                left=left,
                height=0.38,
                color=_group_color(group),
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
    ax.set_ylim(-0.52, len(plot_roles) - 0.48)
    ax.set_yticks(
        [y_positions[role] for role in plot_roles],
        ["Transit-Observed", "No Transit\nEvidence"],
    )
    ax.set_xlabel("OS Distribution [%]")
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
        Patch(facecolor=_group_color(group), edgecolor="none", label=_group_label(group))
        for group in data.groups
    ]
    if handles:
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
            "Title": "Observed network role by operating-system distribution",
            "Subject": "CAIDA ITDK transit evidence and resolved OS groups",
            "Creator": "ipid-analysis",
        },
    )
    plt.close(fig)


def render_itdk_os_analysis(
    source: IpidMeasurement | StrategyMerge,
    dataset: ITDKDataset,
    os_measurement_id: str,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    raw_root: Path = RAW_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    compression: str | None = "zstd",
    threads: int = 0,
) -> ITDKOSOutputs:
    """Join one measured population to ITDK and resolved protocol OS groups."""
    strategies = source.artifact_path(processed_root, "strategies")
    os_path = raw_root / "os" / os_measurement_id / OS_INPUT_NAME
    group_path = processed_root / "os" / os_measurement_id / OS_GROUP_INPUT_NAME
    for path in (strategies, dataset.interfaces_path, os_path):
        if not path.is_file():
            raise FileNotFoundError(path)
    group_stats = write_os_groups(
        os_path,
        group_path,
        compression=compression,
        threads=threads,
    )
    outputs = _artifact_paths(
        source,
        processed_root=processed_root,
        figures_root=figures_root,
    )
    con = duckdb.connect(config={"threads": threads} if threads else {})
    try:
        _write_query(
            con,
            f"""
            WITH population AS (
                SELECT DISTINCT CAST(IP_ADDR AS VARCHAR) AS IP_ADDR
                FROM read_parquet('{_sql_path(strategies)}')
            )
            SELECT
                p.IP_ADDR,
                i.NODE_ID,
                i.T,
                i.D,
                i.IP_ADDR IS NOT NULL AS ITDK_MATCH,
                g.OS_GROUP,
                g.OS_GROUP IS NOT NULL AS OS_RESOLVED,
                CASE
                    WHEN i.IP_ADDR IS NULL THEN NULL
                    WHEN i.T THEN '{TRANSIT_ROLE}'
                    ELSE '{NO_TRANSIT_ROLE}'
                END AS OBSERVED_ROLE
            FROM population AS p
            LEFT JOIN read_parquet('{_sql_path(dataset.interfaces_path)}') AS i USING (IP_ADDR)
            LEFT JOIN read_parquet('{_sql_path(group_path)}') AS g USING (IP_ADDR)
            """,
            outputs.joined,
        )
        _write_query(
            con,
            f"""
            WITH grouped AS (
                SELECT
                    OBSERVED_ROLE,
                    OS_GROUP,
                    count(*)::BIGINT AS COUNT
                FROM read_parquet('{_sql_path(outputs.joined)}')
                WHERE ITDK_MATCH AND OS_RESOLVED
                GROUP BY OBSERVED_ROLE, OS_GROUP
            ),
            role_totals AS (
                SELECT
                    OBSERVED_ROLE,
                    sum(COUNT)::BIGINT AS ROLE_RESOLVED_TOTAL
                FROM grouped
                GROUP BY OBSERVED_ROLE
            )
            SELECT
                g.OBSERVED_ROLE,
                g.OS_GROUP,
                g.COUNT,
                t.ROLE_RESOLVED_TOTAL,
                100.0 * g.COUNT / t.ROLE_RESOLVED_TOTAL AS PERCENTAGE
            FROM grouped AS g
            JOIN role_totals AS t USING (OBSERVED_ROLE)
            ORDER BY g.OBSERVED_ROLE, g.OS_GROUP
            """,
            outputs.distribution,
        )
        rows = con.execute(
            f"""
            SELECT OBSERVED_ROLE, OS_GROUP, COUNT
            FROM read_parquet('{_sql_path(outputs.distribution)}')
            ORDER BY OBSERVED_ROLE, OS_GROUP
            """
        ).fetchall()
        coverage = con.execute(
            f"""
            SELECT
                count(*)::BIGINT AS total,
                count(*) FILTER (WHERE ITDK_MATCH)::BIGINT AS itdk_matched,
                count(*) FILTER (WHERE NOT ITDK_MATCH)::BIGINT AS itdk_unmatched,
                count(*) FILTER (WHERE OS_RESOLVED)::BIGINT AS os_resolved,
                count(*) FILTER (WHERE ITDK_MATCH AND OS_RESOLVED)::BIGINT
                    AS itdk_os_resolved,
                count(*) FILTER (WHERE ITDK_MATCH AND T)::BIGINT AS transit_total,
                count(*) FILTER (WHERE ITDK_MATCH AND T AND OS_RESOLVED)::BIGINT
                    AS transit_os_resolved,
                count(*) FILTER (WHERE ITDK_MATCH AND NOT T)::BIGINT AS no_transit_total,
                count(*) FILTER (WHERE ITDK_MATCH AND NOT T AND OS_RESOLVED)::BIGINT
                    AS no_transit_os_resolved
            FROM read_parquet('{_sql_path(outputs.joined)}')
            """
        ).fetchone()
    finally:
        con.close()

    if not rows:
        raise ValueError(
            f"{source.target}: no resolved OS address matched the measured ITDK population"
        )
    data = _role_plot_data([(str(role), str(group), int(count)) for role, group, count in rows])
    _plot_roles(data, outputs.role_pdf)

    (
        total,
        itdk_matched,
        itdk_unmatched,
        os_resolved,
        itdk_os_resolved,
        transit_total,
        transit_os_resolved,
        no_transit_total,
        no_transit_os_resolved,
    ) = map(int, coverage)
    report = {
        **_source_metadata(source),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "itdk": {
            "release": dataset.release,
            "topology": dataset.topology,
            "interfaces": str(dataset.interfaces_path),
            "metadata": str(dataset.metadata_path),
        },
        "os": {
            "measurement_id": os_measurement_id,
            "source": str(os_path),
            "groups": str(group_path),
            "taxonomy": "canonical resolved OS_TAG mapped to OS_GROUP",
            **group_stats,
        },
        "role_definition": {
            TRANSIT_ROLE: "ITDK match with T=true; D arbitrary",
            NO_TRANSIT_ROLE: "ITDK match with T=false; D arbitrary",
        },
        "methodology": {
            "population": "measured IPs with an ITDK match and a resolved OS group",
            "normalization": "each observed-role bar is normalized independently to 100%",
            "legend_order": list(PAPER_OS_GROUP_ORDER),
            "selection": (
                "all groups when at most 10 are represented; otherwise the 9 groups with "
                "the largest mean role-normalized share plus Other"
            ),
            "unresolved_os": "excluded from the bars and reported in coverage",
        },
        "coverage": {
            "total_measured": total,
            "itdk_matched": itdk_matched,
            "itdk_unmatched": itdk_unmatched,
            "os_resolved": os_resolved,
            "itdk_os_resolved": itdk_os_resolved,
            "transit_observed_total": transit_total,
            "transit_observed_os_resolved": transit_os_resolved,
            "transit_observed_os_coverage_percentage": (
                100.0 * transit_os_resolved / transit_total if transit_total else 0.0
            ),
            "no_transit_evidence_total": no_transit_total,
            "no_transit_evidence_os_resolved": no_transit_os_resolved,
            "no_transit_evidence_os_coverage_percentage": (
                100.0 * no_transit_os_resolved / no_transit_total if no_transit_total else 0.0
            ),
        },
        "plot_groups": [
            {
                "group": group,
                "label": _group_label(group),
                "color": _group_color(group),
            }
            for group in data.groups
        ],
        "plot_percentages": data.percentages,
        "plot_counts": data.counts,
        "raw_distribution": [
            {"role": str(role), "os_group": str(group), "count": int(count)}
            for role, group, count in rows
        ],
        "outputs": {
            "joined": str(outputs.joined),
            "distribution": str(outputs.distribution),
            "pdf": str(outputs.role_pdf),
        },
    }
    _write_json(outputs.role_json, report)
    return outputs
