"""RIPE Atlas observed-role distributions for resolved OS groups."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.network_role_utils import (
    NETWORK_ROLE_ANALYSIS_VERSION,
    log_cache_reuse,
    output_cache_is_current,
)
from ipid_analysis.plot_itdk_os import (
    _plot_roles,
    _role_plot_data,
    _source_metadata,
    _write_json,
    _write_query,
)
from ipid_analysis.plot_itdk_strategy import NO_TRANSIT_ROLE, TRANSIT_ROLE
from ipid_analysis.plot_os_group_strategy import (
    OS_GROUP_INPUT_NAME,
    OS_INPUT_NAME,
    PAPER_OS_GROUP_ORDER,
    write_os_groups,
)
from ipid_analysis.plot_ripe_atlas_strategy import render_ripe_atlas_analysis
from ipid_analysis.ripe_atlas import RipeAtlasDataset
from ipid_analysis.strategy_merge import StrategyMerge

KIND = "ripe-atlas-role-by-os"


@dataclass(frozen=True)
class RipeAtlasOSOutputs:
    joined: Path
    distribution: Path
    role_pdf: Path
    role_json: Path


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _artifact_paths(
    source: IpidMeasurement | StrategyMerge,
    *,
    processed_root: Path,
    figures_root: Path,
) -> RipeAtlasOSOutputs:
    return RipeAtlasOSOutputs(
        joined=source.artifact_path(processed_root, "ripe-atlas-os-role-join"),
        distribution=source.artifact_path(processed_root, "ripe-atlas-role-os-distribution"),
        role_pdf=source.artifact_path(figures_root, KIND, "pdf"),
        role_json=source.artifact_path(figures_root, KIND, "json"),
    )


def render_ripe_atlas_os_analysis(
    source: IpidMeasurement | StrategyMerge,
    dataset: RipeAtlasDataset,
    os_measurement_id: str,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    raw_root: Path = RAW_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    compression: str | None = "zstd",
    threads: int = 0,
) -> RipeAtlasOSOutputs:
    """Render OS distributions over the same matched-only RIPE role population."""
    strategies = source.artifact_path(processed_root, "strategies")
    os_path = raw_root / "os" / os_measurement_id / OS_INPUT_NAME
    group_path = processed_root / "os" / os_measurement_id / OS_GROUP_INPUT_NAME
    for path in (strategies, dataset.roles_path, os_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    group_stats = write_os_groups(
        os_path,
        group_path,
        compression=compression,
        threads=threads,
    )
    # This is cache-aware and guarantees that the shared role table has the
    # current matched-only schema even for direct calls outside postprocess.py.
    role_outputs = render_ripe_atlas_analysis(
        source,
        dataset,
        processed_root=processed_root,
        figures_root=figures_root,
        threads=threads,
    )
    role_matches = role_outputs.joined
    outputs = _artifact_paths(source, processed_root=processed_root, figures_root=figures_root)
    if output_cache_is_current(
        metadata_path=outputs.role_json,
        outputs=tuple(outputs.__dict__.values()),
        inputs=(strategies, role_matches, os_path, group_path),
    ):
        log_cache_reuse(source.target, outputs.role_json)
        return outputs

    con = duckdb.connect(config={"threads": threads} if threads else {})
    try:
        _write_query(
            con,
            f"""
            SELECT
                r.IP_ADDR,
                r.T,
                r.D,
                TRUE AS RIPE_MATCH,
                g.OS_GROUP,
                TRUE AS OS_RESOLVED,
                r.OBSERVED_ROLE
            FROM read_parquet('{_sql_path(role_matches)}') AS r
            INNER JOIN read_parquet('{_sql_path(group_path)}') AS g USING (IP_ADDR)
            """,
            outputs.joined,
        )
        _write_query(
            con,
            f"""
            WITH grouped AS (
                SELECT OBSERVED_ROLE, OS_GROUP, count(*)::BIGINT AS COUNT
                FROM read_parquet('{_sql_path(outputs.joined)}')
                GROUP BY OBSERVED_ROLE, OS_GROUP
            ), totals AS (
                SELECT OBSERVED_ROLE, sum(COUNT)::BIGINT AS ROLE_RESOLVED_TOTAL
                FROM grouped GROUP BY OBSERVED_ROLE
            )
            SELECT
                g.OBSERVED_ROLE,
                g.OS_GROUP,
                g.COUNT,
                t.ROLE_RESOLVED_TOTAL,
                100.0 * g.COUNT / t.ROLE_RESOLVED_TOTAL AS PERCENTAGE
            FROM grouped AS g
            INNER JOIN totals AS t USING (OBSERVED_ROLE)
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
            WITH population AS (
                SELECT DISTINCT CAST(IP_ADDR AS VARCHAR) AS IP_ADDR
                FROM read_parquet('{_sql_path(strategies)}')
            ), os_matches AS (
                SELECT p.IP_ADDR FROM population AS p
                INNER JOIN read_parquet('{_sql_path(group_path)}') AS g USING (IP_ADDR)
            ), role_matches AS (
                SELECT T FROM read_parquet('{_sql_path(role_matches)}')
            ), resolved_roles AS (
                SELECT T FROM read_parquet('{_sql_path(outputs.joined)}')
            )
            SELECT
                (SELECT count(*)::BIGINT FROM population),
                (SELECT count(*)::BIGINT FROM role_matches),
                (SELECT count(*)::BIGINT FROM os_matches),
                (SELECT count(*)::BIGINT FROM resolved_roles),
                (SELECT count(*) FILTER (WHERE T)::BIGINT FROM role_matches),
                (SELECT count(*) FILTER (WHERE T)::BIGINT FROM resolved_roles),
                (SELECT count(*) FILTER (WHERE NOT T)::BIGINT FROM role_matches),
                (SELECT count(*) FILTER (WHERE NOT T)::BIGINT FROM resolved_roles)
            """
        ).fetchone()
    finally:
        con.close()

    if not rows:
        raise ValueError(
            f"{source.target}: no resolved OS address matched the measured RIPE population"
        )
    data = _role_plot_data([(str(role), str(group), int(count)) for role, group, count in rows])
    _plot_roles(
        data,
        outputs.role_pdf,
        subject="RIPE Atlas traceroute roles and resolved OS groups",
    )
    (
        total,
        ripe_matched,
        os_resolved,
        ripe_os_resolved,
        transit_total,
        transit_os_resolved,
        no_transit_total,
        no_transit_os_resolved,
    ) = map(int, coverage)
    report = {
        **_source_metadata(source),
        "analysis_cache_version": NETWORK_ROLE_ANALYSIS_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ripe_atlas": {
            "source": dataset.source,
            "window_start": dataset.window.start.isoformat(),
            "window_end": dataset.window.end.isoformat(),
            "roles": str(dataset.roles_path),
        },
        "os": {
            "measurement_id": os_measurement_id,
            "source": str(os_path),
            "groups": str(group_path),
            "taxonomy": "canonical resolved OS_TAG mapped to OS_GROUP",
            **group_stats,
        },
        "role_definition": {
            TRANSIT_ROLE: "RIPE observation with T=true; D arbitrary",
            NO_TRANSIT_ROLE: "RIPE observation with T=false; for included addresses D=true",
        },
        "methodology": {
            "population": "measured IPs with a RIPE match and a resolved OS group",
            "normalization": "each observed-role bar is normalized independently to 100%",
            "legend_order": list(PAPER_OS_GROUP_ORDER),
            "unresolved_os": "excluded from the bars and reported in coverage",
        },
        "coverage": {
            "total_measured": total,
            "ripe_matched": ripe_matched,
            "ripe_unmatched": total - ripe_matched,
            "os_resolved": os_resolved,
            "ripe_os_resolved": ripe_os_resolved,
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
        "plot_percentages": data.percentages,
        "plot_counts": data.counts,
        "raw_distribution": [
            {"role": str(role), "os_group": str(group), "count": int(count)}
            for role, group, count in rows
        ],
        "artifact_semantics": {
            "joined": "matched RIPE addresses with a resolved OS group only",
        },
        "outputs": {key: str(value) for key, value in outputs.__dict__.items()},
    }
    _write_json(outputs.role_json, report)
    return outputs
