"""IP-ID measurement coverage for plot metadata."""

from pathlib import Path

import duckdb

from ipid_analysis.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement


def ipid_measurement_coverage(strategies_path: Path, zmap_path: Path) -> float:
    """Return distinct strategy-result IPs as a percentage of distinct ZMap IPs."""
    with duckdb.connect() as con:
        result_count, zmap_count = con.execute(
            """
            SELECT
                (SELECT count(DISTINCT IP_ADDR) FROM read_parquet($strategies)),
                (SELECT count(DISTINCT IP_ADDR) FROM read_parquet($zmap))
            """,
            {"strategies": str(strategies_path), "zmap": str(zmap_path)},
        ).fetchone()
    return 100.0 * result_count / zmap_count if zmap_count else 0.0


def coverage_for_measurement(
    measurement: IpidMeasurement,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    raw_root: Path = RAW_DATA_DIR,
) -> float:
    """Resolve one manifest measurement to its strategy and ZMap coverage inputs."""
    if not measurement.zmap_id:
        raise ValueError(f"{measurement.target}: no zmap id in manifest")
    return ipid_measurement_coverage(
        measurement.artifact_path(processed_root, "strategies"),
        raw_root / "zmap" / measurement.zmap_id / "zmap.pq",
    )
