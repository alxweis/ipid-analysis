"""Write coverage metadata for one IP-ID measurement."""

import json
from pathlib import Path

import duckdb

from ipid_analysis.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement, resolve


def _target_path(m: IpidMeasurement, manifest: dict, raw_root: Path) -> Path:
    if (m.connection_mode, m.interval, m.scale) != (
        "no-connection",
        "fixed-interval",
        "mass",
    ):
        return raw_root / "zmap" / m.zmap_id / "zmap.pq"

    rt_base = resolve(manifest, f"{m.protocol}.ipid.no-connection.rt-based.base")
    if rt_base is None:
        raise ValueError(f"{m.target}: corresponding RT-based base measurement is missing")
    return raw_root / rt_base.input_key / "zmap_unclassified.pq"


def write_coverage(
    m: IpidMeasurement,
    manifest: dict,
    raw_root: Path = RAW_DATA_DIR,
    processed_root: Path = PROCESSED_DATA_DIR,
) -> Path:
    measurement_dir = raw_root / m.input_key
    ipid_path = measurement_dir / "ipid.pq"
    zmap_path = _target_path(m, manifest, raw_root)

    for path in (ipid_path, zmap_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    with duckdb.connect() as con:
        ipid_count, zmap_count, outside_target_count = con.execute(
            """
            SELECT
                (SELECT count(DISTINCT IP_ADDR) FROM read_parquet($ipid)),
                (SELECT count(DISTINCT IP_ADDR) FROM read_parquet($zmap)),
                (SELECT count(*) FROM (
                    SELECT DISTINCT IP_ADDR FROM read_parquet($ipid)
                    EXCEPT
                    SELECT DISTINCT IP_ADDR FROM read_parquet($zmap)
                ))
            """,
            {"ipid": str(ipid_path), "zmap": str(zmap_path)},
        ).fetchone()

    if zmap_count == 0:
        raise ValueError(f"{zmap_path}: measurement target is empty")
    if outside_target_count:
        raise ValueError(
            f"{ipid_path}: {outside_target_count} IP address(es) are not present "
            f"in the measurement target {zmap_path}"
        )

    output_path = m.artifact_path(processed_root, "coverage", "json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.write_text(
        json.dumps(
            {
                "zmap_ip_count": int(zmap_count),
                "ipid_ip_count": int(ipid_count),
                "coverage_percent": 100.0 * ipid_count / zmap_count if zmap_count else 0.0,
            },
            indent=2,
        )
        + "\n"
    )
    temporary.replace(output_path)
    return output_path
