"""Write coverage metadata for one IP-ID measurement."""

import json
from pathlib import Path

import duckdb

from ipid_analysis.config import RAW_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement


def write_coverage(m: IpidMeasurement, raw_root: Path = RAW_DATA_DIR) -> Path:
    measurement_dir = raw_root / m.input_key
    ipid_path = measurement_dir / "ipid.pq"
    zmap_paths = sorted(path for path in measurement_dir.glob("zmap*.pq") if path.is_file())

    if not ipid_path.is_file():
        raise FileNotFoundError(ipid_path)
    if not zmap_paths:
        raise FileNotFoundError(measurement_dir / "zmap*.pq")
    if len(zmap_paths) > 1:
        raise ValueError(
            f"{measurement_dir}: expected exactly one zmap*.pq, found {len(zmap_paths)}"
        )

    with duckdb.connect() as con:
        ipid_count, zmap_count = con.execute(
            """
            SELECT
                (SELECT count(DISTINCT IP_ADDR) FROM read_parquet($ipid)),
                (SELECT count(DISTINCT IP_ADDR) FROM read_parquet($zmap))
            """,
            {"ipid": str(ipid_path), "zmap": str(zmap_paths[0])},
        ).fetchone()

    output_path = measurement_dir / "coverage.json"
    output_path.write_text(
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
    return output_path
