import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from ipid_analysis.config import MeasurementID, IPID_DATA_DIR, IPID_MEASURE_NAME, ZMAP_DATA_DIR, ZMAP_MEASURE_NAME, \
    IPID_CONFIG_SNAPSHOT_NAME, load_config, FIGURES_DIR, COVERAGE_JSON_NAME


def create_info(ipid_id: MeasurementID) -> tuple[dict[str, Any], Path]:
    ipid_path = IPID_DATA_DIR / str(ipid_id) / IPID_MEASURE_NAME
    snapshot_path = IPID_DATA_DIR / str(ipid_id) / IPID_CONFIG_SNAPSHOT_NAME
    cfg = load_config(snapshot_path)
    zmap_path = ZMAP_DATA_DIR / str(cfg.zmap) / ZMAP_MEASURE_NAME

    attempted = pq.ParquetFile(zmap_path).metadata.num_rows
    valid = pq.ParquetFile(ipid_path).metadata.num_rows

    cov = valid / attempted if attempted else 0.0
    info = {
        "ipid": str(ipid_id),
        "zmap": str(cfg.zmap),
        "attempted": attempted,
        "valid": valid,
        "coverage": cov,
        "coverage_pct": round(100.0 * cov, 2),
    }
    output_path = FIGURES_DIR / str(ipid_id) / COVERAGE_JSON_NAME
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(info, indent=2) + "\n")
    return info, output_path
