"""Opt-in real-mount publication check in a fresh, isolated test namespace.

No existing object is overwritten or deleted. Each invocation leaves a small
UUID-scoped fixture for inspection. Run from an installed analysis environment.
"""

import argparse
import hashlib
import json
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.artifacts import assert_complete, completion_path, write_table, write_text
from ipid_analysis.config import PROCESSED_DATA_DIR, REPORTS_DIR
from ipid_analysis.runtime import mounted_storage, start_run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="publish isolated test objects")
    args = parser.parse_args()
    if not args.execute:
        parser.error("requires --execute; this check writes isolated test objects")
    if not mounted_storage():
        parser.error("requires IPID_ANALYSIS_STORAGE_MODE=mounted-s3")
    start_run()
    namespace = ".ipid-analysis-tests/" + uuid4().hex
    roots = (PROCESSED_DATA_DIR / namespace, REPORTS_DIR / namespace)
    if any(root.exists() for root in roots):
        raise FileExistsError("test namespace unexpectedly exists")
    output = roots[0] / "publication.pq"
    for values in ([1, 2], [3, 4, 5]):
        write_table(pa.table({"value": values}), output)
        assert_complete(output)
        assert pq.read_table(output)["value"].to_pylist() == values
        metadata = json.loads(completion_path(output).read_text())
        assert hashlib.sha256(output.read_bytes()).hexdigest() == metadata["sha256"]
    report = roots[1] / "result.json"
    write_text(report, json.dumps({"namespace": namespace, "rows": 3, "status": "passed"}))
    assert_complete(report)
    print(json.dumps({"status": "passed", "retained_test_paths": [str(root) for root in roots]}))


if __name__ == "__main__":
    main()
