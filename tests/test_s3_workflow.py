import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.s3_workflow import Request, build_unclassified_targets, process_request


class FakeS3Client:
    def __init__(self, objects):
        self.objects = objects
        self.uploads = []

    def exists(self, uri):
        return uri in self.objects

    def download(self, uri, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.objects[uri])

    def upload(self, path, uri):
        self.objects[uri] = path.read_bytes()
        self.uploads.append(uri)


class S3WorkflowTest(unittest.TestCase):
    def test_build_unclassified_targets_uses_zmap_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategies = root / "strategies.pq"
            output = root / "zmap_unclassified.pq"
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["192.0.2.1", "192.0.2.2", "192.0.2.3"],
                        "IPID_SELECTION_STRATEGY": ["UNCLASSIFIED", "RANDOM", "UNCLASSIFIED"],
                    }
                ),
                strategies,
            )

            rows = build_unclassified_targets(strategies, output)
            table = pq.read_table(output)
            self.assertEqual(rows, 2)
            self.assertEqual(table.column_names, ["IP_ADDR", "REPLY_TYPE"])
            self.assertEqual(table["IP_ADDR"].to_pylist(), ["192.0.2.1", "192.0.2.3"])

    def test_request_rejects_result_outside_job_prefix(self):
        data = {
            "version": 1,
            "job_id": "tcp-80_2026-01-01_00-00-00",
            "protocol": "tcp",
            "measurement_id": "tcp-80_2026-01-01_00-00-00",
            "ipid_uri": "s3://bucket/raw/ipid/run/ipid.pq",
            "snapshot_uri": "s3://bucket/raw/ipid/run/ipid.snapshot.yaml",
            "result_uri": "s3://other/result.pq",
            "done_uri": "s3://bucket/workflow/jobs/tcp-80_2026-01-01_00-00-00/done.json",
            "failed_uri": "s3://bucket/workflow/jobs/tcp-80_2026-01-01_00-00-00/failed.json",
            "created_at": "2026-01-01T00:00:00Z",
        }
        with self.assertRaises(ValueError):
            Request.parse(data, "s3://bucket/workflow")

    def test_worker_uploads_filtered_targets_before_done_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.pq"
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["192.0.2.10", "192.0.2.11"],
                        "IPID_SEQUENCE": [",".join(["7"] * 16), "1,2"],
                    }
                ),
                source,
            )
            prefix = "s3://bucket/workflow"
            job_id = "tcp-80_2026-01-01_00-00-00"
            job_prefix = f"{prefix}/jobs/{job_id}"
            request_uri = f"{job_prefix}/request.json"
            result_uri = f"{job_prefix}/zmap_unclassified.pq"
            done_uri = f"{job_prefix}/done.json"
            request = {
                "version": 1,
                "job_id": job_id,
                "protocol": "tcp",
                "measurement_id": job_id,
                "ipid_uri": "s3://bucket/raw/ipid/run/ipid.pq",
                "snapshot_uri": "s3://bucket/raw/ipid/run/ipid.snapshot.yaml",
                "result_uri": result_uri,
                "done_uri": done_uri,
                "failed_uri": f"{job_prefix}/failed.json",
                "created_at": "2026-01-01T00:00:00Z",
            }
            client = FakeS3Client(
                {
                    request_uri: json.dumps(request).encode(),
                    request["ipid_uri"]: source.read_bytes(),
                    request["snapshot_uri"]: (
                        b"connection_count: 4\nrequests_per_connection: 4\n"
                        b"request_ip_ids: [1, 2, 3, 4]\n"
                    ),
                }
            )

            self.assertTrue(process_request(client, request_uri, prefix, root / "work", 100, 1))
            self.assertEqual(client.uploads[-2:], [result_uri, done_uri])
            self.assertEqual(json.loads(client.objects[done_uri])["rows"], 1)

            result = root / "result.pq"
            result.write_bytes(client.objects[result_uri])
            self.assertEqual(pq.read_table(result)["IP_ADDR"].to_pylist(), ["192.0.2.11"])
            self.assertFalse((root / "work" / job_id).exists())


if __name__ == "__main__":
    unittest.main()
