import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.s3_workflow import (
    ANALYSIS_JOB_VERSION,
    PROTOCOL_VERSION,
    AnalysisRequest,
    Request,
    build_unclassified_targets,
    process_analysis_request,
    process_request,
)


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

    def test_request_rejects_result_outside_measurement_prefix(self):
        data = {
            "version": PROTOCOL_VERSION,
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

    def _assert_worker_uploads_filtered_targets(self, protocol, job_id):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.pq"
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["192.0.2.10", "192.0.2.11"],
                        "IPID_SEQUENCE": [
                            ",".join(["7"] * 16),
                            ",".join(
                                str(value)
                                for value in [
                                    33_375,
                                    55_746,
                                    60_367,
                                    41_743,
                                    55_073,
                                    33_497,
                                    48_632,
                                    17_680,
                                    59_576,
                                    20_173,
                                    15_785,
                                    2_685,
                                    61_469,
                                    4_930,
                                    10_166,
                                    1_083,
                                ]
                            ),
                        ],
                    }
                ),
                source,
            )
            prefix = "s3://bucket/workflow"
            job_prefix = f"{prefix}/jobs/{job_id}"
            request_uri = f"{job_prefix}/request.json"
            strategies_uri = "s3://bucket/raw/ipid/run/strategies.pq"
            result_uri = "s3://bucket/raw/ipid/run/zmap_unclassified.pq"
            done_uri = f"{job_prefix}/done.json"
            request = {
                "version": PROTOCOL_VERSION,
                "job_id": job_id,
                "protocol": protocol,
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
                        b"fixed_interval:\n  minimum_reply_rate: 0.8\n"
                    ),
                }
            )

            self.assertTrue(process_request(client, request_uri, prefix, root / "work", 100, 1))
            self.assertEqual(client.uploads[-3:], [strategies_uri, result_uri, done_uri])
            self.assertEqual(json.loads(client.objects[done_uri])["rows"], 1)

            persisted = root / "persisted-strategies.pq"
            persisted.write_bytes(client.objects[strategies_uri])
            self.assertEqual(
                pq.read_table(persisted)["IP_ADDR"].to_pylist(),
                ["192.0.2.10", "192.0.2.11"],
            )

            result = root / "result.pq"
            result.write_bytes(client.objects[result_uri])
            self.assertEqual(pq.read_table(result)["IP_ADDR"].to_pylist(), ["192.0.2.11"])
            self.assertFalse((root / "work" / job_id).exists())

    def test_worker_supports_all_measurement_protocols(self):
        cases = [
            ("icmp", "icmp_2026-01-01_00-00-00"),
            ("tcp", "tcp-80_2026-01-01_00-00-00"),
            ("udp-dns", "udp-dns-53_2026-01-01_00-00-00"),
        ]
        for protocol, job_id in cases:
            with self.subTest(protocol=protocol):
                self._assert_worker_uploads_filtered_targets(protocol, job_id)

    def test_request_rejects_unsupported_protocol(self):
        prefix = "s3://bucket/workflow"
        job_id = "sctp_2026-01-01_00-00-00"
        job_prefix = f"{prefix}/jobs/{job_id}"
        data = {
            "version": PROTOCOL_VERSION,
            "job_id": job_id,
            "protocol": "sctp",
            "measurement_id": job_id,
            "ipid_uri": "s3://bucket/raw/ipid/run/ipid.pq",
            "snapshot_uri": "s3://bucket/raw/ipid/run/ipid.snapshot.yaml",
            "result_uri": "s3://bucket/raw/ipid/run/zmap_unclassified.pq",
            "done_uri": f"{job_prefix}/done.json",
            "failed_uri": f"{job_prefix}/failed.json",
            "created_at": "2026-01-01T00:00:00Z",
        }

        with self.assertRaisesRegex(ValueError, "unsupported protocol"):
            Request.parse(data, prefix)

    def test_request_rejects_protocol_mismatching_measurement_id(self):
        prefix = "s3://bucket/workflow"
        job_id = "icmp_2026-01-01_00-00-00"
        job_prefix = f"{prefix}/jobs/{job_id}"
        data = {
            "version": PROTOCOL_VERSION,
            "job_id": job_id,
            "protocol": "tcp",
            "measurement_id": job_id,
            "ipid_uri": "s3://bucket/raw/ipid/run/ipid.pq",
            "snapshot_uri": "s3://bucket/raw/ipid/run/ipid.snapshot.yaml",
            "result_uri": "s3://bucket/raw/ipid/run/zmap_unclassified.pq",
            "done_uri": f"{job_prefix}/done.json",
            "failed_uri": f"{job_prefix}/failed.json",
            "created_at": "2026-01-01T00:00:00Z",
        }

        with self.assertRaisesRegex(ValueError, "does not match measurement id"):
            Request.parse(data, prefix)

    def test_analysis_worker_downloads_manifest_inputs_and_runs_postprocessing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prefix = "s3://bucket/workflow"
            job_id = "icmp_2026-07-22_10-00-00"
            os_id = "icmp_2026-07-22_10-00-01"
            rt_id = "icmp_2026-07-22_10-00-02"
            mass_id = "icmp_2026-07-22_10-00-03"
            fixed_id = "icmp_2026-07-22_10-00-04"
            job_prefix = f"{prefix}/analysis-jobs/{job_id}"
            request_uri = f"{job_prefix}/request.json"
            manifest_uri = f"{job_prefix}/manifest.json"
            request = {
                "version": ANALYSIS_JOB_VERSION,
                "job_id": job_id,
                "protocol": "icmp",
                "manifest_uri": manifest_uri,
                "zmap_prefix": "s3://bucket/raw/zmap/",
                "os_prefix": "s3://bucket/raw/os/",
                "ipid_prefix": "s3://bucket/raw/ipid/",
                "done_uri": f"{job_prefix}/done.json",
                "failed_uri": f"{job_prefix}/failed.json",
                "created_at": "2026-07-22T10:05:00Z",
            }
            manifest = {
                "icmp": {
                    "zmap": job_id,
                    "os": os_id,
                    "ipid": {
                        "no-connection": {
                            "rt-based": {"base": rt_id},
                            "fixed-interval": {"base": fixed_id, "mass": mass_id},
                        }
                    },
                }
            }
            objects = {
                request_uri: json.dumps(request).encode(),
                manifest_uri: json.dumps(manifest).encode(),
                f"s3://bucket/raw/zmap/{job_id}/zmap.pq": b"zmap",
                f"s3://bucket/raw/os/{os_id}/os.pq": b"os",
                f"s3://bucket/raw/ipid/{rt_id}/zmap_unclassified.pq": b"targets",
                f"s3://bucket/raw/ipid/{rt_id}/strategies.pq": b"strategies",
            }
            for measurement_id in (rt_id, mass_id, fixed_id):
                objects[f"s3://bucket/raw/ipid/{measurement_id}/ipid.pq"] = b"ipid"
                objects[f"s3://bucket/raw/ipid/{measurement_id}/ipid.snapshot.yaml"] = b"snapshot"
            client = FakeS3Client(objects)
            calls = []

            def fake_postprocess(manifest_path, log_path, batch_size, threads):
                calls.append((manifest_path, batch_size, threads))
                self.assertTrue((root / "raw" / "zmap" / job_id / "zmap.pq").is_file())
                self.assertTrue((root / "raw" / "os" / os_id / "os.pq").is_file())
                self.assertTrue((root / "raw" / "ipid" / rt_id / "strategies.pq").is_file())
                self.assertTrue((root / "raw" / "ipid" / rt_id / "zmap_unclassified.pq").is_file())
                log_path.write_text("postprocessing complete\n")

            self.assertTrue(
                process_analysis_request(
                    client,
                    request_uri,
                    prefix,
                    root / "jobs",
                    1234,
                    2,
                    postprocess=fake_postprocess,
                    raw_root=root / "raw",
                )
            )
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][1:], (1234, 2))
            self.assertEqual(
                client.uploads[-2:],
                [f"{job_prefix}/postprocess.log", f"{job_prefix}/done.json"],
            )
            done = json.loads(client.objects[f"{job_prefix}/done.json"])
            self.assertEqual(done["job_id"], job_id)
            self.assertTrue((root / "jobs" / job_id / "manifest.json").is_file())

    def test_analysis_request_rejects_noncanonical_manifest_location(self):
        prefix = "s3://bucket/workflow"
        job_id = "tcp-80_2026-07-22_10-00-00"
        data = {
            "version": ANALYSIS_JOB_VERSION,
            "job_id": job_id,
            "protocol": "tcp",
            "manifest_uri": "s3://other/manifest.json",
            "zmap_prefix": "s3://bucket/raw/zmap/",
            "os_prefix": "s3://bucket/raw/os/",
            "ipid_prefix": "s3://bucket/raw/ipid/",
            "done_uri": f"{prefix}/analysis-jobs/{job_id}/done.json",
            "failed_uri": f"{prefix}/analysis-jobs/{job_id}/failed.json",
            "created_at": "2026-07-22T10:05:00Z",
        }
        with self.assertRaisesRegex(ValueError, "manifest_uri"):
            AnalysisRequest.parse(data, prefix)


if __name__ == "__main__":
    unittest.main()
