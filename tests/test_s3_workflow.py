import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.s3_workflow import (
    ANALYSIS_JOB_VERSION,
    INTERPROTOCOL_JOB_VERSION,
    INTERPROTOCOL_TARGET_JOB_VERSION,
    PROTOCOL_VERSION,
    AnalysisRequest,
    InterprotocolRequest,
    Request,
    build_unclassified_targets,
    download_analysis_inputs,
    process_analysis_request,
    process_interprotocol_request,
    process_interprotocol_target_request,
    process_request,
    validate_analysis_manifest,
)
from ipid_analysis.strategy_merge import iter_strategy_merges


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
    def test_interprotocol_target_worker_builds_and_uploads_all_groups(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed_root = root / "processed"
            manifests = {}
            rows = {
                "icmp": [("192.0.2.1", "SINGLE"), ("192.0.2.2", "PER_BUCKET")],
                "tcp": [("192.0.2.1", "SINGLE"), ("192.0.2.2", "PER_BUCKET")],
                "udp": [("192.0.2.1", "SINGLE")],
            }
            objects = {}
            for protocol, protocol_rows in rows.items():
                manifest_protocol = "udp-dns" if protocol == "udp" else protocol
                measurement_prefix = (
                    "udp-dns-53"
                    if protocol == "udp"
                    else "tcp-80"
                    if protocol == "tcp"
                    else "icmp"
                )
                manifest = {
                    manifest_protocol: {
                        "zmap": f"{measurement_prefix}_2026-01-01_00-00-00",
                        "ipid": {
                            "no-connection": {
                                "rt-based": {"base": f"{measurement_prefix}_2026-01-01_00-00-01"},
                                "fixed-interval": {
                                    "mass": f"{measurement_prefix}_2026-01-01_00-00-02"
                                },
                            }
                        },
                    }
                }
                strategy_path = iter_strategy_merges(manifest)[0].artifact_path(
                    processed_root, "strategies"
                )
                strategy_path.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(
                    pa.table(
                        {
                            "IP_ADDR": [row[0] for row in protocol_rows],
                            "IPID_SELECTION_STRATEGY": [row[1] for row in protocol_rows],
                        }
                    ),
                    strategy_path,
                )
                uri = f"s3://bucket/workflow/analysis-jobs/{protocol}-job/manifest.json"
                manifests[protocol] = uri
                objects[uri] = json.dumps(manifest).encode()

            prefix = "s3://bucket/workflow"
            run_id = "interprotocol_2026-01-01_00-00-00"
            job_prefix = f"{prefix}/interprotocol-target-jobs/{run_id}"
            request_uri = f"{job_prefix}/request.json"
            request = {
                "version": INTERPROTOCOL_TARGET_JOB_VERSION,
                "job_id": run_id,
                "campaign_id": run_id,
                "manifests": manifests,
                "target_prefix": f"{job_prefix}/targets",
                "done_uri": f"{job_prefix}/done.json",
                "failed_uri": f"{job_prefix}/failed.json",
                "created_at": "2026-01-01T00:00:00Z",
            }
            objects[request_uri] = json.dumps(request).encode()
            client = FakeS3Client(objects)

            self.assertTrue(
                process_interprotocol_target_request(
                    client,
                    request_uri,
                    prefix,
                    root / "jobs",
                    output_root=root / "targets-output",
                    processed_root=processed_root,
                    figures_root=root / "figures",
                )
            )
            for group in ("icmp-tcp", "icmp-udp", "tcp-udp", "icmp-tcp-udp"):
                self.assertIn(
                    f"{request['target_prefix']}/{group}-targets.pq",
                    client.objects,
                )
            done = json.loads(client.objects[request["done_uri"]])
            self.assertEqual(done["rows"]["icmp-tcp-udp"], 1)
            self.assertEqual(done["rows"]["icmp-tcp"], 1)
            self.assertIn(
                f"{job_prefix}/reports/figures/target-population.pdf",
                client.objects,
            )

    def test_interprotocol_worker_classifies_campaign_and_uploads_results(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "interprotocol.pq"
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["192.0.2.1"],
                        "IPID_SELECTION_STRATEGY": ["SINGLE"],
                        "IPID_SEQUENCE": [",".join(str(value) for value in range(1, 33))],
                    }
                ),
                source,
            )
            prefix = "s3://bucket/workflow"
            ipid_prefix = "s3://bucket/raw/ipid"
            run_id = "campaign-2026-01-01"
            measurement_id = "interprotocol-icmp-udp_2026-01-01_00-00-00"
            job_prefix = f"{prefix}/interprotocol-jobs/{run_id}"
            request_uri = f"{job_prefix}/request.json"
            manifest = {
                "version": 1,
                "campaign_id": "campaign",
                "run_id": run_id,
                "groups": {
                    "icmp-udp": {
                        "protocols": ["icmp", "udp"],
                        "status": "complete",
                        "measurement_id": measurement_id,
                    }
                },
            }
            request = {
                "version": INTERPROTOCOL_JOB_VERSION,
                "job_id": run_id,
                "campaign_id": "campaign",
                "manifest_uri": f"{job_prefix}/manifest.json",
                "ipid_prefix": ipid_prefix,
                "result_prefix": f"{job_prefix}/results",
                "done_uri": f"{job_prefix}/done.json",
                "failed_uri": f"{job_prefix}/failed.json",
                "created_at": "2026-01-01T00:00:00Z",
            }
            client = FakeS3Client(
                {
                    request_uri: json.dumps(request).encode(),
                    request["manifest_uri"]: json.dumps(manifest).encode(),
                    f"{ipid_prefix}/{measurement_id}/interprotocol.pq": source.read_bytes(),
                    f"{ipid_prefix}/{measurement_id}/interprotocol.snapshot.yaml": (
                        b"connection_count: 4\nrequests_per_connection: 4\n"
                        b"interprotocol:\n  protocols: [icmp, udp]\n"
                    ),
                }
            )

            processed = process_interprotocol_request(
                client,
                request_uri,
                prefix,
                root / "jobs",
                raw_root=root / "raw",
                output_root=root / "processed",
                figures_root=root / "figures",
            )

            self.assertTrue(processed)
            self.assertIn(request["done_uri"], client.uploads)
            summary_uri = f"{request['result_prefix']}/interprotocol-campaign-summary.json"
            combined_uri = f"{request['result_prefix']}/interprotocol-campaign-deployments.pq"
            self.assertEqual(json.loads(client.objects[summary_uri])["rows"], 1)
            self.assertIn(combined_uri, client.objects)
            self.assertIn(
                f"{request['result_prefix']}/reports/figures/interprotocol-campaign-single.pdf",
                client.objects,
            )
            self.assertEqual(
                InterprotocolRequest.parse(request, prefix).campaign_id,
                "campaign",
            )

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

    def test_worker_prepares_bounded_random_reproducibility_targets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prefix = "s3://bucket/workflow"
            job_id = "icmp_2026-01-01_00-00-00"
            job_prefix = f"{prefix}/jobs/{job_id}"
            measurement_prefix = f"s3://bucket/raw/ipid/{job_id}"
            request_uri = f"{job_prefix}/request.json"
            request = {
                "version": PROTOCOL_VERSION,
                "job_id": job_id,
                "protocol": "icmp",
                "measurement_id": job_id,
                "ipid_uri": f"{measurement_prefix}/ipid.pq",
                "snapshot_uri": f"{measurement_prefix}/ipid.snapshot.yaml",
                "result_uri": f"{measurement_prefix}/random-reproducibility-targets.pq",
                "done_uri": f"{job_prefix}/done.json",
                "failed_uri": f"{job_prefix}/failed.json",
                "created_at": "2026-01-01T00:00:00Z",
                "purpose": "random-reproducibility",
                "maximum_targets": 10_000,
                "selection_seed": 42,
            }
            client = FakeS3Client(
                {
                    request_uri: json.dumps(request).encode(),
                    request["ipid_uri"]: b"input",
                    request["snapshot_uri"]: b"snapshot",
                }
            )
            classify_calls = []

            def fake_classify(input_path, snapshot_path, output_path, **kwargs):
                classify_calls.append(kwargs)
                pq.write_table(
                    pa.table(
                        {
                            "IP_ADDR": ["192.0.2.1"],
                            "IPID_SELECTION_STRATEGY": ["UNCLASSIFIED"],
                        }
                    ),
                    output_path,
                )

            def fake_prepare(*args, **kwargs):
                pq.write_table(
                    pa.table({"IP_ADDR": ["192.0.2.1"], "REPLY_TYPE": [""]}),
                    kwargs["target_path"],
                )
                pq.write_table(pa.table({"IP_ADDR": ["192.0.2.1"]}), kwargs["cohort_path"])
                kwargs["json_path"].write_text("{}\n")
                return {
                    "targets": kwargs["target_path"],
                    "cohort": kwargs["cohort_path"],
                    "json": kwargs["json_path"],
                }

            with (
                patch("ipid_analysis.s3_workflow.classify_paths", side_effect=fake_classify),
                patch(
                    "ipid_analysis.s3_workflow.prepare_random_reproducibility_paths",
                    side_effect=fake_prepare,
                ),
            ):
                self.assertTrue(
                    process_request(client, request_uri, prefix, root / "work", 100, 1)
                )

            self.assertTrue(classify_calls[0]["mass"])
            self.assertIn(
                f"{measurement_prefix}/random-reproducibility-cohort.pq",
                client.uploads,
            )
            self.assertIn(request["result_uri"], client.uploads)

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
            repeat_ids = [f"icmp_2026-07-22_10-00-{value:02d}" for value in range(10, 15)]
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
                    "random_reproducibility": {
                        "baseline": mass_id,
                        "repeats": repeat_ids,
                        "target_file": "random-reproducibility-targets.pq",
                        "cohort_file": "random-reproducibility-cohort.pq",
                        "prepare_metadata_file": "random-reproducibility-prepare.json",
                        "selection_seed": 42,
                        "maximum_targets": 10000,
                    },
                }
            }
            objects = {
                request_uri: json.dumps(request).encode(),
                manifest_uri: json.dumps(manifest).encode(),
                f"s3://bucket/raw/zmap/{job_id}/zmap.pq": b"zmap",
                f"s3://bucket/raw/os/{os_id}/os.pq": b"os",
                f"s3://bucket/raw/os/{os_id}/os-coverage.json": b"{}",
                f"s3://bucket/raw/ipid/{rt_id}/zmap_unclassified.pq": b"targets",
                f"s3://bucket/raw/ipid/{rt_id}/strategies.pq": b"strategies",
                f"s3://bucket/raw/ipid/{mass_id}/random-reproducibility-targets.pq": b"targets",
                f"s3://bucket/raw/ipid/{mass_id}/random-reproducibility-cohort.pq": b"cohort",
                f"s3://bucket/raw/ipid/{mass_id}/random-reproducibility-prepare.json": b"{}",
            }
            for measurement_id in (rt_id, mass_id, fixed_id):
                objects[f"s3://bucket/raw/ipid/{measurement_id}/ipid.pq"] = b"ipid"
                objects[f"s3://bucket/raw/ipid/{measurement_id}/ipid.snapshot.yaml"] = b"snapshot"
            for measurement_id in repeat_ids:
                objects[f"s3://bucket/raw/ipid/{measurement_id}/ipid.pq"] = b"ipid"
                objects[f"s3://bucket/raw/ipid/{measurement_id}/ipid.snapshot.yaml"] = b"snapshot"
            client = FakeS3Client(objects)
            calls = []

            def fake_postprocess(manifest_path, log_path, batch_size, threads):
                calls.append((manifest_path, batch_size, threads))
                self.assertTrue((root / "raw" / "zmap" / job_id / "zmap.pq").is_file())
                self.assertTrue((root / "raw" / "os" / os_id / "os.pq").is_file())
                self.assertTrue((root / "raw" / "os" / os_id / "os-coverage.json").is_file())
                self.assertTrue((root / "raw" / "ipid" / rt_id / "strategies.pq").is_file())
                self.assertTrue((root / "raw" / "ipid" / rt_id / "zmap_unclassified.pq").is_file())
                self.assertTrue(
                    (
                        root / "raw" / "ipid" / mass_id / "random-reproducibility-cohort.pq"
                    ).is_file()
                )
                for measurement_id in repeat_ids:
                    self.assertTrue((root / "raw" / "ipid" / measurement_id / "ipid.pq").is_file())
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

    def test_analysis_request_accepts_canonical_tcp_fixed_base_target(self):
        prefix = "s3://bucket/workflow"
        job_id = "tcp-80_2026-07-22_10-00-00"
        data = {
            "version": ANALYSIS_JOB_VERSION,
            "job_id": job_id,
            "protocol": "tcp",
            "manifest_uri": f"{prefix}/analysis-jobs/{job_id}/manifest.json",
            "zmap_prefix": "s3://bucket/raw/zmap/",
            "os_prefix": "s3://bucket/raw/os/",
            "ipid_prefix": "s3://bucket/raw/ipid/",
            "done_uri": f"{prefix}/analysis-jobs/{job_id}/done.json",
            "failed_uri": f"{prefix}/analysis-jobs/{job_id}/failed.json",
            "created_at": "2026-07-22T10:05:00Z",
            "fixed_base_target_uri": (f"s3://bucket/raw/zmap/{job_id}/zmap-fixed-base-sample.pq"),
        }

        request = AnalysisRequest.parse(data, prefix)

        self.assertEqual(request.fixed_base_target_uri, data["fixed_base_target_uri"])

    def test_analysis_request_rejects_noncanonical_fixed_base_target(self):
        prefix = "s3://bucket/workflow"
        job_id = "icmp_2026-07-22_10-00-00"
        data = {
            "version": ANALYSIS_JOB_VERSION,
            "job_id": job_id,
            "protocol": "icmp",
            "manifest_uri": f"{prefix}/analysis-jobs/{job_id}/manifest.json",
            "zmap_prefix": "s3://bucket/raw/zmap/",
            "os_prefix": "s3://bucket/raw/os/",
            "ipid_prefix": "s3://bucket/raw/ipid/",
            "done_uri": f"{prefix}/analysis-jobs/{job_id}/done.json",
            "failed_uri": f"{prefix}/analysis-jobs/{job_id}/failed.json",
            "created_at": "2026-07-22T10:05:00Z",
            "fixed_base_target_uri": "s3://other/zmap-fixed-base-sample.pq",
        }

        with self.assertRaisesRegex(ValueError, "canonical S3 location"):
            AnalysisRequest.parse(data, prefix)

    def test_downloads_icmp_udp_fixed_base_samples(self):
        for protocol, id_prefix in (("icmp", "icmp"), ("udp-dns", "udp-dns-53")):
            with self.subTest(protocol=protocol), tempfile.TemporaryDirectory() as directory:
                prefix = "s3://bucket/workflow"
                job_id = id_prefix + "_2026-07-22_10-00-00"
                os_id = id_prefix + "_2026-07-22_10-00-01"
                rt_id = id_prefix + "_2026-07-22_10-00-02"
                fixed_id = id_prefix + "_2026-07-22_10-00-03"
                sample_uri = f"s3://bucket/raw/zmap/{job_id}/zmap-fixed-base-sample.pq"
                metadata_uri = f"s3://bucket/raw/zmap/{job_id}/zmap-fixed-base-sample.json"
                request = AnalysisRequest.parse(
                    {
                        "version": ANALYSIS_JOB_VERSION,
                        "job_id": job_id,
                        "protocol": protocol,
                        "manifest_uri": f"{prefix}/analysis-jobs/{job_id}/manifest.json",
                        "zmap_prefix": "s3://bucket/raw/zmap/",
                        "os_prefix": "s3://bucket/raw/os/",
                        "ipid_prefix": "s3://bucket/raw/ipid/",
                        "done_uri": f"{prefix}/analysis-jobs/{job_id}/done.json",
                        "failed_uri": f"{prefix}/analysis-jobs/{job_id}/failed.json",
                        "created_at": "2026-07-22T10:05:00Z",
                        "fixed_base_target_uri": sample_uri,
                    },
                    prefix,
                )
                manifest = {
                    protocol: {
                        "zmap": job_id,
                        "os": os_id,
                        "ipid": {
                            "no-connection": {
                                "rt-based": {"base": rt_id},
                                "fixed-interval": {"base": fixed_id},
                            }
                        },
                    }
                }
                objects = {
                    f"s3://bucket/raw/zmap/{job_id}/zmap.pq": b"zmap",
                    sample_uri: b"sample",
                    metadata_uri: b"metadata",
                    f"s3://bucket/raw/os/{os_id}/os.pq": b"os",
                    f"s3://bucket/raw/os/{os_id}/os-coverage.json": b"{}",
                    f"s3://bucket/raw/ipid/{rt_id}/ipid.pq": b"rt",
                    f"s3://bucket/raw/ipid/{rt_id}/ipid.snapshot.yaml": b"rt-snapshot",
                    f"s3://bucket/raw/ipid/{rt_id}/zmap_unclassified.pq": b"targets",
                    f"s3://bucket/raw/ipid/{fixed_id}/ipid.pq": b"fixed",
                    f"s3://bucket/raw/ipid/{fixed_id}/ipid.snapshot.yaml": b"fixed-snapshot",
                }
                validate_analysis_manifest(manifest, request)
                root = Path(directory)
                download_analysis_inputs(FakeS3Client(objects), request, manifest, root)
                self.assertEqual(
                    (root / "zmap" / job_id / "zmap-fixed-base-sample.pq").read_bytes(), b"sample"
                )
                self.assertEqual(
                    (root / "zmap" / job_id / "zmap-fixed-base-sample.json").read_bytes(),
                    b"metadata",
                )
                with self.assertRaisesRegex(ValueError, "invalid connection_target_uri"):
                    AnalysisRequest.parse(
                        request.__dict__
                        | {
                            "connection_target_uri": f"s3://bucket/raw/zmap/{job_id}/zmap-connection-sample.pq"
                        },
                        prefix,
                    )
                del objects[sample_uri]
                with self.assertRaises(KeyError):
                    download_analysis_inputs(FakeS3Client(objects), request, manifest, root)

    def test_downloads_tcp_fixed_base_target_and_metadata(self):
        prefix = "s3://bucket/workflow"
        job_id = "tcp-80_2026-07-22_10-00-00"
        os_id = "tcp-80_2026-07-22_10-00-01"
        rt_id = "tcp-80_2026-07-22_10-00-02"
        fixed_id = "tcp-80_2026-07-22_10-00-03"
        sample_uri = f"s3://bucket/raw/zmap/{job_id}/zmap-fixed-base-sample.pq"
        request = AnalysisRequest.parse(
            {
                "version": ANALYSIS_JOB_VERSION,
                "job_id": job_id,
                "protocol": "tcp",
                "manifest_uri": f"{prefix}/analysis-jobs/{job_id}/manifest.json",
                "zmap_prefix": "s3://bucket/raw/zmap/",
                "os_prefix": "s3://bucket/raw/os/",
                "ipid_prefix": "s3://bucket/raw/ipid/",
                "done_uri": f"{prefix}/analysis-jobs/{job_id}/done.json",
                "failed_uri": f"{prefix}/analysis-jobs/{job_id}/failed.json",
                "created_at": "2026-07-22T10:05:00Z",
                "fixed_base_target_uri": sample_uri,
                "connection_target_uri": f"s3://bucket/raw/zmap/{job_id}/zmap-connection-sample.pq",
            },
            prefix,
        )
        with self.assertRaisesRegex(ValueError, "invalid connection_target_uri"):
            AnalysisRequest.parse(request.__dict__ | {"connection_target_uri": sample_uri}, prefix)
        manifest = {
            "tcp": {
                "zmap": job_id,
                "connection_target": "zmap-connection-sample.pq",
                "os": os_id,
                "ipid": {
                    "no-connection": {
                        "rt-based": {"base": rt_id},
                        "fixed-interval": {"base": fixed_id},
                    }
                },
            }
        }
        objects = {
            f"s3://bucket/raw/zmap/{job_id}/zmap.pq": b"zmap",
            sample_uri: b"sample",
            request.connection_target_uri: b"synack-targets",
            f"s3://bucket/raw/zmap/{job_id}/zmap-connection-sample.json": b"synack-metadata",
            f"s3://bucket/raw/zmap/{job_id}/zmap-fixed-base-sample.json": b"metadata",
            f"s3://bucket/raw/os/{os_id}/os.pq": b"os",
            f"s3://bucket/raw/os/{os_id}/os-coverage.json": b"{}",
            f"s3://bucket/raw/ipid/{rt_id}/ipid.pq": b"rt",
            f"s3://bucket/raw/ipid/{rt_id}/ipid.snapshot.yaml": b"rt-snapshot",
            f"s3://bucket/raw/ipid/{rt_id}/zmap_unclassified.pq": b"targets",
            f"s3://bucket/raw/ipid/{fixed_id}/ipid.pq": b"fixed",
            f"s3://bucket/raw/ipid/{fixed_id}/ipid.snapshot.yaml": b"fixed-snapshot",
        }

        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory)
            validate_analysis_manifest(manifest, request)
            manifest["tcp"]["connection_target"] = "zmap-fixed-base-sample.pq"
            with self.assertRaisesRegex(ValueError, "connection target does not match"):
                validate_analysis_manifest(manifest, request)
            manifest["tcp"]["connection_target"] = "zmap-connection-sample.pq"
            download_analysis_inputs(FakeS3Client(objects), request, manifest, raw_root)
            self.assertEqual(
                (raw_root / "zmap" / job_id / "zmap-connection-sample.pq").read_bytes(),
                b"synack-targets",
            )
            self.assertEqual(
                (raw_root / "zmap" / job_id / "zmap-connection-sample.json").read_bytes(),
                b"synack-metadata",
            )

            self.assertEqual(
                (raw_root / "zmap" / job_id / "zmap-fixed-base-sample.pq").read_bytes(),
                b"sample",
            )
            self.assertEqual(
                (raw_root / "zmap" / job_id / "zmap-fixed-base-sample.json").read_bytes(),
                b"metadata",
            )


if __name__ == "__main__":
    unittest.main()
