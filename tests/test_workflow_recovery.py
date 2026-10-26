"""Worker recovery and strict postprocessing without remote services."""

from contextlib import ExitStack
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from typer.testing import CliRunner
import yaml

from ipid_analysis import (
    coverage,
    increments,
    plot_increments,
    plot_probing_intervals,
    plot_strategies,
    postprocess,
    probing_intervals,
    strategies,
)
from ipid_analysis.artifacts import fingerprints
from ipid_analysis.s3_workflow import (
    ANALYSIS_JOB_VERSION,
    S3Client,
    cleanup_completed_job,
    process_analysis_request,
    retry_failed_request,
)


class FakeClient:
    def __init__(self, objects):
        self.objects = objects
        self.uploads, self.deletions = [], []

    def exists(self, uri):
        return uri in self.objects

    def download(self, uri, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.objects[uri])

    def upload(self, path, uri):
        self.objects[uri] = path.read_bytes()
        self.uploads.append(uri)

    def delete_failure(self, uri):
        del self.objects[uri]
        self.deletions.append(uri)


class WorkflowRecoveryTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        environment = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
            },
        )
        environment.start()
        self.addCleanup(environment.stop)

    def analysis_job(self):
        prefix = "s3://isolated-test/workflow"
        job = "icmp_2026-01-01_00-00-00"
        measurement = "icmp_2026-01-01_00-00-01"
        remote = f"{prefix}/analysis-jobs/{job}"
        request = {
            "version": ANALYSIS_JOB_VERSION,
            "job_id": job,
            "protocol": "icmp",
            "manifest_uri": remote + "/manifest.json",
            "done_uri": remote + "/done.json",
            "failed_uri": remote + "/failed.json",
            "zmap_prefix": "s3://isolated-test/raw/zmap/",
            "ipid_prefix": "s3://isolated-test/raw/ipid/",
            "os_prefix": "s3://isolated-test/raw/os/",
            "created_at": "2026-01-01T00:00:00Z",
        }
        os_id = "icmp_2026-01-01_00-00-02"
        manifest = {
            "icmp": {
                "zmap": job,
                "os": os_id,
                "ipid": {"no-connection": {"rt-based": {"base": measurement}}},
            }
        }
        objects = {
            remote + "/request.json": json.dumps(request).encode(),
            request["manifest_uri"]: json.dumps(manifest).encode(),
            f"s3://isolated-test/raw/zmap/{job}/zmap.pq": b"raw-zmap",
            f"s3://isolated-test/raw/ipid/{measurement}/ipid.pq": b"raw-ipid",
            f"s3://isolated-test/raw/ipid/{measurement}/ipid.snapshot.yaml": b"snapshot",
        }
        objects[f"s3://isolated-test/raw/ipid/{measurement}/zmap_unclassified.pq"] = b"targets"
        objects[f"s3://isolated-test/raw/os/{os_id}/os.pq"] = b"raw-os"
        objects[f"s3://isolated-test/raw/os/{os_id}/os-coverage.json"] = b"{}"
        return prefix, remote, request, FakeClient(objects)

    def test_exit_zero_without_status_report_is_not_done(self):
        prefix, remote, request, client = self.analysis_job()
        with self.assertRaises(FileNotFoundError):
            process_analysis_request(
                client,
                remote + "/request.json",
                prefix,
                self.root / "jobs",
                10,
                1,
                raw_root=self.root / "raw",
                postprocess=lambda *args: None,
            )
        self.assertNotIn(request["done_uri"], client.objects)
        self.assertIn(request["failed_uri"], client.objects)

    def test_empty_success_report_cannot_hide_missing_outputs(self):
        prefix, remote, request, client = self.analysis_job()

        def incomplete(manifest, log, *_args):
            log.with_name("postprocess-status.json").write_text(
                json.dumps(
                    {
                        "version": 1,
                        "required_complete": True,
                        "manifest": fingerprints([manifest]),
                        "artifacts": [],
                    }
                )
            )

        with self.assertRaisesRegex(ValueError, "omitted required artifacts"):
            process_analysis_request(
                client,
                remote + "/request.json",
                prefix,
                self.root / "jobs",
                10,
                1,
                raw_root=self.root / "raw",
                postprocess=incomplete,
            )
        self.assertNotIn(request["done_uri"], client.objects)

    def test_explicit_retry_archives_failure_before_removing_only_failure_marker(self):
        prefix, remote, request, client = self.analysis_job()
        client.objects[request["failed_uri"]] = b'{"error":"temporary outage"}'
        retry_failed_request(client, remote + "/request.json", prefix)
        self.assertEqual(client.deletions, [request["failed_uri"]])
        archived = [uri for uri in client.uploads if "/attempts/" in uri]
        self.assertEqual(len(archived), 1)
        self.assertEqual(client.objects[archived[0]], b'{"error":"temporary outage"}')
        self.assertIn(remote + "/request.json", client.objects)

    def test_completed_job_cannot_be_requeued(self):
        prefix, remote, request, client = self.analysis_job()
        client.objects[request["done_uri"]] = b"{}"
        client.objects[request["failed_uri"]] = b"{}"
        with self.assertRaisesRegex(ValueError, "already completed"):
            retry_failed_request(client, remote + "/request.json", prefix)
        self.assertFalse(client.deletions)

    def test_cleanup_failure_after_durable_completion_is_nonfatal(self):
        for error in (OSError("locked local file"), ValueError("unsafe symlink")):
            with (
                self.subTest(error=error),
                patch("ipid_analysis.s3_workflow.cleanup_work", side_effect=error),
            ):
                cleanup_completed_job(self.root / "jobs" / "completed")

    def test_dot_job_names_are_rejected(self):
        from ipid_analysis.s3_workflow import JOB_ID_RE

        self.assertIsNone(JOB_ID_RE.fullmatch("."))
        self.assertIsNone(JOB_ID_RE.fullmatch(".."))

    def test_transient_s3_failure_is_retried_but_bad_credentials_are_not(self):
        failure = subprocess.CalledProcessError(1, "s3cmd", stderr="503 SlowDown")
        result = subprocess.CompletedProcess("s3cmd", 0, stdout="done")
        with (
            patch("ipid_analysis.s3_workflow.time.sleep"),
            patch("subprocess.run", side_effect=[failure, result]) as run,
        ):
            self.assertEqual(S3Client(run=run)._command("ls", "s3://isolated-test/prefix"), "done")
            self.assertEqual(run.call_count, 2)
            self.assertIn("timeout", run.call_args.kwargs)
        denied = subprocess.CalledProcessError(1, "s3cmd", stderr="403 AccessDenied")
        with patch("subprocess.run", side_effect=denied) as run:
            with self.assertRaises(subprocess.CalledProcessError):
                S3Client(run=run)._command("ls", "s3://isolated-test/prefix")
            self.assertEqual(run.call_count, 1)

    def pipeline_fixture(self):
        job, measurement = "icmp_2026-01-01_00-00-00", "icmp_2026-01-01_00-00-01"
        raw, processed, figures = self.root / "raw", self.root / "processed", self.root / "figures"
        source = raw / "ipid" / measurement
        source.mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "IP_ADDR": ["192.0.2.1"],
                    "IPID_SEQUENCE": [",".join(["7"] * 16)],
                    "SEND_TIMESTAMP_SEQUENCE": [",".join(str(i * i * 100) for i in range(16))],
                }
            ),
            source / "ipid.pq",
        )
        (source / "ipid.snapshot.yaml").write_text(
            yaml.safe_dump(
                {
                    "connection_count": 4,
                    "requests_per_connection": 4,
                    "request_ip_ids": [1000, 2000, 1001, 2001],
                }
            )
        )
        zmap = raw / "zmap" / job
        zmap.mkdir(parents=True)
        pq.write_table(
            pa.table({"IP_ADDR": ["192.0.2.1"], "REPLY_TYPE": ["echo_reply"]}), zmap / "zmap.pq"
        )
        manifest = self.root / "manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "icmp": {
                        "zmap": job,
                        "ipid": {"no-connection": {"rt-based": {"base": measurement}}},
                    }
                }
            )
        )
        stack = ExitStack()
        for module in (
            postprocess,
            increments,
            probing_intervals,
            plot_strategies,
            plot_increments,
            plot_probing_intervals,
        ):
            for name, value in (
                ("RAW_DATA_DIR", raw),
                ("PROCESSED_DATA_DIR", processed),
                ("FIGURES_DIR", figures),
            ):
                if hasattr(module, name):
                    stack.enter_context(patch.object(module, name, value))
        stack.enter_context(
            patch.object(
                postprocess,
                "write_coverage",
                side_effect=lambda m, data: coverage.write_coverage(
                    m, data, raw_root=raw, processed_root=processed
                ),
            )
        )
        stack.enter_context(
            patch.object(
                postprocess,
                "classify_measurement",
                side_effect=lambda m, **kwargs: strategies.classify_measurement(
                    m, raw_root=raw, processed_root=processed, **kwargs
                ),
            )
        )
        stack.enter_context(
            patch.object(
                postprocess,
                "render_strategies_plot",
                side_effect=lambda m: plot_strategies.render(
                    m, processed_root=processed, figures_root=figures
                ),
            )
        )
        self.addCleanup(stack.close)
        return manifest

    def test_real_pipeline_produces_strict_completion_report(self):
        manifest = self.pipeline_fixture()
        report = self.root / "status.json"
        result = CliRunner().invoke(
            postprocess.app,
            [
                str(manifest),
                "--strict",
                "--status-file",
                str(report),
                "--skip-itdk",
                "--skip-ripe",
                "--threads",
                "1",
            ],
        )
        self.assertEqual(result.exit_code, 0, f"{result.exception}\n{result.output}")
        status = json.loads(report.read_text())
        self.assertTrue(status["required_complete"])
        self.assertEqual(len(status["artifacts"]), 10)
        self.assertTrue(all(Path(path).is_file() for path in status["artifacts"]))
        self.assertEqual(status["stages"][0]["status"], "completed")

    def test_no_os_real_pipeline_and_worker_retry_finish_without_os_inputs(self):
        manifest_path = self.pipeline_fixture()
        manifest = json.loads(manifest_path.read_text())
        manifest["icmp"]["os_status"] = "not_requested"
        prefix, remote, request, client = self.analysis_job()
        request.update(os_status="not_requested", os_prefix="")
        client.objects[remote + "/request.json"] = json.dumps(request).encode()
        client.objects[request["manifest_uri"]] = json.dumps(manifest).encode()
        client.objects[remote + "/os-policy.json"] = json.dumps(
            dict(version=1, job_id=request["job_id"], os_status="not_requested")
        ).encode()
        for uri in list(client.objects):
            if "/raw/os/" in uri:
                del client.objects[uri]
            elif "/raw/" in uri and not uri.endswith("zmap_unclassified.pq"):
                relative = uri.split("/raw/", 1)[1]
                client.objects[uri] = (self.root / "raw" / relative).read_bytes()
        original_download = client.download

        def download(uri, path):
            self.assertNotIn("/raw/os/", uri)
            original_download(uri, path)

        client.download = download

        def failure(*args):
            raise RuntimeError("temporary non-OS failure")

        options = dict(
            raw_root=self.root / "raw",
            processed_root=self.root / "processed",
            figures_root=self.root / "figures",
        )
        with self.assertRaisesRegex(RuntimeError, "temporary"):
            process_analysis_request(
                client,
                remote + "/request.json",
                prefix,
                self.root / "jobs",
                10,
                1,
                postprocess=failure,
                **options,
            )
        self.assertNotIn(request["done_uri"], client.objects)
        retry_failed_request(client, remote + "/request.json", prefix)

        def actual_pipeline(path, log, *args):
            result = CliRunner().invoke(
                postprocess.app,
                [
                    str(path),
                    "--strict",
                    "--status-file",
                    str(log.with_name("postprocess-status.json")),
                    "--skip-itdk",
                    "--skip-ripe",
                    "--threads",
                    "1",
                ],
            )
            self.assertEqual(result.exit_code, 0, f"{result.exception}\n{result.output}")

        with patch.object(
            postprocess, "render_os_strategy_plot", side_effect=AssertionError("OS plot called")
        ):
            self.assertTrue(
                process_analysis_request(
                    client,
                    remote + "/request.json",
                    prefix,
                    self.root / "jobs",
                    10,
                    1,
                    postprocess=actual_pipeline,
                    **options,
                )
            )
        done = json.loads(client.objects[request["done_uri"]])
        status = json.loads(client.objects[remote + "/postprocess-status.json"])
        self.assertEqual(done["os_status"], "not_requested")
        self.assertEqual(status["optional"]["os"], {"icmp": "not_requested"})
        self.assertTrue(status["required_complete"])
        self.assertEqual(len(status["artifacts"]), 10)
        self.assertFalse((self.root / "raw/os").exists())
        self.assertFalse(
            process_analysis_request(
                client,
                remote + "/request.json",
                prefix,
                self.root / "jobs",
                10,
                1,
                postprocess=failure,
                **options,
            )
        )

    def test_no_os_request_manifest_and_policy_must_agree(self):
        from ipid_analysis.s3_workflow import (
            AnalysisRequest,
            load_analysis_request,
            validate_analysis_manifest,
        )

        prefix, remote, request, client = self.analysis_job()
        old = AnalysisRequest.parse(request, prefix)
        self.assertNotIn("os_status", old.payload())
        bad = dict(request, os_prefix="")
        with self.assertRaisesRegex(ValueError, "os_prefix"):
            AnalysisRequest.parse(bad, prefix)
        for state in ("failed", "pending", "disabled"):
            with self.assertRaises(ValueError):
                AnalysisRequest.parse(dict(bad, os_status=state), prefix)
        with self.assertRaises(ValueError):
            AnalysisRequest.parse(dict(request, os_status="not_requested"), prefix)
        disabled = AnalysisRequest.parse(dict(bad, os_status="not_requested"), prefix)
        manifest = json.loads(client.objects[request["manifest_uri"]])
        with self.assertRaisesRegex(ValueError, "intent mismatch"):
            validate_analysis_manifest(manifest, disabled)
        manifest["icmp"]["os_status"] = "not_requested"
        with self.assertRaisesRegex(ValueError, "measurement ID"):
            validate_analysis_manifest(manifest, disabled)
        manifest["icmp"].pop("os")
        validate_analysis_manifest(manifest, disabled)
        with self.assertRaisesRegex(ValueError, "intent mismatch"):
            validate_analysis_manifest(manifest, old)
        client.objects[remote + "/os-policy.json"] = json.dumps(
            dict(version=1, job_id=request["job_id"], os_status="not_requested")
        ).encode()
        with self.assertRaisesRegex(ValueError, "persisted OS intent"):
            load_analysis_request(
                client, remote + "/request.json", prefix, self.root / "policy-check"
            )

    def test_no_os_marks_only_os_stages_disabled_and_preserves_non_os_failures(self):
        manifest = self.pipeline_fixture()
        data = json.loads(manifest.read_text())
        data["icmp"]["os_status"] = "not_requested"
        data["icmp"]["ipid"]["no-connection"]["fixed-interval"] = {
            "mass": "icmp_2026-01-01_00-00-03"
        }
        manifest.write_text(json.dumps(data))
        report = self.root / "no-os-missing.json"
        with patch.object(
            postprocess, "render_os_strategy_plot", side_effect=AssertionError("OS plot executed")
        ):
            result = CliRunner().invoke(
                postprocess.app,
                [
                    str(manifest),
                    "--strict",
                    "--status-file",
                    str(report),
                    "--skip-itdk",
                    "--skip-ripe",
                    "--threads",
                    "1",
                ],
            )
        self.assertNotEqual(result.exit_code, 0)
        status = json.loads(report.read_text())
        self.assertFalse(status["required_complete"])
        disabled = [stage for stage in status["stages"] if stage["status"] == "disabled"]
        self.assertEqual(len(disabled), 3)
        self.assertTrue(
            all(not stage["required"] and stage["reason"] == "not_requested" for stage in disabled)
        )
        self.assertTrue(
            any(stage["required"] and stage["status"] == "failed" for stage in status["stages"])
        )

    def test_strict_pipeline_reports_skipped_mandatory_measurement_as_failure(self):
        manifest = self.pipeline_fixture()
        report = self.root / "status.json"
        with patch.object(
            postprocess, "classify_measurement", side_effect=FileNotFoundError("missing input")
        ):
            result = CliRunner().invoke(
                postprocess.app,
                [
                    str(manifest),
                    "--strict",
                    "--status-file",
                    str(report),
                    "--skip-itdk",
                    "--skip-ripe",
                ],
            )
        self.assertNotEqual(result.exit_code, 0)
        status = json.loads(report.read_text())
        self.assertFalse(status["required_complete"])
        self.assertTrue(
            any(stage["required"] and stage["status"] == "failed" for stage in status["stages"])
        )

    def test_optional_external_failure_does_not_prevent_core_completion(self):
        manifest = self.pipeline_fixture()
        report = self.root / "status.json"
        with (
            patch.object(postprocess, "prepare_itdk", side_effect=OSError("offline")),
            patch.object(postprocess, "prepare_ripe_atlas", side_effect=OSError("offline")),
        ):
            result = CliRunner().invoke(
                postprocess.app,
                [str(manifest), "--strict", "--status-file", str(report), "--threads", "1"],
            )
        self.assertEqual(result.exit_code, 0, f"{result.exception}\n{result.output}")
        status = json.loads(report.read_text())
        self.assertTrue(status["required_complete"])
        self.assertTrue(
            any(
                not stage["required"] and stage["status"] == "failed" for stage in status["stages"]
            )
        )


if __name__ == "__main__":
    unittest.main()
