from contextlib import contextmanager
from dataclasses import asdict
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

import pyarrow as pa
import pyarrow.parquet as pq
from tests.test_continental import FakeClient

from ipid_analysis import parallel as p
from ipid_analysis.artifacts import file_digest
from ipid_analysis.scheduler import Task, tasks_from_objects

ROOT = "s3://bucket/workflow"
CAMPAIGN = ROOT + "/continental/round"
SITE = CAMPAIGN + "/sites/eu"
FLOW = SITE + "/protocols/icmp"
RAW = "s3://bucket/raw"


class ParallelTests(unittest.TestCase):
    def test_geographic_roles_forward_ripe_lookback(self):
        for days, expected in ((None, 14), ("28", 28)):
            with self.subTest(days=days):
                task = Task("parallel-roles", FLOW + "/strategy-jobs/run/request.json", FLOW, "continental/round/eu", 10)
                env = dict(IPID_ANALYSIS_SKIP_ITDK="1", IPID_ANALYSIS_SKIP_RIPE="0", IPID_ANALYSIS_RIPE_CAMPAIGN_START="2026-11-07T01:00:00Z")
                with (
                    patch.dict(os.environ, env),
                    patch.object(p, "protocol_request", return_value=(SimpleNamespace(job_id="run"), {})),
                    patch.object(p, "sources", return_value=[]),
                    patch("ipid_analysis.ripe_atlas.prepare_ripe_atlas") as prepare,
                ):
                    if days is None:
                        os.environ.pop("IPID_ANALYSIS_RIPE_LOOKBACK_DAYS", None)
                    else:
                        os.environ["IPID_ANALYSIS_RIPE_LOOKBACK_DAYS"] = days
                    p.roles(self.client, task, self.work, 4)
                self.assertEqual(prepare.call_args.kwargs["lookback_days"], expected)
                self.assertEqual(json.loads(self.client.values[p.success_uri(task)])["optional_errors"], [])

    def test_missing_ripe_dump_is_reported_as_optional_geo_error(self):
        task = Task("parallel-roles", FLOW + "/strategy-jobs/run/request.json", FLOW, "continental/round/eu", 10)
        core = FLOW + "/core-jobs/run/done.json"
        self.client.json(core, {"finished": True})
        with (
            patch.dict(os.environ, dict(IPID_ANALYSIS_SKIP_ITDK="1", IPID_ANALYSIS_SKIP_RIPE="0", IPID_ANALYSIS_RIPE_LOOKBACK_DAYS="28", IPID_ANALYSIS_RIPE_CAMPAIGN_START="2026-11-07T01:00:00Z")),
            patch.object(p, "protocol_request", return_value=(SimpleNamespace(job_id="run"), {})),
            patch.object(p, "sources", return_value=[]),
            patch("ipid_analysis.ripe_atlas.prepare_ripe_atlas", side_effect=HTTPError("https://example.invalid/dump", 404, "Not Found", None, None)),
        ):
            p.roles(self.client, task, self.work, 4)
        result = json.loads(self.client.values[p.success_uri(task)])
        self.assertEqual(len(result["optional_errors"]), 1)
        self.assertIn("404", result["optional_errors"][0])
        self.assertEqual(json.loads(self.client.values[core]), {"finished": True})
        self.assertNotIn(p.success_uri(task).replace("done.json", "failed.json"), self.client.values)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.work = self.root / "work"
        self.work.mkdir()
        self.client = FakeClient()
        env = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
                "IPID_ANALYSIS_CONTROL_ROOT": str(self.root / "control"),
                "IPID_ANALYSIS_S3_ROOTS": "{}",
            },
        )
        env.start()
        self.addCleanup(env.stop)
        for target in ("PROCESSED_STORAGE_DIR",):
            patcher = patch.object(p, target, self.root / "processed")
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client.json(
            CAMPAIGN + "/campaign.json",
            dict(
                version=1,
                campaign_id="round",
                sites=list(p.SITES),
                protocols=list(p.PROTOCOLS),
                raw_root=RAW,
            ),
        )
        self.client.json(
            CAMPAIGN + "/parallel.json",
            dict(version=1, mode="parallel-protocols", campaign_id="round"),
        )

    def event(self, site, protocol):
        mid = {"icmp": "icmp", "tcp": "tcp-80", "udp-dns": "udp-dns-53"}[
            protocol
        ] + "_2026-10-25_01-00-00"
        raw = f"{RAW}/continental/round/{site}/zmap"
        source = self.root / "source.pq"
        pq.write_table(
            pa.table(
                dict(IP_ADDR=["192.0.2.1", "192.0.2.1", "198.51.100.2"], REPLY_TYPE=["", "", ""])
            ),
            source,
        )
        self.client.values[f"{raw}/{mid}/zmap.pq"] = source.read_bytes()
        self.client.json(
            f"{raw}/{mid}/measurement-complete.json",
            dict(
                version=1,
                measurement_id=mid,
                files={
                    "zmap.pq": dict(size_bytes=source.stat().st_size, sha256=file_digest(source))
                },
            ),
        )
        flow = f"{CAMPAIGN}/sites/{site}/protocols/{protocol}"
        self.client.json(
            flow + "/discovery.json",
            dict(
                version=1,
                campaign_id="round",
                site=site,
                protocol=protocol,
                measurement_id=mid,
                raw_prefix=raw,
            ),
        )
        return Task(
            "parallel-discovery", flow + "/discovery.json", flow, f"continental/round/{site}", 1
        )

    def test_streaming_and_core_are_not_gated_on_os_or_other_continents(self):
        uri = FLOW + "/strategy-jobs/icmp_2026-10-25_01-00-00/request.json"
        objects = {
            CAMPAIGN + "/parallel.json",
            uri,
            FLOW + "/measurement-jobs/icmp_2026-10-25_01-00-00/request.json",
            FLOW + "/jobs/icmp_2026-10-25_01-00-00/request.json",
        }
        kinds = {t.kind for t in tasks_from_objects(objects, ROOT)}
        self.assertTrue(
            {"parallel-handoff", "parallel-strategies", "parallel-measurement"} <= kinds
        )
        self.assertNotIn("parallel-core", kinds)
        objects.add(uri.replace("request.json", "done.json"))
        kinds = {t.kind for t in tasks_from_objects(objects, ROOT)}
        self.assertIn("parallel-core", kinds)
        self.assertNotIn("parallel-os", kinds)

    def test_incremental_discoveries_and_union_are_durable_and_deduplicated(self):
        for site in p.SITES:
            for protocol in p.PROTOCOLS:
                task = self.event(site, protocol)
                p.discovery(self.client, task, self.work)
                self.assertIn(p.success_uri(task), self.client.values)
        objects = set(self.client.values)
        task = next(t for t in tasks_from_objects(objects, ROOT) if t.kind == "parallel-union")
        p.union(self.client, task, self.work)
        request = self.client.value(CAMPAIGN + "/os/request.json")
        self.assertEqual(len(request["members"]), 18)
        self.assertEqual(request["target"]["rows"], 2)
        self.assertEqual(self.client.writes[-1], CAMPAIGN + "/os/request.json")
        missing = objects - {CAMPAIGN + "/sites/au/protocols/tcp/discovery-ready.json"}
        self.assertNotIn("parallel-union", {t.kind for t in tasks_from_objects(missing, ROOT)})

    def test_stale_listing_cannot_rewrite_committed_discovery(self):
        from ipid_analysis import s3_workflow, workflow_task

        task = self.event("eu", "icmp")
        stale_objects = set(self.client.values)
        spec = dict(task=asdict(task), batch_size=10, cost={"cpus": 1})
        with patch.object(s3_workflow, "S3Client", return_value=self.client):
            self.assertEqual(workflow_task.execute(spec), 0)
            original = dict(self.client.values)
            original_writes = list(self.client.writes)
            partition = Path(self.client.value(p.success_uri(task))["partition"])
            original_bytes = partition.read_bytes()
            original_mtime = partition.stat().st_mtime_ns
            # A listing started before publication can still offer this job.
            offered = next(t for t in tasks_from_objects(stale_objects, ROOT)
                           if t.kind == "parallel-discovery")
            self.assertEqual(offered.key, task.key)
            with patch.object(p, "discovery", side_effect=AssertionError("discovery repeated")):
                self.assertEqual(workflow_task.execute(spec), 0)
        self.assertEqual(self.client.values, original)
        self.assertEqual(self.client.writes, original_writes)
        self.assertEqual(partition.read_bytes(), original_bytes)
        self.assertEqual(partition.stat().st_mtime_ns, original_mtime)

    def test_completion_is_rechecked_after_acquiring_the_job_lease(self):
        from ipid_analysis import runtime, s3_workflow, workflow_task

        task = self.event("eu", "icmp")
        acquire = runtime.exclusive_lock

        @contextmanager
        def completed_while_acquiring(identity, **kwargs):
            with acquire(identity, **kwargs):
                self.client.json(p.success_uri(task), {"version": 1})
                yield

        with (
            patch.object(s3_workflow, "S3Client", return_value=self.client),
            patch.object(runtime, "exclusive_lock", completed_while_acquiring),
            patch.object(p, "execute", side_effect=AssertionError("completed task executed")),
        ):
            self.assertEqual(workflow_task.execute(
                dict(task=asdict(task), batch_size=10, cost={"cpus": 1})
            ), 0)

    def test_completed_legacy_handoff_is_not_repeated(self):
        from ipid_analysis import s3_workflow, workflow_task

        uri = ROOT + "/jobs/icmp_2026-10-25_01-00-00/request.json"
        task = Task("handoff", uri, ROOT)
        self.client.json(uri.replace("request.json", "done.json"), {"version": 1})
        original = dict(self.client.values)
        with (
            patch.object(s3_workflow, "S3Client", return_value=self.client),
            patch.object(s3_workflow, "process_request", side_effect=AssertionError("handoff repeated")),
        ):
            self.assertEqual(workflow_task.execute(
                dict(task=asdict(task), batch_size=10, cost={"cpus": 1})
            ), 0)
        self.assertEqual(self.client.values, original)

    def test_corrupt_partition_or_upload_cannot_start_os(self):
        for site in p.SITES:
            for protocol in p.PROTOCOLS:
                task = self.event(site, protocol)
                p.discovery(self.client, task, self.work)
        ready = self.client.value(FLOW + "/discovery-ready.json")
        Path(ready["partition"]).write_bytes(b"corrupt")
        task = Task("parallel-union", CAMPAIGN + "/union/request.json", CAMPAIGN, priority=1)
        with self.assertRaises(ValueError):
            p.union(self.client, task, self.work)
        self.assertNotIn(CAMPAIGN + "/os/request.json", self.client.values)

    def test_failure_markers_are_stage_specific_and_terminal(self):
        uri = FLOW + "/strategy-jobs/icmp_2026-10-25_01-00-00/request.json"
        core = Task("parallel-core", uri, FLOW)
        strategy = Task("parallel-strategies", uri, FLOW)
        self.assertNotEqual(p.failure_uri(core), p.failure_uri(strategy))
        objects = {
            CAMPAIGN + "/parallel.json",
            uri,
            uri.replace("request.json", "done.json"),
            p.failure_uri(core),
        }
        self.assertNotIn("parallel-core", {t.kind for t in tasks_from_objects(objects, ROOT)})

    def test_core_and_os_references_do_not_need_final_or_interprotocol(self):
        job = "icmp_2026-10-25_01-00-00"
        uri = f"{FLOW}/strategy-jobs/{job}/request.json"
        objects = {
            CAMPAIGN + "/parallel.json",
            uri,
            f"{FLOW}/core-jobs/{job}/done.json",
            CAMPAIGN + "/os/done.json",
        }
        kinds = {t.kind for t in tasks_from_objects(objects, ROOT)}
        self.assertIn("parallel-os", kinds)
        self.assertIn("parallel-roles", kinds)
        self.assertNotIn("parallel-site", kinds)

    def test_cross_site_discovery_is_rejected(self):
        task = self.event("eu", "icmp")
        value = self.client.value(task.uri)
        value["site"] = "as"
        self.client.json(task.uri, value)
        with self.assertRaisesRegex(ValueError, "discovery"):
            p.discovery(self.client, task, self.work)

    def test_os_for_one_measurement_does_not_wait_for_protocol_core(self):
        request = FLOW + "/measurement-jobs/icmp_2026-10-25_01-00-00/request.json"
        objects = {
            CAMPAIGN + "/parallel.json",
            CAMPAIGN + "/os/done.json",
            request,
            request.replace("request.json", "done.json"),
        }
        kinds = {t.kind for t in tasks_from_objects(objects, ROOT)}
        self.assertIn("parallel-measurement-os", kinds)
        self.assertNotIn("parallel-core", kinds)

    def test_explicit_retry_archives_only_its_failed_stage(self):
        task = self.event("eu", "icmp")
        failed = p.failure_uri(task)
        self.client.json(failed, {"error": "temporary upload failure"})
        original = dict(self.client.values)
        self.assertTrue(p.retry(self.client, task.uri, ROOT))
        self.assertNotIn(failed, self.client.values)
        archived = [u for u in self.client.values if "/attempts/" in u]
        self.assertEqual(len(archived), 1)
        self.assertEqual(self.client.values[archived[0]], original[failed])
        for u, v in original.items():
            if u != failed:
                self.assertEqual(self.client.values[u], v)
        self.client.json(p.success_uri(task), {"version": 1})
        self.client.json(failed, {"error": "must remain"})
        with self.assertRaises(ValueError):
            p.retry(self.client, task.uri, ROOT)
        self.assertIn(failed, self.client.values)

    def test_protocol_finish_rechecks_every_streaming_completion(self):
        request = SimpleNamespace(
            job_id="icmp_job",
            protocol="icmp",
            manifest_uri=FLOW + "/analysis-jobs/icmp_job/manifest.json",
        )
        task = Task("parallel-finish", FLOW + "/strategy-jobs/icmp_job/request.json", FLOW)
        ids = ["icmp_" + str(i) for i in range(8)]
        for category in ("core-jobs", "role-jobs"):
            self.client.json(FLOW + f"/{category}/icmp_job/done.json", dict(job_id=request.job_id))
        for mid in ids[:-1]:
            self.client.json(FLOW + f"/measurement-jobs/{mid}/done.json", dict(measurement_id=mid))
        with (
            patch.object(p, "protocol_request", return_value=(request, {})),
            patch.object(p, "descriptor", return_value=dict(os_mode="disabled")),
            patch(
                "ipid_analysis.manifest.iter_ipid_measurements",
                return_value=[SimpleNamespace(measurement_id=mid) for mid in ids],
            ),
            patch("ipid_analysis.manifest.iter_random_reproducibility", return_value=[]),
        ):
            with self.assertRaises(KeyError):
                p.finish(self.client, task, self.work)
            self.client.json(
                FLOW + f"/measurement-jobs/{ids[-1]}/done.json", dict(measurement_id="wrong")
            )
            with self.assertRaisesRegex(ValueError, "identity"):
                p.finish(self.client, task, self.work)
            self.client.json(
                FLOW + f"/measurement-jobs/{ids[-1]}/done.json", dict(measurement_id=ids[-1])
            )
            p.finish(self.client, task, self.work)
            self.assertTrue(self.client.exists(p.success_uri(task)))


if __name__ == "__main__":
    unittest.main()
