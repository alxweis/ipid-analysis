"""Scheduling policy checks; no S3 calls, Internet scans, or bulk datasets."""
import io
import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from ipid_analysis import scheduler as s


class GeographicResourcesTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.env = patch.dict(os.environ, {
            "IPID_ANALYSIS_RESOURCE_POLICY": "geographic",
            "IPID_ANALYSIS_STORAGE_MODE": "local",
            "IPID_ANALYSIS_TMP_DIR": str(self.root / "tmp"),
            "IPID_ANALYSIS_RIPE_TRANSFER_DIR": str(self.root / "transfers"),
            "IPID_ANALYSIS_WORKER_MEMORY_BYTES": str(128 * s.GIB),
            "IPID_ANALYSIS_WORKER_SCRATCH_BYTES": str(200 * s.GIB),
            "IPID_ANALYSIS_BACKGROUND_SLOTS": "1",
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        for mocked in (
            patch.object(s, "available_memory", return_value=256 * s.GIB),
            patch.object(s, "cpu_budget", return_value=24),
            patch.object(s.os, "sched_getaffinity", return_value=set(range(24)), create=True),
            patch.object(s.shutil, "disk_usage", return_value=SimpleNamespace(free=280 * s.GIB)),
        ):
            mocked.start()
            self.addCleanup(mocked.stop)
        self.worker = s.Scheduler(Mock(), "s3://bucket/workflow", self.root)
        self.addCleanup(self.worker.close)

    def task(self, kind, protocol="icmp", priority=10, mid="one"):
        flow = f"s3://bucket/workflow/continental/full/sites/na/protocols/{protocol}"
        return s.Task(kind, f"{flow}/jobs/{mid}/request.json", flow,
                      "continental/full/na", priority)

    def running(self, task):
        active = s.Active(task, self.worker.cost(task), Mock(pid=2147483647),
                          self.root / task.key, io.StringIO(), time.monotonic())
        self.worker.active[task.key] = active

    def test_large_handoff_keeps_priority_and_gains_bulk_memory_and_scratch(self):
        for protocol in ("icmp", "tcp"):
            task = self.task("parallel-handoff", protocol, priority=0)
            self.assertEqual(task.priority, 0)
            self.assertEqual(self.worker.resource_profile(task), "large")
            cost = self.worker.cost(task)
            self.assertGreater(cost.memory, self.worker.critical.memory)
            self.assertGreater(cost.scratch, self.worker.critical.scratch)
            self.assertEqual(cost.cpus, 20)
        self.assertEqual(self.worker.resource_profile(
            self.task("parallel-handoff", "udp-dns", priority=0)), "medium")

    def test_cross_protocol_and_global_os_work_never_inherit_udp_small_input_budget(self):
        for kind in ("targets", "parallel-union", "interprotocol", "parallel-reference",
                     "parallel-reference-comparison", "parallel-os", "parallel-os-roles",
                     "parallel-measurement-os"):
            with self.subTest(kind=kind):
                self.assertEqual(self.worker.resource_profile(
                    self.task(kind, "udp-dns", priority=1)), "large")

    def test_three_udp_analyses_use_all_cpus_without_large_jobs(self):
        for i in range(3):
            task = self.task("parallel-measurement", "udp-dns", mid=str(i))
            self.assertTrue(self.worker.admitted(task))
            self.running(task)
        self.assertFalse(self.worker.admitted(
            self.task("parallel-core", "udp-dns", mid="fourth")))
        self.assertEqual(sum(a.cost.cpus for a in self.worker.active.values()), 24)

    def test_large_job_and_two_small_completions_fit_without_oversubscription(self):
        for task in (self.task("parallel-handoff", priority=0),
                     self.task("parallel-finish", mid="first"),
                     self.task("parallel-site", mid="second")):
            self.assertTrue(self.worker.admitted(task))
            self.running(task)
        for field in ("cpus", "memory", "scratch"):
            self.assertLessEqual(sum(getattr(a.cost, field) for a in self.worker.active.values()),
                                 getattr(self.worker.total, field))
        self.assertFalse(self.worker.admitted(self.task("parallel-core", mid="extra-large")))
        self.assertFalse(self.worker.admitted(self.task("parallel-finish", mid="extra-small")))

    def test_waiting_large_handoff_prevents_endless_small_backfill(self):
        self.running(self.task("parallel-core", mid="running"))
        handoff = self.task("parallel-handoff", priority=0, mid="waiting")
        completion = self.task("parallel-finish", mid="cheap")
        self.assertTrue(self.worker.admitted(completion))
        with patch.object(self.worker, "start") as start:
            self.worker.dispatch([completion, handoff])
        start.assert_not_called()
        self.worker.active.clear()
        seen = []

        def launch(task):
            seen.append(task.kind)
            self.running(task)

        with patch.object(self.worker, "start", side_effect=launch):
            self.worker.dispatch([completion, handoff])
        self.assertEqual(seen, ["parallel-handoff", "parallel-finish"])

    def test_profiles_do_not_bypass_real_memory_or_scratch_admission(self):
        task = self.task("parallel-handoff", priority=0)
        with patch.object(s, "available_memory", return_value=1):
            self.assertFalse(self.worker.admitted(task))
        with patch.object(s.shutil, "disk_usage", return_value=SimpleNamespace(free=1)):
            self.assertFalse(self.worker.admitted(task))

    def test_geographic_profile_and_limits_reach_the_child_specification(self):
        task = self.task("parallel-handoff", priority=0)
        with patch.object(s.subprocess, "Popen", return_value=Mock(pid=2147483647)) as launch:
            self.worker.start(task)
        spec = json.loads((self.worker.active[task.key].directory / "task.json").read_text())
        env = launch.call_args.kwargs["env"]
        self.assertEqual(spec["resource_profile"], "large")
        self.assertEqual(len(spec["cpu_ids"]), 20)
        self.assertEqual(int(env["IPID_ANALYSIS_JOB_MEMORY_BYTES"]), spec["cost"]["memory"])
        self.assertEqual(int(env["IPID_ANALYSIS_JOB_SCRATCH_BYTES"]), spec["cost"]["scratch"])
        self.assertEqual(env["IPID_ANALYSIS_DUCKDB_MEMORY"], f"{spec['cost']['memory'] // 2}B")
        self.worker.active[task.key].log.close()
        self.worker.active.clear()

    def test_only_disabled_external_role_work_is_small(self):
        task = self.task("parallel-roles")
        with patch.dict(os.environ, {"IPID_ANALYSIS_SKIP_ITDK": "1", "IPID_ANALYSIS_SKIP_RIPE": "true"}):
            self.assertEqual(self.worker.resource_profile(task), "small")
        with patch.dict(os.environ, {"IPID_ANALYSIS_SKIP_ITDK": "0"}):
            self.assertEqual(self.worker.resource_profile(task), "large")

    def test_policy_is_opt_in_and_invalid_policy_fails_preflight(self):
        with patch.dict(os.environ, {"IPID_ANALYSIS_RESOURCE_POLICY": "legacy"}):
            legacy = s.Scheduler(Mock(), self.worker.prefix, self.root / "legacy")
        self.addCleanup(legacy.close)
        task = self.task("parallel-handoff", priority=0)
        self.assertEqual(legacy.cost(task), legacy.critical)
        with patch.dict(os.environ, {"IPID_ANALYSIS_RESOURCE_POLICY": "typo"}):
            with self.assertRaisesRegex(ValueError, "resource policy"):
                s.Scheduler(Mock(), self.worker.prefix, self.root / "invalid")

    def test_ram_scratch_and_process_memory_share_the_physical_budget(self):
        ram = self.root / "ram"
        transfers = ram / "transfers"
        transfers.mkdir(parents=True)
        with (
            patch.object(s, "ram_scratch", return_value=True),
            patch.object(s, "ram_root", return_value=ram),
            patch.object(s, "ram_limit", return_value=64 * s.GIB),
            patch.object(s, "ripe_transfer_root", return_value=transfers),
        ):
            worker = s.Scheduler(Mock(), self.worker.prefix, self.root / "ram-worker")
        self.addCleanup(worker.close)
        self.assertLessEqual(worker.total.memory + worker.ram_bytes, 128 * s.GIB)
        self.assertLessEqual(worker.total.scratch + worker.transfer_limit, worker.ram_bytes)
        for field in ("cpus", "memory", "scratch"):
            self.assertLessEqual(
                getattr(worker.profiles["large"], field)
                + 2 * getattr(worker.profiles["small"], field),
                getattr(worker.total, field),
            )

    def test_profile_selection_never_reads_scientific_data_or_remote_metadata(self):
        client = Mock()
        self.worker.client = client
        for protocol in ("icmp", "tcp", "udp-dns"):
            for kind in ("parallel-handoff", "parallel-measurement", "parallel-core",
                         "parallel-union", "targets", "parallel-site"):
                task = self.task(kind, protocol)
                self.worker.cost(task)
                self.worker.admitted(task)
        self.assertEqual(client.mock_calls, [])


if __name__ == "__main__":
    unittest.main()
