import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from ipid_analysis import scheduler as s


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.env = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "tmp"),
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_WORKER_MEMORY_BYTES": str(128 * s.GIB),
                "IPID_ANALYSIS_WORKER_SCRATCH_BYTES": str(200 * s.GIB),
            },
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self.patches = [
            patch.object(s, "available_memory", return_value=256 * s.GIB),
            patch.object(s, "cpu_budget", return_value=24),
            patch.object(s.os, "sched_getaffinity", return_value=set(range(24)), create=True),
            patch.object(s.shutil, "disk_usage", return_value=SimpleNamespace(free=280 * s.GIB)),
        ]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)
        self.client = Mock()
        self.scheduler = s.Scheduler(self.client, "s3://bucket/workflow", self.root)
        self.addCleanup(self.scheduler.close)

    def active(self, task):
        cost = self.scheduler.cost(task)
        return s.Active(task, cost, Mock(), self.root / task.key, io.StringIO(), time.monotonic())

    def test_reserved_critical_capacity_and_global_cpu_ram_scratch_admission(self):
        background = s.Task(
            "analysis", "s3://bucket/analysis/request.json", "s3://bucket", priority=10
        )
        first = s.Task("handoff", "s3://bucket/jobs/1/request.json", "s3://bucket")
        second = s.Task("handoff", "s3://bucket/jobs/2/request.json", "s3://bucket")
        third = s.Task("handoff", "s3://bucket/jobs/3/request.json", "s3://bucket")
        self.scheduler.active[background.key] = self.active(background)
        self.assertTrue(self.scheduler.admitted(first))
        self.scheduler.active[first.key] = self.active(first)
        self.assertTrue(self.scheduler.admitted(second))
        self.scheduler.active[second.key] = self.active(second)
        self.assertFalse(self.scheduler.admitted(third))
        self.assertFalse(self.scheduler.admitted(background))
        for field in ("cpus", "memory", "scratch"):
            self.assertLessEqual(
                sum(getattr(a.cost, field) for a in self.scheduler.active.values()),
                getattr(self.scheduler.total, field),
            )

    def test_dispatch_prioritizes_handoffs_without_blocking_background(self):
        background = s.Task(
            "analysis", "s3://bucket/analysis/request.json", "s3://bucket", priority=10
        )
        critical = s.Task("handoff", "s3://bucket/jobs/1/request.json", "s3://bucket")
        seen = []

        def start(task):
            seen.append(task.kind)
            self.scheduler.active[task.key] = self.active(task)

        with patch.object(self.scheduler, "start", side_effect=start):
            self.scheduler.dispatch([background, critical])
        self.assertEqual(seen, ["handoff", "analysis"])
        another = s.Task("targets", "s3://bucket/targets/request.json", "s3://bucket", priority=2)
        self.assertTrue(self.scheduler.admitted(another))

    def test_process_environment_caps_every_descendant_and_allocates_disjoint_cpus(self):
        task = s.Task(
            "handoff", "s3://bucket/jobs/1/request.json", "s3://bucket", "continental/round/eu"
        )
        fake = Mock(pid=123)
        with patch.object(s.subprocess, "Popen", return_value=fake) as launch:
            self.scheduler.start(task)
        env = launch.call_args.kwargs["env"]
        spec = json.loads((self.scheduler.active[task.key].directory / "task.json").read_text())
        self.assertEqual(env["IPID_ANALYSIS_SCOPE"], "continental/round/eu")
        self.assertEqual(
            env["IPID_ANALYSIS_JOB_MEMORY_BYTES"], str(self.scheduler.critical.memory)
        )
        self.assertEqual(len(spec["cpu_ids"]), self.scheduler.critical.cpus)
        self.assertEqual(env["OPENBLAS_NUM_THREADS"], str(self.scheduler.critical.cpus))
        self.assertEqual(env["IPID_ANALYSIS_WORKFLOW_ROOT"], self.scheduler.prefix)
        self.scheduler.active[task.key].log.close()

    def test_memory_and_scratch_watchdog_terminate_and_publish_failure(self):
        task = s.Task("handoff", "s3://bucket/jobs/1/request.json", "s3://bucket")
        active = self.active(task)
        active.process.poll.return_value = None
        active.process.returncode = -15
        self.scheduler.active[task.key] = active

        def stop(a):
            a.process.poll.return_value = -15

        with (
            patch.object(s, "directory_bytes", return_value=active.cost.scratch + 1),
            patch.object(self.scheduler, "terminate", side_effect=stop) as terminate,
            patch("ipid_analysis.continental.publish_task_failure") as failure,
        ):
            self.scheduler.reap()
            for future in self.scheduler.failure_futures.values():
                future.result(timeout=5)
        terminate.assert_called_once()
        failure.assert_called_once()
        self.assertFalse(self.scheduler.active)
        self.assertTrue(self.scheduler.errors)

    def test_busy_lease_requeues_without_terminal_failure(self):
        task = s.Task("handoff", "s3://bucket/jobs/1/request.json", "s3://bucket")
        active = self.active(task)
        active.process.poll.return_value = 75
        active.process.returncode = 75
        self.scheduler.active[task.key] = active
        with patch("ipid_analysis.continental.publish_task_failure") as failure:
            self.scheduler.reap()
        failure.assert_not_called()
        self.assertGreater(self.scheduler.retry_after[task.key], time.monotonic())

    def test_once_performs_listing_even_for_empty_queue(self):
        self.client.objects.return_value = []
        self.scheduler.run(once=True, poll_interval=1)
        self.client.objects.assert_called_once_with(self.scheduler.prefix)

    def test_insufficient_free_scratch_and_memory_do_not_start_work(self):
        task = s.Task("handoff", "s3://bucket/jobs/1/request.json", "s3://bucket")
        with patch.object(s.shutil, "disk_usage", return_value=SimpleNamespace(free=1)):
            self.assertFalse(self.scheduler.admitted(task))
        with patch.object(s, "available_memory", return_value=1):
            self.assertFalse(self.scheduler.admitted(task))

    @unittest.skipUnless(sys.platform == "linux", "Linux production process supervision")
    def test_parent_death_terminates_task_and_grandchild(self):
        pidfile = self.root / "pids.json"
        child_code = "from ipid_analysis.workflow_task import bind_parent; bind_parent(); import subprocess,sys,time,json; from pathlib import Path; p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']); Path(sys.argv[1]).write_text(json.dumps([__import__(\"os\").getpid(),p.pid])); time.sleep(60)"
        parent_code = "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',sys.argv[1],sys.argv[2]],start_new_session=True); time.sleep(60)"
        parent = subprocess.Popen([sys.executable, "-c", parent_code, child_code, str(pidfile)])
        self.addCleanup(lambda: parent.poll() is None and parent.kill())
        deadline = time.monotonic() + 10
        while not pidfile.is_file() and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertTrue(pidfile.is_file())
        pids = json.loads(pidfile.read_text())
        parent.kill()
        parent.wait(timeout=5)

        def live(pid):
            try:
                return (Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]) != "Z"
            except FileNotFoundError:
                return False

        deadline = time.monotonic() + 5
        while any(live(pid) for pid in pids) and time.monotonic() < deadline:
            time.sleep(0.05)
        for pid in pids:
            if live(pid):
                os.kill(pid, signal.SIGKILL)
        self.assertFalse(any(live(pid) for pid in pids))

    def test_global_job_lease_survives_different_child_scratch_roots(self):
        lockdir = self.root / "global-locks"
        code = "import sys; from ipid_analysis.runtime import exclusive_lock;\ntry:\n with exclusive_lock('job:round:eu:1',timeout_seconds=0): print('owned',flush=True); sys.stdin.read()\nexcept TimeoutError: sys.exit(75)"
        env = dict(
            os.environ,
            IPID_ANALYSIS_LOCK_DIR=str(lockdir),
            IPID_ANALYSIS_TMP_DIR=str(self.root / "first"),
        )
        first = subprocess.Popen(
            [sys.executable, "-c", code],
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            self.assertEqual(first.stdout.readline().strip(), "owned")
            env["IPID_ANALYSIS_TMP_DIR"] = str(self.root / "second")
            second = subprocess.run(
                [sys.executable, "-c", code],
                env=env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(second.returncode, 75, second.stderr)
        finally:
            first.communicate(input="", timeout=10)


if __name__ == "__main__":
    unittest.main()
