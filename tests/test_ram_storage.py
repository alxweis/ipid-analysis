"""RAM data paths, unchanged publication semantics and combined resource budgets."""

import bz2
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack
from datetime import date, datetime, timezone
import io
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import artifacts, downloads, runtime, scheduler


def prepare_in_ram(arguments):
    """Independent Linux processes share RAM transfers and disk control locks."""
    from ipid_analysis import ripe_atlas as ripe

    root, ram, job, day = map(str, arguments)
    os.environ.update(
        IPID_ANALYSIS_STORAGE_MODE="local",
        IPID_ANALYSIS_SCRATCH_STORAGE="ram",
        IPID_ANALYSIS_CONTROL_ROOT=root + "/control",
        IPID_ANALYSIS_RAM_DIR=ram,
        IPID_ANALYSIS_RAM_JOB_DIR=ram + "/" + job,
        IPID_ANALYSIS_RAM_BYTES=str(16 << 20),
        IPID_ANALYSIS_RAM_RESERVE_BYTES="0",
        IPID_ANALYSIS_RIPE_TRANSFER_DIR=ram + "/ripe-transfers",
        IPID_ANALYSIS_RIPE_TRANSFER_BYTES=str(8 << 20),
        IPID_ANALYSIS_RIPE_CACHE_S3="",
    )
    payload = bz2.compress(
        (
            json.dumps(
                {
                    "af": 4,
                    "timestamp": 1768478400,
                    "dst_addr": "1.1.1.1",
                    "destination_ip_responded": True,
                }
            )
            + "\n"
        ).encode()
    )

    def get(request, **kwargs):
        with (Path(root) / "http-calls").open("a") as stream:
            stream.write(request.full_url + "\n")
        response = io.BytesIO(payload)
        response.status = 200
        response.headers = {"Content-Length": str(len(payload)), "ETag": '"fixture"'}
        return response

    with (
        patch.object(ripe, "_available_dump_days", return_value=(date(2026, 1, 15),)),
        patch.object(ripe, "_remote_file_size", return_value=len(payload)),
        patch.object(ripe, "urlopen", side_effect=get),
    ):
        result = ripe.prepare_ripe_atlas(
            campaign_start=datetime(2026, 1, int(day), tzinfo=timezone.utc),
            dump_samples=1,
            preprocess_workers=1,
            raw_root=Path(root) / "fake-s3-raw",
            processed_root=Path(root) / "fake-s3-processed",
        )
    return pq.read_table(result.roles_path).num_rows


class RamStorageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ram = self.root / "ram"
        self.ram.mkdir()
        self.environment = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_SCRATCH_STORAGE": "ram",
                "IPID_ANALYSIS_RAM_BYTES": str(32 << 20),
                "IPID_ANALYSIS_RAM_RESERVE_BYTES": "0",
                "IPID_ANALYSIS_RAM_JOB_DIR": str(self.ram / "job"),
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "control"),
                "IPID_ANALYSIS_CONTROL_ROOT": str(self.root / "control"),
                "IPID_ANALYSIS_LOCK_DIR": str(self.root / "control" / "locks"),
            },
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)
        for target, value in (("ram_root", self.ram), ("filesystem_type", "tmpfs")):
            mock = patch.object(runtime, target, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)

    def test_mounted_mode_defaults_to_ram_but_local_compatibility_defaults_to_disk(self):
        with patch.dict(os.environ, {"IPID_ANALYSIS_SCRATCH_STORAGE": ""}):
            with self.assertRaises(ValueError):
                runtime.ram_scratch()
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(runtime.ram_scratch())
            with patch.dict(os.environ, {"IPID_ANALYSIS_STORAGE_MODE": "mounted-s3"}):
                self.assertTrue(runtime.ram_scratch())

    def test_bulk_outputs_and_downloads_use_ram_but_control_and_locks_use_disk(self):
        self.assertIn(self.ram, runtime.temporary_path(Path("output.pq")).parents)
        self.assertIn(self.ram, runtime.work_path("http://fixture").parents)
        for category in ("locks", "terminal", "retry"):
            self.assertIn(
                runtime.control_root(), runtime.work_path("key", category=category).parents
            )
        with runtime.exclusive_lock("fixture"):
            self.assertTrue(list((runtime.control_root() / "locks").rglob("owner.lock")))
        self.assertFalse(list(runtime.control_root().rglob("*.pq")))

    def test_explicit_lock_override_is_honored_outside_the_control_directory(self):
        custom = self.root / "custom-locks"
        with patch.dict(os.environ, {"IPID_ANALYSIS_LOCK_DIR": str(custom)}):
            with runtime.exclusive_lock("fixture"):
                self.assertEqual(len(list(custom.rglob("owner.lock"))), 1)
        self.assertFalse(list(self.ram.rglob("owner.lock")))

    def test_enrichment_retry_cleans_only_its_control_namespace_in_ram_mode(self):
        from ipid_analysis.s3_workflow import retry_failed_request

        uri = "s3://test/workflow/continental/campaign/enrichments/os/request.json"
        root = runtime.work_path(uri, category="retry")
        sentinel = runtime.control_root() / "keep.json"
        sentinel.write_text("keep")
        other = runtime.work_path("unrelated", category="retry")
        (other / "keep.json").write_text("keep")
        with patch("ipid_analysis.os_enrichment.retry", return_value=True):
            retry_failed_request(Mock(), uri, "s3://test/workflow")
        self.assertFalse(root.exists())
        self.assertEqual(sentinel.read_text(), "keep")
        self.assertTrue((other / "keep.json").exists())
        with self.assertRaisesRegex(ValueError, "outside owned"):
            runtime.cleanup_work(sentinel, root=runtime.control_root() / "retry")

    def test_job_directory_cannot_escape_ram_or_use_an_ext4_submount(self):
        with patch.dict(os.environ, {"IPID_ANALYSIS_RAM_JOB_DIR": str(self.root / "disk")}):
            with self.assertRaisesRegex(ValueError, "inside"):
                runtime.scratch_root()
        with patch.object(runtime, "filesystem_type", return_value="ext4"):
            with self.assertRaisesRegex(OSError, "disk filesystem"):
                runtime.scratch_root()

    def test_global_and_job_ram_capacity_include_existing_files(self):
        (self.ram / "retained-transfer").write_bytes(b"x" * 100)
        job = runtime.scratch_root()
        (job / "output").write_bytes(b"x" * 20)
        with patch.dict(
            os.environ, {"IPID_ANALYSIS_RAM_BYTES": "150", "IPID_ANALYSIS_JOB_SCRATCH_BYTES": "40"}
        ):
            self.assertEqual(runtime.scratch_free(), 20)
            with self.assertRaisesRegex(OSError, "unreserved"):
                with runtime.reserve_download(21):
                    pass
            with runtime.reserve_download(15):
                with self.assertRaises(OSError):
                    with runtime.reserve_download(10):
                        pass

    def test_unknown_length_http_cannot_exceed_ram_budget(self):
        body = io.BytesIO(b"larger than remaining budget")
        body.status, body.headers = 200, {}
        target = runtime.work_path("http://fixture") / "dump.bz2"
        with patch.object(downloads, "scratch_free", return_value=2):
            with self.assertRaisesRegex(ValueError, "RAM scratch budget"):
                downloads.download("http://fixture", target, opener=lambda *a, **k: body)
        self.assertEqual(target.with_suffix(".bz2.part").stat().st_size, 0)
        self.assertFalse(target.exists())

    def test_http_resume_reuses_retained_ram_partial_after_worker_restart(self):
        payload = b"complete HTTP object"
        target = runtime.work_path("http://fixture") / "dump.bz2"

        def response(data, status=200, offset=0):
            stream = io.BytesIO(data)
            stream.status = status
            stream.headers = {"Content-Length": str(len(payload) - offset), "ETag": '"same"'}
            if offset:
                stream.headers["Content-Range"] = (
                    f"bytes {offset}-{len(payload) - 1}/{len(payload)}"
                )
            return stream

        with self.assertRaises(OSError):
            downloads.download(
                "http://fixture",
                target,
                max_attempts=1,
                opener=lambda *a, **k: response(payload[:5]),
            )
        self.assertEqual(target.with_suffix(".bz2.part").read_bytes(), payload[:5])

        def resume(request, **kwargs):
            self.assertEqual(request.get_header("Range"), "bytes=5-")
            return response(payload[5:], 206, 5)

        downloads.download("http://fixture", target, opener=resume)
        self.assertEqual(target.read_bytes(), payload)

    def test_ripe_download_publishes_raw_once_and_reuses_parts_across_windows(self):
        from ipid_analysis import ripe_atlas as ripe

        payload = bz2.compress(
            (
                json.dumps(
                    {
                        "af": 4,
                        "timestamp": 1768478400,
                        "dst_addr": "1.1.1.1",
                        "src_addr": "10.0.0.1",
                        "destination_ip_responded": True,
                        "result": [
                            {"hop": 1, "result": [{"from": "8.8.8.8"}]},
                            {"hop": 2, "result": [{"from": "1.1.1.1"}]},
                        ],
                    }
                )
                + "\n"
            ).encode()
        )

        def get(request, **kwargs):
            response = io.BytesIO(payload)
            response.status = 200
            response.headers = {"Content-Length": str(len(payload)), "ETag": '"same"'}
            return response

        with (
            patch.dict(
                os.environ,
                {
                    "IPID_ANALYSIS_RIPE_TRANSFER_DIR": str(self.ram / "ripe-transfers"),
                    "IPID_ANALYSIS_RIPE_TRANSFER_BYTES": str(8 << 20),
                    "IPID_ANALYSIS_RIPE_CACHE_S3": "",
                },
            ),
            patch.object(ripe, "_available_dump_days", return_value=(date(2026, 1, 15),)),
            patch.object(ripe, "_remote_file_size", return_value=len(payload)),
            patch.object(ripe, "urlopen", side_effect=get) as http,
            patch.object(ripe, "_build_role_part", wraps=ripe._build_role_part) as parser,
        ):
            options = dict(
                dump_samples=1,
                preprocess_workers=1,
                raw_root=self.root / "s3-raw",
                processed_root=self.root / "s3-processed",
            )
            with patch.object(ripe, "_build_role_part", side_effect=RuntimeError("parser interrupted")):
                with self.assertRaisesRegex(RuntimeError, "parser interrupted"):
                    ripe.prepare_ripe_atlas(
                        campaign_start=datetime(2026, 1, 29, tzinfo=timezone.utc), **options
                    )
            self.assertEqual(len(list((self.root / "s3-raw").rglob("*.bz2"))), 1)
            first = ripe.prepare_ripe_atlas(
                campaign_start=datetime(2026, 1, 29, tzinfo=timezone.utc), **options
            )
            self.assertEqual(pq.read_table(first.roles_path).num_rows, 2)
            self.assertEqual(len(list((self.root / "s3-raw").rglob("*.bz2"))), 1)
            self.assertFalse(list((self.ram / "ripe-transfers/downloads").rglob("*.bz2")))
            with patch.dict(
                os.environ, {"IPID_ANALYSIS_RAM_JOB_DIR": str(self.ram / "another-job")}
            ):
                ripe.prepare_ripe_atlas(
                    campaign_start=datetime(2026, 1, 30, tzinfo=timezone.utc), **options
                )
            self.assertEqual(http.call_count, 1)
            self.assertEqual(parser.call_count, 1)
            self.assertEqual(
                ripe._cache_lock_path("shared", "part.lock").parent,
                runtime.control_root() / "ripe-locks",
            )

    def test_itdk_reuses_committed_raw_download_without_another_http_request(self):
        from ipid_analysis import caida_itdk as itdk

        destination = self.root / "fake-s3" / "interfaces.bz2"
        source = runtime.temporary_path(destination)
        source.write_bytes(b"compressed-fixture")
        artifacts.publish_file(source, destination, parameters={"source_url": "http://fixture"})
        with patch.object(itdk, "download_http") as http:
            self.assertEqual(itdk._download("http://fixture", destination), destination)
        http.assert_not_called()

    def test_native_publication_reads_closed_ram_files_and_preserves_marker_integrity(self):
        destination = self.root / "s3" / "table.pq"
        sources = []

        def put(source, target):
            self.assertIn(self.ram, source.parents)
            sources.append(source)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)

        with (
            patch.object(artifacts, "mounted_storage", return_value=True),
            patch.object(artifacts, "persistent_roots", return_value=(destination.parent,)),
            patch.object(artifacts, "validate_storage"),
            patch.object(artifacts, "native_put", side_effect=put),
        ):
            artifacts.write_table(pa.table({"IP_ADDR": ["1.1.1.1"]}), destination)
        self.assertEqual(pq.read_table(destination).num_rows, 1)
        artifacts.assert_complete(destination)
        self.assertEqual(len(sources), 3)
        previous = destination.read_bytes()
        with self.assertRaises(RuntimeError):
            with artifacts.atomic_output(destination) as partial:
                partial.write_bytes(b"interrupted")
                raise RuntimeError("interrupt")
        self.assertEqual(destination.read_bytes(), previous)

    def test_over_budget_output_cannot_replace_a_committed_artifact(self):
        destination = self.root / "s3" / "result.json"
        destination.parent.mkdir()
        destination.write_text("previous")
        with patch.dict(os.environ, {"IPID_ANALYSIS_JOB_SCRATCH_BYTES": "10"}):
            with self.assertRaisesRegex(OSError, "RAM scratch budget exceeded"):
                with artifacts.atomic_output(destination) as temporary:
                    temporary.write_bytes(b"x" * 11)
        self.assertEqual(destination.read_text(), "previous")

    def test_duckdb_uses_ram_spill_and_cleans_it_after_failure(self):
        with self.assertRaisesRegex(RuntimeError, "interrupt"):
            with runtime.connect(config={"memory_limit": "32MB", "threads": 1}) as con:
                directory = Path(
                    con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
                )
                self.assertIn(self.ram, directory.parents)
                self.assertEqual(
                    con.execute("SELECT sum(i) FROM range(1000) t(i)").fetchone()[0], 499500
                )
                raise RuntimeError("interrupt")
        self.assertFalse(directory.exists())

    def test_ram_scheduler_reserves_tmpfs_in_addition_to_compute_ram_and_keeps_critical_slots(
        self,
    ):
        gib = 1 << 30
        with ExitStack() as stack:
            stack.enter_context(
                patch.dict(
                    os.environ,
                    {
                        "IPID_ANALYSIS_RAM_BYTES": str(32 * gib),
                        "IPID_ANALYSIS_WORKER_MEMORY_BYTES": str(128 * gib),
                        "IPID_ANALYSIS_RIPE_TRANSFER_DIR": str(self.ram / "ripe-transfers"),
                    },
                )
            )
            for name, value in (
                ("ram_root", self.ram),
                ("ram_scratch", True),
                ("available_memory", 256 * gib),
                ("cpu_budget", 24),
                ("ram_limit", 32 * gib),
            ):
                stack.enter_context(patch.object(scheduler, name, return_value=value))
            stack.enter_context(
                patch.object(
                    scheduler.shutil, "disk_usage", return_value=SimpleNamespace(free=64 * gib)
                )
            )
            worker = scheduler.Scheduler(Mock(), "s3://test/workflow", self.root / "control")
            self.addCleanup(worker.close)
            self.assertEqual(worker.total.memory + worker.ram_bytes, 128 * gib)
            self.assertEqual(worker.total.scratch + worker.transfer_limit, worker.ram_bytes)
            tasks = [
                scheduler.Task("analysis", "analysis", "root", priority=10),
                scheduler.Task("handoff", "first", "root"),
                scheduler.Task("handoff", "second", "root"),
            ]
            for task in tasks:
                self.assertTrue(worker.admitted(task))
                active = scheduler.Active(
                    task,
                    worker.cost(task),
                    Mock(pid=2147483647),
                    self.root / task.key,
                    io.StringIO(),
                    0,
                )
                worker.active[task.key] = active
            self.assertFalse(worker.admitted(scheduler.Task("handoff", "third", "root")))
            for active in worker.active.values():
                active.log.close()
            worker.active.clear()

            # Restart admission credits only this task's retained allocation.
            retained = worker.scratch_directory(tasks[0])

            def usage(path):
                return worker.background.scratch if Path(path) in (self.ram, retained) else 0

            with patch.object(scheduler, "directory_bytes", side_effect=usage):
                self.assertTrue(worker.admitted(tasks[0]))
                self.assertFalse(
                    worker.admitted(scheduler.Task("analysis", "other", "root", priority=10))
                )

                # A fresh worker must not count retained job data as RIPE usage.
                restarted = scheduler.Scheduler(
                    Mock(), "s3://test/workflow", self.root / "control"
                )
                self.addCleanup(restarted.close)
                self.assertTrue(restarted.admitted(tasks[0]))


@unittest.skipUnless(
    sys.platform == "linux" and Path("/dev/shm").is_dir(), "Linux tmpfs integration"
)
class LinuxRamIntegration(unittest.TestCase):
    def test_parallel_jobs_with_different_ram_directories_download_and_process_once(self):
        with (
            tempfile.TemporaryDirectory(prefix="ipid-ram-test-", dir="/dev/shm") as ram,
            tempfile.TemporaryDirectory() as root,
        ):
            with ProcessPoolExecutor(
                max_workers=2, mp_context=multiprocessing.get_context("spawn")
            ) as executor:
                results = list(
                    executor.map(
                        prepare_in_ram, [(root, ram, "first", 29), (root, ram, "second", 30)]
                    )
                )
            self.assertEqual(results, [1, 1])
            self.assertEqual(len((Path(root) / "http-calls").read_text().splitlines()), 1)
            self.assertEqual(
                len(list((Path(root) / "fake-s3-processed/ripe-atlas/file-parts").glob("*.pq"))), 1
            )
            self.assertFalse(list((Path(root) / "control").rglob("*.pq")))

    def test_mount_root_and_ext4_are_rejected_without_configuration_changes(self):
        with patch.dict(os.environ, {"IPID_ANALYSIS_RAM_DIR": "/dev/shm"}):
            with self.assertRaises(ValueError):
                runtime.ram_root()
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, {"IPID_ANALYSIS_RAM_DIR": directory}):
                if runtime.filesystem_type(Path(directory).resolve()) == "tmpfs":
                    self.skipTest("system temporary directory already uses tmpfs")
                with self.assertRaisesRegex(OSError, "tmpfs"):
                    runtime.ram_root()

    def test_real_tmpfs_preserves_files_across_processes_without_mount_changes(self):
        with tempfile.TemporaryDirectory(prefix="ipid-ram-test-", dir="/dev/shm") as directory:
            with patch.dict(
                os.environ,
                {
                    "IPID_ANALYSIS_STORAGE_MODE": "local",
                    "IPID_ANALYSIS_SCRATCH_STORAGE": "ram",
                    "IPID_ANALYSIS_RAM_DIR": directory,
                    "IPID_ANALYSIS_RAM_JOB_DIR": directory + "/job",
                    "IPID_ANALYSIS_RAM_BYTES": str(8 << 20),
                    "IPID_ANALYSIS_RAM_RESERVE_BYTES": "0",
                },
            ):
                self.assertEqual(runtime.filesystem_type(runtime.ram_root()), "tmpfs")
                path = runtime.temporary_path(Path("fixture.pq"))
                pq.write_table(pa.table({"value": [1, 2, 3]}), path)
                import subprocess

                result = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        "import sys; import pyarrow.parquet as p; assert p.read_table(sys.argv[1]).num_rows == 3",
                        str(path),
                    ],
                    capture_output=True,
                    timeout=30,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
