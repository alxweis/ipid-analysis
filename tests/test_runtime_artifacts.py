"""Failure and resource contracts, using only isolated local fixtures."""

from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import artifacts, runtime, transport
from ipid_analysis.maintenance import cleanup_candidates


class RuntimeArtifactsTest(unittest.TestCase):
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

    def test_missing_mount_is_rejected_before_output_creation(self):
        roots = tuple(self.root / name for name in ("raw", "processed", "reports"))
        with (
            patch.dict(os.environ, {"IPID_ANALYSIS_STORAGE_MODE": "mounted-s3"}),
            patch.object(runtime, "persistent_roots", return_value=roots),
        ):
            with self.assertRaisesRegex(OSError, "required S3 mount"):
                runtime.validate_storage()
        self.assertFalse(any(root.exists() for root in roots))

    def test_scratch_cannot_overlap_persistent_storage_or_its_parent(self):
        raw = self.root / "storage" / "raw"
        with patch.object(runtime, "persistent_roots", return_value=(raw,)):
            for invalid in (raw, raw / "temp", raw.parent):
                with patch.dict(os.environ, {"IPID_ANALYSIS_TMP_DIR": str(invalid)}):
                    with self.assertRaises(ValueError):
                        runtime.scratch_root()

    def test_duckdb_spill_directory_is_local_and_removed_on_error(self):
        with self.assertRaisesRegex(RuntimeError, "interrupt"):
            with runtime.connect(config={"threads": 1, "memory_limit": "32MB"}) as con:
                directory = Path(
                    con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
                )
                self.assertIn(runtime.scratch_root(), directory.parents)
                self.assertTrue(directory.is_dir())
                result = con.execute("""
                    SELECT sum(i) FROM (
                        SELECT i, repeat(md5(CAST(i AS VARCHAR)), 4) AS s
                        FROM range(300000) AS t(i) ORDER BY s
                    )
                """).fetchone()[0]
                self.assertEqual(result, 300000 * 299999 // 2)
                raise RuntimeError("interrupt")
        self.assertFalse(directory.exists())

    def test_cpu_and_batch_budgets_respect_small_allocations(self):
        with patch.object(runtime, "available_memory", return_value=64 << 20):
            self.assertLess(runtime.batch_budget(1_000_000, width=100), 10000)
        with self.assertRaises(ValueError):
            runtime.batch_budget(0)
        with patch.dict(os.environ, {"IPID_ANALYSIS_THREADS": "999999"}):
            available = (
                len(os.sched_getaffinity(0))
                if hasattr(os, "sched_getaffinity")
                else os.cpu_count()
            )
            self.assertLessEqual(runtime.cpu_budget(), available)

    def test_abrupt_process_exit_cannot_replace_committed_output(self):
        output = self.root / "result.pq"
        artifacts.write_table(pa.table({"IP_ADDR": ["original"]}), output)
        before = output.read_bytes()
        code = """
import os, sys
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from ipid_analysis.artifacts import atomic_output
with atomic_output(Path(sys.argv[1])) as partial:
    pq.write_table(pa.table({'IP_ADDR': ['prefix']}), partial)
    os._exit(9)
"""
        result = subprocess.run(
            [sys.executable, "-c", code, str(output)], capture_output=True, timeout=30
        )
        self.assertEqual(result.returncode, 9, result.stderr)
        self.assertEqual(output.read_bytes(), before)
        artifacts.assert_complete(output)

    def test_concurrent_download_reservations_cannot_double_spend_free_space(self):
        with (
            patch.object(
                runtime.shutil, "disk_usage", return_value=shutil._ntuple_diskusage(100, 0, 100)
            ),
            patch.dict(os.environ, {"IPID_ANALYSIS_SCRATCH_RESERVE_BYTES": "10"}),
        ):
            with runtime.reserve_download(60):
                with self.assertRaisesRegex(OSError, "unreserved"):
                    with runtime.reserve_download(40):
                        pass
            with runtime.reserve_download(80):
                pass

    def test_cleanup_rejects_root_and_external_paths(self):
        root = runtime.scratch_root()
        for invalid in (root, self.root):
            with self.assertRaises(ValueError):
                runtime.cleanup_work(invalid)
        own = root / "owned"
        own.mkdir()
        (own / "part").write_text("temporary")
        runtime.cleanup_work(own)
        self.assertFalse(own.exists())

    def test_failed_job_cleanup_requires_durable_failure_acknowledgement(self):
        root = runtime.scratch_root()
        job = root / "workflow" / "analysis" / "job"
        job.mkdir(parents=True)
        (job / "failed.json").write_text("{}")
        self.assertNotIn(job, list(cleanup_candidates(root, float("inf"), include_failed=True)))
        (job / ".failure-published").write_text("s3://test/job/failed.json")
        self.assertIn(job, list(cleanup_candidates(root, float("inf"), include_failed=True)))

    def test_local_interruption_preserves_previous_committed_output(self):
        output = self.root / "result.pq"
        artifacts.write_table(pa.table({"IP_ADDR": ["old"]}), output)
        before = output.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "interrupt"):
            with artifacts.atomic_output(output) as temporary:
                pq.write_table(pa.table({"IP_ADDR": ["partial"]}), temporary)
                raise RuntimeError("interrupt")
        self.assertEqual(output.read_bytes(), before)
        artifacts.assert_complete(output)
        self.assertFalse(list((runtime.scratch_root() / "artifacts").glob("artifact-*")))

    def test_valid_parquet_prefix_is_rejected_by_expected_row_count(self):
        output = self.root / "result.pq"
        with self.assertRaisesRegex(ValueError, "expected 2 rows"):
            with artifacts.atomic_output(output, expected_rows=2) as temporary:
                pq.write_table(pa.table({"IP_ADDR": ["prefix"]}), temporary)
        self.assertFalse(output.exists())

    def test_cache_tracks_source_identity_and_parameters(self):
        source = self.root / "input"
        source.write_text("first")
        output = self.root / "result.pq"
        with artifacts.atomic_output(
            output, inputs=[source], parameters={"algorithm": 1}
        ) as temporary:
            pq.write_table(pa.table({"IP_ADDR": ["one"]}), temporary)
        self.assertTrue(artifacts.cache_current(output, [source], {"algorithm": 1}))
        self.assertFalse(artifacts.cache_current(output, [source], {"algorithm": 2}))
        before = source.stat()
        source.write_text("other")
        os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000_000))
        self.assertFalse(artifacts.cache_current(output, [source], {"algorithm": 1}))

    def test_incomplete_marker_is_not_accepted_as_legacy(self):
        output = self.root / "result.pq"
        artifacts.write_table(pa.table({"IP_ADDR": ["one"]}), output)
        artifacts.completion_path(output).write_text('{"status":"writing"}')
        with self.assertRaisesRegex(ValueError, "incomplete artifact"):
            artifacts.assert_complete(output)

    def test_native_publication_uses_no_fuse_rename(self):
        remote = self.root / "remote"
        output = remote / "result.pq"

        def native(source, destination):
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)

        with (
            patch.object(artifacts, "mounted_storage", return_value=True),
            patch.object(artifacts, "persistent_roots", return_value=(remote,)),
            patch.object(artifacts, "validate_storage"),
            patch.object(artifacts, "native_put", side_effect=native) as put,
            patch.object(Path, "replace", side_effect=OSError("FUSE rename unsupported")),
        ):
            artifacts.write_table(pa.table({"IP_ADDR": ["one"]}), output)
        self.assertEqual(put.call_count, 3)  # writing marker, complete data, final marker
        artifacts.assert_complete(output)
        info = json.loads(artifacts.completion_path(output).read_text())
        self.assertEqual(info["sha256"], hashlib.sha256(output.read_bytes()).hexdigest())

    def test_failed_native_upload_preserves_old_bytes_and_does_not_commit(self):
        remote = self.root / "remote"
        output = remote / "result.bin"
        artifacts.write_text(output, "original")
        before = output.read_bytes()

        def native(source, destination):
            if destination == output:
                raise OSError("upload interrupted before object commit")
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)

        with (
            patch.object(artifacts, "mounted_storage", return_value=True),
            patch.object(artifacts, "persistent_roots", return_value=(remote,)),
            patch.object(artifacts, "validate_storage"),
            patch.object(artifacts, "native_put", side_effect=native),
        ):
            with self.assertRaisesRegex(OSError, "upload interrupted"):
                artifacts.write_text(output, "replacement")
        self.assertEqual(output.read_bytes(), before)
        with self.assertRaises(ValueError):
            artifacts.assert_complete(output)
        state = json.loads(artifacts.completion_path(output).read_text())
        self.assertEqual(state["previous"]["status"], "complete")

    def test_inventory_contains_only_current_successful_publications(self):
        old = self.root / "old.json"
        artifacts.write_text(old, "old")
        with artifacts.publication_inventory() as inventory:
            current = self.root / "current.json"
            artifacts.write_text(current, "current")
            with self.assertRaises(RuntimeError):
                with artifacts.atomic_output(self.root / "failed.json"):
                    raise RuntimeError("interrupt")
        self.assertEqual(inventory, [current])

    def test_transport_refreshes_only_the_discovered_mount_after_upload(self):
        source, destination = self.root / "source", self.root / "destination"
        source.write_bytes(b"complete object")

        def copy(command, **kwargs):
            self.assertEqual(command[-1], "remote:bucket/object")
            self.assertIn("--ignore-times", command)
            shutil.copy2(source, destination)

        with (
            patch.object(
                transport,
                "_mount",
                return_value=nullcontext((["rclone"], "remote:bucket/object", 123)),
            ),
            patch.object(transport.subprocess, "run", side_effect=copy),
            patch.object(transport.signal, "SIGHUP", 1, create=True),
            patch.object(transport.signal, "pidfd_send_signal", create=True) as refresh,
        ):
            transport.native_put(source, destination)
        refresh.assert_called_once_with(123, 1)

    def test_transport_skips_bucket_check_without_environment_override(self):
        source, destination = self.root / "source", self.root / "destination"
        source.write_bytes(b"complete object")

        def copy(command, **kwargs):
            self.assertNotIn("RCLONE_S3_NO_CHECK_BUCKET", os.environ)
            self.assertIn("--s3-no-check-bucket", command)
            shutil.copy2(source, destination)

        with (
            patch.dict(os.environ),
            patch.object(
                transport,
                "_mount",
                return_value=nullcontext((["rclone"], "remote:bucket/object", 123)),
            ),
            patch.object(transport.subprocess, "run", side_effect=copy) as upload,
            patch.object(transport.signal, "SIGHUP", 1, create=True),
            patch.object(transport.signal, "pidfd_send_signal", create=True),
        ):
            os.environ.pop("RCLONE_S3_NO_CHECK_BUCKET", None)
            transport.native_put(source, destination)
        upload.assert_called_once()

    def test_transport_never_treats_ambiguous_failure_as_success(self):
        source = self.root / "source"
        source.write_bytes(b"new")
        with (
            patch.object(
                transport,
                "_mount",
                return_value=nullcontext((["rclone"], "remote:bucket/object", 123)),
            ),
            patch.object(
                transport.subprocess, "run", side_effect=subprocess.TimeoutExpired("rclone", 1)
            ),
            patch.object(transport.signal, "SIGHUP", 1, create=True),
            patch.object(transport.signal, "pidfd_send_signal", create=True) as refresh,
        ):
            with self.assertRaises(subprocess.TimeoutExpired):
                transport.native_put(source, self.root / "destination")
        refresh.assert_called_once()


if __name__ == "__main__":
    unittest.main()
