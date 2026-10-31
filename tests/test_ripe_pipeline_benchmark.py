import bz2
import cProfile
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from benchmarks import ripe_pipeline as bench
import pyarrow as pa
import pyarrow.parquet as pq


class BenchmarkTests(unittest.TestCase):
    def test_profile_report_accepts_path_and_reports_decode_functions(self):
        with tempfile.TemporaryDirectory() as temporary:
            profile = Path(temporary) / "decode.pstats"
            data = bz2.compress(b'{"af": 4}')
            profiler = cProfile.Profile()
            profiler.enable()
            json.loads(bz2.decompress(data))
            profiler.disable()
            profiler.dump_stats(profile)
            report = bench.profile_report(profile)
            self.assertTrue(report["bz2_decompression"])
            self.assertTrue(report["json_decode"])
            self.assertIn("function calls", profile.with_suffix(".txt").read_text())

    def test_workspace_monitor_tolerates_concurrent_temporary_file_removal(self):
        root, item = unittest.mock.Mock(), unittest.mock.Mock()
        root.rglob.return_value = [item]
        item.is_file.return_value = True
        item.stat.side_effect = FileNotFoundError
        self.assertEqual(bench.tree_bytes(root), 0)

    def test_environment_excludes_all_production_scope_and_cache_settings(self):
        with patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_SCOPE": "invalid-production-scope",
                "IPID_ANALYSIS_S3_PREFIX": "s3://production",
                "IPID_ANALYSIS_RIPE_CACHE_S3": "s3://production-cache",
                "IPID_ANALYSIS_RUN_OWNER": "123",
                "IPID_ANALYSIS_LOCK_DIR": "/production/locks",
            },
        ):
            env = bench.isolated_environment(Path("/private-test"))
        self.assertNotIn("IPID_ANALYSIS_SCOPE", env)
        self.assertNotIn("IPID_ANALYSIS_S3_PREFIX", env)
        self.assertNotIn("IPID_ANALYSIS_RUN_OWNER", env)
        self.assertNotIn("IPID_ANALYSIS_LOCK_DIR", env)
        self.assertEqual(env["IPID_ANALYSIS_STORAGE_MODE"], "local")
        self.assertEqual(env["IPID_ANALYSIS_RIPE_CACHE_S3"], "")
        self.assertEqual(env["PYTHON_DOTENV_DISABLED"], "1")
        self.assertEqual(env["IPID_ANALYSIS_TMP_DIR"], str(Path("/private-test")))

    def test_digest_is_canonical_not_parquet_bytes_or_row_groups(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            table = pa.table(
                {
                    "IP_V4": pa.array([1, 9], type=pa.uint32()),
                    "T": [True, False],
                    "D": [False, True],
                }
            )
            expected = hashlib.sha256(struct.pack("!IBBIBB", 1, 1, 0, 9, 0, 1)).hexdigest()
            for compression, groups in [("none", 1), ("zstd", 2)]:
                path = root / (compression + ".pq")
                pq.write_table(table, path, compression=compression, row_group_size=groups)
                self.assertEqual(bench.roles_digest(path), expected)

    def test_digest_rejects_duplicate_and_unsorted_addresses(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "invalid.pq"
            for addresses in ([1, 1], [9, 1]):
                with self.subTest(addresses=addresses):
                    pq.write_table(
                        pa.table(
                            {
                                "IP_V4": pa.array(addresses, type=pa.uint32()),
                                "T": [True, False],
                                "D": [False, True],
                            }
                        ),
                        path,
                    )
                    with self.assertRaisesRegex(ValueError, "ordered unique"):
                        bench.roles_digest(path)

    def test_copy_limit_never_writes_more_than_its_budget(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, target = root / "source", root / "target"
            source.write_bytes(b"123456789")
            with self.assertRaisesRegex(ValueError, "workspace cap"):
                bench.copy_input(source, target, False, 5)
            self.assertLessEqual(target.stat().st_size, 5)
            self.assertEqual(source.read_bytes(), b"123456789")

    def test_copy_cold_policy_applies_only_to_owned_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, target = root / "source", root / "target"
            source.write_bytes(b"unchanged-source")
            with (
                patch.object(os, "fsync"),
                patch.object(os, "posix_fadvise", create=True) as advise,
                patch.object(os, "POSIX_FADV_DONTNEED", 4, create=True),
            ):
                report = bench.copy_input(source, target, True, 100)
            advise.assert_called_once()
            self.assertEqual(report["sha256"], bench.sha256(source))
            self.assertIn("not guaranteed", report["page_cache_policy"])

    def test_profiled_run_excluded_from_timing_comparison(self):
        rows = []
        for backend, duration, profile in [
            ("ext4", 4, False),
            ("ram", 2, False),
            ("ext4", 6, False),
            ("ram", 4, False),
            ("ram", 999, True),
        ]:
            rows.append(
                dict(
                    backend=backend,
                    wall_seconds=duration,
                    profiled=profile,
                    roles_sha256="same",
                    source_sha256="source",
                    addresses=2,
                    traces=3,
                    skipped_non_ipv4=0,
                    skipped_outside_window=0,
                )
            )
        self.assertEqual(bench.validate_results(rows), {"ext4": 5, "ram": 3})
        rows[-1]["roles_sha256"] = "changed"
        with self.assertRaisesRegex(ValueError, "outputs differ"):
            bench.validate_results(rows)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux mount paths")
    def test_mountinfo_uses_longest_matching_decoded_mount(self):
        mountinfo = "1 0 0:1 / / rw - ext4 /dev/root rw\n2 1 0:2 / /a\\040b rw - tmpfs tmpfs rw\n"
        with patch.object(Path, "read_text", return_value=mountinfo):
            self.assertEqual(bench.filesystem(Path("/a b/test")), "tmpfs")
            self.assertEqual(bench.filesystem(Path("/elsewhere")), "ext4")

    def test_bad_limits_rejected_without_running_a_parser(self):
        for value in ("0", "-1", "nan", "inf"):
            with self.subTest(value=value), patch.object(sys, "platform", "linux"):
                with self.assertRaisesRegex(ValueError, "positive"):
                    bench.main(
                        [
                            "--input",
                            "unused.bz2",
                            "--window-start",
                            "2026-10-02",
                            "--window-end",
                            "2026-10-30",
                            "--memory-gib",
                            value,
                        ]
                    )

    def test_reported_peak_over_limit_fails_even_if_monitor_missed_peak(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = root / "result.json"
            bench.write_json(result, {"kernel_peak_rss_bytes": 11})
            process = unittest.mock.Mock(returncode=0)
            process.poll.return_value = 0
            with patch.object(bench.subprocess, "Popen", return_value=process):
                with self.assertRaisesRegex(RuntimeError, "peak RSS"):
                    bench.run_trial(
                        dict(workspace=str(root), result=str(result)), lambda: 100, 10, 100, 1, 10
                    )

    def test_timeout_terminates_child_and_retains_log(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            process = unittest.mock.Mock(pid=123)
            process.poll.return_value = None
            with (
                patch.object(bench.subprocess, "Popen", return_value=process),
                patch.object(bench, "proc_rss", return_value=0),
                patch.object(bench.time, "monotonic", side_effect=[0, 2]),
                patch.object(bench, "stop") as stop,
            ):
                with self.assertRaises(TimeoutError):
                    bench.run_trial(
                        dict(workspace=str(root), result=str(root / "r.json")),
                        lambda: 100,
                        100,
                        10000,
                        1,
                        1,
                    )
                stop.assert_called_once_with(process)
            self.assertTrue((root / "r.log").exists())

    def test_monitored_resource_breaches_stop_child(self):
        for rss, space, free, message in (
            (101, 0, 100, "limit exceeded"),
            (0, 101, 100, "limit exceeded"),
            (0, 0, 0, "free-memory reserve"),
        ):
            with self.subTest(message=message), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                process = unittest.mock.Mock(pid=123)
                process.poll.return_value = None
                with (
                    patch.object(bench.subprocess, "Popen", return_value=process),
                    patch.object(bench, "proc_rss", return_value=rss),
                    patch.object(bench, "tree_bytes", return_value=space),
                    patch.object(bench, "stop") as stop,
                ):
                    with self.assertRaisesRegex(RuntimeError, message):
                        bench.run_trial(
                            dict(workspace=str(root), result=str(root / "r.json")),
                            lambda: free,
                            100,
                            100,
                            1,
                            10,
                        )
                    stop.assert_called_once_with(process)

    def test_tmpfs_requirement_cannot_silently_use_disk(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (
                patch.object(sys, "platform", "linux"),
                patch.object(bench, "PROJECT", root),
                patch.object(bench, "filesystem", return_value="ext4"),
            ):
                with self.assertRaisesRegex(ValueError, "existing tmpfs"):
                    bench.main(
                        [
                            "--input",
                            "unused.bz2",
                            "--window-start",
                            "2026-10-02",
                            "--window-end",
                            "2026-10-30",
                            "--work-dir",
                            str(root / "tmp" / "private"),
                            "--ram-dir",
                            str(root),
                        ]
                    )

    def test_result_directory_outside_checkout_tmp_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(sys, "platform", "linux"):
                with self.assertRaisesRegex(ValueError, "descendant"):
                    bench.main(
                        [
                            "--input",
                            "unused.bz2",
                            "--window-start",
                            "2026-10-02",
                            "--window-end",
                            "2026-10-30",
                            "--work-dir",
                            temporary,
                        ]
                    )


@unittest.skipUnless(sys.platform.startswith("linux"), "real tmpfs/ext4 benchmark requires Linux")
class LinuxBenchmarkTests(unittest.TestCase):
    def test_real_cli_production_parser_profile_and_no_cache_publication(self):
        # Small synthetic dump; no RIPE HTTP or production mount is contacted.
        parent = bench.PROJECT / "tmp" / "benchmark-tests"
        parent.mkdir(parents=True, exist_ok=True)
        if bench.filesystem(parent) != "ext4" or not Path("/dev/shm").is_dir():
            self.skipTest("ext4 and existing /dev/shm required")
        with tempfile.TemporaryDirectory(dir=parent) as temporary:
            root = Path(temporary)
            source = root / "fixture.bz2"
            trace = dict(
                af=4,
                type="traceroute",
                timestamp=1791936000,
                src_addr="10.0.0.1",
                dst_addr="1.1.1.1",
                result=[
                    dict(hop=1, result=[{"from": "8.8.8.8"}]),
                    dict(hop=2, result=[{"from": "1.1.1.1"}]),
                ],
            )
            records = [trace] * 3 + [dict(trace, af=6), dict(trace, timestamp=0)]
            body = bz2.compress(("\n".join(json.dumps(row) for row in records) + "\n").encode())
            source.write_bytes(body)
            before = source.stat().st_mtime_ns
            env = dict(
                os.environ,
                IPID_ANALYSIS_SCOPE="deliberately-invalid",
                IPID_ANALYSIS_RIPE_CACHE_S3="s3://never-contact",
                IPID_ANALYSIS_TMP_DIR="/never-write",
                PYTHON_DOTENV_DISABLED="1",
            )
            command = [
                sys.executable,
                "-m",
                "benchmarks.ripe_pipeline",
                "--input",
                str(source),
                "--window-start",
                "2026-10-02",
                "--window-end",
                "2026-10-30",
                "--work-dir",
                str(root / "work"),
                "--repeats",
                "1",
                "--profile",
                "--memory-gib",
                "1",
                "--workspace-gib",
                "0.01",
                "--reserve-gib",
                "0.01",
            ]
            completed = subprocess.run(
                command, cwd=bench.PROJECT, env=env, capture_output=True, text=True, timeout=120
            )
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            result_path = next((root / "work").glob("*/results.json"))
            report = json.loads(result_path.read_text())
            self.assertEqual(report["status"], "passed")
            self.assertEqual(len(report["results"]), 3)
            self.assertEqual(report["results"][0]["addresses"], 2)
            # Production daily-dump parts do not apply a second window filter.
            self.assertEqual(report["results"][0]["traces"], 4)
            self.assertEqual(report["results"][0]["skipped_outside_window"], 0)
            self.assertFalse(report["results"][0]["window_filter_applied"])
            expected = hashlib.sha256(
                struct.pack("!IBBIBB", 0x01010101, 0, 1, 0x08080808, 1, 0)
            ).hexdigest()
            self.assertEqual(report["results"][0]["roles_sha256"], expected)
            self.assertEqual(source.read_bytes(), body)
            self.assertEqual(source.stat().st_mtime_ns, before)
            self.assertEqual(len(list(result_path.parent.glob("*.pstats"))), 1)
            self.assertTrue(
                report["results"][-1]["profile_functions"]["role_derivation_inclusive"]
            )
            self.assertTrue(report["results"][-1]["profile_functions"]["bz2_decompression"])
            self.assertTrue(report["results"][-1]["profile_functions"]["json_decode"])
            self.assertTrue(report["results"][-1]["profile_functions"]["bitmap_add_inclusive"])
            self.assertFalse(list(result_path.parent.glob("ripe-trial-*")))
            self.assertFalse(list(result_path.parent.rglob("roles.pq")))

    def test_invalid_dump_reports_failure_preserves_source_and_cleans_private_trial(self):
        parent = bench.PROJECT / "tmp" / "benchmark-tests"
        parent.mkdir(parents=True, exist_ok=True)
        if bench.filesystem(parent) != "ext4" or not Path("/dev/shm").is_dir():
            self.skipTest("ext4 and existing /dev/shm required")
        with tempfile.TemporaryDirectory(dir=parent) as temporary:
            root = Path(temporary)
            source = root / "bad.bz2"
            source.write_bytes(b"not a bz2 stream")
            command = [
                sys.executable,
                "-m",
                "benchmarks.ripe_pipeline",
                "--input",
                str(source),
                "--window-start",
                "2026-10-02",
                "--window-end",
                "2026-10-30",
                "--work-dir",
                str(root / "work"),
                "--repeats",
                "1",
                "--memory-gib",
                "1",
                "--workspace-gib",
                "0.01",
                "--reserve-gib",
                "0.01",
            ]
            completed = subprocess.run(
                command, cwd=bench.PROJECT, capture_output=True, text=True, timeout=120
            )
            self.assertNotEqual(completed.returncode, 0)
            result_path = next((root / "work").glob("*/results.json"))
            report = json.loads(result_path.read_text())
            self.assertEqual(report["status"], "failed")
            self.assertIn("parser exited", report["error"])
            self.assertEqual(source.read_bytes(), b"not a bz2 stream")
            self.assertFalse(list(result_path.parent.glob("ripe-trial-*")))
            self.assertTrue(list(result_path.parent.glob("*.log")))


if __name__ == "__main__":
    unittest.main()
