"""s3cmd cache-miss handling without remote services."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.ripe_cache import RoleCache, read_metadata, validate_roles


class RoleCacheDownloadTests(unittest.TestCase):
    def test_metadata_checksum_is_verified_even_when_json_and_file_size_are_valid(self):
        from ipid_analysis.artifacts import completion_path

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.json"
            path.write_text(json.dumps({"cache_version": "4"}))
            marker = completion_path(path)
            marker.parent.mkdir()
            marker.write_text(
                json.dumps(
                    {"status": "complete", "size_bytes": path.stat().st_size, "sha256": "0" * 64}
                )
            )
            with self.assertRaisesRegex(ValueError, "metadata checksum"):
                read_metadata(path)

    def test_local_legacy_window_is_validated_without_rebuilding_or_rewriting(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "roles.pq"
            pq.write_table(
                pa.table(
                    {
                        "IP_V4": pa.array([1], type=pa.uint32()),
                        "IP_ADDR": ["0.0.0.1"],
                        "T": [False],
                        "D": [True],
                    }
                ),
                path,
            )
            before = (path.read_bytes(), path.stat().st_mtime_ns)
            validate_roles(
                path,
                {"cache_version": "4", "cache_identity": "window", "addresses": 1},
                "4",
                identity="window",
            )
            self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), before)

    def test_parquet_marker_hash_is_checked_even_with_a_valid_metadata_hash(self):
        from ipid_analysis.artifacts import completion_path

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "roles.pq"
            pq.write_table(
                pa.table({"IP_V4": pa.array([1], type=pa.uint32()), "T": [False], "D": [True]}),
                path,
            )
            marker = completion_path(path)
            marker.parent.mkdir()
            marker.write_text(
                json.dumps(
                    {"status": "complete", "size_bytes": path.stat().st_size, "sha256": "0" * 64}
                )
            )
            info = {
                "cache_version": "2",
                "parquet_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            with self.assertRaisesRegex(ValueError, "checksum"):
                validate_roles(path, info, "2")

    def test_local_cache_rejects_checksum_row_count_schema_and_identity_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "roles.pq"
            pq.write_table(
                pa.table(
                    {
                        "IP_V4": pa.array([1], type=pa.uint32()),
                        "IP_ADDR": ["0.0.0.1"],
                        "T": [False],
                        "D": [True],
                    }
                ),
                path,
            )
            info = {
                "cache_version": "4",
                "cache_identity": "window",
                "addresses": 1,
                "parquet_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for changed, message in (
                ({"parquet_sha256": "0" * 64}, "checksum"),
                ({"addresses": 2}, "row count"),
                ({"cache_identity": "different"}, "identity"),
            ):
                with self.subTest(changed=changed), self.assertRaisesRegex(ValueError, message):
                    validate_roles(path, {**info, **changed}, "4", identity="window")
            pq.write_table(
                pa.table(
                    {"IP_V4": ["wrong type"], "IP_ADDR": ["0.0.0.1"], "T": [False], "D": [True]}
                ),
                path,
            )
            with self.assertRaisesRegex(ValueError, "schema"):
                validate_roles(path, {"cache_version": "4"}, "4")

    def test_remote_window_identity_is_rejected_before_any_output_is_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            destination = root / "roles.pq"
            destination.write_bytes(b"previous valid generation")

            def get(uri, path):
                path.write_text(
                    json.dumps(
                        {
                            "cache_version": "4",
                            "cache_identity": "wrong",
                            "parquet_sha256": "0" * 64,
                        }
                    )
                )
                return True

            with (
                patch.dict(os.environ, {"IPID_ANALYSIS_TMP_DIR": str(root / "scratch")}),
                patch.object(self.cache, "_get", side_effect=get),
                self.assertRaisesRegex(ValueError, "identity"),
            ):
                self.cache.restore("windows/expected", destination, root / "source.json", "4")
            self.assertEqual(destination.read_bytes(), b"previous valid generation")

    def test_cache_validation_does_not_turn_permission_errors_into_cache_misses(self):
        with patch(
            "ipid_analysis.ripe_cache.assert_complete",
            side_effect=PermissionError("access denied"),
        ):
            with self.assertRaises(PermissionError):
                validate_roles(Path("unavailable"), {"cache_version": "4"}, "4")

    def setUp(self):
        environment = patch.dict(os.environ, {"IPID_ANALYSIS_RIPE_CACHE_S3": "s3://test/roles"})
        environment.start()
        self.addCleanup(environment.stop)
        self.cache = RoleCache()
        self.uri = f"{self.cache.prefix}/windows/test/manifest.json"
        self.destination = Path("unused-cache-download.json")

    def test_source_object_does_not_exist_is_a_cache_miss(self):
        message = f"ERROR: Parameter problem: Source object '{self.uri}' does not exist."
        for stderr in (message, "\n" + message + "\n"):
            with (
                self.subTest(stderr=stderr),
                patch(
                    "ipid_analysis.ripe_cache.subprocess.run",
                    return_value=subprocess.CompletedProcess([], 1, stderr=stderr),
                ),
            ):
                self.assertFalse(self.cache._get(self.uri, self.destination))

    def test_existing_missing_object_formats_remain_cache_misses(self):
        for stderr in ("ERROR: S3 error: 404 (Not Found)", "ERROR: NoSuchKey"):
            with (
                self.subTest(stderr=stderr),
                patch(
                    "ipid_analysis.ripe_cache.subprocess.run",
                    return_value=subprocess.CompletedProcess([], 1, stderr=stderr),
                ),
            ):
                self.assertFalse(self.cache._get(self.uri, self.destination))

    def test_other_s3_network_and_parameter_errors_still_raise(self):
        missing = f"ERROR: Parameter problem: Source object '{self.uri}' does not exist."
        for stderr in (
            "ERROR: S3 error: 403 (AccessDenied)",
            "ERROR: S3 error: 503 (SlowDown)",
            "ERROR: Connection to server lost.",
            "ERROR: Parameter problem: Invalid access key.",
            "ERROR: Parameter problem: Source bucket 'test' does not exist.",
            "ERROR: Parameter problem: Source object 's3://other/key' does not exist.",
            missing + "\nERROR: S3 error: 403 (AccessDenied)",
            "ERROR: S3 error: 403 (AccessDenied): s3://test/cache/404-object",
            "ERROR: NoSuchKey\nERROR: S3 error: 403 (AccessDenied)",
        ):
            with (
                self.subTest(stderr=stderr),
                patch(
                    "ipid_analysis.ripe_cache.subprocess.run",
                    return_value=subprocess.CompletedProcess([], 1, stderr=stderr),
                ),
            ):
                with self.assertRaises(RuntimeError) as raised:
                    self.cache._get(self.uri, self.destination)
                self.assertIn(stderr, str(raised.exception))

    def test_transport_exceptions_are_not_converted_to_cache_misses(self):
        for error in (subprocess.TimeoutExpired("s3cmd", 1800), OSError("network failure")):
            with (
                self.subTest(error=error),
                patch("ipid_analysis.ripe_cache.subprocess.run", side_effect=error),
            ):
                with self.assertRaises(type(error)) as raised:
                    self.cache._get(self.uri, self.destination)
                self.assertIs(raised.exception, error)

    def test_successful_download_keeps_command_and_timeout(self):
        with patch(
            "ipid_analysis.ripe_cache.subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, stderr=""),
        ) as run:
            self.assertTrue(self.cache._get(self.uri, self.destination))
        run.assert_called_once_with(
            ["s3cmd", "get", "--force", "--no-progress", self.uri, str(self.destination)],
            capture_output=True,
            check=False,
            text=True,
            timeout=1800,
        )

    def test_restore_missing_manifest_or_object_returns_false_without_publication(self):
        for missing in ("manifest", "object"):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                parquet, metadata = root / "roles.pq", root / "source.json"
                parquet.write_bytes(b"previous data")
                metadata.write_bytes(b"previous metadata")

                def download(command, **kwargs):
                    uri, destination = command[-2:]
                    if missing == "object" and uri.endswith("/manifest.json"):
                        Path(destination).write_text(
                            json.dumps({"cache_version": "2", "parquet_sha256": "0" * 64})
                        )
                        return subprocess.CompletedProcess(command, 0, stderr="")
                    return subprocess.CompletedProcess(
                        command,
                        1,
                        stderr=f"ERROR: Parameter problem: Source object '{uri}' does not exist.",
                    )

                with (
                    patch.dict(os.environ, {"IPID_ANALYSIS_TMP_DIR": str(root / "scratch")}),
                    patch("ipid_analysis.ripe_cache.subprocess.run", side_effect=download) as run,
                    patch("ipid_analysis.ripe_cache.publish_file") as publish,
                ):
                    self.assertFalse(self.cache.restore("windows/test", parquet, metadata, "2"))
                self.assertEqual(run.call_count, 1 if missing == "manifest" else 2)
                publish.assert_not_called()
                self.assertEqual(parquet.read_bytes(), b"previous data")
                self.assertEqual(metadata.read_bytes(), b"previous metadata")
                self.assertFalse(list((root / "scratch").glob(".ripe-cache-*")))


if __name__ == "__main__":
    unittest.main()
