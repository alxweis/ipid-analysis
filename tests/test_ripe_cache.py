"""s3cmd cache-miss handling without remote services."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from ipid_analysis.ripe_cache import RoleCache


class RoleCacheDownloadTests(unittest.TestCase):
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
