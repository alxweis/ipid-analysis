"""MaxMind staging uses isolated files; no licensed database or live S3 access."""

from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ipid_analysis import maxmind, runtime


class MaxMindTest(unittest.TestCase):
    def setUp(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        self.raw = self.root / "raw"
        self.source = self.raw / "maxmind" / "2026-11-01" / "GeoLite2-Country.mmdb"
        self.source.parent.mkdir(parents=True)
        self.content = b"isolated MMDB fixture, metadata reader stubbed"
        self.source.write_bytes(self.content)
        self.sha256 = hashlib.sha256(self.content).hexdigest()
        self.scratch = self.root / "ram"
        self.scratch.mkdir()
        self.uri = "s3://example/raw/maxmind/2026-11-01/GeoLite2-Country.mmdb"
        stack.enter_context(
            patch.dict(
                os.environ,
                {
                    "IPID_MAXMIND_DB": "",
                    "IPID_MAXMIND_DB_S3": self.uri,
                    "IPID_MAXMIND_DB_SHA256": self.sha256,
                    "IPID_MAXMIND_DB_VERSION": "2026-11-01",
                    "IPID_MAXMIND_DB_MAX_BYTES": str(256 << 20),
                    "IPID_ANALYSIS_S3_ROOTS": json.dumps({"raw": "s3://example/raw"}),
                    "IPID_ANALYSIS_LOCK_DIR": str(self.root / "locks"),
                },
            )
        )
        stack.enter_context(patch("ipid_analysis.s3_workflow.RAW_STORAGE_DIR", self.raw))
        stack.enter_context(patch.object(maxmind, "REFERENCES_DIR", self.root / "references"))
        stack.enter_context(patch.object(maxmind, "mounted_storage", return_value=True))
        stack.enter_context(patch.object(maxmind, "ram_scratch", return_value=True))
        stack.enter_context(patch.object(runtime, "ram_scratch", return_value=True))
        stack.enter_context(patch.object(maxmind, "scratch_root", return_value=self.scratch))
        self.free = stack.enter_context(
            patch.object(runtime, "scratch_free", return_value=64 << 20)
        )
        self.copy_free = stack.enter_context(
            patch.object(maxmind, "scratch_free", return_value=64 << 20)
        )
        self.validate = stack.enter_context(patch.object(maxmind, "validate_storage"))
        self.budget = stack.enter_context(patch.object(maxmind, "check_scratch_budget"))
        self.metadata = stack.enter_context(
            patch.object(
                maxmind,
                "_metadata",
                return_value={
                    "database_type": "GeoLite2-Country",
                    "build_epoch": 1793491200,
                },
            )
        )
        self.addCleanup(maxmind._staged.clear)

    def test_s3_copy_is_pinned_atomic_and_reports_identity(self):
        from ipid_analysis.paper_figures import default_maxmind_database

        database = default_maxmind_database()
        self.assertEqual(database.read_bytes(), self.content)
        self.assertIn(self.scratch, database.parents)
        self.assertEqual(self.source.read_bytes(), self.content)
        self.assertFalse(list(self.scratch.rglob("*.part")))
        self.budget.assert_called()
        self.assertEqual(
            maxmind.database_provenance(database),
            {
                "source_uri": self.uri,
                "version": "2026-11-01",
                "sha256": self.sha256,
                "size_bytes": len(self.content),
                "database_type": "GeoLite2-Country",
                "build_epoch": 1793491200,
            },
        )

    def test_reuses_verified_copy_within_job_without_reading_remote_again(self):
        first = maxmind.default_database()
        self.source.unlink()
        self.assertEqual(maxmind.default_database(), first)

    def test_checksum_mismatch_cannot_publish_and_removes_partial(self):
        with patch.dict(os.environ, {"IPID_MAXMIND_DB_SHA256": "0" * 64}):
            with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                maxmind.default_database()
        self.assertEqual(list((self.scratch / "maxmind").iterdir()), [])

    def test_source_size_limit_applies_before_copy(self):
        with patch.dict(os.environ, {"IPID_MAXMIND_DB_MAX_BYTES": "1"}):
            with self.assertRaisesRegex(ValueError, "size limit"):
                maxmind.default_database()
        self.metadata.assert_not_called()

    def test_reservation_respects_remaining_job_scratch_before_copy(self):
        self.free.return_value = 1
        with self.assertRaisesRegex(OSError, "insufficient unreserved"):
            maxmind.default_database()
        self.assertEqual(list((self.scratch / "maxmind").iterdir()), [])

    def test_scratch_exhaustion_during_copy_removes_partial(self):
        self.copy_free.return_value = 1
        with self.assertRaisesRegex(OSError, "insufficient RAM scratch"):
            maxmind.default_database()
        self.assertEqual(list((self.scratch / "maxmind").iterdir()), [])

    def test_global_budget_failure_cannot_publish(self):
        self.budget.side_effect = OSError("RAM scratch budget exceeded")
        with self.assertRaisesRegex(OSError, "budget exceeded"):
            maxmind.default_database()
        self.assertEqual(list((self.scratch / "maxmind").iterdir()), [])

    def test_invalid_database_cannot_become_cached_input(self):
        self.metadata.side_effect = ValueError("invalid MMDB")
        with self.assertRaisesRegex(ValueError, "invalid MMDB"):
            maxmind.default_database()
        self.assertEqual(list((self.scratch / "maxmind").iterdir()), [])

    def test_changed_cached_copy_is_rejected(self):
        database = maxmind.default_database()
        database.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "cached MaxMind"):
            maxmind.default_database()
        with self.assertRaisesRegex(ValueError, "changed after preparation"):
            maxmind.database_provenance(database)

    def test_configuration_errors_do_not_fall_back_to_unpinned_reference(self):
        references = maxmind.REFERENCES_DIR
        references.mkdir()
        (references / "GeoLite2-Country.mmdb").write_bytes(self.content)
        for key, value in (
            ("IPID_MAXMIND_DB_SHA256", ""),
            ("IPID_MAXMIND_DB_VERSION", ""),
            ("IPID_MAXMIND_DB_MAX_BYTES", "0"),
            ("IPID_MAXMIND_DB_S3", "https://invalid/db.mmdb"),
        ):
            with self.subTest(key=key), patch.dict(os.environ, {key: value}):
                with self.assertRaises(ValueError):
                    maxmind.default_database()

    def test_source_must_map_inside_configured_mount(self):
        for uri in (
            "s3://other-bucket/db.mmdb",
            "s3://example/raw/../db.mmdb",
        ):
            with self.subTest(uri=uri), patch.dict(os.environ, {"IPID_MAXMIND_DB_S3": uri}):
                with self.assertRaises(ValueError):
                    maxmind.default_database()
        self.assertFalse((self.scratch / "maxmind").exists())

    def test_disk_scratch_is_rejected_for_s3_mode(self):
        with patch.object(maxmind, "ram_scratch", return_value=False):
            with self.assertRaisesRegex(ValueError, "RAM scratch"):
                maxmind.default_database()

    def test_existing_local_override_still_wins_and_has_content_provenance(self):
        with patch.dict(os.environ, {"IPID_MAXMIND_DB": str(self.source)}):
            self.assertEqual(maxmind.default_database(), self.source)
            info = maxmind.database_provenance(self.source)
        self.assertEqual(info["source_path"], str(self.source))
        self.assertEqual(info["sha256"], self.sha256)
        self.validate.assert_not_called()

    def test_legacy_reference_search_and_absent_database_are_unchanged(self):
        with patch.dict(os.environ, {"IPID_MAXMIND_DB_S3": ""}):
            self.assertIsNone(maxmind.default_database())
            maxmind.REFERENCES_DIR.mkdir()
            database = maxmind.REFERENCES_DIR / "GeoLite2-City.mmdb"
            database.write_bytes(self.content)
            self.assertEqual(maxmind.default_database(), database)

    def test_real_reader_rejects_invalid_mmdb(self):
        with patch.object(maxmind, "_metadata", wraps=_real_metadata):
            with self.assertRaises(ValueError):
                maxmind.default_database()
        self.assertEqual(list((self.scratch / "maxmind").iterdir()), [])


_real_metadata = maxmind._metadata


if __name__ == "__main__":
    unittest.main()
