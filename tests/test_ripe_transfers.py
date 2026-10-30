"""Shared RIPE transfer lifecycle with fake HTTP and isolated local storage."""

import bz2
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import date, datetime, timezone
import io
import json
import multiprocessing
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from ipid_analysis import ripe_atlas as ripe
from ipid_analysis.runtime import cleanup_work, directory_bytes, ripe_transfer_root

DAY = date(2026, 1, 15)
START = datetime(2026, 1, 29, tzinfo=timezone.utc)
BODY = bz2.compress(
    (
        json.dumps(
            {
                "af": 4,
                "timestamp": 1768478400,
                "src_addr": "10.0.0.1",
                "dst_addr": "1.1.1.1",
                "result": [
                    {"hop": 1, "result": [{"from": "8.8.8.8"}]},
                    {"hop": 2, "result": [{"from": "1.1.1.1"}]},
                ],
            }
        )
        + "\n"
    ).encode()
)


class Response(io.BytesIO):
    def __init__(self, data, *, offset=0):
        super().__init__(data)
        self.status = 206 if offset else 200
        self.headers = {"ETag": '"fixture"', "Content-Length": str(len(BODY) - offset)}
        if offset:
            self.headers["Content-Range"] = f"bytes {offset}-{len(BODY) - 1}/{len(BODY)}"


def concurrent_prepare(arguments):
    """Separate processes have genuinely different job TMP_DIR environments."""
    root, job, day = arguments
    root = Path(root)
    os.environ.update(
        IPID_ANALYSIS_STORAGE_MODE="local",
        IPID_ANALYSIS_RIPE_CACHE_S3="",
        IPID_ANALYSIS_TMP_DIR=str(root / job),
        IPID_ANALYSIS_RIPE_TRANSFER_DIR=str(root / "transfers"),
    )

    def open_http(request, **kwargs):
        with (root / "http-calls.txt").open("a", encoding="utf-8") as output:
            output.write(request.full_url + "\n")
        return Response(BODY)

    with (
        patch.object(ripe, "_available_dump_days", return_value=(DAY,)),
        patch.object(ripe, "_remote_file_size", return_value=len(BODY)),
        patch.object(ripe, "urlopen", side_effect=open_http),
    ):
        result = ripe.prepare_ripe_atlas(
            campaign_start=START.replace(day=day),
            dump_samples=1,
            preprocess_workers=1,
            raw_root=root / "raw",
            processed_root=root / "processed",
        )
    return str(result.roles_path)


class SharedTransfersTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        environment = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_RIPE_CACHE_S3": "",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "job-a"),
                "IPID_ANALYSIS_RIPE_TRANSFER_DIR": str(self.root / "transfers"),
                "IPID_ANALYSIS_RIPE_TRANSFER_BYTES": str(8 << 20),
            },
        )
        environment.start()
        self.addCleanup(environment.stop)

    def inputs(self, samples=1):
        return ripe._daily_dump_files(
            ripe.campaign_window(START),
            samples=samples,
            raw_root=self.root / "raw",
            base_url=ripe.DEFAULT_DUMP_BASE_URL,
            parts_root=self.root / "processed/ripe-atlas/file-parts",
        )[0]

    def prepare(self):
        return ripe.prepare_ripe_atlas(
            campaign_start=START,
            dump_samples=1,
            preprocess_workers=1,
            raw_root=self.root / "raw",
            processed_root=self.root / "processed",
        )

    def test_complete_unprocessed_download_survives_job_cleanup_without_http_or_head(self):
        with (
            patch.object(ripe, "_available_dump_days", return_value=(DAY,)),
            patch.object(ripe, "_remote_file_size", return_value=len(BODY)),
            patch.object(ripe, "urlopen", return_value=Response(BODY)) as http,
        ):
            first = list(self.inputs())[0][0]
        self.assertEqual(http.call_count, 1)
        self.assertEqual(first.read_bytes(), BODY)
        (self.root / "job-a").mkdir(exist_ok=True)
        cleanup_work(self.root / "job-a", root=self.root)
        with (
            patch.dict(os.environ, {"IPID_ANALYSIS_TMP_DIR": str(self.root / "job-b")}),
            patch.object(ripe, "_available_dump_days", return_value=(DAY,)),
            patch.object(ripe, "_remote_file_size", side_effect=AssertionError("HEAD not needed")),
            patch.object(ripe, "urlopen", side_effect=AssertionError("valid download repeated")),
        ):
            second = list(self.inputs())[0][0]
            result = ripe.build_role_lookup(
                [(second, first.parent.name, True)],
                self.root / "processed/ripe-atlas/window/source/roles.pq",
                window=ripe.campaign_window(START),
                source="daily-dumps-s1",
                workers=1,
            )
        self.assertEqual(first, second)
        self.assertEqual(result["addresses"], 2)
        self.assertFalse(first.parent.exists())

    def test_failed_transfer_resumes_in_a_different_job_after_cleanup(self):
        offset = len(BODY) // 2
        real_download = ripe._download

        def one_attempt(url, destination, **kwargs):
            return real_download(url, destination, max_attempts=1, **kwargs)

        with (
            patch.object(ripe, "_available_dump_days", return_value=(DAY,)),
            patch.object(ripe, "_remote_file_size", return_value=len(BODY)),
            patch.object(ripe, "_download", side_effect=one_attempt),
            patch.object(ripe, "urlopen", return_value=Response(BODY[:offset])),
        ):
            with self.assertRaisesRegex(OSError, "incomplete HTTP download"):
                self.prepare()
        partial = next((self.root / "transfers").rglob("*.bz2.part"))
        self.assertEqual(partial.stat().st_size, offset)
        cleanup_work(self.root / "job-a", root=self.root)

        def resume(request, **kwargs):
            self.assertEqual(request.get_header("Range"), f"bytes={offset}-")
            self.assertEqual(request.get_header("If-range"), '"fixture"')
            return Response(BODY[offset:], offset=offset)

        with (
            patch.dict(os.environ, {"IPID_ANALYSIS_TMP_DIR": str(self.root / "job-b")}),
            patch.object(ripe, "_available_dump_days", return_value=(DAY,)),
            patch.object(ripe, "_remote_file_size", return_value=len(BODY)),
            patch.object(ripe, "urlopen", side_effect=resume) as http,
        ):
            result = self.prepare()
        self.assertEqual(http.call_count, 1)
        self.assertTrue(result.roles_path.is_file())
        self.assertEqual(list((self.root / "transfers/downloads").rglob("*")), [])

    def test_prefetch_failure_checkpoints_the_other_completed_download(self):
        ready = threading.Event()
        real_download = ripe._download

        def fetch(url, destination, **kwargs):
            if "T0000" in url:
                self.assertTrue(ready.wait(5))
                raise OSError("failed earlier slot")
            result = real_download(url, destination, **kwargs)
            ready.set()
            return result

        with (
            patch.object(ripe, "_available_dump_days", return_value=(DAY,)),
            patch.object(ripe, "_remote_file_size", return_value=len(BODY)),
            patch.object(ripe, "_download", side_effect=fetch),
            patch.object(ripe, "urlopen", side_effect=lambda *a, **k: Response(BODY)),
        ):
            with self.assertRaisesRegex(OSError, "failed earlier slot"):
                ripe.build_role_lookup(
                    self.inputs(samples=2),
                    self.root / "processed/ripe-atlas/window/source/roles.pq",
                    window=ripe.campaign_window(START),
                    source="daily-dumps-s2",
                    workers=1,
                )
        parts = list((self.root / "processed/ripe-atlas/file-parts").glob("*.pq"))
        self.assertEqual(len(parts), 1)
        self.assertEqual(
            ripe._cached_role_part(parts[0], parts[0].with_suffix(".json"))["addresses"], 2
        )
        self.assertFalse((self.root / "processed/ripe-atlas/window/source/roles.pq").exists())

    def test_concurrent_distinct_job_roots_and_windows_download_and_parse_shared_hour_once(self):
        for days in ((29, 29), (29, 30)):
            with self.subTest(days=days):
                root = self.root / f"windows-{days[0]}-{days[1]}"
                root.mkdir()
                (root / "transfers/locks").mkdir(parents=True)
                (root / "processed/ripe-atlas/file-parts").mkdir(parents=True)
                context = multiprocessing.get_context("spawn")
                with ProcessPoolExecutor(max_workers=2, mp_context=context) as executor:
                    paths = list(
                        executor.map(
                            concurrent_prepare,
                            [
                                (str(root), "job-a", days[0]),
                                (str(root), "job-b", days[1]),
                            ],
                        )
                    )
                self.assertEqual(len((root / "http-calls.txt").read_text().splitlines()), 1)
                self.assertEqual(
                    len(list((root / "processed/ripe-atlas/file-parts").glob("*.pq"))), 1
                )
                self.assertTrue(all(Path(path).is_file() for path in paths))
                self.assertEqual(paths[0] == paths[1], days[0] == days[1])

    def test_current_window_transfers_are_not_evicted_and_other_windows_are_lru_bounded(self):
        root = ripe_transfer_root()
        inactive = root / "downloads" / ("a" * 64)
        inactive.mkdir(parents=True)
        (inactive / "old.bz2.part").write_bytes(b"x" * (3 << 20))
        protected = root / "downloads" / ("b" * 64)
        protected.mkdir(parents=True)
        (protected / "current.bz2.part").write_bytes(b"keep")
        persistent = self.root / "processed/ripe-atlas/file-parts/valid.pq"
        persistent.parent.mkdir(parents=True)
        persistent.write_bytes(b"persistent cache is untouched")
        with patch.dict(os.environ, {"IPID_ANALYSIS_RIPE_TRANSFER_BYTES": str(4 << 20)}):
            budget = ripe._TransferBudget([protected.name, "c" * 64])
            with budget.reserve(root / "downloads" / ("c" * 64) / "new.bz2", 2 << 20):
                self.assertFalse(inactive.exists())
        self.assertEqual((protected / "current.bz2.part").read_bytes(), b"keep")
        self.assertEqual(persistent.read_bytes(), b"persistent cache is untouched")
        self.assertLess(directory_bytes(root), 4 << 20)

    def test_oversized_input_fails_without_creating_a_transfer(self):
        budget = ripe._TransferBudget([])
        destination = budget.root / "downloads/too-large/file.bz2"
        with self.assertRaisesRegex(OSError, "exceeds shared transfer budget"):
            with budget.reserve(destination, budget.limit):
                self.fail("oversized transfer admitted")
        self.assertFalse(destination.exists())

    def test_prefetch_failure_cancels_space_waiters_without_a_six_hour_stall(self):
        budget = ripe._TransferBudget([])
        budget.failed.set()
        with self.assertRaisesRegex(OSError, "prefetch stopped"):
            with budget.reserve(budget.root / "waiting.bz2", 1):
                self.fail("a failed prefetch must not wait for space")

    def test_incompatible_retained_partials_fail_without_waiting_or_discarding_them(self):
        with patch.dict(os.environ, {"IPID_ANALYSIS_RIPE_TRANSFER_BYTES": str(2 << 20)}):
            budget = ripe._TransferBudget(["a" * 64])
        directory = budget.root / "downloads" / ("a" * 64)
        directory.mkdir(parents=True)
        partial = directory / "old.bz2.part"
        partial.write_bytes(b"x" * (1 << 20))
        with self.assertRaisesRegex(OSError, "protected partials retained"):
            with budget.reserve(directory / "new.bz2", 1 << 20):
                self.fail("unfulfillable budget must not wait for a nonexistent parser")
        self.assertEqual(partial.stat().st_size, 1 << 20)

    def test_invalid_retained_raw_file_is_neither_overwritten_nor_used_for_new_transfer(self):
        original = (
            self.root / "raw/ripe-atlas/daily-dumps/2026-01-15/traceroute-2026-01-15T0000.bz2"
        )
        original.parent.mkdir(parents=True)
        original.write_bytes(b"old partial, keep for inspection")
        with (
            patch.object(ripe, "_available_dump_days", return_value=(DAY,)),
            patch.object(ripe, "_remote_file_size", return_value=len(BODY)),
            patch.object(ripe, "urlopen", return_value=Response(BODY)),
        ):
            source, _, cleanup = list(self.inputs())[0]
        self.assertIn(ripe_transfer_root(), source.parents)
        self.assertTrue(cleanup)
        self.assertEqual(original.read_bytes(), b"old partial, keep for inspection")

    def test_complete_download_is_consumable_when_metadata_uses_the_last_free_bytes(self):
        with patch.dict(os.environ, {"IPID_ANALYSIS_RIPE_TRANSFER_BYTES": str(2 << 20)}):
            budget = ripe._TransferBudget(["a" * 64])
        destination = budget.root / "downloads" / ("a" * 64) / "complete.bz2"
        destination.parent.mkdir(parents=True)
        destination.write_bytes(b"x" * (1 << 20))
        destination.with_suffix(".http.json").write_bytes(b"metadata")
        with patch.object(ripe, "DEFAULT_LOCK_TIMEOUT_SECONDS", 0.01):
            with budget.reserve(destination, 1 << 20):
                pass  # No extra raw bytes or sidecars will be written on reuse.

    def test_legacy_work_directory_selects_a_shared_subdirectory(self):
        with patch.dict(os.environ, {"IPID_ANALYSIS_RIPE_WORK_DIR": str(self.root / "legacy")}):
            del os.environ["IPID_ANALYSIS_RIPE_TRANSFER_DIR"]
            self.assertEqual(
                ripe_transfer_root(), (self.root / "legacy/shared-transfers").resolve()
            )

    def test_concurrent_download_reservations_wait_instead_of_oversubscribing(self):
        with patch.dict(os.environ, {"IPID_ANALYSIS_RIPE_TRANSFER_BYTES": str(2 << 20)}):
            budget = ripe._TransferBudget([])
        entered = threading.Event()

        def second():
            with budget.reserve(budget.root / "second", 1):
                entered.set()

        with ThreadPoolExecutor(max_workers=1) as executor:
            with budget.reserve(budget.root / "first", 1):
                future = executor.submit(second)
                self.assertFalse(entered.wait(0.2))
            future.result(timeout=5)
        self.assertTrue(entered.is_set())

    def test_transfer_root_and_locks_are_independent_of_job_tmp(self):
        first = ripe._cache_lock_path("ripe-resources", "resources.lock")
        with patch.dict(os.environ, {"IPID_ANALYSIS_TMP_DIR": str(self.root / "job-b")}):
            second = ripe._cache_lock_path("ripe-resources", "resources.lock")
        self.assertEqual(first, second)

    def test_dead_locks_from_other_keys_are_reclaimed_without_stealing_live_owners(self):
        dead = ripe._cache_lock_path("old-window", "part.lock")
        live = ripe._cache_lock_path("current-window", "part.lock")
        dead.parent.mkdir(parents=True, exist_ok=True)
        dead.write_text("pid=512719\n")
        with ripe._exclusive_lock(live):
            with patch.object(ripe, "_pid_is_alive", return_value=False):
                ripe._prune_stale_cache_locks()
            self.assertFalse(dead.exists())
            self.assertTrue(live.exists())

    def test_production_transfer_path_cannot_escape_analysis_checkout(self):
        with (
            patch.dict(os.environ, {"IPID_ANALYSIS_STORAGE_MODE": "mounted-s3"}),
            self.assertRaisesRegex(ValueError, "inside the analysis checkout"),
        ):
            ripe_transfer_root()


if __name__ == "__main__":
    unittest.main()
