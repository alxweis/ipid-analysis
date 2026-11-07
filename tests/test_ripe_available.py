import bz2
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import ripe_atlas as ripe
from ipid_analysis.artifacts import completion_path, publish_file
from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.plot_ripe_atlas_strategy import render_ripe_atlas_analysis
from ipid_analysis.ripe_cache import RoleCache


class AvailableRipeWindowTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.args = dict(
            campaign_start=datetime(2026, 11, 7, tzinfo=timezone.utc),
            lookback_days=28,
            preprocess_workers=1,
            raw_root=self.root / "raw",
            processed_root=self.root / "processed",
        )
        env = patch.dict(os.environ, {
            "IPID_ANALYSIS_STORAGE_MODE": "local",
            "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
            "IPID_ANALYSIS_CONTROL_ROOT": str(self.root / "control"),
            "IPID_ANALYSIS_RIPE_CACHE_S3": "",
            "IPID_ANALYSIS_RIPE_DOWNLOADS": "2",
        })
        env.start()
        self.addCleanup(env.stop)
        self.slots = ripe.daily_dump_slots(ripe.campaign_window(
            self.args["campaign_start"], lookback_days=28,
        ))

    def body(self, slot):
        record = {
            "af": 4, "type": "traceroute", "timestamp": int(slot.timestamp()),
            "src_addr": "10.0.0.1", "dst_addr": "1.1.1.1",
            "result": [
                {"hop": 1, "result": [{"from": "8.8.8.8"}]},
                {"hop": 2, "result": [{"from": "1.1.1.1"}]},
            ],
        }
        return bz2.compress((json.dumps(record) + "\n").encode())

    def retain_raw(self, slot, *, marker=False):
        target = self.root / "raw/ripe-atlas/daily-dumps" / f"{slot:%Y-%m-%d}" / f"traceroute-{slot:%Y-%m-%dT%H00}.bz2"
        target.parent.mkdir(parents=True, exist_ok=True)
        if marker:
            source = self.root / "source.bz2"
            source.write_bytes(self.body(slot))
            publish_file(source, target)
        else:
            target.write_bytes(self.body(slot))
        return target

    def unavailable(self, url):
        raise HTTPError(url, 404, "not published", {}, None)

    def test_27_of_28_days_prepare_plot_and_reuse_with_coverage(self):
        for slot in self.slots[:-1]:
            self.retain_raw(slot)
        with patch.object(ripe, "_remote_file_size", side_effect=self.unavailable) as heads:
            result = ripe.prepare_ripe_atlas(**self.args)
        self.assertEqual(heads.call_count, 1)
        info = json.loads(result.metadata_path.read_text())
        self.assertEqual(info["lookback_days"], 28)
        self.assertEqual(info["window_start"], "2026-10-10T00:00:00+00:00")
        self.assertEqual(info["window_end"], "2026-11-07T00:00:00+00:00")
        self.assertEqual(info["requested_dump_samples"], 28)
        self.assertEqual(info["dump_samples"], 27)
        self.assertFalse(info["daily_dump_complete"])
        self.assertAlmostEqual(info["daily_dump_coverage_percentage"], 100 * 27 / 28)
        self.assertEqual(info["missing_dump_slots"][0]["slot"], self.slots[-1].isoformat())
        self.assertEqual(info["daily_dump_missing_day_count"], 1)
        self.assertEqual(len(info["sampled_dump_slots"]), 27)
        # The prepared reference snapshot is reused even if an archive changes.
        with patch.object(ripe, "_remote_file_size", side_effect=AssertionError("repeated HTTP")):
            again = ripe.prepare_ripe_atlas(**self.args)
        self.assertEqual(result, again)

        source = IpidMeasurement(
            protocol="icmp", connection_mode="no-connection", interval="rt-based",
            scale="base", measurement_id="icmp_2026-11-07_02-30-00", zmap_id="icmp_2026-11-07_02-00-00",
        )
        strategies = source.artifact_path(self.root / "strategies", "strategies")
        strategies.parent.mkdir(parents=True)
        pq.write_table(pa.table({"IP_ADDR": ["8.8.8.8", "1.1.1.1"], "IPID_SELECTION_STRATEGY": ["SINGLE", "RANDOM"]}), strategies)
        with patch("ipid_analysis.plot_ripe_atlas_strategy.configure_paper_style"):
            outputs = render_ripe_atlas_analysis(
                source, result, processed_root=self.root / "strategies", figures_root=self.root / "figures", threads=1,
            )
        report = json.loads(outputs.role_json.read_text())
        self.assertEqual(report["ripe_atlas"]["temporal_coverage"]["dump_samples"], 27)
        self.assertEqual(report["ripe_atlas"]["temporal_coverage"]["requested_dump_samples"], 28)
        self.assertTrue(list((self.root / "figures").rglob("*.pdf")))

    def test_missing_slot_does_not_stop_later_archive_download(self):
        self.args["lookback_days"] = 3
        slots = self.slots[-3:]
        self.retain_raw(slots[0], marker=True)

        def size(url):
            if f"{slots[1]:%Y-%m-%dT%H00}" in url:
                raise HTTPError(url, 410, "gone", {}, None)
            return len(self.body(slots[2]))

        def download(url, destination, **kwargs):
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(self.body(slots[2]))
            return destination

        with (
            patch.object(ripe, "_remote_file_size", side_effect=size) as heads,
            patch.object(ripe, "_download", side_effect=download) as downloads,
        ):
            result = ripe.prepare_ripe_atlas(**self.args)
        self.assertEqual(heads.call_count, 2)
        self.assertEqual(downloads.call_count, 1)
        info = json.loads(result.metadata_path.read_text())
        self.assertEqual(info["dump_samples"], 2)
        self.assertEqual(info["missing_dump_slots"][0]["http_status"], 410)
        self.assertIn(slots[2].isoformat(), info["sampled_dump_slots"])

    def test_raw_s3_object_survives_archive_expiry_and_is_validated(self):
        self.args["lookback_days"] = 1
        raw = self.retain_raw(self.slots[-1], marker=True)
        with patch.object(ripe, "_remote_file_size", side_effect=AssertionError("archive contacted")):
            result = ripe.prepare_ripe_atlas(**self.args)
        self.assertEqual(json.loads(result.metadata_path.read_text())["dump_samples"], 1)
        # Same size and valid completion declaration cannot conceal changed bytes.
        content = bytearray(raw.read_bytes())
        content[-1] ^= 1
        raw.write_bytes(content)
        marker = completion_path(raw)
        metadata = json.loads(marker.read_text())
        metadata.pop("mtime_ns", None)
        marker.write_text(json.dumps(metadata))
        self.args["force"] = True
        shutil.rmtree(self.root / "processed")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            ripe.prepare_ripe_atlas(**self.args)

    def test_valid_s3_hour_part_is_used_before_archive(self):
        self.args["lookback_days"] = 1
        self.retain_raw(self.slots[-1])
        objects = {}

        def get(client, uri, target):
            if uri not in objects:
                return False
            target.write_bytes(objects[uri])
            return True

        def put(client, source, uri):
            objects[uri] = source.read_bytes()

        with (
            patch.dict(os.environ, {"IPID_ANALYSIS_RIPE_CACHE_S3": "s3://test/cache"}),
            patch.object(RoleCache, "_get", get), patch.object(RoleCache, "_put", put),
        ):
            ripe.prepare_ripe_atlas(**self.args)
            shutil.rmtree(self.root / "processed")
            shutil.rmtree(self.root / "raw")
            # Simulate another window cache being unavailable while hour parts remain.
            objects = {key: value for key, value in objects.items() if "/windows/" not in key}
            with patch.object(ripe, "_remote_file_size", side_effect=AssertionError("archive contacted")):
                result = ripe.prepare_ripe_atlas(**self.args)
        self.assertEqual(json.loads(result.metadata_path.read_text())["dump_samples"], 1)

    def test_non_missing_http_errors_are_not_silently_skipped(self):
        self.args["lookback_days"] = 2
        self.retain_raw(self.slots[-2])
        for status in (403, 429, 500, 503):
            with self.subTest(status=status), patch.object(
                ripe, "_remote_file_size", side_effect=HTTPError("https://example.invalid/dump", status, "error", {}, None),
            ), self.assertRaises(HTTPError):
                ripe.prepare_ripe_atlas(**self.args)
        self.assertFalse(list((self.root / "processed").rglob("ipv4-roles.pq")))

    def test_no_available_inputs_never_publish_an_empty_reference(self):
        self.args["lookback_days"] = 2
        with patch.object(ripe, "_remote_file_size", side_effect=self.unavailable), self.assertRaisesRegex(ValueError, "input list is empty"):
            ripe.prepare_ripe_atlas(**self.args)
        self.assertFalse(list((self.root / "processed").rglob("ipv4-roles.pq")))


if __name__ == "__main__":
    unittest.main()
