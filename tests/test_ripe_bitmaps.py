import bz2
from datetime import date, datetime, timezone
import ipaddress
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import ripe_atlas as ripe
from ipid_analysis.ripe_bitmaps import ROLE_SCHEMA, RoleBitmaps, trace_roles
from ipid_analysis.ripe_cache import RoleCache


def trace(hops, source="10.0.0.1", destination="1.1.1.1"):
    return {
        "af": 4,
        "src_addr": source,
        "dst_addr": destination,
        "timestamp": 1768478400,
        "result": [
            {"hop": ttl, "result": [{"from": ip} for ip in addresses]} for ttl, addresses in hops
        ],
    }


def roles(record):
    return {str(ipaddress.IPv4Address(ip)): flags for ip, flags in trace_roles(record).items()}


class BitmapTests(unittest.TestCase):
    def test_last_response_and_same_ttl_are_not_transit(self):
        self.assertEqual(
            roles(trace([(1, ["8.8.8.8", "9.9.9.9"])])),
            {"8.8.8.8": (False, False), "9.9.9.9": (False, False)},
        )

    def test_out_of_order_hops_private_predecessor_and_missing_source(self):
        record = trace([(5, ["1.1.1.1"]), (3, ["8.8.8.8"]), (1, ["192.168.1.1"])], source=None)
        self.assertEqual(roles(record), {"8.8.8.8": (True, False), "1.1.1.1": (False, True)})
        self.assertFalse(
            roles(trace([(3, ["8.8.8.8"]), (5, ["1.1.1.1"])], source=None))["8.8.8.8"][0]
        )

    def test_private_successor_does_not_prove_transit(self):
        self.assertFalse(roles(trace([(1, ["8.8.8.8"]), (2, ["10.0.0.2"])]))["8.8.8.8"][0])

    def test_source_and_destination_never_transit_within_same_trace(self):
        result = roles(
            trace([(1, ["8.8.8.8"]), (2, ["1.1.1.1"]), (3, ["9.9.9.9"])], source="8.8.8.8")
        )
        self.assertEqual(result["8.8.8.8"], (False, False))
        self.assertEqual(result["1.1.1.1"], (False, True))

    def test_duplicate_ttl_and_invalid_answers(self):
        record = trace([(1, ["8.8.8.8"]), (1, ["1.1.1.1", "bad", "::1", "224.0.0.1"])])
        self.assertEqual(roles(record), {"8.8.8.8": (False, False), "1.1.1.1": (False, True)})

    def test_destination_flag_does_not_invent_a_later_hop(self):
        record = trace([(1, ["8.8.8.8"])])
        record["destination_ip_responded"] = True
        self.assertEqual(roles(record), {"8.8.8.8": (False, False), "1.1.1.1": (False, True)})

    def test_bit_boundaries_seen_only_and_or_union(self):
        bitmap = RoleBitmaps()
        try:
            for ip in (0, 7, 8, 262143, 262144, 2**32 - 1):
                bitmap.add(ip, False, False)
            bitmap.add(8, True, False)
            bitmap.add(8, False, True)
            rows = [row for batch in bitmap.batches() for row in batch.to_pylist()]
            self.assertEqual([row["IP_V4"] for row in rows], [0, 7, 8, 262143, 262144, 2**32 - 1])
            self.assertEqual(rows[2], {"IP_V4": 8, "T": True, "D": True})
            self.assertEqual(rows[-1]["T"], False)
        finally:
            bitmap.close()


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"IPID_ANALYSIS_RIPE_CACHE_S3": "s3://test/roles"})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.objects = {}
        self.puts = []

        def get(client, uri, path):
            if uri not in self.objects:
                return False
            path.write_bytes(self.objects[uri])
            return True

        def put(client, path, uri):
            self.objects[uri] = path.read_bytes()
            self.puts.append(uri)

        self.get = patch.object(RoleCache, "_get", get)
        self.put = patch.object(RoleCache, "_put", put)
        self.get.start()
        self.put.start()
        self.addCleanup(self.get.stop)
        self.addCleanup(self.put.stop)

    def test_roundtrip_integrity_manifest_last_and_version(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data, meta = root / "a.pq", root / "a.json"
            pq.write_table(
                pa.table({"IP_V4": [1], "T": [False], "D": [False]}, schema=ROLE_SCHEMA), data
            )
            meta.write_text(json.dumps({"cache_version": "2"}))
            cache = RoleCache()
            cache.publish("parts/test", data, meta)
            self.assertTrue(self.puts[-1].endswith("manifest.json"))
            self.assertFalse(cache.restore("parts/test", root / "b.pq", root / "b.json", "1"))
            self.assertTrue(cache.restore("parts/test", root / "b.pq", root / "b.json", "2"))
            self.assertEqual(data.read_bytes(), (root / "b.pq").read_bytes())
            self.objects[self.puts[0]] = b"corrupt"
            with self.assertRaisesRegex(ValueError, "checksum"):
                cache.restore("parts/test", root / "c.pq", root / "c.json", "2")
            self.assertFalse((root / "c.json").exists())

    def test_daily_and_window_cache_survive_raw_removal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = dict(
                campaign_start=datetime(2026, 1, 29, tzinfo=timezone.utc),
                dump_samples=1,
                preprocess_workers=1,
                raw_root=root / "raw",
                processed_root=root / "processed",
            )

            def download(url, path, **kwargs):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(
                    bz2.compress(
                        (json.dumps(trace([(1, ["8.8.8.8"]), (2, ["1.1.1.1"])])) + "\n").encode()
                    )
                )
                return path

            with (
                patch.object(ripe, "_available_dump_days", return_value=(date(2026, 1, 15),)),
                patch.object(ripe, "_remote_file_size", return_value=100),
                patch.object(ripe, "_download", side_effect=download) as downloads,
            ):
                first = ripe.prepare_ripe_atlas(**args)
                expected = pq.read_table(first.roles_path).to_pylist()
                self.assertEqual(downloads.call_count, 1)
                self.assertEqual(list((root / "processed").rglob("*.bz2")), [])
            with patch.object(
                ripe, "_available_dump_days", side_effect=AssertionError("must not crawl")
            ):
                first.roles_path.write_bytes(b"corrupt local window cache")
                repaired = ripe.prepare_ripe_atlas(**args)
                self.assertEqual(pq.read_table(repaired.roles_path).to_pylist(), expected)
            shutil.rmtree(root / "processed")
            with patch.object(
                ripe, "_available_dump_days", side_effect=AssertionError("must not crawl")
            ):
                restored = ripe.prepare_ripe_atlas(**args)
                self.assertEqual(pq.read_table(restored.roles_path).to_pylist(), expected)
            # A neighboring campaign reuses the same hour from S3 without downloading.
            args["campaign_start"] = datetime(2026, 1, 30, tzinfo=timezone.utc)
            with (
                patch.object(ripe, "_available_dump_days", return_value=(date(2026, 1, 15),)),
                patch.object(
                    ripe, "_remote_file_size", side_effect=AssertionError("must not fetch raw")
                ),
                patch.object(
                    ripe, "_parse_role_file", side_effect=AssertionError("must not parse")
                ),
            ):
                reused = ripe.prepare_ripe_atlas(**args)
                self.assertEqual(pq.read_table(reused.roles_path).to_pylist(), expected)


if __name__ == "__main__":
    unittest.main()
