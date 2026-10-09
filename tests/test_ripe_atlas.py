import bz2
from datetime import date, datetime, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.caida_itdk import ITDKDataset
from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.plot_ripe_atlas_strategy import render_ripe_atlas_analysis
from ipid_analysis.ripe_atlas import (
    RipeAtlasDataset,
    _available_dump_days,
    _download,
    build_role_lookup,
    campaign_window,
    infer_campaign_start,
    prepare_ripe_atlas,
    sampled_available_dump_slots,
    sampled_dump_slots,
)


class _HTTPResponse(io.BytesIO):
    def __init__(self, data: bytes, *, status: int):
        super().__init__(data)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def _record(
    timestamp: int,
    destination: str,
    hops: list[str],
    *,
    measurement: int,
    probe: int,
    protocol: str = "ICMP",
) -> dict:
    return {
        "af": 4,
        "type": "traceroute",
        "timestamp": timestamp,
        "dst_addr": destination,
        "destination_ip_responded": destination in hops,
        "msm_id": measurement,
        "prb_id": probe,
        "proto": protocol,
        "result": [
            {"hop": index, "result": [{"from": address, "rtt": float(index)}]}
            for index, address in enumerate(hops, start=1)
        ],
    }


class RipeAtlasTest(unittest.TestCase):
    def test_campaign_window_prefers_zmap_and_samples_evenly(self):
        manifest = {
            "icmp": {
                "zmap": "icmp_2026-09-21_02-09-12",
                "ipid": {"no-connection": {"rt-based": {"base": "icmp_2026-09-21_04-00-00"}}},
            }
        }
        start = infer_campaign_start(manifest)
        self.assertEqual(start, datetime(2026, 9, 21, 2, 9, 12, tzinfo=timezone.utc))
        window = campaign_window(start)
        self.assertEqual(window.start.date().isoformat(), "2026-08-24")
        self.assertEqual(window.end.date().isoformat(), "2026-09-21")
        slots = sampled_dump_slots(window, 4)
        self.assertEqual(len(slots), 4)
        self.assertEqual(len(set(slots)), 4)
        self.assertTrue(all(window.start <= slot < window.end for slot in slots))
        self.assertEqual([slot.hour for slot in slots], [0, 7, 14, 21])

    def test_daily_dump_slots_use_available_overlap_only(self):
        window = campaign_window(datetime(2026, 9, 21, 2, 9, 12, tzinfo=timezone.utc))
        available = [date(2026, 9, day) for day in range(9, 21)]
        slots = sampled_available_dump_slots(window, 4, available)
        self.assertEqual(
            [slot.isoformat() for slot in slots],
            [
                "2026-09-10T00:00:00+00:00",
                "2026-09-13T07:00:00+00:00",
                "2026-09-16T14:00:00+00:00",
                "2026-09-19T21:00:00+00:00",
            ],
        )
        self.assertTrue(all(window.start <= slot < window.end for slot in slots))

    def test_daily_dump_directory_index_is_parsed(self):
        response = MagicMock()
        response.__enter__.return_value = response
        response.read.return_value = b"""
            <a href="2026-09-10/">2026-09-10/</a>
            <a href="not-a-day/">not-a-day/</a>
            <a href="2026-09-09/">2026-09-09/</a>
        """
        with patch("ipid_analysis.ripe_atlas.urlopen", return_value=response):
            days = _available_dump_days("https://example.invalid/dumps")
        self.assertEqual(days, (date(2026, 9, 9), date(2026, 9, 10)))

    def test_download_retains_and_resumes_incomplete_stream(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "dump.bz2"
            with patch(
                "ipid_analysis.ripe_atlas.urlopen",
                return_value=_HTTPResponse(b"abc", status=200),
            ):
                with self.assertRaisesRegex(OSError, "3/5 bytes"):
                    _download("https://example.invalid/dump", destination, expected_size=5)
            self.assertFalse(destination.exists())
            self.assertEqual(destination.with_suffix(".bz2.part").read_bytes(), b"abc")

            with patch(
                "ipid_analysis.ripe_atlas.urlopen",
                return_value=_HTTPResponse(b"de", status=206),
            ):
                _download("https://example.invalid/dump", destination, expected_size=5)
            self.assertEqual(destination.read_bytes(), b"abcde")

    def test_download_recovers_truncated_final_file(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "dump.bz2"
            destination.write_bytes(b"abc")
            with patch(
                "ipid_analysis.ripe_atlas.urlopen",
                return_value=_HTTPResponse(b"de", status=206),
            ):
                _download("https://example.invalid/dump", destination, expected_size=5)
            self.assertEqual(destination.read_bytes(), b"abcde")

    def test_daily_dump_slots_reject_missing_overlap(self):
        window = campaign_window(datetime(2026, 9, 21, 2, 9, 12, tzinfo=timezone.utc))
        with self.assertRaisesRegex(FileNotFoundError, "--source api"):
            sampled_available_dump_slots(window, 4, [date(2026, 10, 1)])

    def _dataset(self, root: Path) -> RipeAtlasDataset:
        campaign_start = datetime(2026, 1, 29, 12, tzinfo=timezone.utc)
        window = campaign_window(campaign_start)
        timestamp = int(datetime(2026, 1, 15, 12, tzinfo=timezone.utc).timestamp())
        records = [
            _record(
                timestamp,
                "1.1.1.1",
                ["10.0.0.1", "8.8.8.8", "1.1.1.1"],
                measurement=10,
                probe=100,
            ),
            _record(
                timestamp + 60,
                "8.8.8.8",
                ["9.9.9.9", "8.8.8.8"],
                measurement=11,
                probe=101,
                protocol="TCP",
            ),
            {"af": 6, "type": "traceroute", "timestamp": timestamp, "result": []},
        ]
        source = root / "traceroute.jsonl.bz2"
        with bz2.open(source, "wt", encoding="utf-8") as output:
            for record in records:
                output.write(json.dumps(record) + "\n")
        directory = root / "ripe"
        dataset = RipeAtlasDataset(
            source="test",
            window=window,
            directory=directory,
            roles_path=directory / "ipv4-roles.pq",
            metadata_path=directory / "source.json",
        )
        stats = build_role_lookup(
            [source], dataset.roles_path, window=window, source=dataset.source
        )
        dataset.metadata_path.write_text(json.dumps(stats), encoding="utf-8")
        self.assertEqual(stats["traces"], 2)
        self.assertEqual(stats["addresses"], 3)
        return dataset

    def test_role_lookup_and_strategy_plot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = self._dataset(root)
            roles = {row["IP_ADDR"]: row for row in pq.read_table(dataset.roles_path).to_pylist()}
            self.assertEqual(set(roles), {"1.1.1.1", "8.8.8.8", "9.9.9.9"})
            self.assertTrue(roles["8.8.8.8"]["T"])
            self.assertTrue(roles["8.8.8.8"]["D"])
            self.assertFalse(roles["1.1.1.1"]["T"])
            self.assertTrue(roles["1.1.1.1"]["D"])

            processed = root / "processed"
            figures = root / "figures"
            measurement = IpidMeasurement(
                protocol="icmp",
                connection_mode="no-connection",
                interval="rt-based",
                scale="base",
                measurement_id="icmp_2026-01-29_12-00-00",
                zmap_id="icmp_2026-01-29_10-00-00",
            )
            strategies = measurement.artifact_path(processed, "strategies")
            strategies.parent.mkdir(parents=True)
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["8.8.8.8", "1.1.1.1", "9.9.9.9", "4.4.4.4"],
                        "IPID_SELECTION_STRATEGY": [
                            "SINGLE",
                            "RANDOM",
                            "PER_BUCKET",
                            "CONSTANT",
                        ],
                    }
                ),
                strategies,
            )

            itdk_directory = root / "itdk"
            itdk_directory.mkdir()
            interfaces = itdk_directory / "interfaces.pq"
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["8.8.8.8", "1.1.1.1", "9.9.9.9"],
                        "NODE_ID": ["N1", "N2", "N3"],
                        "T": [True, False, False],
                        "D": [False, True, True],
                    }
                ),
                interfaces,
            )
            itdk = ITDKDataset(
                release="test",
                topology="test",
                directory=itdk_directory,
                ifaces_path=itdk_directory / "test.ifaces.bz2",
                interfaces_path=interfaces,
                metadata_path=itdk_directory / "source.json",
                readme_path=itdk_directory / "README.txt",
            )

            with patch("ipid_analysis.plot_ripe_atlas_strategy.configure_paper_style"):
                outputs = render_ripe_atlas_analysis(
                    measurement,
                    dataset,
                    itdk=itdk,
                    processed_root=processed,
                    figures_root=figures,
                )

            self.assertTrue(outputs.joined.is_file())
            self.assertTrue(outputs.distribution.is_file())
            self.assertTrue(outputs.role_pdf.is_file())
            self.assertTrue(outputs.role_json.is_file())
            self.assertIsNotNone(outputs.caida_agreement)
            self.assertTrue(outputs.caida_agreement.is_file())
            report = json.loads(outputs.role_json.read_text())
            self.assertEqual(report["coverage"]["total"], 4)
            self.assertEqual(report["coverage"]["matched"], 3)
            self.assertEqual(report["coverage"]["unmatched"], 1)
            self.assertEqual(report["coverage"]["t1_d1"], 1)
            self.assertEqual(report["coverage"]["t1_d0"], 1)
            self.assertEqual(report["coverage"]["t0_d1"], 1)
            self.assertEqual(report["plot_percentages"]["Destination-Only"]["RANDOM"], 100.0)
            agreement = {row["category"]: row["count"] for row in report["caida_ripe_agreement"]}
            self.assertEqual(agreement["Both Transit-Observed"], 1)
            self.assertEqual(agreement["RIPE Only Transit-Observed"], 1)
            self.assertEqual(agreement["Neither Transit-Observed"], 1)

    def test_prepare_local_input_is_copied_and_cached(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_dir = root / "input"
            source_dir.mkdir()
            timestamp = int(datetime(2026, 1, 15, 12, tzinfo=timezone.utc).timestamp())
            with bz2.open(source_dir / "traceroute.bz2", "wt", encoding="utf-8") as output:
                output.write(
                    json.dumps(
                        _record(
                            timestamp,
                            "1.1.1.1",
                            ["8.8.8.8", "1.1.1.1"],
                            measurement=10,
                            probe=100,
                        )
                    )
                    + "\n"
                )
            arguments = {
                "campaign_start": datetime(2026, 1, 29, 12, tzinfo=timezone.utc),
                "input_dir": source_dir,
                "raw_root": root / "raw",
                "processed_root": root / "processed",
            }
            first = prepare_ripe_atlas(**arguments)
            second = prepare_ripe_atlas(**arguments)
            self.assertEqual(first, second)
            self.assertTrue(first.roles_path.is_file())
            metadata = json.loads(first.metadata_path.read_text())
            self.assertEqual(metadata["source_kind"], "local-files")
            self.assertEqual(metadata["lookback_days"], 28)
            self.assertEqual(metadata["addresses"], 2)
            copied = Path(metadata["files"][0]["path"])
            self.assertTrue(copied.is_file())
            self.assertIn("ripe-atlas", copied.parts)


if __name__ == "__main__":
    unittest.main()
