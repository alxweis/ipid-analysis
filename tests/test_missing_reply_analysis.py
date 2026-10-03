import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.missing_reply_analysis import (
    OUTPUT_STEM,
    analyze_missing_replies,
)


class MissingReplyAnalysisTest(unittest.TestCase):
    @staticmethod
    def _sequence(missing: int, *, length: int = 100) -> str:
        values = [str(index) for index in range(length)]
        for index in range(missing):
            values[index] = "-"
        return ",".join(values)

    @staticmethod
    def _write_measurement(root: Path, measurement_id: str, missing: list[int]) -> None:
        directory = root / "raw" / "ipid" / measurement_id
        directory.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "IP_ADDR": [f"192.0.2.{index + 1}" for index in range(len(missing))],
                    "IPID_SEQUENCE": [
                        MissingReplyAnalysisTest._sequence(count) for count in missing
                    ],
                }
            ),
            directory / "ipid.pq",
        )
        (directory / "ipid.snapshot.yaml").write_text(
            "connection_count: 4\n"
            "requests_per_connection: 25\n"
            "request_ip_ids: [1, 2, 3, 4]\n"
            "fixed_interval:\n"
            "  minimum_reply_rate: 0.8\n"
        )

    @staticmethod
    def _write_manifest(path: Path) -> None:
        path.write_text(
            json.dumps(
                {
                    "tcp": {
                        "zmap": "tcp-zmap",
                        "ipid": {
                            "no-connection": {
                                "rt-based": {"base": "tcp-base"},
                                "fixed-interval": {"mass": "tcp-mass"},
                            }
                        },
                    },
                    "icmp": {
                        "zmap": "icmp-zmap",
                        "ipid": {
                            "no-connection": {
                                "fixed-interval": {"mass": "icmp-mass"},
                            }
                        },
                    },
                }
            )
        )

    def test_analysis_writes_complete_distribution_and_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            self._write_manifest(manifest)
            self._write_measurement(root, "tcp-mass", [0, 0, 1, 20])
            self._write_measurement(root, "icmp-mass", [0, 2])

            with patch("ipid_analysis.missing_reply_analysis.configure_paper_style"):
                outputs = analyze_missing_replies(
                    manifest,
                    raw_root=root / "raw",
                    processed_root=root / "processed",
                    figures_root=root / "figures",
                )

            self.assertEqual(outputs["aggregate"].name, f"{OUTPUT_STEM}.pq")
            self.assertTrue(outputs["aggregate"].is_file())
            self.assertTrue(outputs["json"].is_file())
            self.assertTrue(outputs["pdf"].is_file())

            rows = pq.read_table(outputs["aggregate"]).to_pylist()
            self.assertEqual(len(rows), 42)
            tcp = {row["MISSING_REPLY_COUNT"]: row for row in rows if row["PROTOCOL"] == "tcp"}
            self.assertEqual(tcp[0]["SEQUENCE_COUNT"], 2)
            self.assertEqual(tcp[1]["SEQUENCE_COUNT"], 1)
            self.assertEqual(tcp[20]["SEQUENCE_COUNT"], 1)
            self.assertEqual(tcp[0]["PERCENTAGE"], 50.0)
            self.assertEqual(tcp[20]["CUMULATIVE_PERCENTAGE"], 100.0)
            self.assertEqual(sum(row["PERCENTAGE"] for row in tcp.values()), 100.0)

            metadata = json.loads(outputs["json"].read_text())
            self.assertEqual(metadata["schema_version"], 1)
            self.assertEqual(len(metadata["measurements"]), 2)
            tcp_metadata = next(
                item for item in metadata["measurements"] if item["protocol"] == "tcp"
            )
            self.assertEqual(tcp_metadata["configured_maximum_missing_replies"], 20)
            self.assertEqual(tcp_metadata["persisted_sequence_count"], 4)
            self.assertEqual(tcp_metadata["complete_sequence_count"], 2)
            self.assertEqual(tcp_metadata["maximum_observed_missing_replies"], 20)
            self.assertEqual(tcp_metadata["sequences_below_configured_minimum_count"], 0)
            self.assertEqual(
                metadata["semantics"]["population"],
                "persisted fixed-interval Mass sequences only",
            )

    def test_rejects_sequence_with_wrong_number_of_fixed_positions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "tcp": {
                            "zmap": "tcp-zmap",
                            "ipid": {
                                "no-connection": {
                                    "fixed-interval": {"mass": "tcp-mass"},
                                }
                            },
                        }
                    }
                )
            )
            self._write_measurement(root, "tcp-mass", [0])
            input_path = root / "raw" / "ipid" / "tcp-mass" / "ipid.pq"
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["192.0.2.1"],
                        "IPID_SEQUENCE": [self._sequence(0, length=99)],
                    }
                ),
                input_path,
            )

            with self.assertRaisesRegex(ValueError, "expected 100 fixed positions"):
                analyze_missing_replies(
                    manifest,
                    raw_root=root / "raw",
                    processed_root=root / "processed",
                    figures_root=root / "figures",
                )

    def test_requires_fixed_interval_mass_measurement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "tcp": {
                            "zmap": "tcp-zmap",
                            "ipid": {
                                "no-connection": {
                                    "rt-based": {"base": "tcp-base"},
                                }
                            },
                        }
                    }
                )
            )

            with self.assertRaisesRegex(ValueError, "no fixed-interval Mass measurement"):
                analyze_missing_replies(manifest)


if __name__ == "__main__":
    unittest.main()
