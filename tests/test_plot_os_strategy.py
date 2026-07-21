from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.plot_os_strategy import (
    GENERAL_PURPOSE_GROUP,
    KIND,
    NETWORK_GROUP,
    render,
    resolve_os_measurement_id,
)
from ipid_analysis.strategy_merge import StrategyMerge


class OSStrategyPlotTest(unittest.TestCase):
    def setUp(self):
        self.base = IpidMeasurement(
            protocol="tcp",
            connection_mode="no-connection",
            interval="rt-based",
            scale="base",
            measurement_id="tcp-base",
            zmap_id="tcp-zmap",
        )
        self.mass = IpidMeasurement(
            protocol="tcp",
            connection_mode="no-connection",
            interval="fixed-interval",
            scale="mass",
            measurement_id="tcp-mass",
            zmap_id="tcp-zmap",
        )
        self.merge = StrategyMerge(self.base, self.mass)

    @staticmethod
    def _write(path: Path, columns: dict[str, list[str]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table(columns), path)

    def test_render_os_strategy_heatmap_from_merged_strategies(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed = root / "processed"
            raw = root / "raw"
            figures = root / "figures"
            merged_addresses = [
                "192.0.2.1",
                "192.0.2.2",
                "192.0.2.3",
                "192.0.2.4",
                "192.0.2.5",
                "192.0.2.6",
            ]
            self._write(
                self.merge.artifact_path(processed, "strategies"),
                {
                    "IP_ADDR": merged_addresses,
                    "IPID_SELECTION_STRATEGY": [
                        "CONSTANT",
                        "CONSTANT",
                        "SINGLE",
                        "REFLECTION",
                        "RANDOM",
                        "NOT_ENOUGH_SAMPLES",
                    ],
                },
            )
            self._write(
                raw / "os" / "tcp-os" / "os.pq",
                {
                    "IP_ADDR": merged_addresses + ["198.51.100.1"],
                    "OS_NAME": [
                        "ubuntu",
                        "Ubuntu",
                        "ubuntu",
                        "cisco-ios",
                        "CISCO-IOS",
                        "ubuntu",
                        "debian",
                    ],
                },
            )

            pdf_path, json_path, aggregate_path = render(
                self.merge,
                "tcp-os",
                processed_root=processed,
                raw_root=raw,
                figures_root=figures,
            )

            self.assertTrue(pdf_path.is_file())
            self.assertTrue(json_path.is_file())
            self.assertEqual(aggregate_path, self.merge.artifact_path(processed, KIND))
            rows = pq.read_table(aggregate_path).to_pylist()
            shares = {
                (row["OS_NAME"], row["IPID_SELECTION_STRATEGY"]): row["PERCENTAGE"] for row in rows
            }
            self.assertEqual(shares[("ubuntu", "CONSTANT")], 50.0)
            self.assertEqual(shares[("ubuntu", "SINGLE")], 25.0)
            self.assertEqual(shares[("ubuntu", "NOT_ENOUGH_SAMPLES")], 25.0)
            self.assertEqual(shares[("cisco-ios", "REFLECTION")], 50.0)
            self.assertEqual(shares[("cisco-ios", "RANDOM")], 50.0)
            groups = {row["OS_NAME"]: row["OS_GROUP"] for row in rows}
            self.assertEqual(groups["ubuntu"], GENERAL_PURPOSE_GROUP)
            self.assertEqual(groups["cisco-ios"], NETWORK_GROUP)
            self.assertEqual(len(rows), 2 * 10)
            self.assertEqual(shares[("ubuntu", "PER_CONNECTION")], 0.0)
            self.assertEqual(
                sum(row["PERCENTAGE"] for row in rows if row["OS_NAME"] == "ubuntu"),
                100.0,
            )

            metadata = json.loads(json_path.read_text())
            self.assertEqual(metadata["os_ip_count"], 7)
            self.assertEqual(metadata["matched_ip_count"], 6)
            self.assertEqual(metadata["included_ip_count"], 6)
            self.assertEqual(metadata["not_enough_samples_ip_count"], 1)
            self.assertNotIn("excluded_not_enough_samples_ip_count", metadata)
            self.assertEqual(metadata["unmatched_os_ip_count"], 1)
            self.assertEqual(metadata["os_measurement_id"], "tcp-os")
            self.assertEqual(
                metadata["methodology"]["normalization"],
                "each operating-system row is normalized independently to 100%",
            )

    def test_rejects_unknown_os_name(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed = root / "processed"
            raw = root / "raw"
            addresses = ["192.0.2.1", "192.0.2.2"]
            self._write(
                self.merge.artifact_path(processed, "strategies"),
                {
                    "IP_ADDR": addresses,
                    "IPID_SELECTION_STRATEGY": ["CONSTANT", "RANDOM"],
                },
            )
            self._write(
                raw / "os" / "tcp-os" / "os.pq",
                {"IP_ADDR": addresses, "OS_NAME": ["ubuntu", "new-router-os"]},
            )

            with self.assertRaisesRegex(ValueError, "unknown OS_NAME"):
                render(
                    self.merge,
                    "tcp-os",
                    processed_root=processed,
                    raw_root=raw,
                    figures_root=root / "figures",
                )

    def test_rejects_duplicate_os_addresses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed = root / "processed"
            raw = root / "raw"
            self._write(
                self.merge.artifact_path(processed, "strategies"),
                {
                    "IP_ADDR": ["192.0.2.1", "192.0.2.2"],
                    "IPID_SELECTION_STRATEGY": ["CONSTANT", "RANDOM"],
                },
            )
            self._write(
                raw / "os" / "tcp-os" / "os.pq",
                {
                    "IP_ADDR": ["192.0.2.1", "192.0.2.1", "192.0.2.2"],
                    "OS_NAME": ["ubuntu", "ubuntu", "cisco-ios"],
                },
            )

            with self.assertRaisesRegex(ValueError, "duplicate IP addresses in OS result"):
                render(
                    self.merge,
                    "tcp-os",
                    processed_root=processed,
                    raw_root=raw,
                    figures_root=root / "figures",
                )

    def test_supports_icmp_and_udp_dns_merges(self):
        for protocol in ("icmp", "udp-dns"):
            with self.subTest(protocol=protocol), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                processed = root / "processed"
                raw = root / "raw"
                merge = StrategyMerge(
                    replace(
                        self.base,
                        protocol=protocol,
                        measurement_id=f"{protocol}-base",
                        zmap_id=f"{protocol}-zmap",
                    ),
                    replace(
                        self.mass,
                        protocol=protocol,
                        measurement_id=f"{protocol}-mass",
                        zmap_id=f"{protocol}-zmap",
                    ),
                )
                addresses = ["192.0.2.1", "192.0.2.2"]
                self._write(
                    merge.artifact_path(processed, "strategies"),
                    {
                        "IP_ADDR": addresses,
                        "IPID_SELECTION_STRATEGY": ["CONSTANT", "RANDOM"],
                    },
                )
                self._write(
                    raw / "os" / f"{protocol}-os" / "os.pq",
                    {"IP_ADDR": addresses, "OS_NAME": ["ubuntu", "cisco-ios"]},
                )

                pdf_path, _, aggregate_path = render(
                    merge,
                    f"{protocol}-os",
                    processed_root=processed,
                    raw_root=raw,
                    figures_root=root / "figures",
                )

                self.assertTrue(pdf_path.is_file())
                self.assertTrue(aggregate_path.is_file())

    def test_requires_no_connection_rt_based_merge(self):
        invalid = StrategyMerge(
            replace(self.base, connection_mode="connection"),
            replace(self.mass, connection_mode="connection"),
        )
        with self.assertRaisesRegex(ValueError, "OS strategy heatmap requires"):
            render(invalid, "tcp-os")

    def test_resolves_os_measurement_id(self):
        manifest = {
            "icmp": {"os": "icmp-os"},
            "tcp": {"os": "tcp-os"},
            "udp-dns": {"os": "udp-os"},
        }
        self.assertEqual(resolve_os_measurement_id(manifest, "icmp"), "icmp-os")
        self.assertEqual(resolve_os_measurement_id(manifest, "tcp"), "tcp-os")
        self.assertEqual(resolve_os_measurement_id(manifest, "udp-dns"), "udp-os")
        self.assertIsNone(resolve_os_measurement_id({"tcp": {}}, "tcp"))
        with self.assertRaisesRegex(ValueError, "non-empty measurement id"):
            resolve_os_measurement_id({"tcp": {"os": ""}}, "tcp")


if __name__ == "__main__":
    unittest.main()
