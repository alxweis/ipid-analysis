from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.plot_tcp_flags_strategy import (
    ALL_FLAGS,
    KIND,
    RST_FLAGS,
    SYNACK_FLAGS,
    render,
)
from ipid_analysis.strategy_merge import StrategyMerge


class TCPFlagsStrategyPlotTest(unittest.TestCase):
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

    def test_render_tcp_flags_plot_from_merged_strategies(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed = root / "processed"
            raw = root / "raw"
            figures = root / "figures"
            addresses = ["192.0.2.1", "192.0.2.2", "192.0.2.3", "192.0.2.4"]
            self._write(
                self.merge.artifact_path(processed, "strategies"),
                {
                    "IP_ADDR": addresses,
                    "IPID_SELECTION_STRATEGY": [
                        "CONSTANT",
                        "SINGLE",
                        "MULTI",
                        "UNCLASSIFIED",
                    ],
                },
            )
            self._write(
                raw / "zmap" / self.merge.zmap_id / "zmap.pq",
                {
                    "IP_ADDR": addresses,
                    "REPLY_TYPE": ["synack", "SYNACK", "rst", "RST"],
                },
            )

            pdf_path, json_path, aggregate_path = render(
                self.merge,
                processed_root=processed,
                raw_root=raw,
                figures_root=figures,
            )

            self.assertTrue(pdf_path.is_file())
            self.assertTrue(json_path.is_file())
            self.assertEqual(aggregate_path, self.merge.artifact_path(processed, KIND))
            shares = {
                (row["TCP_FLAGS"], row["IPID_SELECTION_STRATEGY"]): row["PERCENTAGE"]
                for row in pq.read_table(aggregate_path).to_pylist()
            }
            self.assertEqual(shares[(ALL_FLAGS, "CONSTANT")], 25.0)
            self.assertEqual(shares[(SYNACK_FLAGS, "CONSTANT")], 50.0)
            self.assertEqual(shares[(SYNACK_FLAGS, "SINGLE")], 50.0)
            self.assertEqual(shares[(RST_FLAGS, "MULTI")], 50.0)
            self.assertEqual(shares[(RST_FLAGS, "UNCLASSIFIED")], 50.0)

            metadata = json.loads(json_path.read_text())
            self.assertEqual(metadata["matched_ip_count"], 4)
            self.assertEqual(metadata["synack_ip_count"], 2)
            self.assertEqual(metadata["rst_ip_count"], 2)
            self.assertEqual(metadata["ipid_measurement_coverage"], 100.0)
            self.assertEqual(
                metadata["sources"]["merged_strategies"],
                str(self.merge.artifact_path(processed, "strategies")),
            )

    def test_rejects_unsupported_zmap_reply_classification(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed = root / "processed"
            raw = root / "raw"
            addresses = ["192.0.2.1", "192.0.2.2", "192.0.2.3"]
            self._write(
                self.merge.artifact_path(processed, "strategies"),
                {
                    "IP_ADDR": addresses,
                    "IPID_SELECTION_STRATEGY": ["CONSTANT", "MULTI", "SINGLE"],
                },
            )
            self._write(
                raw / "zmap" / self.merge.zmap_id / "zmap.pq",
                {
                    "IP_ADDR": addresses,
                    "REPLY_TYPE": ["synack", "rst", "other"],
                },
            )

            with self.assertRaisesRegex(ValueError, "do not have a synack or rst"):
                render(
                    self.merge,
                    processed_root=processed,
                    raw_root=raw,
                    figures_root=root / "figures",
                )

    def test_rejects_merged_ip_missing_from_zmap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed = root / "processed"
            raw = root / "raw"
            self._write(
                self.merge.artifact_path(processed, "strategies"),
                {
                    "IP_ADDR": ["192.0.2.1", "192.0.2.2", "192.0.2.3"],
                    "IPID_SELECTION_STRATEGY": ["CONSTANT", "MULTI", "SINGLE"],
                },
            )
            self._write(
                raw / "zmap" / self.merge.zmap_id / "zmap.pq",
                {
                    "IP_ADDR": ["192.0.2.1", "192.0.2.2"],
                    "REPLY_TYPE": ["synack", "rst"],
                },
            )

            with self.assertRaisesRegex(ValueError, "missing from ZMap"):
                render(
                    self.merge,
                    processed_root=processed,
                    raw_root=raw,
                    figures_root=root / "figures",
                )

    def test_requires_tcp_rt_based_merge(self):
        invalid = StrategyMerge(
            replace(self.base, protocol="udp-dns"),
            replace(self.mass, protocol="udp-dns"),
        )
        with self.assertRaisesRegex(ValueError, "TCP flags plot requires"):
            render(invalid)


if __name__ == "__main__":
    unittest.main()
