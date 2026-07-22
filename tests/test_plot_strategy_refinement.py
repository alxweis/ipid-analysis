from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.plot_strategy_refinement import (
    CONNECTION_MODE,
    KIND,
    KIND_WITH_CONNECTION,
    render,
    render_with_connection,
)
from ipid_analysis.strategy_merge import StrategyMerge


class StrategyRefinementPlotTest(unittest.TestCase):
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
        self.connection = IpidMeasurement(
            protocol="tcp",
            connection_mode="connection",
            interval="rt-based",
            scale="base",
            measurement_id="tcp-connection-base",
            zmap_id="tcp-zmap",
        )
        self.merge = StrategyMerge(self.base, self.mass)

    @staticmethod
    def _write_strategies(path: Path, addresses: list[str], strategies: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "IP_ADDR": addresses,
                    "IPID_SELECTION_STRATEGY": strategies,
                }
            ),
            path,
        )

    @staticmethod
    def _write_zmap(path: Path, addresses: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"IP_ADDR": addresses}), path)

    def test_render_refinement_plot_and_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed = root / "processed"
            raw = root / "raw"
            figures = root / "figures"
            base_path = self.base.artifact_path(processed, "strategies")
            mass_path = self.mass.artifact_path(processed, "strategies")
            self._write_strategies(
                base_path,
                ["192.0.2.1", "192.0.2.2", "192.0.2.3", "192.0.2.4", "192.0.2.5"],
                ["REFLECTION", "SINGLE", "UNCLASSIFIED", "UNCLASSIFIED", "UNCLASSIFIED"],
            )
            self._write_strategies(
                mass_path,
                ["192.0.2.3", "192.0.2.4"],
                ["MULTI", "RANDOM"],
            )
            self._write_zmap(
                raw / "zmap" / self.merge.zmap_id / "zmap.pq",
                [f"192.0.2.{index}" for index in range(1, 7)],
            )

            pdf_path, json_path, aggregate_path = render(
                self.merge,
                processed_root=processed,
                figures_root=figures,
                raw_root=raw,
            )

            self.assertTrue(pdf_path.is_file())
            self.assertTrue(json_path.is_file())
            self.assertTrue(aggregate_path.is_file())
            self.assertEqual(
                aggregate_path,
                self.merge.artifact_path(processed, KIND),
            )

            aggregate = pq.read_table(aggregate_path).to_pylist()
            shares = {
                (row["MEASUREMENT_TYPE"], row["IPID_SELECTION_STRATEGY"]): row["PERCENTAGE"]
                for row in aggregate
            }
            self.assertEqual(shares[("RT-based", "REFLECTION")], 20.0)
            self.assertEqual(shares[("RT-based", "SINGLE")], 20.0)
            self.assertEqual(shares[("RT-based", "UNCLASSIFIED")], 60.0)
            self.assertAlmostEqual(shares[("Fixed-Interval", "MULTI")], 100 / 3)
            self.assertAlmostEqual(shares[("Fixed-Interval", "RANDOM")], 100 / 3)
            self.assertAlmostEqual(shares[("Fixed-Interval", "NOT_ENOUGH_SAMPLES")], 100 / 3)
            self.assertAlmostEqual(
                sum(
                    row["PERCENTAGE"]
                    for row in aggregate
                    if row["MEASUREMENT_TYPE"] == "Fixed-Interval"
                ),
                100.0,
            )

            metadata = json.loads(json_path.read_text())
            self.assertEqual(metadata["rt_based_unclassified_ip_count"], 3)
            self.assertEqual(metadata["fixed_interval_target_ip_count"], 3)
            self.assertEqual(metadata["fixed_interval_result_ip_count"], 2)
            self.assertEqual(metadata["fixed_interval_missing_result_ip_count"], 1)
            self.assertEqual(metadata["not_enough_samples_count"], 1)
            self.assertAlmostEqual(metadata["fixed_interval_result_coverage_percent"], 200 / 3)
            self.assertAlmostEqual(metadata["ipid_measurement_coverage"], 500 / 6)

    def test_rejects_fixed_interval_address_that_was_not_unclassified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed = root / "processed"
            figures = root / "figures"
            self._write_strategies(
                self.base.artifact_path(processed, "strategies"),
                ["192.0.2.1", "192.0.2.2"],
                ["SINGLE", "UNCLASSIFIED"],
            )
            self._write_strategies(
                self.mass.artifact_path(processed, "strategies"),
                ["192.0.2.1"],
                ["RANDOM"],
            )

            with self.assertRaisesRegex(ValueError, "were not UNCLASSIFIED"):
                render(
                    self.merge,
                    processed_root=processed,
                    figures_root=figures,
                )

    def test_render_refinement_plot_with_connection_oriented_bar(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed = root / "processed"
            raw = root / "raw"
            figures = root / "figures"
            self._write_strategies(
                self.base.artifact_path(processed, "strategies"),
                ["192.0.2.1", "192.0.2.2", "192.0.2.3"],
                ["CONSTANT", "UNCLASSIFIED", "UNCLASSIFIED"],
            )
            self._write_strategies(
                self.mass.artifact_path(processed, "strategies"),
                ["192.0.2.2"],
                ["MULTI"],
            )
            self._write_strategies(
                self.connection.artifact_path(processed, "strategies"),
                ["192.0.2.1", "192.0.2.2", "192.0.2.3", "192.0.2.4"],
                ["PER_CONNECTION", "PER_CONNECTION", "SINGLE", "UNCLASSIFIED"],
            )
            self._write_zmap(
                raw / "zmap" / self.merge.zmap_id / "zmap.pq",
                [f"192.0.2.{index}" for index in range(1, 5)],
            )

            pdf_path, json_path, aggregate_path = render_with_connection(
                self.merge,
                self.connection,
                processed_root=processed,
                figures_root=figures,
                raw_root=raw,
            )

            self.assertTrue(pdf_path.is_file())
            self.assertTrue(json_path.is_file())
            self.assertEqual(
                aggregate_path,
                self.merge.artifact_path(processed, KIND_WITH_CONNECTION),
            )
            shares = {
                (row["MEASUREMENT_TYPE"], row["IPID_SELECTION_STRATEGY"]): row["PERCENTAGE"]
                for row in pq.read_table(aggregate_path).to_pylist()
            }
            self.assertEqual(shares[(CONNECTION_MODE, "PER_CONNECTION")], 50.0)
            self.assertEqual(shares[(CONNECTION_MODE, "SINGLE")], 25.0)
            self.assertEqual(shares[(CONNECTION_MODE, "UNCLASSIFIED")], 25.0)
            self.assertEqual(shares[("Fixed-Interval", "MULTI")], 50.0)
            self.assertEqual(shares[("Fixed-Interval", "NOT_ENOUGH_SAMPLES")], 50.0)

            metadata = json.loads(json_path.read_text())
            self.assertEqual(metadata["connection_oriented_ip_count"], 4)
            self.assertEqual(metadata["not_enough_samples_count"], 1)
            self.assertEqual(
                metadata["measurements"]["rt_based_connection_oriented"],
                "tcp-connection-base",
            )

    def test_rejects_wrong_connection_oriented_target(self):
        invalid = replace(self.connection, interval="fixed-interval")
        with self.assertRaisesRegex(ValueError, "tcp.ipid.connection.rt-based.base"):
            render_with_connection(self.merge, invalid)

    def test_requires_rt_base_followed_by_fixed_interval_mass(self):
        invalid = StrategyMerge(
            replace(self.base, interval="fixed-interval"),
            replace(self.mass, interval="rt-based"),
        )
        with self.assertRaisesRegex(ValueError, "requires an RT-based base target"):
            render(invalid)


if __name__ == "__main__":
    unittest.main()
