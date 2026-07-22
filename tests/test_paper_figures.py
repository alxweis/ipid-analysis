import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.comparison import BaseComparison, iter_base_comparisons
from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.paper_figures import (
    INTERSECTION_KIND,
    aggregate_increment_distributions,
    aggregate_strategy_intersection,
    render_increment_comparison,
    render_probing_interval_comparison,
    render_strategy_intersection,
)


class PaperFiguresTest(unittest.TestCase):
    def setUp(self):
        self.rt = IpidMeasurement(
            protocol="udp-dns",
            connection_mode="connection",
            interval="rt-based",
            scale="base",
            measurement_id="udp-rt",
            zmap_id="udp-zmap",
        )
        self.fixed = IpidMeasurement(
            protocol="udp-dns",
            connection_mode="connection",
            interval="fixed-interval",
            scale="base",
            measurement_id="udp-fixed",
            zmap_id="udp-zmap",
        )
        self.comparison = BaseComparison(self.rt, self.fixed)

    def _write_inputs(self, processed: Path, raw: Path) -> None:
        intervals = {
            "IP_ADDR": ["192.0.2.1", "192.0.2.2", "198.51.100.1", "203.0.113.1"],
            "PROBING_INTERVALS": [
                [100_000, 120_000, 110_000],
                [140_000, 160_000, 150_000],
                [20_000, 30_000, 40_000],
                [50_000, 60_000, 70_000],
            ],
        }
        fixed_intervals = {
            "IP_ADDR": ["192.0.2.1", "192.0.2.2", "198.51.100.1", "203.0.113.1"],
            "PROBING_INTERVALS": [
                [20_000, 20_000, 20_000],
                [21_000, 20_000, 19_000],
                [20_000, 21_000, 20_000],
                [20_000, 20_000, 20_000],
            ],
        }
        increment_rows = {
            "IP_ADDR": ["192.0.2.1", "192.0.2.2", "198.51.100.1", "203.0.113.1"],
            "IPID_SELECTION_STRATEGY": [
                "SINGLE",
                "PER_DESTINATION",
                "PER_CONNECTION",
                "PER_BUCKET",
            ],
            "INCREMENTS": [
                [1] * 999 + [2, 10_000],
                [1, 2, 4, 8],
                [1, 3, 9, 27],
                [2, 4, 8, 16],
            ],
        }
        fixed_increment_rows = {
            **increment_rows,
            "INCREMENTS": [
                [1] * 999 + [3, 20_000],
                [1, 5, 10, 20],
                [1, 4, 16, 64],
                [2, 6, 18, 54],
            ],
        }
        strategy_rows = {
            "IP_ADDR": ["192.0.2.1", "192.0.2.2", "198.51.100.1", "203.0.113.1"],
            "IPID_SELECTION_STRATEGY": ["SINGLE", "SINGLE", "CONSTANT", "UNCLASSIFIED"],
        }
        fixed_strategy_rows = {
            "IP_ADDR": ["192.0.2.1", "192.0.2.2", "198.51.100.1", "203.0.113.1"],
            "IPID_SELECTION_STRATEGY": [
                "SINGLE",
                "PER_BUCKET",
                "CONSTANT",
                "UNCLASSIFIED",
            ],
        }
        for measurement, interval_data, increment_data, strategy_data in (
            (self.rt, intervals, increment_rows, strategy_rows),
            (self.fixed, fixed_intervals, fixed_increment_rows, fixed_strategy_rows),
        ):
            for kind, data in (
                ("probing-intervals", interval_data),
                ("increments", increment_data),
                ("strategies", strategy_data),
            ):
                path = measurement.artifact_path(processed, kind)
                path.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(pa.table(data), path)
        zmap_path = raw / "zmap" / self.comparison.zmap_id / "zmap.pq"
        zmap_path.parent.mkdir(parents=True)
        pq.write_table(
            pa.table({"IP_ADDR": strategy_rows["IP_ADDR"] + ["203.0.113.2"]}), zmap_path
        )

    def test_comparison_path_and_manifest_iteration(self):
        self.assertEqual(
            self.comparison.artifact_path(Path("reports/figures"), INTERSECTION_KIND, "pdf"),
            Path("reports/figures")
            / "udp-zmap"
            / "connection"
            / "comparison"
            / "rt-based-base_fixed-interval-base"
            / "c-rt-b_fi-b_strategy-intersection.pdf",
        )
        manifest = {
            "udp-dns": {
                "zmap": "udp-zmap",
                "ipid": {
                    "no-connection": {
                        "rt-based": {"base": "udp-n-rt"},
                        "fixed-interval": {"base": "udp-n-fi"},
                    },
                    "connection": {
                        "rt-based": {"base": "udp-rt"},
                        "fixed-interval": {"base": "udp-fixed"},
                    },
                },
            }
        }
        self.assertEqual(
            [comparison.connection_mode for comparison in iter_base_comparisons(manifest)],
            ["no-connection", "connection"],
        )

    def test_all_aggregates_and_renderers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed = root / "processed"
            raw = root / "raw"
            figures = root / "figures"
            self._write_inputs(processed, raw)

            def lookup(address: str):
                if address.startswith("192.0.2."):
                    return "NA", "North America"
                if address.startswith("198.51.100."):
                    return "EU", "Europe"
                return None

            interval_pdf, interval_json, interval_aggregate = render_probing_interval_comparison(
                self.comparison,
                continent_lookup=lookup,
                processed_root=processed,
                figures_root=figures,
                raw_root=raw,
            )
            increment_pdf, increment_json, increment_aggregate = render_increment_comparison(
                self.comparison,
                processed_root=processed,
                figures_root=figures,
                raw_root=raw,
            )
            intersection_pdf, intersection_json, intersection_aggregate = (
                render_strategy_intersection(
                    self.comparison,
                    processed_root=processed,
                    figures_root=figures,
                    raw_root=raw,
                )
            )

            for path in (
                interval_pdf,
                interval_json,
                interval_aggregate,
                increment_pdf,
                increment_json,
                increment_aggregate,
                intersection_pdf,
                intersection_json,
                intersection_aggregate,
            ):
                self.assertTrue(path.is_file(), path)

            interval_report = json.loads(interval_json.read_text())
            self.assertEqual(interval_report["mapped_ip_count"], 3)
            self.assertEqual(interval_report["unmapped_ip_count"], 1)
            self.assertEqual(
                interval_report["methodology"]["per_ip_statistic"],
                "median of consecutive probing intervals",
            )
            expected_coverage = {"rt_based": 80.0, "fixed_interval": 80.0}
            for report_path in (interval_json, increment_json, intersection_json):
                self.assertEqual(
                    json.loads(report_path.read_text())["ipid_measurement_coverage"],
                    expected_coverage,
                )

            increment_table = pq.read_table(increment_aggregate)
            single_rt = increment_table.filter(
                pa.compute.and_(
                    pa.compute.equal(increment_table["MODE"], "RT-based"),
                    pa.compute.equal(increment_table["IPID_SELECTION_STRATEGY"], "SINGLE"),
                )
            )
            self.assertEqual(single_rt["INCREMENT"].to_pylist(), [1, 2])
            self.assertEqual(single_rt["CLIPPED_COUNT"].to_pylist(), [1, 1])
            self.assertAlmostEqual(single_rt["CUMULATIVE_PERCENTAGE"][-1].as_py(), 100.0)

            matrix = pq.read_table(intersection_aggregate).to_pylist()
            values = {
                (row["RT_BASED_STRATEGY"], row["FIXED_INTERVAL_STRATEGY"]): row["PERCENTAGE"]
                for row in matrix
            }
            self.assertEqual(values[("SINGLE", "SINGLE")], 50.0)
            self.assertEqual(values[("SINGLE", "PER_BUCKET")], 50.0)
            self.assertEqual(values[("CONSTANT", "CONSTANT")], 100.0)
            self.assertEqual(json.loads(intersection_json.read_text())["included_ip_count"], 4)

    def test_low_level_aggregators_require_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(FileNotFoundError):
                aggregate_increment_distributions(root / "a", root / "b", root / "out")
            with self.assertRaises(FileNotFoundError):
                aggregate_strategy_intersection(root / "a", root / "b", root / "out")

    def test_only_base_measurements_are_comparable(self):
        mass = IpidMeasurement(
            protocol="udp-dns",
            connection_mode="connection",
            interval="fixed-interval",
            scale="mass",
            measurement_id="udp-mass",
            zmap_id="udp-zmap",
        )
        with self.assertRaises(ValueError):
            BaseComparison(self.rt, mass)


if __name__ == "__main__":
    unittest.main()
