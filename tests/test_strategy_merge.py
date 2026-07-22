from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.plot_strategies import render_merged
from ipid_analysis.strategy_merge import (
    StrategyMerge,
    iter_strategy_merges,
    merge_strategies,
)


class StrategyMergeTest(unittest.TestCase):
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

    def test_artifact_path_uses_merged_variant_schema(self):
        self.assertEqual(
            self.merge.artifact_path(Path("data/processed"), "strategies"),
            Path("data/processed")
            / "tcp-zmap"
            / "no-connection"
            / "merged"
            / "rt-based-base_fixed-interval-mass"
            / "n-rt-b_fi-m_strategies.pq",
        )

    def test_merge_rules_and_plot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed = root / "processed"
            raw = root / "raw"
            figures = root / "figures"
            base_path = self.base.artifact_path(processed, "strategies")
            mass_path = self.mass.artifact_path(processed, "strategies")
            base_path.parent.mkdir(parents=True)
            mass_path.parent.mkdir(parents=True)

            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["192.0.2.1", "192.0.2.2", "192.0.2.3", "192.0.2.4"],
                        "IPID_SELECTION_STRATEGY": [
                            "CONSTANT",
                            "UNCLASSIFIED",
                            "UNCLASSIFIED",
                            "UNCLASSIFIED",
                        ],
                    }
                ),
                base_path,
            )
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["192.0.2.1", "192.0.2.2", "192.0.2.3", "192.0.2.99"],
                        "IPID_SELECTION_STRATEGY": [
                            "RANDOM",
                            "MULTI",
                            "UNCLASSIFIED",
                            "RANDOM",
                        ],
                    }
                ),
                mass_path,
            )
            zmap_path = raw / "zmap" / self.merge.zmap_id / "zmap.pq"
            zmap_path.parent.mkdir(parents=True)
            pq.write_table(
                pa.table({"IP_ADDR": [f"192.0.2.{index}" for index in (1, 1, 2, 3, 4, 99)]}),
                zmap_path,
            )

            output, stats = merge_strategies(self.merge, processed_root=processed)

            table = pq.read_table(output)
            result = dict(
                zip(
                    table["IP_ADDR"].to_pylist(),
                    table["IPID_SELECTION_STRATEGY"].to_pylist(),
                )
            )
            self.assertEqual(
                result,
                {
                    "192.0.2.1": "CONSTANT",
                    "192.0.2.2": "MULTI",
                    "192.0.2.3": "UNCLASSIFIED",
                    "192.0.2.4": "NOT_ENOUGH_SAMPLES",
                },
            )
            self.assertTrue(pa.types.is_dictionary(table.schema.field(1).type))
            self.assertEqual(stats.rows, 4)
            self.assertEqual(stats.not_enough_samples, 1)

            pdf_path, json_path = render_merged(
                self.merge,
                processed_root=processed,
                figures_root=figures,
                raw_root=raw,
            )
            self.assertTrue(pdf_path.is_file())
            report = json.loads(json_path.read_text())
            self.assertEqual(report["total_ips"], 4)
            self.assertEqual(report["counts"]["NOT_ENOUGH_SAMPLES"], 1)
            self.assertEqual(report["ipid_measurement_coverage"], 80.0)
            self.assertEqual(
                pdf_path,
                self.merge.artifact_path(figures, "strategies", "pdf"),
            )

    def test_only_matching_base_and_mass_targets_can_merge(self):
        invalid = [
            (replace(self.base, scale="mass"), self.mass),
            (self.base, replace(self.mass, scale="base")),
            (self.base, replace(self.mass, protocol="icmp")),
            (self.base, replace(self.mass, connection_mode="connection")),
            (self.base, replace(self.mass, zmap_id="other-zmap")),
        ]
        for base, mass in invalid:
            with self.subTest(base=base, mass=mass):
                with self.assertRaises(ValueError):
                    StrategyMerge(base, mass)

    def test_manifest_iteration_finds_only_canonical_pair(self):
        manifest = {
            "tcp": {
                "zmap": "tcp-zmap",
                "ipid": {
                    "no-connection": {
                        "rt-based": {"base": "tcp-base"},
                        "fixed-interval": {"base": "tcp-fi-base", "mass": "tcp-mass"},
                    }
                },
            },
            "icmp": {
                "zmap": "icmp-zmap",
                "ipid": {
                    "no-connection": {
                        "rt-based": {"base": "icmp-base"},
                    }
                },
            },
        }

        merges = iter_strategy_merges(manifest)

        self.assertEqual([merge.target for merge in merges], [self.merge.target])


if __name__ == "__main__":
    unittest.main()
