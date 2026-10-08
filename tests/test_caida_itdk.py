import bz2
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.caida_itdk import prepare_itdk
from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.plot_itdk_strategy import (
    DISCORDANT_X_LABEL,
    PLOT_STRATEGIES,
    _combination_value_label,
    _format_percentage,
    render_itdk_analysis,
)
from ipid_analysis.plot_strategy_refinement import PLOT_STRATEGY_ORDER

IFACES = """\
192.0.2.1 N1 L1 T
192.0.2.2 N1 L2 T D
192.0.2.3 N2 D
192.0.2.4 N2
192.0.2.5 N3 D
192.0.2.6 N3 D
192.0.2.7 N4 T
192.0.2.8 N4 T D
224.0.0.1 N9 T
not-an-ip N10 D
"""


class CaidaITDKTest(unittest.TestCase):
    def test_paper_order_and_discordant_labels_match_manuscript_style(self):
        self.assertEqual(
            PLOT_STRATEGIES,
            tuple(
                strategy for strategy in PLOT_STRATEGY_ORDER if strategy != "NOT_ENOUGH_SAMPLES"
            ),
        )
        self.assertEqual(DISCORDANT_X_LABEL, "Discordant Nodes [% (#)]")
        self.assertEqual(_format_percentage(30.000000), "30")
        self.assertEqual(_format_percentage(30.235252), "30.2353")
        self.assertEqual(_format_percentage(50.4200000), "50.42")
        self.assertEqual(_format_percentage(0.00006), "0.0001")
        self.assertEqual(_combination_value_label(40.0, 2), "40 (2)")

    def _prepare(self, root: Path):
        source = root / "private" / "custom.ifaces.bz2"
        source.parent.mkdir(parents=True, exist_ok=True)
        with bz2.open(source, "wt", encoding="utf-8") as output:
            output.write(IFACES)
        return prepare_itdk(
            release="2026-03",
            topology="midar-iff-snmp-tnt",
            ifaces=source,
            raw_root=root / "raw",
        )

    def test_prepare_local_release_parses_roles_and_reuses_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = self._prepare(root)

            table = pq.read_table(dataset.interfaces_path)
            self.assertEqual(table.num_rows, 8)
            self.assertEqual(table.column_names, ["IP_ADDR", "NODE_ID", "T", "D"])
            rows = {row["IP_ADDR"]: row for row in table.to_pylist()}
            self.assertEqual(rows["192.0.2.1"]["NODE_ID"], "N1")
            self.assertTrue(rows["192.0.2.2"]["T"])
            self.assertTrue(rows["192.0.2.2"]["D"])
            self.assertNotIn("224.0.0.1", rows)

            metadata = json.loads(dataset.metadata_path.read_text())
            self.assertEqual(metadata["release"], "2026-03")
            self.assertEqual(metadata["interfaces"], 8)
            self.assertEqual(metadata["skipped_invalid"], 1)
            self.assertEqual(metadata["skipped_placeholder"], 1)
            self.assertEqual(self._prepare(root), dataset)

    def test_role_and_cross_interface_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = self._prepare(root)
            processed = root / "processed"
            figures = root / "figures"
            measurement = IpidMeasurement(
                protocol="tcp",
                connection_mode="connection",
                interval="rt-based",
                scale="base",
                measurement_id="tcp-connection",
                zmap_id="tcp-zmap",
            )
            strategies = measurement.artifact_path(processed, "strategies")
            strategies.parent.mkdir(parents=True)
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": [
                            "192.0.2.1",
                            "192.0.2.2",
                            "192.0.2.3",
                            "192.0.2.4",
                            "192.0.2.5",
                            "192.0.2.6",
                            "192.0.2.7",
                            "192.0.2.8",
                            "198.51.100.1",
                        ],
                        "IPID_SELECTION_STRATEGY": [
                            "SINGLE",
                            "SINGLE",
                            "SINGLE",
                            "RANDOM",
                            "UNCLASSIFIED",
                            "NOT_ENOUGH_SAMPLES",
                            "PER_BUCKET",
                            "SINGLE",
                            "CONSTANT",
                        ],
                    }
                ),
                strategies,
            )

            with patch("ipid_analysis.plot_itdk_strategy.configure_paper_style"):
                outputs = render_itdk_analysis(
                    measurement,
                    dataset,
                    processed_root=processed,
                    figures_root=figures,
                )

            for output in outputs.__dict__.values():
                self.assertTrue(output.is_file(), output)
            role = json.loads(outputs.role_json.read_text())
            self.assertEqual(role["coverage"]["total"], 9)
            self.assertEqual(role["coverage"]["matched"], 8)
            self.assertEqual(role["coverage"]["unmatched"], 1)
            self.assertEqual(role["coverage"]["t1_d1"], 2)
            self.assertEqual(role["coverage"]["t1_d0"], 2)
            self.assertEqual(role["coverage"]["t0_d1"], 3)
            self.assertEqual(role["coverage"]["t0_d0"], 1)
            self.assertEqual(
                role["plot_percentages"]["No Transit Evidence"]["UNCLASSIFIED"],
                50.0,
            )

            consistency = json.loads(outputs.consistency_json.read_text())
            self.assertEqual(consistency["matched_multi_interface_nodes"], 4)
            self.assertEqual(consistency["eligible_nodes_with_two_classified_interfaces"], 3)
            self.assertEqual(consistency["excluded_for_classification_coverage"], 1)
            self.assertEqual(consistency["strict_agreement_nodes"], 1)
            self.assertAlmostEqual(consistency["strict_agreement_percentage"], 100 / 3)
            self.assertAlmostEqual(consistency["mean_node_pairwise_agreement"], 1 / 3)
            self.assertEqual(consistency["transit_evidenced_eligible_nodes"], 2)
            self.assertEqual(consistency["transit_evidenced_strict_percentage"], 50.0)

            combinations = pq.read_table(outputs.combinations).to_pylist()
            self.assertEqual(len(combinations), 2)
            self.assertEqual({row["PERCENTAGE"] for row in combinations}, {50.0})


if __name__ == "__main__":
    unittest.main()
