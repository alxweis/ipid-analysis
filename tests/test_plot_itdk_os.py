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
from ipid_analysis.plot_itdk_os import (
    MAX_LEGEND_GROUPS,
    NO_TRANSIT_ROLE,
    OTHER_GROUP,
    TRANSIT_ROLE,
    _role_plot_data,
    render_itdk_os_analysis,
)
from ipid_analysis.plot_os_group_strategy import (
    GROUP_INFO,
    PAPER_OS_GROUP_COLORS,
    PAPER_OS_GROUP_ORDER,
    RAW_OS_COLUMNS,
)

IFACES = """\
192.0.2.1 N1 T
192.0.2.2 N2 T D
192.0.2.3 N3 T
192.0.2.4 N4 D
192.0.2.5 N5 D
192.0.2.6 N6
192.0.2.8 N8
"""


class ITDKOSPlotTest(unittest.TestCase):
    @staticmethod
    def _write_os(path: Path) -> None:
        columns = {
            "IP_ADDR": [
                "192.0.2.1",
                "192.0.2.2",
                "192.0.2.3",
                "192.0.2.4",
                "192.0.2.5",
                "192.0.2.6",
                "198.51.100.7",
            ],
            "OS_STATUS": [
                "resolved",
                "resolved",
                "ambiguous",
                "resolved",
                "resolved",
                "unclassified",
                "resolved",
            ],
            "OS_TAG": [
                "ubuntu",
                "cisco-iosxe",
                None,
                "windows",
                "ubuntu",
                None,
                "debian",
            ],
        }
        for name in RAW_OS_COLUMNS[3:]:
            columns[name] = [None] * len(columns["IP_ADDR"])
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table(columns), path)

    @staticmethod
    def _prepare_itdk(root: Path):
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

    def test_fixed_order_colors_and_other_selection(self):
        self.assertEqual(set(PAPER_OS_GROUP_ORDER), set(GROUP_INFO))
        self.assertEqual(set(PAPER_OS_GROUP_ORDER), set(PAPER_OS_GROUP_COLORS))
        self.assertEqual(
            len({PAPER_OS_GROUP_COLORS[group] for group in PAPER_OS_GROUP_ORDER[:10]}),
            10,
        )
        rows = []
        groups = PAPER_OS_GROUP_ORDER[:11]
        for index, group in enumerate(groups):
            rows.append((TRANSIT_ROLE, group, 110 - index))
            rows.append((NO_TRANSIT_ROLE, group, 55 - index))
        data = _role_plot_data(rows)
        self.assertEqual(len(data.groups), MAX_LEGEND_GROUPS)
        self.assertEqual(data.groups[-1], OTHER_GROUP)
        self.assertEqual(sum(data.percentages[TRANSIT_ROLE].values()), 100.0)
        self.assertEqual(sum(data.percentages[NO_TRANSIT_ROLE].values()), 100.0)
        named = data.groups[:-1]
        self.assertEqual(
            named,
            tuple(sorted(named, key=PAPER_OS_GROUP_ORDER.index)),
        )

    def test_render_joins_measured_itdk_and_resolved_os_populations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = self._prepare_itdk(root)
            processed = root / "processed"
            raw = root / "raw"
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
            addresses = [
                "192.0.2.1",
                "192.0.2.2",
                "192.0.2.3",
                "192.0.2.4",
                "192.0.2.5",
                "192.0.2.6",
                "198.51.100.7",
                "192.0.2.8",
            ]
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": addresses,
                        "IPID_SELECTION_STRATEGY": ["SINGLE"] * len(addresses),
                    }
                ),
                strategies,
            )
            os_id = "tcp-os"
            self._write_os(raw / "os" / os_id / "os.pq")

            with patch("ipid_analysis.plot_itdk_os.configure_paper_style"):
                outputs = render_itdk_os_analysis(
                    measurement,
                    dataset,
                    os_id,
                    processed_root=processed,
                    raw_root=raw,
                    figures_root=figures,
                )

            for output in outputs.__dict__.values():
                self.assertTrue(output.is_file(), output)
            distribution = pq.read_table(outputs.distribution).to_pylist()
            self.assertEqual(len(distribution), 4)
            self.assertTrue(all(row["ROLE_RESOLVED_TOTAL"] == 2 for row in distribution))
            self.assertTrue(all(row["PERCENTAGE"] == 50.0 for row in distribution))
            report = json.loads(outputs.role_json.read_text())
            self.assertEqual(report["coverage"]["total_measured"], 8)
            self.assertEqual(report["coverage"]["itdk_matched"], 7)
            self.assertEqual(report["coverage"]["itdk_unmatched"], 1)
            self.assertEqual(report["coverage"]["os_resolved"], 5)
            self.assertEqual(report["coverage"]["itdk_os_resolved"], 4)
            self.assertEqual(report["coverage"]["transit_observed_total"], 3)
            self.assertEqual(report["coverage"]["transit_observed_os_resolved"], 2)
            self.assertEqual(report["coverage"]["no_transit_evidence_total"], 4)
            self.assertEqual(report["coverage"]["no_transit_evidence_os_resolved"], 2)
            self.assertEqual(report["plot_percentages"][TRANSIT_ROLE]["ubuntu"], 50.0)
            self.assertEqual(report["plot_percentages"][TRANSIT_ROLE]["cisco"], 50.0)
            self.assertEqual(report["plot_percentages"][NO_TRANSIT_ROLE]["ubuntu"], 50.0)
            self.assertEqual(report["plot_percentages"][NO_TRANSIT_ROLE]["windows"], 50.0)


if __name__ == "__main__":
    unittest.main()
