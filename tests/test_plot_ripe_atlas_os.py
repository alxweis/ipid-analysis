from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.plot_os_group_strategy import RAW_OS_COLUMNS
from ipid_analysis.plot_ripe_atlas_os import render_ripe_atlas_os_analysis
from ipid_analysis.ripe_atlas import RipeAtlasDataset, campaign_window


class RipeAtlasOSPlotTest(unittest.TestCase):
    def test_render_uses_same_matched_population_and_fixed_os_taxonomy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processed = root / "processed"
            raw = root / "raw"
            figures = root / "figures"
            measurement = IpidMeasurement(
                protocol="icmp",
                connection_mode="no-connection",
                interval="rt-based",
                scale="base",
                measurement_id="icmp-test",
                zmap_id="icmp-zmap",
            )
            strategies = measurement.artifact_path(processed, "strategies")
            strategies.parent.mkdir(parents=True)
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["1.1.1.1", "8.8.8.8", "9.9.9.9", "4.4.4.4"],
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
            window = campaign_window(datetime(2026, 1, 29, tzinfo=timezone.utc))
            dataset_dir = processed / "ripe"
            dataset_dir.mkdir(parents=True)
            roles = dataset_dir / "ipv4-roles.pq"
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["1.1.1.1", "8.8.8.8", "9.9.9.9"],
                        "T": [True, False, True],
                        "D": [False, True, True],
                        "TRACE_COUNT": [2, 1, 3],
                        "PROBE_COUNT": [1, 1, 2],
                        "MEASUREMENT_COUNT": [1, 1, 2],
                        "FIRST_SEEN": [1, 1, 1],
                        "LAST_SEEN": [2, 2, 2],
                        "PROTOCOLS": ["ICMP", "ICMP", "ICMP,TCP"],
                    }
                ),
                roles,
            )
            dataset = RipeAtlasDataset(
                source="test",
                window=window,
                directory=dataset_dir,
                roles_path=roles,
                metadata_path=dataset_dir / "source.json",
            )
            os_id = "icmp-os"
            os_path = raw / "os" / os_id / "os.pq"
            os_path.parent.mkdir(parents=True)
            columns = {
                "IP_ADDR": ["1.1.1.1", "8.8.8.8", "4.4.4.4"],
                "OS_STATUS": ["resolved", "resolved", "resolved"],
                "OS_TAG": ["ubuntu", "windows", "debian"],
            }
            for name in RAW_OS_COLUMNS[3:]:
                columns[name] = [None, None, None]
            pq.write_table(pa.table(columns), os_path)

            with (
                patch("ipid_analysis.plot_itdk_os.configure_paper_style"),
                patch("ipid_analysis.plot_ripe_atlas_strategy.configure_paper_style"),
            ):
                outputs = render_ripe_atlas_os_analysis(
                    measurement,
                    dataset,
                    os_id,
                    processed_root=processed,
                    raw_root=raw,
                    figures_root=figures,
                )
            self.assertTrue(all(path.is_file() for path in outputs.__dict__.values()))
            report = json.loads(outputs.role_json.read_text(encoding="utf-8"))
            self.assertEqual(report["coverage"]["total_measured"], 4)
            self.assertEqual(report["coverage"]["ripe_matched"], 3)
            self.assertEqual(report["coverage"]["ripe_os_resolved"], 2)
            self.assertEqual(report["plot_counts"]["Transit-Observed"]["ubuntu"], 1)
            self.assertEqual(report["plot_counts"]["No Transit Evidence"]["windows"], 1)


if __name__ == "__main__":
    unittest.main()
