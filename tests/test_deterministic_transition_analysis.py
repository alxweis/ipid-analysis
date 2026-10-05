from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from ipid_analysis.deterministic_transition_analysis import (
    analyze_deterministic_transitions,
    rule_diagnostics,
)
from ipid_analysis.strategies import MeasurementConfig


class DeterministicTransitionAnalysisTest(unittest.TestCase):
    @staticmethod
    def _write_raw(root: Path, measurement_id: str, rows: dict[str, str]) -> None:
        directory = root / "raw" / "ipid" / measurement_id
        directory.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "IP_ADDR": list(rows),
                    "IPID_SEQUENCE": list(rows.values()),
                }
            ),
            directory / "ipid.pq",
        )
        requests = 25 if measurement_id.endswith("mass") else 4
        (directory / "ipid.snapshot.yaml").write_text(
            yaml.safe_dump(
                {
                    "connection_count": 4,
                    "requests_per_connection": requests,
                    "request_ip_ids": [1000, 2000, 1001, 2001],
                }
            )
        )

    @staticmethod
    def _write_strategies(
        root: Path,
        zmap_id: str,
        variant: str,
        stem: str,
        rows: dict[str, str],
    ) -> None:
        directory = root / "processed" / zmap_id / "no-connection" / variant
        directory.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "IP_ADDR": list(rows),
                    "IPID_SELECTION_STRATEGY": list(rows.values()),
                }
            ),
            directory / f"{stem}_strategies.pq",
        )

    @staticmethod
    def _per_bucket(connection_count: int, requests: int) -> np.ndarray:
        starts = np.asarray([100, 10000, 20000, 30000], dtype=np.int64)
        return np.asarray(
            [
                starts[index % connection_count] + index // connection_count
                for index in range(connection_count * requests)
            ],
            dtype=np.int64,
        )

    def test_rule_diagnostics_counts_one_observed_per_bucket_violation(self):
        cfg = MeasurementConfig(4, 4, np.asarray([1000, 2000, 1001, 2001]))
        sequence = self._per_bucket(4, 4)
        sequence[8] = (sequence[8] + 30000) % 65536

        diagnostics = rule_diagnostics(
            sequence,
            cfg,
            "PER_BUCKET",
            skip_first=False,
        )

        self.assertEqual(diagnostics.present_count, 16)
        self.assertEqual(diagnostics.evaluated_constraints, 12)
        self.assertEqual(diagnostics.missing_constraints, 0)
        self.assertEqual(diagnostics.rule_violations, 2)
        self.assertFalse(diagnostics.exact_match)

    def test_analysis_joins_three_measurements_and_writes_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw_root = root / "raw"
            processed_root = root / "processed"
            zmap_id = "icmp-zmap"
            rt_id = "icmp-rt-base"
            fixed_id = "icmp-fi-base"
            mass_id = "icmp-fi-mass"
            ip = "192.0.2.1"
            ignored = "192.0.2.2"

            rt_sequence = self._per_bucket(4, 4)
            rt_sequence[8] = (rt_sequence[8] + 30000) % 65536
            fixed_sequence = self._per_bucket(4, 4)
            mass_sequence = self._per_bucket(4, 25)
            self._write_raw(
                root,
                rt_id,
                {
                    ip: ",".join(map(str, rt_sequence)),
                    ignored: ",".join(map(str, fixed_sequence)),
                },
            )
            self._write_raw(
                root,
                fixed_id,
                {
                    ip: ",".join(map(str, fixed_sequence)),
                    ignored: ",".join(map(str, fixed_sequence)),
                },
            )
            self._write_raw(
                root,
                mass_id,
                {
                    ip: ",".join(map(str, mass_sequence)),
                    ignored: ",".join(map(str, mass_sequence)),
                },
            )
            self._write_strategies(
                root,
                zmap_id,
                "rt-based-base",
                "n-rt-b",
                {ip: "UNCLASSIFIED", ignored: "UNCLASSIFIED"},
            )
            self._write_strategies(
                root,
                zmap_id,
                "fixed-interval-base",
                "n-fi-b",
                {ip: "PER_BUCKET", ignored: "PER_BUCKET"},
            )
            self._write_strategies(
                root,
                zmap_id,
                "fixed-interval-mass",
                "n-fi-m",
                {ip: "PER_BUCKET", ignored: "RANDOM"},
            )

            manifest_path = root / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "icmp": {
                            "zmap": zmap_id,
                            "ipid": {
                                "no-connection": {
                                    "rt-based": {"base": rt_id},
                                    "fixed-interval": {
                                        "base": fixed_id,
                                        "mass": mass_id,
                                    },
                                }
                            },
                        }
                    }
                )
            )
            data_dir = root / "analysis-data"
            figure_dir = root / "figures"

            artifacts = analyze_deterministic_transitions(
                manifest_path,
                raw_root=raw_root,
                processed_root=processed_root,
                data_dir=data_dir,
                figure_dir=figure_dir,
                render_plots=True,
            )

            table = pq.read_table(artifacts["aggregate"])
            self.assertEqual(table.num_rows, 1)
            row = table.to_pylist()[0]
            self.assertEqual(row["IP_ADDR"], ip)
            self.assertEqual(row["MASS_CLASS"], "PER_BUCKET")
            self.assertEqual(row["RT_BASE_RULE_VIOLATIONS"], 2)
            self.assertFalse(row["RT_BASE_EXACT_MATCH_TO_MASS_CLASS"])
            self.assertEqual(row["FIXED_BASE_RULE_VIOLATIONS"], 0)
            self.assertTrue(row["FIXED_BASE_EXACT_MATCH_TO_MASS_CLASS"])
            self.assertEqual(row["MASS_RULE_VIOLATIONS"], 0)
            self.assertTrue(row["MASS_EXACT_MATCH_TO_MASS_CLASS"])
            metadata = json.loads(artifacts["json"].read_text())
            self.assertEqual(metadata["transition_count"], 1)
            self.assertEqual(metadata["plot_count"], 1)
            plots = list((figure_dir / "sequences").rglob("*.png"))
            self.assertEqual(len(plots), 1)


if __name__ == "__main__":
    unittest.main()
