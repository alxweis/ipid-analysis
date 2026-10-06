from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from ipid_analysis.random_classifier_candidate import create_candidate_null_tables
from ipid_analysis.random_reproducibility_analysis import (
    evaluate_random_reproducibility,
    prepare_random_reproducibility,
)


class RandomReproducibilityAnalysisTest(unittest.TestCase):
    @staticmethod
    def _sequence(values: np.ndarray, missing: set[int] | None = None) -> str:
        missing = missing or set()
        return ",".join("-" if index in missing else str(int(value)) for index, value in enumerate(values))

    @staticmethod
    def _write_measurement(
        raw_root: Path,
        measurement_id: str,
        rows: dict[str, str],
    ) -> None:
        directory = raw_root / "ipid" / measurement_id
        directory.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table({"IP_ADDR": list(rows), "IPID_SEQUENCE": list(rows.values())}),
            directory / "ipid.pq",
        )
        (directory / "ipid.snapshot.yaml").write_text(
            yaml.safe_dump(
                {
                    "connection_count": 4,
                    "requests_per_connection": 25,
                    "request_ip_ids": [1000, 2000, 1001, 2001],
                }
            )
        )

    def test_prepare_and_evaluate_write_isolated_reproducibility_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw_root = root / "raw"
            processed_root = root / "processed"
            data_dir = root / "analysis"
            figure_dir = root / "figures"
            zmap_id = "icmp-zmap"
            baseline_id = "icmp-baseline-mass"
            repeat_one = "icmp-repeat-one"
            repeat_two = "icmp-repeat-two"
            unclassified_ip = "192.0.2.1"
            random_ip = "192.0.2.2"

            structured = np.arange(100, dtype=np.int64)
            rng = np.random.default_rng(7)
            random_values = rng.integers(0, 65536, size=100, dtype=np.uint16)
            self._write_measurement(
                raw_root,
                baseline_id,
                {
                    unclassified_ip: self._sequence(structured),
                    random_ip: self._sequence(random_values),
                },
            )
            self._write_measurement(
                raw_root,
                repeat_one,
                {random_ip: self._sequence(random_values)},
            )
            self._write_measurement(
                raw_root,
                repeat_two,
                {unclassified_ip: self._sequence(structured, {10, 20, 30})},
            )

            zmap_dir = raw_root / "zmap" / zmap_id
            zmap_dir.mkdir(parents=True)
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": [unclassified_ip, random_ip],
                        "REPLY_TYPE": ["echo_reply", "echo_reply"],
                    }
                ),
                zmap_dir / "zmap.pq",
            )
            strategy_dir = (
                processed_root / zmap_id / "no-connection" / "fixed-interval-mass"
            )
            strategy_dir.mkdir(parents=True)
            strategy_path = strategy_dir / "n-fi-m_strategies.pq"
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": [unclassified_ip, random_ip],
                        "IPID_SELECTION_STRATEGY": pa.array(
                            ["UNCLASSIFIED", "RANDOM"]
                        ).dictionary_encode(),
                    }
                ),
                strategy_path,
            )
            original_strategy_bytes = strategy_path.read_bytes()

            manifest_path = root / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "icmp": {
                            "zmap": zmap_id,
                            "ipid": {
                                "no-connection": {
                                    "fixed-interval": {"mass": baseline_id}
                                }
                            },
                        }
                    }
                )
            )
            tables = create_candidate_null_tables(sample_count=1_000, seed=123)

            prepared = prepare_random_reproducibility(
                manifest_path,
                raw_root=raw_root,
                processed_root=processed_root,
                data_dir=data_dir,
                figure_dir=figure_dir,
                maximum_targets=2,
                null_tables=tables,
            )

            cohort = pq.read_table(prepared["cohort"]).to_pylist()
            self.assertEqual(len(cohort), 2)
            self.assertEqual({row["COHORT"] for row in cohort}, {"UNCLASSIFIED", "RANDOM"})
            target = pq.read_table(prepared["targets"])
            self.assertEqual(target.column_names, ["IP_ADDR", "REPLY_TYPE"])
            self.assertEqual(target.num_rows, 2)
            prepare_metadata = json.loads(prepared["json"].read_text())
            self.assertEqual(prepare_metadata["selection"]["maximum_targets"], 2)
            self.assertEqual(prepare_metadata["selection"]["unclassified_selected"], 1)
            self.assertEqual(prepare_metadata["selection"]["random_controls"], 1)

            evaluated = evaluate_random_reproducibility(
                manifest_path,
                [repeat_one, repeat_two],
                raw_root=raw_root,
                processed_root=processed_root,
                data_dir=data_dir,
                figure_dir=figure_dir,
                null_tables=tables,
            )

            repetitions = pq.read_table(evaluated["repetitions"]).to_pylist()
            self.assertEqual(len(repetitions), 4)
            self.assertIn("P_GAP", pq.read_schema(evaluated["repetitions"]).names)
            missing = [row for row in repetitions if not row["OBSERVED"]]
            self.assertEqual(len(missing), 2)
            summary = {row["IP_ADDR"]: row for row in pq.read_table(evaluated["summary"]).to_pylist()}
            self.assertEqual(summary[unclassified_ip]["OBSERVED_COUNT"], 1)
            self.assertEqual(summary[random_ip]["OBSERVED_COUNT"], 1)
            self.assertTrue(evaluated["pdf"].is_file())
            self.assertEqual(strategy_path.read_bytes(), original_strategy_bytes)


if __name__ == "__main__":
    unittest.main()
