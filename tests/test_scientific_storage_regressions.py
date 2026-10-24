"""Scientific equality across batching, history reuse and interrupted outputs."""

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from ipid_analysis import increments
from ipid_analysis import random_reproducibility_analysis as reproducibility
from ipid_analysis.artifacts import completion_path
from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.random_classifier_candidate import create_candidate_null_tables
from ipid_analysis.strategies import MeasurementConfig, classify_measurement


class ScientificStorageRegressions(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        environment = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
            },
        )
        environment.start()
        self.addCleanup(environment.stop)

    def measurement(self, sequences, *, requests=4):
        raw = self.root / "raw"
        measurement = IpidMeasurement(
            "icmp", "no-connection", "rt-based", "base", "icmp-test", "icmp-zmap"
        )
        folder = raw / measurement.input_key
        folder.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "IP_ADDR": [f"192.0.2.{i + 1}" for i in range(len(sequences))],
                    "IPID_SEQUENCE": sequences,
                }
            ),
            folder / "ipid.pq",
        )
        snapshot = folder / "ipid.snapshot.yaml"
        snapshot.write_text(
            yaml.safe_dump(
                {
                    "connection_count": 4,
                    "requests_per_connection": requests,
                    "request_ip_ids": [1000, 2000, 1001, 2001],
                }
            )
        )
        return measurement, raw, self.root / "processed"

    def test_increments_follow_historical_labels_and_invalidate_on_label_change(self):
        sequence = ",".join(map(str, range(16)))
        measurement, raw, processed = self.measurement([sequence])
        strategies = measurement.artifact_path(processed, "strategies")
        strategies.parent.mkdir(parents=True)
        pq.write_table(
            pa.table({"IP_ADDR": ["192.0.2.1"], "IPID_SELECTION_STRATEGY": ["PER_CONNECTION"]}),
            strategies,
        )
        with (
            patch.object(increments, "RAW_DATA_DIR", raw),
            patch.object(increments, "PROCESSED_DATA_DIR", processed),
        ):
            output = increments.extract_increments(measurement, batch_size=1, threads=1)
            row = pq.read_table(output).to_pylist()[0]
            self.assertEqual(row["IPID_SELECTION_STRATEGY"], "PER_CONNECTION")
            self.assertEqual(row["INCREMENTS"], [4] * 12)
            pq.write_table(
                pa.table({"IP_ADDR": ["192.0.2.1"], "IPID_SELECTION_STRATEGY": ["SINGLE"]}),
                strategies,
            )
            increments.extract_increments(measurement, batch_size=1, threads=1)
            row = pq.read_table(output).to_pylist()[0]
            self.assertEqual(row["IPID_SELECTION_STRATEGY"], "SINGLE")
            self.assertEqual(row["INCREMENTS"], [1] * 15)
            before = output.stat().st_mtime_ns
            increments.extract_increments(measurement, batch_size=1, threads=1)
            self.assertEqual(output.stat().st_mtime_ns, before)

    def test_legacy_valid_prefix_is_rebuilt_instead_of_reused(self):
        measurement, raw, processed = self.measurement(
            [",".join(["7"] * 16), ",".join(["8"] * 16)]
        )
        output = measurement.artifact_path(processed, "strategies")
        output.parent.mkdir(parents=True)
        pq.write_table(
            pa.table({"IP_ADDR": ["192.0.2.1"], "IPID_SELECTION_STRATEGY": ["CONSTANT"]}), output
        )
        classify_measurement(
            measurement, raw_root=raw, processed_root=processed, batch_size=1, threads=1
        )
        self.assertEqual(pq.read_metadata(output).num_rows, 2)
        self.assertEqual(json.loads(completion_path(output).read_text())["status"], "complete")

    def test_valid_historical_labels_are_adopted_without_reclassification(self):
        measurement, raw, processed = self.measurement([",".join(["7"] * 16)])
        output = measurement.artifact_path(processed, "strategies")
        output.parent.mkdir(parents=True)
        schema = pa.schema(
            [("IP_ADDR", pa.string()), ("IPID_SELECTION_STRATEGY", pa.string())],
            metadata={b"classifier_version": b"historical"},
        )
        pq.write_table(
            pa.Table.from_pylist(
                [{"IP_ADDR": "192.0.2.1", "IPID_SELECTION_STRATEGY": "UNCLASSIFIED"}],
                schema=schema,
            ),
            output,
        )
        before = output.read_bytes()
        classify_measurement(measurement, raw_root=raw, processed_root=processed)
        self.assertEqual(output.read_bytes(), before)
        self.assertEqual(
            json.loads(completion_path(output).read_text())["operation"],
            "validated-legacy-adoption",
        )

    def test_incomplete_processed_copy_is_restored_from_authoritative_labels(self):
        measurement, raw, processed = self.measurement([",".join(["7"] * 16)])
        persisted = raw / measurement.input_key / "strategies.pq"
        pq.write_table(
            pa.table({"IP_ADDR": ["192.0.2.1"], "IPID_SELECTION_STRATEGY": ["UNCLASSIFIED"]}),
            persisted,
        )
        output = classify_measurement(measurement, raw_root=raw, processed_root=processed)
        completion_path(output).write_text('{"status":"writing"}')
        classify_measurement(measurement, raw_root=raw, processed_root=processed)
        self.assertEqual(output.read_bytes(), persisted.read_bytes())
        self.assertEqual(
            pq.read_table(output)["IPID_SELECTION_STRATEGY"].to_pylist(), ["UNCLASSIFIED"]
        )

    def test_streamed_cohort_matches_original_selection_exactly(self):
        rng = np.random.default_rng(42)
        rows = [
            (
                f"192.0.2.{index + 1}",
                "UNCLASSIFIED" if index < 95 else "RANDOM",
                ",".join(map(str, rng.integers(0, 65536, 100))),
            )
            for index in range(160)
        ]
        raw, strategies, snapshot = (
            self.root / "raw.pq",
            self.root / "strategies.pq",
            self.root / "snapshot.yaml",
        )
        pq.write_table(
            pa.table(
                {"IP_ADDR": [row[0] for row in rows], "IPID_SEQUENCE": [row[2] for row in rows]}
            ),
            raw,
        )
        pq.write_table(
            pa.table(
                {
                    "IP_ADDR": [row[0] for row in rows],
                    "IPID_SELECTION_STRATEGY": [row[1] for row in rows],
                }
            ),
            strategies,
        )
        snapshot.write_text(
            yaml.safe_dump(
                {
                    "connection_count": 4,
                    "requests_per_connection": 25,
                    "request_ip_ids": [1000, 2000, 1001, 2001],
                }
            )
        )
        cfg = MeasurementConfig(4, 25, np.array([1000, 2000, 1001, 2001]))
        tables = create_candidate_null_tables(256, 123)
        population = [
            {
                "IP_ADDR": ip,
                "COHORT": strategy,
                "BASELINE_CLASS": strategy,
                "BASELINE_MEASUREMENT_ID": "icmp-mass",
                "BASELINE_SEQUENCE": sequence,
                **reproducibility._diagnostics(
                    reproducibility.parse_sequence(sequence, 100), cfg, tables
                ),
            }
            for ip, strategy, sequence in sorted(rows)
        ]
        unclassified = reproducibility._proportional_stratified_sample(
            [row for row in population if row["COHORT"] == "UNCLASSIFIED"], 9, 42
        )
        random_rows = [row for row in population if row["COHORT"] == "RANDOM"]
        near_count = (len(unclassified) + 1) // 2
        near = sorted(random_rows, key=lambda row: (row["RANDOM_SCORE"], row["IP_ADDR"]))[
            :near_count
        ]
        near_ips = {row["IP_ADDR"] for row in near}
        far = sorted(
            [row for row in random_rows if row["IP_ADDR"] not in near_ips],
            key=lambda row: reproducibility._stable_key(row["IP_ADDR"], 42),
        )[: len(unclassified) - near_count]
        for row in unclassified:
            row["CONTROL_STRATUM"] = ""
        for name, selected in (("NEAR_THRESHOLD", near), ("RANDOM_SAMPLE", far)):
            for row in selected:
                row.update(CONTROL_STRATUM=name, SELECTION_PROBABILITY=None, SELECTION_WEIGHT=None)
        expected = pa.Table.from_pylist(
            sorted(unclassified + near + far, key=lambda row: row["IP_ADDR"])
        )
        expected = expected.select(sorted(expected.column_names))
        for batch_size in (7, 4096):
            folder = self.root / f"batch-{batch_size}"
            with patch.object(reproducibility, "batch_budget", return_value=batch_size):
                outputs = reproducibility.prepare_random_reproducibility_paths(
                    raw,
                    snapshot,
                    strategies,
                    measurement_id="icmp-mass",
                    target="icmp.ipid.no-connection.fixed-interval.mass",
                    cohort_path=folder / "cohort.pq",
                    target_path=folder / "targets.pq",
                    json_path=folder / "prepare.json",
                    maximum_targets=18,
                    seed=42,
                    null_tables=tables,
                )
            observed = pq.read_table(outputs["cohort"])
            self.assertEqual(observed.column_names, expected.column_names)
            self.assertEqual(observed.to_pylist(), expected.to_pylist())
            self.assertEqual(pq.read_metadata(outputs["targets"]).num_rows, 18)
            metadata = json.loads(outputs["json"].read_text())
            self.assertEqual(metadata["selection"]["unclassified_population"], 95)
            self.assertEqual(metadata["selection"]["random_population"], 65)


if __name__ == "__main__":
    unittest.main()
