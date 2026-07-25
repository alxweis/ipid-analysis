import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.classifier_validation import (
    FIXED_IDEAL_DATASET,
    FIXED_LOSSY_DATASET,
    FIXED_REORDERED_DATASET,
    FIXED_STRATEGIES,
    RT_DATASET,
    RT_STRATEGIES,
    TRIVIAL_SAMPLES_PER_STRATEGY,
    apply_fixed_interval_impairments,
    generate_fixed_sequences,
    generate_rt_sequences,
    validate_classifier,
)
from ipid_analysis.strategies import IPIDStrategy, classify_batch, classify_batch_mass


class ClassifierValidationTest(unittest.TestCase):
    def test_generators_match_measurement_shapes_and_expected_classes(self):
        rt_config, rt_sequences = generate_rt_sequences(16, np.random.default_rng(1))
        self.assertEqual(tuple(rt_sequences), RT_STRATEGIES)
        for strategy, values in rt_sequences.items():
            self.assertEqual(values.shape, (16, 16))
            detected = classify_batch(values, rt_config)
            self.assertTrue(
                np.all(detected == int(IPIDStrategy[strategy])),
                strategy,
            )
            tcp_detected = classify_batch(values, rt_config, skip_first=True)
            self.assertTrue(
                np.all(tcp_detected == int(IPIDStrategy[strategy])),
                f"{strategy} after TCP first-round skip",
            )

        fixed_sequences = generate_fixed_sequences(16, np.random.default_rng(2))
        self.assertEqual(tuple(fixed_sequences), FIXED_STRATEGIES)
        for strategy, values in fixed_sequences.items():
            self.assertEqual(values.shape, (16, 100))
            detected = classify_batch_mass(
                pa.array(values.astype(np.int64).tolist(), type=pa.list_(pa.int64()))
            )
            self.assertTrue(
                np.all(detected == int(IPIDStrategy[strategy])),
                strategy,
            )

    def test_impairments_remove_twenty_and_reorder_present_values_only(self):
        ideal = np.tile(np.arange(100, dtype=np.uint16), (8, 1))
        loss_mask, lossy, reordered = apply_fixed_interval_impairments(
            ideal,
            np.random.default_rng(3),
        )

        self.assertTrue(np.all(loss_mask.sum(axis=1) == 20))
        np.testing.assert_array_equal(lossy, ideal)
        for row_index in range(len(ideal)):
            present = ~loss_mask[row_index]
            self.assertEqual(
                sorted(reordered[row_index, present].tolist()),
                sorted(ideal[row_index, present].tolist()),
            )
            self.assertTrue(np.any(reordered[row_index, present] != ideal[row_index, present]))

    def test_validation_writes_sequences_metrics_and_figures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outputs = validate_classifier(
                samples_per_strategy=8,
                seed=42,
                processed_root=root / "processed",
                figures_root=root / "figures",
            )

            for path in outputs.values():
                self.assertTrue(path.is_file(), path)

            table = pq.read_table(outputs["dataset"])
            rt_row_count = 2 * TRIVIAL_SAMPLES_PER_STRATEGY + 5 * 8
            fixed_row_count = TRIVIAL_SAMPLES_PER_STRATEGY + 3 * 8
            self.assertEqual(table.num_rows, rt_row_count + 3 * fixed_row_count)
            rows = table.to_pylist()
            datasets = {row["DATASET"] for row in rows}
            self.assertEqual(
                datasets,
                {
                    RT_DATASET,
                    FIXED_IDEAL_DATASET,
                    FIXED_LOSSY_DATASET,
                    FIXED_REORDERED_DATASET,
                },
            )
            for row in rows:
                tokens = row["IPID_SEQUENCE"].split(",")
                expected_length = 16 if row["DATASET"] == RT_DATASET else 100
                self.assertEqual(len(tokens), expected_length)
                if row["DATASET"] in (FIXED_LOSSY_DATASET, FIXED_REORDERED_DATASET):
                    self.assertEqual(tokens.count("-"), 20)
                    self.assertEqual(row["LOSS_COUNT"], 20)
                if row["DATASET"] == FIXED_REORDERED_DATASET:
                    self.assertEqual(row["REORDERED_COUNT"], 16)

            rt_report = json.loads(outputs["rt_based_json"].read_text())
            fixed_report = json.loads(outputs["fixed_interval_json"].read_text())
            impaired_report = json.loads(outputs["impaired_json"].read_text())
            self.assertEqual(
                rt_report["samples_by_dataset_and_strategy"][RT_DATASET],
                {
                    "REFLECTION": TRIVIAL_SAMPLES_PER_STRATEGY,
                    "CONSTANT": TRIVIAL_SAMPLES_PER_STRATEGY,
                    "SINGLE": 8,
                    "PER_CONNECTION": 8,
                    "PER_DESTINATION": 8,
                    "PER_BUCKET": 8,
                    "UNCLASSIFIED": 8,
                },
            )
            self.assertEqual(
                fixed_report["samples_by_dataset_and_strategy"][FIXED_IDEAL_DATASET],
                {
                    "CONSTANT": TRIVIAL_SAMPLES_PER_STRATEGY,
                    "MULTI": 8,
                    "RANDOM": 8,
                    "UNCLASSIFIED": 8,
                },
            )
            self.assertEqual(rt_report["metrics"]["accuracy"], 1.0)
            self.assertEqual(fixed_report["metrics"]["macro"]["f1"], 1.0)
            self.assertEqual(
                impaired_report["datasets"]["lossy"]["metrics"]["accuracy"],
                1.0,
            )
            self.assertEqual(
                impaired_report["datasets"]["lossy_reordered"]["metrics"]["accuracy"],
                1.0,
            )


if __name__ == "__main__":
    unittest.main()
