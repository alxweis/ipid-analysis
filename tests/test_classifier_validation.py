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
    FIXED_OUT_OF_SCOPE_DATASET,
    FIXED_OUT_OF_SCOPE_STRATEGIES,
    FIXED_REORDERED_DATASET,
    FIXED_STRATEGIES,
    PER_BUCKET_MAX_INC,
    RT_DATASET,
    RT_OUT_OF_SCOPE_DATASET,
    RT_OUT_OF_SCOPE_STRATEGIES,
    RT_STRATEGIES,
    TRIVIAL_SAMPLES_PER_STRATEGY,
    _generate_multi_sequences,
    apply_fixed_interval_impairments,
    generate_fixed_out_of_scope_sequences,
    generate_fixed_sequences,
    generate_rt_out_of_scope_sequences,
    generate_rt_sequences,
    validate_classifier,
)
from ipid_analysis.strategies import (
    MAX_INC,
    MULTI_MAX_CLUSTERS,
    IPIDStrategy,
    MeasurementConfig,
    _cluster_counts_mass,
    _mass_padded,
    classify_batch,
    classify_batch_mass,
)


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
        single_increments = np.diff(rt_sequences["SINGLE"], axis=1)
        self.assertGreaterEqual(int(single_increments.min()), 1)
        self.assertLessEqual(int(single_increments.max()), MAX_INC)
        self.assertGreater(int(single_increments.max()), 2_000)
        bucket_connections = rt_sequences["PER_BUCKET"].reshape(16, 4, 4).transpose(0, 2, 1)
        bucket_increments = np.diff(bucket_connections, axis=2)
        self.assertGreaterEqual(int(bucket_increments.min()), 1)
        self.assertLessEqual(int(bucket_increments.max()), PER_BUCKET_MAX_INC)
        self.assertGreater(int(bucket_increments.max()), 2_000)
        rt_out_of_scope = generate_rt_out_of_scope_sequences(
            16,
            np.random.default_rng(2),
        )
        self.assertEqual(tuple(rt_out_of_scope), RT_OUT_OF_SCOPE_STRATEGIES)
        for strategy, values in rt_out_of_scope.items():
            self.assertEqual(values.shape, (16, 16))
            detected = classify_batch(values, rt_config)
            self.assertTrue(
                np.all(detected == int(IPIDStrategy.UNCLASSIFIED)),
                strategy,
            )
            tcp_detected = classify_batch(values, rt_config, skip_first=True)
            self.assertTrue(
                np.all(tcp_detected == int(IPIDStrategy.UNCLASSIFIED)),
                f"{strategy} after TCP first-round skip",
            )
            mass_detected = classify_batch_mass(
                pa.array(values.astype(np.int64).tolist(), type=pa.list_(pa.int64()))
            )
            self.assertTrue(
                np.all(mass_detected == int(IPIDStrategy.MULTI)),
                strategy,
            )

        fixed_sequences = generate_fixed_sequences(16, np.random.default_rng(3))
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
        fixed_out_of_scope = generate_fixed_out_of_scope_sequences(
            16,
            np.random.default_rng(4),
        )
        self.assertEqual(tuple(fixed_out_of_scope), FIXED_OUT_OF_SCOPE_STRATEGIES)
        fixed_config = MeasurementConfig(
            connection_count=4,
            requests_per_connection=25,
            request_ip_ids=np.asarray([18933, 18932, 3717, 3718, 3719]),
        )
        for strategy, values in fixed_out_of_scope.items():
            self.assertEqual(values.shape, (16, 100))
            detected = classify_batch(values, fixed_config)
            self.assertTrue(
                np.all(detected == int(IPIDStrategy.SINGLE)),
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

    def test_multi_generator_uses_complete_cluster_count_range(self):
        values = _generate_multi_sequences(2_048, 100, np.random.default_rng(5))
        lengths, present, padded = _mass_padded(
            pa.array(values.astype(np.int64).tolist(), type=pa.list_(pa.int64()))
        )
        cluster_counts = _cluster_counts_mass(padded, present, lengths)
        self.assertEqual(
            set(cluster_counts.tolist()),
            set(range(2, MULTI_MAX_CLUSTERS + 1)),
        )

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
            rt_row_count = 2 * TRIVIAL_SAMPLES_PER_STRATEGY + 4 * 8
            fixed_row_count = TRIVIAL_SAMPLES_PER_STRATEGY + 2 * 8
            self.assertEqual(
                table.num_rows,
                rt_row_count + 8 + 3 * fixed_row_count + 8,
            )
            rows = table.to_pylist()
            datasets = {row["DATASET"] for row in rows}
            self.assertEqual(
                datasets,
                {
                    RT_DATASET,
                    RT_OUT_OF_SCOPE_DATASET,
                    FIXED_IDEAL_DATASET,
                    FIXED_OUT_OF_SCOPE_DATASET,
                    FIXED_LOSSY_DATASET,
                    FIXED_REORDERED_DATASET,
                },
            )
            for row in rows:
                if row["DATASET"] in (
                    RT_OUT_OF_SCOPE_DATASET,
                    FIXED_OUT_OF_SCOPE_DATASET,
                ):
                    self.assertNotEqual(row["GENERATOR_STRATEGY"], "UNCLASSIFIED")
                    self.assertEqual(row["EXPECTED_STRATEGY"], "UNCLASSIFIED")
                tokens = row["IPID_SEQUENCE"].split(",")
                expected_length = (
                    16 if row["DATASET"] in (RT_DATASET, RT_OUT_OF_SCOPE_DATASET) else 100
                )
                self.assertEqual(len(tokens), expected_length)
                if row["DATASET"] in (FIXED_LOSSY_DATASET, FIXED_REORDERED_DATASET):
                    self.assertEqual(tokens.count("-"), 20)
                    self.assertEqual(row["LOSS_COUNT"], 20)
                if row["DATASET"] == FIXED_REORDERED_DATASET:
                    self.assertEqual(row["REORDERED_COUNT"], 16)

            rt_report = json.loads(outputs["rt_based_json"].read_text())
            fixed_report = json.loads(outputs["fixed_interval_json"].read_text())
            impaired_report = json.loads(outputs["impaired_json"].read_text())
            out_of_scope_report = json.loads(outputs["out_of_scope_json"].read_text())
            self.assertEqual(
                rt_report["samples_by_dataset_and_strategy"][RT_DATASET],
                {
                    "REFLECTION": TRIVIAL_SAMPLES_PER_STRATEGY,
                    "CONSTANT": TRIVIAL_SAMPLES_PER_STRATEGY,
                    "SINGLE": 8,
                    "PER_CONNECTION": 8,
                    "PER_DESTINATION": 8,
                    "PER_BUCKET": 8,
                },
            )
            self.assertEqual(
                fixed_report["samples_by_dataset_and_strategy"][FIXED_IDEAL_DATASET],
                {
                    "CONSTANT": TRIVIAL_SAMPLES_PER_STRATEGY,
                    "MULTI": 8,
                    "RANDOM": 8,
                },
            )
            self.assertEqual(
                rt_report["samples_by_dataset_and_strategy"][RT_OUT_OF_SCOPE_DATASET],
                {"MULTI": 8},
            )
            self.assertEqual(
                fixed_report["samples_by_dataset_and_strategy"][FIXED_OUT_OF_SCOPE_DATASET],
                {"SINGLE": 8},
            )
            self.assertNotIn(
                "UNCLASSIFIED",
                rt_report["metrics"]["confusion_matrix"]["generated_class_order"],
            )
            self.assertEqual(
                rt_report["metrics"]["confusion_matrix"]["detected_class_order"][-1],
                "UNCLASSIFIED",
            )
            self.assertEqual(
                rt_report["synthetic_generator_parameters"]["SINGLE"]["increment_range_inclusive"],
                [1, MAX_INC],
            )
            self.assertEqual(
                rt_report["synthetic_generator_parameters"]["PER_BUCKET"][
                    "increment_range_inclusive"
                ],
                [1, PER_BUCKET_MAX_INC],
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
            self.assertEqual(
                out_of_scope_report["tests"]["rt_based"]["metrics"]["rejection_rate"],
                1.0,
            )
            fixed_rejection = out_of_scope_report["tests"]["fixed_interval"]["metrics"]
            self.assertEqual(fixed_rejection["sample_count"], 8)
            self.assertEqual(fixed_rejection["expected_output"], "UNCLASSIFIED")
            self.assertEqual(
                sum(fixed_rejection["by_generator"]["SINGLE"]["detected_output_counts"].values()),
                8,
            )


if __name__ == "__main__":
    unittest.main()
