import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.classifier_validation import (
    BASE_REORDERED_3_DATASET,
    BASE_REORDERED_4_DATASET,
    CONFUSION_CELL_HEIGHT_INCHES,
    CONFUSION_CELL_TEXT_DOWNWARD_OFFSET_POINTS,
    CONFUSION_CELL_WIDTH_INCHES,
    CONFUSION_COLORBAR_GAP_INCHES,
    CONFUSION_COLORBAR_WIDTH_INCHES,
    CONFUSION_FIGURE_WIDTH_INCHES,
    CONFUSION_TITLE_GAP_INCHES,
    CONFUSION_VERTICAL_PANEL_GAP_INCHES,
    CONFUSION_XLABEL_GAP_INCHES,
    CONFUSION_YLABEL_GAP_INCHES,
    FIXED_CONFIG,
    FIXED_IDEAL_DATASET,
    FIXED_IMPAIRED_STRATEGIES,
    FIXED_LOSSY_DATASET,
    FIXED_REORDERED_DATASET,
    FIXED_STRATEGIES,
    MASS_REORDERED_DATASET,
    RT_DATASET,
    RT_OUT_OF_SCOPE_DATASET,
    RT_OUT_OF_SCOPE_STRATEGIES,
    RT_STRATEGIES,
    TRIVIAL_SAMPLES_PER_STRATEGY,
    _confusion_metrics,
    _format_matrix_percentage,
    _generate_multi_sequences,
    apply_fixed_interval_impairments,
    apply_reordering,
    generate_fixed_sequences,
    generate_rt_out_of_scope_sequences,
    generate_rt_sequences,
    validate_classifier,
)
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_INCREMENT_BIN_COUNTS,
    CANDIDATE_NULL_TABLE_SEED,
    CANDIDATE_NULL_TABLE_VERSION,
    CANDIDATE_RANDOM_METRICS,
    CANDIDATE_RANDOM_MIN_SCORE,
    CANDIDATE_RANDOM_SCORE_VERSION,
)
from ipid_analysis.strategies import (
    MAX_INC,
    MULTI_MAX_CLUSTERS,
    IPIDStrategy,
    _cluster_counts_mass,
    _mass_padded,
    classify_batch,
    classify_batch_mass,
)


class ClassifierValidationTest(unittest.TestCase):
    def test_confusion_matrix_layout_constants_are_dimensioned_and_stable(self):
        self.assertEqual(CONFUSION_FIGURE_WIDTH_INCHES, 7.0)
        self.assertEqual(CONFUSION_CELL_WIDTH_INCHES * 9, 2.50)
        self.assertEqual(CONFUSION_CELL_HEIGHT_INCHES * 8, 1.40)
        self.assertEqual(CONFUSION_COLORBAR_GAP_INCHES, 0.10)
        self.assertEqual(CONFUSION_COLORBAR_WIDTH_INCHES, 0.065)
        self.assertEqual(CONFUSION_VERTICAL_PANEL_GAP_INCHES, 0.40)
        self.assertEqual(CONFUSION_TITLE_GAP_INCHES, 0.05)
        self.assertEqual(CONFUSION_XLABEL_GAP_INCHES, 0.60)
        self.assertEqual(CONFUSION_YLABEL_GAP_INCHES, 0.93)
        self.assertEqual(CONFUSION_CELL_TEXT_DOWNWARD_OFFSET_POINTS, 0.50)

    def test_small_nonzero_confusion_percentages_are_not_rendered_as_zero(self):
        self.assertEqual(_format_matrix_percentage(0.0), "-")
        self.assertEqual(_format_matrix_percentage(0.001), "<0.1")
        self.assertEqual(_format_matrix_percentage(0.099), "<0.1")
        self.assertEqual(_format_matrix_percentage(0.1), "0.1")
        self.assertEqual(_format_matrix_percentage(12.34), "12.3")

    def test_multiclass_mcc_does_not_overflow_for_production_sized_counts(self):
        expected = ["A"] * 50_000 + ["B"] * 50_000
        detected = ["A"] * 40_000 + ["B"] * 10_000 + ["A"] * 10_000 + ["B"] * 40_000

        metrics = _confusion_metrics(
            expected,
            detected,
            generated_classes=("A", "B"),
            detected_classes=("A", "B"),
        )
        mcc = metrics["multiclass_matthews_correlation_coefficient"]

        self.assertTrue(np.isfinite(mcc))
        self.assertGreaterEqual(mcc, -1.0)
        self.assertLessEqual(mcc, 1.0)
        self.assertAlmostEqual(mcc, 0.6)

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
        self.assertLessEqual(int(bucket_increments.max()), MAX_INC)
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
        fixed_sequences = generate_fixed_sequences(16, np.random.default_rng(3))
        self.assertEqual(tuple(fixed_sequences), FIXED_STRATEGIES)
        for strategy, values in fixed_sequences.items():
            self.assertEqual(values.shape, (16, 100))
            detected = classify_batch_mass(
                pa.array(values.astype(np.int64).tolist(), type=pa.list_(pa.int64())),
                FIXED_CONFIG,
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

        reordered_only = apply_reordering(
            ideal[:, :16],
            np.random.default_rng(4),
            reordered_count=3,
        )
        for row_index in range(len(reordered_only)):
            self.assertEqual(
                sorted(reordered_only[row_index].tolist()),
                sorted(ideal[row_index, :16].tolist()),
            )
            self.assertTrue(np.any(reordered_only[row_index] != ideal[row_index, :16]))

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
            with patch("ipid_analysis.classifier_validation.configure_compact_validation_style"):
                outputs = validate_classifier(
                    samples_per_strategy=8,
                    seed=42,
                    candidate_null_table_samples=64,
                    processed_root=root / "processed",
                    figures_root=root / "figures",
                )

            for path in outputs.values():
                self.assertTrue(path.is_file(), path)
            self.assertEqual(
                {
                    outputs["mass_ideal_pdf"].name,
                    outputs["mass_lossy_vs_reordered_pdf"].name,
                    outputs["mass_lossy_vs_lossy_reordered_pdf"].name,
                },
                {
                    "mass-4x25-classifier-confusion-ideal.pdf",
                    "mass-4x25-classifier-confusion-lossy-vs-reordered.pdf",
                    "mass-4x25-classifier-confusion-lossy-vs-lossy-reordered.pdf",
                },
            )

            table = pq.read_table(outputs["dataset"])
            rt_row_count = 2 * TRIVIAL_SAMPLES_PER_STRATEGY + 4 * 8
            fixed_ideal_row_count = 2 * TRIVIAL_SAMPLES_PER_STRATEGY + 6 * 8
            fixed_impaired_row_count = fixed_ideal_row_count
            self.assertEqual(
                table.num_rows,
                3 * rt_row_count + 2 * 8 + 4 * fixed_impaired_row_count,
            )
            rows = table.to_pylist()
            datasets = {row["DATASET"] for row in rows}
            self.assertEqual(
                datasets,
                {
                    RT_DATASET,
                    BASE_REORDERED_3_DATASET,
                    BASE_REORDERED_4_DATASET,
                    RT_OUT_OF_SCOPE_DATASET,
                    FIXED_IDEAL_DATASET,
                    FIXED_LOSSY_DATASET,
                    MASS_REORDERED_DATASET,
                    FIXED_REORDERED_DATASET,
                },
            )
            for row in rows:
                if row["DATASET"] == RT_OUT_OF_SCOPE_DATASET:
                    self.assertNotEqual(row["GENERATOR_STRATEGY"], "UNCLASSIFIED")
                    self.assertEqual(row["EXPECTED_STRATEGY"], "UNCLASSIFIED")
                tokens = row["IPID_SEQUENCE"].split(",")
                expected_length = (
                    16
                    if row["DATASET"]
                    in (
                        RT_DATASET,
                        BASE_REORDERED_3_DATASET,
                        BASE_REORDERED_4_DATASET,
                        RT_OUT_OF_SCOPE_DATASET,
                    )
                    else 100
                )
                self.assertEqual(len(tokens), expected_length)
                if row["DATASET"] in (FIXED_LOSSY_DATASET, FIXED_REORDERED_DATASET):
                    self.assertEqual(tokens.count("-"), 20)
                    self.assertEqual(row["LOSS_COUNT"], 20)
                if row["DATASET"] == FIXED_REORDERED_DATASET:
                    self.assertEqual(row["REORDERED_COUNT"], 16)
                if row["DATASET"] == MASS_REORDERED_DATASET:
                    self.assertEqual(row["REORDERED_COUNT"], 20)
                if row["DATASET"] == BASE_REORDERED_3_DATASET:
                    self.assertEqual(row["REORDERED_COUNT"], 3)
                if row["DATASET"] == BASE_REORDERED_4_DATASET:
                    self.assertEqual(row["REORDERED_COUNT"], 4)

            base_3_report = json.loads(outputs["base_reordered_3_json"].read_text())
            base_4_report = json.loads(outputs["base_reordered_4_json"].read_text())
            mass_report = json.loads(outputs["mass_json"].read_text())
            out_of_scope_report = json.loads(outputs["out_of_scope_json"].read_text())
            self.assertEqual(
                base_3_report["samples_by_dataset_and_strategy"][RT_DATASET],
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
                mass_report["samples_by_dataset_and_strategy"][FIXED_IDEAL_DATASET],
                {
                    "REFLECTION": TRIVIAL_SAMPLES_PER_STRATEGY,
                    "CONSTANT": TRIVIAL_SAMPLES_PER_STRATEGY,
                    "SINGLE": 8,
                    "PER_CONNECTION": 8,
                    "PER_DESTINATION": 8,
                    "PER_BUCKET": 8,
                    "MULTI": 8,
                    "RANDOM": 8,
                },
            )
            random_score = mass_report["random_structure_score"]
            self.assertEqual(random_score["version"], CANDIDATE_RANDOM_SCORE_VERSION)
            self.assertEqual(random_score["threshold"], CANDIDATE_RANDOM_MIN_SCORE)
            self.assertEqual(random_score["metrics"], list(CANDIDATE_RANDOM_METRICS))
            self.assertEqual(random_score["combiner"], "minimum")
            self.assertEqual(
                random_score["null_tables"]["version"],
                CANDIDATE_NULL_TABLE_VERSION,
            )
            self.assertEqual(random_score["null_tables"]["sample_count"], 64)
            self.assertEqual(
                random_score["increment_uniformity"]["bin_counts"],
                list(CANDIDATE_INCREMENT_BIN_COUNTS),
            )
            self.assertEqual(
                random_score["null_tables"]["component_seeds"],
                {
                    "increment_uniformity": CANDIDATE_NULL_TABLE_SEED + 1,
                    "gap_uniformity": CANDIDATE_NULL_TABLE_SEED + 2,
                },
            )
            self.assertTrue(random_score["validation_only"])
            self.assertFalse(random_score["production_classifier_changed"])
            self.assertEqual(
                base_3_report["samples_by_dataset_and_strategy"][RT_OUT_OF_SCOPE_DATASET],
                {"MULTI": 8, "RANDOM": 8},
            )
            self.assertEqual(
                mass_report["datasets"]["lossy"]["metrics"]["confusion_matrix"][
                    "generated_class_order"
                ],
                list(FIXED_IMPAIRED_STRATEGIES),
            )
            self.assertNotIn(
                "UNCLASSIFIED",
                base_3_report["datasets"]["ideal"]["metrics"]["confusion_matrix"][
                    "generated_class_order"
                ],
            )
            self.assertEqual(
                base_3_report["datasets"]["ideal"]["metrics"]["confusion_matrix"][
                    "detected_class_order"
                ][-1],
                "UNCLASSIFIED",
            )
            self.assertEqual(
                base_3_report["synthetic_generator_parameters"]["SINGLE"][
                    "increment_range_inclusive"
                ],
                [1, MAX_INC],
            )
            self.assertEqual(
                base_3_report["synthetic_generator_parameters"]["PER_BUCKET"][
                    "increment_range_inclusive"
                ],
                [1, MAX_INC],
            )
            self.assertEqual(base_3_report["datasets"]["ideal"]["metrics"]["accuracy"], 1.0)
            self.assertEqual(base_4_report["datasets"]["ideal"]["metrics"]["accuracy"], 1.0)
            self.assertEqual(mass_report["datasets"]["ideal"]["metrics"]["macro"]["f1"], 1.0)
            self.assertEqual(
                out_of_scope_report["tests"]["base"]["metrics"]["rejection_rate"],
                1.0,
            )
            self.assertEqual(set(out_of_scope_report["tests"]), {"base"})


if __name__ == "__main__":
    unittest.main()
