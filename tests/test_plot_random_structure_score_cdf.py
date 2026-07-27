import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow.parquet as pq

from ipid_analysis.plot_chi2_pvalue_cdf import (
    IDEAL_DATASET,
    IDEAL_SEQUENCE_LENGTH,
    LOSSY_DATASET,
    LOSSY_REORDERED_DATASET,
    PLOT_STRATEGIES,
    TRIVIAL_SAMPLES_PER_STRATEGY,
    generate_chi2_sequences,
)
from ipid_analysis.plot_random_structure_score_cdf import (
    DEFAULT_RANDOM_FALSE_REJECTION_RATE,
    DEFAULT_STRUCTURE_SAMPLES_PER_STRATEGY,
    DEFAULT_THRESHOLD_SAMPLES,
    RawFeatures,
    SCORE_VERSION,
    UNIFORMITY_BINS,
    _log_axis_parameters,
    _positive_ecdf_coordinates,
    _zero_percentages,
    bounded_increment_pvalues,
    calculate_raw_features,
    calculate_scores,
    load_or_calibrate_threshold,
    render,
)


class RandomStructureScoreCDFTest(unittest.TestCase):
    def test_default_sample_budget(self):
        self.assertEqual(DEFAULT_STRUCTURE_SAMPLES_PER_STRATEGY, 10_000)
        self.assertEqual(DEFAULT_THRESHOLD_SAMPLES, 10_000)
        self.assertEqual(DEFAULT_RANDOM_FALSE_REJECTION_RATE, 0.01)

    def test_log_axis_ignores_zero_scores(self):
        scores = {
            strategy: np.array([0.0])
            for strategy in PLOT_STRATEGIES
        }
        scores["RANDOM"] = np.array([0.0, 1e-5])

        axis_minimum, major_ticks, _ = _log_axis_parameters(scores, 1e-3)

        self.assertEqual(axis_minimum, 1e-6)
        self.assertEqual(major_ticks[0], 1e-6)

    def test_zero_mass_and_positive_cdf_are_kept_separate(self):
        scores = {
            strategy: np.array([1e-10])
            for strategy in PLOT_STRATEGIES
        }
        scores["CONSTANT"] = np.array([0.0, 0.0, 1e-4, 1e-3])

        self.assertEqual(_zero_percentages(scores)["CONSTANT"], 50.0)
        x_values, percentages = _positive_ecdf_coordinates(scores["CONSTANT"])
        np.testing.assert_array_equal(x_values, np.array([1e-4, 1e-4, 1e-3]))
        np.testing.assert_array_equal(percentages, np.array([50.0, 75.0, 100.0]))

    def test_features_reuse_one_sorted_multiset(self):
        values = np.zeros((2, IDEAL_SEQUENCE_LENGTH), dtype=np.uint16)
        values[0] = 7
        values[1, :40] = np.arange(40)
        values[1, 40:80] = np.arange(10_000, 10_040)
        mask = np.zeros_like(values, dtype=bool)
        mask[1, 80:] = True

        features = calculate_raw_features(values, mask)

        self.assertEqual(features.unique_count.tolist(), [1, 80])
        self.assertEqual(features.sample_count.tolist(), [100, 80])
        self.assertEqual(features.maximum_gap.shape, (2,))

    def test_raw_features_are_invariant_to_reordering(self):
        rng = np.random.default_rng(17)
        values = rng.integers(
            0,
            1 << 16,
            size=(32, IDEAL_SEQUENCE_LENGTH),
            dtype=np.uint16,
        )
        mask = np.zeros_like(values, dtype=bool)
        mask[:, -20:] = True
        reordered = values.copy()
        for row in range(len(reordered)):
            reordered[row, :80] = rng.permutation(reordered[row, :80])

        original = calculate_raw_features(values, mask)
        shuffled = calculate_raw_features(reordered, mask)
        for field in (
            "sample_count",
            "unique_count",
            "maximum_gap",
            "uniformity_pvalue",
            "occupancy_pvalue",
            "maximum_gap_pvalue",
        ):
            np.testing.assert_array_equal(
                getattr(original, field),
                getattr(shuffled, field),
            )

    def test_score_has_no_strategy_or_sample_count_hard_gate(self):
        values = np.zeros((1, IDEAL_SEQUENCE_LENGTH), dtype=np.uint16)
        mask = np.ones_like(values, dtype=bool)
        mask[:, 0] = False
        features = RawFeatures(
            sample_count=np.array([1]),
            unique_count=np.array([1]),
            maximum_gap=np.array([0]),
            uniformity_pvalue=np.array([0.4]),
            occupancy_pvalue=np.array([0.3]),
            maximum_gap_pvalue=np.array([0.2]),
        )

        with (
            patch(
                "ipid_analysis.plot_random_structure_score_cdf.calculate_raw_features",
                return_value=features,
            ),
            patch(
                "ipid_analysis.plot_random_structure_score_cdf.bounded_increment_pvalues",
                return_value=np.array([0.5]),
            ),
        ):
            score = calculate_scores(values, mask)

        np.testing.assert_array_equal(score, np.array([0.2]))

    def test_bounded_increment_component_detects_counter(self):
        values = np.arange(IDEAL_SEQUENCE_LENGTH, dtype=np.uint16)[None, :]
        pvalue = bounded_increment_pvalues(
            values,
            np.zeros_like(values, dtype=bool),
        )[0]

        self.assertLess(pvalue, 1e-20)

    def test_score_is_a_finite_probability_like_value(self):
        rng = np.random.default_rng(23)
        values = generate_chi2_sequences(32, rng)["RANDOM"]
        scores = calculate_scores(values, np.zeros_like(values, dtype=bool))

        self.assertTrue(np.all(np.isfinite(scores)))
        self.assertTrue(np.all((scores >= 0.0) & (scores <= 1.0)))

    def test_threshold_calibration_is_cached(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "calibration.json"
            first = load_or_calibrate_threshold(
                cache,
                sample_count=128,
                false_rejection_rate=0.05,
                seed=42,
            )
            second = load_or_calibrate_threshold(
                cache,
                sample_count=128,
                false_rejection_rate=0.05,
                seed=42,
            )

            self.assertFalse(first[3])
            self.assertTrue(second[3])
            self.assertEqual(first[:3], second[:3])

    def test_rendered_artifacts_and_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("ipid_analysis.plot_random_structure_score_cdf.configure_paper_style"):
                outputs = render(
                    samples_per_strategy=8,
                    threshold_samples=64,
                    false_rejection_rate=0.05,
                    seed=23,
                    processed_root=root / "processed",
                    figures_root=root / "figures",
                )

            for path in outputs:
                self.assertTrue(path.is_file(), path)

            ideal_pdf, ideal_json, _, lossy_json, _, reordered_json, aggregate = outputs
            self.assertEqual(ideal_pdf.suffix, ".pdf")
            table = pq.read_table(aggregate)
            expected_rows = 3 * (2 * TRIVIAL_SAMPLES_PER_STRATEGY + 6 * 8)
            self.assertEqual(table.num_rows, expected_rows)
            self.assertEqual(
                set(table.column("DATASET").to_pylist()),
                {IDEAL_DATASET, LOSSY_DATASET, LOSSY_REORDERED_DATASET},
            )
            self.assertEqual(
                set(table.column("IPID_SELECTION_STRATEGY").to_pylist()),
                set(PLOT_STRATEGIES),
            )

            metadata = json.loads(ideal_json.read_text())
            lossy_metadata = json.loads(lossy_json.read_text())
            reordered_metadata = json.loads(reordered_json.read_text())
            self.assertGreater(metadata["threshold"]["tau"], 0.0)
            self.assertEqual(metadata["threshold"]["tau"], lossy_metadata["threshold"]["tau"])
            self.assertEqual(
                metadata["threshold"]["tau"],
                reordered_metadata["threshold"]["tau"],
            )
            self.assertEqual(metadata["score"]["version"], SCORE_VERSION)
            self.assertEqual(metadata["score"]["uniformity_bins"], UNIFORMITY_BINS)
            self.assertEqual(metadata["score"]["sorts_per_sequence"], 1)
            self.assertTrue(metadata["score"]["raw_components_reordering_invariant"])
            self.assertEqual(metadata["score"]["random_compatible_when"], "S >= tau")
            self.assertEqual(
                metadata["score"]["range"],
                "[0, 1] without a positive score floor",
            )
            self.assertIn(
                "zero_count",
                metadata["summary_by_strategy"]["CONSTANT"],
            )
            self.assertNotIn("hard_rejections", metadata["score"])
            self.assertNotIn("hard_rejection_score", metadata["score"])
            self.assertTrue(Path(metadata["threshold"]["cache"]).is_file())


if __name__ == "__main__":
    unittest.main()
