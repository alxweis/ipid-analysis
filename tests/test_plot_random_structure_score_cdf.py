import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow.parquet as pq

from ipid_analysis.plot_chi2_pvalue_cdf import (
    PLOT_STRATEGIES,
    TRIVIAL_SAMPLES_PER_STRATEGY,
    generate_chi2_sequences,
)
from ipid_analysis.plot_random_structure_score_cdf import (
    DEFAULT_NULL_TABLE_SAMPLES,
    DEFAULT_RANDOM_FALSE_REJECTION_RATE,
    DEFAULT_STRUCTURE_SAMPLES_PER_STRATEGY,
    MASS_IDEAL_DATASET,
    MASS_LOSSY_DATASET,
    MASS_LOSSY_REORDERED_DATASET,
    MASS_REORDERED_DATASET,
    MIN_COMPATIBILITY_SCORE,
    SCORE_VERSION,
    X_MAJOR_EXPONENT_STEP,
    _floor_only_strategies,
    _log_axis_parameters,
    calculate_scores,
    render,
)
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_NULL_TABLE_VERSION,
    CANDIDATE_RANDOM_METRICS,
    CANDIDATE_RANDOM_MIN_SCORE,
    CANDIDATE_RANDOM_SCORE_VERSION,
)
from ipid_analysis.random_classifier_evaluation import EmpiricalNullTables


class RandomStructureScoreCDFTest(unittest.TestCase):
    def test_default_sample_budget(self):
        self.assertEqual(DEFAULT_STRUCTURE_SAMPLES_PER_STRATEGY, 100_000)
        self.assertEqual(DEFAULT_NULL_TABLE_SAMPLES, 1_000_000)
        self.assertEqual(DEFAULT_RANDOM_FALSE_REJECTION_RATE, 0.0001)

    def test_log_axis_keeps_floor_cdfs_inside_plot(self):
        scores = {strategy: np.array([MIN_COMPATIBILITY_SCORE]) for strategy in PLOT_STRATEGIES}

        axis_minimum, major_ticks, minor_ticks = _log_axis_parameters(scores, 1e-3)

        self.assertEqual(axis_minimum, 1e-21)
        np.testing.assert_array_equal(
            np.log10(major_ticks),
            np.arange(-20, 1, X_MAJOR_EXPONENT_STEP),
        )
        np.testing.assert_array_equal(
            np.log10(minor_ticks),
            np.arange(-21, 0, X_MAJOR_EXPONENT_STEP),
        )

    def test_fully_coincident_floor_strategies_are_identified(self):
        scores = {strategy: np.array([1e-10]) for strategy in PLOT_STRATEGIES}
        scores["CONSTANT"] = np.full(4, MIN_COMPATIBILITY_SCORE)
        scores["PER_CONNECTION"] = np.full(4, MIN_COMPATIBILITY_SCORE)

        self.assertEqual(
            _floor_only_strategies(scores),
            ["CONSTANT", "PER_CONNECTION"],
        )

    def test_score_is_a_finite_probability_like_value(self):
        rng = np.random.default_rng(23)
        values = generate_chi2_sequences(32, rng)["RANDOM"]
        scores = calculate_scores(
            values,
            np.zeros_like(values, dtype=bool),
            EmpiricalNullTables(128, seed=17),
        )

        self.assertTrue(np.all(np.isfinite(scores)))
        self.assertTrue(np.all((scores >= MIN_COMPATIBILITY_SCORE) & (scores <= 1.0)))

    def test_rendered_artifacts_and_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("ipid_analysis.plot_random_structure_score_cdf.configure_paper_style"):
                outputs = render(
                    samples_per_strategy=8,
                    null_table_samples=64,
                    null_table_seed=17,
                    seed=23,
                    processed_root=root / "processed",
                    figures_root=root / "figures",
                )

            for path in outputs:
                self.assertTrue(path.is_file(), path)

            (
                ideal_pdf,
                ideal_json,
                _,
                lossy_json,
                _,
                reordered_json,
                _,
                lossy_reordered_json,
                aggregate,
            ) = outputs
            self.assertEqual(ideal_pdf.suffix, ".pdf")
            table = pq.read_table(aggregate)
            expected_rows = 4 * (2 * TRIVIAL_SAMPLES_PER_STRATEGY + 6 * 8)
            self.assertEqual(table.num_rows, expected_rows)
            self.assertEqual(
                set(table.column("DATASET").to_pylist()),
                {
                    MASS_IDEAL_DATASET,
                    MASS_LOSSY_DATASET,
                    MASS_REORDERED_DATASET,
                    MASS_LOSSY_REORDERED_DATASET,
                },
            )
            self.assertEqual(
                set(table.column("IPID_SELECTION_STRATEGY").to_pylist()),
                set(PLOT_STRATEGIES),
            )

            metadata = json.loads(ideal_json.read_text())
            lossy_metadata = json.loads(lossy_json.read_text())
            reordered_metadata = json.loads(reordered_json.read_text())
            lossy_reordered_metadata = json.loads(lossy_reordered_json.read_text())
            self.assertEqual(metadata["threshold"]["tau"], CANDIDATE_RANDOM_MIN_SCORE)
            self.assertEqual(metadata["threshold"]["tau"], lossy_metadata["threshold"]["tau"])
            self.assertEqual(
                metadata["threshold"]["tau"],
                reordered_metadata["threshold"]["tau"],
            )
            self.assertEqual(
                metadata["threshold"]["tau"],
                lossy_reordered_metadata["threshold"]["tau"],
            )
            self.assertEqual(metadata["score"]["version"], SCORE_VERSION)
            self.assertEqual(SCORE_VERSION, CANDIDATE_RANDOM_SCORE_VERSION)
            self.assertEqual(
                metadata["score"]["components"],
                list(CANDIDATE_RANDOM_METRICS),
            )
            self.assertEqual(metadata["score"]["combiner"], "minimum")
            self.assertEqual(
                metadata["score"]["null_tables"]["version"],
                CANDIDATE_NULL_TABLE_VERSION,
            )
            self.assertEqual(metadata["score"]["null_tables"]["sample_count"], 64)
            self.assertTrue(metadata["score"]["validation_only"])
            self.assertFalse(metadata["score"]["production_classifier_changed"])
            self.assertEqual(metadata["score"]["random_compatible_when"], "S >= tau")
            self.assertNotIn("hard_rejections", metadata["score"])
            self.assertNotIn("hard_rejection_score", metadata["score"])
            self.assertNotIn("cache", metadata["threshold"])


if __name__ == "__main__":
    unittest.main()
