import csv
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import numpy as np

from ipid_analysis.empirical_random_uniformity import gap_uniformity_pvalues
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_GAP_NULL_TABLE_SEED,
    CANDIDATE_NULL_TABLE_SAMPLES,
    CANDIDATE_NULL_TABLE_SEED,
    CANDIDATE_NULL_TABLE_VERSION,
    CANDIDATE_RANDOM_CALIBRATION_REPORT,
    CANDIDATE_RANDOM_METRICS,
    CANDIDATE_RANDOM_MIN_SCORE,
    CANDIDATE_RANDOM_SCORE_VERSION,
    CANDIDATE_RANDOM_SELECTION,
    CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE,
    CANDIDATE_RANDOM_THRESHOLD_STATUS,
    candidate_random_score_components,
    candidate_random_threshold_status,
    create_candidate_null_tables,
    production_candidate_null_tables,
)
from ipid_analysis.strategies import (
    RANDOM_STRUCTURE_MIN_SCORE,
    RANDOM_STRUCTURE_SCORE_VERSION,
    MeasurementConfig,
)


class RandomClassifierCandidateTest(unittest.TestCase):
    def setUp(self):
        self.cfg = MeasurementConfig(4, 25, np.array([1, 2, 3, 4]))

    def test_score_uses_only_increment_and_gap_minimum(self):
        values = np.zeros((3, 100), dtype=np.int64)
        with (
            patch(
                "ipid_analysis.random_classifier_candidate.increment_uniformity_pvalues",
                return_value=np.array([0.2, 0.9, 0.3]),
            ),
            patch(
                "ipid_analysis.random_classifier_candidate.gap_uniformity_pvalues",
                return_value=np.array([0.8, 0.1, 0.3]),
            ),
            patch(
                "ipid_analysis.strategies.random_structure_features",
                side_effect=AssertionError("raw component must not run"),
            ),
        ):
            components = candidate_random_score_components(
                values,
                np.ones_like(values, bool),
                create_candidate_null_tables(8),
                config=self.cfg,
            )
        self.assertEqual(CANDIDATE_RANDOM_METRICS, ("increment_uniformity", "gap_uniformity"))
        self.assertFalse(hasattr(components, "raw_uniformity"))
        np.testing.assert_array_equal(components.score, [0.2, 0.1, 0.3])

    def test_gap_is_unchanged_including_duplicates_and_missing_values(self):
        values = np.random.default_rng(7).integers(0, 65536, size=(2, 100), dtype=np.uint16)
        values[:, 1] = values[:, 0]
        present = np.ones_like(values, bool)
        present[1, ::5] = False
        tables = create_candidate_null_tables(128, seed=9)
        components = candidate_random_score_components(values, present, tables, config=self.cfg)
        np.testing.assert_array_equal(
            components.gap_uniformity, gap_uniformity_pvalues(values, present, tables.gap)
        )
        np.testing.assert_array_equal(
            components.score,
            np.minimum(components.increment_uniformity, components.gap_uniformity),
        )

    def test_threshold_matches_recorded_calibration_and_independent_evaluation(self):
        self.assertEqual(RANDOM_STRUCTURE_SCORE_VERSION, CANDIDATE_RANDOM_SCORE_VERSION)
        self.assertEqual(RANDOM_STRUCTURE_MIN_SCORE, CANDIDATE_RANDOM_MIN_SCORE)
        self.assertEqual(CANDIDATE_RANDOM_MIN_SCORE, 4.0383019016847365e-05)
        self.assertIn("seed-20261102", CANDIDATE_RANDOM_SELECTION)
        self.assertEqual(CANDIDATE_GAP_NULL_TABLE_SEED, CANDIDATE_NULL_TABLE_SEED + 23)
        report = Path(__file__).resolve().parents[1] / CANDIDATE_RANDOM_CALIBRATION_REPORT
        metadata = json.loads(report.read_text())
        point = metadata["calibration"][CANDIDATE_RANDOM_SCORE_VERSION]
        self.assertEqual(point["threshold"], CANDIDATE_RANDOM_MIN_SCORE)
        self.assertEqual(point["threshold"], min(point["threshold_by_condition"].values()))
        self.assertEqual(
            metadata["target_random_false_rejection_rate"],
            CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE,
        )
        production = metadata["production_score"]
        self.assertEqual(production["score_version"], CANDIDATE_RANDOM_SCORE_VERSION)
        self.assertEqual(production["gap_null_table_seed"], CANDIDATE_GAP_NULL_TABLE_SEED)
        self.assertEqual(production["gap_null_table_samples"], CANDIDATE_NULL_TABLE_SAMPLES)
        self.assertEqual(production["gap_null_table_version"], CANDIDATE_NULL_TABLE_VERSION)
        with (report.parent / "operating-point-by-scenario.csv").open(newline="") as handle:
            rows = [
                row
                for row in csv.DictReader(handle)
                if row["generator_strategy"] == "RANDOM"
                and float(row["target_random_false_rejection_rate"])
                == CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE
            ]
        self.assertEqual(len(rows), 8)
        self.assertTrue(all(float(row["threshold"]) == CANDIDATE_RANDOM_MIN_SCORE for row in rows))

    def test_calibration_status_requires_matching_threshold_and_null_reference(self):
        parameters = [
            CANDIDATE_RANDOM_MIN_SCORE,
            CANDIDATE_NULL_TABLE_SAMPLES,
            CANDIDATE_NULL_TABLE_SEED,
        ]
        self.assertEqual(CANDIDATE_RANDOM_THRESHOLD_STATUS, "empirically-calibrated")
        self.assertEqual(
            candidate_random_threshold_status(*parameters), CANDIDATE_RANDOM_THRESHOLD_STATUS
        )
        for index, value in enumerate((0.01, 128, 42)):
            changed = parameters.copy()
            changed[index] = value
            self.assertEqual(candidate_random_threshold_status(*changed), "custom-uncalibrated")

    def test_only_gap_null_tables_are_constructed_and_cached(self):
        production_candidate_null_tables.cache_clear()
        first = production_candidate_null_tables()
        self.assertIs(first, production_candidate_null_tables())
        self.assertFalse(hasattr(first, "increment"))
        self.assertEqual(first.gap.sample_count, 1_000_000)
        self.assertEqual(first.gap.seed, CANDIDATE_GAP_NULL_TABLE_SEED)


if __name__ == "__main__":
    unittest.main()
