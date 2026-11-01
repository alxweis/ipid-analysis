import unittest
from unittest.mock import patch

import numpy as np

from ipid_analysis.empirical_random_uniformity import gap_uniformity_pvalues
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_GAP_NULL_TABLE_SEED,
    CANDIDATE_NULL_TABLE_SEED,
    CANDIDATE_RANDOM_METRICS,
    CANDIDATE_RANDOM_MIN_SCORE,
    CANDIDATE_RANDOM_SCORE_VERSION,
    CANDIDATE_RANDOM_SELECTION,
    candidate_random_score_components,
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

    def test_unchanged_threshold_is_provisional(self):
        self.assertEqual(RANDOM_STRUCTURE_SCORE_VERSION, CANDIDATE_RANDOM_SCORE_VERSION)
        self.assertEqual(RANDOM_STRUCTURE_MIN_SCORE, CANDIDATE_RANDOM_MIN_SCORE)
        self.assertEqual(CANDIDATE_RANDOM_MIN_SCORE, 7.89999176049605e-05)
        self.assertIn("calibration pending", CANDIDATE_RANDOM_SELECTION)
        self.assertEqual(CANDIDATE_GAP_NULL_TABLE_SEED, CANDIDATE_NULL_TABLE_SEED + 23)

    def test_only_gap_null_tables_are_constructed_and_cached(self):
        production_candidate_null_tables.cache_clear()
        first = production_candidate_null_tables()
        self.assertIs(first, production_candidate_null_tables())
        self.assertFalse(hasattr(first, "increment"))
        self.assertEqual(first.gap.sample_count, 1_000_000)
        self.assertEqual(first.gap.seed, CANDIDATE_GAP_NULL_TABLE_SEED)


if __name__ == "__main__":
    unittest.main()
