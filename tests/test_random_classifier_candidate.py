import unittest

import numpy as np

from ipid_analysis.increment_bin_rule_evaluation import (
    BIN_RULE_BY_NAME,
    IncrementBinNullTables,
    increment_view_pvalues_for_rule,
)
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_GAP_NULL_TABLE_SEED,
    CANDIDATE_INCREMENT_BIN_COUNTS,
    CANDIDATE_INCREMENT_NULL_TABLE_SEED,
    CANDIDATE_NULL_TABLE_SEED,
    CANDIDATE_RANDOM_METRICS,
    CANDIDATE_RANDOM_MIN_SCORE,
    CANDIDATE_RANDOM_SCORE_VERSION,
    CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE,
    CandidateRandomScoreComponents,
    candidate_random_score_components,
    create_candidate_null_tables,
)
from ipid_analysis.random_classifier_view_evaluation import (
    _increment_valid_views,
    hierarchical_score,
)
from ipid_analysis.strategies import (
    RANDOM_STRUCTURE_MIN_SCORE,
    RANDOM_STRUCTURE_SCORE_VERSION,
)


class RandomClassifierCandidateTest(unittest.TestCase):
    def test_candidate_score_is_minimum_of_declared_components(self):
        rng = np.random.default_rng(7)
        values = rng.integers(0, 1 << 16, size=(8, 100), dtype=np.uint16)
        present = np.ones_like(values, dtype=bool)
        components = candidate_random_score_components(
            values,
            present,
            create_candidate_null_tables(128, seed=9),
        )

        self.assertIsInstance(components, CandidateRandomScoreComponents)
        self.assertEqual(
            CANDIDATE_RANDOM_METRICS,
            ("raw_uniformity", "increment_uniformity", "gap_uniformity"),
        )
        np.testing.assert_allclose(
            components.score,
            np.minimum.reduce(
                [
                    components.raw_uniformity,
                    components.increment_uniformity,
                    components.gap_uniformity,
                ]
            ),
        )

    def test_candidate_increment_component_matches_confirmed_evidence_aggregation(self):
        rng = np.random.default_rng(17)
        values = rng.integers(0, 1 << 16, size=(8, 100), dtype=np.uint16)
        present = rng.random(values.shape) >= 0.20
        components = candidate_random_score_components(
            values,
            present,
            create_candidate_null_tables(256, seed=9),
        )
        rule = BIN_RULE_BY_NAME["multiscale-3-4-8-16"]
        views = increment_view_pvalues_for_rule(
            values,
            present,
            rule,
            IncrementBinNullTables(256, seed=20),
        )
        expected = hierarchical_score(views, _increment_valid_views(present, rule))

        self.assertEqual(CANDIDATE_INCREMENT_BIN_COUNTS, (3, 4, 8, 16))
        self.assertEqual(
            CANDIDATE_RANDOM_SCORE_VERSION,
            "raw-multiscale-increment-evidence-gap-min-v3",
        )
        self.assertEqual(CANDIDATE_RANDOM_MIN_SCORE, 7.89999176049605e-05)
        self.assertEqual(CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE, 0.0005)
        np.testing.assert_array_equal(components.increment_uniformity, expected)

    def test_candidate_null_table_seeds_match_confirmation_evaluator(self):
        self.assertEqual(CANDIDATE_INCREMENT_NULL_TABLE_SEED, CANDIDATE_NULL_TABLE_SEED + 11)
        self.assertEqual(CANDIDATE_GAP_NULL_TABLE_SEED, CANDIDATE_NULL_TABLE_SEED + 23)

    def test_candidate_does_not_change_production_constants(self):
        self.assertEqual(RANDOM_STRUCTURE_SCORE_VERSION, "raw-multiset-bounded-v2")
        self.assertEqual(RANDOM_STRUCTURE_MIN_SCORE, 0.000016313656391956604)
        self.assertNotEqual(CANDIDATE_RANDOM_MIN_SCORE, RANDOM_STRUCTURE_MIN_SCORE)


if __name__ == "__main__":
    unittest.main()
