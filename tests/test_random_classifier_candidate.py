import unittest

import numpy as np

from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_RANDOM_METRICS,
    CANDIDATE_RANDOM_MIN_SCORE,
    CandidateRandomScoreComponents,
    candidate_random_score_components,
)
from ipid_analysis.random_classifier_evaluation import EmpiricalNullTables
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
            EmpiricalNullTables(128, seed=9),
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

    def test_candidate_does_not_change_production_constants(self):
        self.assertEqual(RANDOM_STRUCTURE_SCORE_VERSION, "raw-multiset-bounded-v2")
        self.assertEqual(RANDOM_STRUCTURE_MIN_SCORE, 0.000016313656391956604)
        self.assertNotEqual(CANDIDATE_RANDOM_MIN_SCORE, RANDOM_STRUCTURE_MIN_SCORE)


if __name__ == "__main__":
    unittest.main()
