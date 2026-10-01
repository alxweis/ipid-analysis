from pathlib import Path
import tempfile
import unittest

import numpy as np

from ipid_analysis.classifier_validation import FIXED_CONFIG
from ipid_analysis.increment_bin_rule_evaluation import (
    IncrementBinNullTables,
    IncrementBinRule,
    increment_uniformity_pvalues_for_rule,
    increment_view_pvalues_for_rule,
    selected_bin_counts,
)
from ipid_analysis.random_classifier_evaluation import EmpiricalNullTables
from ipid_analysis.random_classifier_view_evaluation import (
    ALL_CANDIDATES,
    BASELINE_NAME,
    CORE_CONDITIONS,
    EvaluationPreset,
    _calibrate,
    _evaluate,
    _summaries,
    fisher_compatibility,
    gap_view_pvalues,
    hierarchical_score,
)


class RandomClassifierViewEvaluationTest(unittest.TestCase):
    def test_single_scale_selects_exactly_one_resolution(self):
        rule = IncrementBinRule(
            "test-single",
            "single",
            3,
            bin_counts=(3, 4, 8, 16),
            minimum_transition_count=9,
        )
        self.assertEqual(selected_bin_counts(rule, 9), (3,))
        self.assertEqual(selected_bin_counts(rule, 15), (4,))
        self.assertEqual(selected_bin_counts(rule, 24), (8,))
        self.assertEqual(selected_bin_counts(rule, 60), (16,))

    def test_increment_views_preserve_established_minimum(self):
        rng = np.random.default_rng(1)
        values = rng.integers(
            0,
            65536,
            size=(12, FIXED_CONFIG.sequence_length),
            dtype=np.uint16,
        )
        present = np.ones_like(values, dtype=bool)
        present[:, 11] = False
        rule = IncrementBinRule(
            "test-multiscale",
            "multiscale",
            2,
            bin_counts=(3, 4, 8, 16),
        )
        tables = IncrementBinNullTables(500, 2)
        views = increment_view_pvalues_for_rule(values, present, rule, tables)
        established = increment_uniformity_pvalues_for_rule(values, present, rule, tables)
        self.assertEqual(views.shape, (12, 7))
        np.testing.assert_allclose(established, views.min(axis=1))

    def test_fisher_aggregation_accumulates_moderate_evidence(self):
        pvalues = np.asarray([[0.05, 0.05, 0.05, 0.05], [0.01, 1.0, 1.0, 1.0]])
        valid = np.ones_like(pvalues, dtype=bool)
        combined = fisher_compatibility(pvalues, valid)
        self.assertLess(combined[0], 0.05)
        self.assertGreater(combined[1], 0.01)

    def test_hierarchical_score_combines_only_within_view_families(self):
        pvalues = np.asarray([[0.8, 0.05, 0.05, 0.2, 0.2, 0.2, 0.2]])
        valid = np.ones_like(pvalues, dtype=bool)
        expected = min(
            0.8,
            fisher_compatibility(pvalues[:, 1:3], valid[:, 1:3])[0],
            fisher_compatibility(pvalues[:, 3:7], valid[:, 3:7])[0],
        )
        self.assertAlmostEqual(hierarchical_score(pvalues, valid)[0], expected)

    def test_gap_views_are_order_invariant_within_each_view(self):
        values = np.tile(np.arange(FIXED_CONFIG.sequence_length, dtype=np.uint16), (2, 1))
        present = np.ones_like(values, dtype=bool)
        reordered = values.copy()
        # Swapping complete request rounds preserves each connection's multiset.
        first = reordered[:, :4].copy()
        reordered[:, :4] = reordered[:, 4:8]
        reordered[:, 4:8] = first
        tables = EmpiricalNullTables(500, 3)
        original = gap_view_pvalues(values, present, tables)
        changed = gap_view_pvalues(reordered, present, tables)
        np.testing.assert_allclose(original, changed)

    def test_primary_conditions_use_global_practical_impairments(self):
        self.assertEqual(
            [condition.name for condition in CORE_CONDITIONS],
            ["ideal", "lossy", "reordered", "lossy-reordered"],
        )
        self.assertEqual(CORE_CONDITIONS[2].reorder_fraction, 0.20)
        self.assertEqual(CORE_CONDITIONS[3].loss_fraction, 0.20)
        self.assertIn("multiscale-e2", BASELINE_NAME)
        self.assertEqual(len(ALL_CANDIDATES), 63)
        self.assertTrue(any(candidate.name == "raw-only" for candidate in ALL_CANDIDATES))

    def test_tiny_end_to_end_run_writes_reusable_view_scores(self):
        candidates = tuple(
            candidate
            for candidate in ALL_CANDIDATES
            if candidate.name
            in {
                BASELINE_NAME,
                "raw+multiscale-e2:inc-aggregate:gap-aggregate",
            }
        )
        increment_tables = IncrementBinNullTables(100, 7)
        spacing_tables = EmpiricalNullTables(100, 8)
        calibrated = _calibrate(
            8,
            4,
            9,
            candidates,
            increment_tables,
            spacing_tables,
        )
        preset = EvaluationPreset(2, 2, 8, 100, 2)
        with tempfile.TemporaryDirectory() as directory:
            score_path = Path(directory) / "view-scores.pq"
            details, catches = _evaluate(
                preset,
                9,
                candidates,
                calibrated,
                increment_tables,
                spacing_tables,
                score_path,
            )
            self.assertTrue(score_path.exists())
            self.assertGreater(score_path.stat().st_size, 0)
            self.assertTrue(details)
            self.assertTrue(catches)
            summaries = _summaries(
                details,
                calibrated,
                {candidate.name: 1.0 for candidate in candidates},
                candidates,
            )
            self.assertEqual(len(summaries), 2 * len(candidates))


if __name__ == "__main__":
    unittest.main()
