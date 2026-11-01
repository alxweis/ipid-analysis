import unittest

import numpy as np
from scipy.stats import chi2, chisquare

from ipid_analysis.increment_uniformity import (
    _view_pvalues,
    increment_uniformity_pvalues,
    increment_uniformity_view_pvalues,
)
from ipid_analysis.strategies import MeasurementConfig


class IncrementUniformityTest(unittest.TestCase):
    def setUp(self):
        self.cfg = MeasurementConfig(4, 25, np.array([1, 2, 3, 4]))

    def test_bin_boundaries_and_cap(self):
        for count, expected in (
            (0, 0),
            (14, 0),
            (15, 3),
            (19, 3),
            (20, 4),
            (79, 15),
            (80, 16),
            (99, 16),
        ):
            with self.subTest(count=count):
                values = np.zeros((1, count + 1), dtype=np.int64)
                actual, valid = _view_pvalues(values, np.ones_like(values, bool))
                self.assertEqual(bool(valid[0]), expected != 0)
                if expected:
                    first_bin_probability = np.ceil(65536 / expected) / 65536
                    statistic = count * (1 - first_bin_probability) / first_bin_probability
                    self.assertAlmostEqual(actual[0], chi2.sf(statistic, expected - 1))
                else:
                    self.assertEqual(actual[0], 1)

    def test_wraparound_and_unequal_bin_probabilities_match_scipy(self):
        increments = np.array([0] * 5 + [21846] * 6 + [43691] * 4)
        values = np.concatenate(([65535], (65535 + np.cumsum(increments)) % 65536))
        values = values.astype(np.uint16)[None, :]
        actual, valid = _view_pvalues(values, np.ones_like(values, dtype=bool))
        expected = chisquare([5, 6, 4], 15 * np.array([21846, 21845, 21845]) / 65536).pvalue
        self.assertTrue(valid[0])
        self.assertAlmostEqual(actual[0], expected, places=14)

    def test_all_seven_views_match_individual_pearson_tests(self):
        values = np.random.default_rng(9).integers(0, 65536, size=(2, 100), dtype=np.uint16)
        pvalues, valid = increment_uniformity_view_pvalues(
            values, np.ones_like(values, bool), self.cfg
        )
        self.assertTrue(valid.all())
        for row in range(2):
            views = [values[row]] + [values[row, i::2] for i in range(2)]
            views += [values[row, i::4] for i in range(4)]
            for column, (view, bins) in enumerate(zip(views, (16, 9, 9, 4, 4, 4, 4))):
                increments = np.diff(view.astype(np.int64)) % 65536
                edges = np.ceil(np.linspace(0, 65536, bins + 1)).astype(int)
                counts, _ = np.histogram(increments, edges)
                expected = len(increments) * np.diff(edges) / 65536
                self.assertAlmostEqual(pvalues[row, column], chisquare(counts, expected).pvalue)
        np.testing.assert_array_equal(
            increment_uniformity_pvalues(values, np.ones_like(values, bool), self.cfg),
            pvalues.min(axis=1),
        )

    def test_missing_slots_are_not_bridged_and_views_use_own_counts(self):
        values = np.arange(100)[None, :]
        present = np.zeros_like(values, dtype=bool)
        present[:, ::2] = True
        pvalues, valid = increment_uniformity_view_pvalues(values, present, self.cfg)
        np.testing.assert_array_equal(valid[0], [False, True, False, True, False, True, False])
        self.assertEqual(pvalues[0, 0], 1.0)
        present[:] = False
        present[:, :60:4] = True
        _, valid = increment_uniformity_view_pvalues(values, present, self.cfg)
        self.assertFalse(valid.any())
        present[:, 60] = True
        _, valid = increment_uniformity_view_pvalues(values, present, self.cfg)
        np.testing.assert_array_equal(valid[0], [False, False, False, True, False, False, False])

    def test_unavailable_views_are_neutral_and_empty_batches_work(self):
        values = np.zeros((1, 100), dtype=np.int64)
        self.assertEqual(
            increment_uniformity_pvalues(values, np.zeros_like(values, bool), self.cfg)[0], 1
        )
        result = increment_uniformity_pvalues(
            np.empty((0, 100)), np.empty((0, 100), bool), self.cfg
        )
        self.assertEqual(result.shape, (0,))

    def test_rejects_wrong_layout_and_mask(self):
        values = np.zeros((1, 100))
        with self.assertRaises(ValueError):
            increment_uniformity_pvalues(values, np.ones((1, 99), bool), self.cfg)
        with self.assertRaises(ValueError):
            increment_uniformity_pvalues(values[:, :99], np.ones((1, 99), bool), self.cfg)
        with self.assertRaises(ValueError):
            increment_uniformity_pvalues(
                values, np.ones_like(values, bool), MeasurementConfig(4, 4, np.array([1]))
            )
