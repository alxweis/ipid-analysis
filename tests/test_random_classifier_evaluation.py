import csv
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

import numpy as np
import pyarrow.parquet as pq

from ipid_analysis.random_classifier_evaluation import (
    CONDITIONS,
    EVALUATION_GENERATORS,
    METRIC_NAMES,
    EmpiricalNullTables,
    ImpairmentCondition,
    _increment_view_pvalues,
    _raw_uniformity_pvalues,
    apply_impairment,
    evaluate_random_classifier_metrics,
    gap_uniformity_pvalues,
)
from ipid_analysis.strategies import random_structure_features


class RecordingIncrementNull:
    def __init__(self):
        self.keys = []

    def increment(self, transition_count: int, bin_count: int) -> np.ndarray:
        self.keys.append((transition_count, bin_count))
        return np.linspace(0.0, 100.0, 101)


class RandomClassifierEvaluationTest(unittest.TestCase):
    def test_raw_uniformity_matches_production_component(self):
        rng = np.random.default_rng(3)
        values = rng.integers(0, 1 << 16, size=(16, 100), dtype=np.uint16)
        present = rng.random(values.shape) > 0.20

        expected = random_structure_features(values, present).uniformity_pvalue
        actual = _raw_uniformity_pvalues(values, present)

        np.testing.assert_allclose(actual, expected)

    def test_increment_uniformity_uses_only_originally_adjacent_positions(self):
        values = np.arange(13, dtype=np.uint16)[None, :]
        present = np.ones_like(values, dtype=bool)
        present[0, 6] = False
        null = RecordingIncrementNull()

        scores = _increment_view_pvalues(values, present, null)

        self.assertEqual(scores.shape, (1,))
        # Twelve possible transitions minus the two touching the missing value.
        # Compacting the sequence would incorrectly create eleven transitions.
        self.assertEqual(null.keys, [(10, 2)])

    def test_gap_uniformity_is_invariant_to_sequence_order(self):
        values = np.asarray(
            [[100, 200, 300, 400, 500], [500, 200, 400, 100, 300]],
            dtype=np.uint16,
        )
        present = np.ones_like(values, dtype=bool)
        null = EmpiricalNullTables(sample_count=256, seed=5, batch_size=64)

        scores = gap_uniformity_pvalues(values, present, null)

        self.assertEqual(scores[0], scores[1])

    def test_concentrated_loss_stays_within_selected_subsequence(self):
        ideal = np.tile(np.arange(100, dtype=np.uint16), (32, 1))
        for pattern, stride in (("one_destination", 2), ("one_connection", 4)):
            condition = ImpairmentCondition(
                f"loss-20-{pattern}",
                loss_fraction=0.20,
                loss_pattern=pattern,
            )
            _, present, reordered_count = apply_impairment(
                ideal,
                condition,
                np.random.default_rng(7),
            )
            self.assertEqual(reordered_count, 0)
            self.assertTrue(np.all((~present).sum(axis=1) == 20))
            for missing in ~present:
                self.assertEqual(len(set(np.flatnonzero(missing) % stride)), 1)

    def test_full_evaluation_writes_all_subset_and_review_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outputs = evaluate_random_classifier_metrics(
                samples_per_strategy=2,
                calibration_samples_per_condition=4,
                null_table_samples=64,
                target_random_frr=0.10,
                batch_size=2,
                seed=11,
                output_dir=root / "data",
                figure_dir=root / "figures",
                benchmark_sample_count=16,
            )

            for path in outputs.values():
                self.assertTrue(path.is_file(), path)

            summary = json.loads(outputs["summary"].read_text(encoding="utf-8"))
            self.assertFalse(summary["production_classifier_changed"])
            self.assertEqual(summary["subset_count"], 63)
            self.assertEqual(summary["metric_order"], list(METRIC_NAMES))
            self.assertEqual(len(summary["conditions"]), len(CONDITIONS))

            with outputs["subset_results"].open(newline="", encoding="utf-8") as handle:
                subset_rows = list(csv.DictReader(handle))
            self.assertEqual(len(subset_rows), 63)

            scores = pq.read_table(outputs["metric_scores"])
            self.assertEqual(
                scores.num_rows,
                2 * len(CONDITIONS) * len(EVALUATION_GENERATORS),
            )
            self.assertEqual(
                scores.schema.names[-len(METRIC_NAMES) :],
                [name.upper() for name in METRIC_NAMES],
            )

            with zipfile.ZipFile(outputs["review_bundle"]) as archive:
                names = set(archive.namelist())
            self.assertIn("summary.json", names)
            self.assertIn("subset-results.csv", names)
            self.assertIn("subset-by-scenario.csv", names)
            self.assertIn("metric-false-random-heatmap.pdf", names)


if __name__ == "__main__":
    unittest.main()
