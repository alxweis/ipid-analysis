import csv
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

import numpy as np
import pyarrow.parquet as pq

from ipid_analysis.random_classifier_evaluation import METRIC_NAMES
from ipid_analysis.random_classifier_evaluation_v2 import (
    COMBINER_NAMES,
    CONDITIONS,
    DATASET_NAMES,
    GENERATOR_NAMES,
    combine_metric_scores,
    evaluate_random_classifier_metrics_v2,
    generate_v2_sequences,
)


class RandomClassifierEvaluationV2Test(unittest.TestCase):
    def test_profiles_span_both_small_and_large_steps_without_classifier_limits(self):
        for profile, seed in (("selection", 3), ("heldout", 4)):
            generated = generate_v2_sequences(
                4096,
                np.random.default_rng(seed),
                profile,
            )
            steps = generated["SINGLE_FIXED"].base_step
            self.assertLess(int(steps.min()), 64)
            self.assertGreater(int(steps.max()), 50_000)

    def test_combiners_reject_more_extreme_component_vectors(self):
        ordinary = np.asarray([[0.2, 0.3, 0.4]], dtype=float)
        extreme = np.asarray([[1e-8, 0.3, 0.4]], dtype=float)
        scores = np.concatenate([ordinary, extreme], axis=0)
        indices = np.asarray([0, 1, 2])
        weights = np.asarray([0.5, 0.3, 0.2, 0.0, 0.0, 0.0])
        for combiner in COMBINER_NAMES:
            combined = combine_metric_scores(scores, indices, combiner, weights)
            self.assertLess(combined[1], combined[0], combiner)

    def test_full_v2_evaluation_writes_split_combiner_and_review_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outputs = evaluate_random_classifier_metrics_v2(
                selection_samples_per_strategy=2,
                test_samples_per_strategy=2,
                weight_training_samples_per_strategy=2,
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
            self.assertEqual(summary["experiment_version"], "2")
            self.assertFalse(summary["production_classifier_changed"])
            self.assertEqual(summary["combiner_order"], list(COMBINER_NAMES))
            self.assertEqual(
                summary["candidate_count_per_dataset"],
                len(COMBINER_NAMES) * (2 ** len(METRIC_NAMES) - 1),
            )

            with outputs["combination_results"].open(newline="", encoding="utf-8") as handle:
                result_rows = list(csv.DictReader(handle))
            self.assertEqual(
                len(result_rows),
                len(DATASET_NAMES) * len(COMBINER_NAMES) * (2 ** len(METRIC_NAMES) - 1),
            )

            scores = pq.read_table(outputs["heldout_metric_scores"])
            self.assertEqual(
                scores.num_rows,
                2 * len(CONDITIONS) * len(GENERATOR_NAMES),
            )
            self.assertEqual(
                scores.schema.names[-len(METRIC_NAMES) :],
                [name.upper() for name in METRIC_NAMES],
            )

            with zipfile.ZipFile(outputs["review_bundle"]) as archive:
                names = set(archive.namelist())
            self.assertIn("summary.json", names)
            self.assertIn("combination-results.csv", names)
            self.assertIn("combination-by-scenario.csv", names)
            self.assertIn("step-sensitivity.csv", names)
            self.assertIn("metric-step-sensitivity.pdf", names)


if __name__ == "__main__":
    unittest.main()
