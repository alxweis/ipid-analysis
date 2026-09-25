import csv
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

import numpy as np

from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_RANDOM_METRICS,
    CANDIDATE_RANDOM_MIN_SCORE,
    CandidateRandomScoreComponents,
    candidate_random_score_components,
)
from ipid_analysis.random_classifier_candidate_validation import (
    CONDITIONS,
    MODEL_NAMES,
    validate_random_classifier_candidate,
)
from ipid_analysis.random_classifier_evaluation import EmpiricalNullTables
from ipid_analysis.random_classifier_evaluation_v2 import GENERATOR_NAMES
from ipid_analysis.strategies import (
    RANDOM_STRUCTURE_MIN_SCORE,
    RANDOM_STRUCTURE_SCORE_VERSION,
)


class RandomClassifierCandidateValidationTest(unittest.TestCase):
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

    def test_validation_writes_candidate_and_comparison_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outputs = validate_random_classifier_candidate(
                samples_per_generator=2,
                null_table_samples=64,
                null_table_seed=11,
                candidate_threshold=0.10,
                batch_size=2,
                seed=13,
                output_dir=root / "data",
                figure_dir=root / "figures",
            )

            for path in outputs.values():
                self.assertTrue(path.is_file(), path)

            report = json.loads(outputs["validation_json"].read_text(encoding="utf-8"))
            self.assertFalse(report["production_classifier_changed"])
            self.assertFalse(report["candidate_specification_matches_selected_evaluation"])
            self.assertEqual(report["candidate"]["combiner"], "minimum")
            self.assertEqual(report["candidate"]["metrics"], list(CANDIDATE_RANDOM_METRICS))
            self.assertEqual(set(report["binary_metrics"]), set(MODEL_NAMES))

            with outputs["aggregate_csv"].open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(
                len(rows),
                len(MODEL_NAMES) * len(CONDITIONS) * len(GENERATOR_NAMES),
            )

            with zipfile.ZipFile(outputs["review_bundle"]) as archive:
                names = set(archive.namelist())
            self.assertIn("random-classifier-candidate-validation.csv", names)
            self.assertIn("random-classifier-candidate-validation.json", names)
            self.assertIn("random-classifier-candidate-confusion.pdf", names)
            self.assertIn("random-classifier-current-vs-candidate-confusion.pdf", names)
            self.assertIn("random-classifier-candidate-by-generator.pdf", names)

    def test_selected_candidate_does_not_change_production_constants(self):
        self.assertEqual(RANDOM_STRUCTURE_SCORE_VERSION, "raw-multiset-bounded-v2")
        self.assertEqual(RANDOM_STRUCTURE_MIN_SCORE, 0.000016313656391956604)
        self.assertNotEqual(CANDIDATE_RANDOM_MIN_SCORE, RANDOM_STRUCTURE_MIN_SCORE)


if __name__ == "__main__":
    unittest.main()
