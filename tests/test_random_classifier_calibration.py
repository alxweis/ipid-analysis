import json
import logging
from pathlib import Path
import tempfile
import traceback
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from typer.testing import CliRunner

from ipid_analysis import random_classifier_view_evaluation as evaluation
from ipid_analysis.classifier_validation import FIXED_CONFIG
from ipid_analysis.empirical_random_uniformity import _stable_rng
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_GAP_NULL_TABLE_SEED,
    CANDIDATE_RANDOM_MIN_SCORE,
    candidate_random_score_components,
    create_candidate_null_tables,
)


class RandomClassifierCalibrationTest(unittest.TestCase):
    def setUp(self):
        self.tables = create_candidate_null_tables(128)
        self.candidates = (evaluation.PRODUCTION_CANDIDATE,)

    def test_evaluator_uses_exact_production_scores_with_missing_positions(self):
        rng = np.random.default_rng(71)
        values = rng.integers(0, 65536, size=(8, 100), dtype=np.uint16)
        values[0, :10] = 0
        present = rng.random(values.shape) >= 0.20
        present[1, 1::2] = False
        present[2, :] = False
        present[2, :2] = True
        with (
            patch.object(
                evaluation, "raw_uniformity_pvalues", side_effect=AssertionError("historical raw")
            ),
            patch.object(
                evaluation,
                "increment_view_pvalues_for_rule",
                side_effect=AssertionError("historical increments"),
            ),
        ):
            primitive = evaluation._primitive_scores(
                values, present, None, self.tables.gap, production_tables=self.tables
            )
        expected = candidate_random_score_components(values, present, self.tables, FIXED_CONFIG)
        np.testing.assert_array_equal(
            evaluation.candidate_scores(primitive, self.candidates)[:, 0], expected.score
        )
        np.testing.assert_array_equal(
            primitive["production_increment"], expected.increment_uniformity
        )
        np.testing.assert_array_equal(primitive["production_gap"], expected.gap_uniformity)
        self.assertEqual(evaluation._required_rules(self.candidates), ())

    def test_calibration_selects_worst_condition_without_rounding_scores(self):
        sample_count, batch_size, seed = 32, 16, 41
        targets = (0.0005, 0.25)
        # Known lower tails, with lossy as the limiting condition. The small
        # offset detects accidental float32 rounding before threshold selection.
        bases = (0.02, 0.01, 0.04, 0.03)
        by_condition = {
            condition.name: (base + np.arange(sample_count) * 0.001 + 1e-12)[::-1]
            for condition, base in zip(evaluation.CORE_CONDITIONS, bases, strict=True)
        }
        batches = [
            {"production_score": scores[offset : offset + batch_size]}
            for scores in by_condition.values()
            for offset in range(0, sample_count, batch_size)
        ]
        with patch.object(evaluation, "_primitive_scores", side_effect=batches):
            result = evaluation._calibrate(
                sample_count,
                batch_size,
                seed,
                self.candidates,
                None,
                self.tables.gap,
                target_rates=targets,
                production_tables=self.tables,
            )[self.candidates[0].name]
        for target in targets:
            point = result["operating_points"][f"{target:.8g}"]
            expected = {
                condition.name: base + (0 if target == 0.0005 else 0.008) + 1e-12
                for condition, base in zip(evaluation.CORE_CONDITIONS, bases, strict=True)
            }
            self.assertEqual(point["threshold_by_condition"], expected)
            self.assertEqual(point["threshold"], min(expected.values()))
            for scores in by_condition.values():
                self.assertLessEqual(np.mean(scores < point["threshold"]), target)
            self.assertNotEqual(point["threshold"], float(np.float32(point["threshold"])))

    def test_calibration_accepts_ties_at_the_threshold(self):
        with patch.object(
            evaluation, "candidate_scores", return_value=np.array([[0.1], [0.1], [0.2], [0.3]])
        ):
            result = evaluation._calibrate(
                4,
                4,
                11,
                self.candidates,
                None,
                self.tables.gap,
                target_rates=(0.0005, 0.25),
                production_tables=self.tables,
            )[self.candidates[0].name]
        point = result["operating_points"]["0.25"]
        self.assertEqual(point["threshold"], 0.1)
        self.assertEqual(set(point["false_rejection_by_condition"].values()), {0.0})

    def test_tiny_production_cli_writes_reviewable_independent_results(self):
        tiny = evaluation.EvaluationPreset(2, 2, 8, 128, 2)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.dict(evaluation.PRESETS, {"confirmation": tiny}),
                patch.object(
                    evaluation, "production_candidate_null_tables", return_value=self.tables
                ) as table_factory,
                patch.object(evaluation, "CANDIDATE_NULL_TABLE_SAMPLES", 128),
                patch.object(evaluation, "_stable_rng", wraps=_stable_rng) as streams,
                patch.object(
                    evaluation,
                    "IncrementBinNullTables",
                    side_effect=AssertionError("production needs no increment null tables"),
                ),
                patch.object(evaluation, "local_log_handler", return_value=logging.NullHandler()),
                patch.object(evaluation.logging, "basicConfig"),
            ):
                result = CliRunner().invoke(
                    evaluation.app,
                    [
                        "--production-only",
                        "--preset",
                        "confirmation",
                        "--seed",
                        "123",
                        "--output-dir",
                        str(root / "data"),
                        "--figure-dir",
                        str(root / "figures"),
                    ],
                )
            self.assertEqual(
                result.exit_code,
                0,
                result.output + "".join(traceback.format_exception(result.exception))
                if result.exception
                else result.output,
            )
            table_factory.assert_called_once_with()
            stream_ids = {call.args[1] for call in streams.call_args_list}
            self.assertTrue({101, 102, 201, 202}.issubset(stream_ids))
            output = root / "data" / "production" / "confirmation"
            metadata = json.loads((output / "summary.json").read_text())
            self.assertEqual(metadata["mode"], "production-only")
            self.assertEqual(len(metadata["candidates"]), 1)
            self.assertEqual(metadata["raw_bin_counts"], [])
            self.assertEqual(metadata["bin_rules"], [])
            self.assertIsNone(metadata["seeds"]["increment_null_tables"])
            self.assertEqual(metadata["seeds"]["base"], 123)
            self.assertEqual(metadata["seeds"]["gap_null_tables"], CANDIDATE_GAP_NULL_TABLE_SEED)
            self.assertFalse(metadata["production_score"]["threshold_updated"])
            self.assertEqual(
                metadata["production_score"]["current_threshold"],
                CANDIDATE_RANDOM_MIN_SCORE,
            )
            self.assertEqual(
                metadata["production_score"]["current_threshold_status"], "empirically-calibrated"
            )
            scores = pq.read_table(output / "view-scores.pq")
            self.assertNotIn("RAW", scores.column_names)
            for name in ("INCREMENT", "GAP", "SCORE"):
                self.assertEqual(scores.schema.field(name).type, pa.float64())
            np.testing.assert_array_equal(
                scores["SCORE"].to_numpy(),
                np.minimum(scores["INCREMENT"].to_numpy(), scores["GAP"].to_numpy()),
            )
            self.assertEqual(set(scores["PROFILE"].to_pylist()), {"paper", "heldout"})
            self.assertEqual(set(scores["PRESENT_COUNT"].to_pylist()), {80, 100})
            self.assertTrue((output / "operating-point-by-scenario.csv").exists())
            self.assertTrue((output / "random-classifier-view-review-bundle.zip").exists())

    def test_production_mode_rejects_historical_variant_selection(self):
        result = CliRunner().invoke(
            evaluation.app, ["--production-only", "--variants", evaluation.BASELINE_NAME]
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("cannot be combined", result.output)


if __name__ == "__main__":
    unittest.main()
