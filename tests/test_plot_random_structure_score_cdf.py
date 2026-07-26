import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow.parquet as pq

from ipid_analysis.plot_chi2_pvalue_cdf import (
    IDEAL_DATASET,
    IDEAL_SEQUENCE_LENGTH,
    LOSSY_DATASET,
    LOSSY_REORDERED_DATASET,
    PLOT_STRATEGIES,
    TRIVIAL_SAMPLES_PER_STRATEGY,
    generate_chi2_sequences,
)
from ipid_analysis.plot_random_structure_score_cdf import (
    INCREMENT_VIEWS,
    build_null_tables,
    calculate_scores,
    calculate_statistics,
    render,
)


class RandomStructureScoreCDFTest(unittest.TestCase):
    def test_loss_positions_are_not_bridged(self):
        values = np.zeros((1, IDEAL_SEQUENCE_LENGTH), dtype=np.int64)
        values[0, :4] = [10, 11, 999, 13]
        loss_mask = np.ones_like(values, dtype=bool)
        loss_mask[0, [0, 1, 3]] = False

        statistics = calculate_statistics(values, loss_mask)

        self.assertEqual(
            statistics["full.increment.ks"].sample_count.tolist(),
            [1],
        )
        self.assertEqual(
            statistics["full.second-difference.ks"].sample_count.tolist(),
            [0],
        )

    def test_score_is_a_probability_like_minimum(self):
        rng = np.random.default_rng(17)
        null_tables = build_null_tables(64, rng)
        sequences = generate_chi2_sequences(8, rng)
        values = sequences["RANDOM"]
        masks = np.zeros_like(values, dtype=bool)

        scores = calculate_scores(values, masks, null_tables)

        self.assertTrue(np.all(np.isfinite(scores)))
        self.assertTrue(np.all((scores > 0.0) & (scores <= 1.0)))

    def test_rendered_artifacts_and_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("ipid_analysis.plot_random_structure_score_cdf.configure_paper_style"):
                outputs = render(
                    samples_per_strategy=8,
                    null_samples_per_length=64,
                    threshold_samples=64,
                    false_rejection_rate=0.05,
                    seed=23,
                    processed_root=root / "processed",
                    figures_root=root / "figures",
                )

            for path in outputs:
                self.assertTrue(path.is_file(), path)

            ideal_pdf, ideal_json, _, lossy_json, _, reordered_json, aggregate = outputs
            self.assertEqual(ideal_pdf.suffix, ".pdf")
            table = pq.read_table(aggregate)
            expected_rows = 3 * (2 * TRIVIAL_SAMPLES_PER_STRATEGY + 6 * 8)
            self.assertEqual(table.num_rows, expected_rows)
            self.assertEqual(
                set(table.column("DATASET").to_pylist()),
                {IDEAL_DATASET, LOSSY_DATASET, LOSSY_REORDERED_DATASET},
            )
            self.assertEqual(
                set(table.column("IPID_SELECTION_STRATEGY").to_pylist()),
                set(PLOT_STRATEGIES),
            )
            self.assertEqual(
                table.column_names,
                [
                    "DATASET",
                    "IPID_SELECTION_STRATEGY",
                    "SAMPLE_INDEX",
                    "RANDOM_COMPATIBILITY_SCORE",
                    "IS_RANDOM_COMPATIBLE",
                ],
            )

            metadata = json.loads(ideal_json.read_text())
            lossy_metadata = json.loads(lossy_json.read_text())
            reordered_metadata = json.loads(reordered_json.read_text())
            self.assertGreater(metadata["threshold"]["tau"], 0.0)
            self.assertEqual(
                metadata["threshold"]["tau"],
                lossy_metadata["threshold"]["tau"],
            )
            self.assertEqual(
                metadata["threshold"]["tau"],
                reordered_metadata["threshold"]["tau"],
            )
            self.assertEqual(
                metadata["score"]["increment_views"],
                list(INCREMENT_VIEWS),
            )
            self.assertEqual(
                metadata["score"]["random_compatible_when"],
                "S >= tau",
            )
            self.assertEqual(metadata["null_calibration"]["runtime_simulation"], False)
            self.assertEqual(metadata["ideal_sequence_length"], IDEAL_SEQUENCE_LENGTH)


if __name__ == "__main__":
    unittest.main()
