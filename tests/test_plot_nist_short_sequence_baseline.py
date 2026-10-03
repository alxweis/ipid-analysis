import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import numpy as np
import pyarrow.parquet as pq

from ipid_analysis.nist_short_sequence import NIST_TEST_NAMES
from ipid_analysis.plot_nist_short_sequence_baseline import (
    CONDITION_LABELS,
    CONDITIONS,
    HEATMAP_LABELS,
    NIST_COMBINED_THRESHOLD,
    NIST_HEATMAP_CELL_FONT_SIZE,
    NIST_HEATMAP_CELL_HEIGHT_INCHES,
    NIST_HEATMAP_CELL_WIDTH_INCHES,
    NIST_HEATMAP_COLORBAR_WIDTH_INCHES,
    NIST_HEATMAP_FIGURE_SIZE_INCHES,
    TARGET_RANDOM_FALSE_REJECTION_RATE,
    _next_lower_power_of_ten,
    render,
)


class PlotNistShortSequenceBaselineTest(unittest.TestCase):
    def test_paper_labels_and_compact_axis_are_stable(self):
        self.assertEqual(TARGET_RANDOM_FALSE_REJECTION_RATE, 0.0005)
        self.assertEqual(NIST_COMBINED_THRESHOLD, 0.0005)
        self.assertEqual(
            CONDITION_LABELS,
            {
                "ideal": "Ideal",
                "lossy": "20% Lossy",
                "reordered": "20% Reordered",
                "lossy-reordered": "20% Lossy + 20% Reordered",
            },
        )
        self.assertEqual(HEATMAP_LABELS["combined"], "Combined score")
        self.assertEqual(NIST_HEATMAP_FIGURE_SIZE_INCHES, (7.0, 5.15))
        self.assertEqual(NIST_HEATMAP_CELL_WIDTH_INCHES, 0.30)
        self.assertEqual(NIST_HEATMAP_CELL_HEIGHT_INCHES, 0.155)
        self.assertEqual(NIST_HEATMAP_COLORBAR_WIDTH_INCHES, 0.065)
        self.assertEqual(NIST_HEATMAP_CELL_FONT_SIZE, 8.0)

    def test_cdf_axis_starts_at_the_next_lower_decade(self):
        self.assertEqual(
            _next_lower_power_of_ten(
                {
                    "A": np.asarray([6.4e-5, 0.2]),
                    "B": np.asarray([8.0e-5, 0.4]),
                }
            ),
            1e-5,
        )
        self.assertEqual(
            _next_lower_power_of_ten({"A": np.asarray([1e-4, 0.2])}),
            1e-5,
        )

    def test_render_writes_four_cdfs_heatmap_metadata_and_scores(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch(
                    "ipid_analysis.plot_nist_short_sequence_baseline."
                    "configure_compact_validation_style"
                ),
                patch(
                    "ipid_analysis.plot_random_structure_score_cdf."
                    "configure_compact_validation_style"
                ),
            ):
                outputs = render(
                    samples_per_strategy=2,
                    null_table_samples=32,
                    null_table_seed=11,
                    batch_size=8,
                    seed=13,
                    output_dir=root / "data",
                    figure_dir=root / "figures",
                )

            for path in outputs.values():
                self.assertTrue(path.is_file(), path)
            for condition in CONDITIONS:
                self.assertIn(f"{condition}_pdf", outputs)
                self.assertIn(f"{condition}_json", outputs)

            report = json.loads(outputs["summary_json"].read_text(encoding="utf-8"))
            self.assertTrue(report["not_a_nist_validation_claim"])
            self.assertEqual(report["included_tests"], list(NIST_TEST_NAMES))
            self.assertEqual(report["ideal_bit_length"], 1_600)
            self.assertEqual(report["lossy_bit_length"], 1_280)
            ideal_metadata = json.loads(outputs["ideal_json"].read_text(encoding="utf-8"))
            self.assertGreater(
                min(
                    values["minimum"] for values in ideal_metadata["summary_by_strategy"].values()
                ),
                ideal_metadata["figure_axis"]["positive_display_minimum"],
            )

            table = pq.read_table(outputs["aggregate"])
            self.assertIn("NIST_COMPATIBILITY_SCORE", table.column_names)
            self.assertEqual(set(table.column("DATASET").to_pylist()), set(CONDITIONS))

            with zipfile.ZipFile(outputs["review_bundle"]) as archive:
                names = set(archive.namelist())
            self.assertIn("nist-test-strategy-heatmap.pdf", names)
            self.assertIn("mass-4x25-nist-score-cdf-ideal.pdf", names)
            self.assertNotIn(outputs["aggregate"].name, names)


if __name__ == "__main__":
    unittest.main()
