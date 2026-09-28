import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from ipid_analysis.plot_random_classifier_diagnostics import CONDITIONS, METHODS, render


class PlotRandomClassifierDiagnosticsTest(unittest.TestCase):
    def test_render_writes_updated_heldout_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch(
                "ipid_analysis.plot_random_classifier_diagnostics._configure_evaluation_style"
            ):
                outputs = render(
                    samples_per_generator=1,
                    candidate_null_samples=16,
                    candidate_null_seed=11,
                    nist_null_samples=16,
                    nist_null_seed=12,
                    batch_size=1,
                    seed=13,
                    output_dir=root / "data",
                    figure_dir=root / "figures",
                )

            for path in outputs.values():
                self.assertTrue(path.is_file(), path)
            report = json.loads(outputs["report_json"].read_text(encoding="utf-8"))
            self.assertFalse(report["production_classifier_changed"])
            self.assertEqual(report["methods"], list(METHODS))
            self.assertEqual(report["conditions"], [condition.name for condition in CONDITIONS])
            with zipfile.ZipFile(outputs["review_bundle"]) as archive:
                names = set(archive.namelist())
            self.assertIn("random-classifier-candidate-confusion.pdf", names)
            self.assertIn("random-classifier-method-confusion.pdf", names)
            self.assertIn("random-classifier-candidate-by-generator.pdf", names)


if __name__ == "__main__":
    unittest.main()
