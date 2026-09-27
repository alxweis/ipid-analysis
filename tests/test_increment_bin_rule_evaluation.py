import csv
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

import numpy as np

from ipid_analysis.increment_bin_rule_evaluation import (
    BIN_RULE_BY_NAME,
    BIN_RULES,
    IncrementBinNullTables,
    _discrete_bin_probabilities,
    evaluate_increment_bin_rules,
    increment_uniformity_pvalues_for_rule,
    selected_bin_counts,
)
from ipid_analysis.random_classifier_evaluation import (
    EmpiricalNullTables,
    increment_uniformity_pvalues,
)


class IncrementBinRuleEvaluationTest(unittest.TestCase):
    def test_bin_rules_cover_agreed_resolutions_and_cap_thirds_at_twelve(self):
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e5"], 10), (2,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e5-min3"], 10), (3,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e5"], 15), (2,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e5-min3"], 15), (3,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e5"], 24), (4,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e5-min3"], 24), (4,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e3"], 10), (2,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e3-min3"], 10), (3,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e3"], 12), (4,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e3-min3"], 12), (4,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e3"], 24), (8,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e3-min3"], 24), (8,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["power2-e5-min3"], 9), ())
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["fixed-3"], 24), (3,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["thirds-e3"], 24), (6,))
        self.assertEqual(selected_bin_counts(BIN_RULE_BY_NAME["thirds-e2"], 99), (12,))
        self.assertEqual(
            selected_bin_counts(BIN_RULE_BY_NAME["multiscale-3-4-8-16"], 24),
            (3, 4, 8),
        )

    def test_ternary_bins_use_exact_discrete_probabilities(self):
        probabilities = _discrete_bin_probabilities(3)
        np.testing.assert_allclose(probabilities.sum(), 1.0)
        counts = probabilities * 65_536
        np.testing.assert_array_equal(counts.astype(int), np.asarray([21_846, 21_845, 21_845]))

    def test_increment_views_use_only_originally_adjacent_positions(self):
        values = np.tile(np.arange(100, dtype=np.uint16), (2, 1))
        present = np.ones_like(values, dtype=bool)
        present[0, 1] = False
        changed = values.copy()
        changed[0, 1] = 60_000
        tables = IncrementBinNullTables(128, seed=9)
        for rule in BIN_RULES:
            original = increment_uniformity_pvalues_for_rule(values, present, rule, tables)
            modified = increment_uniformity_pvalues_for_rule(changed, present, rule, tables)
            self.assertEqual(original[0], modified[0], rule.name)

    def test_power2_e5_reproduces_the_current_increment_component(self):
        rng = np.random.default_rng(17)
        values = rng.integers(0, 65_536, size=(8, 100), dtype=np.uint16)
        present = rng.random(values.shape) >= 0.20
        candidate_tables = IncrementBinNullTables(256, seed=21)
        baseline_tables = EmpiricalNullTables(256, seed=21)
        actual = increment_uniformity_pvalues_for_rule(
            values,
            present,
            BIN_RULE_BY_NAME["power2-e5"],
            candidate_tables,
        )
        expected = increment_uniformity_pvalues(values, present, baseline_tables)
        np.testing.assert_array_equal(actual, expected)

    def test_full_evaluation_writes_review_artifacts_without_changing_production(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outputs = evaluate_increment_bin_rules(
                paper_samples_per_strategy=1,
                selection_samples_per_strategy=1,
                test_samples_per_strategy=1,
                calibration_samples_per_condition=2,
                null_table_samples=32,
                target_random_frr=0.10,
                batch_size=1,
                seed=13,
                output_dir=root / "data",
                figure_dir=root / "figures",
                benchmark_sample_count=2,
            )
            for path in outputs.values():
                self.assertTrue(path.is_file(), path)

            summary = json.loads(outputs["summary"].read_text(encoding="utf-8"))
            self.assertEqual(summary["experiment_version"], "2")
            self.assertFalse(summary["production_classifier_changed"])
            self.assertEqual(summary["thirds_bin_cap"], 12)
            self.assertEqual(
                [rule["name"] for rule in summary["bin_rules"]],
                [rule.name for rule in BIN_RULES],
            )

            with outputs["results"].open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 3 * len(BIN_RULES))

            with zipfile.ZipFile(outputs["review_bundle"]) as archive:
                names = set(archive.namelist())
            self.assertIn("summary.json", names)
            self.assertIn("bin-rule-by-scenario.csv", names)
            self.assertIn("bin-rule-false-random-heatmap.pdf", names)


if __name__ == "__main__":
    unittest.main()
