"""Export verified validation sidecars and include all twelve PDFs in the ZIP."""

import ast
from contextlib import ExitStack
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from tests.test_paper_values import Fixture

from paper_values.figure_zip import bundle
from paper_values.validation_paper import CLASSES, CONDITIONS, E2E, FIGURES, MASS, MODEL_FILES


def metric(counts):
    supports = [sum(row) for row in counts]
    return {
        "classes": list(CLASSES),
        "predicted_classes": [*CLASSES, "UNCLASSIFIED"],
        "counts": counts,
        "supports": supports,
        "percentages": [[100 * n / supports[i] for n in row] for i, row in enumerate(counts)],
        "accuracy": sum(counts[i][i] for i in range(8)) / sum(supports),
        "balanced_accuracy": sum(counts[i][i] / supports[i] for i in range(8)) / 8,
        "unclassified_count": sum(row[-1] for row in counts),
        "recall_by_class": {k: counts[i][i] / supports[i] for i, k in enumerate(CLASSES)},
        "precision_by_class": {
            k: counts[i][i] / sum(row[i] for row in counts) if sum(row[i] for row in counts) else 0
            for i, k in enumerate(CLASSES)
        },
    }


class PaperValidationValuesTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.fixture = Fixture(self.root)
        self.directory = self.root / "reports/figures/classifier-validation"
        self.directory.mkdir(parents=True)
        code = Path(__file__).resolve().parents[1] / "ipid_analysis"
        self.model = {
            "model_sources": {
                n: hashlib.sha256((code / n).read_bytes()).hexdigest() for n in MODEL_FILES
            },
            "samples_per_nontrivial_class": 100000,
            "trivial_samples_per_class": 1000,
            "null_samples": 1000000,
            "random_threshold": 4.0383019016847365e-5,
            "nist_threshold": 0.0005,
            "random_score_version": "increment-pearson-gap-min-v4",
            "seed": 42,
            "random_null_seed": 20260927,
            "nist_null_seed": 20260930,
        }
        rt = [[0] * 9 for _ in range(8)]
        for i in range(8):
            rt[i][i if i < 6 else 8] = 1000 if i < 2 else 100000
        rt[6][8] -= 1
        rt[6][0] += 1
        retained = copy.deepcopy(rt)
        for row in retained:
            row[-1] = 0
        self.outcomes = {}
        for c in CONDITIONS:
            final = copy.deepcopy(retained)
            final[6][6] = rt[6][8]
            final[7][7] = 99960
            final[7][8] = 40
            self.outcomes[c] = metric(final)
        self.e2e = {
            "e2e": self.outcomes,
            "e2e_rt": metric(rt),
            "e2e_routing": {
                k: {
                    "support": sum(rt[i]),
                    "forwarded_to_fi": rt[i][-1],
                    "retained_rt_counts": retained[i],
                }
                for i, k in enumerate(CLASSES)
            },
            "model": {
                "version": "paired-116-v1",
                "rt_observations": 16,
                "fi_new_observations": 100,
            },
        }
        self.paths = {}
        for figure in FIGURES:
            if figure == E2E:
                payload = self.e2e
            elif figure in MASS.values():
                payload = {
                    "panels": [
                        {"condition": c, "metrics": self.outcomes[c]}
                        for c in {
                            "mass-4x25-classifier-confusion-ideal": ("ideal",),
                            "mass-4x25-classifier-confusion-lossy-vs-lossy-reordered": (
                                "lossy", "lossy-reordered"
                            ),
                            "mass-4x25-classifier-confusion-lossy-vs-reordered": (
                                "lossy", "reordered"
                            ),
                        }[figure]
                    ]
                }
            else:
                payload = {
                    "summary_by_strategy": {
                        k: {
                            "count": 1000 if i < 2 else 100000,
                            "random_compatible_count": 1000 if i < 2 else 99960,
                            "minimum": 1e-6,
                            "maximum": 1,
                        }
                        for i, k in enumerate(CLASSES)
                    }
                }
            if "-score-cdf-" in figure:
                method = "nist" if "-nist-score-" in figure else "random"
                payload["method"] = method
                payload["condition"] = figure.removeprefix(f"mass-4x25-{method}-score-cdf-")
                payload["threshold"] = self.model[method + "_threshold"]
            self.paths[figure] = self.sidecar(figure, payload)

    def sidecar(self, figure, payload):
        directory = self.directory / ("nist-baseline" if "-nist-score-" in figure else "")
        directory.mkdir(exist_ok=True)
        path = directory / (figure + "__paper.json")
        path.with_suffix(".pdf").write_bytes(b"%PDF-1.4\nunit-test fixture")
        path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "paper_ready": True,
                    "figure": figure,
                    "validation": self.model,
                    **payload,
                }
            )
        )
        return path

    def test_accuracy_routing_and_all_figures_are_exported_read_only(self):
        before = {
            p: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in self.directory.rglob("*")
            if p.is_file()
        }
        with patch("pyarrow.parquet.ParquetFile", side_effect=AssertionError("no raw-data reads")):
            result = self.fixture.run(strict=True)
        values = result["values"]
        for c in CONDITIONS:
            key = "validation.e2e." + c
            self.assertAlmostEqual(
                values[key + ".accuracy-pct"]["raw"], 100 * self.outcomes[c]["accuracy"]
            )
            self.assertAlmostEqual(
                values[key + ".balanced-accuracy-pct"]["raw"],
                100 * self.outcomes[c]["balanced_accuracy"],
            )
            self.assertEqual(values[key + ".total.count"]["raw"], 602000)
            self.assertEqual(values[key + ".correct.count"]["raw"], 601959)
            self.assertEqual(values[key + ".unclassified.count"]["raw"], 40)
        self.assertEqual(values["validation.e2e.routing.multi.forwarded.count"]["raw"], 99999)
        self.assertEqual(values["validation.e2e.routing.multi.support.count"]["raw"], 100000)
        self.assertAlmostEqual(
            values["validation.random-score.ideal.random.rejection-pct"]["raw"], 0.04
        )
        self.assertEqual(values["validation.random.threshold"]["raw"], 4.0383019016847365e-5)
        self.assertEqual(values["validation.data.fi.sequence-length"]["raw"], 100)
        self.assertEqual(values["validation.data.rt.sequence-length"]["raw"], 16)
        # Six supported RT classes; do not count the expected forwarding of
        # MULTI/RANDOM as errors in the legacy RT-only accuracy key.
        self.assertEqual(values["validation.rt-ideal.accuracy-pct"]["raw"], 100)
        figures = {
            v["raw"] for k, v in values.items() if k.startswith("fig.validation.") and v["raw"]
        }
        self.assertEqual(len(figures), 12)
        self.assertTrue(all(p.endswith("__paper.pdf") for p in figures))
        self.assertEqual(
            before,
            {
                p: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.directory.rglob("*")
                if p.is_file()
            },
        )
        tex = (self.root / "tmp/values.tex").read_text()
        self.assertIn(r"\setval{validation.e2e.ideal.accuracy-pct}", tex)
        bundle(repository_root=self.root)
        with zipfile.ZipFile(self.root / "tmp/paper-figures.zip") as archive:
            self.assertEqual(set(archive.namelist()), figures)

    def test_verified_sidecars_supersede_old_summary_without_changing_it(self):
        old = self.directory / "classifier-validation-summary.json"
        old.write_text(
            json.dumps({"schema_version": 1, "production_profile": True, "classifier_sources": {}})
        )
        before = old.read_bytes()
        values = self.fixture.run(strict=True)["values"]
        self.assertIsNotNone(values["validation.fi-ideal.accuracy-pct"]["raw"])
        self.assertEqual(old.read_bytes(), before)

    def test_real_mass_renderer_exports_both_panels_and_all_figures(self):
        from ipid_analysis import artifacts, paper_ready, paper_validation

        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_SCRATCH_STORAGE": "disk",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
                "MPLCONFIGDIR": str(self.root / "mpl"),
            }))
            stack.enter_context(patch("ipid_analysis.runtime.available_memory", return_value=8 << 30))
            aggregate = self.root / "mass.json"
            aggregate.write_text(json.dumps(self.outcomes))
            manifest = self.root / "manifest.json"
            manifest.write_text(json.dumps({
                "version": 1, **self.model, "mass": aggregate.name,
                "components": [artifacts.fingerprint(aggregate)],
            }))
            jobs = []
            for figure in MASS.values():
                if any(job.figure == figure for job in jobs):
                    continue
                path = self.paths[figure]
                path.unlink()
                path.with_suffix(".pdf").unlink()
                jobs.append(paper_validation.ValidationJob(manifest, path.with_suffix(".pdf"), figure))
            results = paper_ready.run(jobs, workers=1)
            self.assertEqual([(r.status, r.error) for r in results], [("created", "")] * 3)
        values = self.fixture.run(strict=True)["values"]
        self.assertEqual(values["validation.data.reordering.pct"]["raw"], 20)
        self.assertIsNotNone(values["validation.fi-reordered.accuracy-pct"]["raw"])
        bundle(repository_root=self.root)
        with zipfile.ZipFile(self.root / "tmp/paper-figures.zip") as archive:
            self.assertEqual(len(archive.namelist()), 12)

    def test_paired_mass_metadata_requires_both_conditions(self):
        from paper_values.validation_paper import PaperValidation

        figure = "mass-4x25-classifier-confusion-lossy-vs-reordered"
        path = self.paths[figure]
        value = json.loads(path.read_text())
        self.assertEqual([p["condition"] for p in value["panels"]], ["lossy", "reordered"])
        for panels in ([value["panels"][1]], [value["panels"][0]] * 2):
            value["panels"] = panels
            path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, "panels"):
                PaperValidation(self.directory).load(figure)

    def test_legacy_summary_is_rejected_after_the_generator_model_changes(self):
        from paper_values.validation import Validation, legacy_cli_compatible

        code = Path(__file__).resolve().parents[1] / "ipid_analysis"
        names = ("strategies.py", "random_classifier_candidate.py", "increment_uniformity.py",
                 "empirical_random_uniformity.py", "classifier_validation.py")
        expected = {n: hashlib.sha256((code / n).read_bytes()).hexdigest() for n in names}
        recorded = {**expected, "classifier_validation.py":
                    "6a23e0822a9fc8f708a2e17df5538f0a01c0a87fdc1e7e6683e766747b51d4b2"}
        summary = self.directory / "classifier-validation-summary.json"
        summary.write_text(json.dumps({
            "schema_version": 1, "production_profile": True,
            "classifier_sources": recorded,
            "datasets": {"mass-4x25-ideal": {"metrics": {"accuracy": 0.99}}},
        }))
        before = summary.read_bytes()
        # Exercise the real fallback with all new sidecars absent.
        for path in self.paths.values():
            path.unlink()
            path.with_suffix(".pdf").unlink()
        reader = Validation(self.directory)
        # The generator changed numerically in 6ea242a (Multi increments), not
        # just in main(). Old validation must now be rejected by the real guard.
        self.assertFalse(legacy_cli_compatible(recorded, expected, code / "classifier_validation.py"))
        with self.assertRaisesRegex(ValueError, "different classifier code"):
            reader.load()
        self.assertEqual(summary.read_bytes(), before)
        # An exact current fingerprint still loads through the ordinary path.
        current = json.loads(before)
        current["classifier_sources"] = expected
        summary.write_text(json.dumps(current))
        current_bytes = summary.read_bytes()
        self.assertEqual(reader.load()["classifier_sources"], expected)
        value, _ = reader.read({
            "field": ["datasets", "mass-4x25-ideal", "metrics", "accuracy"], "factor": 100,
        })
        self.assertEqual(value, 99)
        self.assertEqual(summary.read_bytes(), current_bytes)

    def test_legacy_cli_guard_accepts_only_cli_changes_to_a_frozen_model(self):
        from paper_values.validation import LEGACY_GENERATOR_SHA256, legacy_cli_compatible

        # A small frozen model keeps this unit test independent of future
        # production-generator evolution. Only this fixture's model hash is
        # injected; the production hash and acceptance policy remain untouched.
        original = "MODEL = 1\ndef generate():\n    return MODEL\ndef main():\n    return 'old CLI'\n"
        tree = ast.parse(original)
        tree.body = [n for n in tree.body if not isinstance(n, ast.FunctionDef) or n.name != "main"]
        model = hashlib.sha256(ast.dump(tree, include_attributes=False).encode()).hexdigest()
        expected = {"strategies.py": "1" * 64, "classifier_validation.py": "2" * 64}
        recorded = {**expected, "classifier_validation.py": LEGACY_GENERATOR_SHA256}
        changed = self.root / "generator.py"
        with patch("paper_values.validation.LEGACY_GENERATOR_MODEL_SHA256", model):
            changed.write_text(original.replace("'old CLI'", "'new CLI'"))
            self.assertTrue(legacy_cli_compatible(recorded, expected, changed))
            self.assertFalse(legacy_cli_compatible({**recorded, "strategies.py": "0" * 64}, expected, changed))
            self.assertFalse(legacy_cli_compatible({**recorded, "classifier_validation.py": "0" * 64}, expected, changed))
            self.assertFalse(legacy_cli_compatible(None, expected, changed))
            changed.write_text(original.replace("MODEL = 1", "MODEL = 2"))
            self.assertFalse(legacy_cli_compatible(recorded, expected, changed))
            changed.write_text(original + "\nUNEXPECTED_MODEL_CHANGE = 1\n")
            self.assertFalse(legacy_cli_compatible(recorded, expected, changed))

    def test_invalid_metrics_or_model_preserve_previous_export(self):
        self.fixture.run(strict=True)
        before = self.fixture.output_hashes()
        path = self.paths[E2E]
        original = json.loads(path.read_text())
        for change in ("accuracy", "routing", "model", "small"):
            doc = copy.deepcopy(original)
            if change == "accuracy":
                doc["e2e"]["ideal"]["accuracy"] = 1
            elif change == "routing":
                doc["e2e_routing"]["MULTI"]["forwarded_to_fi"] += 1
            elif change == "model":
                doc["validation"]["model_sources"]["strategies.py"] = "0" * 64
            else:
                doc["validation"]["samples_per_nontrivial_class"] = 10
            path.write_text(json.dumps(doc))
            with self.assertRaises(ValueError, msg=change):
                self.fixture.run(strict=True)
            self.assertEqual(before, self.fixture.output_hashes())
        path.write_text(json.dumps(original))

    def test_export_does_not_import_analysis_or_recompute(self):
        import builtins

        original = builtins.__import__

        def guarded(name, *args, **kwargs):
            if name.startswith("ipid_analysis"):
                raise AssertionError("paper-values cannot import analysis modules")
            return original(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=guarded):
            self.fixture.run(strict=True)


if __name__ == "__main__":
    unittest.main()
