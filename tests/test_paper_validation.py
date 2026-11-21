"""Validation data integrity, real RT routing and immutable paper exports."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import copy
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from typer.testing import CliRunner

from ipid_analysis import artifacts, paper_ready
from ipid_analysis import classifier_validation_preparation as data
from ipid_analysis import paper_validation as pv
from ipid_analysis.strategies import IPIDStrategy


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


def config(samples=10):
    return {
        "classifier_version": data.cv.CLASSIFIER_VERSION,
        "synthetic_generator_version": "4",
        "seed": 42,
        "samples_per_strategy": samples,
        "trivial_samples_per_strategy": 1000,
        "connection_count": 4,
        "request_ip_ids": data.cv.REQUEST_IP_IDS.tolist(),
        "random_structure_score": {
            "version": data.CANDIDATE_RANDOM_SCORE_VERSION,
            "threshold": data.CANDIDATE_RANDOM_MIN_SCORE,
            "threshold_status": data.CANDIDATE_RANDOM_THRESHOLD_STATUS,
            "null_tables": {"sample_count": 1_000_000, "base_seed": 20260927},
        },
    }


def e2e_fixture(n=10):
    rt = np.zeros((8, 9), dtype=np.int64)
    for i in range(8):
        rt[i, i if i < 6 else 8] = 1000 if i < 2 else n
    # A genuine wrong RT decision must remain wrong, never corrected by FI.
    rt[6, 8] -= 1
    rt[6, 0] += 1
    final = rt.copy()
    final[6, 6], final[7, 7] = rt[6, 8], rt[7, 8]
    final[6:, 8] = 0
    retained = rt.copy()
    retained[:, -1] = 0
    return {
        "e2e_rt": data.matrix_metrics(rt),
        "e2e": {c: data.matrix_metrics(final) for c in data.CONDITIONS},
        "e2e_routing": {
            k: {
                "support": int(rt[i].sum()),
                "forwarded_to_fi": int(rt[i, -1]),
                "retained_rt_counts": retained[i].tolist(),
            }
            for i, k in enumerate(data.CLASSES)
        },
        "model": {"version": "test-fixture", "rt_observations": 16, "fi_new_observations": 100},
    }


class ValidationTest(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(
            patch.dict(
                os.environ,
                {
                    "IPID_ANALYSIS_STORAGE_MODE": "local",
                    "IPID_ANALYSIS_SCRATCH_STORAGE": "disk",
                    "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
                    "MPLCONFIGDIR": str(self.root / "mpl"),
                },
            )
        )
        stack.enter_context(patch("ipid_analysis.runtime.available_memory", return_value=8 << 30))
        from ipid_analysis.paper_plot_style import _font

        _font.cache_clear()
        self.addCleanup(_font.cache_clear)
        self.reports, self.processed = self.root / "reports", self.root / "processed"

    def originals(self):
        source, meta = data.source_paths(self.reports, self.processed)
        source.parent.mkdir(parents=True)
        meta.parent.mkdir(parents=True)
        cfg = config()
        rows = []
        for dataset in data.MASS_DATASETS.values():
            for i, k in enumerate(data.CLASSES):
                n = 1000 if i < 2 else cfg["samples_per_strategy"]
                rows.extend(
                    [{"DATASET": dataset, "EXPECTED_STRATEGY": k, "DETECTED_STRATEGY": k}] * n
                )
        pq.write_table(pa.Table.from_pylist(rows), source)
        meta.write_text(json.dumps(cfg))
        source.with_suffix(".pdf").write_bytes(b"ordinary original remains unchanged")
        return source, meta

    def prepared(self):
        self.originals()
        manifest = data.cache_manifest(self.reports, self.processed)
        manifest.parent.mkdir(parents=True)
        e2e = e2e_fixture()
        mass = {c: e2e["e2e"][c] for c in data.CONDITIONS}
        components = []
        for name, obj in [("mass.json", mass), ("e2e.json", e2e)]:
            path = manifest.parent / name
            path.write_text(json.dumps(obj))
            components.append(path)
        scores = {}
        for method in ("random", "nist"):
            for c in data.CONDITIONS:
                path = manifest.parent / f"{method}-{c}.pq"
                values = {
                    k: np.geomspace(1e-7 if method == "random" else 1e-6, 1, 1000 if i < 2 else 10)
                    for i, k in enumerate(data.CLASSES)
                }
                pq.write_table(data.score_histogram(values), path)
                components.append(path)
                scores[f"{method}:{c}"] = path.name
        manifest.write_text(
            json.dumps(
                {
                    "version": 1,
                    "mass": "mass.json",
                    "e2e": "e2e.json",
                    "scores": scores,
                    "components": [artifacts.fingerprint(p) for p in components],
                    "random_threshold": data.CANDIDATE_RANDOM_MIN_SCORE,
                    "nist_threshold": 0.0005,
                    "samples_per_nontrivial_class": 10,
                }
            )
        )
        return pv.discover(self.reports, self.processed)

    def test_discovery_and_dry_run_are_read_only(self):
        self.originals()
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch.object(data, "prepare", side_effect=AssertionError("dry run cannot prepare")):
            self.assertEqual(
                paper_ready.main(
                    [
                        "--validation",
                        "--dry-run",
                        "--reports-root",
                        str(self.reports),
                        "--processed-root",
                        str(self.processed),
                    ]
                ),
                0,
            )
        self.assertEqual(before, {p: stamp(p) for p in self.root.rglob("*") if p.is_file()})
        jobs = pv.discover(self.reports, self.processed)
        self.assertEqual(len(jobs), 12)
        self.assertEqual(len({j.destination for j in jobs}), 12)
        self.assertEqual(sum(j.destination.parent.name == "nist-baseline" for j in jobs), 4)
        result = paper_ready.run([jobs[0]], workers=1)[0]
        self.assertEqual(result.status, "failed")
        self.assertIn("make validate-classifier", result.error)

    def test_validation_cli_dry_run_cannot_compute_or_modify(self):
        self.originals()
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with (
            patch.object(data, "prepare", side_effect=AssertionError("no computation")),
            patch.object(
                data.cv, "validate_classifier", side_effect=AssertionError("no exporter")
            ),
            patch.object(data.cv, "start_run", side_effect=AssertionError("no scratch writes")),
        ):
            result = CliRunner().invoke(
                data.cv.app,
                [
                    "--prepare-paper",
                    "--dry-run",
                    "--reports-root",
                    str(self.reports),
                    "--processed-root",
                    str(self.processed),
                ],
            )
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("manifest", result.output)
        self.assertEqual(before, {p: stamp(p) for p in self.root.rglob("*") if p.is_file()})

    def test_validation_cli_modes_and_overrides(self):
        with patch.object(data.cv, "validate_classifier", return_value={}) as original:
            result = CliRunner().invoke(
                data.cv.app, ["--samples-per-strategy", "3", "--seed", "7"]
            )
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(original.call_args.kwargs["samples_per_strategy"], 3)
        self.assertEqual(original.call_args.kwargs["seed"], 7)
        for args in (
            ["--dry-run"],
            ["--reports-root", str(self.reports)],
            ["--prepare-paper", "--seed", "42"],
            ["--prepare-paper", "--samples-per-strategy", "100000"],
            ["--prepare-paper", "--legacy-random-score"],
        ):
            with patch.object(data, "prepare", side_effect=AssertionError("invalid mode")):
                result = CliRunner().invoke(data.cv.app, args)
            self.assertEqual(result.exit_code, 2, result.output)

    @unittest.skipUnless(shutil.which("make"), "requires GNU Make")
    def test_make_preparation_never_calls_legacy_cdf_exporter(self):
        shutil.copyfile(Path(__file__).resolve().parents[1] / "Makefile", self.root / "Makefile")
        recorder = self.root / "record.py"
        log = self.root / "calls.jsonl"
        recorder.write_text(
            "import json,sys\nfrom pathlib import Path\n"
            "with Path('calls.jsonl').open('a') as stream: stream.write(json.dumps(sys.argv[1:])+'\\n')\n"
        )
        for args, count in (
            ("--prepare-paper --dry-run", 1),
            ("--samples-per-strategy 3 --seed 7", 2),
        ):
            log.unlink(missing_ok=True)
            result = subprocess.run(
                [
                    "make",
                    "--no-print-directory",
                    "validate-classifier",
                    f"PYTHON_INTERPRETER={shlex.quote(sys.executable)} {shlex.quote(str(recorder))}",
                    f"ARGS={args}",
                ],
                cwd=self.root,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertEqual(len(calls), count)
            self.assertEqual(calls[0][:2], ["-m", "ipid_analysis.classifier_validation"])
            if count == 2:
                self.assertEqual(
                    calls[1][:2], ["-m", "ipid_analysis.plot_random_structure_score_cdf"]
                )

    def test_paper_renderer_rejects_former_preparation_flag(self):
        with patch.object(data, "prepare", side_effect=AssertionError("render only")):
            with self.assertRaises(SystemExit) as error:
                paper_ready.main(["--validation", "--prepare-validation"])
        self.assertEqual(error.exception.code, 2)

    def test_mass_counts_project_only_existing_labels_and_check_support(self):
        source, _ = self.originals()
        before = stamp(source)
        out = data.mass_metrics(source, config())
        self.assertEqual(out["ideal"]["accuracy"], 1)
        self.assertEqual(out["ideal"]["supports"], [1000, 1000, 10, 10, 10, 10, 10, 10])
        self.assertEqual(stamp(source), before)
        with self.assertRaisesRegex(ValueError, "supports"):
            data.mass_metrics(source, config(11))

    def test_actual_rt_gate_preserves_wrong_early_labels(self):
        rt = np.ones((4, 16), dtype=np.uint16)
        fi = np.arange(400, dtype=np.uint16).reshape(4, 100)
        codes = np.array(
            [
                IPIDStrategy.SINGLE,
                IPIDStrategy.UNCLASSIFIED,
                IPIDStrategy.CONSTANT,
                IPIDStrategy.UNCLASSIFIED,
            ]
        )

        def mass(v, missing, tables):
            self.assertEqual(len(v), 2)
            return np.array([IPIDStrategy.MULTI, IPIDStrategy.RANDOM])

        with (
            patch.object(
                data, "paired_sequences", return_value=iter((k, rt, fi) for k in data.CLASSES)
            ),
            patch.object(data, "classify_batch", return_value=codes),
            patch.object(data, "_mass_codes", side_effect=mass) as classify,
        ):
            result = data.e2e_metrics(4, 42, None)
        self.assertEqual(classify.call_count, 32)
        data.validate_e2e(result)
        row = result["e2e"]["ideal"]["counts"][0]
        self.assertEqual(row, [0, 1, 1, 0, 0, 0, 1, 1, 0])
        self.assertEqual(result["e2e_routing"]["REFLECTION"]["forwarded_to_fi"], 2)

    def test_retained_rt_class_never_generates_fi_impairments(self):
        rt = np.ones((4, 16), dtype=np.uint16)
        fi = np.ones((4, 100), dtype=np.uint16)
        with (
            patch.object(data, "condition_inputs", side_effect=AssertionError("no FI work")),
            patch.object(data, "_mass_codes", side_effect=AssertionError("no FI classifier")),
        ):
            _, counts, routing, final = data._e2e_class((1, "CONSTANT", rt, fi, 42, None))
        self.assertEqual(routing["forwarded_to_fi"], 0)
        self.assertEqual(final, {c: counts for c in data.CONDITIONS})

    def test_disjoint_phase_geometry_and_reflection_reset(self):
        cohorts = dict((k, (rt, fi)) for k, rt, fi in data.paired_sequences(10, 42))
        for k, (rt, fi) in cohorts.items():
            self.assertEqual(rt.shape, (1000 if k in data.cv.TRIVIAL_STRATEGIES else 10, 16))
            self.assertEqual(fi.shape[1], 100)
            self.assertFalse(np.shares_memory(rt, fi))
        rt, fi = cohorts["REFLECTION"]
        np.testing.assert_array_equal(rt[:, :4], fi[:, :4])
        self.assertFalse(np.array_equal(cohorts["RANDOM"][0], cohorts["RANDOM"][1][:, :16]))

    def test_invalid_metrics_or_oracle_routing_fail(self):
        valid = e2e_fixture()
        data.validate_e2e(valid)
        invalid = copy.deepcopy(valid)
        invalid["e2e_routing"]["MULTI"]["forwarded_to_fi"] += 1
        with self.assertRaisesRegex(ValueError, "routing"):
            data.validate_e2e(invalid)
        invalid = copy.deepcopy(valid)
        invalid["e2e"]["ideal"]["percentages"][0][0] = 0
        with self.assertRaisesRegex(ValueError, "percentages"):
            data.validate_e2e(invalid)

    def test_histograms_are_exact_and_reject_malformed_inputs(self):
        self.root.mkdir(exist_ok=True)
        path = self.root / "scores.pq"
        values = {k: np.array([0, 0.1, 0.1, 1]) for k in data.CLASSES}
        pq.write_table(data.score_histogram(values), path)
        loaded = pv.load_scores(path)
        for k in values:
            np.testing.assert_array_equal(loaded[k], values[k])
        table = data.score_histogram(values).to_pydict()
        for bad in (-1, 2**63 - 1):
            changed = copy.deepcopy(table)
            changed["COUNT"][0] = bad
            pq.write_table(pa.table(changed, schema=data.SCORE_SCHEMA), path)
            with self.assertRaisesRegex(ValueError, "histogram"):
                pv.load_scores(path)
        changed = copy.deepcopy(table)
        changed["STRATEGY"][0] = "UNKNOWN"
        pq.write_table(pa.table(changed, schema=data.SCORE_SCHEMA), path)
        with self.assertRaises(ValueError):
            pv.load_scores(path)

    def test_twelve_outputs_metadata_and_existing_files_preserved(self):
        jobs = self.prepared()
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        results = paper_ready.run(jobs, workers=1)
        self.assertEqual([(r.status, r.error) for r in results], [("created", "")] * 12)
        for p, s in before.items():
            self.assertEqual(stamp(p), s)
        for job in jobs:
            meta = json.loads(job.metadata.read_text())
            self.assertAlmostEqual(meta["layout"]["width_pt"], 239.76)
            self.assertEqual(meta["layout"]["font_pt"], 7)
            self.assertIn("sources", meta)
            artifacts.assert_complete(job.destination)
            if job.figure.startswith("e2e"):
                self.assertAlmostEqual(meta["layout"]["stage_gap_pt"], 14.4)
                self.assertEqual(set(meta["e2e"]), set(data.CONDITIONS))
                self.assertEqual(meta["layout"]["stage_headings"], ["RT-based", "Fixed-Interval"])
            if "-cdf-" in job.figure:
                self.assertEqual(meta["layout"]["curve_marker_spacing_pt"], 36)
                self.assertEqual(meta["layout"]["threshold_color"], "#4B4B4B")
        created = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch.object(data, "e2e_metrics", side_effect=AssertionError("no recomputation")):
            self.assertTrue(all(r.status == "skipped-existing" for r in paper_ready.run(jobs)))
        self.assertEqual(created, {p: stamp(p) for p in self.root.rglob("*") if p.is_file()})

    def test_partial_pair_and_changed_cache_are_not_overwritten(self):
        jobs = self.prepared()
        job = jobs[0]
        job.metadata.write_text("partial old file")
        before = stamp(job.metadata)
        self.assertEqual(paper_ready.run([job], workers=1)[0].status, "failed")
        self.assertEqual(stamp(job.metadata), before)
        (job.source.parent / "mass.json").write_text("{}")
        result = paper_ready.run([jobs[1]], workers=1)[0]
        self.assertEqual(result.status, "failed")
        self.assertIn("changed", result.error)
        self.assertFalse(jobs[1].destination.exists())

    def test_concurrent_pair_has_one_creator(self):
        job = self.prepared()[0]
        # Import Matplotlib and prepare its font outside concurrent invocations.
        from ipid_analysis.paper_validation_plot import draw

        self.assertTrue(callable(draw))
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(pv.worker, [job, job]))
        self.assertEqual(sorted(r.status for r in results), ["created", "skipped-existing"])

    def test_changed_input_during_render_is_not_published(self):
        job = self.prepared()[0]
        from ipid_analysis import paper_validation_plot

        original = paper_validation_plot.draw

        def changed(*args):
            layout = original(*args)
            with (job.source.parent / "mass.json").open("a") as stream:
                stream.write(" ")
            return layout

        with patch.object(paper_validation_plot, "draw", side_effect=changed):
            result = pv.worker(job)
        self.assertEqual(result.status, "failed")
        self.assertIn("changed", result.error)
        self.assertFalse(job.destination.exists())
        self.assertFalse(job.metadata.exists())

    def test_spawn_pool_renders_validation_pairs(self):
        jobs = self.prepared()[:2]
        results = paper_ready.run(jobs, workers=2)
        self.assertEqual([r.status for r in results], ["created", "created"])
        self.assertTrue(all(j.metadata.is_file() for j in jobs))

    def test_original_metadata_requires_production_calibration(self):
        _, meta = self.originals()
        self.assertEqual(data._original_config(meta)["samples_per_strategy"], 10)
        bad = config()
        bad["random_structure_score"]["null_tables"]["sample_count"] = 100
        meta.write_text(json.dumps(bad))
        with self.assertRaisesRegex(ValueError, "calibration"):
            data._original_config(meta)

    def test_prepare_cache_is_persistent_and_only_computes_once(self):
        source, meta = self.originals()
        before = stamp(source), stamp(meta)

        class Nist:
            def __init__(self, *args):
                self.args = args

            def combined_pvalues(self, bits):
                return np.ones(len(bits)), {}

        with (
            patch.object(data, "create_candidate_null_tables", return_value=None),
            patch.object(data, "prepare_nulls", return_value=[]),
            patch.object(data, "e2e_metrics", return_value=e2e_fixture()) as e2e,
            patch.object(data, "ShortNistNullTables", Nist),
            patch.object(data, "calculate_scores", side_effect=lambda v, m, t: np.ones(len(v))),
            patch.object(
                data.cv, "validate_classifier", side_effect=AssertionError("no old exporter")
            ),
            patch.object(
                data.cv, "start_run", side_effect=AssertionError("no legacy writer lease")
            ),
        ):
            command = [
                "--prepare-paper",
                "--reports-root",
                str(self.reports),
                "--processed-root",
                str(self.processed),
            ]
            result = CliRunner().invoke(data.cv.app, command)
            self.assertEqual(result.exit_code, 0, (result.output, result.exception))
            manifest = data.cache_manifest(self.reports, self.processed)
            self.assertTrue(manifest.is_file())
            self.assertEqual(e2e.call_count, 1)
            self.assertEqual(e2e.call_args.kwargs, {})  # E2E is not sent to the GIL-bound pool.
        stored = {p: stamp(p) for p in manifest.parent.rglob("*") if p.is_file()}
        with patch.object(
            data, "create_candidate_null_tables", side_effect=AssertionError("cache hit")
        ):
            result = CliRunner().invoke(data.cv.app, command)
            self.assertEqual(result.exit_code, 0, (result.output, result.exception))
        self.assertEqual(stored, {p: stamp(p) for p in manifest.parent.rglob("*") if p.is_file()})
        self.assertEqual(before, (stamp(source), stamp(meta)))
        self.assertEqual(len(data.read_json(manifest)["components"]), 10)
        for job in pv.discover(self.reports, self.processed):
            pv.load_payload(job)


if __name__ == "__main__":
    unittest.main()
