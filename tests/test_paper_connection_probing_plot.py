"""Connection intervals reuse exact p99.9 preparation and the approved violin design."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
from test_paper_probing_plot import fixture, stamp

from ipid_analysis import artifacts, paper_ready
from ipid_analysis import paper_probing_plot as probing
from ipid_analysis.paper_figures import CONTINENT_ORDER, MODES
from ipid_analysis.paper_plot_style import _font


class ConnectionProbingTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        env = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_SCRATCH_STORAGE": "disk",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
                "MPLCONFIGDIR": str(self.root / "mpl"),
            },
        )
        env.start()
        self.addCleanup(env.stop)
        _font.cache_clear()
        self.addCleanup(_font.cache_clear)
        self.job = fixture(self.root, protocol="tcp-80", connection=True)
        self.report = json.loads(self.job.source.read_text())
        self.aggregate, self.metadata = probing.paths(self.job.source, self.report)

    def test_opt_in_preparation_reuses_aggregator_and_actual_population_quartiles(self):
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        from ipid_analysis.paper_figures import aggregate_probing_intervals

        with patch(
            "ipid_analysis.paper_figures.aggregate_probing_intervals",
            wraps=aggregate_probing_intervals,
        ) as aggregate:
            self.assertEqual(probing.prepare(self.job), "created")
        self.assertEqual(aggregate.call_count, 1)
        self.assertEqual(aggregate.call_args.kwargs["quantile_points"], 256)
        self.assertEqual(aggregate.call_args.kwargs["upper_quantile"], 0.999)
        self.assertEqual(
            aggregate.call_args.args[:3],
            tuple(
                Path(self.report["sources"][key])
                for key in ("rt_based", "fixed_interval", "continent_map")
            ),
        )
        self.assertEqual(before, {p: stamp(p) for p in before})
        rows = probing.load_series(self.aggregate, self.metadata, self.job.source)
        original = pq.read_table(self.report["aggregate"]).to_pylist()
        for row in rows:
            self.assertEqual(len(row["QUANTILES_MS"]), 256)
            self.assertEqual(row["IP_COUNT_UNION"], 400)
            self.assertEqual(row["IP_COUNT_MODE"], 400)
            if row["MODE"] == MODES[0]:
                factor = CONTINENT_ORDER.index(row["CONTINENT_CODE"]) + 1
                for key, expected in (("Q1_MS", 100.5), ("MEDIAN_MS", 200.5), ("Q3_MS", 300.5)):
                    self.assertAlmostEqual(row[key], expected * factor, places=6)
                old = next(
                    r
                    for r in original
                    if (r["CONTINENT_CODE"], r["MODE"]) == (row["CONTINENT_CODE"], row["MODE"])
                )
                self.assertGreater(row["CUTOFF_MS"], old["P99_5_MS"])
            else:
                self.assertEqual([row[k] for k in ("Q1_MS", "MEDIAN_MS", "Q3_MS")], [20] * 3)
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch("duckdb.connect", side_effect=AssertionError("must reuse preparation")):
            self.assertEqual(probing.prepare(self.job), "skipped-current")
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_missing_or_wrong_quantiles_never_trigger_implicit_queries(self):
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch("duckdb.connect", side_effect=AssertionError("no implicit query")):
            result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "failed")
        self.assertIn("--prepare-probing-intervals", result.error)
        self.assertEqual(before, {p: stamp(p) for p in before})
        probing.prepare(self.job)
        metadata = json.loads(self.metadata.read_text())
        for key, value in (
            ("upper_quantile", 0.995),
            ("source_report_sha256", "wrong-mode-report"),
        ):
            with self.subTest(key=key):
                self.metadata.write_text(json.dumps(metadata | {key: value}))
                with patch("duckdb.connect", side_effect=AssertionError("no fallback")):
                    result = paper_ready.run([self.job], workers=1)[0]
                self.assertEqual(result.status, "failed")
                self.assertFalse(self.job.destination.exists())
        self.metadata.write_text(json.dumps(metadata))

    def test_context_and_comparison_inputs_cannot_cross_modes_or_phases(self):
        original = self.job.source.read_text()
        cases = [
            {"protocol": "icmp"},
            {"connection_mode": "no-connection"},
            {"target": "tcp.ipid.connection.rt-based.base"},
            {"figure": "other"},
            {"aggregate": self.report["aggregate"].replace("/connection/", "/no-connection/")},
            {
                "aggregate": self.report["aggregate"].replace(
                    "rt-based-base_fixed-interval-base", "rt-based-base_fixed-interval-mass"
                )
            },
        ]
        for changes in cases:
            with (
                self.subTest(changes=changes),
                patch(
                    "duckdb.connect", side_effect=AssertionError("invalid context must fail first")
                ),
            ):
                self.job.source.write_text(json.dumps(self.report | changes))
                with self.assertRaises(ValueError):
                    probing.prepare(self.job)
                self.assertEqual(paper_ready.worker(self.job).status, "failed")
        sources = dict(self.report["sources"])
        sources["rt_based"] = sources["rt_based"].replace("/connection/", "/no-connection/")
        self.job.source.write_text(json.dumps(self.report | {"sources": sources}))
        with self.assertRaisesRegex(ValueError, "original comparison"):
            probing.prepare(self.job)
        self.job.source.write_text(original)
        self.assertFalse(self.aggregate.exists())
        self.assertFalse(self.metadata.exists())

    def test_discovery_dry_run_and_processed_root_preparation_for_both_modes(self):
        stateless = fixture(self.root, protocol="tcp-80")
        fixture(self.root, site="af", protocol="icmp", connection=True)
        # A misleading copy in the wrong mode/phase must not be discovered.
        for suffix in (
            "no-connection/comparison/rt-based-base_fixed-interval-base",
            "connection/merged/rt-based-base_fixed-interval-mass",
        ):
            path = self.job.source.parents[3] / suffix / self.job.source.name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(self.job.source.read_text())
        found = paper_ready.discover(
            self.root / "reports", "campaign", sites=["eu"], protocols=["tcp"]
        )
        self.assertEqual(set(found), {self.job, stateless})
        self.assertEqual(
            paper_ready.discover(self.root / "reports", "campaign", protocols=["icmp"]), []
        )
        for job in (self.job, stateless):
            report = json.loads(job.source.read_text())
            old_root = str(self.root / "processed")
            report["aggregate"] = report["aggregate"].replace(old_root, "/other-vm/processed")
            report["sources"] = {
                k: v.replace(old_root, "/other-vm/processed") for k, v in report["sources"].items()
            }
            job.source.write_text(json.dumps(report))
            remapped = paper_ready.job_for(job.source, processed_root=self.root / "processed")
            before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
            with patch(
                "ipid_analysis.paper_probing_plot.prepare",
                side_effect=AssertionError("dry-run must not prepare"),
            ):
                self.assertEqual(
                    paper_ready.main(
                        [
                            str(job.source),
                            "--processed-root",
                            str(self.root / "processed"),
                            "--prepare-probing-intervals",
                            "--dry-run",
                        ]
                    ),
                    0,
                )
            self.assertEqual(before, {p: stamp(p) for p in before})
            self.assertEqual(probing.prepare(remapped), "created")
            self.assertEqual(before, {p: stamp(p) for p in before})

    def test_shared_geometry_exact_quartiles_and_immutable_render(self):
        from matplotlib.figure import Figure
        from matplotlib.text import Text

        probing.prepare(self.job)
        rows = probing.load_series(self.aggregate, self.metadata, self.job.source)
        expected = [row[k] for row in rows for k in ("Q1_MS", "MEDIAN_MS", "Q3_MS")]
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        actual_save = Figure.savefig

        def inspect(fig, *args, **kwargs):
            ax = fig.axes[0]
            self.assertAlmostEqual(fig.get_size_inches()[0], 3.33)
            self.assertAlmostEqual(ax.get_position().height * fig.get_size_inches()[1], 0.88)
            self.assertEqual({t.get_fontsize() for t in fig.findobj(Text)}, {7.0})
            self.assertEqual([line.get_ydata()[0] for line in ax.lines], expected)
            self.assertEqual(ax.get_xticklabels()[0].get_text(), "N. America\n(400)")
            self.assertEqual(ax.get_xticklabels()[3].get_text(), "S. America\n(400)")
            majors, minors = ax.get_yticks(), ax.yaxis.get_minorticklocs()
            for left, right in zip(majors, majors[1:]):
                if left >= ax.get_ylim()[0] and right <= ax.get_ylim()[1]:
                    self.assertEqual(sum(left < v < right for v in minors), 1)
            legend, renderer = ax.get_legend(), fig.canvas.get_renderer()
            box = legend.get_window_extent(renderer)
            self.assertTrue(ax.bbox.contains(box.x0, box.y0) and ax.bbox.contains(box.x1, box.y1))
            for handle, text in zip(legend.legend_handles, legend.get_texts()):
                self.assertEqual((handle.get_width(), handle.get_height()), (4, 4))
                gap = text.get_window_extent(renderer).x0 - handle.get_window_extent(renderer).x1
                self.assertAlmostEqual(gap * 72 / fig.dpi, 1.2)
            return actual_save(fig, *args, **kwargs)

        with (
            patch.object(Figure, "savefig", new=inspect),
            patch(
                "duckdb.connect", side_effect=AssertionError("render reads compact quantiles only")
            ),
        ):
            result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "created", result.error)
        self.assertEqual(before, {p: stamp(p) for p in before})
        receipt = json.loads(artifacts.completion_path(self.job.destination).read_text())
        self.assertEqual(
            [Path(s["path"]) for s in receipt["sources"]],
            [self.job.source, self.aggregate, self.metadata],
        )
        self.assertEqual(receipt["parameters"]["kind"], paper_ready.CONNECTION_PROBING_KIND)
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(probing.prepare(self.job), "skipped-existing")
        self.assertEqual(paper_ready.run([self.job])[0].status, "skipped-existing")
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_explicit_prepare_cli_and_completed_repeat_preserve_inputs(self):
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        arguments = [str(self.job.source), "--prepare-probing-intervals", "--workers", "1"]
        self.assertEqual(paper_ready.main(arguments), 0)
        self.assertTrue(self.job.destination.is_file())
        self.assertEqual(before, {p: stamp(p) for p in before})
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch("duckdb.connect", side_effect=AssertionError("repeat must skip preparation")):
            self.assertEqual(paper_ready.main(arguments), 0)
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_mixed_spawn_batch_renders_prepared_tables_without_interval_inputs(self):
        jobs = [
            self.job,
            fixture(self.root, site="na", protocol="tcp-80", connection=True),
            fixture(self.root, protocol="tcp-80"),
        ]
        for job in jobs:
            self.assertEqual(probing.prepare(job), "created")
            # Test-owned fixtures: prove normal rendering has no per-IP fallback.
            for path in json.loads(job.source.read_text())["sources"].values():
                Path(path).unlink()
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        command = [
            sys.executable,
            "-B",
            "-m",
            "ipid_analysis.paper_ready",
            "--campaign",
            "campaign",
            "--reports-root",
            str(self.root / "reports"),
            "--workers",
            "2",
        ]
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertIn("3 created, 0 failed", result.stdout)
        self.assertEqual(before, {p: stamp(p) for p in before})
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertIn(
            "0 created, 0 failed",
            subprocess.run(command, check=True, capture_output=True, text=True).stdout,
        )
        self.assertEqual(before, {p: stamp(p) for p in before})


if __name__ == "__main__":
    unittest.main()
