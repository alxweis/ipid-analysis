"""Connection ECDFs/heatmaps reuse exact aggregates, shared design and publication."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from test_paper_increment_plot import fixture as increment_fixture
from test_paper_increment_plot import stamp
from test_paper_intersection_plot import fixture as intersection_fixture

from ipid_analysis import artifacts, paper_ready
from ipid_analysis import paper_increment_plot as increment
from ipid_analysis import paper_intersection_plot as intersection
from ipid_analysis.paper_plot_style import _font


def fixture(root, kind, *, site="eu", protocol="tcp-80", connection=True):
    mode = "connection" if connection else "no-connection"
    directory = root / "reports/continental/campaign" / site / (protocol + "_fixture")
    directory = directory / mode / "comparison/rt-based-base_fixed-interval-base"
    if kind == "increment-distributions":
        source, aggregate = increment_fixture(directory, connection=connection)
        return paper_ready.job_for(source), aggregate
    return intersection_fixture(directory, connection=connection)


class ConnectionComparisonTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
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
        self.pairs = {
            kind: fixture(self.root, kind)
            for kind in ("increment-distributions", "strategy-intersection")
        }

    def test_shared_renderer_identical_pixels_and_immutable_sources(self):
        from matplotlib.figure import Figure
        from matplotlib.text import Text

        for kind, (job, aggregate) in self.pairs.items():
            with self.subTest(kind=kind):
                n_job, _ = fixture(self.root, kind, connection=False)
                before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
                images = []
                actual_save = Figure.savefig

                def inspect(fig, *args, **kwargs):
                    self.assertAlmostEqual(fig.get_size_inches()[0], 3.33)
                    self.assertEqual({t.get_fontsize() for t in fig.findobj(Text)}, {7.0})
                    images.append(np.asarray(fig.canvas.buffer_rgba()).copy())
                    return actual_save(fig, *args, **kwargs)

                with (
                    patch.object(Figure, "savefig", new=inspect),
                    patch("duckdb.connect", side_effect=AssertionError("only compact aggregates")),
                ):
                    for selected in (n_job, job):
                        result = paper_ready.run([selected], workers=1)[0]
                        self.assertEqual(result.status, "created", result.error)
                np.testing.assert_array_equal(*images)
                self.assertEqual(before, {p: stamp(p) for p in before})
                receipt = json.loads(artifacts.completion_path(job.destination).read_text())
                self.assertEqual(
                    [Path(s["path"]) for s in receipt["sources"]], [job.source, aggregate]
                )
                before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
                self.assertEqual(paper_ready.run([job])[0].status, "skipped-existing")
                self.assertEqual(before, {p: stamp(p) for p in before})

    def test_exact_stored_values_and_vertical_jumps_are_retained(self):
        job, aggregate = self.pairs["increment-distributions"]
        rows = increment.load_histogram(aggregate, json.loads(job.source.read_text()))
        self.assertEqual(len(rows), 8)
        for row in rows:
            if row["strategy"] in ("PER_DESTINATION", "PER_CONNECTION"):
                np.testing.assert_array_equal(row["increments"], [1, 1])
                np.testing.assert_array_equal(row["percentages"], [0, 100])
        job, aggregate = self.pairs["strategy-intersection"]
        matrix = intersection.load_matrix(aggregate, json.loads(job.source.read_text()))
        self.assertEqual(matrix["percentages"][1, 2], 0.01)
        self.assertEqual(matrix["counts"][1, 2], 1)
        self.assertEqual(matrix["percentages"][1, 1], 99.99)

    def test_discovery_separates_mode_phase_protocol_and_site(self):
        for kind, (job, _) in self.pairs.items():
            fixture(self.root, kind, site="af", protocol="icmp")
            fixture(self.root, kind, site="na")
            fixture(self.root, kind, connection=False)
            for suffix in (
                "no-connection/comparison/rt-based-base_fixed-interval-base",
                "connection/merged/rt-based-base_fixed-interval-mass",
            ):
                path = job.source.parents[3] / suffix / job.source.name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(job.source.read_text())
        found = paper_ready.discover(
            self.root / "reports", "campaign", sites=["eu"], protocols=["tcp"]
        )
        self.assertEqual(len(found), 4)
        self.assertEqual(
            sum(job.kind in paper_ready.CONNECTION_COMPARISON_KINDS for job in found), 2
        )
        self.assertEqual(
            paper_ready.discover(self.root / "reports", "campaign", protocols=["icmp"]), []
        )
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertTrue(
            all(r.status == "would-create" for r in paper_ready.run(found, dry_run=True))
        )
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_wrong_context_and_missing_or_invalid_aggregates_fail_without_fallback(self):
        for kind, (job, aggregate) in self.pairs.items():
            original = job.source.read_text()
            report = json.loads(original)
            for changes in (
                {"protocol": "icmp"},
                {"connection_mode": "no-connection"},
                {"target": "tcp.ipid.connection.rt-based.base"},
                {"figure": "other"},
                {"aggregate": str(aggregate).replace("/connection/", "/no-connection/")},
                {
                    "aggregate": str(aggregate).replace(
                        "rt-based-base_fixed-interval-base", "rt-based-base_fixed-interval-mass"
                    )
                },
                {"aggregate": str(aggregate.with_name("wrong.pq"))},
            ):
                with (
                    self.subTest(kind=kind, changes=changes),
                    patch("duckdb.connect", side_effect=AssertionError("no fallback query")),
                ):
                    job.source.write_text(json.dumps(report | changes))
                    self.assertEqual(paper_ready.worker(job).status, "failed")
                    self.assertFalse(job.destination.exists())
            job.source.write_text(original)
            # Test-owned table: a valid footer with missing required columns must fail.
            table = pq.read_table(aggregate)
            pq.write_table(pa.table({"unexpected": [1]}), aggregate)
            with patch("duckdb.connect", side_effect=AssertionError("no fallback query")):
                self.assertEqual(paper_ready.worker(job).status, "failed")
            pq.write_table(table, aggregate)

    def test_processed_root_remaps_only_matching_connection_comparisons(self):
        for kind, (job, aggregate) in self.pairs.items():
            original = json.loads(job.source.read_text())
            parts = aggregate.parts
            relative = Path(*parts[parts.index("continental") :])
            mapped = self.root / "processed" / relative
            mapped.parent.mkdir(parents=True, exist_ok=True)
            mapped.write_bytes(aggregate.read_bytes())
            report = original | {"aggregate": str(Path("/old-vm/processed") / relative)}
            job.source.write_text(json.dumps(report))
            remapped = paper_ready.job_for(job.source, processed_root=self.root / "processed")
            before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
            self.assertEqual(paper_ready._comparison_aggregate(remapped, report), mapped)
            result = paper_ready.run([remapped], workers=1)[0]
            self.assertEqual(result.status, "created", result.error)
            self.assertEqual(before, {p: stamp(p) for p in before})
            bad = report | {
                "aggregate": report["aggregate"].replace("/connection/", "/no-connection/")
            }
            with self.assertRaises(ValueError):
                paper_ready._comparison_aggregate(remapped, bad)

    def test_mixed_spawn_batch_and_completed_repeat(self):
        jobs = [job for job, _ in self.pairs.values()]
        for kind in self.pairs:
            jobs.append(fixture(self.root, kind, site="na")[0])
            jobs.append(fixture(self.root, kind, connection=False)[0])
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
        self.assertIn(
            "6 created, 0 failed",
            subprocess.run(command, check=True, capture_output=True, text=True).stdout,
        )
        self.assertEqual(before, {p: stamp(p) for p in before})
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertIn(
            "0 created, 0 failed",
            subprocess.run(command, check=True, capture_output=True, text=True).stdout,
        )
        self.assertEqual(before, {p: stamp(p) for p in before})


if __name__ == "__main__":
    unittest.main()
