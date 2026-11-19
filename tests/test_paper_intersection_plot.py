"""Exact compact intersection data, approved layout and immutable publication."""

import copy
import hashlib
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

from ipid_analysis import artifacts, paper_ready
from ipid_analysis import paper_intersection_plot as intersection
from ipid_analysis.paper_figures import INTERSECTION_SCHEMA, INTERSECTION_STRATEGIES
from ipid_analysis.paper_plot_style import _font

NAME = "n-rt-b_fi-b_strategy-intersection"


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


def fixture(directory, *, empty=False, no_hundred=False):
    directory.mkdir(parents=True, exist_ok=True)
    report = directory / (NAME + ".json")
    aggregate = directory / (NAME + ".pq")
    rows, included = [], 0
    for i, rt in enumerate(INTERSECTION_STRATEGIES):
        counts = [0] * 7
        if not empty:
            counts[i] = 10000
            if no_hundred:
                counts[i] -= 250
                counts[(i + 1) % 7] += 250
            elif i == 1:
                counts[i] -= 1
                counts[(i + 1) % 7] += 1  # 0.01%, displayed as 0.0, not '-'.
        total = sum(counts)
        included += total
        for fixed, count in zip(INTERSECTION_STRATEGIES, counts):
            rows.append(
                dict(
                    RT_BASED_STRATEGY=rt,
                    FIXED_INTERVAL_STRATEGY=fixed,
                    COUNT=count,
                    RT_BASED_TOTAL=total,
                    PERCENTAGE=100 * count / total if total else 0.0,
                )
            )
    pq.write_table(pa.Table.from_pylist(rows, schema=INTERSECTION_SCHEMA), aggregate)
    report.write_text(
        json.dumps(
            dict(
                figure="strategy-intersection",
                aggregate=str(aggregate),
                strategies=list(INTERSECTION_STRATEGIES),
                included_ip_count=included,
                intersection_ip_count=included + 3,
                excluded_strategy_ip_count=3,
                sources={"rt_based": "/do-not-read/rt.pq", "fixed_interval": "/do-not-read/fi.pq"},
            )
        )
    )
    report.with_suffix(".pdf").write_bytes(b"original PDF is never read or changed")
    return paper_ready.job_for(report), aggregate


class PaperIntersectionTest(unittest.TestCase):
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
        self.job, self.aggregate = fixture(self.root / "data")
        self.report = json.loads(self.job.source.read_text())

    def test_exact_percentage_and_row_order_are_preserved(self):
        rows = pq.read_table(self.aggregate).to_pylist()
        data = intersection.load_matrix(self.aggregate, self.report)
        self.assertEqual(data["percentages"][1, 2], 0.01)
        self.assertEqual(data["counts"][1, 2], 1)
        self.assertEqual(data["percentages"][1, 1], 99.99)
        self.assertEqual(data["percentages"][0, 0], 100)
        pq.write_table(
            pa.Table.from_pylist(rows[::-1], schema=INTERSECTION_SCHEMA), self.aggregate
        )
        reverse = intersection.load_matrix(self.aggregate, self.report)
        np.testing.assert_array_equal(data["percentages"], reverse["percentages"])
        np.testing.assert_array_equal(data["counts"], reverse["counts"])

    def test_approved_layout_json_and_compact_only_immutable_render(self):
        from matplotlib.figure import Figure
        from matplotlib.text import Text

        before = {p: stamp(p) for p in self.job.source.parent.iterdir()}
        savefig = Figure.savefig

        def inspect(fig, *args, **kwargs):
            ax, cax = fig.axes
            renderer = fig.canvas.get_renderer()
            self.assertEqual(fig.get_size_inches()[0], 3.33)
            self.assertEqual({t.get_fontsize() for t in fig.findobj(Text)}, {7.0})
            self.assertEqual(ax.get_xlabel(), intersection.X_LABEL)
            self.assertEqual(ax.get_ylabel(), intersection.Y_LABEL)
            self.assertTrue(all(t.get_rotation() == 30 for t in ax.get_xticklabels()))
            self.assertEqual(cax.get_ylabel(), "Percentage [%]")
            self.assertAlmostEqual(cax.get_position().width * 3.33, 0.05)
            self.assertEqual(len(ax.texts), 49)
            self.assertIn("0.0", [t.get_text() for t in ax.texts])
            self.assertIn("-", [t.get_text() for t in ax.texts])
            text = next(t for t in ax.texts if t.get_text() == "100.0")
            box = text.get_window_extent(renderer)
            x, y = text.get_position()
            left, bottom = ax.transData.transform((x - 0.5, y + 0.5))
            right, top = ax.transData.transform((x + 0.5, y - 0.5))
            for gap in (box.x0 - left, right - box.x1, box.y0 - bottom, top - box.y1):
                self.assertAlmostEqual(gap, 5)
            self.assertGreater(ax.yaxis.labelpad, 2)
            self.assertLessEqual(ax.yaxis.labelpad, 6)
            self.assertGreaterEqual(ax.yaxis.label.get_window_extent(renderer).x0, 0.99)
            return savefig(fig, *args, **kwargs)

        with (
            patch.object(Figure, "savefig", new=inspect),
            patch("duckdb.connect", side_effect=AssertionError("no queries")),
            patch(
                "pyarrow.parquet.read_table",
                side_effect=AssertionError("do not read per-IP tables"),
            ),
            patch(
                "ipid_analysis.paper_figures.aggregate_strategy_intersection",
                side_effect=AssertionError("no join"),
            ),
        ):
            result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "created", result.error)
        self.assertEqual(before, {p: stamp(p) for p in before})
        receipt = json.loads(artifacts.completion_path(self.job.destination).read_text())
        self.assertEqual(
            [Path(v["path"]) for v in receipt["sources"]], [self.job.source, self.aggregate]
        )
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch.object(intersection, "load_matrix", side_effect=AssertionError("must skip")):
            self.assertEqual(paper_ready.run([self.job])[0].status, "skipped-existing")
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_empty_rows_and_no_actual_hundred_still_use_same_cell_geometry(self):
        from matplotlib.figure import Figure

        savefig = Figure.savefig
        sizes = []

        def inspect(fig, *args, **kwargs):
            ax = fig.axes[0]
            sizes.append((ax.bbox.width, ax.bbox.height))
            self.assertEqual(len(ax.texts), 49)
            if len(sizes) > 1:
                self.assertNotIn("100.0", [t.get_text() for t in ax.texts])
            return savefig(fig, *args, **kwargs)

        with patch.object(Figure, "savefig", new=inspect):
            for options in ({}, {"empty": True}, {"no_hundred": True}):
                job, _ = fixture(self.root / str(len(sizes)), **options)
                result = paper_ready.run([job], workers=1)[0]
                self.assertEqual(result.status, "created", result.error)
        np.testing.assert_allclose(sizes, [sizes[0]] * 3)

    def test_invalid_aggregates_or_reports_fail_without_publication(self):
        base = pq.read_table(self.aggregate).to_pylist()
        for case in (
            "missing",
            "duplicate",
            "unknown",
            "negative",
            "nan",
            "bad-share",
            "row-total",
            "population",
            "schema",
            "order",
        ):
            with self.subTest(case=case):
                rows, report = copy.deepcopy(base), dict(self.report)
                if case == "missing":
                    rows.pop()
                elif case == "duplicate":
                    rows[1] = dict(rows[0])
                elif case == "unknown":
                    rows[0]["RT_BASED_STRATEGY"] = "UNKNOWN"
                elif case == "negative":
                    rows[0]["COUNT"] = -1
                elif case == "nan":
                    rows[0]["PERCENTAGE"] = float("nan")
                elif case == "bad-share":
                    rows[0]["PERCENTAGE"] = 99.0
                elif case == "row-total":
                    rows[1]["RT_BASED_TOTAL"] += 1
                elif case == "population":
                    report["included_ip_count"] += 1
                elif case == "order":
                    report["strategies"] = report["strategies"][::-1]
                table = pa.Table.from_pylist(rows, schema=INTERSECTION_SCHEMA)
                if case == "schema":
                    table = table.drop(["COUNT"])
                pq.write_table(table, self.aggregate)
                self.job.source.write_text(json.dumps(report))
                before = stamp(self.aggregate), stamp(self.job.source)
                result = paper_ready.run([self.job], workers=1)[0]
                self.assertEqual(result.status, "failed", result.error)
                self.assertFalse(self.job.destination.exists())
                self.assertEqual(before, (stamp(self.aggregate), stamp(self.job.source)))

    def test_missing_aggregate_and_existing_outputs_are_not_repaired(self):
        self.aggregate.unlink()  # Test-owned fixture only.
        self.assertEqual(paper_ready.run([self.job], workers=1)[0].status, "failed")
        self.job.destination.write_bytes(b"keep existing artifact")
        before = stamp(self.job.destination)
        self.assertEqual(paper_ready.run([self.job])[0].status, "skipped-existing")
        self.assertEqual(stamp(self.job.destination), before)
        self.job.destination.unlink()
        receipt = artifacts.completion_path(self.job.destination)
        receipt.parent.mkdir(parents=True)
        receipt.write_text("keep orphan receipt")
        before = stamp(receipt)
        self.assertEqual(paper_ready.run([self.job], workers=1)[0].status, "failed")
        self.assertEqual(stamp(receipt), before)

    def test_campaign_selection_remap_and_dry_run(self):
        from ipid_analysis.paper_increment_plot import aggregate_path

        root = self.root / "reports"
        selected = []
        for site in ("eu", "na"):
            for protocol in ("icmp", "tcp-80", "udp-dns-53"):
                for mode in ("connection", "no-connection"):
                    job, _ = fixture(
                        root
                        / "continental/campaign"
                        / site
                        / "figures"
                        / (protocol + "_fixture")
                        / mode
                        / "comparison/rt-based-base_fixed-interval-base"
                    )
                    if site == "eu" and mode == "no-connection":
                        selected.append(job)
        before = {p: stamp(p) for p in root.rglob("*") if p.is_file()}
        jobs = paper_ready.discover(root, "campaign", sites=["eu"])
        self.assertEqual(set(jobs), set(selected))
        self.assertTrue(
            all(r.status == "would-create" for r in paper_ready.run(jobs, dry_run=True))
        )
        self.assertEqual(before, {p: stamp(p) for p in before})
        recorded = Path(
            "/recorded/processed/continental/campaign/eu/icmp_fixture/no-connection/comparison/rt-based-base_fixed-interval-base"
        ) / (NAME + ".pq")
        path = aggregate_path(
            self.job.source, {"aggregate": str(recorded)}, processed_root=self.root / "processed"
        )
        self.assertEqual(path, self.root / "processed" / Path(*recorded.parts[3:]))

    def test_spawned_render_and_completed_repeat(self):
        jobs = [self.job, fixture(self.root / "second")[0]]
        command = [
            sys.executable,
            "-B",
            "-m",
            "ipid_analysis.paper_ready",
            "--workers",
            "2",
            *(str(j.source) for j in jobs),
        ]
        self.assertIn(
            "2 created, 0 failed",
            subprocess.run(command, check=True, capture_output=True, text=True).stdout,
        )
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertIn(
            "0 created, 0 failed",
            subprocess.run(command, check=True, capture_output=True, text=True).stdout,
        )
        self.assertEqual(before, {p: stamp(p) for p in before})


if __name__ == "__main__":
    unittest.main()
