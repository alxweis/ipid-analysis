"""Increment papers use only compact aggregates and preserve all existing files."""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import artifacts, paper_increment_plot, paper_ready
from ipid_analysis.paper_figures import INCREMENT_SCHEMA, INCREMENT_STRATEGIES, MODES


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


def fixture(directory, *, connection=False):
    directory.mkdir(parents=True, exist_ok=True)
    prefix = "c" if connection else "n"
    source = directory / f"{prefix}-rt-b_fi-b_increment-distributions.json"
    aggregate = source.with_suffix(".pq")
    rows, summaries = [], {}
    for strategy in INCREMENT_STRATEGIES:
        for mode in MODES:
            values = (
                [1] if strategy in ("PER_DESTINATION", "PER_CONNECTION") else [1, 10, 100, 12544]
            )
            retained = 3 * len(values)
            summaries[f"{mode}:{strategy}"] = dict(
                total_count=retained + 1,
                retained_count=retained,
                clipped_count=1,
                cutoff_increment=values[-1],
            )
            for n, value in enumerate(values, 1):
                rows.append(
                    dict(
                        MODE=mode,
                        IPID_SELECTION_STRATEGY=strategy,
                        INCREMENT=value,
                        COUNT=3,
                        CUMULATIVE_COUNT=3 * n,
                        CUMULATIVE_PERCENTAGE=100 * 3 * n / retained,
                        TOTAL_COUNT=retained + 1,
                        RETAINED_COUNT=retained,
                        CLIPPED_COUNT=1,
                    )
                )
    pq.write_table(pa.Table.from_pylist(rows, schema=INCREMENT_SCHEMA), aggregate)
    source.write_text(
        json.dumps(
            dict(
                aggregate=str(aggregate.absolute()),
                percentile=0.999,
                series=summaries,
                **(
                    dict(
                        figure="increment-distributions",
                        protocol="tcp",
                        connection_mode="connection",
                        target="tcp.ipid.connection.rt-based.base+tcp.ipid.connection.fixed-interval.base",
                    )
                    if connection
                    else {}
                ),
            )
        )
    )
    source.with_suffix(".pdf").write_bytes(b"existing original PDF; never parse or replace")
    return source, aggregate


class PaperIncrementTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        environment = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_SCRATCH_STORAGE": "disk",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
                "MPLCONFIGDIR": str(self.root / "mpl"),
            },
        )
        environment.start()
        self.addCleanup(environment.stop)
        from ipid_analysis.paper_plot_style import _font

        _font.cache_clear()
        self.addCleanup(_font.cache_clear)
        self.source, self.aggregate = fixture(self.root / "data")
        self.job = paper_ready.job_for(self.source)

    def test_exact_aggregate_values_baseline_and_single_increment_series(self):
        data = paper_increment_plot.load_histogram(
            self.aggregate, json.loads(self.source.read_text())
        )
        self.assertEqual(len(data), 8)
        per_destination = next(row for row in data if row["strategy"] == "PER_DESTINATION")
        np.testing.assert_array_equal(per_destination["increments"], [1, 1])
        np.testing.assert_array_equal(per_destination["percentages"], [0, 100])
        single = next(row for row in data if row["strategy"] == "SINGLE")
        np.testing.assert_array_equal(single["percentages"], [0, 25, 50, 75, 100])

    def test_render_preserves_sources_and_records_both_dependencies(self):
        before = {path: stamp(path) for path in self.source.parent.iterdir()}
        with (
            patch("duckdb.connect", side_effect=AssertionError("no queries")),
            patch(
                "ipid_analysis.paper_figures.aggregate_increment_distributions",
                side_effect=AssertionError("no reaggregation"),
            ),
            patch(
                "ipid_analysis.paper_figures.plot_increment_distributions",
                side_effect=AssertionError("do not invoke original renderer"),
            ),
        ):
            result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "created", result.error)
        self.assertEqual(before, {path: stamp(path) for path in before})
        self.assertEqual(
            self.job.destination.name, "n-rt-b_fi-b_increment-distributions__paper.pdf"
        )
        artifacts.assert_complete(self.job.destination)
        receipt = json.loads(artifacts.completion_path(self.job.destination).read_text())
        self.assertEqual(
            [Path(item["path"]) for item in receipt["sources"]],
            [self.source.resolve(), self.aggregate.resolve()],
        )
        all_before = {path: stamp(path) for path in self.root.rglob("*") if path.is_file()}
        with patch.object(
            paper_increment_plot, "load_histogram", side_effect=AssertionError("skip")
        ):
            self.assertEqual(paper_ready.run([self.job])[0].status, "skipped-existing")
        self.assertEqual(all_before, {path: stamp(path) for path in all_before})

    def test_approved_geometry_and_decade_ticks(self):
        from matplotlib.figure import Figure
        from matplotlib.lines import Line2D
        from matplotlib.patches import Rectangle
        from matplotlib.text import Text

        savefig = Figure.savefig

        def inspect(fig, *args, **kwargs):
            ax = fig.axes[0]
            self.assertAlmostEqual(ax.get_position().height * fig.get_size_inches()[1], 0.8)
            self.assertAlmostEqual(fig.get_size_inches()[0], 3.33)
            self.assertEqual(
                [text.get_text() for text in ax.get_xticklabels()],
                ["10⁰", "10¹", "10²", "10³", "10⁴"],
            )
            below_one = [tick for tick in ax.xaxis.get_minorticklocs() if 0.85 <= tick < 1]
            np.testing.assert_allclose(below_one, [0.9])
            self.assertEqual({text.get_fontsize() for text in fig.findobj(Text)}, {7.0})
            for artist in fig.artists:
                if isinstance(artist, Rectangle):
                    self.assertAlmostEqual(artist.get_width() * 3.33 * 72, 4)
                    self.assertAlmostEqual(artist.get_height() * 1.44 * 72, 4)
                if isinstance(artist, Line2D):
                    self.assertAlmostEqual(np.diff(artist.get_xdata())[0] * 3.33 * 72, 10)
            return savefig(fig, *args, **kwargs)

        with patch.object(Figure, "savefig", new=inspect):
            result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "created", result.error)
        pdf = self.job.destination.read_bytes()
        box = [float(v) for v in re.search(rb"/MediaBox\s*\[([^]]+)\]", pdf)[1].split()]
        self.assertAlmostEqual(box[2], 239.76)
        self.assertNotIn(b"/Subtype /Type3", pdf)

    def test_missing_aggregate_fails_without_rebuilding(self):
        self.aggregate.unlink()
        with patch("duckdb.connect", side_effect=AssertionError("no fallback")):
            result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "failed")
        self.assertIn("FileNotFoundError", result.error)
        self.assertFalse(self.job.destination.exists())

    def test_empty_histogram_and_report_stay_empty(self):
        pq.write_table(pa.Table.from_pylist([], schema=INCREMENT_SCHEMA), self.aggregate)
        report = json.loads(self.source.read_text())
        report["series"] = {}
        self.source.write_text(json.dumps(report))
        self.assertEqual(paper_increment_plot.load_histogram(self.aggregate, report), [])
        result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "created", result.error)

    def test_full_16_bit_cutoff_keeps_decade_label_inside_page(self):
        table = pq.read_table(self.aggregate)
        values = table["INCREMENT"].to_pylist()
        values = [65535 if value == 12544 else value for value in values]
        table = table.set_column(
            table.schema.get_field_index("INCREMENT"),
            "INCREMENT",
            pa.array(values, type=pa.int32()),
        )
        pq.write_table(table, self.aggregate)
        report = json.loads(self.source.read_text())
        for summary in report["series"].values():
            if summary["cutoff_increment"] == 12544:
                summary["cutoff_increment"] = 65535
        self.source.write_text(json.dumps(report))
        result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "created", result.error)

    def test_corrupt_histogram_is_rejected_without_modifying_inputs(self):
        table = pq.read_table(self.aggregate)
        values = table["CUMULATIVE_PERCENTAGE"].to_pylist()
        values[-1] = 90
        table = table.set_column(
            table.schema.get_field_index("CUMULATIVE_PERCENTAGE"),
            "CUMULATIVE_PERCENTAGE",
            pa.array(values),
        )
        pq.write_table(table, self.aggregate)
        before = {path: stamp(path) for path in self.source.parent.iterdir()}
        result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "failed")
        self.assertIn("inconsistent increment histogram", result.error)
        self.assertFalse(self.job.destination.exists())
        self.assertEqual(before, {path: stamp(path) for path in before})

    def test_histogram_row_bound_is_checked_before_read(self):
        with patch.object(paper_increment_plot, "MAX_HISTOGRAM_ROWS", 2):
            result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "failed")
        self.assertIn("small one-row-per-increment histogram", result.error)

    def test_discovery_phase_selection_and_processed_root_remapping(self):
        root = self.root / "reports"
        tail = Path(
            "continental/campaign/eu/figures/icmp_run/no-connection/comparison/"
            "rt-based-base_fixed-interval-base"
        )
        source, aggregate = fixture(root / tail)
        wrong = (
            source.parent.parent.parent / "connection/comparison/rt-based-base_fixed-interval-base"
        )
        fixture(wrong)
        expected = paper_ready.job_for(source, processed_root=self.root / "processed")
        self.assertEqual(
            paper_ready.discover(
                root,
                "campaign",
                sites=["eu"],
                protocols=["icmp"],
                processed_root=self.root / "processed",
            ),
            [expected],
        )
        report = json.loads(source.read_text())
        report["aggregate"] = (
            "/old-vm/data/processed/continental/campaign/eu/icmp_run/no-connection/"
            "comparison/rt-based-base_fixed-interval-base/" + aggregate.name
        )
        mapped = paper_increment_plot.aggregate_path(
            source, report, processed_root=self.root / "processed"
        )
        self.assertEqual(
            mapped,
            self.root / "processed/continental/campaign/eu/icmp_run/"
            "no-connection/comparison/rt-based-base_fixed-interval-base" / aggregate.name,
        )

    def test_aggregate_change_during_render_is_not_published(self):
        def change(kind, data, output):
            output.write_bytes(b"rendered PDF")
            with self.aggregate.open("ab") as stream:
                stream.write(b"changed")

        with patch.object(paper_increment_plot, "draw", side_effect=change):
            result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "failed")
        self.assertIn("source input changed", result.error)
        self.assertFalse(self.job.destination.exists())

    def test_spawned_increment_batch(self):
        second, _ = fixture(self.root / "second")
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "ipid_analysis.paper_ready",
                "--workers",
                "2",
                str(self.source),
                str(second),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("2 created, 0 failed", result.stdout)


if __name__ == "__main__":
    unittest.main()
