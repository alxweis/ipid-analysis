"""Three-bar paper refinement reads its existing JSON only and preserves files."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from ipid_analysis import artifacts, paper_ready
from ipid_analysis.paper_plot_style import _font
from ipid_analysis.plot_strategy_refinement import CONNECTION_MODE, MODES_WITH_CONNECTION

NAME = "n-rt-b_fi-m_measurement-type-by-strategy-with-connection"


def report():
    return {
        "protocol": "tcp",
        "bars": {
            "RT-based": {
                "percentages": {
                    "CONSTANT": 78,
                    "SINGLE": 8,
                    "PER_DESTINATION": 2,
                    "PER_BUCKET": 7,
                    "UNCLASSIFIED": 5,
                }
            },
            "Fixed-Interval": {
                "percentages": {"MULTI": 13, "RANDOM": 80, "NOT_ENOUGH_SAMPLES": 7}
            },
            CONNECTION_MODE: {
                "percentages": {
                    "SINGLE": 2,
                    "PER_DESTINATION": 5,
                    "PER_CONNECTION": 91,
                    "PER_BUCKET": 2,
                }
            },
        },
    }


def fixture(directory, *, with_connection=True):
    directory.mkdir(parents=True, exist_ok=True)
    name = NAME if with_connection else NAME.removesuffix("-with-connection")
    source = directory / (name + ".json")
    source.write_text(json.dumps(report()))
    source.with_suffix(".pdf").write_bytes(b"original PDF; never read or changed")
    return paper_ready.job_for(source)


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


class PaperConnectionTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
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
        _font.cache_clear()
        self.addCleanup(_font.cache_clear)

    def test_three_independent_distributions_are_read_without_reweighting(self):
        data = report()
        result = paper_ready.plot_data(paper_ready.CONNECTION_KIND, data)
        self.assertEqual(tuple(result), MODES_WITH_CONNECTION)
        self.assertEqual(result[CONNECTION_MODE]["PER_CONNECTION"], 91)
        self.assertEqual(result["Fixed-Interval"]["NOT_ENOUGH_SAMPLES"], 7)
        self.assertEqual(
            result, {mode: data["bars"][mode]["percentages"] for mode in MODES_WITH_CONNECTION}
        )

    def test_approved_three_bar_geometry_and_read_only_json_rendering(self):
        from matplotlib.figure import Figure
        from matplotlib.patches import Polygon, Rectangle
        from matplotlib.text import Text

        job = fixture(self.root / "input")
        before = {path: stamp(path) for path in job.source.parent.iterdir()}
        savefig = Figure.savefig

        def inspect(fig, *args, **kwargs):
            ax = fig.axes[0]
            self.assertAlmostEqual(fig.get_size_inches()[0], 3.33)
            self.assertAlmostEqual(fig.get_size_inches()[1], 1.4025)
            self.assertAlmostEqual(ax.get_position().x0 * 3.33, 1.01)
            self.assertAlmostEqual(ax.get_position().height * 1.4025, 0.6325)
            self.assertEqual(
                [text.get_text() for text in ax.get_yticklabels()],
                ["RT-based", "Fixed-Interval", "RT-based &\nConnection-oriented"],
            )
            self.assertEqual({text.get_fontsize() for text in fig.findobj(Text)}, {7.0})
            title = next(text for text in fig.texts if text.get_text() == "Measurement Type")
            frame_center = (ax.bbox.y0 + ax.bbox.y1) / 2
            self.assertAlmostEqual(
                title.get_position()[1] * fig.bbox.height, frame_center - 2 * fig.dpi / 72
            )
            guides = [item for item in ax.patches if isinstance(item, Polygon)]
            self.assertEqual(len(guides), 1)
            self.assertEqual(
                guides[0].get_xy()[:4].tolist(), [[95, 1.47], [100, 1.47], [100, 1.05], [0, 1.05]]
            )
            self.assertAlmostEqual(guides[0].get_alpha(), 0.21)
            for item in fig.artists:
                if isinstance(item, Rectangle):
                    self.assertAlmostEqual(item.get_width() * 3.33 * 72, 4)
                    self.assertAlmostEqual(item.get_height() * 1.4025 * 72, 4)
            self.assertIn("91", [text.get_text() for text in ax.texts])
            return savefig(fig, *args, **kwargs)

        with (
            patch.object(Figure, "savefig", new=inspect),
            patch("duckdb.connect", side_effect=AssertionError("no queries")),
            patch("pyarrow.parquet.read_table", side_effect=AssertionError("no parquet reads")),
            patch("pyarrow.parquet.ParquetFile", side_effect=AssertionError("no parquet reads")),
        ):
            result = paper_ready.run([job], workers=1)[0]
        self.assertEqual(result.status, "created", result.error)
        self.assertEqual(before, {path: stamp(path) for path in before})
        self.assertEqual(job.destination.name, NAME + "__paper.pdf")
        artifacts.assert_complete(job.destination)
        all_before = {path: stamp(path) for path in self.root.rglob("*") if path.is_file()}
        with patch("ipid_analysis.paper_plot_style.draw", side_effect=AssertionError("must skip")):
            self.assertEqual(paper_ready.run([job])[0].status, "skipped-existing")
        self.assertEqual(all_before, {path: stamp(path) for path in all_before})

    def test_invalid_or_missing_connection_bar_is_not_published(self):
        for kind in ("missing", "wrong-total", "nan", "wrong-protocol"):
            with self.subTest(kind=kind):
                job = fixture(self.root / kind)
                data = report()
                if kind == "missing":
                    del data["bars"][CONNECTION_MODE]
                elif kind == "wrong-total":
                    data["bars"][CONNECTION_MODE]["percentages"]["PER_CONNECTION"] = 90
                elif kind == "nan":
                    data["bars"][CONNECTION_MODE]["percentages"]["PER_CONNECTION"] = float("nan")
                else:
                    data["protocol"] = "icmp"
                job.source.write_text(json.dumps(data))
                before = stamp(job.source)
                result = paper_ready.run([job], workers=1)[0]
                self.assertEqual(result.status, "failed")
                self.assertFalse(job.destination.exists())
                self.assertEqual(stamp(job.source), before)

    def test_discovery_selects_only_tcp_no_connection_merged_reports(self):
        root = self.root / "reports"
        selected = []
        for site in ("eu", "na"):
            for run in ("icmp_fixture", "tcp-80_fixture", "udp-dns-53_fixture"):
                for connection in ("no-connection", "connection"):
                    path = (
                        root
                        / "continental/campaign"
                        / site
                        / "figures"
                        / run
                        / connection
                        / "merged/rt-based-base_fixed-interval-mass"
                    )
                    job = fixture(path)
                    if site == "eu" and run == "tcp-80_fixture" and connection == "no-connection":
                        selected.append(job)
        before = {path: stamp(path) for path in root.rglob("*") if path.is_file()}
        jobs = paper_ready.discover(
            root, "campaign", sites=["eu"], protocols=["icmp", "tcp", "udp"]
        )
        self.assertEqual(jobs, selected)
        self.assertEqual(paper_ready.run(jobs, dry_run=True)[0].status, "would-create")
        self.assertEqual(before, {path: stamp(path) for path in before})

    def test_existing_three_bar_paper_is_preserved_even_if_invalid(self):
        job = fixture(self.root / "existing")
        job.destination.write_bytes(b"pre-existing paper PDF")
        before = stamp(job.destination)
        self.assertEqual(paper_ready.run([job])[0].status, "skipped-existing")
        self.assertEqual(stamp(job.destination), before)

    def test_spawned_mixed_two_and_three_bar_batch(self):
        jobs = [fixture(self.root / "mixed", with_connection=value) for value in (False, True)]
        command = [
            sys.executable,
            "-B",
            "-m",
            "ipid_analysis.paper_ready",
            "--workers",
            "2",
            *(str(job.source) for job in jobs),
        ]
        first = subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertIn("2 created, 0 failed", first.stdout)
        before = {path: stamp(path) for path in self.root.rglob("*") if path.is_file()}
        second = subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertIn("0 created, 0 failed", second.stdout)
        self.assertEqual(before, {path: stamp(path) for path in before})


if __name__ == "__main__":
    unittest.main()
