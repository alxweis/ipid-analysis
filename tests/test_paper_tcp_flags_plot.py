"""TCP-flags papers preserve the three stored distributions and existing files."""

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
from ipid_analysis.plot_tcp_flags_strategy import FLAG_GROUPS
from ipid_analysis.strategies import STRATEGY_NAMES

NAME = "n-rt-b_fi-m_tcp-flags-by-strategy"


def report():
    synack = {
        "REFLECTION": 1,
        "CONSTANT": 88,
        "SINGLE": 2,
        "PER_DESTINATION": 4,
        "PER_CONNECTION": 1,
        "PER_BUCKET": 2,
        "RANDOM": 1,
        "NOT_ENOUGH_SAMPLES": 1,
    }
    rst = {
        "REFLECTION": 1,
        "CONSTANT": 67,
        "SINGLE": 14,
        "PER_BUCKET": 9,
        "MULTI": 1,
        "RANDOM": 7,
        "NOT_ENOUGH_SAMPLES": 1,
    }
    counts = {
        "SYN-ACK": {key: synack.get(key, 0) for key in STRATEGY_NAMES},
        "RST": {key: 3 * rst.get(key, 0) for key in STRATEGY_NAMES},
    }
    counts["SYN-ACK/RST"] = {
        key: counts["SYN-ACK"][key] + counts["RST"][key] for key in STRATEGY_NAMES
    }
    return {
        "protocol": "tcp",
        "bars": {
            flags: {
                "total": sum(counts[flags].values()),
                "counts": counts[flags],
                "percentages": {
                    key: 100 * value / sum(counts[flags].values())
                    for key, value in counts[flags].items()
                },
            }
            for flags in FLAG_GROUPS
        },
    }


def fixture(directory):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (NAME + ".json")
    path.write_text(json.dumps(report()))
    path.with_suffix(".pdf").write_bytes(b"original PDF; never read or overwritten")
    return paper_ready.job_for(path)


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


class PaperTcpFlagsTest(unittest.TestCase):
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

    def test_source_percentages_are_copied_without_unweighted_averaging(self):
        data = report()
        values = paper_ready.plot_data(paper_ready.TCP_FLAGS_KIND, data)
        self.assertEqual(tuple(values), FLAG_GROUPS)
        self.assertEqual(values["SYN-ACK/RST"]["CONSTANT"], 72.25)
        self.assertEqual(values["SYN-ACK"]["CONSTANT"], 88)
        self.assertEqual(values["RST"]["CONSTANT"], 67)
        self.assertEqual(
            values, {flags: data["bars"][flags]["percentages"] for flags in FLAG_GROUPS}
        )

    def test_approved_layout_and_json_only_rendering(self):
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
            self.assertAlmostEqual(ax.get_position().x0 * 3.33, 0.80)
            self.assertAlmostEqual(ax.get_position().height * 1.4025, 0.6325)
            self.assertEqual([text.get_text() for text in ax.get_yticklabels()], list(FLAG_GROUPS))
            self.assertEqual({text.get_fontsize() for text in fig.findobj(Text)}, {7.0})
            title = next(text for text in fig.texts if text.get_text() == "TCP Flags")
            self.assertAlmostEqual(
                title.get_position()[1] * fig.bbox.height, (ax.bbox.y0 + ax.bbox.y1) / 2
            )
            self.assertFalse(any(isinstance(item, Polygon) for item in ax.patches))
            self.assertIn("1", [text.get_text() for text in ax.texts])
            for item in fig.artists:
                if isinstance(item, Rectangle):
                    self.assertAlmostEqual(item.get_width() * 3.33 * 72, 4)
                    self.assertAlmostEqual(item.get_height() * 1.4025 * 72, 4)
            return savefig(fig, *args, **kwargs)

        with (
            patch.object(Figure, "savefig", new=inspect),
            patch("duckdb.connect", side_effect=AssertionError("no queries")),
            patch("pyarrow.parquet.read_table", side_effect=AssertionError("no parquet")),
            patch("pyarrow.parquet.ParquetFile", side_effect=AssertionError("no parquet")),
            patch(
                "ipid_analysis.plot_tcp_flags_strategy.aggregate_tcp_flags_strategies",
                side_effect=AssertionError("no reaggregation"),
            ),
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

    def test_invalid_missing_or_non_tcp_data_is_not_published(self):
        for condition in ("missing-rst", "bad-total", "nan", "non-tcp"):
            with self.subTest(condition=condition):
                job = fixture(self.root / condition)
                data = report()
                if condition == "missing-rst":
                    del data["bars"]["RST"]
                elif condition == "bad-total":
                    data["bars"]["RST"]["percentages"]["CONSTANT"] = 66
                elif condition == "nan":
                    data["bars"]["SYN-ACK"]["percentages"]["CONSTANT"] = float("nan")
                else:
                    data["protocol"] = "udp-dns"
                job.source.write_text(json.dumps(data))
                before = stamp(job.source)
                self.assertEqual(paper_ready.run([job], workers=1)[0].status, "failed")
                self.assertFalse(job.destination.exists())
                self.assertEqual(stamp(job.source), before)

    def test_campaign_selection_and_dry_run(self):
        root = self.root / "reports"
        selected = []
        for site in ("eu", "na"):
            for run in ("icmp_fixture", "tcp-80_fixture", "udp-dns-53_fixture"):
                for mode in ("connection", "no-connection"):
                    job = fixture(
                        root
                        / "continental/campaign"
                        / site
                        / "figures"
                        / run
                        / mode
                        / "merged/rt-based-base_fixed-interval-mass"
                    )
                    if site == "eu" and run == "tcp-80_fixture" and mode == "no-connection":
                        selected.append(job)
        before = {path: stamp(path) for path in root.rglob("*") if path.is_file()}
        jobs = paper_ready.discover(root, "campaign", sites=["eu"])
        self.assertEqual(jobs, selected)
        self.assertEqual(paper_ready.run(jobs, dry_run=True)[0].status, "would-create")
        self.assertEqual(before, {path: stamp(path) for path in before})

    def test_existing_paper_is_preserved_even_if_invalid(self):
        job = fixture(self.root / "existing")
        job.destination.write_bytes(b"existing paper artifact")
        before = stamp(job.destination)
        self.assertEqual(paper_ready.run([job])[0].status, "skipped-existing")
        self.assertEqual(stamp(job.destination), before)

    def test_spawned_batch_and_repeat(self):
        jobs = [fixture(self.root / name) for name in ("eu", "na")]
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
