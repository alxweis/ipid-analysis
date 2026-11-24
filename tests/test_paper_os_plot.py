"""Approved OS paper layout, JSON-only populations and shared pastel colors."""

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

from ipid_analysis import artifacts, paper_ready
from ipid_analysis.paper_plot_style import _font
from ipid_analysis.plot_itdk_os import _group_color
from ipid_analysis.plot_os_group_strategy import (
    GROUP_INFO,
    PAPER_OS_COLOR_PALETTE,
    PAPER_OS_GROUP_COLORS,
    PAPER_OS_OTHER_COLOR,
)

NAME = "n-rt-b_fi-m_itdk-role-by-os"
ROLES = ("Transit-Observed", "No Transit Evidence")
GROUPS = (
    "ubuntu",
    "debian",
    "centos",
    "windows",
    "cisco",
    "huawei",
    "mikrotik",
    "router",
    "server",
    "__other__",
)


def report():
    values = ((12, 19, 1, 5, 23, 4, 22, 2, 6, 6), (52, 19, 4, 9, 1, 0, 3, 0, 6, 6))
    return {
        # Old report colors/labels must not override the central approved style.
        "plot_groups": [
            {"group": key, "color": "#000000", "label": "stale label"} for key in GROUPS
        ],
        "plot_percentages": {role: dict(zip(GROUPS, row)) for role, row in zip(ROLES, values)},
        "plot_counts": {
            role: {key: value * factor for key, value in zip(GROUPS, row)}
            for role, row, factor in zip(ROLES, values, (1, 3))
        },
    }


def fixture(directory):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (NAME + ".json")
    path.write_text(json.dumps(report()))
    path.with_suffix(".pdf").write_bytes(b"original PDF is never read or changed")
    return paper_ready.job_for(path)


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


class PaperOSTest(unittest.TestCase):
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

    def test_source_selection_order_and_independent_populations_are_preserved(self):
        source = report()
        source["plot_groups"].reverse()
        before = copy.deepcopy(source)
        data = paper_ready.plot_data("itdk-role-by-os", source)
        self.assertEqual(data.groups, GROUPS[::-1])
        self.assertEqual(data.percentages, source["plot_percentages"])
        self.assertEqual(data.counts, source["plot_counts"])
        self.assertEqual(data.role_totals, dict(zip(ROLES, (100, 300))))
        self.assertEqual(source, before)

    def test_approved_palette_is_shared_with_original_os_plots(self):
        self.assertEqual(
            PAPER_OS_COLOR_PALETTE,
            (
                "#ECD37E",
                "#94AECB",
                "#F5AA60",
                "#ED9393",
                "#9ACBC7",
                "#C5A0BC",
                "#82D486",
                "#EDA7D2",
                "#BE9D8B",
                "#AED3CE",
                "#F5B2BA",
                "#BC9AE6",
                "#A7DDA0",
                "#D5BF7A",
                "#83B9B5",
                "#E4A0BA",
                "#B1A7A3",
                "#E2BBD9",
                "#F7D2DF",
                "#9AD4EC",
            ),
        )
        self.assertEqual(len(PAPER_OS_GROUP_COLORS), 94)
        self.assertEqual(set(PAPER_OS_GROUP_COLORS), set(GROUP_INFO))
        self.assertEqual(len(set(PAPER_OS_GROUP_COLORS.values())), 20)
        self.assertEqual(_group_color("ubuntu"), "#ECD37E")
        self.assertEqual(_group_color("rocky-linux"), _group_color("ubuntu"))
        self.assertEqual(_group_color("mikrotik"), "#BC9AE6")
        self.assertEqual(_group_color("__other__"), PAPER_OS_OTHER_COLOR)
        self.assertEqual(PAPER_OS_OTHER_COLOR, "#D0C8C4")

    def test_json_only_approved_layout_and_immutable_repeat(self):
        from matplotlib.colors import to_rgba
        from matplotlib.figure import Figure
        from matplotlib.patches import Polygon, Rectangle
        from matplotlib.text import Text

        job = fixture(self.root / "input")
        before = {path: stamp(path) for path in job.source.parent.iterdir()}
        savefig = Figure.savefig

        def inspect(fig, *args, **kwargs):
            ax = fig.axes[0]
            self.assertEqual(tuple(fig.get_size_inches()), (3.33, 1.1715))
            self.assertAlmostEqual(ax.get_position().x0 * 3.33, 0.59)
            self.assertAlmostEqual(ax.get_position().height * 1.1715, 0.4015)
            self.assertEqual([t.get_text() for t in ax.get_yticklabels()], ["Router", "All Other"])
            self.assertEqual(ax.get_xlabel(), "OS Distribution [%]")
            self.assertEqual({text.get_fontsize() for text in fig.findobj(Text)}, {7.0})
            title = next(t for t in fig.texts if t.get_text() == "Device Type")
            self.assertAlmostEqual(
                title.get_position()[1] * fig.bbox.height, (ax.bbox.y0 + ax.bbox.y1) / 2
            )
            self.assertFalse(any(isinstance(p, Polygon) for p in ax.patches))
            self.assertIn("1", [t.get_text() for t in ax.texts])
            self.assertIn("Microsoft Windows", [t.get_text() for t in fig.texts])
            self.assertNotIn("stale label", [t.get_text() for t in fig.texts])
            self.assertEqual(ax.patches[0].get_facecolor(), to_rgba("#ECD37E"))
            for item in fig.artists:
                if isinstance(item, Rectangle):
                    self.assertAlmostEqual(item.get_width() * 3.33 * 72, 4)
                    self.assertAlmostEqual(item.get_height() * 1.1715 * 72, 4)
            return savefig(fig, *args, **kwargs)

        with (
            patch.object(Figure, "savefig", new=inspect),
            patch("duckdb.connect", side_effect=AssertionError("no queries")),
            patch("pyarrow.parquet.read_table", side_effect=AssertionError("no parquet")),
            patch("pyarrow.parquet.ParquetFile", side_effect=AssertionError("no parquet")),
            patch(
                "ipid_analysis.plot_itdk_os.write_os_groups",
                side_effect=AssertionError("no OS analysis"),
            ),
        ):
            result = paper_ready.run([job], workers=1)[0]
        self.assertEqual(result.status, "created", result.error)
        self.assertEqual(before, {p: stamp(p) for p in before})
        artifacts.assert_complete(job.destination)
        all_before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch("ipid_analysis.paper_plot_style.draw", side_effect=AssertionError("must skip")):
            self.assertEqual(paper_ready.run([job])[0].status, "skipped-existing")
        self.assertEqual(all_before, {p: stamp(p) for p in all_before})
        receipt = json.loads(artifacts.completion_path(job.destination).read_text())
        self.assertIn("profile_sha256", receipt["parameters"])

    def test_invalid_reports_fail_without_publication(self):
        for condition in (
            "unknown",
            "duplicate",
            "missing-role",
            "missing-group",
            "nan",
            "bad-sum",
            "bad-count",
            "bool",
            "inconsistent",
            "too-many",
        ):
            with self.subTest(condition=condition):
                job = fixture(self.root / condition)
                source = report()
                if condition == "unknown":
                    source["plot_groups"][0]["group"] = "unknown-os"
                elif condition == "duplicate":
                    source["plot_groups"][0] = source["plot_groups"][1]
                elif condition == "missing-role":
                    del source["plot_percentages"][ROLES[1]]
                elif condition == "missing-group":
                    del source["plot_counts"][ROLES[0]]["ubuntu"]
                elif condition == "nan":
                    source["plot_percentages"][ROLES[0]]["ubuntu"] = float("nan")
                elif condition == "bad-sum":
                    source["plot_percentages"][ROLES[0]]["ubuntu"] = 13
                elif condition == "bad-count":
                    source["plot_counts"][ROLES[0]]["ubuntu"] = -1
                elif condition == "bool":
                    source["plot_percentages"][ROLES[0]]["centos"] = True
                elif condition == "inconsistent":
                    source["plot_counts"][ROLES[0]]["ubuntu"] += 1
                else:
                    source["plot_groups"].append({"group": "linux"})
                job.source.write_text(json.dumps(source))
                before = stamp(job.source)
                self.assertEqual(paper_ready.run([job], workers=1)[0].status, "failed")
                self.assertFalse(job.destination.exists())
                self.assertFalse(artifacts.completion_path(job.destination).exists())
                self.assertEqual(stamp(job.source), before)

    def test_empty_roles_and_fewer_groups_are_not_fabricated(self):
        for length in (0, 1, 3, 10):
            job = fixture(self.root / str(length))
            source = report()
            source["plot_groups"] = source["plot_groups"][:length]
            for role in ROLES:
                source["plot_counts"][role] = dict.fromkeys(GROUPS[:length], 0)
                source["plot_percentages"][role] = dict.fromkeys(GROUPS[:length], 0)
            if length:
                source["plot_counts"][ROLES[1]]["ubuntu"] = 1
                source["plot_percentages"][ROLES[1]]["ubuntu"] = 100
            job.source.write_text(json.dumps(source))
            result = paper_ready.run([job], workers=1)[0]
            self.assertEqual(result.status, "created", result.error)
            self.assertEqual(
                sum(
                    paper_ready.plot_data("itdk-role-by-os", source).percentages[ROLES[0]].values()
                ),
                0,
            )

    def test_discovery_all_protocols_and_dry_run(self):
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
                    if site == "eu" and mode == "no-connection":
                        selected.append(job)
        before = {p: stamp(p) for p in root.rglob("*") if p.is_file()}
        jobs = paper_ready.discover(root, "campaign", sites=["eu"])
        self.assertEqual(set(jobs), set(selected + [paper_ready.final_job(j) for j in selected]))
        self.assertTrue(
            all(r.status == "would-create" for r in paper_ready.run(jobs, dry_run=True))
        )
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_spawned_batch_existing_pdf_and_orphan_receipt(self):
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
        job = fixture(self.root / "orphan")
        receipt = artifacts.completion_path(job.destination)
        receipt.parent.mkdir(parents=True)
        receipt.write_text("preserve orphan receipt")
        before = stamp(receipt)
        self.assertEqual(paper_ready.run([job], workers=1)[0].status, "failed")
        self.assertEqual(stamp(receipt), before)
        self.assertFalse(job.destination.exists())


if __name__ == "__main__":
    unittest.main()
