"""Exact selected OS cells, approved design and create-only publication."""

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
from ipid_analysis import paper_os_group_plot as plot
from ipid_analysis.paper_plot_style import _font, measure_heatmap_cell
from ipid_analysis.plot_os_group_strategy import AGGREGATE_SCHEMA, GROUP_INFO, HEATMAP_STRATEGIES

NAME = "n-rt-b_fi-m_operating-system-group-by-strategy"


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


def fixture(directory, *, missing=(), hundred=False):
    directory.mkdir(parents=True, exist_ok=True)
    source, aggregate = directory / (NAME + ".json"), directory / (NAME + ".pq")
    keys = [key for _, selection in plot.SELECTION for key in selection] + ["linux"]
    groups, rows = {}, []
    for i, group in enumerate(keys):
        if group in missing:
            continue
        section, label = GROUP_INFO[group]
        groups[group] = dict(section=section, label=label, ip_count=1_000_000)
        counts = [0] * 10
        counts[i % 10] = 1_000_000 if hundred else 990_000
        if not hundred:
            counts[(i + 1) % 10], counts[(i + 2) % 10] = 9999, 1
        for strategy, count in zip(HEATMAP_STRATEGIES, counts):
            rows.append(
                dict(
                    OS_SECTION=section,
                    OS_GROUP=group,
                    OS_GROUP_LABEL=label,
                    IPID_SELECTION_STRATEGY=strategy,
                    COUNT=count,
                    OS_GROUP_TOTAL=1_000_000,
                    PERCENTAGE=100 * count / 1_000_000,
                )
            )
    pq.write_table(pa.Table.from_pylist(rows, schema=AGGREGATE_SCHEMA), aggregate)
    source.write_text(
        json.dumps(
            dict(
                figure=paper_ready.OS_GROUP_KIND,
                groups=groups,
                aggregate=str(aggregate),
                os_group_count=len(groups),
                matched_grouped_ip_count=1_000_000 * len(groups),
                not_enough_samples_ip_count=sum(
                    row["COUNT"]
                    for row in rows
                    if row["IPID_SELECTION_STRATEGY"] == "NOT_ENOUGH_SAMPLES"
                ),
                sources={"os": "/do-not-read/os.pq", "strategies": "/do-not-read/strategies.pq"},
            )
        )
    )
    source.with_suffix(".pdf").write_bytes(b"unchanged original PDF; never read by adapter")
    return paper_ready.job_for(source), aggregate


class PaperOSGroupTest(unittest.TestCase):
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
        self.job, self.aggregate = fixture(self.root / "data")
        self.report = json.loads(self.job.source.read_text())

    def test_exact_values_selection_order_and_absent_groups(self):
        data = plot.load_data(self.aggregate, self.report)
        self.assertEqual([s["groups"] for s in data["sections"]], [s[1] for s in plot.SELECTION])
        self.assertEqual(data["sections"][0]["percentages"][0, 2], 0.0001)
        rows = pq.read_table(self.aggregate).to_pylist()
        pq.write_table(pa.Table.from_pylist(rows[::-1], schema=AGGREGATE_SCHEMA), self.aggregate)
        reverse = plot.load_data(self.aggregate, self.report)
        for a, b in zip(data["sections"], reverse["sections"]):
            np.testing.assert_array_equal(a["percentages"], b["percentages"])
        job, aggregate = fixture(self.root / "missing", missing=("ubuntu", "windows", "aruba"))
        data = plot.load_data(aggregate, json.loads(job.source.read_text()))
        self.assertEqual(data["sections"][0]["totals"][0], 0)
        self.assertEqual(data["sections"][1]["totals"][-1], 0)
        self.assertFalse(data["sections"][0]["counts"][0].any())

    def test_design_and_immutable_compact_only_render(self):
        from matplotlib.figure import Figure
        from matplotlib.text import Text

        before = {p: stamp(p) for p in self.job.source.parent.iterdir()}
        savefig = Figure.savefig

        def inspect(fig, *args, **kwargs):
            general, network, bar = fig.axes
            self.assertEqual(fig.get_size_inches()[0], 3.33)
            self.assertEqual({t.get_fontsize() for t in fig.findobj(Text)}, {7.0})
            self.assertEqual(general.title.get_text(), "General-Purpose OS")
            self.assertEqual(network.title.get_text(), "Network OS")
            self.assertAlmostEqual((general.bbox.y0 - network.bbox.y1) / fig.dpi, 0.20)
            ticks = general.xaxis.get_major_ticks()
            self.assertEqual(len(ticks), 10)
            self.assertTrue(all(t.tick1line.get_visible() for t in ticks))
            self.assertFalse(any(t.label1.get_visible() for t in ticks))
            for ax, (_, keys) in zip((general, network), plot.SELECTION):
                self.assertEqual(len(ax.texts), 10 * len(keys))
                self.assertTrue(all("\n" not in t.get_text() for t in ax.get_yticklabels()))
                self.assertEqual(
                    [t.get_text().replace("\n", " ").split(" (")[0] for t in ax.get_yticklabels()],
                    [GROUP_INFO[key][1] for key in keys],
                )
                bbox = ax.title.get_window_extent(fig.canvas.get_renderer())
                self.assertAlmostEqual((bbox.x0 + bbox.x1) / 2, (ax.bbox.x0 + ax.bbox.x1) / 2)
                self.assertIn("0.0", [t.get_text() for t in ax.texts])
                self.assertIn("-", [t.get_text() for t in ax.texts])
            self.assertIn(plot.Y_LABEL, [t.get_text() for t in fig.texts])
            self.assertEqual(network.get_xlabel(), "IP-ID Selection Strategy")
            self.assertTrue(all(t.get_rotation() == 30 for t in network.get_xticklabels()))
            self.assertAlmostEqual(bar.bbox.width / fig.dpi, 0.05)
            self.assertAlmostEqual(bar.bbox.height, 7 * measure_heatmap_cell(fig, _font()[0])[1])
            self.assertAlmostEqual(
                (bar.bbox.y0 + bar.bbox.y1) / 2, (network.bbox.y0 + general.bbox.y1) / 2
            )
            return savefig(fig, *args, **kwargs)

        with (
            patch.object(Figure, "savefig", new=inspect),
            patch("duckdb.connect", side_effect=AssertionError("no joins")),
            patch("pyarrow.parquet.read_table", side_effect=AssertionError("no per-IP reads")),
        ):
            result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "created", result.error)
        self.assertEqual(before, {p: stamp(p) for p in before})
        receipt = json.loads(artifacts.completion_path(self.job.destination).read_text())
        self.assertEqual(
            [Path(s["path"]) for s in receipt["sources"]], [self.job.source, self.aggregate]
        )
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(paper_ready.run([self.job])[0].status, "skipped-existing")
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_hundred_and_absent_group_labels_fit_without_clipping(self):
        job, _ = fixture(self.root / "hundred", hundred=True, missing=("openbsd", "aruba"))
        result = paper_ready.run([job], workers=1)[0]
        self.assertEqual(result.status, "created", result.error)
        missing = tuple(g for _, keys in plot.SELECTION for g in keys)
        job, _ = fixture(self.root / "all-empty", missing=missing)
        result = paper_ready.run([job], workers=1)[0]
        self.assertEqual(result.status, "created", result.error)

    def test_invalid_input_fails_without_publication(self):
        original = pq.read_table(self.aggregate).to_pylist()
        for case in (
            "missing",
            "duplicate",
            "null",
            "unknown",
            "count",
            "share",
            "nan",
            "label",
            "schema",
            "report",
            "total",
            "missing-group",
        ):
            with self.subTest(case=case):
                rows, report = copy.deepcopy(original), copy.deepcopy(self.report)
                if case == "missing":
                    rows.pop()
                elif case == "duplicate":
                    rows[-1] = rows[0]
                elif case == "null":
                    rows[0]["OS_GROUP"] = None
                elif case == "unknown":
                    rows[0]["IPID_SELECTION_STRATEGY"] = "unknown"
                elif case == "count":
                    rows[0]["COUNT"] = -1
                elif case == "share":
                    rows[0]["PERCENTAGE"] = 50.0
                elif case == "nan":
                    rows[0]["PERCENTAGE"] = float("nan")
                elif case == "label":
                    rows[0]["OS_GROUP_LABEL"] = "wrong"
                elif case == "report":
                    report["matched_grouped_ip_count"] += 1
                elif case == "total":
                    rows[0]["OS_GROUP_TOTAL"] += 1
                elif case == "missing-group":
                    report["groups"].pop("ubuntu")
                table = pa.Table.from_pylist(rows, schema=AGGREGATE_SCHEMA)
                if case == "schema":
                    table = table.drop(["COUNT"])
                pq.write_table(table, self.aggregate)
                self.job.source.write_text(json.dumps(report))
                before = stamp(self.job.source), stamp(self.aggregate)
                result = paper_ready.run([self.job], workers=1)[0]
                self.assertEqual(result.status, "failed", result.error)
                self.assertFalse(self.job.destination.exists())
                self.assertEqual(before, (stamp(self.job.source), stamp(self.aggregate)))

    def test_campaign_discovery_remap_and_dry_run(self):
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
                        / "merged/rt-based-base_fixed-interval-mass"
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
            "/stored/processed/continental/campaign/eu/icmp_fixture/no-connection/merged/rt-based-base_fixed-interval-mass"
        ) / (NAME + ".pq")
        path = aggregate_path(
            self.job.source,
            {"aggregate": str(recorded)},
            processed_root=self.root / "processed",
            phase=paper_ready.PHASES[paper_ready.OS_GROUP_KIND][1:],
        )
        self.assertEqual(path, self.root / "processed" / Path(*recorded.parts[3:]))

    def test_spawn_and_repeat(self):
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
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertIn("2 created, 0 failed", result.stdout)
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertIn(
            "0 created, 0 failed",
            subprocess.run(command, check=True, capture_output=True, text=True).stdout,
        )
        self.assertEqual(before, {p: stamp(p) for p in before})


if __name__ == "__main__":
    unittest.main()
