"""Exact BASE-only OS cells, approved physical geometry and immutable outputs."""

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
from ipid_analysis.plot_os_group_strategy import AGGREGATE_SCHEMA, GROUP_INFO, HEATMAP_STRATEGIES

NAME = "c-rt-b_operating-system-group-by-strategy"


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


def fixture(directory, *, missing=(), hundred=False):
    directory.mkdir(parents=True, exist_ok=True)
    source, aggregate = directory / (NAME + ".json"), directory / (NAME + ".pq")
    rows, groups = [], {}
    for group in [g for _, keys in plot.SELECTION for g in keys] + ["linux"]:
        if group in missing:
            continue
        section, label = GROUP_INFO[group]
        total = {
            "ubuntu": 593399,
            "debian": 160599,
            "windows": 162197,
            "rhel": 100000,
            "f5": 100001,
        }.get(group, 10001)
        groups[group] = dict(section=section, label=label, ip_count=total)
        counts = {key: 0 for key in HEATMAP_STRATEGIES}
        counts["PER_CONNECTION"] = total if hundred else total * 85 // 100
        if not hundred:
            counts["SINGLE"], counts["CONSTANT"] = total - counts["PER_CONNECTION"] - 1, 1
        for strategy, count in counts.items():
            rows.append(
                dict(
                    OS_SECTION=section,
                    OS_GROUP=group,
                    OS_GROUP_LABEL=label,
                    IPID_SELECTION_STRATEGY=strategy,
                    COUNT=count,
                    OS_GROUP_TOTAL=total,
                    PERCENTAGE=100 * count / total,
                )
            )
    pq.write_table(pa.Table.from_pylist(rows, schema=AGGREGATE_SCHEMA), aggregate)
    source.write_text(
        json.dumps(
            dict(
                figure=paper_ready.OS_GROUP_KIND,
                protocol="tcp",
                connection_mode="connection",
                target="tcp.ipid.connection.rt-based.base",
                aggregate=str(aggregate),
                groups=groups,
                os_group_count=len(groups),
                matched_grouped_ip_count=sum(g["ip_count"] for g in groups.values()),
                not_enough_samples_ip_count=0,
            )
        )
    )
    source.with_suffix(".pdf").write_bytes(b"original PDF is never read or modified")
    return paper_ready.job_for(source), aggregate


class ConnectionOSGroupTest(unittest.TestCase):
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
        self.job, self.aggregate = fixture(self.root / "data")
        from ipid_analysis.paper_plot_style import _font

        _font.cache_clear()
        self.addCleanup(_font.cache_clear)

    def test_exact_seven_column_projection_and_absent_groups(self):
        report = json.loads(self.job.source.read_text())
        result = plot.load_data(self.aggregate, report, connection_base=True)
        original = {
            (r["OS_GROUP"], r["IPID_SELECTION_STRATEGY"]): r
            for r in pq.read_table(self.aggregate).to_pylist()
        }
        self.assertEqual(result["strategies"], plot.BASE_STRATEGIES)
        self.assertEqual(len(result["strategies"]), 7)
        for section, (_, keys) in zip(result["sections"], plot.SELECTION):
            self.assertEqual(section["groups"], keys)
            for i, group in enumerate(keys):
                for j, strategy in enumerate(plot.BASE_STRATEGIES):
                    self.assertEqual(section["counts"][i, j], original[group, strategy]["COUNT"])
                    self.assertEqual(
                        section["percentages"][i, j], original[group, strategy]["PERCENTAGE"]
                    )
        job, aggregate = fixture(self.root / "missing", missing=("ubuntu", "aruba"))
        data = plot.load_data(aggregate, json.loads(job.source.read_text()), connection_base=True)
        self.assertEqual(data["sections"][0]["totals"][0], 0)
        self.assertFalse(data["sections"][0]["counts"][0].any())

    def test_approved_geometry_compact_only_and_immutable_render(self):
        from matplotlib.figure import Figure
        from matplotlib.text import Text

        before = {p: stamp(p) for p in self.job.source.parent.iterdir()}
        savefig = Figure.savefig

        def inspect(fig, *args, **kwargs):
            general, network, bar = fig.axes
            self.assertEqual(fig.get_size_inches()[0], 3.33)
            self.assertEqual({t.get_fontsize() for t in fig.findobj(Text)}, {7.0})
            for ax, row_count in zip((general, network), (8, 12)):
                self.assertAlmostEqual(ax.bbox.width / 7 / fig.dpi * 72, 11.9145)
                self.assertAlmostEqual(ax.bbox.height / row_count / fig.dpi * 72, 10)
                self.assertEqual(len(ax.texts), 7 * row_count)
                title = ax.title.get_window_extent(fig.canvas.get_renderer())
                self.assertAlmostEqual((title.x0 + title.x1) / 2, (ax.bbox.x0 + ax.bbox.x1) / 2)
            self.assertEqual(
                [t.get_text() for t in network.get_xticklabels()],
                [
                    "Reflection",
                    "Constant",
                    "Single",
                    "Per-Connection",
                    "Per-Destination",
                    "Per-Bucket",
                    "Unclassified",
                ],
            )
            self.assertEqual(len(general.xaxis.get_major_ticks()), 7)
            self.assertTrue(
                all(
                    t.tick1line.get_visible() and not t.label1.get_visible()
                    for t in general.xaxis.get_major_ticks()
                )
            )
            self.assertAlmostEqual((general.bbox.y0 - network.bbox.y1) / fig.dpi * 72, 14.4)
            np.testing.assert_allclose(
                [bar.bbox.width, bar.bbox.height],
                np.asarray((3.6, 70)) * fig.dpi / 72,
            )
            self.assertAlmostEqual((bar.bbox.x0 - network.bbox.x1) / fig.dpi * 72, 4)
            self.assertEqual(bar.yaxis.labelpad, 0)
            self.assertAlmostEqual(
                (bar.bbox.y0 + bar.bbox.y1) / 2, (general.bbox.y1 + network.bbox.y0) / 2
            )
            self.assertIn("Ubuntu (593k)", [t.get_text() for t in general.get_yticklabels()])
            self.assertIn("Debian (161k)", [t.get_text() for t in general.get_yticklabels()])
            self.assertIn(
                "Microsoft Windows (162k)", [t.get_text() for t in general.get_yticklabels()]
            )
            self.assertIn("RHEL (100k)", [t.get_text() for t in general.get_yticklabels()])
            return savefig(fig, *args, **kwargs)

        with (
            patch.object(Figure, "savefig", new=inspect),
            patch("duckdb.connect", side_effect=AssertionError("no queries")),
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

    def test_nonzero_omitted_columns_are_rejected_even_for_unselected_groups(self):
        rows = pq.read_table(self.aggregate).to_pylist()
        base_report = json.loads(self.job.source.read_text())
        for key in plot.BASE_OMITTED:
            for group in ("ubuntu", "linux"):
                with self.subTest(key=key, group=group):
                    changed, report = copy.deepcopy(rows), copy.deepcopy(base_report)
                    for row in changed:
                        if row["OS_GROUP"] != group:
                            continue
                        if row["IPID_SELECTION_STRATEGY"] == key:
                            row["COUNT"] += 1
                        elif row["IPID_SELECTION_STRATEGY"] == "PER_CONNECTION":
                            row["COUNT"] -= 1
                        row["PERCENTAGE"] = 100 * row["COUNT"] / row["OS_GROUP_TOTAL"]
                    if key == "NOT_ENOUGH_SAMPLES":
                        report["not_enough_samples_ip_count"] += 1
                    pq.write_table(
                        pa.Table.from_pylist(changed, schema=AGGREGATE_SCHEMA), self.aggregate
                    )
                    self.job.source.write_text(json.dumps(report))
                    result = paper_ready.run([self.job], workers=1)[0]
                    self.assertEqual(result.status, "failed")
                    self.assertIn("omitted", result.error)
                    self.assertFalse(self.job.destination.exists())

    def test_invalid_report_context_fails(self):
        base = json.loads(self.job.source.read_text())
        for field, value in (
            ("protocol", "udp-dns"),
            ("connection_mode", "no-connection"),
            ("target", "tcp.ipid.connection.fixed-interval.mass"),
            ("target", None),
        ):
            report = copy.deepcopy(base)
            if value is None:
                report.pop(field)
            else:
                report[field] = value
            self.job.source.write_text(json.dumps(report))
            before = stamp(self.job.source)
            result = paper_ready.run([self.job], workers=1)[0]
            self.assertEqual(result.status, "failed", result.error)
            self.assertFalse(self.job.destination.exists())
            self.assertEqual(stamp(self.job.source), before)

    def test_campaign_discovery_and_processed_root_remapping(self):
        from ipid_analysis.paper_increment_plot import aggregate_path

        root = self.root / "reports"
        expected = []
        for site in ("eu", "na"):
            for run in ("tcp-80_fixture", "icmp_fixture", "udp-dns-53_fixture"):
                for mode in ("connection", "no-connection"):
                    for phase in ("rt-based-base", "merged/rt-based-base_fixed-interval-mass"):
                        job, _ = fixture(
                            root / "continental/campaign" / site / "figures" / run / mode / phase
                        )
                        if (
                            site == "eu"
                            and run.startswith("tcp")
                            and mode == "connection"
                            and phase == "rt-based-base"
                        ):
                            expected.append(job)
        before = {p: stamp(p) for p in root.rglob("*") if p.is_file()}
        jobs = paper_ready.discover(root, "campaign", sites=["eu"], protocols=["tcp"])
        self.assertEqual(set(jobs), set(expected))
        self.assertTrue(
            all(r.status == "would-create" for r in paper_ready.run(jobs, dry_run=True))
        )
        self.assertEqual(before, {p: stamp(p) for p in before})
        recorded = Path(
            "/stored/processed/continental/campaign/eu/tcp-80_fixture/connection/rt-based-base"
        ) / (NAME + ".pq")
        result = aggregate_path(
            self.job.source,
            {"aggregate": str(recorded)},
            processed_root=self.root / "remap",
            phase=("rt-based-base",),
            mode="connection",
        )
        self.assertEqual(result, self.root / "remap" / Path(*recorded.parts[3:]))
        with self.assertRaises(ValueError):
            aggregate_path(
                self.job.source,
                {"aggregate": str(recorded)},
                processed_root=self.root / "remap",
                phase=("rt-based-base",),
            )

    def test_hundred_empty_rows_and_spawned_repeat(self):
        from matplotlib.figure import Figure

        job, _ = fixture(self.root / "hundred", hundred=True, missing=("openbsd", "aruba"))
        savefig = Figure.savefig

        def inspect(fig, *args, **kwargs):
            annotations = [t.get_text() for ax in fig.axes[:2] for t in ax.texts]
            self.assertIn("100", annotations)
            self.assertNotIn("100.0", annotations)
            self.assertEqual(set(annotations), {"100", "-"})
            return savefig(fig, *args, **kwargs)

        with patch.object(Figure, "savefig", new=inspect):
            self.assertEqual(paper_ready.run([job], workers=1)[0].status, "created")
        missing = tuple(g for _, keys in plot.SELECTION for g in keys)
        empty, _ = fixture(self.root / "all-empty", missing=missing)
        self.assertEqual(paper_ready.run([empty], workers=1)[0].status, "created")
        command = [
            sys.executable,
            "-B",
            "-m",
            "ipid_analysis.paper_ready",
            "--workers",
            "2",
            str(self.job.source),
            str(fixture(self.root / "second")[0].source),
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
