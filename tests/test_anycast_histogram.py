"""Exact empirical bins, reusable preparation and query-free histogram rendering."""

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from tests.test_anycast_probing import dataset
from tests.test_anycast_window import window_dataset
from tests.test_paper_probing_plot import fixture, stamp

from ipid_analysis import anycast_histogram as hist
from ipid_analysis import anycast_probing as filtered
from ipid_analysis.anycast_census import load_dataset
from ipid_analysis.paper_plot_style import _font


class AnycastHistogramTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        env = patch.dict(
            "os.environ",
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_SCRATCH_STORAGE": "disk",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
            },
        )
        env.start()
        self.addCleanup(env.stop)
        _font.cache_clear()
        self.addCleanup(_font.cache_clear)
        base = fixture(self.root)
        self.report = json.loads(base.source.read_text())
        self.census = dataset(self.root, ("203.0.113.0/24",))
        self.job = filtered.Job(base.source, base.kind, None, self.census)
        self.sources = {k: Path(p) for k, p in self.report["sources"].items()}
        vals = [
            0.0,
            9999.0,
            10000.0,
            19999.0,
            20000.0,
            29999.0,
            30000.0,
            1000000.0,
            None,
            -1.0,
            float("inf"),
            float("nan"),
        ]
        ips = [f"192.0.0.{n}" for n in range(len(vals))]
        pq.write_table(
            pa.table({"IP_ADDR": ips, "PROBING_INTERVALS": [[v] for v in vals]}),
            self.sources["rt_based"],
        )
        pq.write_table(
            pa.table(
                {"IP_ADDR": ips, "PROBING_INTERVALS": [[20000.0]] * 8 + [[v] for v in vals[8:]]}
            ),
            self.sources["fixed_interval"],
        )

    def prepared(self):
        self.assertEqual(hist.prepare(self.job, anycast=self.census), "created")
        aggregate, metadata = hist.paths(self.job.source, self.report, anycast=self.census)
        return (
            aggregate,
            metadata,
            hist.load_series(aggregate, metadata, self.job.source, anycast=self.census),
        )

    def test_actual_ip_counts_half_open_boundaries_and_constant_fixed_interval(self):
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        aggregate, metadata, rows = self.prepared()
        self.assertEqual(before, {p: stamp(p) for p in before})
        rt, fi = (
            next(r for r in rows if r["MODE"] == "RT-based"),
            next(r for r in rows if r["MODE"] == "Fixed-Interval"),
        )
        self.assertEqual(rt["IP_COUNT_MODE"], 8)
        self.assertEqual(
            [(b["LOWER_MS"], b["UPPER_MS"], b["IP_COUNT"], b["PERCENT"]) for b in rt["BINS"]],
            [
                (0, 10, 2, 25.0),
                (10, 20, 2, 25.0),
                (20, 30, 2, 25.0),
                (30, 40, 1, 12.5),
                (1000, 1010, 1, 12.5),
            ],
        )
        self.assertEqual(fi["BINS"], [dict(LOWER_MS=20, UPPER_MS=30, IP_COUNT=8, PERCENT=100.0)])
        self.assertEqual((fi["Q1_MS"], fi["MEDIAN_MS"], fi["Q3_MS"]), (20.0, 20.0, 20.0))
        self.assertEqual(sum(b["PERCENT"] for b in rt["BINS"]), 100.0)
        doc = json.loads(metadata.read_text())
        self.assertEqual(doc["histogram"], hist.METHOD)
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch.object(
            hist, "aggregate_histogram", side_effect=AssertionError("cached bins rescanned")
        ):
            # Keep the function source digest stable while spying on expensive work.
            with patch.object(hist, "paths", return_value=(aggregate, metadata)):
                self.assertEqual(hist.prepare(self.job, anycast=self.census), "skipped-current")
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_display_cutoff_keeps_entire_bin_and_percent_denominator_includes_tail(self):
        _, _, rows = self.prepared()
        rt = deepcopy(next(r for r in rows if r["MODE"] == "RT-based"))
        rt["CUTOFF_MS"] = 25.0
        _, prefixes = load_dataset(self.census)
        out = self.root / "tail.pq"
        hist.aggregate_histogram(self.sources, prefixes, [rt], out)
        row = pq.read_table(out).to_pylist()[0]
        self.assertEqual([b["LOWER_MS"] for b in hist.displayed_bins(row)], [0, 10, 20])
        self.assertEqual((row["DISPLAY_COUNT"], row["TAIL_COUNT"]), (6, 2))
        self.assertEqual(sum(b["PERCENT"] for b in hist.displayed_bins(row)), 75.0)
        self.assertEqual(sum(b["PERCENT"] for b in row["BINS"]), 100.0)

    def test_no_queries_or_kde_during_render_and_missing_bins_require_preparation(self):
        from matplotlib.axes import Axes

        result = filtered.worker(self.job)
        self.assertEqual(result.status, "failed")
        self.assertIn("--prepare", result.error)
        self.prepared()
        with (
            patch("ipid_analysis.runtime.connect", side_effect=AssertionError("render queried")),
            patch.object(Axes, "violinplot", side_effect=AssertionError("KDE used")),
        ):
            result = filtered.worker(self.job)
        self.assertEqual((result.status, result.error), ("created", ""))

    def test_bad_percent_counts_and_stale_metadata_are_rejected(self):
        _, metadata, rows = self.prepared()
        table, meta = self.root / "bad.pq", self.root / "bad.json"
        meta.write_bytes(metadata.read_bytes())
        for name, change in (
            ("percentage", lambda r: r[0]["BINS"][0].update(PERCENT=999.0)),
            ("count", lambda r: r[0].update(TAIL_COUNT=999)),
            ("width", lambda r: r[0]["BINS"][0].update(UPPER_MS=11)),
        ):
            corrupt = deepcopy(rows)
            change(corrupt)
            pq.write_table(pa.Table.from_pylist(corrupt, schema=hist.schema()), table)
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "histogram"):
                hist.load_series(table, meta, self.job.source, anycast=self.census)
        pq.write_table(pa.Table.from_pylist(rows, schema=hist.schema()), table)
        doc = json.loads(meta.read_text())
        doc["histogram"]["bin_width_ms"] = 20
        meta.write_text(json.dumps(doc))
        with self.assertRaisesRegex(ValueError, "metadata"):
            hist.load_series(table, meta, self.job.source, anycast=self.census)

    def test_spawn_workers_render_prepared_stateless_and_connection_histograms(self):
        base = fixture(self.root / "parallel", protocol="tcp-80", connection=True)
        connection = filtered.Job(base.source, base.kind, None, self.census)
        jobs = [self.job, connection]
        before = {job.source: stamp(job.source) for job in jobs}
        results = filtered.run(jobs, prepare=True, workers=2)
        self.assertEqual(
            [(r.status, r.error) for r in results], [("created", ""), ("created", "")]
        )
        self.assertEqual(before, {job.source: stamp(job.source) for job in jobs})

    def test_step_geometry_has_zero_gaps_equal_maximum_width_and_no_diagonals(self):
        import matplotlib.pyplot as plt

        _, _, rows = self.prepared()
        fig, ax = plt.subplots()
        self.addCleanup(plt.close, fig)
        for side, row in zip((True, False), sorted(rows, key=lambda r: r["MODE"], reverse=True)):
            body = hist.body(ax, row, 0, side)
            vertices = body.get_paths()[0].vertices
            self.assertAlmostEqual(max(abs(vertices[:, 0])), 0.41)
            self.assertTrue(
                all(a[0] == b[0] or a[1] == b[1] for a, b in zip(vertices, vertices[1:]))
            )
        rt = next(r for r in rows if r["MODE"] == "RT-based")
        self.assertEqual(hist.quartile_edge(rt, 0, 500.0), 0)

    def test_profile_refresh_reuses_bins_backs_up_old_pdf_and_exports_same_keys(self):
        from ipid_analysis.anycast_refresh import refresh
        from ipid_analysis.artifacts import completion_path
        from paper_values.anycast_metadata import read

        base = fixture(self.root / "migration")
        census = window_dataset(self.root / "migration-mask")
        job = filtered.Job(base.source, base.kind, None, census)
        self.assertEqual(filtered.run([job], prepare=True, workers=1)[0].status, "created")
        aggregate, metadata = hist.paths(
            job.source, json.loads(job.source.read_text()), anycast=census
        )
        original = job.destination.read_bytes()
        before = stamp(aggregate), stamp(metadata)
        marker = completion_path(job.destination)
        receipt = json.loads(marker.read_text())
        receipt["parameters"]["profile_sha256"] = "0" * 64
        marker.write_text(json.dumps(receipt))
        with patch(
            "ipid_analysis.runtime.connect",
            side_effect=AssertionError("cached preparation rescanned"),
        ):
            result = refresh([job], prepare=True, workers=1, backup_root=self.root / "backups")
        self.assertEqual(result[0].status, "updated")
        self.assertEqual((stamp(aggregate), stamp(metadata)), before)
        backups = list((self.root / "backups").glob("*/before/0/pdf.pdf"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_bytes(), original)
        values, context = read(
            job.destination, job.source, "fig.eu.icmp.rt-vs-fi-base.intervals-by-continent"
        )
        self.assertTrue(context)
        self.assertIn("results.eu.icmp.rt-vs-fi-base.anycast.addresses.excluded.count", values)
        self.assertEqual(
            json.loads(job.destination.with_suffix(".json").read_text())["histogram"], hist.METHOD
        )
