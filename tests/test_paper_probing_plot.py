"""Paper violins use p99.9/256 aggregates and actual population quartiles."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import artifacts, paper_ready
from ipid_analysis import paper_probing_plot as probing
from ipid_analysis.paper_figures import (
    CONTINENT_NAMES,
    CONTINENT_ORDER,
    INTERVAL_SCHEMA,
    MODES,
    aggregate_probing_intervals,
)
from ipid_analysis.paper_plot_style import _font

NAME = "n-rt-b_fi-b_probing-intervals-by-continent"


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


def fixture(root, site="eu", protocol="icmp", *, connection=False):
    prefix = "c" if connection else "n"
    name = f"{prefix}-rt-b_fi-b_probing-intervals-by-continent"
    stem = protocol + "_fixture"
    mode = "connection" if connection else "no-connection"
    relative = Path("continental/campaign") / site / stem / mode
    phase = relative / "comparison/rt-based-base_fixed-interval-base"
    report = root / "reports" / phase / (name + ".json")
    aggregate = root / "processed" / phase / (name + ".pq")
    report.parent.mkdir(parents=True)
    aggregate.parent.mkdir(parents=True)
    rt = root / "processed" / relative / f"rt-based-base/{prefix}-rt-b_probing-intervals.pq"
    fi = root / "processed" / relative / f"fixed-interval-base/{prefix}-fi-b_probing-intervals.pq"
    geo = aggregate.with_name(f"{prefix}-rt-b_fi-b_ip-continents.pq")
    addresses, codes, values = [], [], []
    for index, code in enumerate(CONTINENT_ORDER):
        for n in range(400):
            addresses.append(
                f"192.{index}.{n // 256}.{n % 256}"
            )  # Join keys only; no network access.
            codes.append(code)
            values.append((n + 1) * (index + 1) * 1000.0)
    rt.parent.mkdir(parents=True)
    fi.parent.mkdir(parents=True)
    pq.write_table(
        pa.table({"IP_ADDR": addresses, "PROBING_INTERVALS": [[v] * 3 for v in values]}), rt
    )
    pq.write_table(
        pa.table(
            {"IP_ADDR": addresses, "PROBING_INTERVALS": [[20000.0, 20000.0]] * len(addresses)}
        ),
        fi,
    )
    pq.write_table(
        pa.table(
            {
                "IP_ADDR": addresses,
                "CONTINENT_CODE": codes,
                "CONTINENT": [CONTINENT_NAMES[c] for c in codes],
            }
        ),
        geo,
    )
    aggregate_probing_intervals(rt, fi, geo, aggregate)
    report.write_text(
        json.dumps(
            {
                "figure": probing.KIND,
                "aggregate": str(aggregate),
                "sources": {
                    "rt_based": str(rt),
                    "fixed_interval": str(fi),
                    "continent_map": str(geo),
                },
                "methodology": {"display_limit": "99.5th percentile"},
                **(
                    {
                        "protocol": "tcp",
                        "connection_mode": mode,
                        "target": "tcp.ipid.connection.rt-based.base+tcp.ipid.connection.fixed-interval.base",
                    }
                    if connection
                    else {}
                ),
            }
        )
    )
    report.with_suffix(".pdf").write_bytes(b"original PDF is never read or replaced")
    return paper_ready.job_for(report)


class PaperProbingTest(unittest.TestCase):
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
        self.job = fixture(self.root)
        self.report = json.loads(self.job.source.read_text())
        self.aggregate, self.metadata = probing.paths(self.job.source, self.report)

    def test_p999_preparation_preserves_originals_and_population_quartiles(self):
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(probing.prepare(self.job), "created")
        self.assertEqual(before, {p: stamp(p) for p in before})
        old = pq.read_table(self.report["aggregate"])
        self.assertEqual(old.schema, INTERVAL_SCHEMA)
        rows = probing.load_series(self.aggregate, self.metadata, self.job.source)
        self.assertEqual(len(rows), 14)
        for row in rows:
            self.assertEqual(len(row["QUANTILES_MS"]), 256)
            self.assertEqual(row["IP_COUNT_UNION"], 400)
            self.assertEqual(row["IP_COUNT_MODE"], 400)
            if row["MODE"] == MODES[0]:
                factor = CONTINENT_ORDER.index(row["CONTINENT_CODE"]) + 1
                self.assertAlmostEqual(row["MEDIAN_MS"], 200.5 * factor, places=6)
                self.assertAlmostEqual(row["Q1_MS"], 100.5 * factor, places=6)
                self.assertAlmostEqual(row["Q3_MS"], 300.5 * factor, places=6)
                legacy = next(
                    r
                    for r in old.to_pylist()
                    if r["MODE"] == row["MODE"] and r["CONTINENT_CODE"] == row["CONTINENT_CODE"]
                )
                self.assertGreater(row["CUTOFF_MS"], legacy["P99_5_MS"])
            else:
                self.assertEqual(row["Q1_MS"], 20)
                self.assertEqual(row["MEDIAN_MS"], 20)
                self.assertEqual(row["Q3_MS"], 20)
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch(
            "ipid_analysis.paper_figures.aggregate_probing_intervals",
            side_effect=AssertionError("must reuse"),
        ):
            self.assertEqual(probing.prepare(self.job), "skipped-current")
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_missing_preparation_never_starts_queries_or_fakes_p995(self):
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch("duckdb.connect", side_effect=AssertionError("no queries")):
            result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "failed")
        self.assertIn("--prepare-probing-intervals", result.error)
        self.assertFalse(self.job.destination.exists())
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_compact_render_geometry_real_quartile_lines_and_sources(self):
        from matplotlib.figure import Figure
        from matplotlib.text import Text

        probing.prepare(self.job)
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        rows = probing.load_series(self.aggregate, self.metadata, self.job.source)
        expected_lines = [r[key] for r in rows for key in ("Q1_MS", "MEDIAN_MS", "Q3_MS")]
        savefig = Figure.savefig

        def inspect(fig, *args, **kwargs):
            ax = fig.axes[0]
            self.assertAlmostEqual(fig.get_size_inches()[0], 3.33)
            self.assertAlmostEqual(ax.get_position().height * probing.HEIGHT, 0.88)
            self.assertEqual({t.get_fontsize() for t in fig.findobj(Text)}, {7.0})
            self.assertEqual([line.get_ydata()[0] for line in ax.lines], expected_lines)
            labels = [t.get_text() for t in ax.get_xticklabels()]
            self.assertTrue(labels[0].startswith("N. America\n"))
            self.assertTrue(labels[3].startswith("S. America\n"))
            majors = ax.get_yticks()
            minors = ax.yaxis.get_minorticklocs()
            for left, right in zip(majors, majors[1:]):
                if left >= ax.get_ylim()[0] and right <= ax.get_ylim()[1]:
                    self.assertEqual(sum(left < m < right for m in minors), 1)
            legend = ax.get_legend()
            renderer = fig.canvas.get_renderer()
            box = legend.get_window_extent(renderer)
            self.assertTrue(ax.bbox.contains(box.x0, box.y0))
            self.assertTrue(ax.bbox.contains(box.x1, box.y1))
            for handle in legend.legend_handles:
                self.assertAlmostEqual(handle.get_width(), 4)
                self.assertAlmostEqual(handle.get_height(), 4)
            for handle, text in zip(legend.legend_handles, legend.get_texts()):
                gap = text.get_window_extent(renderer).x0 - handle.get_window_extent(renderer).x1
                self.assertAlmostEqual(gap * 72 / fig.dpi, 1.2)
            return savefig(fig, *args, **kwargs)

        with (
            patch.object(Figure, "savefig", new=inspect),
            patch("duckdb.connect", side_effect=AssertionError("rendering must not query")),
            patch(
                "ipid_analysis.paper_figures.aggregate_probing_intervals",
                side_effect=AssertionError("no aggregation"),
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
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(paper_ready.run([self.job])[0].status, "skipped-existing")
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_invalid_compact_inputs_fail_without_output(self):
        probing.prepare(self.job)
        original = pq.read_table(self.aggregate)
        meta = json.loads(self.metadata.read_text())
        for condition in (
            "p995",
            "points",
            "nonmonotone",
            "nan",
            "quartiles",
            "duplicate",
            "population",
            "schema",
        ):
            with self.subTest(condition=condition):
                bad = self.root / (condition + ".pq")
                bad_meta = self.root / (condition + ".json")
                rows = original.to_pylist()
                info = dict(meta)
                if condition == "p995":
                    info["upper_quantile"] = 0.995
                elif condition == "points":
                    rows[0]["QUANTILES_MS"].pop()
                elif condition == "nonmonotone":
                    rows[0]["QUANTILES_MS"][2] = -1
                elif condition == "nan":
                    rows[0]["QUANTILES_MS"][3] = float("nan")
                elif condition == "quartiles":
                    rows[0]["Q1_MS"] = rows[0]["Q3_MS"] + 1
                elif condition == "duplicate":
                    rows[1] = dict(rows[0])
                elif condition == "population":
                    rows[0]["IP_COUNT_MODE"] = 401
                table = pa.Table.from_pylist(rows, schema=original.schema)
                if condition == "schema":
                    table = table.drop(["Q1_MS"])
                pq.write_table(table, bad)
                bad_meta.write_text(json.dumps(info))
                with self.assertRaises(ValueError):
                    probing.load_series(bad, bad_meta, self.job.source)
        self.assertFalse(self.job.destination.exists())

    def test_prepare_rejects_conflicts_and_changed_sources(self):
        self.aggregate.write_bytes(b"existing file")
        before = stamp(self.aggregate)
        with self.assertRaises(FileExistsError):
            probing.prepare(self.job)
        self.assertEqual(stamp(self.aggregate), before)
        self.aggregate.unlink()  # Test-owned fixture only.
        actual = aggregate_probing_intervals

        def changed(*args, **kwargs):
            result = actual(*args, **kwargs)
            self.job.source.write_text(self.job.source.read_text() + "\n")
            return result

        with patch("ipid_analysis.paper_figures.aggregate_probing_intervals", side_effect=changed):
            with self.assertRaisesRegex(ValueError, "source changed"):
                probing.prepare(self.job)
        self.assertFalse(self.aggregate.exists())
        self.assertFalse(self.metadata.exists())

    def test_explicit_prepare_cli_and_read_only_dry_run(self):
        with patch(
            "ipid_analysis.paper_probing_plot.prepare", side_effect=AssertionError("dry run")
        ):
            self.assertEqual(
                paper_ready.main(
                    [str(self.job.source), "--prepare-probing-intervals", "--dry-run"]
                ),
                0,
            )
        self.assertFalse(self.aggregate.exists())
        self.assertEqual(
            paper_ready.main(
                [str(self.job.source), "--prepare-probing-intervals", "--workers", "1"]
            ),
            0,
        )
        self.assertTrue(self.job.destination.is_file())

    def test_remap_discovery_and_spawn_render(self):
        probing.prepare(self.job)
        remapped, _ = probing.paths(self.job.source, self.report, self.root / "processed")
        self.assertEqual(remapped, self.aggregate)
        jobs = paper_ready.discover(
            self.root / "reports", "campaign", sites=["eu"], protocols=["icmp"]
        )
        self.assertEqual(jobs, [self.job])
        command = [
            sys.executable,
            "-B",
            "-m",
            "ipid_analysis.paper_ready",
            "--workers",
            "2",
            str(self.job.source),
        ]
        # A second prepared fixture ensures run() actually uses spawned workers.
        second = fixture(self.root, "na", "tcp-80")
        probing.prepare(second)
        command.append(str(second.source))
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
