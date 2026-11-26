"""Exact inclusive dates, globally distinct prefixes and backed-up canonical refresh."""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import test_anycast_probing as base_fixtures
from test_anycast_probing import dataset
from test_paper_probing_plot import fixture, stamp

from ipid_analysis import anycast_census as census
from ipid_analysis import anycast_probing as filtered
from ipid_analysis.anycast_refresh import refresh
from ipid_analysis.artifacts import completion_path
from ipid_analysis.paper_plot_style import _font
from ipid_analysis.paper_ready import Result


def window_dataset(root):
    manifest = dataset(root / "window")
    doc = json.loads(manifest.read_text())
    days = census.period_dates("2026-11", "2026-11-07", "2026-11-22")
    doc.update(
        schema_version=2,
        period_start=days[0],
        period_end=days[-1],
        dates=days,
        snapshots=[dict(date=day) for day in days],
        missing_dates=[],
        complete_period=True,
        expected_snapshots=16,
        confidence="high-confidence",
    )
    manifest.write_text(json.dumps(doc))
    return manifest


class AnycastWindowTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        env = patch.dict(
            os.environ,
            dict(
                IPID_ANALYSIS_STORAGE_MODE="local",
                IPID_ANALYSIS_SCRATCH_STORAGE="disk",
                IPID_ANALYSIS_TMP_DIR=str(self.root / "scratch"),
                MPLCONFIGDIR=str(self.root / "mpl"),
            ),
        )
        env.start()
        self.addCleanup(env.stop)
        _font.cache_clear()
        self.addCleanup(_font.cache_clear)

    def test_period_downloads_only_the_sixteen_inclusive_days_and_keeps_month_cache(self):
        frames, tree = {}, []
        for n in range(1, 25):
            day = f"2026-11-{n:02}"
            path = self.root / (day + ".pq")
            # Strong out-of-window evidence must not contaminate the union.
            prefix = "11.0.0.0/24" if n < 7 else "12.0.0.0/24" if n > 22 else "10.0.0.0/24"
            pq.write_table(pa.table({"prefix": [prefix], "AB": [4], "GCD": [1]}), path)
            data = path.read_bytes()
            url = f"{census.RAW}/{'a' * 40}/2026/11/{n:02}/IPv4.parquet"
            frames[url] = data
            tree.append(
                dict(
                    path=f"{n:02}/IPv4.parquet",
                    type="blob",
                    size=len(data),
                    sha=census._blob_digest(data),
                )
            )

        def api(url):
            return (
                [dict(name="11", type="dir", sha="b" * 40)]
                if "/contents/" in url
                else dict(truncated=False, tree=tree)
            )

        old = self.root / "published/2026-11" / ("a" * 40) / "dataset.json"
        old.parent.mkdir(parents=True)
        old.write_bytes(b"old monthly dataset remains untouched")
        before = stamp(old)
        with (
            patch.object(census, "_json_url", side_effect=api),
            patch.object(census, "_read_url", side_effect=lambda url, _: frames[url]) as downloads,
        ):
            manifest = census.prepare(
                "2026-11",
                self.root / "published",
                cache_root=self.root / "cache",
                revision="a" * 40,
                start_date="2026-11-07",
                end_date="2026-11-22",
            )
        doc, prefixes = census.load_dataset(manifest)
        self.assertEqual(downloads.call_count, 16)
        self.assertEqual(doc["dates"], census.month_dates("2026-11")[6:22])
        self.assertEqual(doc["prefix_count"], 1)
        self.assertEqual(pq.read_table(prefixes)["PREFIX24"].to_pylist(), [10 * 65536])
        self.assertTrue(doc["complete_period"])
        self.assertFalse(doc["complete_month"])
        self.assertEqual(doc["missing_dates"], [])
        self.assertEqual(manifest.parent.name, "20261107-20261122")
        self.assertEqual(stamp(old), before)
        with (
            patch.object(census, "_json_url", side_effect=api),
            patch.object(census, "_read_url", side_effect=AssertionError("redownload")),
        ):
            self.assertEqual(
                census.prepare(
                    "2026-11",
                    self.root / "published",
                    cache_root=self.root / "cache",
                    revision="a" * 40,
                    start_date="2026-11-07",
                    end_date="2026-11-22",
                ),
                manifest,
            )

    def test_invalid_or_partial_period_cannot_be_misrepresented_as_complete(self):
        self.assertEqual(len(census.period_dates("2026-11", "2026-11-07", "2026-11-22")), 16)
        for start, end in (
            ("2026-11-07", None),
            ("2026-11-22", "2026-11-07"),
            ("2026-10-31", "2026-11-22"),
        ):
            with self.assertRaises(ValueError):
                census.period_dates("2026-11", start, end)
        manifest = window_dataset(self.root)
        doc = json.loads(manifest.read_text())
        doc["dates"].pop()
        manifest.write_text(json.dumps(doc))
        with self.assertRaises(ValueError):
            census.load_dataset(manifest)

    def test_prefix_totals_are_distinct_across_continents_and_include_unlocated(self):
        from ipid_analysis.runtime import connect

        geo = self.root / "geo.pq"
        pq.write_table(
            pa.table(
                dict(
                    IP_ADDR=["192.0.0.1", "192.0.0.2", "192.1.0.1", "192.2.0.1"],
                    CONTINENT_CODE=["NA", "EU", "ZZ", "EU"],
                    CONTINENT=["N. America", "Europe", "Unknown", "Europe"],
                )
            ),
            geo,
        )
        _, prefixes = census.load_dataset(dataset(self.root))
        with connect() as con:
            _, report = filtered.filtered_continent_map(con, geo, prefixes)
        stats = report["anycast_filtering"]
        self.assertEqual(stats["total"]["addresses"], dict(original=4, excluded=2, retained=2))
        self.assertEqual(stats["total"]["prefixes"], dict(original=3, excluded=1, retained=2))
        self.assertEqual(
            sum(row["prefixes"]["excluded"] for row in stats["continents"].values()), 2
        )
        self.assertEqual(stats["continents"]["ZZ"]["addresses"]["retained"], 1)

    def test_refresh_replaces_same_filename_after_backup_and_reuses_new_quantiles(self):
        base = fixture(self.root)
        old = filtered.Job(base.source, base.kind, None, dataset(self.root))
        self.assertEqual(filtered.run([old], prepare=True, workers=1)[0].status, "created")
        before_pdf = old.destination.read_bytes()
        old_inputs = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        new = filtered.Job(base.source, base.kind, None, window_dataset(self.root))
        self.assertEqual(new.destination, old.destination)
        result = refresh([new], prepare=True, workers=1, backup_root=self.root / "backups")
        self.assertEqual(result[0].status, "updated")
        doc = json.loads(new.destination.with_suffix(".json").read_text())
        self.assertEqual(
            (doc["anycast_census"]["period_start"], doc["anycast_census"]["period_end"]),
            ("2026-11-07", "2026-11-22"),
        )
        self.assertEqual(doc["anycast_filtering"]["total"]["addresses"]["excluded"], 256)
        receipt = json.loads(completion_path(new.destination).read_text())
        self.assertEqual(receipt["parameters"]["impact_metadata_version"], 2)
        backups = list((self.root / "backups").glob("*/before/0/pdf.pdf"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_bytes(), before_pdf)
        changed = {new.destination, completion_path(new.destination)}
        self.assertEqual(
            {p: s for p, s in old_inputs.items() if p not in changed},
            {p: stamp(p) for p in old_inputs if p not in changed},
        )
        with patch(
            "ipid_analysis.paper_figures.aggregate_probing_intervals",
            side_effect=AssertionError("must reuse"),
        ):
            self.assertEqual(
                refresh([new], prepare=True, workers=1, backup_root=self.root / "backups")[
                    0
                ].status,
                "skipped-current",
            )

    def test_staging_failure_never_replaces_old_pdf(self):
        base = fixture(self.root)
        old = filtered.Job(base.source, base.kind, None, dataset(self.root))
        filtered.run([old], prepare=True, workers=1)
        before = stamp(old.destination), stamp(completion_path(old.destination))
        new = filtered.Job(base.source, base.kind, None, window_dataset(self.root))
        with patch.object(
            filtered,
            "worker",
            return_value=Result("stage", "failed", error="injected render failure"),
        ):
            with self.assertRaisesRegex(ValueError, "canonical PDFs were not replaced"):
                refresh([new], prepare=True, workers=1, backup_root=self.root / "backups")
        self.assertEqual((stamp(old.destination), stamp(completion_path(old.destination))), before)

    def test_refresh_dry_run_reports_replacement_without_preparing_or_writing(self):
        base = fixture(self.root)
        job = filtered.Job(base.source, base.kind, None, window_dataset(self.root))
        job.destination.write_bytes(b"previous Anycast PDF")
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch(
            "ipid_analysis.anycast_refresh.refresh", side_effect=AssertionError("dry run wrote")
        ):
            result = filtered.run([job], prepare=True, refresh=True, dry_run=True)
        self.assertEqual(result[0].status, "would-refresh")
        self.assertEqual(before, {p: stamp(p) for p in self.root.rglob("*") if p.is_file()})

    def test_cli_selects_exactly_three_stateless_eu_and_one_registered_full_job(self):
        case = base_fixtures.AnycastPlotTest()
        case.setUp()
        self.addCleanup(case.doCleanups)
        campaign, reports, _, _ = case.full_fixture()
        normal = [
            fixture(case.root / "normal", protocol=p) for p in ("icmp", "tcp-80", "udp-dns-53")
        ]
        manifest = window_dataset(case.root)
        output = io.StringIO()
        with (
            patch("ipid_analysis.paper_ready.discover", return_value=normal),
            redirect_stdout(output),
        ):
            filtered.main(
                [
                    "--campaign",
                    campaign,
                    "--census",
                    str(manifest),
                    "--reports-root",
                    str(reports),
                    "--sites",
                    "eu",
                    "--protocols",
                    "icmp",
                    "tcp",
                    "udp",
                    "--connection-mode",
                    "no-connection",
                    "--include-tcp-full",
                    "--refresh",
                    "--dry-run",
                ]
            )
        self.assertEqual(output.getvalue().count("would-create:"), 4)
        self.assertIn("__paper__full__anycast.pdf", output.getvalue())


if __name__ == "__main__":
    unittest.main()
