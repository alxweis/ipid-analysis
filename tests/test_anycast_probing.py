"""Anycast sensitivity keeps originals intact and excludes /24s before quantiles."""

from contextlib import redirect_stdout
import hashlib
import io
import ipaddress
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from tests.test_paper_probing_plot import fixture, stamp

from ipid_analysis import anycast_census as census
from ipid_analysis import anycast_probing as filtered
from ipid_analysis import paper_probing_plot as probing
from ipid_analysis.paper_plot_style import _font


def dataset(root, networks=("192.0.0.0/24",), month="2026-11"):
    directory = root / "census" / hashlib.sha256(str(networks).encode()).hexdigest()[:10]
    directory.mkdir(parents=True, exist_ok=True)
    prefixes = directory / "prefixes.pq"
    pq.write_table(
        pa.table(
            {
                "PREFIX24": sorted(
                    int(ipaddress.ip_network(n).network_address) >> 8 for n in networks
                )
            },
            schema=census.PREFIX_SCHEMA,
        ),
        prefixes,
    )
    dates = [month + "-01"]
    metadata = dict(
        schema_version=1,
        repository=census.REPOSITORY,
        revision="a" * 40,
        requested_month=month,
        dates=dates,
        snapshots=[dict(date=dates[0])],
        complete_month=False,
        missing_dates=census.month_dates(month)[1:],
        filter=census.RULE,
        prefix_file="prefixes.pq",
        prefix_count=len(networks),
        prefix_sha256=hashlib.sha256(prefixes.read_bytes()).hexdigest(),
    )
    manifest = directory / "dataset.json"
    manifest.write_text(json.dumps(metadata))
    return manifest


class CensusTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_recommended_high_confidence_uses_max_across_protocols_not_backing_prefix(self):
        path = self.root / "census.pq"
        pq.write_table(
            pa.table(
                {
                    "prefix": [f"1.2.{i}.0/24" for i in range(5)],
                    "AB_ICMPv4": [3, 4, 0, None, 0],
                    "AB_TCPv4": [2, 0, 1, 0, 0],
                    "GCD_ICMPv4": [1, 1, 2, None, 1],
                    "GCD_TCPv4": [0, 0, 1, 2, 0],
                    "AB_ICMPv6": [99] * 5,
                    "backing_prefix": ["1.0.0.0/8"] * 5,
                }
            ),
            path,
        )
        prefixes, stats = census.snapshot_prefixes(path)
        self.assertEqual(
            prefixes,
            {int(ipaddress.ip_network(f"1.2.{i}.0/24").network_address) >> 8 for i in (1, 2, 3)},
        )
        self.assertEqual(stats["selected_rows"], 3)

    def test_summary_schema_and_invalid_wide_candidate_prefix(self):
        path = self.root / "summary.pq"
        pq.write_table(pa.table({"prefix": ["1.2.3.0/24"], "AB": [4], "GCD": [1]}), path)
        self.assertEqual(len(census.snapshot_prefixes(path)[0]), 1)
        pq.write_table(pa.table({"prefix": ["1.2.0.0/16"], "AB": [4], "GCD": [1]}), path)
        with self.assertRaisesRegex(ValueError, "candidate IPv4 /24"):
            census.snapshot_prefixes(path)

    def test_incomplete_month_requires_explicit_opt_in_and_pins_all_urls(self):
        def response(url):
            if url.endswith("/git/ref/heads/main"):
                return {"object": {"sha": "a" * 40}}
            if "/contents/" in url:
                return [dict(name="11", type="dir", sha="b" * 40)]
            return dict(
                truncated=False,
                tree=[dict(path="01/IPv4.parquet", type="blob", sha="c" * 40, size=100)],
            )

        with patch.object(census, "_json_url", side_effect=response):
            with self.assertRaisesRegex(ValueError, "incomplete"):
                census.inventory("2026-11")
            revision, files, missing = census.inventory("2026-11", allow_partial=True)
        self.assertEqual(len(missing), 29)
        self.assertIn(revision, files[0]["url"])
        self.assertEqual(len(census.month_dates("2028-02")), 29)

    def test_manifest_binds_exact_prefix_bytes_and_partial_period(self):
        manifest = dataset(self.root)
        data, prefixes = census.load_dataset(manifest)
        self.assertFalse(data["complete_month"])
        pq.write_table(pa.table({"PREFIX24": [1]}, schema=census.PREFIX_SCHEMA), prefixes)
        with self.assertRaisesRegex(ValueError, "bytes disagree"):
            census.load_dataset(manifest)

    def test_monthly_union_is_filtered_per_snapshot_and_cached_without_more_downloads(self):
        frames, items = {}, []
        for day, rows in (
            ("2026-11-01", [("1.2.3.0/24", 4, 1), ("1.2.4.0/24", 3, 1)]),
            ("2026-11-02", [("1.2.3.0/24", 1, 1), ("1.2.4.0/24", 2, 2)]),
        ):
            path = self.root / (day + ".pq")
            pq.write_table(
                pa.table(
                    {
                        "prefix": [r[0] for r in rows],
                        "AB": [r[1] for r in rows],
                        "GCD": [r[2] for r in rows],
                    }
                ),
                path,
            )
            content = path.read_bytes()
            url = census.RAW + "/" + "a" * 40 + "/2026/11/" + day[-2:] + "/IPv4.parquet"
            frames[url] = content
            items.append(
                dict(
                    date=day,
                    url=url,
                    git_blob=census._blob_digest(content),
                    size_bytes=len(content),
                )
            )
        expected = ("a" * 40, items, census.month_dates("2026-11")[2:])
        with (
            patch.object(census, "inventory", return_value=expected),
            patch.object(census, "_read_url", side_effect=lambda url, _: frames[url]),
        ):
            manifest = census.prepare(
                "2026-11",
                self.root / "published",
                cache_root=self.root / "cache",
                allow_partial=True,
            )
        data, table = census.load_dataset(manifest)
        self.assertEqual(data["prefix_count"], 2)
        self.assertEqual(len(data["snapshots"]), 2)
        self.assertFalse(data["complete_month"])
        before = stamp(table), stamp(manifest)
        with (
            patch.object(census, "inventory", return_value=expected),
            patch.object(
                census, "_read_url", side_effect=AssertionError("cache downloaded again")
            ),
        ):
            self.assertEqual(
                census.prepare(
                    "2026-11",
                    self.root / "published",
                    cache_root=self.root / "cache",
                    allow_partial=True,
                ),
                manifest,
            )
        self.assertEqual((stamp(table), stamp(manifest)), before)


class AnycastPlotTest(unittest.TestCase):
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
                "MPLCONFIGDIR": str(self.root / "mpl"),
            },
        )
        env.start()
        self.addCleanup(env.stop)
        _font.cache_clear()
        self.addCleanup(_font.cache_clear)
        self.base = fixture(self.root)
        self.census = dataset(self.root)
        self.job = filtered.Job(self.base.source, self.base.kind, None, self.census)

    def test_dry_run_creates_nothing_and_never_starts_preparation(self):
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch.object(probing, "prepare", side_effect=AssertionError("dry-run prepared")):
            result = filtered.run([self.job], prepare=True, dry_run=True)
        self.assertEqual(result[0].status, "would-create")
        self.assertTrue(result[0].destination.endswith("__paper__anycast.pdf"))
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_cli_selects_only_eu_icmp_tcp_syn_udp_and_never_connection_or_other_vps(self):
        fixture(self.root, protocol="tcp")
        fixture(self.root, protocol="tcp", connection=True)
        fixture(self.root, protocol="udp")
        fixture(self.root, site="af")
        output = io.StringIO()
        with redirect_stdout(output):
            filtered.main(
                [
                    "--campaign",
                    "campaign",
                    "--census",
                    str(self.census),
                    "--reports-root",
                    str(self.root / "reports"),
                    "--sites",
                    "eu",
                    "--protocols",
                    "icmp",
                    "tcp",
                    "udp",
                    "--connection-mode",
                    "no-connection",
                    "--dry-run",
                ]
            )
        lines = [
            line for line in output.getvalue().splitlines() if line.startswith("would-create:")
        ]
        self.assertEqual(len(lines), 3)
        self.assertTrue(all("/eu/" in line and "/no-connection/" in line for line in lines))

    def test_excludes_entire_detected_24_before_union_counts_quartiles_and_p999(self):
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(probing.prepare(self.job, anycast=self.census), "created")
        report = json.loads(self.job.source.read_text())
        aggregate, metadata = probing.paths(self.job.source, report, anycast=self.census)
        rows = probing.load_series(aggregate, metadata, self.job.source, anycast=self.census)
        removed = [r for r in rows if r["CONTINENT_CODE"] == "NA"]
        self.assertEqual(len(removed), 2)
        self.assertTrue(all(r["IP_COUNT_UNION"] == 144 for r in removed))
        for row in removed:
            self.assertEqual(row["IP_COUNT_MODE"], 144)
            if row["MODE"] == "RT-based":
                self.assertGreater(row["Q1_MS"], 256)
        stats = json.loads(metadata.read_text())
        self.assertEqual(
            stats["anycast_exclusion_by_continent"]["NA"],
            dict(original=400, excluded=256, retained=144),
        )
        self.assertFalse(stats["anycast_census"]["complete_month"])
        self.assertEqual(before, {p: stamp(p) for p in before})
        with patch(
            "ipid_analysis.paper_figures.aggregate_probing_intervals",
            side_effect=AssertionError("cached preparation rescanned"),
        ):
            self.assertEqual(probing.prepare(self.job, anycast=self.census), "skipped-current")
        with self.assertRaisesRegex(ValueError, "filtered quantiles"):
            probing.load_series(aggregate, metadata, self.job.source)

    def test_render_separate_pdf_and_never_touch_original_or_existing_paper_variant(self):
        self.base.destination.write_bytes(b"old paper plot")
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        result = filtered.run([self.job], prepare=True, workers=1)
        self.assertEqual((result[0].status, result[0].error), ("created", ""))
        self.assertTrue(self.job.destination.read_bytes().startswith(b"%PDF-"))
        self.assertEqual(before, {p: stamp(p) for p in before})
        with patch(
            "ipid_analysis.paper_figures.aggregate_probing_intervals",
            side_effect=AssertionError("render repeated raw preparation"),
        ):
            self.assertEqual(filtered.run([self.job], workers=1)[0].status, "skipped-existing")

    def test_different_census_keeps_separate_quantiles_and_cannot_replace_pdf(self):
        from ipid_analysis.anycast_histogram import prepare

        prepare(self.job, anycast=self.census)
        other = dataset(self.root, ("192.1.0.0/24",))
        self.assertEqual(prepare(self.job, anycast=other), "created")
        report = json.loads(self.job.source.read_text())
        first = probing.paths(self.job.source, report, anycast=self.census)[0]
        second = probing.paths(self.job.source, report, anycast=other)[0]
        self.assertNotEqual(first, second)
        self.assertEqual(filtered.run([self.job], workers=1)[0].status, "created")
        with self.assertRaisesRegex(FileExistsError, "another census"):
            filtered.run([filtered.Job(self.base.source, self.base.kind, None, other)])
        changed = filtered.Job(
            self.base.source, self.base.kind, None, other, self.root / "later/anycast.pdf"
        )
        self.assertEqual(filtered.run([changed], workers=1)[0].status, "created")

    def full_fixture(self):
        from paper_values.followups import COMPARISON, registration

        campaign, site = "geo-full-20261107-r1", "eu"
        spec = registration(campaign)
        relative = Path("followups") / spec["measurement_run"] / spec["analysis_id"] / site
        reports = self.root / "reports"
        root = reports / relative
        original = fixture(self.root / "full-fixture", protocol="tcp-80", connection=True)
        report = json.loads(original.source.read_text())
        old = Path(report["aggregate"]).parents[3]
        new = self.root / "data/processed" / relative / "paired" / spec["zmap_ids"][site]
        for path in old.rglob("*.pq"):
            destination = new / path.relative_to(old)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, destination)
        report["aggregate"] = str(new / Path(report["aggregate"]).relative_to(old))
        report["sources"] = {
            key: str(new / Path(value).relative_to(old))
            for key, value in report["sources"].items()
        }
        source = (
            root
            / "paired/figures"
            / spec["zmap_ids"][site]
            / COMPARISON
            / (original.source.stem + "__full.json")
        )
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(json.dumps(report))
        marker = root / "done__full.json"
        marker.write_text(
            json.dumps(
                dict(
                    original_campaign=campaign,
                    site=site,
                    measurement_run=spec["measurement_run"],
                    analysis_id=spec["analysis_id"],
                    artifacts=[
                        dict(
                            path=str(source),
                            size_bytes=source.stat().st_size,
                            sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                        )
                    ],
                )
            )
        )
        return campaign, reports, source, marker

    def test_registered_full_connection_cli_uses_paired_sources_and_separate_pdf(self):
        campaign, reports, source, marker = self.full_fixture()
        job = filtered.full_job(campaign, "eu", reports, self.census)
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(job.source, source)
        self.assertTrue(job.destination.name.endswith("__paper__full__anycast.pdf"))
        arguments = [
            "--campaign",
            campaign,
            "--tcp-full",
            "--census",
            str(self.census),
            "--reports-root",
            str(reports),
            "--sites",
            "eu",
            "--protocols",
            "tcp",
            "--connection-mode",
            "connection",
        ]
        output = io.StringIO()
        with redirect_stdout(output):
            filtered.main(arguments + ["--dry-run"])
        self.assertEqual(output.getvalue().count("would-create:"), 1)
        result = filtered.run([job], prepare=True, workers=1)[0]
        self.assertEqual((result.status, result.error), ("created", ""))
        self.assertTrue(job.destination.read_bytes().startswith(b"%PDF-"))
        aggregate, metadata = probing.paths(
            source, json.loads(source.read_text()), anycast=self.census
        )
        self.assertIn("paired", aggregate.parts)
        self.assertEqual(
            len(probing.load_series(aggregate, metadata, source, anycast=self.census)), 14
        )
        self.assertEqual(before, {p: stamp(p) for p in before})
        self.assertEqual(
            filtered.run([job], prepare=True, workers=1)[0].status, "skipped-existing"
        )

    def test_full_selection_refuses_missing_registration_or_changed_report(self):
        campaign, reports, source, marker = self.full_fixture()
        with self.assertRaisesRegex(ValueError, "no registered"):
            filtered.full_job("not-registered", "eu", reports, self.census)
        source.write_text(source.read_text() + " ")
        with self.assertRaisesRegex(ValueError, "completion inventory"):
            filtered.full_job(campaign, "eu", reports, self.census)
        with self.assertRaisesRegex(ValueError, "explicit anycast"):
            probing.paths(source, json.loads(source.read_text()))

    def test_invalid_map_ip_is_rejected_instead_of_treated_as_unicast(self):
        report = json.loads(self.base.source.read_text())
        path = Path(report["sources"]["continent_map"])
        pq.write_table(
            pa.table(
                {"IP_ADDR": ["999.1.2.3"], "CONTINENT_CODE": ["AF"], "CONTINENT": ["Africa"]}
            ),
            path,
        )
        with self.assertRaisesRegex(ValueError, "invalid/non-IPv4"):
            probing.prepare(self.job, anycast=self.census)


if __name__ == "__main__":
    unittest.main()
