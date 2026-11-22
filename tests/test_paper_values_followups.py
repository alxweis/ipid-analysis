"""Finished full-RT results survive repeated exports without analysis or source writes."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from paper_values.exporter import export, load_metrics
from paper_values.figure_zip import bundle
from paper_values.followups import FIGURES, load, registration
from paper_values.inputs import SITES

CAMPAIGN = "geo-full-20261107-r1"


class FullFollowupTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.reports = self.root / "reports/continental" / CAMPAIGN
        self.spec = registration(CAMPAIGN)
        self.followups = self.root / "reports/followups"
        self.analysis = self.followups / self.spec["measurement_run"] / self.spec["analysis_id"]
        self.metrics = load_metrics()["metrics"]

    def write(self, path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data if isinstance(data, bytes) else json.dumps(data).encode())
        receipt = path.parent / ".ipid-complete" / (path.name + ".json")
        receipt.parent.mkdir(exist_ok=True)
        receipt.write_text(
            json.dumps(
                dict(
                    status="complete",
                    size_bytes=path.stat().st_size,
                    sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                )
            )
        )

    def fixture(self):
        summaries = []
        for site in SITES:
            zmap = self.spec["zmap_ids"][site]
            root = self.analysis / site
            artifacts = []

            def artifact(relative, content):
                path = root / relative
                self.write(path, content)
                # Published paths can come from another machine/mount root.
                artifacts.append(
                    dict(
                        path="/mnt/data01/ipid-analysis/reports/"
                        + path.relative_to(self.root / "reports").as_posix(),
                        size_bytes=path.stat().st_size,
                        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                    )
                )

            phase = f"full/figures/{zmap}/connection/rt-based-base/c-rt-b_"
            artifact(
                phase + "strategies__full.json",
                dict(
                    protocol="tcp",
                    target="tcp.ipid.connection.rt-based.base",
                    measurement_id="tcp-80_2026-11-21_19-36-52",
                    zmap_id=zmap,
                    counts={"SINGLE": 6, "UNCLASSIFIED": 2},
                    total_ips=8,
                ),
            )
            artifact(
                phase + "itdk-role-by-strategy__full.json",
                {
                    "protocol": "tcp",
                    "target": "tcp.ipid.connection.rt-based.base",
                    "zmap_id": zmap,
                    "plot_percentages": {
                        "transit-observed": {"single": 25, "per-connection": 50},
                        "no-transit-evidence": {"single": 75},
                    },
                },
            )
            for population, stem in FIGURES.values():
                artifact(
                    f"{population}/figures/{zmap}/{stem}__paper__full.pdf", b"%PDF-1.4\nfixture"
                )
            # Poison bulk input: the exporter must never open it.
            artifact("full/processed/per-ip-strategies.pq", b"not parquet")
            done = dict(
                version=1,
                site=site,
                analysis_id=self.spec["analysis_id"],
                original_campaign=CAMPAIGN,
                measurement_run=self.spec["measurement_run"],
                paired_population="successful in both new RT and historical FI-BASE",
                full_measurement=dict(
                    stage="full",
                    inflight=4000,
                    total_targets=10,
                    successful_ips=8,
                    measurement_id="tcp-80_2026-11-21_19-36-52",
                ),
                paired_successful_ips=4,
                artifacts=artifacts,
            )
            self.write(root / "done__full.json", done)
            summaries.append(done)
        self.write(
            self.analysis / "done__full.json",
            dict(
                version=1,
                analysis_id=self.spec["analysis_id"],
                sites=summaries,
            ),
        )

    def run_export(self, **kwargs):
        return export(CAMPAIGN, reports_root=self.reports, repository_root=self.root, **kwargs)

    def test_repeated_standard_export_and_zip_preserve_old_values_and_all_inputs(self):
        old = (
            self.reports
            / "eu/figures/tcp-80_2026-11-07_03-30-08/connection/comparison/rt-based-base_fixed-interval-base/c-rt-b_fi-b_strategy-intersection__paper.pdf"
        )
        self.write(old, b"%PDF-1.4\nold plot")
        before = self.run_export(strict=True)["values"]
        self.fixture()
        hashes = {
            path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
            for path in (self.root / "reports").rglob("*")
            if path.is_file()
        }
        with patch("subprocess.run", side_effect=AssertionError("must not launch analysis")):
            first = self.run_export(strict=True)
            second = self.run_export(strict=True)
            bundle(repository_root=self.root)
        self.assertEqual(first["values"], second["values"])
        for key, value in before.items():
            if not key.endswith(".full"):
                self.assertEqual(first["values"][key], value)
        for site in SITES:
            values = first["values"]
            self.assertEqual(
                values[f"results.{site}.tcp.conn.rt-base.success.count.full"]["raw"], 8
            )
            self.assertEqual(
                values[f"results.{site}.tcp.conn.rt-base.success.single.pct.full"]["raw"], 75
            )
            self.assertEqual(
                values[f"results.{site}.tcp.conn.rt-base.coverage.pct.full"]["raw"], 80
            )
            self.assertEqual(
                values[f"results.{site}.tcp.conn.rt-vs-fi-base.paired-success.count.full"]["raw"],
                4,
            )
            self.assertEqual(
                values[f"results.{site}.tcp.conn.rt-base.itdk-strategy.routers.single.pct.full"][
                    "raw"
                ],
                25,
            )
        with zipfile.ZipFile(self.root / "tmp/paper-figures.zip") as archive:
            self.assertEqual(sum("__paper__full.pdf" in name for name in archive.namelist()), 36)
            self.assertIn(old.relative_to(self.root).as_posix(), archive.namelist())
        self.assertEqual(
            hashes,
            {
                path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
                for path in hashes
            },
        )

    def test_missing_pinned_run_does_not_select_a_newer_run(self):
        self.fixture()
        self.analysis.rename(self.analysis.with_name("analysis-newer"))
        result = self.run_export(strict=True)
        self.assertIsNone(
            result["values"]["results.eu.tcp.conn.rt-base.success.count.full"]["raw"]
        )

    def test_corrupt_completed_report_keeps_existing_outputs_in_strict_mode(self):
        self.fixture()
        self.run_export(strict=True)
        before = (self.root / "tmp/values.tex").read_bytes()
        path = next((self.analysis / "eu").rglob("c-rt-b_strategies__full.json"))
        path.write_text("{}")
        with self.assertRaisesRegex(ValueError, "invalid or ambiguous inputs"):
            self.run_export(strict=True)
        self.assertEqual(before, (self.root / "tmp/values.tex").read_bytes())
        relaxed = self.run_export()["values"]
        self.assertTrue(
            all(value["raw"] is None for key, value in relaxed.items() if key.endswith(".full"))
        )

    def test_archive_isolation_and_explicit_followups_root(self):
        self.fixture()
        values, errors = load(CAMPAIGN, self.metrics, self.reports, archive=True)
        self.assertFalse(errors)
        self.assertTrue(all(value["raw"] is None for value in values.values()))
        values, errors = load(
            CAMPAIGN, self.metrics, None, archive=True, followups_root=self.followups
        )
        self.assertFalse(errors)
        self.assertEqual(values["results.eu.tcp.conn.rt-base.success.count.full"]["raw"], 8)

    def test_mismatched_global_site_or_population_is_rejected(self):
        self.fixture()
        marker = self.analysis / "done__full.json"
        done = json.loads(marker.read_text())
        done["sites"][0]["paired_successful_ips"] = 99
        self.write(marker, done)
        with self.assertRaisesRegex(ValueError, "global and site"):
            self.run_export(strict=True)

    def test_unchanged_scalar_cannot_be_imported_from_a_different_discovery(self):
        self.fixture()
        marker = self.analysis / "done__full.json"
        done = json.loads(marker.read_text())
        entry = next(
            item
            for item in done["sites"][0]["artifacts"]
            if item["path"].endswith("c-rt-b_strategies__full.json")
        )
        source = self.root / "reports" / entry["path"].split("/reports/", 1)[1]
        report = json.loads(source.read_text())
        report["zmap_id"] = "tcp-80_2026-01-01_00-00-00"
        self.write(source, report)
        entry.update(
            size_bytes=source.stat().st_size,
            sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        )
        self.write(self.analysis / done["sites"][0]["site"] / "done__full.json", done["sites"][0])
        self.write(marker, done)
        with self.assertRaisesRegex(ValueError, "registered measurement"):
            self.run_export(strict=True)

    def test_no_analysis_or_parquet_library_is_imported_by_followup_loader(self):
        import builtins

        self.fixture()
        original = builtins.__import__

        def guarded(name, *args, **kwargs):
            if name.split(".")[0] in ("ipid_analysis", "pyarrow", "duckdb"):
                raise AssertionError("follow-up export must only read finished summaries")
            return original(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=guarded):
            values, errors = load(CAMPAIGN, self.metrics, self.reports)
        self.assertFalse(errors)
        self.assertEqual(values["results.eu.tcp.conn.rt-base.success.count.full"]["raw"], 8)

    def test_invalid_numeric_summary_does_not_leak_partial_full_values(self):
        self.fixture()
        marker = self.analysis / "done__full.json"
        done = json.loads(marker.read_text())
        site = done["sites"][-1]
        entry = next(
            item
            for item in site["artifacts"]
            if item["path"].endswith("c-rt-b_itdk-role-by-strategy__full.json")
        )
        source = self.root / "reports" / entry["path"].split("/reports/", 1)[1]
        report = json.loads(source.read_text())
        report["plot_percentages"]["transit-observed"]["single"] = float("nan")
        self.write(source, report)
        entry.update(
            size_bytes=source.stat().st_size,
            sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        )
        self.write(self.analysis / site["site"] / "done__full.json", site)
        self.write(marker, done)
        values, errors = load(CAMPAIGN, self.metrics, self.reports)
        self.assertTrue(errors)
        self.assertTrue(all(value["raw"] is None for value in values.values()))


if __name__ == "__main__":
    unittest.main()
