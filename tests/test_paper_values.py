"""Scientific populations, canonical public keys, source integrity and value-cache tests."""

import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from paper_values.cache import ValueCache
from paper_values.exporter import export
from paper_values.tex import key_order, render

CAMPAIGN = "test-campaign"
RUNS = {
    "tcp": "tcp-80_2026-11-07_03-30-08",
    "icmp": "icmp_2026-11-07_03-30-07",
    "udp": "udp-dns-53_2026-11-07_03-30-08",
}
PATHS = {
    "rt-base": "rt-based-base/n-rt-b_",
    "fi-mass": "fixed-interval-mass/n-fi-m_",
    "fi-base": "fixed-interval-base/n-fi-b_",
    "rt-fi": "merged/rt-based-base_fixed-interval-mass/n-rt-b_fi-m_",
    "comparison": "comparison/rt-based-base_fixed-interval-base/n-rt-b_fi-b_",
}


class Fixture:
    def __init__(self, root):
        self.root = Path(root)
        self.reports = self.root / "reports/continental" / CAMPAIGN
        self.processed = self.root / "data/processed/continental" / CAMPAIGN
        self.raw = self.root / "data/raw/continental" / CAMPAIGN

    def report(
        self,
        kind,
        data,
        *,
        protocol="tcp",
        phase="rt-base",
        mode="no-connection",
        site="eu",
        run=None,
        processed=False,
        receipt=False,
    ):
        prefix = self.processed if processed else self.reports
        path = prefix / site
        if not processed:
            path /= "figures"
        stem = PATHS[phase]
        if mode == "connection":
            stem = stem.replace("n-", "c-")
        path = path / (run or RUNS[protocol]) / mode / (stem + kind + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(data).encode()
        path.write_bytes(content)
        if receipt:
            self.receipt(path)
        return path

    def receipt(self, path):
        marker = path.parent / ".ipid-complete" / (path.name + ".json")
        marker.parent.mkdir(exist_ok=True)
        marker.write_text(
            json.dumps(
                dict(
                    status="complete",
                    size_bytes=path.stat().st_size,
                    sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                )
            ),
            encoding="utf-8",
        )
        return marker

    def strategy(self, c, **kwargs):
        protocol = kwargs.get("protocol", "tcp")
        phase = kwargs.get("phase", "rt-base")
        data = dict(
            protocol=protocol,
            counts=c,
            total_ips=sum(c.values()),
            zmap_id=RUNS[protocol],
            measurement_id=protocol + "_" + phase.replace("-", "_"),
        )
        return self.report("strategies", data, **kwargs)

    def baseline(self, **kwargs):
        self.strategy({"CONSTANT": 6, "SINGLE": 2, "UNCLASSIFIED": 2}, **kwargs)
        self.strategy({"RANDOM": 1}, phase="fi-mass", **kwargs)
        self.strategy(
            {"CONSTANT": 6, "SINGLE": 2, "RANDOM": 1, "NOT_ENOUGH_SAMPLES": 1},
            phase="rt-fi",
            **kwargs,
        )
        self.report(
            "coverage",
            dict(zmap_ip_count=20, ipid_ip_count=10),
            processed=True,
            **kwargs,
        )

    def snapshot(self, *, protocol="tcp", phase="rt-base", site="eu", **overrides):
        self.strategy({"CONSTANT": 1}, protocol=protocol, phase=phase, site=site)
        path = (
            self.raw
            / site
            / "ipid"
            / (protocol + "_" + phase.replace("-", "_"))
            / "ipid.snapshot.yaml"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        data = dict(
            connection_count=4,
            requests_per_connection=25 if phase == "fi-mass" else 4,
            maximum_tolerated_rtt="2s",
            fixed_interval=dict(request_interval="20ms", minimum_reply_rate=0.8),
        )
        data.update(overrides)
        path.write_text(json.dumps(data), encoding="utf-8")  # JSON is also valid YAML.
        return path

    def run(self, **kwargs):
        return export(
            CAMPAIGN,
            reports_root=self.reports,
            processed_root=self.processed,
            raw_root=self.raw,
            repository_root=self.root,
            **kwargs,
        )

    def output_hashes(self):
        return {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (self.root / "tmp").iterdir()
            if p.name in ("values.tex", "values.json")
        }


class PaperValuesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.f = Fixture(self.temp.name)

    def test_all_existing_public_keys_and_six_vps_remain(self):
        result = self.f.run(strict=True)
        self.assertEqual(len(result["values"]), 1497)
        self.assertEqual(result["available"], 0)
        self.assertFalse(result["input_errors"])
        self.assertTrue(
            all(".vp-" not in k and not k.startswith("reports.") for k in result["values"])
        )
        for site in ("af", "as", "au", "eu", "na", "sa"):
            self.assertIn(
                f"results.{site}.tcp.no-conn.rt-fi.rt-pop.constant.pct",
                result["values"],
            )
        self.assertNotIn(
            "params.eu.source-addresses.count", result["values"]
        )  # Known future extension, not added silently.
        self.assertIsNone(result["values"]["artifacts.required-memory-gib"]["raw"])

    def test_rt_and_successful_denominators_remain_distinct(self):
        self.f.baseline()
        v = self.f.run(strict=True)["values"]
        self.assertEqual(v["results.eu.tcp.no-conn.rt-base.success.count"]["raw"], 10)
        self.assertEqual(v["results.eu.tcp.no-conn.rt-fi.rt-pop.constant.pct"]["raw"], 60)
        self.assertEqual(v["results.eu.tcp.no-conn.rt-base.success.count"]["raw"], 10)
        self.assertIsNone(v["probing.eu.tcp.no-conn.fi-to-rt-success.pct"]["raw"])

    def test_zero_is_observed_but_missing_vp_is_not_zero(self):
        self.f.strategy({"SINGLE": 10})
        v = self.f.run()["values"]
        self.assertEqual(v["results.eu.tcp.no-conn.rt-base.success.constant.pct"]["raw"], 0)
        self.assertIsNone(v["results.na.tcp.no-conn.rt-base.success.constant.pct"]["raw"])

    def test_zero_denominator_keeps_percentage_empty(self):
        self.f.strategy({})
        v = self.f.run()["values"]
        self.assertEqual(v["results.eu.tcp.no-conn.rt-base.success.count"]["raw"], 0)
        self.assertIsNone(v["results.eu.tcp.no-conn.rt-base.success.constant.pct"]["raw"])

    def test_vp_results_are_independent_even_if_counts_are_equal(self):
        for site in ("eu", "na"):
            self.f.strategy({"CONSTANT": 3, "SINGLE": 7}, site=site)
        v = self.f.run()["values"]
        self.assertEqual(v["results.eu.tcp.no-conn.rt-base.success.constant.pct"]["raw"], 30)
        self.assertEqual(v["results.na.tcp.no-conn.rt-base.success.constant.pct"]["raw"], 30)
        self.f.strategy({"CONSTANT": 8, "SINGLE": 2}, site="na")
        w = self.f.run()["values"]
        self.assertEqual(w["results.eu.tcp.no-conn.rt-base.success.constant.pct"]["raw"], 30)
        self.assertEqual(w["results.na.tcp.no-conn.rt-base.success.constant.pct"]["raw"], 80)

    def test_merged_population_inconsistency_preserves_both_outputs(self):
        self.f.baseline()
        self.f.run()
        before = self.f.output_hashes()
        self.f.strategy({"CONSTANT": 11}, phase="rt-fi")
        with self.assertRaisesRegex(ValueError, "merged population"):
            self.f.run()
        self.assertEqual(before, self.f.output_hashes())

    def test_coverage_inconsistency_preserves_existing_outputs(self):
        self.f.baseline()
        self.f.run()
        before = self.f.output_hashes()
        self.f.report("coverage", dict(zmap_ip_count=20, ipid_ip_count=11), processed=True)
        with self.assertRaisesRegex(ValueError, "coverage"):
            self.f.run()
        self.assertEqual(before, self.f.output_hashes())

    def test_invalid_counts_do_not_leak_partial_values(self):
        self.f.report("strategies", dict(protocol="tcp", total_ips=10, counts={"CONSTANT": 9}))
        result = self.f.run()
        self.assertTrue(result["input_errors"])
        self.assertIsNone(result["values"]["results.eu.tcp.no-conn.rt-base.success.count"]["raw"])
        before = self.f.output_hashes()
        with self.assertRaisesRegex(ValueError, "retained"):
            self.f.run(strict=True)
        self.assertEqual(before, self.f.output_hashes())

    def test_unused_report_family_does_not_block_the_paper_export(self):
        self.f.baseline()
        self.f.report("ripe-atlas-role-by-strategy", {"invalid": "not needed"})
        result = self.f.run(strict=True)
        self.assertFalse(result["input_errors"])
        self.assertEqual(
            result["values"]["results.eu.tcp.no-conn.rt-base.success.count"]["raw"], 10
        )

    def test_canonical_run_wins_over_a_repetition(self):
        self.f.baseline()
        self.f.strategy({"RANDOM": 999}, phase="fi-mass", run="tcp-80_repeat")
        self.assertEqual(
            self.f.run(strict=True)["values"]["results.eu.tcp.no-conn.fi-mass.success.random.pct"][
                "raw"
            ],
            100,
        )

    def test_disagreeing_canonical_copies_are_rejected(self):
        self.f.baseline()
        self.f.strategy({"SINGLE": 10}, phase="rt-fi", processed=True)
        with self.assertRaisesRegex(ValueError, "disagree"):
            self.f.run(strict=True)

    def test_bad_receipt_is_rejected(self):
        path = self.f.strategy({"CONSTANT": 10}, receipt=True)
        path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "receipt"):
            self.f.run(strict=True)

    def test_archive_and_directory_conflicts_are_rejected(self):
        self.f.baseline()
        path = self.f.report(
            "strategies",
            dict(protocol="tcp", counts={"SINGLE": 10}, total_ips=10),
            phase="rt-fi",
        )
        archive = self.f.root / "input.zip"
        with zipfile.ZipFile(archive, "w") as z:
            z.writestr(path.relative_to(self.f.root).as_posix(), path.read_bytes())
        self.f.strategy({"CONSTANT": 10}, phase="rt-fi")
        with self.assertRaisesRegex(ValueError, "disagree"):
            self.f.run(archive=archive, strict=True)

    def test_recorded_configuration_is_shared_without_filling_missing_results(self):
        self.f.snapshot()
        self.f.snapshot(phase="fi-mass")
        v = self.f.run(strict=True)["values"]
        self.assertEqual(v["params.rt-base.sequence-length.count"]["raw"], 16)
        self.assertEqual(v["params.fi-mass.sequence-length.count"]["raw"], 100)
        self.assertEqual(v["params.fi-mass.min-replies.count"]["raw"], 80)
        self.assertEqual(v["params.fi-mass.request-interval"]["raw"], "20ms")
        self.assertIsNone(v["results.na.tcp.no-conn.rt-base.success.count"]["raw"])

    def test_shared_setting_conflict_fails_even_without_strict(self):
        self.f.snapshot()
        self.f.run()
        before = self.f.output_hashes()
        self.f.snapshot(protocol="icmp", requests_per_connection=5)
        with self.assertRaisesRegex(ValueError, "conflicting"):
            self.f.run()
        self.assertEqual(before, self.f.output_hashes())

    def test_invalid_yaml_is_reported_as_an_input_error(self):
        self.f.snapshot().write_text("connection_count: [", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "invalid snapshot"):
            self.f.run(strict=True)

    def test_reported_mass_configuration_works_without_raw_data(self):
        target = "tcp.ipid.no-connection.fixed-interval.mass"
        self.f.report(
            "missing-replies",
            dict(
                measurements=[
                    dict(
                        target=target,
                        sequence_length=100,
                        configured_minimum_replies=80,
                    )
                ]
            ),
            phase="fi-mass",
        )
        v = self.f.run()["values"]
        self.assertEqual(v["params.fi-mass.sequence-length.count"]["raw"], 100)
        self.assertEqual(v["params.fi-mass.min-replies.count"]["raw"], 80)

    def test_scalar_report_lookup_reads_only_the_selected_fields(self):
        self.f.report(
            "itdk-role-by-strategy",
            dict(
                plot_percentages={"Transit-Observed": {"SINGLE": 33.3}},
                plot_role_totals={"Transit-Observed": 1},
                coverage={"total": 3},
            ),
            phase="rt-fi",
        )
        v = self.f.run(strict=True)["values"]
        self.assertEqual(
            v["stats.eu.tcp.no-conn.rt-fi.itdk-strategy.transit-observed.single.pct"]["raw"],
            33.3,
        )
        self.assertAlmostEqual(
            v["claims.eu.tcp.no-conn.rt-fi.itdk.transit-observed.pct"]["raw"], 100 / 3
        )

    def test_missing_role_denominator_does_not_turn_a_count_into_a_percentage(self):
        self.f.report(
            "itdk-role-by-strategy", dict(plot_role_totals={"Transit-Observed": 12}), phase="rt-fi"
        )
        result = self.f.run(strict=True)
        self.assertIsNone(
            result["values"]["claims.eu.tcp.no-conn.rt-fi.itdk.transit-observed.pct"]["raw"]
        )

    def test_legacy_cache_is_discarded(self):
        (self.f.root / "tmp").mkdir()
        (self.f.root / "tmp/values.json").write_text(
            '{"schema_version":1,"contexts":{}}', encoding="utf-8"
        )
        self.f.run()
        data = json.loads((self.f.root / "tmp/values.json").read_text())
        self.assertEqual(data["schema_version"], 2)
        self.assertTrue(all(not ctx["entries"] for ctx in data["contexts"].values()))

    def test_additional_report_uses_canonical_keys_and_the_correct_campaign(self):
        path = self.f.root / "extra.json"
        key = "validation.fi-ideal.accuracy-pct"
        data = dict(
            schema_version=1,
            campaign_id=CAMPAIGN,
            values={
                key: dict(
                    value=99.3,
                    unit="percent",
                    definition="current synthetic validation",
                )
            },
        )
        path.write_text(json.dumps(data), encoding="utf-8")
        self.assertEqual(self.f.run(additional_reports=[path])["values"][key]["raw"], 99.3)
        before = self.f.output_hashes()
        data["campaign_id"] = "wrong"
        path.write_text(json.dumps(data), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "invalid additional"):
            self.f.run(additional_reports=[path])
        self.assertEqual(before, self.f.output_hashes())

    def test_exact_threshold_and_float_precision_are_preserved(self):
        threshold = 4.0383019016847365e-5
        kind = "tcp-no_connection-mass-random-reproducibility-evaluation"
        self.f.report(
            kind,
            dict(protocol="tcp", classifier_version="9", threshold=threshold, repeats=[]),
            phase="fi-mass",
        )
        result = self.f.run(strict=True)
        self.assertEqual(result["values"]["classifier.random.threshold"]["raw"], threshold)
        text = (self.f.root / "tmp/values.tex").read_text()
        self.assertIn(r"\setval{classifier.random.threshold}{4.0383019016847365e-05}", text)
        self.assertIn(r"\setval{classifier.single.max-inc}{21845}", text)

    def test_standard_todo_and_plain_values_in_the_tex_interface(self):
        text = render(
            {
                "example": dict(raw=None, unit=""),
                "count": dict(raw=1234567, unit="ips"),
                "pct": dict(raw=4.30974178195464, unit="percent"),
            },
            CAMPAIGN,
        )
        self.assertIn(r"\todo[inline]", text)
        self.assertIn(r"\setval{count}{1234567}", text)
        self.assertIn(r"\setval{pct}{4.31\%}", text)
        self.assertNotIn("texmeta@", text)

    def test_target_continent_is_not_mistaken_for_vp_scope(self):
        self.assertEqual(
            key_order("stats.eu.tcp.geo.as.count")[1],
            key_order("stats.af.tcp.geo.as.count")[1],
        )
        self.assertNotEqual(
            key_order("stats.eu.tcp.geo.as.count")[1],
            key_order("stats.eu.tcp.geo.af.count")[1],
        )

    def test_selected_pdfs_use_canonical_paths(self):
        self.f.baseline()
        path = (
            self.f.reports
            / "eu/figures"
            / RUNS["tcp"]
            / "no-connection/merged/rt-based-base_fixed-interval-mass/n-rt-b_fi-m_operating-system-group-by-strategy.pdf"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"%PDF-1.4\nfixture")
        v = self.f.run(strict=True)["values"]
        key = "plots.eu.tcp.no-conn.rt-fi.os-by-strategy"
        self.assertTrue(v[key]["raw"].startswith("reports/continental/"))
        path.write_bytes(b"invalid PDF")
        with self.assertRaisesRegex(ValueError, "PDF signature"):
            self.f.run(strict=True)

    def test_output_cannot_replace_analysis_input(self):
        (self.f.root / "tmp").mkdir()
        with self.assertRaisesRegex(ValueError, "inside analysis inputs"):
            export(CAMPAIGN, reports_root=self.f.root, repository_root=self.f.root)

    def test_only_two_export_files_are_written(self):
        self.f.run(strict=True)
        self.assertEqual(
            {p.name for p in (self.f.root / "tmp").iterdir()},
            {"values.tex", "values.json"},
        )

    def test_cli_reads_archive_and_does_not_import_analysis_or_plotting(self):
        archive = self.f.root / "empty.zip"
        with zipfile.ZipFile(archive, "w"):
            pass
        package = Path(__file__).resolve().parents[1] / "paper_values"
        shutil.copytree(
            package,
            self.f.root / "paper_values",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        code = 'import sys; from paper_values.exporter import main; main(); assert not any(m.split(".")[0] in {"ipid_analysis","matplotlib","polars","pyarrow","yaml"} for m in sys.modules)'
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                code,
                "--campaign",
                CAMPAIGN,
                "--archive",
                str(archive),
                "--strict",
            ],
            cwd=self.f.root,
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("1497 empty", result.stdout)

    @unittest.skipUnless(shutil.which("make"), "GNU Make is not installed")
    def test_make_accepts_both_argument_variable_names(self):
        archive = self.f.root / "empty.zip"
        with zipfile.ZipFile(archive, "w"):
            pass
        repository = Path(__file__).resolve().parents[1]
        shutil.copytree(
            repository / "paper_values",
            self.f.root / "paper_values",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        shutil.copyfile(repository / "Makefile", self.f.root / "Makefile")
        for name in ("ARGS", "args"):
            result = subprocess.run(
                [
                    "make",
                    "--no-print-directory",
                    "paper-values",
                    f"CAMPAIGN_ID={CAMPAIGN}",
                    f"PYTHON_INTERPRETER={shlex.quote(sys.executable)}",
                    f"{name}=--archive {shlex.quote(str(archive))} --strict",
                ],
                cwd=self.f.root,
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("1497 empty", result.stdout)


class DerivedCacheTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.f = Fixture(self.temp.name)

    def histogram(self, counts=(98, 1), increments=(1, 900), total=100):
        report = self.f.report("increment-distributions", dict(protocol="tcp"), phase="comparison")
        path = (
            self.f.processed
            / "eu"
            / RUNS["tcp"]
            / "no-connection"
            / (PATHS["comparison"] + "increment-distributions.pq")
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        cumulative = []
        for n in counts:
            cumulative.append((cumulative[-1] if cumulative else 0) + n)
        pq.write_table(
            pa.table(
                dict(
                    MODE=["RT-based"] * len(counts),
                    IPID_SELECTION_STRATEGY=["PER_BUCKET"] * len(counts),
                    INCREMENT=list(increments),
                    COUNT=list(counts),
                    CUMULATIVE_COUNT=cumulative,
                    TOTAL_COUNT=[total] * len(counts),
                )
            ),
            path,
        )
        return path, report

    def test_clipped_histogram_does_not_invent_tail_fractions(self):
        self.histogram()
        v = self.f.run(strict=True)["values"]
        self.assertEqual(v["incs.eu.tcp.no-conn.rt-base.per-bucket.p99"]["raw"], 900)
        self.assertIsNone(v["incs.eu.tcp.no-conn.rt-base.per-bucket.below-1000.pct"]["raw"])

    def test_cached_histogram_is_not_read_again(self):
        self.histogram(counts=(98, 2), total=100)
        first = self.f.run(strict=True)
        self.assertEqual(first["cache"]["histogram_misses"], 1)
        with patch(
            "paper_values.readers.increment_histogram",
            side_effect=AssertionError("must use cache"),
        ):
            second = self.f.run(strict=True)
        self.assertEqual(second["cache"]["histogram_hits"], 1)
        self.assertEqual(first["values"], second["values"])

    def test_changed_input_and_recompute_bypass_cached_values(self):
        self.histogram(counts=(98, 2), increments=(1, 900))
        self.f.run()
        self.histogram(counts=(90, 10), increments=(1, 1200))
        result = self.f.run(strict=True)
        self.assertEqual(result["cache"]["histogram_misses"], 1)
        key = "incs.eu.tcp.no-conn.rt-base.per-bucket.p99"
        self.assertEqual(result["values"][key]["raw"], 1200)
        self.assertEqual(self.f.run(recompute=True)["cache"]["histogram_misses"], 1)

    def test_bad_aggregate_receipt_preserves_both_existing_outputs(self):
        path, _ = self.histogram()
        marker = self.f.receipt(path)
        self.f.run(strict=True)
        before = self.f.output_hashes()
        data = json.loads(marker.read_text())
        data["sha256"] = "0" * 64
        marker.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError, "receipt"):
            self.f.run(strict=True)
        self.assertEqual(before, self.f.output_hashes())

    def test_os_family_ranges_reuse_the_existing_small_aggregate(self):
        groups = {
            g: dict(ip_count=100, section="General-Purpose OS")
            for g in ("rhel", "centos", "fedora")
        }
        self.f.report(
            "operating-system-group-by-strategy",
            dict(strategy_ip_count=500, matched_grouped_ip_count=300, groups=groups),
            protocol="icmp",
            phase="rt-fi",
        )
        path = (
            self.f.processed
            / "eu"
            / RUNS["icmp"]
            / "no-connection"
            / (PATHS["rt-fi"] + "operating-system-group-by-strategy.pq")
        )
        path.parent.mkdir(parents=True)
        pq.write_table(
            pa.table(
                dict(
                    OS_GROUP=["rhel", "centos", "fedora"],
                    IPID_SELECTION_STRATEGY=["PER_BUCKET"] * 3,
                    COUNT=[70, 80, 90],
                    PERCENTAGE=[70.0, 80.0, 90.0],
                )
            ),
            path,
        )
        result = self.f.run(strict=True)
        self.assertEqual(result["cache"]["os-strategy_misses"], 1)
        self.assertEqual(
            result["values"]["claims.eu.icmp.no-conn.rt-fi.os.rpm.per-bucket.min-pct"]["raw"],
            70,
        )
        self.assertEqual(
            result["values"]["claims.eu.icmp.no-conn.rt-fi.os.rpm.per-bucket.max-pct"]["raw"],
            90,
        )

    def test_cache_is_scoped_by_campaign_and_input_path(self):
        p = self.f.root / "input"
        p.write_text("input")
        cache_path = self.f.root / "values.json"
        cache = ValueCache(cache_path, "one")
        cache.derive("stat", p, "report", lambda: dict(value=1))
        cache_path.write_text(cache.serialize())
        other = ValueCache(cache_path, "two")
        self.assertEqual(other.derive("stat", p, "report", lambda: dict(value=2)), dict(value=2))
        self.assertEqual(other.stats["stat_misses"], 1)

    def test_missing_sources_are_rechecked_and_corrupt_cache_is_discarded(self):
        p = self.f.root / "input"
        cache_path = self.f.root / "values.json"
        cache_path.write_text("broken cache")
        cache = ValueCache(cache_path, CAMPAIGN)
        self.assertEqual(cache.derive("stat", p, "report", lambda: dict(value=1)), {})
        p.write_text("appeared")
        self.assertEqual(cache.derive("stat", p, "report", lambda: dict(value=2)), dict(value=2))

    def test_changed_report_and_corrupt_cached_payload_are_recomputed(self):
        p = self.f.root / "input"
        p.write_text("input")
        cache_path = self.f.root / "values.json"
        cache = ValueCache(cache_path, CAMPAIGN)
        cache.derive("stat", p, "report-1", lambda: dict(value=1))
        cache_path.write_text(cache.serialize())
        cache = ValueCache(cache_path, CAMPAIGN)
        self.assertEqual(cache.derive("stat", p, "report-2", lambda: dict(value=2)), dict(value=2))
        for entry in cache.entries.values():
            entry["values"] = {"value": 999}
        cache_path.write_text(cache.serialize())
        cache = ValueCache(cache_path, CAMPAIGN)
        self.assertEqual(cache.derive("stat", p, "report-1", lambda: dict(value=3)), dict(value=3))


@unittest.skipUnless(os.environ.get("IPID_PAPER_VALUES_ARCHIVE"), "optional supplied EU archive")
class RealEUArchiveTest(unittest.TestCase):
    def test_existing_eu_paper_values(self):
        with tempfile.TemporaryDirectory() as root:
            result = export(
                "geo-full-20261107-r1",
                reports_root=None,
                archive=os.environ["IPID_PAPER_VALUES_ARCHIVE"],
                repository_root=Path(root),
                strict=True,
            )
            v = result["values"]
            self.assertEqual((result["available"], result["missing"]), (153, 1344))
            for protocol, total in (
                ("icmp", 319105777),
                ("tcp", 94046744),
                ("udp", 3353747),
            ):
                key = f"results.eu.{protocol}.no-conn.rt-base.success.count"
                self.assertEqual(v[key]["raw"], total)
                self.assertIsNone(v[key.replace(".eu.", ".na.")]["raw"])
            self.assertEqual(
                v["results.eu.tcp.no-conn.rt-fi.rt-pop.constant.pct"]["formatted"],
                "77.80%",
            )
            self.assertEqual(v["refs.maxmind.build-month"]["raw"], "2026-11")
            self.assertTrue(
                v["plots.eu.tcp.no-conn.rt-fi.os-by-strategy"]["raw"].startswith("reports/")
            )
            self.assertFalse(result["input_errors"])


if __name__ == "__main__":
    unittest.main()
