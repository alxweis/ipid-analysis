"""Scientific denominator, partial-publication and LaTeX export regressions."""

import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from paper_values import exporter as pv

CAMPAIGN = "test-geographic-campaign"
ZMAP = "tcp-80_2026-11-07_03-30-08"
MERGED = "no-connection/merged/rt-based-base_fixed-interval-mass/n-rt-b_fi-m_"
RT = "no-connection/rt-based-base/n-rt-b_"
FI = "no-connection/fixed-interval-mass/n-fi-m_"
KEY = "results.vp-eu.tcp.no-connection.rt-fi-merged"


class Fixture:
    def __init__(self, root):
        self.root = Path(root)
        self.reports = self.root / "reports/continental" / CAMPAIGN
        self.processed = self.root / "processed/continental" / CAMPAIGN

    def report(
        self, stem, data, *, site="eu", run=ZMAP, processed=False, receipt=False
    ):
        path = (self.processed if processed else self.reports) / site
        if not processed:
            path /= "figures"
        path = path / run / (stem + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        content = (json.dumps(data) + "\n").encode()
        path.write_bytes(content)
        if receipt:
            marker = path.parent / ".ipid-complete" / (path.name + ".json")
            marker.parent.mkdir(exist_ok=True)
            marker.write_text(
                json.dumps(
                    {
                        "status": "complete",
                        "size_bytes": len(content),
                        "sha256": hashlib.sha256(content).hexdigest(),
                    }
                )
            )
        return path

    def strategy(self, stem, counts, **kwargs):
        data = dict(
            counts=counts, total_ips=sum(counts.values()), protocol="tcp", zmap_id=ZMAP
        )
        if stem == RT:
            data["measurement_id"] = "tcp-80_rt"
        self.report(stem + "strategies", data, **kwargs)

    def pdf(self, stem, *, site="eu", run=ZMAP, processed=False):
        path = (self.processed if processed else self.reports) / site
        if not processed:
            path /= "figures"
        path = path / run / (stem + ".pdf")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"%PDF-1.4\n% path-inventory fixture\n")
        return path

    def baseline(self, *, site="eu"):
        self.strategy(RT, {"CONSTANT": 6, "SINGLE": 2, "UNCLASSIFIED": 2}, site=site)
        self.strategy(FI, {"RANDOM": 1}, site=site)
        self.strategy(
            MERGED,
            {"CONSTANT": 6, "SINGLE": 2, "RANDOM": 1, "NOT_ENOUGH_SAMPLES": 1},
            site=site,
        )
        self.report(
            RT + "coverage",
            {"zmap_ip_count": 20, "ipid_ip_count": 10},
            site=site,
            processed=True,
        )
        self.report(
            FI + "coverage",
            {"zmap_ip_count": 2, "ipid_ip_count": 1},
            site=site,
            processed=True,
        )

    def export(self, **kwargs):
        kwargs.setdefault("profile", "full")
        return pv.export(
            CAMPAIGN,
            reports_root=self.reports,
            processed_root=self.processed,
            repository_root=self.root,
            **kwargs,
        )


class ExportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.fixture = Fixture(cls.directory.name)
        cls.fixture.baseline()
        cls.result = cls.fixture.export()
        cls.values = cls.result["values"]

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def test_merge_preserves_three_distinct_denominators(self):
        self.assertEqual(self.values[KEY + ".rt-results.total.count"]["raw"], 10)
        self.assertEqual(self.values[KEY + ".successful-results.total.count"]["raw"], 9)
        self.assertEqual(self.values[KEY + ".rt-results.constant.share-pct"]["raw"], 60)
        self.assertAlmostEqual(
            self.values[KEY + ".successful-results.constant.share-pct"]["raw"],
            100 * 6 / 9,
        )
        prefix = "coverage.vp-eu.tcp.no-connection.rt-fi-merged"
        self.assertEqual(self.values[prefix + ".all-targets.count"]["raw"], 20)
        self.assertEqual(self.values[prefix + ".successful.share-pct"]["raw"], 45)
        self.assertFalse(self.result["input_errors"])

    def test_missing_other_vp_is_empty_but_observed_zero_is_zero(self):
        key = KEY + ".rt-results.reflection.count"
        self.assertEqual(self.values[key]["formatted"], "0")
        self.assertEqual(self.values[key.replace("vp-eu", "vp-af")]["formatted"], "")
        self.assertEqual(
            self.values[key.replace("vp-eu", "vp-af")]["status"], "missing"
        )

    def test_no_self_pair_or_fake_paired_agreement(self):
        self.assertFalse(
            any(k.startswith("comparison.vp-eu.vp-eu.") for k in self.values)
        )
        paired = "comparison.vp-eu.vp-na.tcp.rt-fi-merged.agreement.share-pct"
        self.assertEqual(self.values[paired]["formatted"], "")
        marginal = (
            "comparison.all-vps.tcp.no-connection.rt-fi-merged.rt-results.constant"
        )
        self.assertEqual(self.values[marginal + ".available-vps.count"]["raw"], 1)
        self.assertEqual(
            self.values[marginal + ".range-percentage-points"]["formatted"], ""
        )

    def test_all_report_family_keys_exist_without_their_artifacts(self):
        key = (
            "reports.vp-af.tcp.no-connection.rt-fi-merged."
            "itdk-consistency-summary.strict-agreement-percentage"
        )
        self.assertIn(key, self.values)
        self.assertEqual(self.values[key]["formatted"], "")
        self.assertEqual(
            self.values["validation.pipeline.end-to-end.accuracy-pct"]["raw"], None
        )

    def test_latex_and_unrounded_provenance(self):
        text = (self.fixture.root / "tmp/values.tex").read_text()
        self.assertIn(
            r"\setval{" + KEY + r".rt-results.constant.share-pct}{60.00\%}", text
        )
        self.assertIn(
            r"\setval{"
            + KEY.replace("vp-eu", "vp-af")
            + r".rt-results.constant.count}{}",
            text,
        )
        sources = self.result["sources"]
        self.assertTrue(all(len(s["sha256"]) == 64 for s in sources))
        self.assertTrue(all(s["completion"] == "legacy-no-receipt" for s in sources))


class CurrentPaperSelectionTest(unittest.TestCase):
    def test_default_is_exact_reviewed_selection_with_independent_vp_values(self):
        from paper_values.profile import load_selection

        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            f.baseline(site="na")
            f.strategy(RT, {"CONSTANT": 3, "SINGLE": 5, "UNCLASSIFIED": 2}, site="na")
            f.strategy(
                MERGED,
                {"CONSTANT": 3, "SINGLE": 5, "RANDOM": 1, "NOT_ENOUGH_SAMPLES": 1},
                site="na",
            )
            result = pv.export(
                CAMPAIGN,
                reports_root=f.reports,
                processed_root=f.processed,
                repository_root=f.root,
            )
            self.assertEqual(result["profile"], "paper")
            expected = {
                entry["key"].format(vp=site)
                for entry in load_selection()["entries"]
                for site in (pv.SITES if "{vp}" in entry["key"] else (None,))
            }
            self.assertEqual(set(result["values"]), expected)
            key = "results.vp-eu.tcp.no-connection.rt-base.successful-results.constant.share-pct"
            self.assertEqual(result["values"][key]["raw"], 60)
            self.assertEqual(result["values"][key.replace("vp-eu", "vp-na")]["raw"], 30)
            self.assertIsNone(result["values"][key.replace("vp-eu", "vp-af")]["raw"])
            self.assertTrue(all(result["paper_references"][k] for k in expected))
            self.assertFalse(
                any(
                    "ripe-atlas" in k
                    or k.startswith(
                        ("interprotocol.", "comparison.", "ambiguity.", "applications.")
                    )
                    for k in expected
                )
            )
            self.assertNotIn("meta.campaign-id", expected)
            self.assertNotIn("plots.vp-eu.interprotocol.campaign.single", expected)
            self.assertFalse(result["input_errors"])
            self.assertLess((f.root / "tmp/values.tex").stat().st_size, 600_000)

    def test_population_comparison_does_not_use_the_full_rt_denominator(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            f.strategy("no-connection/fixed-interval-base/n-fi-b_", {"CONSTANT": 4})
            result = f.export(profile="paper")
            key = (
                "probing-comparison.vp-eu.tcp.no-connection.fi-to-rt-success.share-pct"
            )
            self.assertIsNone(result["values"][key]["raw"])

    def test_synthetic_accuracy_is_not_duplicated_per_vp(self):
        with tempfile.TemporaryDirectory() as root:
            result = Fixture(root).export(profile="paper")
            keys = result["values"]
            self.assertIn("validation.pipeline.fi-ideal.accuracy-pct", keys)
            self.assertFalse(
                any(k.startswith("validation.") and ".vp-" in k for k in keys)
            )
            self.assertNotIn("validation.pipeline.end-to-end.accuracy-pct", keys)

    def test_explicit_full_profile_still_includes_current_paper_keys(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            full = f.export(profile="full")
            compact = f.export(profile="paper")
            self.assertEqual(full["profile"], "full")
            self.assertTrue(set(compact["values"]).issubset(full["values"]))
            for key, value in compact["values"].items():
                self.assertEqual(value["raw"], full["values"][key]["raw"])

    def test_os_composition_uses_full_counts_and_explicit_family_members(self):
        from paper_values.profile import manuscript_metrics

        registry = pv.Registry()
        for group, value in (("rhel", 70), ("centos", 80), ("fedora", 90)):
            registry.put(
                "reports.vp-eu.icmp.no-connection.rt-fi-merged."
                f"operating-system-group-by-strategy.groups.{group}.strategies.per-bucket.share-pct",
                value,
                unit="percent",
            )
        a = pv.Artifact(
            "os.json",
            {},
            "",
            0,
            "",
            "eu",
            "icmp",
            "no-connection",
            "rt-fi-merged",
            "operating-system-group-by-strategy",
        )
        rows = [
            {"role": "Transit-Observed", "os_group": "debian", "count": 2},
            {"role": "Transit-Observed", "os_group": "ubuntu", "count": 1},
            {"role": "Transit-Observed", "os_group": "cisco", "count": 1},
        ]
        b = pv.Artifact(
            "roles.json",
            {"raw_distribution": rows},
            "",
            0,
            "",
            "eu",
            "icmp",
            "no-connection",
            "rt-fi-merged",
            "itdk-role-by-os",
        )
        errors = []
        manuscript_metrics(registry, [a, b], None, errors)
        self.assertFalse(errors)
        self.assertEqual(
            registry.values[
                "claims.vp-eu.icmp.no-connection.rt-fi-merged.os.rpm.per-bucket.min-share-pct"
            ].raw,
            70,
        )
        self.assertEqual(
            registry.values[
                "claims.vp-eu.icmp.no-connection.rt-fi-merged.itdk-os.transit-observed.deb.share-pct"
            ].raw,
            75,
        )

    def test_histogram_uses_full_counts_and_does_not_guess_beyond_clipped_tail(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            stem = "no-connection/comparison/rt-based-base_fixed-interval-base/n-rt-b_fi-b_increment-distributions"
            f.report(stem, {"protocol": "tcp"})
            path = f.processed / "eu" / ZMAP / (stem + ".pq")
            path.parent.mkdir(parents=True)
            pq.write_table(
                pa.table(
                    {
                        "MODE": ["RT-based", "RT-based"],
                        "IPID_SELECTION_STRATEGY": ["PER_BUCKET", "PER_BUCKET"],
                        "INCREMENT": [1, 900],
                        "COUNT": [98, 1],
                        "CUMULATIVE_COUNT": [98, 99],
                        "TOTAL_COUNT": [100, 100],
                    }
                ),
                path,
            )
            result = f.export(profile="paper")
            self.assertFalse(result["input_errors"])
            key = "increments.vp-eu.tcp.no-connection.rt-base.per-bucket.p99"
            self.assertEqual(result["values"][key]["raw"], 900)
            # The last percent may still lie below 1000/2000: the sampled/clipped tail cannot tell us.
            self.assertIsNone(
                result["values"][
                    "increments.vp-eu.tcp.no-connection.rt-base.per-bucket.below-1000.share-pct"
                ]["raw"]
            )


class InputSafetyTest(unittest.TestCase):
    def test_bad_count_report_never_leaks_partially_registered_values(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.report(
                MERGED + "strategies",
                {"protocol": "tcp", "total_ips": 10, "counts": {"CONSTANT": 9}},
            )
            result = f.export()
            self.assertEqual(len(result["input_errors"]), 1)
            self.assertEqual(
                result["values"][KEY + ".rt-results.total.count"]["raw"], None
            )

    def test_duplicate_scientific_summaries_are_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.strategy(MERGED, {"CONSTANT": 10})
            f.strategy(MERGED, {"SINGLE": 10}, processed=True)
            result = f.export()
            self.assertEqual(len(result["input_errors"]), 1)
            self.assertIn("disagree", result["input_errors"][0]["error"])
            self.assertIsNone(result["values"][KEY + ".rt-results.total.count"]["raw"])

    def test_mismatched_completion_receipt_is_not_accepted(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.strategy(MERGED, {"CONSTANT": 10}, receipt=True)
            path = next(f.reports.rglob("*strategies.json"))
            path.write_text(path.read_text() + " ")
            result = f.export()
            self.assertEqual(len(result["input_errors"]), 1)
            self.assertIsNone(result["values"][KEY + ".rt-results.total.count"]["raw"])

    def test_same_archive_path_with_different_bytes_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.strategy(MERGED, {"CONSTANT": 10})
            path = next(f.reports.rglob("*strategies.json"))
            archive = Path(root) / "reports.zip"
            with zipfile.ZipFile(archive, "w") as z:
                z.writestr(
                    path.relative_to(root).as_posix(),
                    json.dumps(
                        {"protocol": "tcp", "total_ips": 10, "counts": {"SINGLE": 10}}
                    ),
                )
            result = f.export(archive=archive)
            self.assertEqual(len(result["input_errors"]), 1)
            self.assertIsNone(result["values"][KEY + ".rt-results.total.count"]["raw"])

    def test_repetition_is_not_used_as_baseline(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            f.strategy(FI, {"RANDOM": 999}, run="tcp-80_repeat")
            result = f.export()
            key = (
                "results.vp-eu.tcp.no-connection.fi-mass.successful-results.total.count"
            )
            self.assertEqual(result["values"][key]["raw"], 1)

    def test_conflicting_coverage_fails_before_replacing_an_existing_export(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            f.export()
            before = (f.root / "tmp/values.tex").read_bytes()
            f.report(
                RT + "coverage",
                {"zmap_ip_count": 20, "ipid_ip_count": 11},
                processed=True,
            )
            with self.assertRaisesRegex(ValueError, "(merged|strategy) population"):
                f.export()
            self.assertEqual((f.root / "tmp/values.tex").read_bytes(), before)

    def test_additional_report_must_belong_to_this_campaign(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            path = Path(root) / "extra.json"
            path.write_text(
                json.dumps({"schema_version": 1, "campaign_id": "wrong", "values": {}})
            )
            with self.assertRaisesRegex(ValueError, "invalid additional"):
                f.export(additional_reports=[path])

    def test_empty_campaign_is_a_supported_partial_export(self):
        with tempfile.TemporaryDirectory() as root:
            result = Fixture(root).export()
            self.assertFalse(result["input_errors"])
            self.assertIsNone(result["values"][KEY + ".rt-results.total.count"]["raw"])


class PlotPathsTest(unittest.TestCase):
    plot_key = (
        "plots.vp-eu.tcp.no-connection.rt-fi-merged.operating-system-group-by-strategy"
    )

    def test_reported_repeat_without_pdf_has_an_empty_key(self):
        registry = pv.Registry()
        a = pv.Artifact(
            "repeat.json",
            {"repeats": [{"repeat_index": 1, "measurement_id": "tcp-80_repeat"}]},
            "",
            0,
            "",
            "eu",
            "tcp",
            "no-connection",
            "fi-mass",
            "random-reproducibility-evaluation",
        )
        pv.plot_values(registry, [], [a], [])
        registry.mirror_missing_vps()
        key = "plots.vp-eu.tcp.no-connection.fi-mass.repetitions.repeat-1.strategies"
        self.assertIsNone(registry.values[key].raw)
        self.assertEqual(registry.values[key].unit, "pdf-path")
        self.assertIsNone(registry.values[key.replace("vp-eu", "vp-af")].raw)

    def test_canonical_pdf_is_relative_and_uses_filename_safe_tex(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            stem = MERGED + "operating-system-group-by-strategy"
            f.pdf(stem)
            f.pdf(stem, run="tcp-80_measurement-copy")
            result = f.export()
            self.assertFalse(result["input_errors"])
            expected = f"reports/continental/{CAMPAIGN}/eu/figures/{ZMAP}/{stem}.pdf"
            self.assertEqual(result["values"][self.plot_key]["raw"], expected)
            self.assertEqual(result["values"][self.plot_key]["unit"], "pdf-path")
            self.assertEqual(
                result["values"][self.plot_key.replace("vp-eu", "vp-af")]["formatted"],
                "",
            )
            text = (f.root / "tmp/values.tex").read_text()
            self.assertIn(
                r"\setval{" + self.plot_key + r"}{\detokenize{" + expected + "}}", text
            )
            line = next(
                line
                for line in text.splitlines()
                if line.startswith(r"\setval{" + self.plot_key)
            )
            self.assertNotIn(r"\_", line)
            self.assertNotIn(str(f.root), line)
            self.assertIn(r"\endcsname%", text)

    def test_plot_without_json_and_renamed_report_mount(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            stem = "interprotocol-campaign-per-bucket"
            f.pdf(stem, run="interprotocol/interprotocol/runs/interprotocol")
            renamed = Path(root) / "renamed report mount"
            f.reports.rename(renamed)
            result = pv.export(
                CAMPAIGN,
                reports_root=renamed,
                repository_root=Path(root),
                profile="full",
            )
            key = "plots.vp-eu.interprotocol.campaign.per-bucket"
            self.assertEqual(
                result["values"][key]["raw"],
                f"reports/continental/{CAMPAIGN}/eu/figures/interprotocol/"
                f"interprotocol/runs/interprotocol/{stem}.pdf",
            )

    def test_report_without_pdf_and_processed_only_pdf_stay_empty(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            f.pdf(MERGED + "operating-system-group-by-strategy", processed=True)
            result = f.export()
            self.assertIsNone(result["values"][self.plot_key]["raw"])

    def test_archive_removes_only_the_archive_and_machine_prefix(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            path = f.pdf(MERGED + "operating-system-group-by-strategy")
            archive = Path(root) / "plots.zip"
            relative = path.relative_to(f.reports).as_posix()
            with zipfile.ZipFile(archive, "w") as z:
                z.writestr(
                    f"bundle/reports/continental/{CAMPAIGN}/{relative}",
                    path.read_bytes(),
                )
            result = pv.export(
                CAMPAIGN, reports_root=None, archive=archive, repository_root=Path(root)
            )
            self.assertEqual(
                result["values"][self.plot_key]["raw"],
                f"reports/continental/{CAMPAIGN}/{relative}",
            )

    def test_repetition_has_an_explicit_key_instead_of_replacing_baseline(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            f.report(
                "no-connection/fixed-interval-mass/random-reproducibility/"
                "tcp-no_connection-mass-random-reproducibility-evaluation",
                {
                    "protocol": "tcp",
                    "repeats": [{"repeat_index": 1, "measurement_id": "tcp-80_repeat"}],
                },
            )
            f.pdf(FI + "strategies")
            f.pdf(FI + "strategies", run="tcp-80_repeat")
            result = f.export()
            self.assertFalse(result["input_errors"])
            baseline = "plots.vp-eu.tcp.no-connection.fi-mass.strategies"
            repeat = (
                "plots.vp-eu.tcp.no-connection.fi-mass.repetitions.repeat-1.strategies"
            )
            self.assertIn(ZMAP, result["values"][baseline]["raw"])
            self.assertIn("tcp-80_repeat", result["values"][repeat]["raw"])

    def test_multiple_unpinned_pdfs_are_not_guessed(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.pdf(MERGED + "operating-system-group-by-strategy", run="tcp-80_first")
            f.pdf(MERGED + "operating-system-group-by-strategy", run="tcp-80_second")
            result = f.export()
            self.assertEqual(len(result["input_errors"]), 1)
            self.assertIsNone(result["values"][self.plot_key]["raw"])

    def test_invalid_pdf_does_not_create_an_available_path(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            path = f.pdf(MERGED + "operating-system-group-by-strategy")
            path.write_bytes(b"not a PDF")
            result = f.export()
            self.assertEqual(len(result["input_errors"]), 1)
            self.assertIsNone(result["values"][self.plot_key]["raw"])

    def test_paths_cannot_escape_the_paper_tree_or_inject_tex(self):
        for path in (
            "/mnt/data01/report.pdf",
            "C:/paper/report.pdf",
            "./reports/test.pdf",
            "reports/continental/../report.pdf",
            "reports/continental/x%bad.pdf",
            "reports/continental/x{bad}.pdf",
        ):
            with self.subTest(path=path), self.assertRaises(ValueError):
                pv.validate_plot_path(path)
        self.assertEqual(
            pv.validate_plot_path("reports/continental/test_plot with spaces.pdf"),
            "reports/continental/test_plot with spaces.pdf",
        )


class DerivedMetricsTest(unittest.TestCase):
    def test_prose_ratios_keep_os_section_and_tcp_difference_denominators(self):
        r = pv.Registry()
        os_data = {
            "matched_grouped_ip_count": 4,
            "strategy_ip_count": 10,
            "groups": {
                "ubuntu": {"ip_count": 3, "section": "General-Purpose OS"},
                "windows": {"ip_count": 1, "section": "General-Purpose OS"},
            },
        }
        a = pv.Artifact(
            "os.json",
            os_data,
            "",
            0,
            "",
            "eu",
            "tcp",
            "no-connection",
            "rt-fi-merged",
            "operating-system-group-by-strategy",
        )
        pv.report_claims(r, a)
        prefix = "claims.vp-eu.tcp.no-connection.rt-fi-merged"
        self.assertEqual(r.values[prefix + ".os.matched-of-results.share-pct"].raw, 40)
        self.assertEqual(
            r.values[prefix + ".os.groups.ubuntu.within-section.share-pct"].raw, 75
        )
        data = {
            "bars": {
                "SYN-ACK": {"total": 5, "counts": {"CONSTANT": 4}},
                "RST": {"total": 5, "counts": {"CONSTANT": 1}},
            }
        }
        a = pv.Artifact(
            "flags.json",
            data,
            "",
            0,
            "",
            "eu",
            "tcp",
            "no-connection",
            "rt-fi-merged",
            "tcp-flags-by-strategy",
        )
        pv.report_claims(r, a)
        difference = r.values[
            prefix + ".flags.constant.syn-ack-minus-rst.percentage-points"
        ]
        self.assertEqual(difference.raw, 60)
        self.assertEqual(difference.unit, "percentage-points")
        self.assertEqual(
            r.values[prefix + ".flags.constant.syn-ack-relative-to-rst.gain-pct"].raw,
            300,
        )

    def test_rt_fi_matrix_uses_existing_paired_cells(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            stem = (
                "no-connection/comparison/rt-based-base_fixed-interval-base/"
                "n-rt-b_fi-b_strategy-intersection"
            )
            f.report(stem, {"protocol": "tcp", "included_ip_count": 4})
            path = f.processed / "eu" / ZMAP / (stem + ".pq")
            path.parent.mkdir(parents=True)
            pq.write_table(
                pa.table(
                    {
                        "RT_BASED_STRATEGY": ["CONSTANT"],
                        "FIXED_INTERVAL_STRATEGY": ["SINGLE"],
                        "COUNT": [3],
                        "RT_BASED_TOTAL": [4],
                        "PERCENTAGE": [75.0],
                    }
                ),
                path,
            )
            result = f.export()
            self.assertFalse(result["input_errors"])
            key = (
                "reports.vp-eu.tcp.no-connection.rt-base-vs-fi-base.strategy-intersection."
                "cells.fixed-interval-strategy-single.rt-based-strategy-constant.percentage"
            )
            self.assertEqual(result["values"][key]["raw"], 75)

    def test_regional_os_coverage_uses_scanned_targets_not_evidence_rows(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.report(
                "shared-os-coverage",
                {
                    "detail": {
                        "targets": {"total": 20, "with_evidence": 10},
                        "classification": {
                            "resolved": 4,
                            "ambiguous": 3,
                            "unclassified": 3,
                        },
                    },
                    "overview": {"resolved_coverage": 0.2},
                },
                processed=True,
            )
            result = f.export()
            self.assertFalse(result["input_errors"])
            self.assertEqual(
                result["values"]["os.vp-eu.tcp.resolved.share-pct"]["raw"], 20
            )
            self.assertEqual(result["values"]["os.vp-eu.tcp.ambiguous.count"]["raw"], 3)

    def test_interprotocol_outcomes_include_insufficient_replies_in_denominator(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.report(
                "interprotocol-campaign-summary",
                {
                    "version": 2,
                    "rows": 10,
                    "groups": {
                        "icmp-tcp": {
                            "rows": 10,
                            "counts": {
                                "SINGLE:PROTOCOL_SHARED": 6,
                                "SINGLE:STRATEGY_CHANGED": 1,
                                "SINGLE:NOT_ENOUGH_SAMPLES": 3,
                            },
                        }
                    },
                },
                processed=True,
                run="interprotocol",
            )
            result = f.export()
            self.assertFalse(result["input_errors"])
            prefix = "interprotocol.vp-eu.icmp-tcp.single"
            self.assertEqual(result["values"][prefix + ".targets.count"]["raw"], 10)
            self.assertEqual(
                result["values"][prefix + ".protocol-shared.share-pct"]["raw"], 60
            )
            self.assertEqual(
                result["values"][prefix + ".protocol-isolated.count"]["raw"], 0
            )

    def test_interprotocol_relationship_identity_survives_strict_export(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            partial = [
                dict(
                    shared_pairs=["icmp-tcp"],
                    compatible_partitions=["icmp-tcp"],
                    count=2,
                ),
                dict(
                    shared_pairs=["icmp-udp"],
                    compatible_partitions=["icmp-udp"],
                    count=3,
                ),
                dict(
                    shared_pairs=["icmp-tcp"],
                    compatible_partitions=["icmp-tcp", "icmp-udp"],
                    count=4,
                ),
            ]
            parameters = [
                dict(matching_pairs=["icmp-tcp"], count=5),
                dict(matching_pairs=["tcp-udp"], count=7),
            ]
            for site in ("eu", "na"):
                f.report(
                    "interprotocol-campaign-summary",
                    {
                        "version": 2,
                        "rows": 21,
                        "groups": {
                            "icmp-tcp-udp": {
                                "rows": 21,
                                "counts": {
                                    "PER_BUCKET:PARTIAL_PROTOCOL_SHARED": 9,
                                    "CONSTANT:PARTIALLY_MATCHING_PARAMETERS": 12,
                                },
                            }
                        },
                        "partial_relationships": [
                            dict(
                                row,
                                baseline="PER_BUCKET",
                                protocol_group="icmp-tcp-udp",
                                outcome="PARTIAL_PROTOCOL_SHARED",
                            )
                            for row in partial
                        ],
                        "parameter_relationships": [
                            dict(
                                row,
                                baseline="CONSTANT",
                                protocol_group="icmp-tcp-udp",
                                outcome="PARTIALLY_MATCHING_PARAMETERS",
                            )
                            for row in parameters
                        ],
                    },
                    site=site,
                    processed=True,
                    run="interprotocol/interprotocol/runs/interprotocol",
                )
            before = {
                p: (p.read_bytes(), p.stat().st_mtime_ns)
                for p in f.processed.rglob("*")
                if p.is_file()
            }
            for profile in ("paper", "full"):
                result = f.export(profile=profile, strict=True)
                self.assertFalse(result["input_errors"])
                if profile == "full":
                    for site in ("eu", "na"):
                        prefix = (
                            f"reports.vp-{site}.all-protocols.mixed.interprotocol."
                            "interprotocol-campaign-summary."
                        )
                        for kind, counts in (
                            ("partial-relationships.", [2, 3, 4]),
                            ("parameter-relationships.", [5, 7]),
                        ):
                            actual = sorted(
                                v["raw"]
                                for k, v in result["values"].items()
                                if k.startswith(prefix + kind) and k.endswith(".count")
                            )
                            self.assertEqual(actual, counts)
                        self.assertEqual(
                            result["values"][
                                f"interprotocol.vp-{site}.icmp-tcp-udp.per-bucket.targets.count"
                            ]["raw"],
                            9,
                        )
                self.assertEqual(
                    {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in before}, before
                )
                self.assertEqual(
                    sorted(p.name for p in (f.root / "tmp").iterdir()),
                    ["values.json", "values.tex"],
                )

    def test_relationship_identifiers_are_lossless_not_punctuation_normalized(self):
        rows = [
            dict(baseline="PER_BUCKET", shared_pairs=pairs, count=count)
            for pairs, count in (
                (["icmp-tcp", "udp"], 1),
                (["icmp", "tcp-udp"], 2),
                ([], 3),
            )
        ]
        flattened = list(pv.flatten({"partial_relationships": rows}))
        self.assertEqual(len({path for path, value in flattened}), 3)
        self.assertEqual(sorted(value for path, value in flattened), [1, 2, 3])

    def test_true_duplicate_relationship_counts_are_still_rejected(self):
        registry = pv.Registry()
        rows = [
            dict(baseline="PER_BUCKET", shared_pairs=["icmp-tcp"], count=count)
            for count in (2, 3)
        ]
        with self.assertRaisesRegex(ValueError, "ambiguous or duplicate"):
            for path, value in pv.flatten({"partial_relationships": rows}):
                registry.put(".".join(path), value)

    def test_only_actual_snapshot_parameters_are_exported(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.strategy(RT, {"CONSTANT": 10})
            raw = f.root / "raw"
            snapshot = raw / "eu/ipid/tcp-80_rt/ipid.snapshot.yaml"
            snapshot.parent.mkdir(parents=True)
            snapshot.write_text(
                "connection_count: 7\nnumber_of_inflight_probes: 4321\n"
            )
            result = f.export(raw_root=raw)
            prefix = "parameters.vp-eu.tcp.no-connection.rt-base"
            self.assertEqual(result["values"][prefix + ".connection-count"]["raw"], 7)
            self.assertIsNone(
                result["values"][prefix + ".requests-per-connection"]["raw"]
            )
            self.assertEqual(
                result["values"][prefix + ".number-of-inflight-probes"]["raw"], 4321
            )

    def test_small_os_aggregate_is_read_without_raw_scan(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            stem = MERGED + "operating-system-group-by-strategy"
            f.report(
                stem,
                {
                    "protocol": "tcp",
                    "zmap_id": ZMAP,
                    "grouped_ip_count": 4,
                    "groups": {"windows": {"ip_count": 4}},
                },
            )
            path = f.processed / "eu" / ZMAP / (stem + ".pq")
            path.parent.mkdir(parents=True)
            pq.write_table(
                pa.table(
                    {
                        "OS_GROUP": ["windows"],
                        "IPID_SELECTION_STRATEGY": ["PER_DESTINATION"],
                        "COUNT": [3],
                        "PERCENTAGE": [75.0],
                    }
                ),
                path,
            )
            result = f.export()
            key = (
                "reports.vp-eu.tcp.no-connection.rt-fi-merged."
                "operating-system-group-by-strategy.groups.windows.strategies.per-destination"
            )
            self.assertEqual(result["values"][key + ".share-pct"]["raw"], 75)
            self.assertEqual(result["values"][key + ".count"]["raw"], 3)

    def test_six_vp_range_is_not_reported_as_paired_agreement(self):
        r = pv.Registry()
        pv.reserve_catalog(r)
        for index, site in enumerate(pv.SITES):
            key = f"results.vp-{site}.tcp.no-connection.rt-fi-merged.rt-results.constant.share-pct"
            r.put(key, 10 * index, unit="percent")
        pv.marginal_comparison(r)
        key = (
            "comparison.all-vps.tcp.no-connection.rt-fi-merged."
            "rt-results.constant.range-percentage-points"
        )
        self.assertEqual(r.values[key].raw, 50)
        self.assertIsNone(
            r.values["comparison.vp-eu.vp-na.tcp.rt-fi-merged.agreement.count"].raw
        )

    def test_zero_denominator_has_zero_count_but_empty_percentage(self):
        r = pv.Registry()
        a = pv.Artifact(
            "zero",
            {"counts": {}, "total_ips": 0},
            "",
            0,
            "",
            "eu",
            "tcp",
            "no-connection",
            "rt-fi-merged",
            "strategies",
        )
        pv.strategy_values(r, a)
        self.assertEqual(r.values[KEY + ".rt-results.constant.count"].raw, 0)
        self.assertIsNone(r.values[KEY + ".rt-results.constant.share-pct"].raw)

    def test_list_rows_have_role_source_and_os_identity(self):
        paths = list(
            pv.flatten(
                {
                    "rows": [
                        {
                            "role": "Transit",
                            "strategy": "SINGLE",
                            "source": "CAIDA",
                            "count": 1,
                        },
                        {
                            "role": "Transit",
                            "strategy": "SINGLE",
                            "source": "RIPE",
                            "count": 2,
                        },
                        {"role": "Transit", "os_group": "windows", "count": 3},
                    ]
                }
            )
        )
        self.assertEqual(len(set(p for p, v in paths)), 3)

    def test_tex_escaping_cannot_execute_an_external_string(self):
        self.assertEqual(
            pv.tex_escape(r"50% & a_b\input{x}"),
            r"50\% \& a\_b\textbackslash{}input\{x\}",
        )


@unittest.skipUnless(
    os.environ.get("IPID_PAPER_VALUES_ARCHIVE"), "optional supplied EU archive"
)
class RealEUArchiveTest(unittest.TestCase):
    def test_verified_archive_counts_and_partial_vps(self):
        with tempfile.TemporaryDirectory() as root:
            result = pv.export(
                "geo-full-20261107-r1",
                profile="full",
                reports_root=None,
                archive=os.environ["IPID_PAPER_VALUES_ARCHIVE"],
                repository_root=Path(root),
            )
            self.assertFalse(result["input_errors"])
            values = result["values"]
            for protocol, total in (
                ("icmp", 319105777),
                ("tcp", 94046744),
                ("udp", 3353747),
            ):
                key = f"results.vp-eu.{protocol}.no-connection.rt-fi-merged.rt-results.total.count"
                self.assertEqual(values[key]["raw"], total)
                self.assertIsNone(values[key.replace("vp-eu", "vp-as")]["raw"])
            self.assertEqual(
                values[KEY + ".rt-results.constant.share-pct"]["formatted"], "77.80%"
            )
            coverage = "coverage.vp-eu.tcp.no-connection.rt-fi-merged.successful"
            self.assertEqual(values[coverage + ".count"]["raw"], 93695603)
            self.assertAlmostEqual(
                values[coverage + ".share-pct"]["raw"], 100 * 93695603 / 109834190
            )
            key = PlotPathsTest.plot_key
            expected = (
                "reports/continental/geo-full-20261107-r1/eu/figures/"
                "tcp-80_2026-11-07_03-30-08/no-connection/merged/"
                "rt-based-base_fixed-interval-mass/"
                "n-rt-b_fi-m_operating-system-group-by-strategy.pdf"
            )
            self.assertEqual(values[key]["raw"], expected)
            self.assertTrue(
                values["plots.vp-eu.interprotocol.campaign.single"]["raw"].endswith(
                    ".pdf"
                )
            )


class CacheTest(unittest.TestCase):
    def test_parallel_reads_match_serial_export_and_ignore_unrelated_json(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            f.report("not-an-analysis-report", {"unused": 1})
            ignored = next(f.reports.rglob("not-an-analysis-report.json"))
            ignored.write_text("not valid JSON; must not be opened")
            serial = f.export(profile="paper", strict=True, io_workers=1)
            parallel = f.export(
                profile="paper", strict=True, io_workers=8, recompute=True
            )
            self.assertEqual(serial["values"], parallel["values"])
            self.assertFalse(parallel["input_errors"])

    def os_aggregate(self, f, *, site="eu", count=3):
        stem = MERGED + "operating-system-group-by-strategy"
        f.report(
            stem,
            {
                "protocol": "tcp",
                "strategy_ip_count": 4,
                "matched_grouped_ip_count": 4,
                "groups": {"windows": {"ip_count": 4, "section": "General-Purpose OS"}},
            },
            site=site,
        )
        path = f.processed / site / ZMAP / (stem + ".pq")
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "OS_GROUP": ["windows", "windows"],
                    "IPID_SELECTION_STRATEGY": ["PER_DESTINATION", "SINGLE"],
                    "COUNT": [count, 4 - count],
                    "PERCENTAGE": [count * 25, (4 - count) * 25],
                }
            ),
            path,
        )
        return path

    def test_warm_cache_avoids_parquet_json_and_pdf_payload_reads(self):
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            aggregate = self.os_aggregate(f)
            pdf = f.pdf(MERGED + "operating-system-group-by-strategy")
            cold = f.export(profile="paper", strict=True)
            tex = (f.root / "tmp/values.tex").read_bytes()
            original = Path.open

            def guarded(path, *args, **kwargs):
                if (
                    path == aggregate
                    or path == pdf
                    or (path.suffix == ".json" and path.is_relative_to(f.reports))
                ):
                    raise AssertionError(f"warm cache read source payload: {path}")
                return original(path, *args, **kwargs)

            with (
                patch.object(Path, "open", new=guarded),
                patch.object(
                    pq, "ParquetFile", side_effect=AssertionError("Parquet reread")
                ),
            ):
                warm = f.export(profile="paper", strict=True)
            self.assertEqual(cold["values"], warm["values"])
            self.assertEqual((f.root / "tmp/values.tex").read_bytes(), tex)
            self.assertEqual(warm["cache"]["aggregate_hits"], 1)
            self.assertEqual(warm["cache"]["pdf_hits"], 1)
            self.assertGreater(warm["cache"]["metadata_hits"], 0)
            self.assertNotIn(b"% Sources:", tex)
            with patch.object(pq, "ParquetFile", wraps=pq.ParquetFile) as reader:
                forced = f.export(profile="paper", strict=True, recompute=True)
                self.assertEqual(reader.call_count, 1)
            self.assertEqual(forced["values"], warm["values"])
            self.assertEqual(forced["cache"]["aggregate_misses"], 1)

    def test_changed_sources_and_new_results_invalidate_only_their_entries(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            self.os_aggregate(f)
            first = f.export(profile="paper", strict=True)
            self.os_aggregate(f, count=2)
            changed = f.export(profile="paper", strict=True)
            key = "reports.vp-eu.tcp.no-connection.rt-fi-merged.operating-system-group-by-strategy.groups.windows.strategies.per-destination.share-pct"
            self.assertEqual(first["values"][key]["raw"], 75)
            self.assertEqual(changed["values"][key]["raw"], 50)
            self.assertEqual(changed["cache"]["aggregate_misses"], 1)
            self.assertGreater(changed["cache"]["metadata_hits"], 0)
            f.baseline(site="na")
            self.os_aggregate(f, site="na", count=1)
            added = f.export(profile="paper", strict=True)
            self.assertEqual(added["values"][key.replace("vp-eu", "vp-na")]["raw"], 25)
            self.assertEqual(added["values"][key]["raw"], 50)

    def test_campaigns_input_roots_profiles_and_corrupt_cache_do_not_mix(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            expected = f.export(profile="paper", strict=True)["values"]
            other = pv.export(
                "other-campaign",
                reports_root=f.reports,
                processed_root=f.processed,
                repository_root=f.root,
                strict=True,
            )
            self.assertTrue(all(v["raw"] is None for v in other["values"].values()))
            again = f.export(profile="paper", strict=True)
            self.assertEqual(again["values"], expected)
            self.assertGreater(again["cache"]["metadata_hits"], 0)
            full = f.export(profile="full", strict=True)
            self.assertGreater(full["cache"]["metadata_misses"], 0)
            alternate = f.root / "alternate/continental" / CAMPAIGN
            shutil.copytree(f.reports, alternate)
            moved = pv.export(
                CAMPAIGN,
                reports_root=alternate,
                processed_root=f.processed,
                repository_root=f.root,
                strict=True,
            )
            self.assertGreater(moved["cache"]["metadata_misses"], 0)
            document = json.loads((f.root / "tmp/values.json").read_text())
            self.assertEqual(len(document["contexts"]), 4)
            (f.root / "tmp/values.json").write_text('{"broken":')
            rebuilt = f.export(profile="paper", strict=True)
            self.assertEqual(rebuilt["values"], expected)
            self.assertGreater(rebuilt["cache"]["metadata_misses"], 0)

    def test_histogram_and_snapshot_are_cached_and_missing_files_are_retried(self):
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.strategy(RT, {"SINGLE": 10})
            stem = "no-connection/comparison/rt-based-base_fixed-interval-base/n-rt-b_fi-b_increment-distributions"
            f.report(stem, {"protocol": "tcp"})
            raw = f.root / "raw"
            snapshot = raw / "eu/ipid/tcp-80_rt/ipid.snapshot.yaml"
            snapshot.parent.mkdir(parents=True)
            snapshot.write_text("connection_count: 4\nrequests_per_connection: 4\n")
            first = f.export(profile="paper", raw_root=raw, strict=True)
            key = "increments.vp-eu.tcp.no-connection.rt-base.per-bucket.p99"
            self.assertIsNone(first["values"][key]["raw"])
            path = f.processed / "eu" / ZMAP / (stem + ".pq")
            path.parent.mkdir(parents=True)
            pq.write_table(
                pa.table(
                    {
                        "MODE": ["RT-based"],
                        "IPID_SELECTION_STRATEGY": ["PER_BUCKET"],
                        "INCREMENT": [10],
                        "COUNT": [100],
                        "CUMULATIVE_COUNT": [100],
                        "TOTAL_COUNT": [100],
                    }
                ),
                path,
            )
            computed = f.export(profile="paper", raw_root=raw, strict=True)
            self.assertEqual(computed["values"][key]["raw"], 10)
            with patch.object(
                pq, "ParquetFile", side_effect=AssertionError("histogram reread")
            ):
                warm = f.export(profile="paper", raw_root=raw, strict=True)
            self.assertEqual(warm["values"], computed["values"])
            self.assertEqual(warm["cache"]["histogram_hits"], 1)
            self.assertEqual(warm["cache"]["snapshot_hits"], 1)

    def test_json_changes_and_receipt_changes_are_never_hidden_by_cache(self):
        with tempfile.TemporaryDirectory() as root:
            f = Fixture(root)
            f.baseline()
            f.export(profile="full", strict=True)
            f.report(
                RT + "coverage",
                {"zmap_ip_count": 25, "ipid_ip_count": 10},
                processed=True,
            )
            updated = f.export(profile="full", strict=True)
            key = "coverage.vp-eu.tcp.no-connection.rt-fi-merged.successful.share-pct"
            self.assertEqual(updated["values"][key]["raw"], 36)
            f.strategy(
                RT, {"CONSTANT": 6, "SINGLE": 2, "UNCLASSIFIED": 2}, receipt=True
            )
            f.export(profile="full", strict=True)
            tex = (f.root / "tmp/values.tex").read_bytes()
            cache = (f.root / "tmp/values.json").read_bytes()
            marker = next(f.reports.rglob(".ipid-complete/*strategies.json.json"))
            record = json.loads(marker.read_text())
            record["sha256"] = "0" * 64
            marker.write_text(json.dumps(record))
            with self.assertRaisesRegex(ValueError, "completion receipt"):
                f.export(profile="full", strict=True)
            self.assertEqual((f.root / "tmp/values.tex").read_bytes(), tex)
            self.assertEqual((f.root / "tmp/values.json").read_bytes(), cache)


class NativeWriterTest(unittest.TestCase):
    def test_real_writer_preserves_empty_zero_and_escaped_percent(self):
        with tempfile.TemporaryDirectory() as root:
            registry = pv.Registry()
            registry.reserve("missing")
            registry.put(
                "zero", 0, sources=("existing-report.json",), definition="Observed zero"
            )
            registry.put("percent", 85.625, unit="percent")
            registry.put("specials", "^~")
            path = "reports/continental/test_plot with spaces.pdf"
            registry.put("plots.test", path, unit="pdf-path")
            registry.reserve("plots.missing", unit="pdf-path")
            pv.emit(registry, Path(root) / "values.tex", CAMPAIGN, [], [])
            text = (Path(root) / "values.tex").read_text()
            self.assertIn(r"\setval{missing}{}", text)
            self.assertIn(r"\setval{zero}{0} % Observed zero", text)
            self.assertNotIn("% Sources:", text)
            self.assertIn(r"\setval{percent}{85.62\%}", text)
            self.assertNotIn(r"\xspace}", text)
            self.assertIn(
                r"\setval{specials}{\textasciicircum{}\textasciitilde{}}", text
            )
            self.assertIn(r"\setval{plots.test}{\detokenize{" + path + "}}", text)
            self.assertIn(r"\setval{plots.missing}{}", text)


class IsolationTest(unittest.TestCase):
    def test_output_link_into_inputs_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = root / "data"
            inputs.mkdir()
            try:
                (root / "tmp").symlink_to(inputs, target_is_directory=True)
            except OSError:
                self.skipTest("symlinks unavailable")
            with self.assertRaises(ValueError):
                pv.fixed_output_path(root, (), ())
            self.assertEqual(list(inputs.iterdir()), [])

    def test_removed_write_and_scan_options_fail_without_output(self):
        source = Path(pv.__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(
                source / "paper_values",
                root / "paper_values",
                ignore=shutil.ignore_patterns("__pycache__"),
            )
            for option in ("--output=other", "--writer=ndpi", "--scan-increments"):
                run = subprocess.run(
                    [
                        sys.executable,
                        "-B",
                        "-m",
                        "paper_values",
                        "--campaign",
                        CAMPAIGN,
                        option,
                    ],
                    cwd=root,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                self.assertEqual(run.returncode, 2, run.stderr)
                self.assertIn("unrecognized arguments", run.stderr)
                self.assertFalse((root / "tmp").exists())

    def test_make_and_rerun_only_replace_fixed_export_outputs(self):
        if not shutil.which("make"):
            self.skipTest("GNU Make required")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = Path(pv.__file__).resolve().parents[1]
            shutil.copytree(
                source / "paper_values",
                root / "paper_values",
                ignore=shutil.ignore_patterns("__pycache__"),
            )
            shutil.copy2(source / "Makefile", root / "Makefile")
            f = Fixture(root)
            f.baseline()
            (root / "tmp").mkdir()
            (root / "tmp/worker-state.json").write_text('{"preserve": true}')
            # Importing this existing analysis package must fail, even with no data.
            (root / "ipid_analysis").mkdir()
            (root / "ipid_analysis/__init__.py").write_text(
                'raise AssertionError("analysis imported")'
            )

            def snapshot():
                return {
                    p.relative_to(root).as_posix(): (
                        p.read_bytes(),
                        p.stat().st_mtime_ns,
                    )
                    for p in root.rglob("*")
                    if p.is_file()
                    and p not in (root / "tmp/values.tex", root / "tmp/values.json")
                }

            before = snapshot()
            for argument in ("ARGS=--strict", "args=--strict"):
                run = subprocess.run(
                    [
                        "make",
                        "paper-values",
                        f"CAMPAIGN_ID={CAMPAIGN}",
                        f"PYTHON_INTERPRETER={shlex.quote(sys.executable)}",
                        argument,
                    ],
                    cwd=root,
                    capture_output=True,
                    text=True,
                    timeout=90,
                )
                self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
                self.assertIn("-B -m paper_values", run.stdout)
                self.assertTrue((root / "tmp/values.tex").is_file())
                self.assertEqual(snapshot(), before)

            # A corrupt input under strict mode preserves a previously valid export.
            sentinel = (root / "tmp/values.tex").read_bytes()
            cache_before = (root / "tmp/values.json").read_bytes()
            f.report(RT + "strategies", {"counts": {"SINGLE": -1}, "total_ips": -1})
            before = snapshot()
            run = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "paper_values",
                    "--campaign",
                    CAMPAIGN,
                    "--strict",
                ],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=90,
            )
            self.assertNotEqual(run.returncode, 0)
            self.assertIn("existing tmp/values.tex retained", run.stderr)
            self.assertIn("strategies.json", run.stderr)
            self.assertEqual((root / "tmp/values.tex").read_bytes(), sentinel)
            self.assertEqual((root / "tmp/values.json").read_bytes(), cache_before)
            self.assertEqual(snapshot(), before)

    def test_no_analysis_imports_and_cwd_does_not_choose_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"
            source = Path(pv.__file__).resolve().parents[1]
            shutil.copytree(
                source / "paper_values",
                root / "paper_values",
                ignore=shutil.ignore_patterns("__pycache__"),
            )
            code = """import runpy, sys
sys.path.insert(0, sys.argv[1])
sys.argv = ["paper_values", "--campaign", "absent-campaign"]
runpy.run_module("paper_values", run_name="__main__")
assert not any(n.split(".")[0] in {"ipid_analysis", "matplotlib", "duckdb", "ndpi"}
               for n in sys.modules)
"""
            run = subprocess.run(
                [sys.executable, "-B", "-c", code, str(root)],
                cwd=directory,
                capture_output=True,
                text=True,
                timeout=90,
            )
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertTrue((root / "tmp/values.tex").is_file())
            self.assertFalse((Path(directory) / "tmp").exists())
            self.assertFalse(list(root.rglob("__pycache__")))

    def test_input_alias_rejected_and_atomic_failure_cleans_staging(self):
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "tmp/values.tex"
            with self.assertRaises(ValueError):
                pv.fixed_output_path(root, (), (output,))
            with self.assertRaises(ValueError):
                pv.fixed_output_path(root, (root / "tmp",), ())
            output.parent.mkdir()
            output.write_text("original")
            with patch.object(pv.os, "replace", side_effect=OSError("test failure")):
                with self.assertRaises(OSError):
                    pv.atomic_text(output, "new")
            self.assertEqual(output.read_text(), "original")
            self.assertEqual(list(output.parent.iterdir()), [output])

    def test_classifier_constants_are_data_not_executable_code(self):
        from paper_values.profile import classifier_constants

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "strategies.py"
            path.write_text("""raise AssertionError("must never execute")
MODULUS = 1 << 16
MIN_STEPS_BEFORE_WRAPAROUND = 3
CLASSIFIER_VERSION = "9"
MAX_INC = math.ceil(MODULUS / MIN_STEPS_BEFORE_WRAPAROUND) - 1
MULTI_MAX_INC = __import__("os").remove("sentinel")
MULTI_MAX_CLUSTERS = 16
""")
            constants = classifier_constants(path)
            self.assertEqual(constants["CLASSIFIER_VERSION"], "9")
            self.assertEqual(constants["MAX_INC"], 21845)
            self.assertEqual(constants["MULTI_MAX_CLUSTERS"], 16)
            self.assertNotIn("MULTI_MAX_INC", constants)


if __name__ == "__main__":
    unittest.main()
