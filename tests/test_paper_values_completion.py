"""Matched scientific populations and the current synthetic-validation export."""

from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
from test_paper_values import RUNS, Fixture

from paper_values.exporter import load_metrics


class CompletionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.f = Fixture(self.temp.name)

    def labels(
        self,
        rows,
        *,
        protocol="icmp",
        mode="no-connection",
        site="eu",
        phase="rt-base",
        version="9",
    ):
        report = self.f.strategy(
            dict(Counter(s for _, s in rows)), protocol=protocol, mode=mode, site=site, phase=phase
        )
        rel = report.relative_to(self.f.reports / site / "figures")
        path = self.f.processed / site / rel.with_suffix(".pq")
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.table(
            {"IP_ADDR": [ip for ip, _ in rows], "IPID_SELECTION_STRATEGY": [s for _, s in rows]}
        )
        pq.write_table(
            table.replace_schema_metadata({b"classifier_version": version.encode()}), path
        )
        return path, report

    def fi_pair(self, *, protocol="icmp", mode="no-connection", site="eu"):
        r = [
            ("1", "CONSTANT"),
            ("2", "UNCLASSIFIED"),
            ("3", "NOT_ENOUGH_SAMPLES"),
            ("4", "SINGLE"),
            ("5", "CONSTANT"),
            ("6", "CONSTANT"),
            ("7", "CONSTANT"),
        ]
        f = [("1", "CONSTANT"), ("3", "UNCLASSIFIED"), ("4", "NOT_ENOUGH_SAMPLES")]
        rt, _ = self.labels(r, protocol=protocol, mode=mode, site=site)
        fi, _ = self.labels(f, protocol=protocol, mode=mode, site=site, phase="fi-base")
        self.f.report(
            "coverage",
            {"zmap_ip_count": 4, "ipid_ip_count": 2},
            protocol=protocol,
            mode=mode,
            site=site,
            phase="fi-base",
            processed=True,
        )
        stem = "zmap-connection-sample" if mode == "connection" else "zmap-fixed-base-sample"
        target = self.f.raw / site / "zmap" / RUNS[protocol] / (stem + ".pq")
        target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"IP_ADDR": ["1", "2", "3", "4"]}), target)
        meta = target.with_suffix(".json")
        meta.write_text(
            json.dumps(
                {
                    "version": 1,
                    "zmap_id": RUNS[protocol],
                    "sample_rows": 4,
                    "reply_type": "synack" if mode == "connection" else "",
                }
            )
        )
        return [rt, fi, target, meta]

    def test_no_selected_recipe_is_an_unimplemented_placeholder(self):
        self.assertFalse(
            any(s["reader"] == "missing" for m in load_metrics()["metrics"] for s in m["sources"])
        )

    def test_four_ratios_use_the_matched_target_cohort_for_all_six_vps(self):
        for site in ("af", "as", "au", "eu", "na", "sa"):
            for proto, mode in (
                ("icmp", "no-connection"),
                ("udp", "no-connection"),
                ("tcp", "no-connection"),
                ("tcp", "connection"),
            ):
                self.fi_pair(protocol=proto, mode=mode, site=site)
        result = self.f.run(strict=True)
        for key, value in result["values"].items():
            if key.endswith(".fi-to-rt-success.pct"):
                # R={1,2,4}, F={1,3}; not the intersection and not all seven RT rows.
                self.assertAlmostEqual(value["raw"], 200 / 3, msg=key)
        self.assertEqual(result["cache"]["fi-rt_misses"], 24)

    def test_paired_cache_skips_join_and_invalidates_on_each_input(self):
        paths = self.fi_pair()
        first = self.f.run(strict=True)
        with patch("paper_values.paired.fi_rt", side_effect=AssertionError("must use cache")):
            warm = self.f.run(strict=True)
        self.assertEqual(warm["cache"]["fi-rt_hits"], 1)
        self.assertEqual(first["values"], warm["values"])
        for path in paths:
            stat = path.stat()
            os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
            changed = self.f.run(strict=True)
            self.assertEqual(changed["cache"]["fi-rt_misses"], 1)
            self.assertEqual(first["values"], changed["values"])
        self.assertEqual(self.f.run(strict=True, recompute=True)["cache"]["fi-rt_misses"], 1)

    def test_missing_cohort_is_empty_and_not_cached_as_a_result(self):
        paths = self.fi_pair()
        paths[-1].unlink()
        result = self.f.run(strict=True)
        self.assertIsNone(result["values"]["results.eu.icmp.fi-to-rt-success.pct"]["raw"])
        self.assertFalse(result["cache"])

    def test_export_preserves_all_analysis_inputs_and_writes_only_the_two_outputs(self):
        self.fi_pair()
        self.validation()

        def fingerprints():
            return {
                str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                for directory in (self.f.root / "reports", self.f.root / "data")
                for path in directory.rglob("*")
                if path.is_file()
            }

        before = fingerprints()
        self.f.run(strict=True)
        self.assertEqual(before, fingerprints())
        self.assertEqual(
            {p.name for p in (self.f.root / "tmp").iterdir()}, {"values.tex", "values.json"}
        )

    def test_duplicate_or_outside_target_addresses_are_rejected(self):
        paths = self.fi_pair()
        self.f.run(strict=True)
        previous = self.f.output_hashes()
        pq.write_table(pa.table({"IP_ADDR": ["1", "1", "3", "4"]}), paths[2])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.f.run(strict=True)
        self.assertEqual(previous, self.f.output_hashes())
        pq.write_table(pa.table({"IP_ADDR": ["1", "2", "8", "4"]}), paths[2])
        with self.assertRaisesRegex(ValueError, "outside"):
            self.f.run(strict=True)

    def test_wrong_sample_identity_or_classifier_version_is_rejected(self):
        paths = self.fi_pair()
        meta = json.loads(paths[-1].read_text())
        meta["zmap_id"] = "another-discovery"
        paths[-1].write_text(json.dumps(meta))
        with self.assertRaisesRegex(ValueError, "metadata"):
            self.f.run(strict=True)
        meta["zmap_id"] = RUNS["icmp"]
        paths[-1].write_text(json.dumps(meta))
        table = pq.read_table(paths[0]).replace_schema_metadata({b"classifier_version": b"8"})
        pq.write_table(table, paths[0])
        with self.assertRaisesRegex(ValueError, "classifier version"):
            self.f.run(strict=True)

    def test_ratio_may_exceed_one_hundred_and_zero_denominator_stays_empty(self):
        self.fi_pair()
        # Three FI successes versus two matched RT successes; outside RT rows do not count.
        self.labels(
            [
                ("1", "CONSTANT"),
                ("2", "SINGLE"),
                ("3", "NOT_ENOUGH_SAMPLES"),
                ("4", "NOT_ENOUGH_SAMPLES"),
                ("5", "CONSTANT"),
            ]
        )
        self.labels([("1", "CONSTANT"), ("2", "SINGLE"), ("3", "UNCLASSIFIED")], phase="fi-base")
        self.f.report(
            "coverage",
            {"zmap_ip_count": 4, "ipid_ip_count": 3},
            phase="fi-base",
            protocol="icmp",
            processed=True,
        )
        key = "results.eu.icmp.fi-to-rt-success.pct"
        self.assertEqual(self.f.run(strict=True)["values"][key]["raw"], 150)
        self.labels([("1", "NOT_ENOUGH_SAMPLES")])
        self.assertIsNone(self.f.run(strict=True)["values"][key]["raw"])

    def test_tcp_transition_uses_the_common_successful_population_and_column_denominator(self):
        self.labels(
            [
                ("1", "CONSTANT"),
                ("2", "CONSTANT"),
                ("3", "SINGLE"),
                ("4", "NOT_ENOUGH_SAMPLES"),
                ("5", "CONSTANT"),
                ("6", "CONSTANT"),
                ("7", "UNCLASSIFIED"),
            ],
            protocol="tcp",
        )
        self.labels(
            [
                ("1", "PER_CONNECTION"),
                ("2", "CONSTANT"),
                ("3", "PER_CONNECTION"),
                ("4", "PER_CONNECTION"),
                ("6", "NOT_ENOUGH_SAMPLES"),
                ("7", "PER_CONNECTION"),
                ("8", "PER_CONNECTION"),
            ],
            protocol="tcp",
            mode="connection",
        )
        key = "results.eu.tcp.rt-no-conn-to-conn.constant.per-connection.column-pct"
        result = self.f.run(strict=True)
        self.assertAlmostEqual(result["values"][key]["raw"], 100 / 3)
        with patch(
            "paper_values.paired.tcp_transition", side_effect=AssertionError("must use cache")
        ):
            self.assertEqual(self.f.run(strict=True)["cache"]["tcp-transition_hits"], 1)
        self.labels([("1", "SINGLE")], protocol="tcp")
        self.assertEqual(self.f.run(strict=True)["values"][key]["raw"], 0)
        self.labels([("1", "CONSTANT")], protocol="tcp", mode="connection")
        self.assertIsNone(self.f.run(strict=True)["values"][key]["raw"])

    def validation(self):
        directory = self.f.root / "reports/figures/classifier-validation"
        directory.mkdir(parents=True)
        report = {"schema_version": 1, "production_profile": True, "datasets": {}, "figures": {}}
        code = Path(__file__).resolve().parents[1] / "ipid_analysis"
        report["classifier_sources"] = {
            name: hashlib.sha256((code / name).read_bytes()).hexdigest()
            for name in (
                "strategies.py",
                "random_classifier_candidate.py",
                "increment_uniformity.py",
                "empirical_random_uniformity.py",
                "classifier_validation.py",
            )
        }
        for name, accuracy in (
            ("base-4x4-ideal", 0.75),
            ("mass-4x25-ideal", 0.99),
            ("mass-4x25-lossy", 0.8),
            ("mass-4x25-reordered", 0.7),
            ("mass-4x25-lossy-reordered", 0.01),
        ):
            report["datasets"][name] = {
                "sequence_length": 16 if name.startswith("base") else 100,
                "loss_pct": 20,
                "reordering_pct": 20,
                "metrics": {
                    "accuracy": accuracy,
                    "classes": {
                        "MULTI": {"support": 10, "correct": 9},
                        "RANDOM": {"support": 10, "correct": 6},
                    },
                },
            }
        for key, filename in (
            ("mass-ideal", "mass-4x25-classifier-confusion-ideal.pdf"),
            ("mass-lossy-vs-reordered", "mass-4x25-classifier-confusion-lossy-vs-reordered.pdf"),
        ):
            pdf = directory / filename
            pdf.write_bytes(b"%PDF-1.4\nfixture")
            report["figures"][key] = {
                "filename": filename,
                "size_bytes": pdf.stat().st_size,
                "sha256": hashlib.sha256(pdf.read_bytes()).hexdigest(),
            }
        path = directory / "classifier-validation-summary.json"
        path.write_text(json.dumps(report))
        return path, report

    def test_tcp_duplicate_labels_cannot_be_hidden_by_reply_filtering(self):
        self.labels([("1", "CONSTANT"), ("1", "NOT_ENOUGH_SAMPLES")], protocol="tcp")
        self.labels([("1", "PER_CONNECTION")], protocol="tcp", mode="connection")
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.f.run(strict=True)

    def test_validation_reads_all_ten_metrics_and_two_matching_pdfs(self):
        self.validation()
        values = self.f.run(strict=True)["values"]
        expected = {
            "validation.data.fi.sequence-length": 100,
            "validation.data.rt.sequence-length": 16,
            "validation.data.loss.pct": 20,
            "validation.data.reordering.pct": 20,
            "validation.fi-ideal.accuracy-pct": 99,
            "validation.fi-ideal.truth.multi.misclassified.pct": 10,
            "validation.fi-lossy.accuracy-pct": 80,
            "validation.fi-reordered.accuracy-pct": 70,
            "validation.lossy-reordered.multi-random.minimum-recall.pct": 60,
            "validation.rt-ideal.accuracy-pct": 75,
        }
        for key, raw in expected.items():
            self.assertEqual(values[key]["raw"], raw)
        for key in (
            "fig.validation.fi-ideal.confusion-matrix",
            "fig.validation.fi-lossy-reordered.confusion-matrix",
        ):
            self.assertTrue(
                values[key]["raw"].startswith("reports/figures/classifier-validation/")
            )

    def test_unmatched_profile_code_or_pdf_does_not_replace_existing_outputs(self):
        path, report = self.validation()
        self.f.run(strict=True)
        previous = self.f.output_hashes()
        report["production_profile"] = False
        path.write_text(json.dumps(report))
        with self.assertRaisesRegex(ValueError, "production-profile"):
            self.f.run(strict=True)
        report["production_profile"] = True
        report["classifier_sources"]["strategies.py"] = "0" * 64
        path.write_text(json.dumps(report))
        with self.assertRaisesRegex(ValueError, "different classifier"):
            self.f.run(strict=True)
        path, report = self.validation_overwrite(path, report)
        (path.parent / report["figures"]["mass-ideal"]["filename"]).write_bytes(
            b"%PDF-1.4\ndifferent"
        )
        with self.assertRaisesRegex(ValueError, "PDF differs"):
            self.f.run(strict=True)
        self.assertEqual(previous, self.f.output_hashes())

    def validation_overwrite(self, path, report):
        code = Path(__file__).resolve().parents[1] / "ipid_analysis/strategies.py"
        report["classifier_sources"]["strategies.py"] = hashlib.sha256(
            code.read_bytes()
        ).hexdigest()
        path.write_text(json.dumps(report))
        return path, report

    def test_archive_does_not_read_implicit_global_validation_results(self):
        self.validation()
        archive = self.f.root / "empty.zip"
        with zipfile.ZipFile(archive, "w"):
            pass
        self.assertEqual(self.f.run(strict=True, archive=archive)["available"], 0)
        result = self.f.run(
            strict=True,
            archive=archive,
            validation_root=self.f.root / "reports/figures/classifier-validation",
        )
        self.assertEqual(result["available"], 12)
