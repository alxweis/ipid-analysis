"""Optional Anycast figures retain normal/Full selection and enter the ordinary ZIP."""

import hashlib
import json
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from test_paper_values import RUNS, Fixture
import test_paper_values_followups as followup_fixtures

from paper_values.anycast_figures import KEYS, KIND, load
from paper_values.exporter import export
from paper_values.figure_zip import bundle, figure_paths

CAMPAIGN = followup_fixtures.CAMPAIGN


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


class AnycastFigureExportTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.f = Fixture(temporary.name)
        for name, directory in (
            ("reports", "reports"),
            ("processed", "data/processed"),
            ("raw", "data/raw"),
        ):
            setattr(self.f, name, self.f.root / directory / "continental" / CAMPAIGN)

    def export(self, **kwargs):
        return export(CAMPAIGN, reports_root=self.f.reports, repository_root=self.f.root, **kwargs)

    def normal(self, protocol="icmp", *, site="eu", run=None):
        source = self.f.report(
            KIND,
            {"figure": KIND, "protocol": protocol, "zmap_id": run or RUNS[protocol]},
            phase="comparison",
            protocol=protocol,
            site=site,
            run=run,
            receipt=True,
        )
        pdf = source.with_name(source.stem + "__paper.pdf")
        pdf.write_bytes(b"%PDF-1.4\nnormal")
        self.f.receipt(pdf)
        return source, pdf

    def anycast(self, source, original, *, legacy=False):
        pdf = original.with_name(original.stem + "__anycast.pdf")
        pdf.write_bytes(b"%PDF-1.4\nanycast-excluded")
        marker = self.f.receipt(pdf)
        receipt = json.loads(marker.read_text())
        stat = source.stat()
        bound = dict(path=str(source.resolve()), size=stat.st_size, mtime_ns=stat.st_mtime_ns)
        if not legacy:
            bound["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        receipt.update(
            parameters=dict(
                operation="paper-anycast-plot-v1", census_sha256="a" * 64, profile_sha256="b" * 64
            ),
            sources=[bound, dict(path="/must-not-read/per-ip.pq")],
        )
        marker.write_text(json.dumps(receipt))
        return pdf, marker

    def full(self):
        helper = followup_fixtures.FullFollowupTest()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        helper.root = self.f.root
        helper.reports = self.f.reports
        helper.followups = self.f.root / "reports/followups"
        helper.analysis = (
            helper.followups / helper.spec["measurement_run"] / helper.spec["analysis_id"]
        )
        helper.fixture()
        stem = "c-rt-b_fi-b_" + KIND
        directory = (
            helper.analysis
            / "eu/paired/figures"
            / RUNS["tcp"]
            / "connection/comparison/rt-based-base_fixed-interval-base"
        )
        source = directory / (stem + "__full.json")
        helper.write(source, {"figure": KIND, "protocol": "tcp", "connection_mode": "connection"})
        marker = helper.analysis / "eu/done__full.json"
        done = json.loads(marker.read_text())
        done["artifacts"].append(
            dict(
                path="/mnt/data01/ipid-analysis/reports/"
                + source.relative_to(self.f.root / "reports").as_posix(),
                size_bytes=source.stat().st_size,
                sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
            )
        )
        helper.write(marker, done)
        global_marker = helper.analysis / "done__full.json"
        global_done = json.loads(global_marker.read_text())
        global_done["sites"] = [
            done if item["site"] == "eu" else item for item in global_done["sites"]
        ]
        helper.write(global_marker, global_done)
        original = directory / (stem + "__paper__full.pdf")
        return source, original

    def test_four_separate_keys_and_zip_preserve_existing_values_and_all_inputs(self):
        normal = [self.normal(p) for p in ("icmp", "tcp", "udp")]
        full = self.full()
        before_values = self.export(strict=True)["values"]
        pdfs = [self.anycast(source, original)[0] for source, original in normal]
        pdfs.append(self.anycast(*full)[0])
        before = {p: stamp(p) for p in (self.f.root / "reports").rglob("*") if p.is_file()}
        with (
            patch("subprocess.run", side_effect=AssertionError("must not run analysis")),
            patch(
                "pyarrow.parquet.ParquetFile",
                side_effect=AssertionError("must not read per-IP tables"),
            ),
        ):
            first = self.export(strict=True)
            second = self.export(strict=True)
            bundle(repository_root=self.f.root)
        self.assertEqual(first["values"], second["values"])
        for key, entry in before_values.items():
            self.assertEqual(first["values"][key], entry)
        added = set(first["values"]) - set(before_values)
        expected = {
            key.removesuffix(".full") + ".anycast" + (".full" if key.endswith(".full") else "")
            for key in KEYS
        }
        self.assertEqual(added, expected)
        paths = {first["values"][key]["raw"] for key in added}
        self.assertEqual(paths, {p.relative_to(self.f.root).as_posix() for p in pdfs})
        tex = (self.f.root / "tmp/values.tex").read_text()
        self.assertTrue(paths <= set(figure_paths(tex)[0]))
        with zipfile.ZipFile(self.f.root / "tmp/paper-figures.zip") as archive:
            self.assertTrue(paths <= set(archive.namelist()))
            for path in paths:
                self.assertEqual(archive.read(path), (self.f.root / path).read_bytes())
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_optional_missing_other_vp_noncanonical_and_old_connection_sample_do_not_fallback(
        self,
    ):
        source, original = self.normal()
        # Only this canonical EU figure is eligible; no need for all four PDFs.
        self.anycast(source, original, legacy=True)
        self.anycast(*self.normal(site="na"))
        self.anycast(*self.normal(run="icmp_2025-01-01_00-00-00"))
        self.f.baseline(protocol="icmp")
        values = self.export(strict=True)["values"]
        added = {k for k in values if ".anycast" in k}
        self.assertEqual(added, {KEYS[0] + ".anycast"})
        self.assertEqual(load(values, archive=True), ({}, []))
        sample = dict(values)
        sample[KEYS[-1].removesuffix(".full")] = sample[KEYS[0]]
        self.assertNotIn(KEYS[-1].removesuffix(".full") + ".anycast.full", load(sample)[0])

    def test_corrupt_optional_figure_receipt_or_binding_keeps_previous_strict_outputs(self):
        source, original = self.normal()
        pdf, marker = self.anycast(source, original)
        self.export(strict=True)
        baseline = self.f.output_hashes()
        pristine = json.loads(marker.read_text())
        for mutation in ("operation", "census", "foreign-source", "signature", "changed-source"):
            with self.subTest(mutation=mutation):
                receipt = json.loads(json.dumps(pristine))
                original_source = source.read_bytes()
                if mutation == "operation":
                    receipt["parameters"]["operation"] = "ordinary-paper-plot"
                elif mutation == "census":
                    receipt["parameters"]["census_sha256"] = "invalid"
                elif mutation == "foreign-source":
                    receipt["sources"][0]["path"] = "/another-run/report.json"
                elif mutation == "signature":
                    pdf.write_bytes(b"broken-but-receipted")
                    receipt.update(
                        size_bytes=pdf.stat().st_size,
                        sha256=hashlib.sha256(pdf.read_bytes()).hexdigest(),
                    )
                else:
                    source.write_bytes(original_source + b" ")
                    self.f.receipt(source)
                marker.write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError, "invalid or ambiguous inputs"):
                    self.export(strict=True)
                self.assertEqual(baseline, self.f.output_hashes())
                source.write_bytes(original_source)
                self.f.receipt(source)
                pdf.write_bytes(b"%PDF-1.4\nanycast-excluded")
        marker.unlink()
        with self.assertRaisesRegex(ValueError, "publication receipt"):
            self.export(strict=True)
        self.assertEqual(baseline, self.f.output_hashes())

    def test_legacy_source_freshness_and_orphan_receipt_are_checked(self):
        source, original = self.normal()
        pdf, marker = self.anycast(source, original, legacy=True)
        self.export(strict=True)
        before = self.f.output_hashes()
        receipt = json.loads(marker.read_text())
        receipt["sources"][0]["mtime_ns"] -= 1
        marker.write_text(json.dumps(receipt))
        with self.assertRaisesRegex(ValueError, "source report changed"):
            self.export(strict=True)
        self.assertEqual(before, self.f.output_hashes())
        pdf.unlink()
        with self.assertRaisesRegex(ValueError, "PDF is missing"):
            self.export(strict=True)
        self.assertEqual(before, self.f.output_hashes())

    def test_original_pdf_fallback_still_finds_the_independent_anycast_variant(self):
        source, paper = self.normal()
        pdf, _ = self.anycast(source, paper)
        original = source.with_suffix(".pdf")
        original.write_bytes(paper.read_bytes())
        paper.unlink()
        (paper.parent / ".ipid-complete" / (paper.name + ".json")).unlink()
        values = self.export(strict=True)["values"]
        self.assertEqual(values[KEYS[0]]["raw"], original.relative_to(self.f.root).as_posix())
        self.assertEqual(
            values[KEYS[0] + ".anycast"]["raw"], pdf.relative_to(self.f.root).as_posix()
        )

    def test_full_report_must_remain_in_the_registered_completion_inventory(self):
        source, original = self.full()
        self.anycast(source, original)
        self.export(strict=True)
        before = self.f.output_hashes()
        # Keep global/site markers consistent; only remove the interval source.
        marker = source.parents[6] / "done__full.json"
        done = json.loads(marker.read_text())
        done["artifacts"] = [
            item for item in done["artifacts"] if not item["path"].endswith("/" + source.name)
        ]
        marker.write_text(json.dumps(done))
        self.f.receipt(marker)
        global_marker = source.parents[7] / "done__full.json"
        global_done = json.loads(global_marker.read_text())
        global_done["sites"] = [
            done if item["site"] == "eu" else item for item in global_done["sites"]
        ]
        global_marker.write_text(json.dumps(global_done))
        self.f.receipt(global_marker)
        with self.assertRaisesRegex(ValueError, "pinned Full input"):
            self.export(strict=True)
        self.assertEqual(before, self.f.output_hashes())


if __name__ == "__main__":
    unittest.main()
