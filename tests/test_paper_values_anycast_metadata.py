"""Paper scalars use the same compact, receipted numerical input as each PDF."""

import copy
import hashlib
import json
import unittest
from unittest.mock import patch
import zipfile

import test_paper_values_anycast as fixtures

from paper_values.anycast_metadata import validate
from paper_values.figure_zip import bundle


def document(source):
    from ipid_analysis.anycast_census import period_dates
    from paper_values.anycast_metadata import RULE

    days = period_dates("2026-11", "2026-11-07", "2026-11-22")
    return dict(
        source_report_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        anycast_census=dict(
            period_start=days[0],
            period_end=days[-1],
            dates=days,
            complete_period=True,
            missing_dates=[],
            expected_snapshots=16,
            confidence="high-confidence",
            filter=RULE,
            revision="a" * 40,
            snapshots=[dict(date=day) for day in days],
            manifest_sha256="a" * 64,
            prefix_count=100,
        ),
        anycast_filtering=dict(
            schema_version=2,
            prefix_length=24,
            total=dict(
                addresses=dict(original=4, excluded=2, retained=2),
                prefixes=dict(original=3, excluded=1, retained=2),
            ),
            continents={
                "NA": dict(
                    addresses=dict(original=1, excluded=1, retained=0),
                    prefixes=dict(original=1, excluded=1, retained=0),
                ),
                "EU": dict(
                    addresses=dict(original=2, excluded=1, retained=1),
                    prefixes=dict(original=2, excluded=1, retained=1),
                ),
                "ZZ": dict(
                    addresses=dict(original=1, excluded=0, retained=1),
                    prefixes=dict(original=1, excluded=0, retained=1),
                ),
            },
        ),
    )


class AnycastMetadataExportTest(unittest.TestCase):
    def setUp(self):
        self.helper = fixtures.AnycastFigureExportTest()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)

    def add_metadata(self, source, original):
        pdf, marker = self.helper.anycast(source, original)
        metadata = pdf.with_suffix(".json")
        metadata.write_text(json.dumps(document(source)))
        self.helper.f.receipt(metadata)
        receipt = json.loads(marker.read_text())
        receipt["parameters"]["impact_metadata_version"] = 2
        receipt["sources"].append(
            dict(
                path="/must-not-read/processed/anycast/identity/"
                + source.stem.removesuffix("__full")
                + "__paper__anycast.json",
                size=metadata.stat().st_size,
                sha256=hashlib.sha256(metadata.read_bytes()).hexdigest(),
            )
        )
        marker.write_text(json.dumps(receipt))
        return pdf, marker, metadata

    def rebind_metadata(self, marker, metadata, doc):
        metadata.write_text(json.dumps(doc))
        self.helper.f.receipt(metadata)
        receipt = json.loads(marker.read_text())
        receipt["sources"][-1].update(
            size=metadata.stat().st_size, sha256=hashlib.sha256(metadata.read_bytes()).hexdigest()
        )
        marker.write_text(json.dumps(receipt))

    def test_four_analyses_have_separate_exact_denominators_and_same_census_context(self):
        jobs = [self.helper.normal(protocol) for protocol in ("icmp", "tcp", "udp")]
        jobs.append(self.helper.full())
        baseline = self.helper.export(strict=True)["values"]
        for job in jobs:
            self.add_metadata(*job)
        before = {
            p: fixtures.stamp(p)
            for p in (self.helper.f.root / "reports").rglob("*")
            if p.is_file()
        }
        with (
            patch("subprocess.run", side_effect=AssertionError("no computation")),
            patch("pyarrow.parquet.ParquetFile", side_effect=AssertionError("no per-IP reads")),
        ):
            result = self.helper.export(strict=True)
            bundle(repository_root=self.helper.f.root)
        values = result["values"]
        for key, entry in baseline.items():
            self.assertEqual(values[key], entry)
        for context in ("icmp", "tcp.no-conn", "udp", "tcp.conn"):
            prefix = f"results.eu.{context}.rt-vs-fi-base.anycast"
            full = ".full" if context == "tcp.conn" else ""

            def raw(suffix):
                return values[prefix + "." + suffix + full]["raw"]

            self.assertEqual(raw("addresses.original.count"), 4)
            self.assertEqual(raw("addresses.excluded.count"), 2)
            self.assertEqual(raw("addresses.excluded.pct"), 50)
            self.assertEqual(raw("addresses.retained.count"), 2)
            self.assertEqual(raw("prefixes.original.count"), 3)
            self.assertAlmostEqual(raw("prefixes.excluded.pct"), 100 / 3)
            self.assertEqual(raw("continent.na.addresses.excluded.pct"), 100)
            self.assertEqual(raw("continent.eu.prefixes.original.count"), 2)
            self.assertEqual(raw("unlocated.original.count"), 1)
        self.assertFalse(any(key.startswith("params.anycast.census.") for key in values))
        tex = (self.helper.f.root / "tmp/values.tex").read_text()
        self.assertNotIn("params.anycast.census.", tex)
        self.assertEqual(values["results.anycast.census.prefixes.count"]["raw"], 100)
        with zipfile.ZipFile(self.helper.f.root / "tmp/paper-figures.zip") as archive:
            self.assertEqual(sum(name.endswith("__anycast.pdf") for name in archive.namelist()), 4)
        self.assertEqual(before, {p: fixtures.stamp(p) for p in before})

    def test_invalid_counts_missing_days_or_hash_mismatch_preserve_old_exports(self):
        source, original = self.helper.normal()
        _, marker, metadata = self.add_metadata(source, original)
        self.helper.export(strict=True)
        before = self.helper.f.output_hashes()
        original_doc = json.loads(metadata.read_text())
        for mutation in ("counts", "days", "prefix-sum", "missing-file", "hash"):
            with self.subTest(mutation=mutation):
                doc = copy.deepcopy(original_doc)
                if mutation == "counts":
                    doc["anycast_filtering"]["total"]["addresses"]["excluded"] = 3
                elif mutation == "days":
                    doc["anycast_census"]["dates"].pop()
                elif mutation == "prefix-sum":
                    # Prefix cardinality must be globally computed, but can never
                    # exceed the sum of distinct per-continent cardinalities.
                    stats = doc["anycast_filtering"]
                    for row in (stats["total"], *stats["continents"].values()):
                        row["addresses"] = {key: n * 10 for key, n in row["addresses"].items()}
                    stats["total"]["prefixes"] = dict(original=5, excluded=2, retained=3)
                self.rebind_metadata(marker, metadata, doc)
                if mutation == "hash":
                    metadata.write_text(metadata.read_text() + " ")
                    self.helper.f.receipt(metadata)
                elif mutation == "missing-file":
                    metadata.unlink()
                with self.assertRaises(ValueError):
                    self.helper.export(strict=True)
                self.assertEqual(before, self.helper.f.output_hashes())
        self.rebind_metadata(marker, metadata, original_doc)
        self.assertEqual(
            self.helper.export(strict=True)["values"][
                "results.eu.icmp.rt-vs-fi-base.anycast.prefixes.excluded.count"
            ]["raw"],
            1,
        )

    def test_mixed_old_new_census_masks_are_not_exported_as_one_comparison(self):
        self.add_metadata(*self.helper.normal("icmp"))
        _, marker = self.helper.anycast(*self.helper.normal("udp"))
        receipt = json.loads(marker.read_text())
        receipt["parameters"]["census_sha256"] = "b" * 64
        marker.write_text(json.dumps(receipt))
        with self.assertRaisesRegex(ValueError, "different Census periods"):
            self.helper.export(strict=True)

    def test_validator_rejects_invented_prefix_counts_and_incomplete_periods(self):
        source, _ = self.helper.normal()
        doc = document(source)
        receipt = dict(parameters=dict(census_sha256="a" * 64))
        self.assertEqual(validate(doc, receipt)[1]["total"]["prefixes"]["original"], 3)
        for mutation in ("negative", "capacity", "period", "census"):
            broken = copy.deepcopy(doc)
            if mutation == "negative":
                broken["anycast_filtering"]["continents"]["NA"]["addresses"]["retained"] = -1
            elif mutation == "capacity":
                broken["anycast_filtering"]["total"]["prefixes"]["original"] = 20
            elif mutation == "period":
                broken["anycast_census"]["complete_period"] = False
            else:
                broken["anycast_census"]["prefix_count"] = 0
            with self.assertRaises(ValueError):
                validate(broken, receipt)


if __name__ == "__main__":
    unittest.main()
