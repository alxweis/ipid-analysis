"""Production storage layout, stable metric keys and explicit selection denominators."""

import hashlib
import unittest
from unittest.mock import patch

from tests.test_paper_values import Fixture

from paper_values.exporter import ValueConflict
from paper_values.review import population_values


class CompactPopulationTest(unittest.TestCase):
    def setUp(self):
        import tempfile

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.f = Fixture(temporary.name)

    def test_real_report_processed_layout_reuses_keys_and_compact_six_vp_blocks(self):
        for site in ("af", "as", "au", "eu", "na", "sa"):
            self.f.baseline(site=site, protocol="icmp")
            self.f.strategy(
                {"UNCLASSIFIED": 3, "NOT_ENOUGH_SAMPLES": 1},
                phase="fi-base",
                site=site,
                protocol="icmp",
            )
            for phase, targets, success in (("fi-mass", 2, 1), ("fi-base", 4, 3)):
                self.f.report(
                    "coverage",
                    dict(zmap_ip_count=targets, ipid_ip_count=success),
                    processed=True,
                    phase=phase,
                    site=site,
                    protocol="icmp",
                )
        sources = {
            p: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.f.root.rglob("*.json")
        }
        result = self.f.run(strict=True)
        values = result["values"]
        expected = {
            "zmap.count": 20,
            "rt-base.targets.count": 20,
            "rt-base.targets.pct": 100,
            "rt-base.success.count": 10,
            "rt-base.success.pct": 50,
            "fi-mass.targets.count": 2,
            "fi-mass.targets.pct": 20,
            "fi-mass.success.count": 1,
            "fi-mass.success.pct": 50,
            "fi-base.targets.count": 4,
            "fi-base.targets.pct": 20,
            "fi-base.success.count": 3,
            "fi-base.success.pct": 75,
        }
        for site in ("af", "as", "au", "eu", "na", "sa"):
            for suffix, n in expected.items():
                self.assertEqual(values[f"results.{site}.icmp.{suffix}"]["raw"], n)
        self.assertFalse(any(".measurements." in key for key in values))
        tex = (self.f.root / "tmp/values.tex").read_text()
        block = "\n".join(
            f"\\setval{{results.{site}.icmp.zmap.count}}{{20}}"
            for site in ("af", "as", "au", "eu", "na", "sa")
        )
        self.assertIn(block, tex)
        self.assertEqual(sources, {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in sources})

    def test_missing_target_evidence_stays_empty_and_zero_is_not_a_percentage(self):
        self.f.strategy({"UNCLASSIFIED": 2}, protocol="icmp")
        result = self.f.run(strict=True)
        prefix = "results.eu.icmp.rt-base."
        self.assertEqual(result["values"][prefix + "success.count"]["raw"], 2)
        self.assertIsNone(result["values"][prefix + "targets.count"]["raw"])
        self.assertIsNone(result["values"][prefix + "success.pct"]["raw"])
        zero = population_values("test", 0, 0, 0, (), "zero cohort")
        self.assertIsNone(zero["test.targets.pct"]["raw"])
        self.assertIsNone(zero["test.success.pct"]["raw"])

    def test_invalid_population_is_rejected(self):
        for args in ((4, 5, 10), (11, 1, 10), (True, 0, 10), (-1, 0, 10)):
            with self.subTest(args=args), self.assertRaises(ValueError):
                population_values("test", *args, (), "source")

    def test_existing_success_key_cannot_be_silently_replaced(self):
        from paper_values.review import record

        self.f.baseline(protocol="icmp")
        with (
            patch(
                "paper_values.review.coverage_values",
                return_value={
                    "results.eu.icmp.rt-base.success.count": record(999, "count", "", ())
                },
            ),
            self.assertRaises(ValueConflict),
        ):
            self.f.run(strict=True)

    def test_tcp_only_has_connection_component_and_retains_full_population(self):
        from paper_values.review import full_coverage_values, record

        self.f.baseline()
        self.f.strategy({"PER_CONNECTION": 2}, mode="connection")
        self.f.report(
            "coverage", dict(zmap_ip_count=5, ipid_ip_count=2), processed=True, mode="connection"
        )
        values = self.f.run(strict=True)["values"]
        self.assertEqual(values["results.eu.tcp.conn.rt-base.targets.pct"]["raw"], 25)
        self.assertEqual(values["results.eu.tcp.conn.rt-base.success.pct"]["raw"], 40)
        values["results.eu.tcp.conn.rt-base.targets.count.full"] = record(
            10, "count", "", ("full",)
        )
        values["results.eu.tcp.conn.rt-base.success.count.full"] = record(
            8, "count", "", ("full",)
        )
        full = full_coverage_values(values)
        self.assertEqual(full["results.eu.tcp.conn.rt-base.targets.pct.full"]["raw"], 50)
        self.assertEqual(full["results.eu.tcp.conn.rt-base.success.pct.full"]["raw"], 80)


if __name__ == "__main__":
    unittest.main()
