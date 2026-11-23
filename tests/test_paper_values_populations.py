"""Production storage layout, stable metric keys and explicit selection denominators."""

import hashlib
import json
import unittest
from unittest.mock import patch

from tests.test_paper_values import Fixture

from paper_values.exporter import ValueConflict
from paper_values.review import population_values
from paper_values.tex import key_order, render


class CompactPopulationTest(unittest.TestCase):
    def setUp(self):
        import tempfile

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.f = Fixture(temporary.name)

    def test_comparison_statistics_form_one_block_and_unrelated_strategies_stay_separate(self):
        values = {}
        prefixes = [
            f"results.vp-comparison.{family}.{strategy}"
            for family in ("icmp.rt-fi", "tcp.no-conn.rt-fi", "tcp.conn.rt-base")
            for strategy in ("constant", "single")
        ]
        for prefix in prefixes:
            for suffix, n, unit in (
                ("min.pct", 1, "percent"),
                ("max.pct", 3, "percent"),
                ("range.pp", 2, "percentage-points"),
            ):
                values[prefix + "." + suffix] = dict(raw=n, unit=unit)
        before = {k: dict(v) for k, v in values.items()}
        text = render(dict(reversed(list(values.items()))), "test")
        blocks = [
            block for block in text.split("\n\n") if "\\setval{results.vp-comparison." in block
        ]
        self.assertEqual(len(blocks), len(prefixes))
        for prefix in prefixes:
            lines = [
                line for line in text.splitlines() if line.startswith("\\setval{" + prefix + ".")
            ]
            self.assertEqual(len(lines), 3)
            self.assertIn("\n".join(lines), text)
        self.assertEqual(values, before)
        self.assertEqual(
            [
                line.split("}", 1)[0].removeprefix("\\setval{")
                for line in text.splitlines()
                if line.startswith("\\setval{results.")
            ],
            sorted(values, key=key_order),
        )

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

    def test_merged_coverage_keeps_original_targets_and_counts_each_ip_once(self):
        for site in ("af", "as", "au", "eu", "na", "sa"):
            for protocol in ("icmp", "tcp", "udp"):
                self.f.baseline(site=site, protocol=protocol)
                # Same ten RT responders: eight retained, one successful
                # refinement, one failed refinement. UNCLASSIFIED is a success.
                self.f.strategy(
                    {"CONSTANT": 6, "SINGLE": 1, "UNCLASSIFIED": 2, "NOT_ENOUGH_SAMPLES": 1},
                    phase="rt-fi",
                    site=site,
                    protocol=protocol,
                )
        sources = {
            p: (p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
            for p in self.f.root.rglob("*.json")
        }
        values = self.f.run(strict=True)["values"]
        for site in ("af", "as", "au", "eu", "na", "sa"):
            for family in ("icmp", "udp", "tcp.no-conn"):
                prefix = f"results.{site}.{family}.rt-fi"
                for suffix, expected in (
                    ("targets.count", 20),
                    ("targets.pct", 100),
                    ("success.count", 9),
                    ("success.pct", 45),
                ):
                    self.assertEqual(values[prefix + "." + suffix]["raw"], expected, prefix)
                self.assertEqual(len(values[prefix + ".targets.count"]["sources"]), 2)
            self.assertFalse(any(k.startswith(f"results.{site}.tcp.conn.rt-fi.") for k in values))
        self.assertEqual(
            sources,
            {
                p: (p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
                for p in sources
            },
        )

    def test_merge_requires_its_report_and_does_not_infer_missing_target_evidence(self):
        self.f.strategy(
            {"UNCLASSIFIED": 2, "NOT_ENOUGH_SAMPLES": 1}, phase="rt-fi", protocol="icmp"
        )
        values = self.f.run(strict=True)["values"]
        prefix = "results.eu.icmp.rt-fi."
        self.assertEqual(values[prefix + "success.count"]["raw"], 2)
        for suffix in ("targets.count", "targets.pct", "success.pct"):
            self.assertIsNone(values[prefix + suffix]["raw"])
        self.f.report(
            "coverage", dict(zmap_ip_count=20, ipid_ip_count=10), processed=True, protocol="udp"
        )
        values = self.f.run(strict=True)["values"]
        self.assertNotIn("results.eu.udp.rt-fi.targets.count", values)

    def test_requested_semantic_blocks_preserve_values_and_six_vp_order(self):
        values = {}
        blocks = [
            (
                "results.eu.interprotocol.per-bucket",
                (
                    "icmp-tcp.targets.count",
                    "icmp-tcp.outcome.fully-protocol-shared.pct",
                    "icmp-tcp-udp.partial.shared-icmp-tcp.pct",
                ),
            ),
            (
                "results.eu.interprotocol.single",
                ("icmp-tcp.targets.count", "icmp-udp.targets.count"),
            ),
            (
                "results.eu.tcp.conn.rt-base.constant",
                (
                    "df0.pct",
                    "df0.pct.full",
                    "df1.pct",
                    "df1.pct.full",
                    "ipid-zero.pct",
                    "ipid-zero.pct.full",
                ),
            ),
            (
                "results.eu.tcp.conn.rt-base.per-connection.handshake",
                ("constant.pct", "constant.pct.full"),
            ),
            ("results.eu.tcp.no-conn.rt-fi.constant", ("df0.pct", "df1.pct", "ipid-zero.pct")),
            (
                "fig.eu.interprotocol",
                ("per-bucket.upset", "per-destination.upset", "single.upset"),
            ),
        ]
        for prefix, suffixes in blocks:
            for suffix in suffixes:
                values[prefix + "." + suffix] = dict(raw=12, unit="percent")
        for site in ("af", "as", "au", "na", "sa"):
            values[f"results.{site}.tcp.conn.rt-base.constant.df0.pct.full"] = dict(
                raw=12, unit="percent"
            )
        before = {k: dict(v) for k, v in values.items()}
        text = render(dict(reversed(list(values.items()))), "test")
        all_lines = text.splitlines()
        for prefix, _ in blocks:
            positions = [
                i
                for i, line in enumerate(all_lines)
                if line.startswith("\\setval{" + prefix + ".")
            ]
            # Other VPs may be interleaved, but no blank lines within the family.
            self.assertTrue(positions)
            self.assertTrue(
                all(all_lines[i] for i in range(min(positions), max(positions) + 1)), prefix
            )
        block = "\n".join(
            f"\\setval{{results.{site}.tcp.conn.rt-base.constant.df0.pct.full}}{{12.00\\%}}"
            for site in ("af", "as", "au", "eu", "na", "sa")
        )
        self.assertIn(block, text)
        bucket = [
            i
            for i, line in enumerate(all_lines)
            if line.startswith("\\setval{results.eu.interprotocol.per-bucket.")
        ]
        single = [
            i
            for i, line in enumerate(all_lines)
            if line.startswith("\\setval{results.eu.interprotocol.single.")
        ]
        self.assertEqual(all_lines[max(bucket) + 1], "")
        self.assertGreater(min(single), max(bucket))
        self.assertEqual(values, before)

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
        from tests.test_paper_values import RUNS

        metadata = self.f.raw / "eu/zmap" / RUNS["tcp"] / "zmap-connection-sample.json"
        metadata.parent.mkdir(parents=True)
        metadata.write_text(
            json.dumps(
                dict(
                    version=1,
                    zmap_id=RUNS["tcp"],
                    reply_type="synack",
                    source_rows=20,
                    eligible_rows=10,
                    sample_rows=5,
                )
            )
        )
        values = self.f.run(strict=True)["values"]
        self.assertEqual(values["results.eu.tcp.conn.rt-base.targets.pct"]["raw"], 50)
        self.assertEqual(values["results.eu.tcp.conn.rt-base.success.pct"]["raw"], 40)
        values["results.eu.tcp.conn.rt-base.targets.count.full"] = record(
            10, "count", "", ("full",)
        )
        values["results.eu.tcp.conn.rt-base.success.count.full"] = record(
            8, "count", "", ("full",)
        )
        full = full_coverage_values(values)
        self.assertEqual(full["results.eu.tcp.conn.rt-base.targets.pct.full"]["raw"], 100)
        self.assertEqual(full["results.eu.tcp.conn.rt-base.success.pct.full"]["raw"], 80)


if __name__ == "__main__":
    unittest.main()
