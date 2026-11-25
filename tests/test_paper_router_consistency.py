"""Export T-evidenced node agreement without opening bulk measurement data."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests import test_paper_values_followups
from tests.test_paper_values import Fixture

from paper_values.inputs import SITES
from paper_values.itdk_consistency import router_strict_agreement


def summary(eligible=4, strict=3):
    return dict(
        eligible_nodes_with_two_classified_interfaces=10,
        strict_agreement_nodes=9,
        strict_agreement_percentage=90,
        transit_evidenced_eligible_nodes=eligible,
        transit_evidenced_strict_nodes=strict,
        transit_evidenced_strict_percentage=100 * strict / eligible if eligible else 0,
    )


def hashes(root):
    return {
        str(path): (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in root.rglob("*")
        if path.is_file() and "tmp" not in path.relative_to(root).parts
    }


class RouterConsistencyTest(unittest.TestCase):
    def full_fixture(self):
        fixture = test_paper_values_followups.FullFollowupTest()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.fixture()
        return fixture

    def add_full_summary(self, fixture, site, data):
        zid = fixture.spec["zmap_ids"][site]
        relative = f"full/figures/{zid}/connection/rt-based-base/c-rt-b_itdk-consistency-summary__full.json"
        path = fixture.analysis / site / relative
        marker = fixture.analysis / site / "done__full.json"
        done = json.loads(marker.read_text())
        fixture.write(
            path,
            dict(data, zmap_id=zid, measurement_id=done["full_measurement"]["measurement_id"]),
        )
        name = (
            "/mnt/data01/ipid-analysis/reports/"
            + path.relative_to(fixture.root / "reports").as_posix()
        )
        done["artifacts"] = [item for item in done["artifacts"] if item["path"] != name]
        done["artifacts"].append(
            dict(
                path=name,
                size_bytes=path.stat().st_size,
                sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            )
        )
        fixture.write(marker, done)
        global_marker = fixture.analysis / "done__full.json"
        global_done = json.loads(global_marker.read_text())
        global_done["sites"] = [done if s["site"] == site else s for s in global_done["sites"]]
        fixture.write(global_marker, global_done)
        return path

    def test_router_subset_and_zero_denominators_do_not_use_all_node_percentage(self):
        self.assertEqual(router_strict_agreement(summary()), 75)
        self.assertEqual(router_strict_agreement(summary(strict=0)), 0)
        self.assertIsNone(router_strict_agreement(summary(0, 0)))
        self.assertIsNone(router_strict_agreement(dict(strict_agreement_percentage=99.9)))

    def test_incomplete_invalid_or_inconsistent_summary_is_rejected(self):
        for change in (
            dict(transit_evidenced_strict_nodes=5),
            dict(transit_evidenced_eligible_nodes=-1),
            dict(transit_evidenced_eligible_nodes=True),
            dict(transit_evidenced_strict_nodes=3.0),
            dict(eligible_nodes_with_two_classified_interfaces=2),
            dict(strict_agreement_nodes=2),
            dict(transit_evidenced_strict_percentage=90),
            dict(transit_evidenced_strict_percentage=float("nan")),
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                router_strict_agreement(dict(summary(), **change))
        with self.assertRaisesRegex(ValueError, "incomplete"):
            router_strict_agreement(dict(transit_evidenced_eligible_nodes=4))

    def test_normal_export_reads_all_six_vps_and_three_protocols_without_bulk_io(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory))
            for site in SITES:
                for protocol in ("icmp", "tcp", "udp"):
                    fixture.report(
                        "itdk-consistency-summary",
                        summary(),
                        site=site,
                        protocol=protocol,
                        phase="rt-fi",
                    )
            before = hashes(fixture.root)
            with (
                patch("duckdb.connect", side_effect=AssertionError("no bulk reads")),
                patch("subprocess.run", side_effect=AssertionError("no analysis")),
            ):
                first = fixture.run(strict=True)
                second = fixture.run(strict=True)
            self.assertEqual(first["values"], second["values"])
            self.assertEqual(first["input_errors"], [])
            for site in SITES:
                for protocol in ("icmp", "tcp.no-conn", "udp"):
                    key = f"results.{site}.{protocol}.rt-fi.itdk.router-strict-agreement.pct"
                    self.assertEqual(first["values"][key]["raw"], 75)
                    self.assertEqual(len(first["values"][key]["sources"]), 1)
                self.assertEqual(
                    first["values"][f"results.{site}.icmp.rt-fi.itdk.strict-agreement.pct"]["raw"],
                    90,
                )
            self.assertEqual(before, hashes(fixture.root))

    def test_invalid_normal_summary_preserves_previous_exports(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory))
            path = fixture.report(
                "itdk-consistency-summary", summary(), protocol="icmp", phase="rt-fi"
            )
            fixture.run(strict=True)
            before = fixture.output_hashes()
            path.write_text(json.dumps(dict(summary(), transit_evidenced_strict_nodes=5)))
            with self.assertRaisesRegex(ValueError, "invalid router ITDK"):
                fixture.run(strict=True)
            self.assertEqual(before, fixture.output_hashes())

    def test_full_export_uses_pinned_summaries_preserves_general_rate_and_all_inputs(self):
        fixture = self.full_fixture()
        for site in SITES:
            self.add_full_summary(fixture, site, summary())
        before = hashes(fixture.root)
        with (
            patch("duckdb.connect", side_effect=AssertionError("no raw/per-IP reads")),
            patch("subprocess.run", side_effect=AssertionError("no analysis")),
        ):
            result = fixture.run_export(strict=True)
        self.assertEqual(result["input_errors"], [])
        for site in SITES:
            prefix = f"results.{site}.tcp.conn.rt-base.itdk."
            self.assertEqual(
                result["values"][prefix + "router-strict-agreement.pct.full"]["raw"], 75
            )
            self.assertEqual(result["values"][prefix + "strict-agreement.pct.full"]["raw"], 90)
            self.assertNotIn(prefix + "router-strict-agreement.pct", result["values"])
        self.assertEqual(before, hashes(fixture.root))

    def test_missing_or_zero_router_population_does_not_fall_back_to_general_or_other_vp(self):
        fixture = self.full_fixture()
        self.add_full_summary(fixture, "eu", summary(0, 0))
        self.add_full_summary(fixture, "na", summary())
        self.add_full_summary(
            fixture,
            "au",
            dict(eligible_nodes_with_two_classified_interfaces=10, strict_agreement_nodes=9),
        )
        result = fixture.run_export(strict=True)
        for site in ("eu", "au", "af", "as", "sa"):
            key = f"results.{site}.tcp.conn.rt-base.itdk.router-strict-agreement.pct.full"
            self.assertIsNone(result["values"][key]["raw"])
        self.assertEqual(
            result["values"]["results.na.tcp.conn.rt-base.itdk.router-strict-agreement.pct.full"][
                "raw"
            ],
            75,
        )

    def test_invalid_pinned_full_router_counts_fail_strict_export_without_output_writes(self):
        fixture = self.full_fixture()
        self.add_full_summary(fixture, "eu", summary())
        fixture.run_export(strict=True)
        outputs = {
            name: (fixture.root / "tmp" / name).read_bytes()
            for name in ("values.tex", "values.json")
        }
        self.add_full_summary(fixture, "eu", dict(summary(), transit_evidenced_strict_nodes=5))
        with self.assertRaisesRegex(ValueError, "invalid router ITDK"):
            fixture.run_export(strict=True)
        for name, data in outputs.items():
            self.assertEqual((fixture.root / "tmp" / name).read_bytes(), data)


if __name__ == "__main__":
    unittest.main()
