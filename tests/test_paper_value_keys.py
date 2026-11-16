"""Public key scope, population distinctions, grouping and serialization contract."""

from pathlib import Path
import tempfile
import unittest

from paper_values.exporter import Registry, emit
from paper_values.keys import export_values, key_order, public_key


class PaperKeyTest(unittest.TestCase):
    def test_target_continent_is_not_mistaken_for_vp_scope(self):
        one = "stats.eu.tcp.no-conn.rt-base.geo.as.count"
        two = "stats.af.tcp.no-conn.rt-base.geo.as.count"
        other = "stats.eu.tcp.no-conn.rt-base.geo.af.count"
        self.assertEqual(key_order(one)[1], key_order(two)[1])
        self.assertNotEqual(key_order(one)[1], key_order(other)[1])
        self.assertEqual(key_order(one)[2], ("eu",))

    def test_short_names_preserve_populations_and_count(self):
        old = "results.vp-eu.tcp.no-connection.rt-fi-merged.successful-results.total.count"
        self.assertEqual(public_key(old), "results.eu.tcp.no-conn.rt-fi.success.count")
        self.assertNotEqual(
            public_key(old), public_key(old.replace("successful-results", "rt-results"))
        )
        old = "claims.vp-af.icmp.no-connection.rt-fi-merged.itdk-os.transit-observed.windows.share-pct"
        self.assertEqual(
            public_key(old), "claims.af.icmp.no-conn.rt-fi.itdk-os.transit-observed.windows.pct"
        )
        self.assertEqual(public_key(public_key(old)), public_key(old))

    def test_shared_settings_require_agreement_not_complete_vp_results(self):
        r = Registry()
        for vp in ("af", "eu", "na"):
            r.put(f"classifier.vp-{vp}.icmp.random.threshold", None if vp == "af" else 0.125)
        values = export_values(r.values)
        self.assertEqual(list(values), ["classifier.random.threshold"])
        self.assertEqual(values["classifier.random.threshold"].raw, 0.125)
        self.assertEqual(r.values["classifier.vp-af.icmp.random.threshold"].status, "missing")
        r.put("classifier.vp-eu.tcp.random.threshold", 0.5)
        with self.assertRaisesRegex(ValueError, "conflicting shared"):
            export_values(r.values)

    def test_common_params_keep_phase_and_site_specific_network_properties(self):
        self.assertEqual(
            public_key(
                "parameters.vp-eu.icmp.no-connection.rt-base.configured-sequence-length.count"
            ),
            "params.rt-base.sequence-length.count",
        )
        self.assertEqual(
            public_key(
                "parameters.vp-na.udp.no-connection.fi-mass.configured-sequence-length.count"
            ),
            "params.fi-mass.sequence-length.count",
        )
        self.assertNotEqual(
            public_key("parameters.vp-eu.source-addresses.common-prefix-length"),
            public_key("parameters.vp-na.source-addresses.common-prefix-length"),
        )

    def test_identical_measured_values_do_not_merge_vps(self):
        r = Registry()
        for vp in ("eu", "na"):
            r.put(f"claims.vp-{vp}.tcp.no-connection.rt-fi-merged.os.matched.count", 42)
        self.assertEqual(len(export_values(r.values)), 2)

    def test_cohort_denominators_and_relative_changes_remain_explicit(self):
        keys = [
            "constant.vp-eu.tcp.connection.rt-base.zero.share-pct",
            "constant.vp-eu.tcp.no-connection.rt-fi-merged.zero.share-pct",
            "tcp-transition.vp-eu.rt-no-connection-to-rt-connection.constant.per-connection.column-share-pct",
        ]
        names = [public_key(k) for k in keys]
        self.assertEqual(len(set(names)), 3)
        self.assertIn("column-pct", names[-1])

    def test_all_vps_are_contiguous_alphabetical_and_comments_are_header_only(self):
        r = Registry()
        for vp in ("eu", "na", "sa", "as", "af", "au"):
            for cls in ("windows", "debian"):
                r.put(
                    f"claims.vp-{vp}.icmp.no-connection.rt-fi-merged.itdk-os.transit-observed.{cls}.share-pct",
                    0,
                    unit="percent",
                )
        r.put("references.vp-eu.itdk.release", "2026-03")
        r.reserve("validation.datasets.fi.sequence-length")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "values.tex"
            emit(r, path, "test", [], [])
            text = path.read_text()
            definitions = [s for s in text.splitlines() if s.startswith(r"\setval{")]
            self.assertTrue(
                all("%" not in s.split("}", 1)[0] and " %" not in s for s in definitions)
            )
            self.assertIn("\\setval{refs.itdk.release}{2026-03}", text)
            self.assertIn("\\setval{validation.data.fi.sequence-length}{}", text)
            lines = [s for s in definitions if ".windows.pct}" in s]
            self.assertEqual(
                [s.split(".")[1] for s in lines], ["af", "as", "au", "eu", "na", "sa"]
            )
            self.assertIn("\n\n\n\n", text)
            self.assertIn("pct: percent", text)
            self.assertIn("{0.00\\%}", text)
        self.assertEqual(sorted(export_values(r.values), key=key_order)[0], "refs.itdk.release")

    def test_conflict_preserves_existing_export(self):
        r = Registry()
        for vp, release in [("eu", "2026-03"), ("af", "2026-04")]:
            r.put(f"references.vp-{vp}.itdk.release", release)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "values.tex"
            path.write_text("retained")
            with self.assertRaisesRegex(ValueError, "conflicting shared"):
                emit(r, path, "test", [], [])
            self.assertEqual(path.read_text(), "retained")


if __name__ == "__main__":
    unittest.main()
