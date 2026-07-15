import unittest
from pathlib import Path

from ipid_analysis.manifest import IpidMeasurement, iter_ipid_measurements, resolve


class ManifestTest(unittest.TestCase):
    def setUp(self):
        self.manifest = {
            "tcp": {
                "zmap": "tcp-zmap-run",
                "ipid": {
                    "no-connection": {
                        "rt-based": {"base": "tcp-n-rt-b"},
                        "fixed-interval": {
                            "base": "tcp-n-fi-b",
                            "mass": "tcp-n-fi-m",
                        },
                    },
                    "connection": {
                        "rt-based": {"base": "tcp-c-rt-b"},
                        "fixed-interval": {"base": "tcp-c-fi-b"},
                    },
                },
            }
        }

    def test_resolve_uses_descriptive_manifest_keys(self):
        measurement = resolve(
            self.manifest,
            "tcp.ipid.no-connection.fixed-interval.mass",
        )

        self.assertEqual(
            measurement,
            IpidMeasurement(
                protocol="tcp",
                connection_mode="no-connection",
                interval="fixed-interval",
                scale="mass",
                measurement_id="tcp-n-fi-m",
                zmap_id="tcp-zmap-run",
            ),
        )
        self.assertEqual(
            measurement.target,
            "tcp.ipid.no-connection.fixed-interval.mass",
        )

    def test_artifact_path_applies_directory_and_filename_schema(self):
        measurement = resolve(
            self.manifest,
            "tcp.ipid.no-connection.fixed-interval.mass",
        )

        self.assertEqual(
            measurement.artifact_path(Path("reports/figures"), "strategies", "pdf"),
            Path("reports/figures")
            / "tcp-zmap-run"
            / "no-connection"
            / "fixed-interval-mass"
            / "n-fi-m_strategies.pdf",
        )

    def test_all_abbreviations_are_used_in_artifact_names(self):
        expected = {
            "tcp.ipid.no-connection.rt-based.base": "n-rt-b_increments.pq",
            "tcp.ipid.no-connection.fixed-interval.mass": "n-fi-m_increments.pq",
            "tcp.ipid.connection.rt-based.base": "c-rt-b_increments.pq",
            "tcp.ipid.connection.fixed-interval.base": "c-fi-b_increments.pq",
        }

        for target, filename in expected.items():
            with self.subTest(target=target):
                measurement = resolve(self.manifest, target)
                self.assertEqual(measurement.artifact_name("increments"), filename)

    def test_iteration_order_is_stable_with_new_keys(self):
        self.assertEqual(
            [measurement.target for measurement in iter_ipid_measurements(self.manifest)],
            [
                "tcp.ipid.no-connection.rt-based.base",
                "tcp.ipid.no-connection.fixed-interval.base",
                "tcp.ipid.no-connection.fixed-interval.mass",
                "tcp.ipid.connection.rt-based.base",
                "tcp.ipid.connection.fixed-interval.base",
            ],
        )

    def test_legacy_abbreviated_target_is_rejected(self):
        with self.assertRaises(ValueError):
            resolve(self.manifest, "tcp.ipid.nec.rt.base")


if __name__ == "__main__":
    unittest.main()
