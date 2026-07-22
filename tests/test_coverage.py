import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.coverage import write_coverage
from ipid_analysis.manifest import IpidMeasurement


class CoverageTest(unittest.TestCase):
    def setUp(self):
        self.manifest = {
            "tcp": {
                "zmap": "tcp-zmap",
                "ipid": {
                    "no-connection": {
                        "rt-based": {"base": "tcp-base"},
                        "fixed-interval": {"mass": "tcp-mass"},
                    }
                },
            }
        }

    @staticmethod
    def _write(path: Path, addresses: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"IP_ADDR": addresses}), path)

    def _measurement(self, interval: str, scale: str, measurement_id: str) -> IpidMeasurement:
        return IpidMeasurement(
            protocol="tcp",
            connection_mode="no-connection",
            interval=interval,
            scale=scale,
            measurement_id=measurement_id,
            zmap_id="tcp-zmap",
        )

    def test_uses_original_zmap_for_regular_measurement(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory)
            measurement = self._measurement("rt-based", "base", "tcp-base")
            measurement_dir = raw_root / measurement.input_key
            self._write(
                raw_root / "zmap" / "tcp-zmap" / "zmap.pq",
                ["192.0.2.1", "192.0.2.1", "192.0.2.2"],
            )
            self._write(measurement_dir / "ipid.pq", ["192.0.2.1"])

            output_path = write_coverage(measurement, self.manifest, raw_root)

            self.assertEqual(
                json.loads(output_path.read_text()),
                {
                    "zmap_ip_count": 2,
                    "ipid_ip_count": 1,
                    "coverage_percent": 50.0,
                },
            )

    def test_uses_rt_unclassified_targets_for_fixed_interval_mass(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory)
            measurement = self._measurement("fixed-interval", "mass", "tcp-mass")
            self._write(
                raw_root / "ipid" / "tcp-base" / "zmap_unclassified.pq",
                ["192.0.2.1", "192.0.2.2", "192.0.2.3"],
            )
            self._write(raw_root / measurement.input_key / "ipid.pq", ["192.0.2.1"])

            output_path = write_coverage(measurement, self.manifest, raw_root)

            self.assertAlmostEqual(
                json.loads(output_path.read_text())["coverage_percent"],
                100 / 3,
            )


if __name__ == "__main__":
    unittest.main()
