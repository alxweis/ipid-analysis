import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.coverage import write_coverage
from ipid_analysis.manifest import IpidMeasurement


class CoverageTest(unittest.TestCase):
    def test_writes_distinct_ip_coverage_for_measurement_target_file(self):
        measurement = IpidMeasurement(
            protocol="tcp",
            connection_mode="no-connection",
            interval="fixed-interval",
            scale="mass",
            measurement_id="tcp-mass",
            zmap_id="tcp-zmap",
        )
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory)
            measurement_dir = raw_root / measurement.input_key
            measurement_dir.mkdir(parents=True)
            pq.write_table(
                pa.table({"IP_ADDR": ["192.0.2.1", "192.0.2.1", "192.0.2.2"]}),
                measurement_dir / "zmap_unclassified.pq",
            )
            pq.write_table(
                pa.table({"IP_ADDR": ["192.0.2.1"]}),
                measurement_dir / "ipid.pq",
            )

            output_path = write_coverage(measurement, raw_root)

            self.assertEqual(output_path, measurement_dir / "coverage.json")
            self.assertEqual(
                json.loads(output_path.read_text()),
                {
                    "zmap_ip_count": 2,
                    "ipid_ip_count": 1,
                    "coverage_percent": 50.0,
                },
            )


if __name__ == "__main__":
    unittest.main()
