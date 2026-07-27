from pathlib import Path
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.inspect_sequences import (
    parse_sequence,
    sample_sequences,
    sequence_diagnostics,
)
from ipid_analysis.strategies import MeasurementConfig


class InspectSequencesTests(unittest.TestCase):
    def test_samples_only_requested_strategy_deterministically(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "ipid.pq"
            strategies = root / "strategies.pq"
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["192.0.2.1", "192.0.2.2", "192.0.2.3"],
                        "IPID_SEQUENCE": ["1,2,3,4", "5,6,7,8", "9,10,11,12"],
                    }
                ),
                raw,
            )
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["192.0.2.1", "192.0.2.2", "192.0.2.3"],
                        "IPID_SELECTION_STRATEGY": pa.array(
                            ["UNCLASSIFIED", "RANDOM", "UNCLASSIFIED"]
                        ).dictionary_encode(),
                    }
                ),
                strategies,
            )

            total, first = sample_sequences(
                raw,
                strategies,
                strategy="UNCLASSIFIED",
                sample_count=2,
                seed=7,
            )
            _, second = sample_sequences(
                raw,
                strategies,
                strategy="UNCLASSIFIED",
                sample_count=2,
                seed=7,
            )

        self.assertEqual(total, 2)
        self.assertEqual(first, second)
        self.assertEqual({sample.ip_addr for sample in first}, {"192.0.2.1", "192.0.2.3"})

    def test_parse_sequence_preserves_missing_positions(self):
        sequence = parse_sequence("1,-,65535,bad", 5)
        np.testing.assert_array_equal(sequence, np.array([1, -1, 65535, -1, -1]))

    def test_diagnostics_use_production_random_score(self):
        cfg = MeasurementConfig(
            connection_count=4,
            requests_per_connection=25,
            request_ip_ids=np.array([1, 2, 3, 4]),
        )
        sequence = np.arange(cfg.sequence_length, dtype=np.int64)

        diagnostics = sequence_diagnostics(sequence, cfg)

        self.assertEqual(diagnostics.present_count, cfg.sequence_length)
        self.assertEqual(diagnostics.unique_count, cfg.sequence_length)
        self.assertGreaterEqual(diagnostics.cluster_count, 1)
        self.assertGreaterEqual(diagnostics.random_score, 0.0)
        self.assertLessEqual(diagnostics.random_score, 1.0)


if __name__ == "__main__":
    unittest.main()
