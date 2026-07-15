from pathlib import Path
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.strategies import (
    IPIDStrategy,
    MeasurementConfig,
    classify_batch,
    classify_batch_mass,
    classify_paths,
    load_config,
)


class StrategyClassificationTest(unittest.TestCase):
    def setUp(self):
        self.base_config = MeasurementConfig(
            connection_count=4,
            requests_per_connection=4,
            request_ip_ids=np.asarray([1, 2, 3, 4], dtype=np.int64),
        )

    def test_base_keeps_cheap_deterministic_strategy(self):
        matrix = np.asarray([[7] * 16], dtype=np.uint16)

        codes = classify_batch(matrix, self.base_config)

        self.assertEqual(codes.tolist(), [int(IPIDStrategy.CONSTANT)])

    def test_base_leaves_clustered_sequence_unclassified(self):
        matrix = np.asarray(
            [
                [
                    100,
                    10_000,
                    200,
                    10_100,
                    30_000,
                    10_200,
                    30_001,
                    10_300,
                    30_002,
                    10_400,
                    30_003,
                    10_500,
                    30_004,
                    10_600,
                    30_005,
                    10_700,
                ]
            ],
            dtype=np.uint16,
        )

        codes = classify_batch(matrix, self.base_config)

        self.assertEqual(codes.tolist(), [int(IPIDStrategy.UNCLASSIFIED)])

    def test_mass_classifies_only_constant_multi_and_random(self):
        rng = np.random.default_rng(42)
        random_values = rng.integers(0, 1 << 16, size=100).tolist()
        multi_values = list(range(40)) + list(range(10_000, 10_040))
        rng.shuffle(multi_values)
        single_values = list(range(100))
        values = pa.array(
            [
                [17] * 80,
                multi_values,
                random_values,
                single_values,
            ],
            type=pa.list_(pa.int64()),
        )

        codes = classify_batch_mass(values)

        self.assertEqual(
            codes.tolist(),
            [
                int(IPIDStrategy.CONSTANT),
                int(IPIDStrategy.MULTI),
                int(IPIDStrategy.RANDOM),
                int(IPIDStrategy.UNCLASSIFIED),
            ],
        )

    def test_mass_does_not_duplicate_measurement_reply_rate_filter(self):
        values = pa.array([[17] * 79], type=pa.list_(pa.int64()))

        codes = classify_batch_mass(values)

        self.assertEqual(codes.tolist(), [int(IPIDStrategy.CONSTANT)])

    def test_mass_classification_is_position_independent(self):
        rng = np.random.default_rng(7)
        random_values = rng.integers(0, 1 << 16, size=100).tolist()
        multi_values = list(range(40)) + list(range(10_000, 10_040))
        original = pa.array([multi_values, random_values], type=pa.list_(pa.int64()))
        shuffled = pa.array(
            [rng.permutation(row).tolist() for row in [multi_values, random_values]],
            type=pa.list_(pa.int64()),
        )

        original_codes = classify_batch_mass(original)
        shuffled_codes = classify_batch_mass(shuffled)

        self.assertEqual(original_codes.tolist(), shuffled_codes.tolist())
        self.assertEqual(
            original_codes.tolist(),
            [int(IPIDStrategy.MULTI), int(IPIDStrategy.RANDOM)],
        )

    def test_snapshot_loads_measurement_shape_without_reply_rate(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = Path(directory) / "ipid.snapshot.yaml"
            snapshot.write_text(
                "connection_count: 4\nrequests_per_connection: 25\nrequest_ip_ids: [1, 2, 3, 4]\n"
            )

            config = load_config(snapshot)

        self.assertEqual(config.sequence_length, 100)

    def test_base_incomplete_sequences_are_unclassified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "ipid.pq"
            snapshot = root / "ipid.snapshot.yaml"
            output = root / "strategies.pq"
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["192.0.2.1", "192.0.2.2"],
                        "IPID_SEQUENCE": [
                            ",".join(["7"] * 12 + ["-"] * 4),
                            ",".join(["7"] * 13 + ["-"] * 3),
                        ],
                    }
                ),
                source,
            )
            snapshot.write_text(
                "connection_count: 4\nrequests_per_connection: 4\nrequest_ip_ids: [1, 2, 3, 4]\n"
            )

            classify_paths(source, snapshot, output, protocol="icmp")

            strategies = pq.read_table(output)["IPID_SELECTION_STRATEGY"].to_pylist()
            self.assertEqual(
                strategies,
                [IPIDStrategy.UNCLASSIFIED.name, IPIDStrategy.UNCLASSIFIED.name],
            )


if __name__ == "__main__":
    unittest.main()
