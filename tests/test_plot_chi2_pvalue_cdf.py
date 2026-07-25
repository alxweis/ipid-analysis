import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.classifier_validation import REQUEST_IP_IDS
from ipid_analysis.plot_chi2_pvalue_cdf import (
    CONNECTION_COUNT,
    PLOT_STRATEGIES,
    REQUESTS_PER_CONNECTION,
    SEQUENCE_LENGTH,
    TRIVIAL_SAMPLES_PER_STRATEGY,
    calculate_strategy_pvalues,
    generate_chi2_sequences,
    render,
)
from ipid_analysis.strategies import (
    IPIDStrategy,
    MeasurementConfig,
    classify_batch,
    classify_batch_mass,
)


class Chi2PvalueCDFTest(unittest.TestCase):
    def test_generators_have_expected_shapes_and_strategy_classes(self):
        sample_count = 16
        sequences = generate_chi2_sequences(sample_count, np.random.default_rng(7))

        self.assertEqual(tuple(sequences), PLOT_STRATEGIES)
        for strategy, values in sequences.items():
            expected_count = (
                TRIVIAL_SAMPLES_PER_STRATEGY
                if strategy in {"REFLECTION", "CONSTANT"}
                else sample_count
            )
            self.assertEqual(values.shape, (expected_count, SEQUENCE_LENGTH), strategy)

        config = MeasurementConfig(
            connection_count=CONNECTION_COUNT,
            requests_per_connection=REQUESTS_PER_CONNECTION,
            request_ip_ids=REQUEST_IP_IDS,
        )
        for strategy in PLOT_STRATEGIES[:6]:
            detected = classify_batch(sequences[strategy], config)
            self.assertTrue(
                np.all(detected == int(IPIDStrategy[strategy])),
                strategy,
            )

        mass_values = pa.array(
            [
                *sequences["MULTI"].astype(np.int64).tolist(),
                *sequences["RANDOM"].astype(np.int64).tolist(),
            ],
            type=pa.list_(pa.int64()),
        )
        mass_detected = classify_batch_mass(mass_values)
        np.testing.assert_array_equal(
            mass_detected[:sample_count],
            np.full(sample_count, int(IPIDStrategy.MULTI)),
        )
        np.testing.assert_array_equal(
            mass_detected[sample_count:],
            np.full(sample_count, int(IPIDStrategy.RANDOM)),
        )

    def test_pvalues_and_rendered_artifacts(self):
        sample_count = 8
        sequences = generate_chi2_sequences(sample_count, np.random.default_rng(11))
        pvalues = calculate_strategy_pvalues(sequences)
        for strategy, values in pvalues.items():
            self.assertTrue(np.all(np.isfinite(values)), strategy)
            self.assertTrue(np.all((values >= 0) & (values <= 1)), strategy)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pdf_path, json_path, aggregate_path = render(
                samples_per_strategy=sample_count,
                seed=11,
                processed_root=root / "processed",
                figures_root=root / "figures",
            )
            self.assertTrue(pdf_path.is_file())
            self.assertTrue(json_path.is_file())
            self.assertTrue(aggregate_path.is_file())

            table = pq.read_table(aggregate_path)
            expected_rows = 2 * TRIVIAL_SAMPLES_PER_STRATEGY + 6 * sample_count
            self.assertEqual(table.num_rows, expected_rows)
            self.assertEqual(
                set(table.column("IPID_SELECTION_STRATEGY").to_pylist()),
                set(PLOT_STRATEGIES),
            )

            metadata = json.loads(json_path.read_text())
            self.assertEqual(metadata["sequence_length"], SEQUENCE_LENGTH)
            self.assertEqual(
                metadata["chi2_uniformity_test"]["scope"],
                "all IP-ID values in one sequence",
            )
            self.assertIsNone(metadata["chi2_uniformity_test"]["subsequence_aggregation"])
            self.assertEqual(
                metadata["samples_by_strategy"]["REFLECTION"],
                TRIVIAL_SAMPLES_PER_STRATEGY,
            )
            self.assertEqual(
                metadata["samples_by_strategy"]["RANDOM"],
                sample_count,
            )


if __name__ == "__main__":
    unittest.main()
