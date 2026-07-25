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
    IDEAL_SEQUENCE_LENGTH,
    LOSSY_DATASET,
    LOSSY_REORDERED_DATASET,
    PLOT_STRATEGIES,
    PRESENT_SEQUENCE_LENGTH,
    REQUESTS_PER_CONNECTION,
    TRIVIAL_SAMPLES_PER_STRATEGY,
    apply_strategy_impairments,
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
            self.assertEqual(
                values.shape,
                (expected_count, IDEAL_SEQUENCE_LENGTH),
                strategy,
            )

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
        loss_masks, lossy_sequences, reordered_sequences = apply_strategy_impairments(
            sequences,
            np.random.default_rng(12),
        )
        lossy_pvalues = calculate_strategy_pvalues(lossy_sequences, loss_masks)
        reordered_pvalues = calculate_strategy_pvalues(reordered_sequences, loss_masks)
        for strategy, values in lossy_pvalues.items():
            self.assertTrue(np.all(loss_masks[strategy].sum(axis=1) == 20), strategy)
            self.assertTrue(np.all(np.isfinite(values)), strategy)
            self.assertTrue(np.all((values >= 0) & (values <= 1)), strategy)
            np.testing.assert_array_equal(values, reordered_pvalues[strategy])

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (
                lossy_pdf_path,
                lossy_json_path,
                reordered_pdf_path,
                reordered_json_path,
                aggregate_path,
            ) = render(
                samples_per_strategy=sample_count,
                seed=11,
                processed_root=root / "processed",
                figures_root=root / "figures",
            )
            for path in (
                lossy_pdf_path,
                lossy_json_path,
                reordered_pdf_path,
                reordered_json_path,
                aggregate_path,
            ):
                self.assertTrue(path.is_file(), path)

            table = pq.read_table(aggregate_path)
            expected_rows = 2 * (2 * TRIVIAL_SAMPLES_PER_STRATEGY + 6 * sample_count)
            self.assertEqual(table.num_rows, expected_rows)
            self.assertEqual(
                set(table.column("DATASET").to_pylist()),
                {LOSSY_DATASET, LOSSY_REORDERED_DATASET},
            )
            self.assertEqual(
                set(table.column("IPID_SELECTION_STRATEGY").to_pylist()),
                set(PLOT_STRATEGIES),
            )

            metadata = json.loads(lossy_json_path.read_text())
            reordered_metadata = json.loads(reordered_json_path.read_text())
            self.assertEqual(metadata["ideal_sequence_length"], IDEAL_SEQUENCE_LENGTH)
            self.assertEqual(
                metadata["present_ipids_per_sequence"],
                PRESENT_SEQUENCE_LENGTH,
            )
            self.assertEqual(metadata["lost_ipids_per_sequence"], 20)
            self.assertEqual(metadata["reordered_ipids_per_sequence"], 16)
            self.assertTrue(metadata["lossy_and_reordered_pvalues_identical"])
            self.assertEqual(reordered_metadata["dataset"], LOSSY_REORDERED_DATASET)
            self.assertEqual(
                metadata["chi2_uniformity_test"]["scope"],
                "all present IP-ID values in one sequence",
            )
            self.assertTrue(metadata["chi2_uniformity_test"]["order_invariant"])
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
