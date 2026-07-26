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
    IDEAL_DATASET,
    IDEAL_SEQUENCE_LENGTH,
    INCREMENT_SUBSEQUENCES,
    LOSSY_DATASET,
    LOSSY_REORDERED_DATASET,
    PLOT_STRATEGIES,
    PRESENT_SEQUENCE_LENGTH,
    REQUESTS_PER_CONNECTION,
    TRIVIAL_SAMPLES_PER_STRATEGY,
    X_AXIS_MAXIMUM,
    X_MAJOR_EXPONENT_STEP,
    X_MINOR_EXPONENT_OFFSET,
    _increment_pvalues,
    _log_axis_parameters,
    apply_strategy_impairments,
    calculate_strategy_pvalues,
    generate_chi2_sequences,
    render,
)
from ipid_analysis.strategies import (
    MAX_INC,
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

    def test_ideal_per_connection_subsequences_have_constant_pvalue(self):
        sequences = generate_chi2_sequences(32, np.random.default_rng(9))[
            "PER_CONNECTION"
        ].astype(np.int64)
        connections = sequences.reshape(
            len(sequences),
            REQUESTS_PER_CONNECTION,
            CONNECTION_COUNT,
        ).transpose(0, 2, 1)
        present = np.ones(
            (len(sequences), REQUESTS_PER_CONNECTION),
            dtype=bool,
        )

        for connection_index in range(CONNECTION_COUNT):
            increments = (
                np.diff(connections[:, connection_index, :], axis=1) & 0xFFFF
            )
            np.testing.assert_array_equal(increments, np.ones_like(increments))
            pvalues = _increment_pvalues(
                connections[:, connection_index, :],
                present,
            )
            self.assertEqual(len(np.unique(pvalues)), 1)

    def test_log_axis_has_one_minor_tick_between_twenty_decade_major_ticks(self):
        axis_minimum, major_ticks, minor_ticks = _log_axis_parameters(
            {"strategy": np.asarray([1e-185, 1.0])}
        )

        self.assertEqual(axis_minimum, 1e-200)
        np.testing.assert_allclose(
            np.log10(major_ticks),
            np.arange(-200, 1, X_MAJOR_EXPONENT_STEP),
        )
        np.testing.assert_allclose(
            np.log10(minor_ticks),
            np.arange(
                -200 + X_MINOR_EXPONENT_OFFSET,
                1,
                X_MAJOR_EXPONENT_STEP,
            ),
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
        changed_by_reordering = []
        for strategy, values in lossy_pvalues.items():
            self.assertTrue(np.all(loss_masks[strategy].sum(axis=1) == 20), strategy)
            self.assertTrue(np.all(np.isfinite(values)), strategy)
            self.assertTrue(np.all((values >= 0) & (values <= 1)), strategy)
            changed_by_reordering.append(np.any(values != reordered_pvalues[strategy]))
        self.assertTrue(any(changed_by_reordering))

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (
                ideal_pdf_path,
                ideal_json_path,
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
                ideal_pdf_path,
                ideal_json_path,
                lossy_pdf_path,
                lossy_json_path,
                reordered_pdf_path,
                reordered_json_path,
                aggregate_path,
            ):
                self.assertTrue(path.is_file(), path)

            table = pq.read_table(aggregate_path)
            expected_rows = 3 * (2 * TRIVIAL_SAMPLES_PER_STRATEGY + 6 * sample_count)
            self.assertEqual(table.num_rows, expected_rows)
            self.assertEqual(
                set(table.column("DATASET").to_pylist()),
                {IDEAL_DATASET, LOSSY_DATASET, LOSSY_REORDERED_DATASET},
            )
            self.assertEqual(
                set(table.column("IPID_SELECTION_STRATEGY").to_pylist()),
                set(PLOT_STRATEGIES),
            )
            self.assertEqual(
                table.column_names,
                [
                    "DATASET",
                    "IPID_SELECTION_STRATEGY",
                    "SAMPLE_INDEX",
                    "MINIMUM_CHI2_P_VALUE",
                ],
            )

            ideal_metadata = json.loads(ideal_json_path.read_text())
            metadata = json.loads(lossy_json_path.read_text())
            reordered_metadata = json.loads(reordered_json_path.read_text())
            self.assertEqual(ideal_metadata["dataset"], IDEAL_DATASET)
            self.assertEqual(
                ideal_metadata["present_ipids_per_sequence"],
                IDEAL_SEQUENCE_LENGTH,
            )
            self.assertEqual(ideal_metadata["lost_ipids_per_sequence"], 0)
            self.assertEqual(ideal_metadata["reordered_ipids_per_sequence"], 0)
            self.assertEqual(metadata["ideal_sequence_length"], IDEAL_SEQUENCE_LENGTH)
            self.assertEqual(
                metadata["present_ipids_per_sequence"],
                PRESENT_SEQUENCE_LENGTH,
            )
            self.assertEqual(metadata["lost_ipids_per_sequence"], 20)
            self.assertEqual(metadata["reordered_ipids_per_sequence"], 0)
            self.assertEqual(reordered_metadata["dataset"], LOSSY_REORDERED_DATASET)
            self.assertEqual(reordered_metadata["reordered_ipids_per_sequence"], 16)
            self.assertEqual(
                metadata["chi2_uniformity_test"]["scope"],
                (
                    "modulo-2^16 increments between consecutive present values "
                    "within each subsequence"
                ),
            )
            self.assertFalse(metadata["chi2_uniformity_test"]["order_invariant"])
            self.assertEqual(
                metadata["chi2_uniformity_test"]["subsequence_aggregation"],
                "minimum",
            )
            self.assertEqual(
                metadata["chi2_uniformity_test"]["subsequences"],
                list(INCREMENT_SUBSEQUENCES),
            )
            self.assertEqual(metadata["chi2_uniformity_test"]["bins"], 4)
            self.assertEqual(metadata["chi2_uniformity_test"]["degrees_of_freedom"], 3)
            self.assertGreater(metadata["x_axis_maximum"], 1.0)
            self.assertEqual(metadata["x_axis_maximum"], X_AXIS_MAXIMUM)
            self.assertEqual(
                metadata["x_axis_major_exponent_step"],
                X_MAJOR_EXPONENT_STEP,
            )
            self.assertEqual(
                metadata["x_axis_minor_exponent_offset"],
                X_MINOR_EXPONENT_OFFSET,
            )
            self.assertEqual(
                metadata["samples_by_strategy"]["REFLECTION"],
                TRIVIAL_SAMPLES_PER_STRATEGY,
            )
            self.assertEqual(
                metadata["samples_by_strategy"]["RANDOM"],
                sample_count,
            )
            self.assertEqual(
                metadata["synthetic_generator_parameters"]["PER_BUCKET"][
                    "increment_range_inclusive"
                ],
                [1, MAX_INC],
            )


if __name__ == "__main__":
    unittest.main()
