import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

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
    _log_axis_parameters,
    apply_strategy_impairments,
    calculate_minimum_increment_pvalues,
    calculate_strategy_pvalues,
    generate_chi2_sequences,
    render,
)
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_NULL_TABLE_VERSION,
    CANDIDATE_RANDOM_SCORE_VERSION,
)
from ipid_analysis.random_classifier_evaluation import EmpiricalNullTables
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
        mass_detected = classify_batch_mass(mass_values, config)
        np.testing.assert_array_equal(
            mass_detected[:sample_count],
            np.full(sample_count, int(IPIDStrategy.MULTI)),
        )
        self.assertTrue(
            np.isin(
                mass_detected[sample_count:],
                [
                    int(IPIDStrategy.RANDOM),
                    int(IPIDStrategy.UNCLASSIFIED),
                ],
            ).all()
        )
        self.assertGreaterEqual(
            int((mass_detected[sample_count:] == int(IPIDStrategy.RANDOM)).sum()),
            sample_count // 2,
        )

    def test_candidate_increment_uniformity_detects_per_connection_counter(self):
        sequences = generate_chi2_sequences(32, np.random.default_rng(9))["PER_CONNECTION"].astype(
            np.int64
        )
        connections = sequences.reshape(
            len(sequences),
            REQUESTS_PER_CONNECTION,
            CONNECTION_COUNT,
        ).transpose(0, 2, 1)
        for connection_index in range(CONNECTION_COUNT):
            increments = np.diff(connections[:, connection_index, :], axis=1) & 0xFFFF
            np.testing.assert_array_equal(increments, np.ones_like(increments))
        pvalues = calculate_minimum_increment_pvalues(
            sequences,
            np.zeros_like(sequences, dtype=bool),
            EmpiricalNullTables(256, seed=9),
        )
        self.assertTrue(np.all(pvalues <= 1.0 / 257.0))

    def test_log_axis_has_one_minor_tick_between_two_decade_major_ticks(self):
        axis_minimum, major_ticks, minor_ticks = _log_axis_parameters(
            {"strategy": np.asarray([1e-5, 1.0])}
        )

        self.assertEqual(axis_minimum, 1e-6)
        np.testing.assert_allclose(
            np.log10(major_ticks),
            np.arange(-6, 1, X_MAJOR_EXPONENT_STEP),
        )
        np.testing.assert_allclose(
            np.log10(minor_ticks),
            np.arange(
                -6 + X_MINOR_EXPONENT_OFFSET,
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
        null_tables = EmpiricalNullTables(128, seed=13)
        lossy_pvalues = calculate_strategy_pvalues(
            lossy_sequences,
            loss_masks,
            null_tables,
        )
        reordered_pvalues = calculate_strategy_pvalues(
            reordered_sequences,
            loss_masks,
            null_tables,
        )
        changed_by_reordering = []
        for strategy, values in lossy_pvalues.items():
            self.assertTrue(np.all(loss_masks[strategy].sum(axis=1) == 20), strategy)
            self.assertTrue(np.all(np.isfinite(values)), strategy)
            self.assertTrue(np.all((values >= 0) & (values <= 1)), strategy)
            changed_by_reordering.append(np.any(values != reordered_pvalues[strategy]))
        self.assertTrue(any(changed_by_reordering))

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("ipid_analysis.plot_chi2_pvalue_cdf.configure_paper_style"):
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
                    null_table_samples=64,
                    null_table_seed=13,
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
                    "INCREMENT_UNIFORMITY_P_VALUE",
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
            self.assertEqual(
                ideal_metadata["random_minimum_p_value_marker"],
                ideal_metadata["summary_by_strategy"]["RANDOM"]["minimum"],
            )
            self.assertEqual(metadata["ideal_sequence_length"], IDEAL_SEQUENCE_LENGTH)
            self.assertEqual(
                metadata["present_ipids_per_sequence"],
                PRESENT_SEQUENCE_LENGTH,
            )
            self.assertEqual(metadata["lost_ipids_per_sequence"], 20)
            self.assertEqual(metadata["reordered_ipids_per_sequence"], 0)
            self.assertEqual(
                metadata["random_minimum_p_value_marker"],
                metadata["summary_by_strategy"]["RANDOM"]["minimum"],
            )
            self.assertEqual(reordered_metadata["dataset"], LOSSY_REORDERED_DATASET)
            self.assertEqual(reordered_metadata["reordered_ipids_per_sequence"], 16)
            self.assertEqual(
                reordered_metadata["random_minimum_p_value_marker"],
                reordered_metadata["summary_by_strategy"]["RANDOM"]["minimum"],
            )
            self.assertEqual(
                metadata["increment_uniformity_test"]["scope"],
                (
                    "modulo-2^16 increments between originally adjacent present positions "
                    "within each subsequence"
                ),
            )
            increment_test = metadata["increment_uniformity_test"]
            self.assertEqual(
                increment_test["candidate_score_version"],
                CANDIDATE_RANDOM_SCORE_VERSION,
            )
            self.assertFalse(increment_test["order_invariant"])
            self.assertEqual(
                increment_test["subsequence_aggregation"],
                "minimum",
            )
            self.assertEqual(
                increment_test["subsequences"],
                list(INCREMENT_SUBSEQUENCES),
            )
            self.assertEqual(
                increment_test["null_tables"]["version"],
                CANDIDATE_NULL_TABLE_VERSION,
            )
            self.assertEqual(increment_test["null_tables"]["sample_count"], 64)
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
