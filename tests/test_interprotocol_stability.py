import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import matplotlib.pyplot as plt
import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.interprotocol import GROUPS, InterprotocolConfig, classify_file, classify_sequence
from ipid_analysis.interprotocol_stability import (
    classify_protocol_rows,
    plot_campaign_strategy_stability,
)
from ipid_analysis.random_classifier_candidate import create_candidate_null_tables


PATTERN = (18933, 18932, 3717, 3718, 3719)


def raw_row(sequence, baseline):
    return {"IP_ADDR": "192.0.2.1", "IPID_SELECTION_STRATEGY": baseline,
            "IPID_SEQUENCE": ",".join("-" if value < 0 else str(value) for value in sequence)}


class InterprotocolStabilityTests(unittest.TestCase):
    def test_class_plots_compare_groups_with_correct_observation_denominators(self):
        table = pa.table({
            "BASELINE_STRATEGY": ["REFLECTION"] * 4 + ["CONSTANT"],
            "PROTOCOL_GROUP": ["icmp-tcp"] * 3 + ["icmp-tcp-udp", "icmp-tcp"],
            "STRATEGY_STATUS": ["UNCHANGED", "CHANGED", "NOT_ENOUGH_SAMPLES",
                                "UNCLASSIFIED", "UNCHANGED"],
        })
        with tempfile.TemporaryDirectory() as directory:
            figures = []
            real_close = plt.close
            def capture(figure):
                if hasattr(figure, "axes"):
                    figures.append(figure)
                real_close(figure)
            with patch("ipid_analysis.interprotocol_stability.plt.close", side_effect=capture):
                paths = plot_campaign_strategy_stability(table, Path(directory), tuple(GROUPS))
            self.assertEqual([p.name for p in paths], [
                f"interprotocol-campaign-{s}.pdf" for s in ("reflection", "constant", "multi", "random")
            ])
            self.assertTrue(all(p.read_bytes().startswith(b"%PDF-") for p in paths))
            reflection = figures[0].axes[0]
            heights = np.asarray([bar.get_height() for bar in reflection.patches]).reshape(4, 4)
            np.testing.assert_allclose(heights[:, 0], [100 / 3, 100 / 3, 100 / 3, 0])
            np.testing.assert_allclose(heights[:, 1:3], 0)
            np.testing.assert_allclose(heights[:, 3], [0, 0, 0, 100])
            self.assertEqual(reflection.get_xticklabels()[0].get_text(), "ICMP$\\cap$TCP\n(N=3)")
            np.testing.assert_allclose(figures[0].get_size_inches(), [6.5, 2.55])
            self.assertEqual(reflection.get_ylabel(), "Percentage [%]")
            self.assertEqual(reflection.get_title(), "")
            self.assertEqual([t.get_text() for t in reflection.get_legend().get_texts()],
                             ["Unchanged", "Changed", "Not Enough Samples", "Unclassified"])
            np.testing.assert_allclose([bar.get_width() for bar in reflection.patches], 0.48)
            # No MULTI or RANDOM cohort must still produce an explicit empty plot.
            for figure in figures[2:]:
                self.assertTrue(all(bar.get_height() == 0 for bar in figure.axes[0].patches))
                self.assertTrue(all("N=0" in tick.get_text() for tick in figure.axes[0].get_xticklabels()))

    def test_reflection_uses_actual_global_request_positions_and_wraparound(self):
        for protocols in GROUPS.values():
            cfg = InterprotocolConfig(protocols, 4, 25)
            requests = np.asarray(PATTERN)[np.arange(cfg.sequence_length) % len(PATTERN)]
            sequence = (requests + 65530) % 65536
            rows = classify_protocol_rows([raw_row(sequence, "REFLECTION")], cfg, PATTERN)
            self.assertEqual([r["STRATEGY_STATUS"] for r in rows], ["UNCHANGED"] * len(protocols))
            self.assertTrue(all(r["OBSERVED_STRATEGY"] == "REFLECTION" for r in rows))

    def test_all_four_additional_classes_are_reclassified(self):
        cfg = InterprotocolConfig(("icmp", "tcp", "udp"), 4, 25)
        rng = np.random.default_rng(7)
        fixtures = {
            "CONSTANT": np.full(cfg.sequence_length, 42),
            "MULTI": np.repeat(np.tile([100, 40000], 50), 3),
            "RANDOM": rng.integers(0, 65536, cfg.sequence_length),
        }
        tables = create_candidate_null_tables(256, seed=9)
        for baseline, sequence in fixtures.items():
            with self.subTest(baseline=baseline):
                rows = classify_protocol_rows(
                    [raw_row(sequence, baseline)], cfg, PATTERN, random_null_tables=tables
                )
                self.assertTrue(all(r["STRATEGY_STATUS"] == "UNCHANGED" for r in rows), rows)
                self.assertEqual(classify_sequence(sequence, cfg, baseline), "NOT_APPLICABLE")

    def test_only_changed_protocol_is_marked_changed(self):
        cfg = InterprotocolConfig(("icmp", "tcp"), 4, 25)
        sequence = np.full(cfg.sequence_length, 42)
        requests = np.asarray(PATTERN)[np.arange(cfg.sequence_length) % len(PATTERN)]
        sequence[0::2] = requests[0::2]
        rows = classify_protocol_rows([raw_row(sequence, "REFLECTION")], cfg, PATTERN)
        self.assertEqual([r["STRATEGY_STATUS"] for r in rows], ["UNCHANGED", "CHANGED"])
        self.assertEqual(rows[1]["OBSERVED_STRATEGY"], "CONSTANT")

    def test_missing_protocol_does_not_erase_other_protocol_result(self):
        cfg = InterprotocolConfig(("tcp", "udp"), 4, 25)
        sequence = np.full(cfg.sequence_length, 42)
        sequence[7] = -1
        rows = classify_protocol_rows([raw_row(sequence, "CONSTANT")], cfg, PATTERN)
        self.assertEqual([r["STRATEGY_STATUS"] for r in rows], ["UNCHANGED", "NOT_ENOUGH_SAMPLES"])
        self.assertEqual(rows[1]["RECEIVED_SAMPLES"], 99)

    def test_unclassified_is_not_reported_as_a_strategy_change(self):
        cfg = InterprotocolConfig(("tcp", "udp"), 4, 25)
        sequence = np.random.default_rng(7).integers(0, 65536, cfg.sequence_length)
        with patch("ipid_analysis.strategies.random_structure_scores", return_value=np.zeros(1)):
            rows = classify_protocol_rows([raw_row(sequence, "RANDOM")], cfg, PATTERN)
        self.assertTrue(all(r["STRATEGY_STATUS"] == "UNCLASSIFIED" for r in rows))

    def test_invalid_metadata_raises_even_for_empty_or_missing_rows(self):
        for cfg, pattern, reason in (
            (InterprotocolConfig(("icmp", "tcp"), 4, 4), PATTERN, "4x25"),
            (InterprotocolConfig(("icmp", "tcp"), 4, 25), (), "requires request_ip_ids"),
            (InterprotocolConfig(("icmp", "tcp"), 4, 25), (1, 2), "constant"),
            (InterprotocolConfig(("icmp", "tcp"), 4, 25), (-1, 65536), "16-bit"),
        ):
            for rows in ([], [raw_row(np.full(cfg.sequence_length, 42), "CONSTANT")],
                         [raw_row(np.full(cfg.sequence_length, -1), "CONSTANT")]):
                with self.assertRaisesRegex(ValueError, reason):
                    classify_protocol_rows(rows, cfg, pattern)

    def test_unsupported_baseline_is_an_error(self):
        cfg = InterprotocolConfig(("icmp", "tcp"), 4, 25)
        with self.assertRaisesRegex(ValueError, "unsupported interprotocol baseline"):
            classify_protocol_rows([raw_row(np.full(200, 42), "UNKNOWN")], cfg, PATTERN)

    def test_invalid_file_snapshot_fails_before_writing_results(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = root / "snapshot.yaml"
            snapshot.write_text("connection_count: 4\nrequests_per_connection: 25\n"
                                "interprotocol:\n  protocols: [icmp, tcp]\n")
            with self.assertRaisesRegex(ValueError, "requires request_ip_ids"):
                classify_file(root / "raw.pq", snapshot, root / "result")
            self.assertFalse((root / "result").exists())

    def test_malformed_length_and_out_of_range_values_are_not_changes(self):
        cfg = InterprotocolConfig(("icmp", "tcp"), 4, 25)
        for sequence in (np.ones(199, dtype=int), np.full(200, 65536)):
            rows = classify_protocol_rows([raw_row(sequence, "CONSTANT")], cfg, PATTERN)
            self.assertTrue(all(r["STRATEGY_STATUS"] == "NOT_ENOUGH_SAMPLES" for r in rows))

    def test_real_file_writes_observations_summary_and_plots(self):
        cfg = InterprotocolConfig(("icmp", "tcp"), 4, 25)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw.pq"
            snapshot = root / "snapshot.yaml"
            pq.write_table(pa.Table.from_pylist([raw_row(np.full(200, 42), "CONSTANT")]), raw)
            snapshot.write_text("connection_count: 4\nrequests_per_connection: 25\n"
                                f"request_ip_ids: {list(PATTERN)}\n"
                                "interprotocol:\n  protocols: [icmp, tcp]\n")
            output = root / "processed"
            figures = root / "figures"
            classify_file(raw, snapshot, output, figure_dir=figures)
            table = pq.read_table(output / "interprotocol-strategy-stability.pq")
            self.assertEqual(table.num_rows, 2)
            self.assertEqual(table["STRATEGY_STATUS"].to_pylist(), ["UNCHANGED", "UNCHANGED"])
            summary = json.loads((output / "interprotocol-strategy-stability-summary.json").read_text())
            self.assertEqual(summary["rows"], 2)
            self.assertTrue((figures / "interprotocol-strategy-stability.pdf").is_file())
            self.assertTrue((figures / "interprotocol-strategy-transitions.pdf").is_file())


if __name__ == "__main__":
    unittest.main()
