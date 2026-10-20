import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from ipid_analysis.interprotocol import (
    InterprotocolConfig,
    classify_campaign,
    load_run_manifest,
    synthetic_sequence,
)
from ipid_analysis.interprotocol_outcomes import classify_rows, prerequisites
from ipid_analysis.random_classifier_candidate import create_candidate_null_tables

PATTERN = [18933, 18932, 3717, 3718, 3719]


def snapshot(sample="base", protocols=("icmp", "tcp", "udp")):
    return {
        "connection_count": 4,
        "requests_per_connection": 4 if sample == "base" else 25,
        "measurement_mode": "rt-based" if sample == "base" else "fixed-interval",
        "request_ip_ids": PATTERN,
        "interprotocol": {
            "sample": sample,
            "protocols": list(protocols),
            "request_ipid_index": "logical",
        },
    }


def raw(sequence, baseline, ip="192.0.2.1"):
    return {
        "IP_ADDR": ip,
        "IPID_SELECTION_STRATEGY": baseline,
        "IPID_SEQUENCE": ",".join(str(int(x)) if x >= 0 else "-" for x in sequence),
    }


def result(sequence, baseline, protocols=("icmp", "tcp", "udp"), sample="base", **kwargs):
    cfg = InterprotocolConfig(protocols, 4, 4 if sample == "base" else 25)
    return classify_rows([raw(sequence, baseline)], cfg, snapshot(sample, protocols), **kwargs)[0]


class OutcomeTests(unittest.TestCase):
    def test_shared_destination_overrides_all_changed_individual_labels(self):
        for protocols in [("icmp", "tcp"), ("icmp", "tcp", "udp")]:
            cfg = InterprotocolConfig(protocols, 4, 4)
            seq = synthetic_sequence(cfg, "PER_DESTINATION", (protocols,), 3)
            r = result(seq, "PER_DESTINATION", protocols)
            self.assertEqual(r["OUTCOME"], "PROTOCOL_SHARED")
            self.assertEqual(r["RAW_CHANGED_PROTOCOLS"], len(protocols))
            self.assertEqual(r["STATE_COUNTS"], [len(protocols), 0, 0])

    def test_all_counter_partitions_and_isolation(self):
        protocols = ("icmp", "tcp", "udp")
        cfg = InterprotocolConfig(protocols, 4, 4)
        for strategy in ("SINGLE", "PER_DESTINATION", "PER_BUCKET"):
            for pair, single in [
                (("icmp", "tcp"), ("udp",)),
                (("icmp", "udp"), ("tcp",)),
                (("tcp", "udp"), ("icmp",)),
            ]:
                # Deterministic per-destination counters distinguish partitions exactly.
                seq = synthetic_sequence(cfg, strategy, (pair, single), 17)
                r = result(seq, strategy)
                self.assertIn(r["OUTCOME"], ("PARTIAL_PROTOCOL_SHARED", "PROTOCOL_SHARED"))
                if r["OUTCOME"] == "PARTIAL_PROTOCOL_SHARED":
                    self.assertIn("-".join(pair), r["COMPATIBLE_PARTITIONS"])
                    self.assertEqual(r["STATE_COUNTS"], [2, 1, 0])
        seq = synthetic_sequence(cfg, "PER_DESTINATION", tuple((p,) for p in protocols), 8)
        self.assertEqual(result(seq, "PER_DESTINATION")["OUTCOME"], "PROTOCOL_ISOLATED")

    def test_partial_pair_with_changed_third_keeps_diagnostic_pair(self):
        cfg = InterprotocolConfig(("icmp", "tcp", "udp"), 4, 4)
        seq = synthetic_sequence(cfg, "SINGLE", (("icmp", "tcp"), ("udp",)), 18)
        seq[2::3] = 42
        r = result(seq, "SINGLE")
        self.assertEqual(r["OUTCOME"], "STRATEGY_CHANGED")
        self.assertIn("icmp-tcp", r["SHARED_PAIRS"])
        self.assertEqual(r["STATE_COUNTS"], [2, 0, 1])
        self.assertEqual(r["OBSERVED_STRATEGIES"][2], "CONSTANT")

    def test_multiple_partial_models_do_not_claim_a_unique_pair(self):
        seq = np.arange(48)

        def compatibility(sequence, cfg, strategy, subset):
            return len(subset) == 2

        with patch("ipid_analysis.interprotocol.compatible", side_effect=compatibility):
            r = result(seq, "SINGLE")
        self.assertEqual(r["OUTCOME"], "PARTIAL_PROTOCOL_SHARED")
        self.assertEqual(len(r["COMPATIBLE_PARTITIONS"]), 3)

    def test_base_loss_and_zero_replies_remain_in_denominator(self):
        for loss in [1, 32]:
            seq = np.full(32, 42)
            seq[:loss] = -1
            r = result(seq, "CONSTANT", ("icmp", "tcp"))
            self.assertEqual(r["OUTCOME"], "NOT_ENOUGH_SAMPLES")
            self.assertIsNone(r["RAW_CHANGED_PROTOCOLS"])

    def test_base_unclassified_counts_as_changed_and_does_not_run_mass(self):
        seq = np.random.default_rng(9).integers(0, 65536, 32)
        with patch(
            "ipid_analysis.interprotocol_outcomes.classify_batch_mass",
            side_effect=AssertionError("MASS called for BASE"),
        ):
            r = result(seq, "CONSTANT", ("icmp", "tcp"))
        self.assertEqual(r["OUTCOME"], "STRATEGY_CHANGED")
        self.assertEqual(r["OBSERVED_STRATEGIES"], ["UNCLASSIFIED"] * 2)
        self.assertEqual(r["RAW_CHANGED_PROTOCOLS"], 2)

    def test_reflection_logical_patterns_offset_wraparound_and_constant_values(self):
        for protocols in [("icmp", "tcp"), ("icmp", "tcp", "udp")]:
            p = len(protocols)
            requests = np.repeat(np.resize(PATTERN, 16), p)
            for offsets, expected in [
                ([65530] * p, "SAME_PARAMETER"),
                (list(range(p)), "DIFFERENT_PARAMETERS"),
            ]:
                r = result((requests + np.tile(offsets, 16)) % 65536, "REFLECTION", protocols)
                self.assertEqual(r["OUTCOME"], expected)
                self.assertEqual(r["PARAMETERS"], offsets)
            r = result(np.tile(np.arange(p) + 42, 16), "CONSTANT", protocols)
            self.assertEqual(r["OUTCOME"], "DIFFERENT_PARAMETERS")

    def test_mass_eighty_percent_is_checked_per_protocol(self):
        seq = np.repeat(np.tile([100, 40000], 50), 2)
        seq[np.arange(0, 40, 2)] = -1
        r = result(seq, "MULTI", ("icmp", "tcp"), "mass")
        self.assertEqual(r["RECEIVED_SAMPLES"], [80, 100])
        self.assertEqual(r["OUTCOME"], "SAME_INTER_STRATEGY")
        seq[40] = -1
        r = result(seq, "MULTI", ("icmp", "tcp"), "mass")
        self.assertEqual(r["OUTCOME"], "NOT_ENOUGH_SAMPLES")

    def test_multi_same_and_different_combined_clusters(self):
        rng = np.random.default_rng(19)
        for offsets, expected in [
            ([0, 0, 0], "SAME_INTER_STRATEGY"),
            ([0, 1365, 2730], "DIFFERENT_INTER_STRATEGY"),
        ]:
            positions = np.tile(np.arange(16), 7)[:100]
            rng.shuffle(positions)
            seq = np.array([[4096 * x + d for d in offsets] for x in positions]).ravel()
            r = result(seq, "MULTI", sample="mass")
            self.assertEqual(r["OBSERVED_STRATEGIES"], ["MULTI"] * 3)
            self.assertEqual(r["OUTCOME"], expected)

    def test_random_is_reclassified_but_joint_score_is_deferred(self):
        tables = create_candidate_null_tables(256, seed=9)
        seq = np.random.default_rng(7).integers(0, 65536, 300)
        r = result(seq, "RANDOM", sample="mass", random_null_tables=tables)
        self.assertEqual(r["OUTCOME"], "UNCHANGED")
        self.assertIsNone(r["COMBINED_MATCH"])

    def test_invalid_inputs_fail_even_for_empty_files(self):
        cfg = InterprotocolConfig(("icmp", "tcp"), 4, 4)
        for field, value in [("request_ip_ids", []), ("measurement_mode", "fixed-interval")]:
            data = snapshot(protocols=cfg.protocols)
            data[field] = value
            with self.assertRaises(ValueError):
                classify_rows([], cfg, data)
        # Two-value logical patterns are valid; no global-index aliasing.
        data = snapshot(protocols=cfg.protocols)
        data["request_ip_ids"] = [1, 2]
        prerequisites(cfg, data)

    def test_v2_campaign_streams_both_stages_and_writes_joint_transitions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ids = {}
            protocols = ("icmp", "tcp")
            for sample in ("base", "mass"):
                mid = f"interprotocol-icmp-tcp_2026-10-20_00-00-00_{sample}"
                ids[sample] = mid
                folder = root / "raw" / mid
                folder.mkdir(parents=True)
                (folder / "interprotocol.snapshot.yaml").write_text(
                    yaml.safe_dump(snapshot(sample, protocols))
                )
                rows = (
                    [raw(np.full(32, 42), "SINGLE"), raw(np.full(32, -1), "CONSTANT", "192.0.2.2")]
                    if sample == "base"
                    else []
                )
                table = (
                    pa.Table.from_pylist(rows)
                    if rows
                    else pa.table(
                        {
                            "IP_ADDR": pa.array([], pa.string()),
                            "IPID_SELECTION_STRATEGY": pa.array([], pa.string()),
                            "IPID_SEQUENCE": pa.array([], pa.string()),
                        }
                    )
                )
                pq.write_table(table, folder / "interprotocol.pq")
            manifest = {
                "version": 2,
                "campaign_id": "campaign",
                "run_id": "run",
                "groups": {
                    "icmp-tcp": {
                        "status": "complete",
                        "protocols": list(protocols),
                        "measurements": ids,
                    }
                },
            }
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest))
            output = classify_campaign(
                path,
                raw_root=root / "raw",
                output_root=root / "processed",
                figures_root=root / "figures",
            )
            report = json.loads((output / "interprotocol-campaign-summary.json").read_text())
            self.assertEqual(report["rows"], 2)
            self.assertEqual(sum(report["groups"]["icmp-tcp"]["counts"].values()), 2)
            alternatives = json.loads(
                (output / "interprotocol-campaign-single-alternatives.json").read_text()
            )["alternatives"]
            self.assertEqual(alternatives[0]["observed"], {"icmp": "CONSTANT", "tcp": "CONSTANT"})
            self.assertEqual(alternatives[0]["percent_of_baseline_group"], 100)
            self.assertEqual(
                pq.read_table(output / "interprotocol-campaign-outcomes.pq").num_rows, 2
            )
            self.assertEqual(
                len(list((root / "figures" / "campaign" / "runs" / "run").glob("*.pdf"))), 7
            )
            del manifest["groups"]["icmp-tcp"]["measurements"]["mass"]
            path.write_text(json.dumps(manifest))
            with self.assertRaises(ValueError):
                load_run_manifest(path)


if __name__ == "__main__":
    unittest.main()
