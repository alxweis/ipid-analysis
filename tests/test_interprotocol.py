from collections import Counter
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.interprotocol import (
    InterprotocolConfig,
    _strategy_deployment_percentages,
    _strategy_group_labels,
    build_target_files,
    classify_campaign,
    classify_file,
    classify_sequence,
    plot_campaign_strategy_deployments,
    synthetic_sequence,
    validate_classifier,
)
from ipid_analysis.strategy_merge import iter_strategy_merges


class InterprotocolClassifierTests(unittest.TestCase):
    def test_pair_shared_and_isolated(self):
        cfg = InterprotocolConfig(("icmp", "tcp"), 4, 4)
        for strategy in ("SINGLE", "PER_DESTINATION", "PER_BUCKET"):
            with self.subTest(strategy=strategy, deployment="shared"):
                sequence = synthetic_sequence(cfg, strategy, (cfg.protocols,), 1)
                self.assertEqual(classify_sequence(sequence, cfg, strategy), "SHARED_ICMP_TCP")
            with self.subTest(strategy=strategy, deployment="isolated"):
                sequence = synthetic_sequence(cfg, strategy, (("icmp",), ("tcp",)), 2)
                self.assertEqual(classify_sequence(sequence, cfg, strategy), "PROTOCOL_ISOLATED")

    def test_triple_partial_sharing(self):
        cfg = InterprotocolConfig(("icmp", "tcp", "udp"), 4, 4)
        for pair, singleton in (
            (("icmp", "tcp"), "udp"),
            (("icmp", "udp"), "tcp"),
            (("tcp", "udp"), "icmp"),
        ):
            sequence = synthetic_sequence(cfg, "SINGLE", (pair, (singleton,)), 10)
            expected = "SHARED_" + "_".join(protocol.upper() for protocol in pair)
            self.assertEqual(classify_sequence(sequence, cfg, "SINGLE"), expected)

    def test_missing_reply_is_not_enough_samples(self):
        cfg = InterprotocolConfig(("icmp", "udp"), 4, 4)
        sequence = synthetic_sequence(cfg, "SINGLE", (cfg.protocols,), 3)
        sequence[5] = -1
        self.assertEqual(classify_sequence(sequence, cfg, "SINGLE"), "NOT_ENOUGH_SAMPLES")

    def test_validation_report_has_perfect_synthetic_accuracy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            processed = root / "processed"
            figures = root / "figures"
            output = validate_classifier(processed, figures, samples=10, seed=42)
            report = json.loads(output.read_text())
            self.assertEqual(report["correct"], report["cases"])
            self.assertEqual(report["accuracy"], 1.0)
            self.assertEqual(output.parent, processed)
            self.assertTrue((figures / "interprotocol-validation-pair-confusion.pdf").is_file())
            self.assertTrue((figures / "interprotocol-validation-triple-confusion.pdf").is_file())
            self.assertFalse(any(processed.glob("*.pdf")))
            self.assertFalse(any(figures.glob("*.json")))

    def test_target_builder_uses_same_strategy_and_exclusive_triple(self):
        rows = {
            "icmp": [
                ("1.1.1.1", "SINGLE"),
                ("2.2.2.2", "PER_DESTINATION"),
                ("3.3.3.3", "PER_BUCKET"),
            ],
            "tcp": [("1.1.1.1", "SINGLE"), ("2.2.2.2", "SINGLE"), ("3.3.3.3", "PER_BUCKET")],
            "udp": [("1.1.1.1", "SINGLE"), ("2.2.2.2", "SINGLE")],
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            processed = root / "processed"
            manifests = {}
            for protocol, protocol_rows in rows.items():
                manifest = {
                    protocol: {
                        "zmap": f"{protocol}-campaign",
                        "ipid": {
                            "no-connection": {
                                "rt-based": {"base": f"{protocol}-base"},
                                "fixed-interval": {"mass": f"{protocol}-mass"},
                            }
                        },
                    }
                }
                manifest_path = root / f"{protocol}.json"
                manifest_path.write_text(json.dumps(manifest))
                manifests[protocol] = manifest_path.name
                strategy_path = iter_strategy_merges(manifest)[0].artifact_path(
                    processed, "strategies"
                )
                strategy_path.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(
                    pa.table(
                        {
                            "IP_ADDR": [row[0] for row in protocol_rows],
                            "IPID_SELECTION_STRATEGY": [row[1] for row in protocol_rows],
                        }
                    ),
                    strategy_path,
                )
            campaign = root / "campaign.json"
            campaign.write_text(json.dumps({"campaign_id": "test", "manifests": manifests}))
            figures = root / "figures"
            output = build_target_files(campaign, processed_root=processed, figures_root=figures)
            triple = pq.read_table(output / "icmp-tcp-udp-targets.pq").to_pylist()
            tcp_udp = pq.read_table(output / "tcp-udp-targets.pq").to_pylist()
            icmp_tcp = pq.read_table(output / "icmp-tcp-targets.pq").to_pylist()
            self.assertEqual([row["IP_ADDR"] for row in triple], ["1.1.1.1"])
            self.assertEqual([row["IP_ADDR"] for row in tcp_udp], ["2.2.2.2"])
            self.assertEqual([row["IP_ADDR"] for row in icmp_tcp], ["3.3.3.3"])
            self.assertTrue((figures / "test" / "target-population.pdf").is_file())
            self.assertFalse((output / "target-population.pdf").exists())

    def test_classify_file_writes_parquet_summary_and_plot(self):
        cfg = InterprotocolConfig(("icmp", "tcp"), 4, 4)
        sequence = synthetic_sequence(cfg, "SINGLE", (cfg.protocols,), 12)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "interprotocol.pq"
            snapshot = root / "interprotocol.snapshot.yaml"
            output = root / "result"
            figures = root / "figures"
            pq.write_table(
                pa.table(
                    {
                        "IP_ADDR": ["1.1.1.1"],
                        "IPID_SELECTION_STRATEGY": ["SINGLE"],
                        "IPID_SEQUENCE": [",".join(map(str, sequence))],
                    }
                ),
                raw,
            )
            snapshot.write_text(
                "connection_count: 4\nrequests_per_connection: 4\n"
                "interprotocol:\n  protocols: [icmp, tcp]\n"
            )
            result = classify_file(raw, snapshot, output, figure_dir=figures)
            self.assertTrue(result.is_file())
            self.assertTrue((output / "interprotocol-summary.json").is_file())
            self.assertTrue((figures / "interprotocol-deployments.pdf").is_file())
            self.assertFalse((output / "interprotocol-deployments.pdf").exists())

    def test_campaign_analysis_classifies_and_combines_groups(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw_root = root / "raw"
            groups = {}
            for group, protocols in (
                ("icmp-tcp", ("icmp", "tcp")),
                ("icmp-tcp-udp", ("icmp", "tcp", "udp")),
            ):
                measurement_id = f"interprotocol-{group}_2026-01-01_00-00-00"
                measurement = raw_root / measurement_id
                measurement.mkdir(parents=True)
                cfg = InterprotocolConfig(protocols, 4, 4)
                sequence = synthetic_sequence(cfg, "SINGLE", (protocols,), 12)
                pq.write_table(
                    pa.table(
                        {
                            "IP_ADDR": [f"192.0.2.{len(groups) + 1}"],
                            "IPID_SELECTION_STRATEGY": ["SINGLE"],
                            "IPID_SEQUENCE": [",".join(map(str, sequence))],
                        }
                    ),
                    measurement / "interprotocol.pq",
                )
                (measurement / "interprotocol.snapshot.yaml").write_text(
                    "connection_count: 4\nrequests_per_connection: 4\n"
                    f"interprotocol:\n  protocols: [{', '.join(protocols)}]\n"
                )
                groups[group] = {
                    "protocols": list(protocols),
                    "status": "complete",
                    "measurement_id": measurement_id,
                }
            manifest = root / "run.json"
            manifest.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "campaign_id": "campaign",
                        "run_id": "campaign-2026-01-01",
                        "groups": groups,
                    }
                )
            )
            output = classify_campaign(
                manifest,
                raw_root=raw_root,
                output_root=root / "processed",
                figures_root=root / "figures",
            )
            combined = pq.read_table(output / "interprotocol-campaign-deployments.pq")
            summary = json.loads((output / "interprotocol-campaign-summary.json").read_text())
            self.assertEqual(combined.num_rows, 2)
            self.assertEqual(summary["rows"], 2)
            self.assertEqual(set(summary["groups"]), set(groups))
            figure_dir = root / "figures" / "campaign" / "runs" / "campaign-2026-01-01"
            self.assertTrue((figure_dir / "interprotocol-campaign-deployments.pdf").is_file())
            self.assertTrue((figure_dir / "interprotocol-campaign-missing.pdf").is_file())
            for strategy in ("per-destination", "single", "per-bucket"):
                self.assertTrue((figure_dir / f"interprotocol-campaign-{strategy}.pdf").is_file())
            self.assertFalse((output / "interprotocol-campaign-deployments.pdf").exists())

    def test_strategy_plots_aggregate_all_shared_partitions(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            counts = {
                "icmp-tcp-udp": Counter(
                    {
                        ("SINGLE", "SHARED_ICMP_TCP_UDP"): 6,
                        ("SINGLE", "SHARED_ICMP_UDP"): 2,
                        ("SINGLE", "PROTOCOL_ISOLATED"): 1,
                        ("SINGLE", "AMBIGUOUS"): 1,
                    }
                )
            }
            paths = plot_campaign_strategy_deployments(counts, output)
            self.assertEqual(len(paths), 3)
            self.assertTrue(all(path.is_file() for path in paths))
            percentages = _strategy_deployment_percentages(counts, "SINGLE")
            triple = percentages["icmp-tcp-udp"]
            self.assertEqual(triple["PROTOCOL_SHARED"], 80.0)
            self.assertEqual(triple["PROTOCOL_ISOLATED"], 10.0)
            self.assertEqual(triple["STRATEGY_NOT_CONFIRMED"], 10.0)
            self.assertEqual(
                _strategy_group_labels(counts, "SINGLE")[-1],
                "ICMP$\\cap$TCP$\\cap$UDP\n(10)",
            )


if __name__ == "__main__":
    unittest.main()
