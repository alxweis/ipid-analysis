import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.interprotocol_alternatives import write_alternative_reports
from ipid_analysis.interprotocol_stability import STABILITY_SCHEMA


class AlternativeReportTests(unittest.TestCase):
    def test_reports_preserve_protocols_triggers_and_group_boundaries(self):
        observations, deployments = [], []
        for ip, group, baseline, deployment, labels in (
            ("192.0.2.1", "icmp-tcp", "SINGLE", "STRATEGY_NOT_CONFIRMED", ("PER_BUCKET", "SINGLE")),
            ("192.0.2.1", "tcp-udp", "SINGLE", "PROTOCOL_ISOLATED", ("SINGLE", "SINGLE")),
            ("192.0.2.2", "icmp-tcp", "SINGLE", "AMBIGUOUS", ("SINGLE", "SINGLE")),
            ("192.0.2.3", "icmp-tcp", "CONSTANT", "NOT_APPLICABLE", ("RANDOM", "CONSTANT")),
        ):
            deployments.append(dict(IP_ADDR=ip, PROTOCOL_GROUP=group,
                                    IPID_SELECTION_STRATEGY=baseline, DEPLOYMENT=deployment))
            for protocol, observed in zip(group.split("-"), labels, strict=True):
                observations.append(dict(
                    IP_ADDR=ip, PROTOCOL_GROUP=group, PROTOCOL=protocol,
                    BASELINE_STRATEGY=baseline, OBSERVED_STRATEGY=observed,
                    STRATEGY_STATUS="UNCHANGED" if baseline == observed else "CHANGED",
                    REASON="CLASSIFIED", RECEIVED_SAMPLES=100, EXPECTED_SAMPLES=100,
                ))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_alternative_reports(
                pa.Table.from_pylist(observations, schema=STABILITY_SCHEMA),
                pa.Table.from_pylist(deployments), root,
                {"icmp-tcp": "measurement-one", "tcp-udp": "measurement-two"},
            )
            summary = json.loads((root / "interprotocol-campaign-single-alternatives.json").read_text())
            group = summary["groups"]["icmp-tcp"]
            self.assertEqual(group["affected_ips"], 2)
            self.assertEqual(group["protocol_observations"], 4)
            self.assertEqual(group["trigger_ip_counts"], {"CHANGED": 1, "STRATEGY_NOT_CONFIRMED": 2})
            self.assertEqual(summary["groups"]["tcp-udp"]["affected_ips"], 0)
            rows = pq.read_table(root / "interprotocol-campaign-single-alternatives.pq").to_pylist()
            self.assertEqual(len(rows), 4)
            self.assertEqual(rows[1]["OBSERVED_STRATEGY"], "SINGLE")
            self.assertEqual(rows[0]["TRIGGERS"], ["CHANGED", "STRATEGY_NOT_CONFIRMED"])
            self.assertEqual(pq.read_table(root / "interprotocol-campaign-reflection-alternatives.pq").num_rows, 0)
            self.assertEqual(pq.read_table(root / "interprotocol-campaign-constant-alternatives.pq").num_rows, 2)


if __name__ == "__main__":
    unittest.main()
