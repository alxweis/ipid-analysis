"""Alternative per-protocol labels for changed or unconfirmed target sequences."""

from collections import Counter
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.interprotocol_stability import STABILITY_SCHEMA, STABILITY_STRATEGIES


def write_alternative_reports(stability, deployments, output_dir: Path, measurements: dict):
    """Keep all protocol observations for each affected IP/group, including unchanged ones.

    Deployment rejection is not itself evidence of a different strategy. The
    alternative is always the independently reclassified per-protocol label.
    AMBIGUOUS is included because the paper deployment plot groups it with
    STRATEGY_NOT_CONFIRMED; its original label is retained in the report.
    """
    def key(row, field):
        return row["IP_ADDR"], row["PROTOCOL_GROUP"], row[field]

    deployment_by_target = {
        key(row, "IPID_SELECTION_STRATEGY"): row["DEPLOYMENT"]
        for row in deployments.to_pylist()
    }
    observations = stability.to_pylist()
    changed = {key(row, "BASELINE_STRATEGY") for row in observations
               if row["STRATEGY_STATUS"] == "CHANGED"}
    schema = STABILITY_SCHEMA.append(pa.field("DEPLOYMENT", pa.string())).append(
        pa.field("TRIGGERS", pa.list_(pa.string()))
    )
    by_strategy = {strategy: [] for strategy in STABILITY_STRATEGIES}
    for row in observations:
        target = key(row, "BASELINE_STRATEGY")
        deployment = deployment_by_target.get(target)
        triggers = []
        if target in changed:
            triggers.append("CHANGED")
        if deployment in ("STRATEGY_NOT_CONFIRMED", "AMBIGUOUS"):
            triggers.append("STRATEGY_NOT_CONFIRMED")
        if triggers:
            by_strategy[row["BASELINE_STRATEGY"]].append(
                dict(row, DEPLOYMENT=deployment, TRIGGERS=triggers)
            )
    for strategy, rows in by_strategy.items():
        stem = "interprotocol-campaign-" + strategy.lower().replace("_", "-") + "-alternatives"
        pq.write_table(pa.Table.from_pylist(rows, schema=schema),
                       output_dir / f"{stem}.pq", compression="zstd")
        groups = {}
        for group, measurement_id in measurements.items():
            selected = [row for row in rows if row["PROTOCOL_GROUP"] == group]
            transitions = Counter((r["PROTOCOL"], r["OBSERVED_STRATEGY"],
                                   r["STRATEGY_STATUS"], r["REASON"], r["DEPLOYMENT"])
                                  for r in selected)
            groups[group] = {
                "measurement_id": measurement_id,
                "affected_ips": len({r["IP_ADDR"] for r in selected}),
                "protocol_observations": len(selected),
                "trigger_ip_counts": {trigger: len({r["IP_ADDR"] for r in selected
                                                    if trigger in r["TRIGGERS"]})
                                      for trigger in ("CHANGED", "STRATEGY_NOT_CONFIRMED")},
                "alternatives": [dict(zip(
                    ("protocol", "observed_strategy", "status", "reason", "deployment", "count"),
                    (*values, count), strict=True,
                )) for values, count in sorted(transitions.items(), key=lambda item: str(item[0]))],
            }
        report = {
            "version": 1,
            "baseline_strategy": strategy,
            "unit": "IP/protocol observations; affected_ips counts unique IPs within each group",
            "selection": "All protocols of targets with CHANGED or unconfirmed deployment (including AMBIGUOUS)",
            "interpretation": "Unconfirmed shared-counter deployment can coexist with unchanged per-protocol labels",
            "trigger_counts_overlap": True,
            "classifier_metadata": {k.decode(): v.decode() for k, v in STABILITY_SCHEMA.metadata.items()},
            "groups": groups,
        }
        (output_dir / f"{stem}.json").write_text(json.dumps(report, indent=2) + "\n")
