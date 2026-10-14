"""Compare each protocol's interleaved replies with its baseline strategy.

This is an observational comparison, separate from shared-counter inference.
The production mass classifier is reused with its required 4x25 layout.
"""

from collections import Counter
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.strategies import (
    CLASSIFIER_VERSION,
    IPIDStrategy,
    MeasurementConfig,
    RANDOM_STRUCTURE_SCORE_VERSION,
    STRATEGY_NAMES,
    classify_batch_mass,
)

STABILITY_STRATEGIES = (
    "REFLECTION", "CONSTANT", "PER_DESTINATION", "SINGLE", "PER_BUCKET", "MULTI", "RANDOM"
)
STATUS_ORDER = ("UNCHANGED", "CHANGED", "UNCLASSIFIED", "NOT_ENOUGH_SAMPLES", "NOT_EVALUATED")
STABILITY_SCHEMA = pa.schema(
    [
        ("IP_ADDR", pa.string()),
        ("PROTOCOL_GROUP", pa.string()),
        ("PROTOCOL", pa.string()),
        ("BASELINE_STRATEGY", pa.string()),
        ("OBSERVED_STRATEGY", pa.string()),
        ("STRATEGY_STATUS", pa.string()),
        ("REASON", pa.string()),
        ("RECEIVED_SAMPLES", pa.int32()),
        ("EXPECTED_SAMPLES", pa.int32()),
    ],
    metadata={
        b"interprotocol_stability_version": b"1",
        b"strategy_classifier_version": CLASSIFIER_VERSION.encode(),
        b"random_structure_score_version": RANDOM_STRUCTURE_SCORE_VERSION.encode(),
    },
)


def classify_protocol_rows(rows, cfg, request_ip_ids, *, random_null_tables=None):
    """Deinterleave without compressing missing positions or resetting request IDs.

    Missing replies affect only their protocol. A classification change requires
    a complete sequence and a concrete observed class, never just rejection of
    the baseline rule. Short historical runs remain readable but unevaluated.
    """
    if not rows:
        return []
    sequences = [
        [int(item) if item != "-" else -1 for item in row["IPID_SEQUENCE"].split(",")]
        for row in rows
    ]
    correct_length = np.asarray([len(s) == cfg.sequence_length for s in sequences])
    values = np.full((len(rows), cfg.sequence_length), -1, dtype=np.int64)
    for index, sequence in enumerate(sequences):
        if correct_length[index]:
            values[index] = sequence
    output = []
    protocol_count = len(cfg.protocols)
    expected = cfg.connection_count * cfg.requests_per_connection
    for protocol_index, protocol in enumerate(cfg.protocols):
        observed = values[:, protocol_index::protocol_count]
        valid = (observed >= 0) & (observed < 65536)
        complete = correct_length & valid.all(axis=1)
        codes = np.full(len(rows), int(IPIDStrategy.NOT_ENOUGH_SAMPLES), dtype=np.int8)
        reason = None
        if (cfg.connection_count, cfg.requests_per_connection) != (4, 25):
            reason = "REQUIRES_4X25_LAYOUT"
        elif not request_ip_ids:
            reason = "MISSING_REQUEST_IP_IDS"
        else:
            # Packet generation indexes the pattern by the GLOBAL sequence
            # number, not the position within a single protocol's subsequence.
            positions = np.arange(protocol_index, cfg.sequence_length, protocol_count)
            pattern = np.asarray(request_ip_ids, dtype=np.int64)
            pattern = pattern[positions % len(pattern)]
            if (pattern < 0).any() or (pattern >= 65536).any() or np.unique(pattern).size < 2:
                reason = "INVALID_OR_CONSTANT_REQUEST_PATTERN"
            elif complete.any():
                config = MeasurementConfig(4, 25, pattern)
                codes[complete] = classify_batch_mass(
                    pa.array(observed[complete].tolist(), type=pa.list_(pa.int64())),
                    config,
                    random_null_tables=random_null_tables,
                )
        for index, row in enumerate(rows):
            baseline = row["IPID_SELECTION_STRATEGY"]
            label = STRATEGY_NAMES[int(codes[index])]
            if not complete[index]:
                status, detail = "NOT_ENOUGH_SAMPLES", "INCOMPLETE_OR_INVALID_SEQUENCE"
            elif baseline not in STABILITY_STRATEGIES:
                status, detail, label = "NOT_EVALUATED", "UNSUPPORTED_BASELINE", None
            elif reason:
                status, detail, label = "NOT_EVALUATED", reason, None
            elif label == "UNCLASSIFIED":
                status, detail = "UNCLASSIFIED", "NO_STRATEGY_CONFIRMED"
            else:
                status = "UNCHANGED" if label == baseline else "CHANGED"
                detail = "CLASSIFIED"
            output.append({
                "IP_ADDR": row["IP_ADDR"],
                "PROTOCOL_GROUP": "-".join(cfg.protocols),
                "PROTOCOL": protocol,
                "BASELINE_STRATEGY": baseline,
                "OBSERVED_STRATEGY": label,
                "STRATEGY_STATUS": status,
                "REASON": detail,
                "RECEIVED_SAMPLES": int(valid[index].sum()),
                "EXPECTED_SAMPLES": expected,
            })
    return output


def write_stability_summary(table: pa.Table, path: Path) -> None:
    counts = Counter(
        (row["PROTOCOL_GROUP"], row["PROTOCOL"], row["BASELINE_STRATEGY"],
         row["OBSERVED_STRATEGY"], row["STRATEGY_STATUS"], row["REASON"])
        for row in table.to_pylist()
    )
    summary = {
        "stability_version": "1",
        "strategy_classifier_version": CLASSIFIER_VERSION,
        "random_structure_score_version": RANDOM_STRUCTURE_SCORE_VERSION,
        "unit": "IP address / protocol observations",
        "rows": table.num_rows,
        "interpretation": "Observed label comparison, not proof of a causal protocol effect",
        "counts": [dict(zip(
            ["protocol_group", "protocol", "baseline", "observed", "status", "reason", "count"],
            (*key, value), strict=True,
        )) for key, value in sorted(counts.items(), key=lambda item: str(item[0]))],
    }
    path.write_text(json.dumps(summary, indent=2) + "\n")


def classify_stability_file(raw_path, cfg, request_ip_ids, output_dir):
    output = output_dir / "interprotocol-strategy-stability.pq"
    temporary = output.with_suffix(".pq.part")
    with pq.ParquetWriter(temporary, STABILITY_SCHEMA, compression="zstd") as writer:
        for batch in pq.ParquetFile(raw_path).iter_batches(
            batch_size=2048, columns=["IP_ADDR", "IPID_SELECTION_STRATEGY", "IPID_SEQUENCE"]
        ):
            rows = classify_protocol_rows(batch.to_pylist(), cfg, request_ip_ids)
            writer.write_table(pa.Table.from_pylist(rows, schema=STABILITY_SCHEMA))
    temporary.replace(output)
    write_stability_summary(
        pq.read_table(output), output_dir / "interprotocol-strategy-stability-summary.json"
    )
    return output


def plot_stability(table: pa.Table, figure_dir: Path, prefix: str) -> None:
    """Status bars and a transition matrix; all denominators are protocol observations."""
    rows = table.to_pylist()
    counts = Counter((r["BASELINE_STRATEGY"], r["STRATEGY_STATUS"]) for r in rows)
    fig, ax = plt.subplots(figsize=(9, 3.6))
    bottom = np.zeros(len(STABILITY_STRATEGIES))
    for status in STATUS_ORDER:
        totals = [sum(counts[s, state] for state in STATUS_ORDER) for s in STABILITY_STRATEGIES]
        values = np.asarray([
            100 * counts[s, status] / total if total else 0
            for s, total in zip(STABILITY_STRATEGIES, totals, strict=True)
        ])
        ax.bar(STABILITY_STRATEGIES, values, bottom=bottom, label=status)
        bottom += values
    ax.set_ylabel("IP / protocol observations [%]")
    ax.set_ylim(0, 100)
    ax.tick_params(axis="x", rotation=25)
    ax.legend(fontsize=7, loc="upper left", bbox_to_anchor=(1.01, 1))
    fig.tight_layout()
    fig.savefig(figure_dir / f"{prefix}-strategy-stability.pdf")
    plt.close(fig)

    labels = STRATEGY_NAMES + ["NOT_EVALUATED"]
    transitions = Counter((r["BASELINE_STRATEGY"], r["OBSERVED_STRATEGY"] or "NOT_EVALUATED")
                          for r in rows)
    matrix = np.asarray([[transitions[s, label] for label in labels] for s in STABILITY_STRATEGIES])
    fig, ax = plt.subplots(figsize=(10, 4.5))
    image = ax.imshow(matrix, cmap="Blues", aspect="auto")
    ax.set_yticks(range(len(STABILITY_STRATEGIES)), STABILITY_STRATEGIES)
    ax.set_xticks(range(len(labels)), labels, rotation=40, ha="right")
    ax.set_xlabel("Observed strategy under interleaving")
    ax.set_ylabel("Baseline strategy")
    fig.colorbar(image, ax=ax, label="IP / protocol observations")
    fig.tight_layout()
    fig.savefig(figure_dir / f"{prefix}-strategy-transitions.pdf")
    plt.close(fig)
