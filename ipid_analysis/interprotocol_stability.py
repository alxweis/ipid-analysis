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

from ipid_analysis.artifacts import publish_file, save_figure, write_text
from ipid_analysis.paper_figures import (
    COMPACT_PAPER_PDF_PADDING_INCHES,
    COMPACT_PAPER_STROKE_WIDTH,
    configure_compact_validation_style,
)
from ipid_analysis.runtime import temporary_path
from ipid_analysis.strategies import (
    CLASSIFIER_VERSION,
    RANDOM_STRUCTURE_SCORE_VERSION,
    STRATEGY_COLORS,
    STRATEGY_NAMES,
    STRATEGY_PRETTY,
    IPIDStrategy,
    MeasurementConfig,
    classify_batch_mass,
)

STABILITY_STRATEGIES = (
    "REFLECTION",
    "CONSTANT",
    "PER_DESTINATION",
    "SINGLE",
    "PER_BUCKET",
    "MULTI",
    "RANDOM",
)
STATUS_ORDER = ("UNCHANGED", "CHANGED", "UNCLASSIFIED", "NOT_ENOUGH_SAMPLES")
PLOT_STATUS_ORDER = ("UNCHANGED", "CHANGED", "NOT_ENOUGH_SAMPLES", "UNCLASSIFIED")
PLOT_LABELS = ("Unchanged", "Changed", "Not Enough Samples", "Unclassified")
PLOT_COLORS = tuple(
    STRATEGY_COLORS[s] for s in ("PER_BUCKET", "RANDOM", "NOT_ENOUGH_SAMPLES", "UNCLASSIFIED")
)
NON_COUNTER_STRATEGIES = ("REFLECTION", "CONSTANT", "MULTI", "RANDOM")


def style_percentage_axes(ax, fig):
    """Match the existing compact interprotocol counter figures."""
    ax.set_ylabel("Percentage [%]")
    ax.set_ylim(0, 100)
    ax.set_yticks(np.arange(0, 101, 20))
    ax.set_axisbelow(True)
    ax.grid(axis="y", color="#D9D9D9", linewidth=COMPACT_PAPER_STROKE_WIDTH)
    ax.tick_params(axis="x", pad=1.5)
    ax.legend(
        frameon=False,
        loc="lower center",
        bbox_to_anchor=(0.5, 1.01),
        ncol=4,
        columnspacing=1.0,
        handlelength=1.25,
        handletextpad=0.45,
        borderaxespad=0,
    )
    fig.subplots_adjust(left=0.095, right=0.995, bottom=0.28, top=0.79)


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
        b"interprotocol_stability_version": b"2",
        b"strategy_classifier_version": CLASSIFIER_VERSION.encode(),
        b"random_structure_score_version": RANDOM_STRUCTURE_SCORE_VERSION.encode(),
    },
)


def protocol_request_patterns(cfg, request_ip_ids):
    """Invalid classifier prerequisites are job errors, never strategy labels."""
    if (cfg.connection_count, cfg.requests_per_connection) != (4, 25):
        raise ValueError("interprotocol stability requires a 4x25 layout per protocol")
    if not request_ip_ids:
        raise ValueError("interprotocol stability requires request_ip_ids in the snapshot")
    pattern = np.asarray(request_ip_ids, dtype=np.int64)
    if pattern.ndim != 1 or (pattern < 0).any() or (pattern >= 65536).any():
        raise ValueError("request_ip_ids must be a list of valid 16-bit IPIDs")
    patterns = []
    for index, protocol in enumerate(cfg.protocols):
        positions = np.arange(index, cfg.sequence_length, len(cfg.protocols))
        selected = pattern[positions % len(pattern)]
        if np.unique(selected).size < 2:
            raise ValueError(f"request_ip_ids is constant for {protocol}; reflection is ambiguous")
        patterns.append(selected)
    return patterns


def classify_protocol_rows(rows, cfg, request_ip_ids, *, random_null_tables=None):
    """Deinterleave without compressing missing positions or resetting request IDs.

    Missing replies affect only their protocol. A classification change requires
    a complete sequence and a concrete observed class, never just rejection of
    the baseline rule. Unsupported layouts or missing metadata raise errors.
    """
    patterns = protocol_request_patterns(cfg, request_ip_ids)
    for row in rows:
        if row["IPID_SELECTION_STRATEGY"] not in STABILITY_STRATEGIES:
            raise ValueError(
                f"unsupported interprotocol baseline: {row['IPID_SELECTION_STRATEGY']}"
            )
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
        if complete.any():
            # Patterns retain GLOBAL packet positions after deinterleaving.
            config = MeasurementConfig(4, 25, patterns[protocol_index])
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
            elif label == "UNCLASSIFIED":
                status, detail = "UNCLASSIFIED", "NO_STRATEGY_CONFIRMED"
            else:
                status = "UNCHANGED" if label == baseline else "CHANGED"
                detail = "CLASSIFIED"
            output.append(
                {
                    "IP_ADDR": row["IP_ADDR"],
                    "PROTOCOL_GROUP": "-".join(cfg.protocols),
                    "PROTOCOL": protocol,
                    "BASELINE_STRATEGY": baseline,
                    "OBSERVED_STRATEGY": label,
                    "STRATEGY_STATUS": status,
                    "REASON": detail,
                    "RECEIVED_SAMPLES": int(valid[index].sum()),
                    "EXPECTED_SAMPLES": expected,
                }
            )
    return output


def _counts(table, fields):
    """Only finite scientific categories are accumulated, never IP populations."""
    counts = Counter()
    if isinstance(table, (str, Path)):
        with pq.ParquetFile(table) as parquet:
            for batch in parquet.iter_batches(batch_size=8192, columns=list(fields)):
                counts.update(tuple(row[field] for field in fields) for row in batch.to_pylist())
    else:
        for batch in table.select(list(fields)).to_batches(max_chunksize=8192):
            counts.update(tuple(row[field] for field in fields) for row in batch.to_pylist())
    return counts


def _row_count(table):
    return pq.read_metadata(table).num_rows if isinstance(table, (str, Path)) else table.num_rows


def write_stability_summary(table: pa.Table, path: Path) -> None:
    counts = _counts(
        table,
        (
            "PROTOCOL_GROUP",
            "PROTOCOL",
            "BASELINE_STRATEGY",
            "OBSERVED_STRATEGY",
            "STRATEGY_STATUS",
            "REASON",
        ),
    )
    summary = {
        "stability_version": "2",
        "strategy_classifier_version": CLASSIFIER_VERSION,
        "random_structure_score_version": RANDOM_STRUCTURE_SCORE_VERSION,
        "unit": "IP address / protocol observations",
        "rows": _row_count(table),
        "interpretation": "Observed label comparison, not proof of a causal protocol effect",
        "counts": [
            dict(
                zip(
                    [
                        "protocol_group",
                        "protocol",
                        "baseline",
                        "observed",
                        "status",
                        "reason",
                        "count",
                    ],
                    (*key, value),
                    strict=True,
                )
            )
            for key, value in sorted(counts.items(), key=lambda item: str(item[0]))
        ],
    }
    write_text(path, json.dumps(summary, indent=2) + "\n")


def classify_stability_file(raw_path, cfg, request_ip_ids, output_dir):
    protocol_request_patterns(cfg, request_ip_ids)
    output = output_dir / "interprotocol-strategy-stability.pq"
    temporary = temporary_path(output)
    with pq.ParquetWriter(temporary, STABILITY_SCHEMA, compression="zstd") as writer:
        for batch in pq.ParquetFile(raw_path).iter_batches(
            batch_size=2048, columns=["IP_ADDR", "IPID_SELECTION_STRATEGY", "IPID_SEQUENCE"]
        ):
            rows = classify_protocol_rows(batch.to_pylist(), cfg, request_ip_ids)
            writer.write_table(pa.Table.from_pylist(rows, schema=STABILITY_SCHEMA))
    publish_file(temporary, output)
    temporary.unlink(missing_ok=True)
    write_stability_summary(output, output_dir / "interprotocol-strategy-stability-summary.json")
    return output


def plot_campaign_strategy_stability(
    table: pa.Table, figure_dir: Path, groups: tuple[str, ...]
) -> list[Path]:
    """One PDF per non-counter class, comparing all four protocol intersections.

    Denominators include every IP/protocol observation of the selected baseline
    class, including losses and unclassified rows. Empty cohorts remain visible
    as N=0, without suggesting that their strategies were unchanged.
    """
    figure_dir.mkdir(parents=True, exist_ok=True)
    configure_compact_validation_style()
    counts = _counts(table, ("BASELINE_STRATEGY", "PROTOCOL_GROUP", "STRATEGY_STATUS"))
    outputs = []
    for strategy in NON_COUNTER_STRATEGIES:
        totals = [
            sum(counts[strategy, group, status] for status in PLOT_STATUS_ORDER)
            for group in groups
        ]
        fig, ax = plt.subplots(figsize=(6.5, 2.55))
        x = np.arange(len(groups)) * 0.84
        bottom = np.zeros(len(groups))
        for status, label, color in zip(PLOT_STATUS_ORDER, PLOT_LABELS, PLOT_COLORS, strict=True):
            values = np.asarray(
                [
                    100.0 * counts[strategy, group, status] / total if total else 0.0
                    for group, total in zip(groups, totals, strict=True)
                ]
            )
            ax.bar(
                x,
                values,
                bottom=bottom,
                width=0.48,
                label=label,
                color=color,
                edgecolor="white",
                linewidth=COMPACT_PAPER_STROKE_WIDTH,
            )
            bottom += values
        ax.set_xticks(
            x,
            [
                r"$\cap$".join(group.upper().split("-")) + f"\n(N={total})"
                for group, total in zip(groups, totals, strict=True)
            ],
        )
        ax.set_xlim(x[0] - 0.34, x[-1] + 0.34)
        ax.set_xlabel("Protocol Intersection (#IP / Protocol Observations)")
        style_percentage_axes(ax, fig)
        output = figure_dir / f"interprotocol-campaign-{strategy.lower()}.pdf"
        save_figure(
            fig,
            output,
            bbox_inches="tight",
            pad_inches=COMPACT_PAPER_PDF_PADDING_INCHES,
            metadata={
                "Title": f"Inter-protocol strategy stability for {STRATEGY_PRETTY[strategy]}",
                "Subject": "Baseline strategy comparison; IP/protocol observation percentages",
                "Creator": "ipid-analysis",
            },
        )
        plt.close(fig)
        outputs.append(output)
    return outputs


def plot_stability(table: pa.Table, figure_dir: Path, prefix: str) -> None:
    """Status bars and a transition matrix; all denominators are protocol observations."""
    configure_compact_validation_style()
    counts = _counts(table, ("BASELINE_STRATEGY", "STRATEGY_STATUS"))
    fig, ax = plt.subplots(figsize=(6.5, 2.55))
    bottom = np.zeros(len(STABILITY_STRATEGIES))
    for status, label, color in zip(PLOT_STATUS_ORDER, PLOT_LABELS, PLOT_COLORS, strict=True):
        totals = [
            sum(counts[s, state] for state in PLOT_STATUS_ORDER) for s in STABILITY_STRATEGIES
        ]
        values = np.asarray(
            [
                100 * counts[s, status] / total if total else 0
                for s, total in zip(STABILITY_STRATEGIES, totals, strict=True)
            ]
        )
        ax.bar(
            [STRATEGY_PRETTY[s] for s in STABILITY_STRATEGIES],
            values,
            bottom=bottom,
            label=label,
            color=color,
            width=0.48,
            edgecolor="white",
            linewidth=COMPACT_PAPER_STROKE_WIDTH,
        )
        bottom += values
    style_percentage_axes(ax, fig)
    ax.tick_params(axis="x", rotation=25)
    save_figure(
        fig,
        figure_dir / f"{prefix}-strategy-stability.pdf",
        bbox_inches="tight",
        pad_inches=COMPACT_PAPER_PDF_PADDING_INCHES,
    )
    plt.close(fig)

    labels = STRATEGY_NAMES
    transitions = _counts(table, ("BASELINE_STRATEGY", "OBSERVED_STRATEGY"))
    matrix = np.asarray(
        [[transitions[s, label] for label in labels] for s in STABILITY_STRATEGIES]
    )
    fig, ax = plt.subplots(figsize=(6.5, 3.2))
    image = ax.imshow(matrix, cmap="Blues", aspect="auto")
    ax.set_yticks(
        range(len(STABILITY_STRATEGIES)), [STRATEGY_PRETTY[s] for s in STABILITY_STRATEGIES]
    )
    ax.set_xticks(
        range(len(labels)), [STRATEGY_PRETTY[s] for s in labels], rotation=40, ha="right"
    )
    ax.tick_params(axis="both", labelsize=7)
    ax.set_xlabel("Observed strategy under interleaving")
    ax.set_ylabel("Baseline strategy")
    fig.colorbar(image, ax=ax, label="IP / protocol observations")
    fig.tight_layout()
    save_figure(
        fig,
        figure_dir / f"{prefix}-strategy-transitions.pdf",
        bbox_inches="tight",
        pad_inches=COMPACT_PAPER_PDF_PADDING_INCHES,
    )
    plt.close(fig)
