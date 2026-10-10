"""Reproducible synthetic validation of the IP-ID strategy classifier.

The generated sequences use the same flattened order as ``ipid-measure``:
request round first, then connection index.  For four connections this is
``c0, c1, c2, c3, c0, c1, ...``; even/odd positions alternate the two source
addresses.  Sequences and timestamps are serialized exactly like ``ipid.pq``.
"""

from __future__ import annotations

from datetime import datetime, timezone
import ipaddress
import json
from pathlib import Path

import matplotlib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.transforms import ScaledTranslation

from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.paper_figures import (
    COMPACT_PAPER_PDF_PADDING_INCHES,
    COMPACT_PAPER_STROKE_WIDTH,
    PERCENTAGE_CMAP,
    configure_compact_validation_style,
    draw_percentage_colorbar_axis,
)
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_GAP_NULL_TABLE_SEED_OFFSET,
    CANDIDATE_INCREMENT_BIN_COUNTS,
    CANDIDATE_INCREMENT_MIN_TRANSITIONS,
    CANDIDATE_INCREMENT_NULL_TABLE_SEED_OFFSET,
    CANDIDATE_INCREMENT_SUBSEQUENCE_AGGREGATION,
    CANDIDATE_INCREMENT_TARGET_EXPECTED_PER_BIN,
    CANDIDATE_NULL_TABLE_SAMPLES,
    CANDIDATE_NULL_TABLE_SEED,
    CANDIDATE_NULL_TABLE_VERSION,
    CANDIDATE_RANDOM_METRICS,
    CANDIDATE_RANDOM_MIN_SCORE,
    CANDIDATE_RANDOM_SCORE_VERSION,
    CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE,
    CandidateNullTables,
    create_candidate_null_tables,
)
from ipid_analysis.strategies import (
    CLASSIFIER_VERSION,
    LEGACY_RANDOM_STRUCTURE_MIN_SCORE,
    LEGACY_RANDOM_STRUCTURE_SCORE_VERSION,
    MAX_INC,
    MULTI_MAX_CLUSTERS,
    MULTI_MAX_INC,
    STRATEGY_PRETTY,
    IPIDStrategy,
    MeasurementConfig,
    classify_batch,
    classify_batch_mass,
    legacy_random_structure_scores,
)

app = typer.Typer()

MODULUS = 1 << 16
CONNECTION_COUNT = 4
RT_REQUESTS_PER_CONNECTION = 4
FIXED_REQUESTS_PER_CONNECTION = 25
DEFAULT_SAMPLES_PER_STRATEGY = 100_000
TRIVIAL_SAMPLES_PER_STRATEGY = 1_000
REQUEST_IP_IDS = np.asarray([18933, 18932, 3717, 3718, 3719], dtype=np.int64)
FIXED_CONFIG = MeasurementConfig(
    connection_count=CONNECTION_COUNT,
    requests_per_connection=FIXED_REQUESTS_PER_CONNECTION,
    request_ip_ids=REQUEST_IP_IDS,
)

RT_DATASET = "base-4x4-ideal"
BASE_REORDERED_3_DATASET = "base-4x4-reordered-3"
BASE_REORDERED_4_DATASET = "base-4x4-reordered-4"
RT_OUT_OF_SCOPE_DATASET = "base-4x4-out-of-scope"
FIXED_IDEAL_DATASET = "mass-4x25-ideal"
FIXED_LOSSY_DATASET = "mass-4x25-lossy"
MASS_REORDERED_DATASET = "mass-4x25-reordered"
FIXED_REORDERED_DATASET = "mass-4x25-lossy-reordered"

RT_STRATEGIES = (
    "REFLECTION",
    "CONSTANT",
    "SINGLE",
    "PER_CONNECTION",
    "PER_DESTINATION",
    "PER_BUCKET",
)
RT_DETECTED_STRATEGIES = (*RT_STRATEGIES, "UNCLASSIFIED")
FIXED_STRATEGIES = (*RT_STRATEGIES, "MULTI", "RANDOM")
FIXED_DETECTED_STRATEGIES = (*FIXED_STRATEGIES, "UNCLASSIFIED")
FIXED_IMPAIRED_STRATEGIES = FIXED_STRATEGIES
FIXED_IMPAIRED_DETECTED_STRATEGIES = (*FIXED_IMPAIRED_STRATEGIES, "UNCLASSIFIED")
RT_OUT_OF_SCOPE_STRATEGIES = ("MULTI", "RANDOM")

# Physical dimensions are fixed across all classifier-validation confusion
# matrices so layouts remain comparable when placed in a two-column paper.
CONFUSION_FIGURE_WIDTH_INCHES = 7.0
CONFUSION_CELL_WIDTH_INCHES = 2.50 / 9.0
CONFUSION_CELL_HEIGHT_INCHES = 1.40 / 8.0
CONFUSION_COLORBAR_GAP_INCHES = 0.10
CONFUSION_COLORBAR_WIDTH_INCHES = 0.065
CONFUSION_HORIZONTAL_PANEL_GAP_INCHES = 0.25
CONFUSION_VERTICAL_PANEL_GAP_INCHES = 0.40
CONFUSION_TITLE_GAP_INCHES = 0.05
CONFUSION_XLABEL_GAP_INCHES = 0.60
CONFUSION_YLABEL_GAP_INCHES = 0.93
# Linux Libertine's visible ink sits slightly below the geometric text anchor.
# Apply the same measured optical correction to cell values and colorbar ticks.
CONFUSION_NUMERIC_TEXT_UPWARD_OFFSET_POINTS = 0.15
TRIVIAL_STRATEGIES = frozenset({"REFLECTION", "CONSTANT"})
SYNTHETIC_GENERATOR_PARAMETERS = {
    "sampling": "independent discrete uniform unless fixed by the strategy",
    "ip_id_range_inclusive": [0, MODULUS - 1],
    "wraparound": f"modulo {MODULUS}",
    "REFLECTION": {"offset_range_inclusive": [0, MODULUS - 1]},
    "CONSTANT": {"value_range_inclusive": [0, MODULUS - 1]},
    "SINGLE": {
        "start_range_inclusive": [0, MODULUS - 1],
        "increment_range_inclusive": [1, MAX_INC],
    },
    "PER_DESTINATION": {
        "start_range_inclusive": [0, MODULUS - 1],
        "increment": 1,
    },
    "PER_CONNECTION": {
        "start_range_inclusive": [0, MODULUS - 1],
        "increment": 1,
    },
    "PER_BUCKET": {
        "start_range_inclusive": [0, MODULUS - 1],
        "increment_range_inclusive": [1, MAX_INC],
    },
    "MULTI": {
        "cluster_count_range_inclusive": [2, MULTI_MAX_CLUSTERS],
        "cluster_start_range_inclusive": [0, MODULUS - 1],
        "within_cluster_offset_range_inclusive": [0, MULTI_MAX_INC],
    },
    "RANDOM": {"value_range_inclusive": [0, MODULUS - 1]},
}

VALIDATION_SCHEMA = pa.schema(
    [
        ("DATASET", pa.string()),
        ("SAMPLE_ID", pa.string()),
        ("IP_ADDR", pa.string()),
        ("CONNECTION_COUNT", pa.int16()),
        ("REQUESTS_PER_CONNECTION", pa.int16()),
        ("GENERATOR_STRATEGY", pa.string()),
        ("EXPECTED_STRATEGY", pa.string()),
        ("DETECTED_STRATEGY", pa.string()),
        ("IPID_SEQUENCE", pa.string()),
        ("SEND_TIMESTAMP_SEQUENCE", pa.string()),
        ("RECEIVE_TIMESTAMP_SEQUENCE", pa.string()),
        ("LOSS_COUNT", pa.int16()),
        ("REORDERED_COUNT", pa.int16()),
    ]
)


def _round_connection_flatten(values: np.ndarray) -> np.ndarray:
    """Flatten ``(sample, request round, connection)`` like ipid-measure."""
    return values.reshape(values.shape[0], -1).astype(np.uint16)


def _cumulative_sequences(
    starts: np.ndarray,
    increments: np.ndarray,
) -> np.ndarray:
    """Build modular sequences from one start and per-step increments."""
    cumulative = np.cumsum(increments, axis=1, dtype=np.int64)
    values = np.concatenate([starts[:, None], starts[:, None] + cumulative], axis=1)
    return (values % MODULUS).astype(np.uint16)


def _generate_multi_sequences(
    sample_count: int,
    sequence_length: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Draw 2..MULTI_MAX_CLUSTERS circular clusters over the full IP-ID space."""
    cluster_counts = rng.integers(
        2,
        MULTI_MAX_CLUSTERS + 1,
        size=sample_count,
    )
    sequences = np.empty((sample_count, sequence_length), dtype=np.uint16)
    minimum_start_gap = 2 * MULTI_MAX_INC + 1

    for cluster_count in range(2, MULTI_MAX_CLUSTERS + 1):
        rows = np.flatnonzero(cluster_counts == cluster_count)
        if not len(rows):
            continue

        extra_gap = MODULUS - cluster_count * minimum_start_gap
        weights = rng.exponential(size=(len(rows), cluster_count))
        scaled = weights / weights.sum(axis=1, keepdims=True) * extra_gap
        extras = np.floor(scaled).astype(np.int64)
        remainder = extra_gap - extras.sum(axis=1)
        fractional_order = np.argsort(scaled - extras, axis=1)[:, ::-1]
        extras[
            np.arange(len(rows))[:, None],
            fractional_order,
        ] += np.arange(cluster_count)[None, :] < remainder[:, None]
        gaps = minimum_start_gap + extras

        phases = rng.integers(0, MODULUS, size=len(rows), dtype=np.int64)
        starts = (
            np.concatenate(
                [
                    phases[:, None],
                    phases[:, None] + np.cumsum(gaps[:, :-1], axis=1, dtype=np.int64),
                ],
                axis=1,
            )
            % MODULUS
        )

        labels = rng.integers(
            0,
            cluster_count,
            size=(len(rows), sequence_length),
        )
        labels[:, :cluster_count] = np.arange(cluster_count)
        labels = np.take_along_axis(
            labels,
            np.argsort(rng.random(labels.shape), axis=1),
            axis=1,
        )
        offsets = rng.integers(
            0,
            MULTI_MAX_INC + 1,
            size=(len(rows), sequence_length),
            dtype=np.int64,
        )
        values = starts[np.arange(len(rows))[:, None], labels] + offsets
        sequences[rows] = (values % MODULUS).astype(np.uint16)

    return sequences


def _generate_exact_sequences(
    samples_per_strategy: int,
    requests_per_connection: int,
    rng: np.random.Generator,
) -> tuple[MeasurementConfig, dict[str, np.ndarray]]:
    """Generate complete sequences for the shared exact strategy rules."""
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    n = samples_per_strategy
    length = CONNECTION_COUNT * requests_per_connection
    config = MeasurementConfig(
        connection_count=CONNECTION_COUNT,
        requests_per_connection=requests_per_connection,
        request_ip_ids=REQUEST_IP_IDS,
    )
    request_pattern = REQUEST_IP_IDS[np.arange(length) % len(REQUEST_IP_IDS)]

    reflection_offsets = rng.integers(0, MODULUS, size=n, dtype=np.int64)
    reflection = ((request_pattern[None, :] + reflection_offsets[:, None]) % MODULUS).astype(
        np.uint16
    )

    constant_values = rng.integers(0, MODULUS, size=n, dtype=np.uint16)
    constant = np.repeat(constant_values[:, None], length, axis=1)

    destination_starts = rng.integers(
        0,
        MODULUS,
        size=(n, 2),
        dtype=np.int64,
    )
    per_destination = np.empty((n, length), dtype=np.uint16)
    destination_steps = np.arange(length // 2, dtype=np.int64)
    per_destination[:, 0::2] = (destination_starts[:, 0, None] + destination_steps) % MODULUS
    per_destination[:, 1::2] = (destination_starts[:, 1, None] + destination_steps) % MODULUS

    connection_starts = rng.integers(
        0,
        MODULUS,
        size=(n, CONNECTION_COUNT),
        dtype=np.int64,
    )
    per_connection_cube = (
        connection_starts[:, None, :]
        + np.arange(requests_per_connection, dtype=np.int64)[None, :, None]
    ) % MODULUS
    per_connection = _round_connection_flatten(per_connection_cube)

    single_starts = rng.integers(0, MODULUS, size=n, dtype=np.int64)
    single_increments = rng.integers(
        1,
        MAX_INC + 1,
        size=(n, length - 1),
        dtype=np.int64,
    )
    single = _cumulative_sequences(single_starts, single_increments)

    bucket_starts = rng.integers(
        0,
        MODULUS,
        size=(n, CONNECTION_COUNT),
        dtype=np.int64,
    )
    bucket_increments = rng.integers(
        1,
        MAX_INC + 1,
        size=(n, CONNECTION_COUNT, requests_per_connection - 1),
        dtype=np.int64,
    )
    bucket_connections = np.concatenate(
        [
            bucket_starts[:, :, None],
            bucket_starts[:, :, None] + np.cumsum(bucket_increments, axis=2, dtype=np.int64),
        ],
        axis=2,
    )
    per_bucket = _round_connection_flatten(bucket_connections.transpose(0, 2, 1) % MODULUS)

    return config, {
        "REFLECTION": reflection,
        "CONSTANT": constant,
        "SINGLE": single,
        "PER_CONNECTION": per_connection,
        "PER_DESTINATION": per_destination,
        "PER_BUCKET": per_bucket,
    }


def generate_rt_sequences(
    samples_per_strategy: int,
    rng: np.random.Generator,
) -> tuple[MeasurementConfig, dict[str, np.ndarray]]:
    """Generate balanced ideal 4x4 sequences for RT-based classification."""
    return _generate_exact_sequences(
        samples_per_strategy,
        RT_REQUESTS_PER_CONNECTION,
        rng,
    )


def generate_rt_out_of_scope_sequences(
    samples_per_strategy: int,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """Generate MULTI-like sequences that RT-based analysis must reject."""
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    n = samples_per_strategy
    length = CONNECTION_COUNT * RT_REQUESTS_PER_CONNECTION
    return {
        "MULTI": _generate_multi_sequences(n, length, rng),
        "RANDOM": rng.integers(0, MODULUS, size=(n, length), dtype=np.uint16),
    }


def generate_fixed_sequences(
    samples_per_strategy: int,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """Generate balanced ideal 4x25 sequences for fixed-interval classification."""
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    n = samples_per_strategy
    length = CONNECTION_COUNT * FIXED_REQUESTS_PER_CONNECTION

    _, sequences = _generate_exact_sequences(
        samples_per_strategy,
        FIXED_REQUESTS_PER_CONNECTION,
        rng,
    )
    multi = _generate_multi_sequences(n, length, rng)
    random = rng.integers(
        0,
        MODULUS,
        size=(n, length),
        dtype=np.uint16,
    )

    return {**sequences, "MULTI": multi, "RANDOM": random}


def apply_fixed_interval_impairments(
    ideal: np.ndarray,
    rng: np.random.Generator,
    *,
    loss_fraction: float = 0.20,
    reorder_fraction: float = 0.20,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return a loss mask plus lossy and partially reordered IP-ID values."""
    if ideal.ndim != 2:
        raise ValueError("ideal sequences must be a two-dimensional matrix")

    sample_count, sequence_length = ideal.shape
    loss_count = round(sequence_length * loss_fraction)
    present_count = sequence_length - loss_count
    reordered_count = round(present_count * reorder_fraction)
    loss_mask = np.zeros((sample_count, sequence_length), dtype=bool)
    reordered = ideal.copy()

    for row_index in range(sample_count):
        missing = rng.choice(sequence_length, size=loss_count, replace=False)
        loss_mask[row_index, missing] = True
        present = np.flatnonzero(~loss_mask[row_index])
        selected = rng.choice(present, size=reordered_count, replace=False)
        permutation = rng.permutation(reordered[row_index, selected])
        if reordered_count > 1 and np.array_equal(permutation, reordered[row_index, selected]):
            permutation = np.roll(permutation, 1)
        reordered[row_index, selected] = permutation

    return loss_mask, ideal.copy(), reordered


def apply_reordering(
    ideal: np.ndarray,
    rng: np.random.Generator,
    *,
    reordered_count: int,
) -> np.ndarray:
    """Permute exactly ``reordered_count`` selected positions in every row."""
    if ideal.ndim != 2:
        raise ValueError("ideal sequences must be a two-dimensional matrix")
    if not 0 <= reordered_count <= ideal.shape[1]:
        raise ValueError("reordered_count must lie within the sequence length")

    reordered = ideal.copy()
    if reordered_count < 2:
        return reordered
    for row_index in range(len(reordered)):
        selected = rng.choice(ideal.shape[1], size=reordered_count, replace=False)
        permutation = rng.permutation(reordered[row_index, selected])
        if np.array_equal(permutation, reordered[row_index, selected]):
            permutation = np.roll(permutation, 1)
        reordered[row_index, selected] = permutation
    return reordered


def _classify_mass(
    values: np.ndarray,
    loss_mask: np.ndarray | None = None,
    *,
    candidate_null_tables: CandidateNullTables | None = None,
    candidate_threshold: float = CANDIDATE_RANDOM_MIN_SCORE,
    legacy_random_score: bool = False,
) -> np.ndarray:
    rows = []
    for row_index, row in enumerate(values):
        if loss_mask is None:
            rows.append([int(value) for value in row])
        else:
            rows.append(
                [
                    -1 if loss_mask[row_index, column_index] else int(value)
                    for column_index, value in enumerate(row)
                ]
            )
    codes = classify_batch_mass(
        pa.array(rows, type=pa.list_(pa.int64())),
        FIXED_CONFIG,
        random_null_tables=candidate_null_tables,
        random_threshold=(
            LEGACY_RANDOM_STRUCTURE_MIN_SCORE if legacy_random_score else candidate_threshold
        ),
        random_score_function=(legacy_random_structure_scores if legacy_random_score else None),
    )
    return codes


def _strategy_names(codes: np.ndarray) -> list[str]:
    return [IPIDStrategy(int(code)).name for code in codes]


def _confusion_metrics(
    expected: list[str],
    detected: list[str],
    generated_classes: tuple[str, ...],
    detected_classes: tuple[str, ...],
) -> dict:
    generated_index = {name: index for index, name in enumerate(generated_classes)}
    detected_index = {name: index for index, name in enumerate(detected_classes)}
    unexpected_expected = sorted(set(expected) - set(generated_classes))
    unexpected_detected = sorted(set(detected) - set(detected_classes))
    if unexpected_expected or unexpected_detected:
        raise ValueError(
            "strategies outside validation matrix: "
            f"expected={unexpected_expected}, detected={unexpected_detected}"
        )

    counts = np.zeros(
        (len(generated_classes), len(detected_classes)),
        dtype=np.int64,
    )
    for truth, prediction in zip(expected, detected, strict=True):
        counts[generated_index[truth], detected_index[prediction]] += 1

    support = counts.sum(axis=1)
    predicted_count = counts.sum(axis=0)
    correct = np.asarray(
        [
            counts[row_index, detected_index[strategy]]
            for row_index, strategy in enumerate(generated_classes)
        ],
        dtype=np.int64,
    )
    matching_predicted_count = np.asarray(
        [predicted_count[detected_index[strategy]] for strategy in generated_classes],
        dtype=np.int64,
    )
    precision = np.divide(
        correct,
        matching_predicted_count,
        out=np.zeros(len(generated_classes), dtype=float),
        where=matching_predicted_count > 0,
    )
    recall = np.divide(
        correct,
        support,
        out=np.zeros(len(generated_classes), dtype=float),
        where=support > 0,
    )
    f1 = np.divide(
        2 * precision * recall,
        precision + recall,
        out=np.zeros(len(generated_classes), dtype=float),
        where=(precision + recall) > 0,
    )
    total = int(counts.sum())
    accuracy = float(correct.sum() / total) if total else 0.0
    percentages = np.divide(
        counts * 100.0,
        support[:, None],
        out=np.zeros_like(counts, dtype=float),
        where=support[:, None] > 0,
    )
    weights = support / total if total else np.zeros(len(generated_classes), dtype=float)

    truth_count = np.zeros(len(detected_classes), dtype=np.int64)
    for row_index, strategy in enumerate(generated_classes):
        truth_count[detected_index[strategy]] = support[row_index]
    total_float = float(total)
    correct_total = float(correct.sum())
    truth_count_float = truth_count.astype(np.float64)
    predicted_count_float = predicted_count.astype(np.float64)
    agreement_count = float(np.dot(truth_count_float, predicted_count_float))
    expected_agreement = agreement_count / (total_float * total_float) if total else 0.0
    cohen_kappa = (
        (accuracy - expected_agreement) / (1.0 - expected_agreement)
        if expected_agreement < 1.0
        else 1.0
    )
    squared_total = total_float * total_float
    predicted_term = max(
        0.0,
        squared_total - float(np.dot(predicted_count_float, predicted_count_float)),
    )
    truth_term = max(
        0.0,
        squared_total - float(np.dot(truth_count_float, truth_count_float)),
    )
    mcc_numerator = correct_total * total_float - agreement_count
    mcc_denominator = float(np.sqrt(predicted_term * truth_term))
    multiclass_mcc = (
        float(np.clip(mcc_numerator / mcc_denominator, -1.0, 1.0)) if mcc_denominator else 0.0
    )

    return {
        "sample_count": total,
        "correct_count": int(correct.sum()),
        "misclassified_count": total - int(correct.sum()),
        "accuracy": accuracy,
        "balanced_accuracy": float(recall.mean()),
        "macro": {
            "precision": float(precision.mean()),
            "recall": float(recall.mean()),
            "f1": float(f1.mean()),
        },
        "weighted": {
            "precision": float(np.dot(precision, weights)),
            "recall": float(np.dot(recall, weights)),
            "f1": float(np.dot(f1, weights)),
        },
        "cohen_kappa": cohen_kappa,
        "multiclass_matthews_correlation_coefficient": multiclass_mcc,
        "classes": {
            strategy: {
                "support": int(support[index]),
                "predicted": int(matching_predicted_count[index]),
                "correct": int(correct[index]),
                "precision": float(precision[index]),
                "recall": float(recall[index]),
                "f1": float(f1[index]),
            }
            for index, strategy in enumerate(generated_classes)
        },
        "detected_output_counts": {
            strategy: int(predicted_count[index])
            for index, strategy in enumerate(detected_classes)
        },
        "confusion_matrix": {
            "generated_class_order": list(generated_classes),
            "detected_class_order": list(detected_classes),
            "counts": counts.tolist(),
            "row_percentages": percentages.tolist(),
        },
    }


def _rejection_metrics(
    detections: dict[str, list[str]],
    *,
    expected_output: str = "UNCLASSIFIED",
) -> dict:
    by_generator = {}
    all_detections = []
    for generator, values in detections.items():
        counts = {strategy: values.count(strategy) for strategy in sorted(set(values))}
        rejected = values.count(expected_output)
        by_generator[generator] = {
            "sample_count": len(values),
            "expected_output": expected_output,
            "rejected_count": rejected,
            "rejection_rate": rejected / len(values) if values else 0.0,
            "detected_output_counts": counts,
        }
        all_detections.extend(values)
    rejected = all_detections.count(expected_output)
    return {
        "sample_count": len(all_detections),
        "expected_output": expected_output,
        "rejected_count": rejected,
        "rejection_rate": rejected / len(all_detections) if all_detections else 0.0,
        "by_generator": by_generator,
    }


def _matrix_percentages(metrics: dict) -> np.ndarray:
    return np.asarray(metrics["confusion_matrix"]["row_percentages"], dtype=float)


def _format_matrix_percentage(percentage: float) -> str:
    """Format cells without presenting a small non-zero rate as zero."""
    if percentage == 0:
        return "-"
    if percentage < 0.1:
        return "<0.1"
    return f"{percentage:.1f}"


def _style_confusion_axis(ax) -> None:
    for spine in ax.spines.values():
        spine.set_linewidth(COMPACT_PAPER_STROKE_WIDTH)
    ax.tick_params(
        which="major",
        width=COMPACT_PAPER_STROKE_WIDTH,
        length=3.0,
        pad=1.5,
    )


def _normalize_title_gap(ax) -> None:
    """Place a panel title at a fixed physical distance above its grid."""
    figure = ax.figure
    figure.canvas.draw()
    renderer = figure.canvas.get_renderer()
    dpi = figure.dpi
    axis_box = ax.get_window_extent(renderer=renderer)
    title_box = ax.title.get_window_extent(renderer=renderer)
    current_gap_inches = (title_box.y0 - axis_box.y1) / dpi
    title_x, title_y = ax.title.get_position()
    ax.title.set_position(
        (
            title_x,
            title_y + (CONFUSION_TITLE_GAP_INCHES - current_gap_inches) * dpi / axis_box.height,
        )
    )
    figure.canvas.draw()
    actual_gap = (ax.title.get_window_extent(renderer=renderer).y0 - axis_box.y1) / dpi
    if not np.isclose(actual_gap, CONFUSION_TITLE_GAP_INCHES, atol=1e-4):
        raise RuntimeError(f"confusion title gap mismatch: {actual_gap}")


def _draw_confusion_matrix(
    ax,
    metrics: dict,
    generated_classes: tuple[str, ...],
    detected_classes: tuple[str, ...],
    *,
    title: str | None = None,
):
    matrix = _matrix_percentages(metrics)
    rows, columns = matrix.shape
    image = ax.pcolormesh(
        np.arange(columns + 1),
        np.arange(rows + 1),
        matrix,
        cmap=PERCENTAGE_CMAP,
        vmin=0,
        vmax=100,
        edgecolors="white",
        linewidth=0.40,
        antialiased=False,
        shading="flat",
    )
    xlabels = [STRATEGY_PRETTY[strategy] for strategy in detected_classes]
    ylabels = [STRATEGY_PRETTY[strategy] for strategy in generated_classes]
    ax.set_xlim(0, columns)
    ax.set_ylim(rows, 0)
    ax.set_xticks(np.arange(columns) + 0.5, xlabels)
    ax.set_yticks(np.arange(rows) + 0.5, ylabels)
    for label in ax.get_xticklabels():
        label.set_rotation(30)
        label.set_rotation_mode("anchor")
        label.set_horizontalalignment("right")
        label.set_verticalalignment("top")
    for label in ax.get_yticklabels():
        label.set_horizontalalignment("right")
        label.set_verticalalignment("center")
    if title:
        ax.set_title(title, pad=0.0, y=1.0)
        _normalize_title_gap(ax)
    _style_confusion_axis(ax)

    for row_index in range(rows):
        for column_index in range(columns):
            percentage = matrix[row_index, column_index]
            text_transform = ax.transData + ScaledTranslation(
                0,
                CONFUSION_NUMERIC_TEXT_UPWARD_OFFSET_POINTS / 72.0,
                ax.figure.dpi_scale_trans,
            )
            ax.text(
                column_index + 0.5,
                row_index + 0.5,
                _format_matrix_percentage(percentage),
                ha="center",
                va="center",
                color="white" if percentage >= 50 else "#222222",
                transform=text_transform,
            )
    return image


def _add_confusion_colorbar(
    fig,
    *,
    left_inches: float,
    bottom_inches: float,
    height_inches: float,
):
    figure_width, figure_height = fig.get_size_inches()
    colorbar_axis = fig.add_axes(
        (
            left_inches / figure_width,
            bottom_inches / figure_height,
            CONFUSION_COLORBAR_WIDTH_INCHES / figure_width,
            height_inches / figure_height,
        )
    )
    draw_percentage_colorbar_axis(
        colorbar_axis,
        label="Percentage [%]",
        stroke_width=COMPACT_PAPER_STROKE_WIDTH,
        tick_length=3.0,
        tick_pad=1.5,
        text_upward_offset_points=CONFUSION_NUMERIC_TEXT_UPWARD_OFFSET_POINTS,
    )
    return colorbar_axis


def _add_normalized_confusion_labels(
    fig,
    *,
    block_left_inches: float,
    block_width_inches: float,
    grid_bottom_inches: float,
    content_center_y_inches: float,
    ylabel: str,
) -> None:
    """Place shared labels at fixed physical distances from the matrix block."""
    figure_width, figure_height = fig.get_size_inches()
    xlabel = fig.text(
        (block_left_inches + block_width_inches / 2.0) / figure_width,
        0.05,
        "Detected IP-ID Selection Strategy",
        ha="center",
        va="bottom",
    )
    ylabel_artist = fig.text(
        0.05,
        content_center_y_inches / figure_height,
        ylabel,
        ha="center",
        va="center",
        rotation=90,
    )

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    dpi = fig.dpi
    xlabel_box = xlabel.get_window_extent(renderer=renderer)
    xlabel_x, xlabel_y = xlabel.get_position()
    xlabel.set_position(
        (
            xlabel_x,
            xlabel_y
            + (grid_bottom_inches - CONFUSION_XLABEL_GAP_INCHES - xlabel_box.y1 / dpi)
            / figure_height,
        )
    )
    ylabel_box = ylabel_artist.get_window_extent(renderer=renderer)
    ylabel_x, ylabel_y = ylabel_artist.get_position()
    ylabel_artist.set_position(
        (
            ylabel_x
            + (block_left_inches - CONFUSION_YLABEL_GAP_INCHES - ylabel_box.x1 / dpi)
            / figure_width,
            ylabel_y,
        )
    )

    fig.canvas.draw()
    actual_x_gap = grid_bottom_inches - xlabel.get_window_extent(renderer=renderer).y1 / dpi
    actual_y_gap = block_left_inches - ylabel_artist.get_window_extent(renderer=renderer).x1 / dpi
    if not np.isclose(actual_x_gap, CONFUSION_XLABEL_GAP_INCHES, atol=1e-4):
        raise RuntimeError(f"confusion x-label gap mismatch: {actual_x_gap}")
    if not np.isclose(actual_y_gap, CONFUSION_YLABEL_GAP_INCHES, atol=1e-4):
        raise RuntimeError(f"confusion y-label gap mismatch: {actual_y_gap}")


def _save_figure(fig, output_path: Path, *, title: str, subject: str) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_path,
        format="pdf",
        bbox_inches="tight",
        pad_inches=COMPACT_PAPER_PDF_PADDING_INCHES,
        metadata={"Title": title, "Subject": subject, "Creator": "ipid-analysis"},
    )
    plt.close(fig)
    return output_path


def plot_ideal_confusion_matrix(
    metrics: dict,
    generated_classes: tuple[str, ...],
    detected_classes: tuple[str, ...],
    output_path: Path,
    *,
    title: str,
) -> Path:
    configure_compact_validation_style()
    figure_height = 2.95
    block_left = 1.66
    grid_bottom = 1.08
    panel_width = len(detected_classes) * CONFUSION_CELL_WIDTH_INCHES
    panel_height = len(generated_classes) * CONFUSION_CELL_HEIGHT_INCHES
    fig = plt.figure(figsize=(CONFUSION_FIGURE_WIDTH_INCHES, figure_height))
    ax = fig.add_axes(
        (
            block_left / CONFUSION_FIGURE_WIDTH_INCHES,
            grid_bottom / figure_height,
            panel_width / CONFUSION_FIGURE_WIDTH_INCHES,
            panel_height / figure_height,
        )
    )
    _draw_confusion_matrix(
        ax,
        metrics,
        generated_classes,
        detected_classes,
    )
    _add_normalized_confusion_labels(
        fig,
        block_left_inches=block_left,
        block_width_inches=panel_width,
        grid_bottom_inches=grid_bottom,
        content_center_y_inches=grid_bottom + panel_height / 2.0,
        ylabel="Generating IP-ID\nSelection Strategy",
    )
    _add_confusion_colorbar(
        fig,
        left_inches=block_left + panel_width + CONFUSION_COLORBAR_GAP_INCHES,
        bottom_inches=grid_bottom,
        height_inches=panel_height,
    )
    return _save_figure(
        fig,
        output_path,
        title=title,
        subject="Synthetic IP-ID classifier confusion matrix",
    )


def plot_confusion_matrix_grid(
    panels: tuple[tuple[str, dict], ...],
    generated_classes: tuple[str, ...],
    detected_classes: tuple[str, ...],
    output_path: Path,
    *,
    nrows: int,
    ncols: int,
    title: str,
    subject: str,
) -> Path:
    """Render a compact shared-scale grid of classifier confusion matrices."""
    if len(panels) != nrows * ncols:
        raise ValueError("panel count must equal nrows * ncols")
    configure_compact_validation_style()
    panel_width = len(detected_classes) * CONFUSION_CELL_WIDTH_INCHES
    panel_height = len(generated_classes) * CONFUSION_CELL_HEIGHT_INCHES
    block_width = ncols * panel_width + (ncols - 1) * CONFUSION_HORIZONTAL_PANEL_GAP_INCHES
    block_left = (
        1.66 if ncols == 1 else max(1.0, (CONFUSION_FIGURE_WIDTH_INCHES - block_width) / 2.0)
    )
    grid_bottom = 1.10 if nrows > 1 else 1.08
    figure_height = 4.85 if nrows > 1 else 2.95
    fig = plt.figure(figsize=(CONFUSION_FIGURE_WIDTH_INCHES, figure_height))
    axes = np.empty((nrows, ncols), dtype=object)
    for row in range(nrows):
        for column in range(ncols):
            left = block_left + column * (panel_width + CONFUSION_HORIZONTAL_PANEL_GAP_INCHES)
            bottom = grid_bottom + (nrows - row - 1) * (
                panel_height + CONFUSION_VERTICAL_PANEL_GAP_INCHES
            )
            axes[row, column] = fig.add_axes(
                (
                    left / CONFUSION_FIGURE_WIDTH_INCHES,
                    bottom / figure_height,
                    panel_width / CONFUSION_FIGURE_WIDTH_INCHES,
                    panel_height / figure_height,
                )
            )
    for axis, (panel_title, metrics) in zip(axes.flat, panels, strict=True):
        _draw_confusion_matrix(
            axis,
            metrics,
            generated_classes,
            detected_classes,
            title=panel_title,
        )
    for axis in axes[:-1, :].flat:
        axis.tick_params(axis="x", bottom=True, labelbottom=False)
    block_height = nrows * panel_height + (nrows - 1) * CONFUSION_VERTICAL_PANEL_GAP_INCHES
    block_center_y = grid_bottom + block_height / 2.0
    _add_normalized_confusion_labels(
        fig,
        block_left_inches=block_left,
        block_width_inches=block_width,
        grid_bottom_inches=grid_bottom,
        content_center_y_inches=block_center_y,
        ylabel="Generating IP-ID Selection Strategy",
    )
    _add_confusion_colorbar(
        fig,
        left_inches=block_left + block_width + CONFUSION_COLORBAR_GAP_INCHES,
        bottom_inches=block_center_y - panel_height / 2.0,
        height_inches=panel_height,
    )
    return _save_figure(
        fig,
        output_path,
        title=title,
        subject=subject,
    )


def _timestamps(
    sequence_length: int,
    *,
    rt_based: bool,
) -> tuple[np.ndarray, np.ndarray]:
    sent = np.empty(sequence_length, dtype=np.int64)
    received = np.empty(sequence_length, dtype=np.int64)
    now = 1_700_000_000_000_000
    for index in range(sequence_length):
        if rt_based:
            sent[index] = now if index == 0 else received[index - 1] + 250
        else:
            sent[index] = now + index * 20_000
        received[index] = sent[index] + 2_000 + (index % CONNECTION_COUNT) * 100
    return sent, received


def _serialize(values: np.ndarray, missing: np.ndarray | None = None) -> str:
    if missing is None:
        return ",".join(str(int(value)) for value in values)
    return ",".join(
        "-" if missing[index] else str(int(value)) for index, value in enumerate(values)
    )


def _synthetic_ip(index: int) -> str:
    base = int(ipaddress.IPv4Address("198.18.0.1"))
    return str(ipaddress.IPv4Address(base + index))


def _append_dataset_rows(
    columns: dict[str, list],
    *,
    dataset: str,
    sequences: dict[str, np.ndarray],
    detections: dict[str, list[str]],
    requests_per_connection: int,
    address_offset: int,
    loss_masks: dict[str, np.ndarray] | None = None,
    reordered_count: int = 0,
    expected_strategies: dict[str, str] | None = None,
) -> int:
    sequence_length = CONNECTION_COUNT * requests_per_connection
    sent, received = _timestamps(
        sequence_length,
        rt_based=requests_per_connection == RT_REQUESTS_PER_CONNECTION,
    )
    row_index = address_offset
    for strategy, strategy_sequences in sequences.items():
        masks = None if loss_masks is None else loss_masks[strategy]
        for sample_index, values in enumerate(strategy_sequences):
            missing = None if masks is None else masks[sample_index]
            sample_id = f"{strategy.lower()}-{sample_index:06d}"
            columns["DATASET"].append(dataset)
            columns["SAMPLE_ID"].append(sample_id)
            columns["IP_ADDR"].append(_synthetic_ip(row_index))
            columns["CONNECTION_COUNT"].append(CONNECTION_COUNT)
            columns["REQUESTS_PER_CONNECTION"].append(requests_per_connection)
            columns["GENERATOR_STRATEGY"].append(strategy)
            columns["EXPECTED_STRATEGY"].append(
                strategy if expected_strategies is None else expected_strategies[strategy]
            )
            columns["DETECTED_STRATEGY"].append(detections[strategy][sample_index])
            columns["IPID_SEQUENCE"].append(_serialize(values, missing))
            columns["SEND_TIMESTAMP_SEQUENCE"].append(_serialize(sent, missing))
            columns["RECEIVE_TIMESTAMP_SEQUENCE"].append(_serialize(received, missing))
            columns["LOSS_COUNT"].append(int(missing.sum()) if missing is not None else 0)
            columns["REORDERED_COUNT"].append(reordered_count)
            row_index += 1
    return row_index


def _empty_validation_columns() -> dict[str, list]:
    return {field.name: [] for field in VALIDATION_SCHEMA}


def _write_parquet(table: pa.Table, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    pq.write_table(table, temporary, compression="zstd")
    temporary.replace(output_path)
    return output_path


def _write_json(value: dict, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(output_path)
    return output_path


def validate_classifier(
    *,
    samples_per_strategy: int = DEFAULT_SAMPLES_PER_STRATEGY,
    seed: int = 42,
    candidate_random_score: bool = True,
    candidate_null_table_samples: int = CANDIDATE_NULL_TABLE_SAMPLES,
    candidate_null_table_seed: int = CANDIDATE_NULL_TABLE_SEED,
    candidate_threshold: float = CANDIDATE_RANDOM_MIN_SCORE,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
) -> dict[str, Path]:
    """Generate synthetic datasets, classify them, and write plots and metrics."""
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")
    if candidate_null_table_samples < 1:
        raise ValueError("candidate_null_table_samples must be positive")
    if not 0.0 <= candidate_threshold <= 1.0:
        raise ValueError("candidate_threshold must lie in [0, 1]")

    candidate_null_tables = None
    if candidate_random_score:
        candidate_null_tables = create_candidate_null_tables(
            candidate_null_table_samples,
            candidate_null_table_seed,
        )
    mass_classifier_kwargs = {
        "candidate_null_tables": candidate_null_tables,
        "candidate_threshold": candidate_threshold,
        "legacy_random_score": not candidate_random_score,
    }

    seed_sequence = np.random.SeedSequence(seed)
    (
        rt_rng,
        rt_out_of_scope_rng,
        fixed_rng,
        base_reorder_3_rng,
        base_reorder_4_rng,
        impairment_rng,
        mass_reorder_rng,
    ) = [np.random.default_rng(child) for child in seed_sequence.spawn(7)]

    generated_sample_count = max(samples_per_strategy, TRIVIAL_SAMPLES_PER_STRATEGY)
    rt_config, rt_sequences = generate_rt_sequences(generated_sample_count, rt_rng)
    rt_sequences = {
        strategy: values[
            : (
                TRIVIAL_SAMPLES_PER_STRATEGY
                if strategy in TRIVIAL_STRATEGIES
                else samples_per_strategy
            )
        ]
        for strategy, values in rt_sequences.items()
    }
    rt_detections = {
        strategy: _strategy_names(classify_batch(values, rt_config))
        for strategy, values in rt_sequences.items()
    }
    rt_matrix = np.concatenate([rt_sequences[strategy] for strategy in RT_STRATEGIES], axis=0)
    rt_class_slices = {}
    rt_class_offset = 0
    for strategy in RT_STRATEGIES:
        next_offset = rt_class_offset + len(rt_sequences[strategy])
        rt_class_slices[strategy] = slice(rt_class_offset, next_offset)
        rt_class_offset = next_offset
    base_reordered_3_matrix = apply_reordering(
        rt_matrix,
        base_reorder_3_rng,
        reordered_count=3,
    )
    base_reordered_4_matrix = apply_reordering(
        rt_matrix,
        base_reorder_4_rng,
        reordered_count=4,
    )
    base_reordered_3_sequences = {
        strategy: base_reordered_3_matrix[rt_class_slices[strategy]] for strategy in RT_STRATEGIES
    }
    base_reordered_4_sequences = {
        strategy: base_reordered_4_matrix[rt_class_slices[strategy]] for strategy in RT_STRATEGIES
    }
    base_reordered_3_detections = {
        strategy: _strategy_names(classify_batch(values, rt_config))
        for strategy, values in base_reordered_3_sequences.items()
    }
    base_reordered_4_detections = {
        strategy: _strategy_names(classify_batch(values, rt_config))
        for strategy, values in base_reordered_4_sequences.items()
    }
    rt_out_of_scope_sequences = generate_rt_out_of_scope_sequences(
        samples_per_strategy,
        rt_out_of_scope_rng,
    )
    rt_out_of_scope_detections = {
        strategy: _strategy_names(classify_batch(values, rt_config))
        for strategy, values in rt_out_of_scope_sequences.items()
    }

    fixed_sequences = generate_fixed_sequences(generated_sample_count, fixed_rng)
    fixed_sequences = {
        strategy: values[
            : (
                TRIVIAL_SAMPLES_PER_STRATEGY
                if strategy in TRIVIAL_STRATEGIES
                else samples_per_strategy
            )
        ]
        for strategy, values in fixed_sequences.items()
    }
    fixed_detections = {
        strategy: _strategy_names(_classify_mass(values, **mass_classifier_kwargs))
        for strategy, values in fixed_sequences.items()
    }
    fixed_matrix = np.concatenate(
        [fixed_sequences[strategy] for strategy in FIXED_IMPAIRED_STRATEGIES],
        axis=0,
    )
    fixed_loss_mask, lossy_matrix, reordered_matrix = apply_fixed_interval_impairments(
        fixed_matrix,
        impairment_rng,
    )
    mass_reordered_matrix = apply_reordering(
        fixed_matrix,
        mass_reorder_rng,
        reordered_count=20,
    )
    class_slices = {}
    class_offset = 0
    for strategy in FIXED_IMPAIRED_STRATEGIES:
        next_offset = class_offset + len(fixed_sequences[strategy])
        class_slices[strategy] = slice(class_offset, next_offset)
        class_offset = next_offset
    lossy_sequences = {
        strategy: lossy_matrix[class_slices[strategy]] for strategy in FIXED_IMPAIRED_STRATEGIES
    }
    reordered_sequences = {
        strategy: reordered_matrix[class_slices[strategy]]
        for strategy in FIXED_IMPAIRED_STRATEGIES
    }
    mass_reordered_sequences = {
        strategy: mass_reordered_matrix[class_slices[strategy]]
        for strategy in FIXED_IMPAIRED_STRATEGIES
    }
    loss_masks = {
        strategy: fixed_loss_mask[class_slices[strategy]] for strategy in FIXED_IMPAIRED_STRATEGIES
    }
    lossy_detections = {
        strategy: _strategy_names(
            _classify_mass(
                values,
                loss_masks[strategy],
                **mass_classifier_kwargs,
            )
        )
        for strategy, values in lossy_sequences.items()
    }
    reordered_detections = {
        strategy: _strategy_names(
            _classify_mass(
                values,
                loss_masks[strategy],
                **mass_classifier_kwargs,
            )
        )
        for strategy, values in reordered_sequences.items()
    }
    mass_reordered_detections = {
        strategy: _strategy_names(_classify_mass(values, **mass_classifier_kwargs))
        for strategy, values in mass_reordered_sequences.items()
    }

    def flatten_labels(
        detections: dict[str, list[str]],
        classes: tuple[str, ...],
    ) -> tuple[list[str], list[str]]:
        expected = [strategy for strategy in classes for _ in range(len(detections[strategy]))]
        detected = [prediction for strategy in classes for prediction in detections[strategy]]
        return expected, detected

    rt_expected, rt_detected = flatten_labels(rt_detections, RT_STRATEGIES)
    base_reordered_3_expected, base_reordered_3_detected = flatten_labels(
        base_reordered_3_detections,
        RT_STRATEGIES,
    )
    base_reordered_4_expected, base_reordered_4_detected = flatten_labels(
        base_reordered_4_detections,
        RT_STRATEGIES,
    )
    fixed_expected, fixed_detected = flatten_labels(fixed_detections, FIXED_STRATEGIES)
    lossy_expected, lossy_detected = flatten_labels(
        lossy_detections,
        FIXED_IMPAIRED_STRATEGIES,
    )
    reordered_expected, reordered_detected = flatten_labels(
        reordered_detections,
        FIXED_IMPAIRED_STRATEGIES,
    )
    mass_reordered_expected, mass_reordered_detected = flatten_labels(
        mass_reordered_detections,
        FIXED_IMPAIRED_STRATEGIES,
    )
    rt_metrics = _confusion_metrics(
        rt_expected,
        rt_detected,
        RT_STRATEGIES,
        RT_DETECTED_STRATEGIES,
    )
    base_reordered_3_metrics = _confusion_metrics(
        base_reordered_3_expected,
        base_reordered_3_detected,
        RT_STRATEGIES,
        RT_DETECTED_STRATEGIES,
    )
    base_reordered_4_metrics = _confusion_metrics(
        base_reordered_4_expected,
        base_reordered_4_detected,
        RT_STRATEGIES,
        RT_DETECTED_STRATEGIES,
    )
    fixed_metrics = _confusion_metrics(
        fixed_expected,
        fixed_detected,
        FIXED_STRATEGIES,
        FIXED_DETECTED_STRATEGIES,
    )
    lossy_metrics = _confusion_metrics(
        lossy_expected,
        lossy_detected,
        FIXED_IMPAIRED_STRATEGIES,
        FIXED_IMPAIRED_DETECTED_STRATEGIES,
    )
    reordered_metrics = _confusion_metrics(
        reordered_expected,
        reordered_detected,
        FIXED_IMPAIRED_STRATEGIES,
        FIXED_IMPAIRED_DETECTED_STRATEGIES,
    )
    mass_reordered_metrics = _confusion_metrics(
        mass_reordered_expected,
        mass_reordered_detected,
        FIXED_IMPAIRED_STRATEGIES,
        FIXED_IMPAIRED_DETECTED_STRATEGIES,
    )
    out_of_scope_metrics = _rejection_metrics(rt_out_of_scope_detections)

    columns = _empty_validation_columns()
    next_address = _append_dataset_rows(
        columns,
        dataset=RT_DATASET,
        sequences=rt_sequences,
        detections=rt_detections,
        requests_per_connection=RT_REQUESTS_PER_CONNECTION,
        address_offset=0,
    )
    next_address = _append_dataset_rows(
        columns,
        dataset=RT_OUT_OF_SCOPE_DATASET,
        sequences=rt_out_of_scope_sequences,
        detections=rt_out_of_scope_detections,
        requests_per_connection=RT_REQUESTS_PER_CONNECTION,
        address_offset=next_address,
        expected_strategies={strategy: "UNCLASSIFIED" for strategy in RT_OUT_OF_SCOPE_STRATEGIES},
    )
    next_address = _append_dataset_rows(
        columns,
        dataset=BASE_REORDERED_3_DATASET,
        sequences=base_reordered_3_sequences,
        detections=base_reordered_3_detections,
        requests_per_connection=RT_REQUESTS_PER_CONNECTION,
        address_offset=next_address,
        reordered_count=3,
    )
    next_address = _append_dataset_rows(
        columns,
        dataset=BASE_REORDERED_4_DATASET,
        sequences=base_reordered_4_sequences,
        detections=base_reordered_4_detections,
        requests_per_connection=RT_REQUESTS_PER_CONNECTION,
        address_offset=next_address,
        reordered_count=4,
    )
    next_address = _append_dataset_rows(
        columns,
        dataset=FIXED_IDEAL_DATASET,
        sequences=fixed_sequences,
        detections=fixed_detections,
        requests_per_connection=FIXED_REQUESTS_PER_CONNECTION,
        address_offset=next_address,
    )
    next_address = _append_dataset_rows(
        columns,
        dataset=FIXED_LOSSY_DATASET,
        sequences=lossy_sequences,
        detections=lossy_detections,
        requests_per_connection=FIXED_REQUESTS_PER_CONNECTION,
        address_offset=next_address,
        loss_masks=loss_masks,
    )
    next_address = _append_dataset_rows(
        columns,
        dataset=MASS_REORDERED_DATASET,
        sequences=mass_reordered_sequences,
        detections=mass_reordered_detections,
        requests_per_connection=FIXED_REQUESTS_PER_CONNECTION,
        address_offset=next_address,
        reordered_count=20,
    )
    reordered_count = round(CONNECTION_COUNT * FIXED_REQUESTS_PER_CONNECTION * (1.0 - 0.20) * 0.20)
    _append_dataset_rows(
        columns,
        dataset=FIXED_REORDERED_DATASET,
        sequences=reordered_sequences,
        detections=reordered_detections,
        requests_per_connection=FIXED_REQUESTS_PER_CONNECTION,
        address_offset=next_address,
        loss_masks=loss_masks,
        reordered_count=reordered_count,
    )

    processed_dir = processed_root / "classifier-validation"
    figure_dir = figures_root / "classifier-validation"
    for legacy_name in (
        "rt-based-4x4-classifier-confusion.pdf",
        "rt-based-4x4-classifier-confusion.json",
        "fixed-interval-4x25-classifier-confusion.pdf",
        "fixed-interval-4x25-classifier-confusion.json",
        "fixed-interval-4x25-impaired-classifier-confusion.pdf",
        "fixed-interval-4x25-impaired-classifier-confusion.json",
        "mass-4x25-classifier-confusion.pdf",
        "mass-4x25-classifier-confusion.json",
    ):
        (figure_dir / legacy_name).unlink(missing_ok=True)
    dataset_path = processed_dir / "synthetic-classifier-validation.pq"
    _write_parquet(pa.table(columns, schema=VALIDATION_SCHEMA), dataset_path)

    if candidate_random_score:
        random_score_metadata = {
            "version": CANDIDATE_RANDOM_SCORE_VERSION,
            "threshold": candidate_threshold,
            "metrics": list(CANDIDATE_RANDOM_METRICS),
            "combiner": "minimum",
            "increment_uniformity": {
                "bin_counts": list(CANDIDATE_INCREMENT_BIN_COUNTS),
                "target_expected_transitions_per_bin": (
                    CANDIDATE_INCREMENT_TARGET_EXPECTED_PER_BIN
                ),
                "minimum_transitions": CANDIDATE_INCREMENT_MIN_TRANSITIONS,
                "scale_aggregation": "jointly calibrated minimum",
                "subsequence_aggregation": CANDIDATE_INCREMENT_SUBSEQUENCE_AGGREGATION,
                "subsequence_groups": {
                    "full": "standalone",
                    "destinations": "Fisher combination of two disjoint views",
                    "connections": "Fisher combination of four disjoint views",
                    "final": "minimum of full, destination, and connection evidence",
                },
            },
            "target_random_false_rejection_rate": (CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE),
            "null_tables": {
                "version": CANDIDATE_NULL_TABLE_VERSION,
                "sample_count": candidate_null_table_samples,
                "base_seed": candidate_null_table_seed,
                "component_seeds": {
                    "increment_uniformity": (
                        candidate_null_table_seed + CANDIDATE_INCREMENT_NULL_TABLE_SEED_OFFSET
                    ),
                    "gap_uniformity": (
                        candidate_null_table_seed + CANDIDATE_GAP_NULL_TABLE_SEED_OFFSET
                    ),
                },
                "pvalue_resolution": 1.0 / (candidate_null_table_samples + 1.0),
            },
            "validation_only": False,
            "production_classifier_changed": True,
            "scope": "fixed-interval mass classifier only",
        }
    else:
        random_score_metadata = {
            "version": LEGACY_RANDOM_STRUCTURE_SCORE_VERSION,
            "threshold": LEGACY_RANDOM_STRUCTURE_MIN_SCORE,
            "validation_only": False,
            "production_classifier_changed": True,
            "historical_baseline": True,
            "scope": "fixed-interval mass classifier only",
        }
    random_score_metadata["applies_after"] = [
        "REFLECTION",
        "CONSTANT",
        "PER_DESTINATION",
        "PER_CONNECTION",
        "SINGLE",
        "PER_BUCKET",
        "MULTI",
    ]

    common_metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "classifier_version": CLASSIFIER_VERSION,
        "random_structure_score": random_score_metadata,
        "synthetic_generator_version": "3",
        "seed": seed,
        "samples_per_strategy": samples_per_strategy,
        "trivial_samples_per_strategy": TRIVIAL_SAMPLES_PER_STRATEGY,
        "trivial_strategies": sorted(TRIVIAL_STRATEGIES),
        "synthetic_generator_parameters": SYNTHETIC_GENERATOR_PARAMETERS,
        "samples_by_dataset_and_strategy": {
            RT_DATASET: {strategy: len(rt_sequences[strategy]) for strategy in RT_STRATEGIES},
            BASE_REORDERED_3_DATASET: {
                strategy: len(base_reordered_3_sequences[strategy]) for strategy in RT_STRATEGIES
            },
            BASE_REORDERED_4_DATASET: {
                strategy: len(base_reordered_4_sequences[strategy]) for strategy in RT_STRATEGIES
            },
            FIXED_IDEAL_DATASET: {
                strategy: len(fixed_sequences[strategy]) for strategy in FIXED_STRATEGIES
            },
            RT_OUT_OF_SCOPE_DATASET: {
                strategy: len(rt_out_of_scope_sequences[strategy])
                for strategy in RT_OUT_OF_SCOPE_STRATEGIES
            },
        },
        "connection_count": CONNECTION_COUNT,
        "request_ip_ids": REQUEST_IP_IDS.tolist(),
        "sequence_order": (
            "request round first, then connection index; even/odd positions "
            "alternate source addresses"
        ),
        "synthetic_dataset": str(dataset_path),
    }
    base_reordered_3_json = figure_dir / "base-4x4-classifier-confusion-reordered-3.json"
    base_reordered_4_json = figure_dir / "base-4x4-classifier-confusion-reordered-4.json"
    out_of_scope_json = figure_dir / "out-of-scope-classifier-rejection.json"
    _write_json(
        {
            **common_metadata,
            "shape": "4x4",
            "reordered_ipids_per_sequence": 3,
            "datasets": {
                "ideal": {"name": RT_DATASET, "metrics": rt_metrics},
                "reordered": {
                    "name": BASE_REORDERED_3_DATASET,
                    "metrics": base_reordered_3_metrics,
                },
            },
        },
        base_reordered_3_json,
    )
    _write_json(
        {
            **common_metadata,
            "shape": "4x4",
            "reordered_ipids_per_sequence": 4,
            "datasets": {
                "ideal": {"name": RT_DATASET, "metrics": rt_metrics},
                "reordered": {
                    "name": BASE_REORDERED_4_DATASET,
                    "metrics": base_reordered_4_metrics,
                },
            },
        },
        base_reordered_4_json,
    )
    _write_json(
        {
            **common_metadata,
            "purpose": (
                "Validate that MULTI and RANDOM, which are outside the Base classifier's "
                "supported label space, are rejected as UNCLASSIFIED."
            ),
            "tests": {
                "base": {
                    "dataset": RT_OUT_OF_SCOPE_DATASET,
                    "generating_strategies": list(RT_OUT_OF_SCOPE_STRATEGIES),
                    "metrics": out_of_scope_metrics,
                },
            },
        },
        out_of_scope_json,
    )

    base_reordered_3_pdf = figure_dir / "base-4x4-classifier-confusion-reordered-3.pdf"
    base_reordered_4_pdf = figure_dir / "base-4x4-classifier-confusion-reordered-4.pdf"
    mass_ideal_pdf = figure_dir / "mass-4x25-classifier-confusion-ideal.pdf"
    mass_lossy_vs_reordered_pdf = (
        figure_dir / "mass-4x25-classifier-confusion-lossy-vs-reordered.pdf"
    )
    mass_lossy_vs_lossy_reordered_pdf = (
        figure_dir / "mass-4x25-classifier-confusion-lossy-vs-lossy-reordered.pdf"
    )
    plot_confusion_matrix_grid(
        (("Ideal", rt_metrics), ("3 Reordered (18.75%)", base_reordered_3_metrics)),
        RT_STRATEGIES,
        RT_DETECTED_STRATEGIES,
        base_reordered_3_pdf,
        nrows=1,
        ncols=2,
        title="Base 4x4 classifier validation with 3 reordered IPIDs",
        subject="Synthetic Base 4x4 IP-ID classifier confusion matrices",
    )
    plot_confusion_matrix_grid(
        (("Ideal", rt_metrics), ("4 Reordered (25%)", base_reordered_4_metrics)),
        RT_STRATEGIES,
        RT_DETECTED_STRATEGIES,
        base_reordered_4_pdf,
        nrows=1,
        ncols=2,
        title="Base 4x4 classifier validation with 4 reordered IPIDs",
        subject="Synthetic Base 4x4 IP-ID classifier confusion matrices",
    )
    plot_ideal_confusion_matrix(
        fixed_metrics,
        FIXED_IMPAIRED_STRATEGIES,
        FIXED_IMPAIRED_DETECTED_STRATEGIES,
        mass_ideal_pdf,
        title="Mass 4x25 ideal classifier validation",
    )
    plot_confusion_matrix_grid(
        (("20% Lossy", lossy_metrics), ("20% Reordered", mass_reordered_metrics)),
        FIXED_IMPAIRED_STRATEGIES,
        FIXED_IMPAIRED_DETECTED_STRATEGIES,
        mass_lossy_vs_reordered_pdf,
        nrows=2,
        ncols=1,
        title="Mass 4x25 classifier validation: Lossy versus Reordered",
        subject="Synthetic Mass 4x25 Lossy and Reordered confusion matrices",
    )
    plot_confusion_matrix_grid(
        (
            ("20% Lossy", lossy_metrics),
            ("20% Lossy + 20% Reordered", reordered_metrics),
        ),
        FIXED_IMPAIRED_STRATEGIES,
        FIXED_IMPAIRED_DETECTED_STRATEGIES,
        mass_lossy_vs_lossy_reordered_pdf,
        nrows=2,
        ncols=1,
        title="Mass 4x25 classifier validation: Lossy impairment comparison",
        subject="Synthetic Mass 4x25 Lossy impairment confusion matrices",
    )

    return {
        "dataset": dataset_path,
        "base_reordered_3_pdf": base_reordered_3_pdf,
        "base_reordered_3_json": base_reordered_3_json,
        "base_reordered_4_pdf": base_reordered_4_pdf,
        "base_reordered_4_json": base_reordered_4_json,
        "mass_ideal_pdf": mass_ideal_pdf,
        "mass_lossy_vs_reordered_pdf": mass_lossy_vs_reordered_pdf,
        "mass_lossy_vs_lossy_reordered_pdf": mass_lossy_vs_lossy_reordered_pdf,
        "out_of_scope_json": out_of_scope_json,
    }


@app.command()
def main(
    samples_per_strategy: int = typer.Option(
        DEFAULT_SAMPLES_PER_STRATEGY,
        min=1,
        help=(
            "synthetic sequences generated for each nontrivial strategy; "
            "REFLECTION and CONSTANT always use 1000"
        ),
    ),
    seed: int = typer.Option(42, help="deterministic random seed"),
    candidate_random_score: bool = typer.Option(
        True,
        "--final-random-score/--legacy-random-score",
        help=(
            "render the established paper figures with the final production "
            "RANDOM score, or reproduce the pre-v7 production baseline"
        ),
    ),
    candidate_null_table_samples: int = typer.Option(
        CANDIDATE_NULL_TABLE_SAMPLES,
        min=1,
        help="Monte Carlo samples per candidate empirical null table",
    ),
    candidate_null_table_seed: int = typer.Option(
        CANDIDATE_NULL_TABLE_SEED,
        help="deterministic seed for candidate empirical null tables",
    ),
    candidate_threshold: float = typer.Option(
        CANDIDATE_RANDOM_MIN_SCORE,
        min=0.0,
        max=1.0,
        help="selected candidate minimum-score threshold",
    ),
) -> None:
    outputs = validate_classifier(
        samples_per_strategy=samples_per_strategy,
        seed=seed,
        candidate_random_score=candidate_random_score,
        candidate_null_table_samples=candidate_null_table_samples,
        candidate_null_table_seed=candidate_null_table_seed,
        candidate_threshold=candidate_threshold,
    )
    for name, path in outputs.items():
        typer.echo(f"{name}: {path}")


if __name__ == "__main__":
    app()
