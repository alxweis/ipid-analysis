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

import matplotlib.pyplot as plt  # noqa: E402

from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR  # noqa: E402
from ipid_analysis.paper_figures import (  # noqa: E402
    PERCENTAGE_CMAP,
    configure_paper_style,
    linux_libertine_font_properties,
)
from ipid_analysis.strategies import (  # noqa: E402
    CLASSIFIER_VERSION,
    MAX_INC,
    MULTI_MAX_CLUSTERS,
    MULTI_MAX_INC,
    RANDOM_STRUCTURE_MIN_SCORE,
    RANDOM_STRUCTURE_SCORE_VERSION,
    STRATEGY_PRETTY,
    IPIDStrategy,
    MeasurementConfig,
    classify_batch,
    classify_batch_mass,
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

RT_DATASET = "rt-based-4x4-ideal"
RT_OUT_OF_SCOPE_DATASET = "rt-based-4x4-out-of-scope"
FIXED_IDEAL_DATASET = "fixed-interval-4x25-ideal"
FIXED_OUT_OF_SCOPE_DATASET = "fixed-interval-4x25-out-of-scope"
FIXED_LOSSY_DATASET = "fixed-interval-4x25-lossy"
FIXED_REORDERED_DATASET = "fixed-interval-4x25-lossy-reordered"

RT_STRATEGIES = (
    "REFLECTION",
    "CONSTANT",
    "SINGLE",
    "PER_CONNECTION",
    "PER_DESTINATION",
    "PER_BUCKET",
)
RT_DETECTED_STRATEGIES = (*RT_STRATEGIES, "UNCLASSIFIED")
FIXED_STRATEGIES = ("CONSTANT", "MULTI", "RANDOM")
FIXED_DETECTED_STRATEGIES = (*FIXED_STRATEGIES, "UNCLASSIFIED")
RT_OUT_OF_SCOPE_STRATEGIES = ("MULTI",)
FIXED_OUT_OF_SCOPE_STRATEGIES = ("SINGLE",)
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


def generate_rt_sequences(
    samples_per_strategy: int,
    rng: np.random.Generator,
) -> tuple[MeasurementConfig, dict[str, np.ndarray]]:
    """Generate balanced ideal 4x4 sequences for RT-based classification."""
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    n = samples_per_strategy
    length = CONNECTION_COUNT * RT_REQUESTS_PER_CONNECTION
    config = MeasurementConfig(
        connection_count=CONNECTION_COUNT,
        requests_per_connection=RT_REQUESTS_PER_CONNECTION,
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
        + np.arange(RT_REQUESTS_PER_CONNECTION, dtype=np.int64)[None, :, None]
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
        size=(n, CONNECTION_COUNT, RT_REQUESTS_PER_CONNECTION - 1),
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


def generate_rt_out_of_scope_sequences(
    samples_per_strategy: int,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """Generate MULTI-like sequences that RT-based analysis must reject."""
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    n = samples_per_strategy
    length = CONNECTION_COUNT * RT_REQUESTS_PER_CONNECTION
    return {"MULTI": _generate_multi_sequences(n, length, rng)}


def generate_fixed_sequences(
    samples_per_strategy: int,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """Generate balanced ideal 4x25 sequences for fixed-interval classification."""
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    n = samples_per_strategy
    length = CONNECTION_COUNT * FIXED_REQUESTS_PER_CONNECTION

    constant_values = rng.integers(0, MODULUS, size=n, dtype=np.uint16)
    constant = np.repeat(constant_values[:, None], length, axis=1)

    multi = _generate_multi_sequences(n, length, rng)
    random = rng.integers(
        0,
        MODULUS,
        size=(n, length),
        dtype=np.uint16,
    )

    return {
        "CONSTANT": constant,
        "MULTI": multi,
        "RANDOM": random,
    }


def generate_fixed_out_of_scope_sequences(
    samples_per_strategy: int,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """Generate SINGLE-like sequences that fixed-interval analysis must reject."""
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    n = samples_per_strategy
    length = CONNECTION_COUNT * FIXED_REQUESTS_PER_CONNECTION
    single_starts = rng.integers(0, MODULUS, size=n, dtype=np.int64)
    single_increments = rng.integers(
        1,
        MAX_INC + 1,
        size=(n, length - 1),
        dtype=np.int64,
    )
    return {"SINGLE": _cumulative_sequences(single_starts, single_increments)}


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


def _classify_mass(values: np.ndarray, loss_mask: np.ndarray | None = None) -> np.ndarray:
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
    return classify_batch_mass(
        pa.array(rows, type=pa.list_(pa.int64())),
        FIXED_CONFIG,
    )


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
    expected_agreement = (
        float(np.dot(truth_count, predicted_count) / (total * total)) if total else 0.0
    )
    cohen_kappa = (
        (accuracy - expected_agreement) / (1.0 - expected_agreement)
        if expected_agreement < 1.0
        else 1.0
    )
    mcc_numerator = float(correct.sum() * total - np.dot(truth_count, predicted_count))
    mcc_denominator = float(
        np.sqrt(
            (total**2 - np.dot(predicted_count, predicted_count))
            * (total**2 - np.dot(truth_count, truth_count))
        )
    )
    multiclass_mcc = mcc_numerator / mcc_denominator if mcc_denominator else 0.0

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


def _draw_confusion_matrix(
    ax,
    metrics: dict,
    generated_classes: tuple[str, ...],
    detected_classes: tuple[str, ...],
    *,
    title: str | None = None,
):
    matrix = _matrix_percentages(metrics)
    image = ax.imshow(
        matrix,
        cmap=PERCENTAGE_CMAP,
        vmin=0,
        vmax=100,
        aspect="auto",
        interpolation="nearest",
    )
    xlabels = [STRATEGY_PRETTY[strategy] for strategy in detected_classes]
    ylabels = [STRATEGY_PRETTY[strategy] for strategy in generated_classes]
    ax.set_xticks(
        np.arange(len(xlabels)),
        xlabels,
        rotation=35,
        ha="right",
        rotation_mode="anchor",
    )
    ax.set_yticks(np.arange(len(ylabels)), ylabels)
    if title:
        title_font = linux_libertine_font_properties("DR", size=11)
        ax.set_title(title, pad=6, fontproperties=title_font)

    for row_index in range(matrix.shape[0]):
        for column_index in range(matrix.shape[1]):
            percentage = matrix[row_index, column_index]
            ax.text(
                column_index,
                row_index,
                "-" if percentage == 0 else f"{percentage:.1f}",
                ha="center",
                va="center",
                color="white" if percentage >= 50 else "#222222",
                fontsize=8,
            )
    return image


def _save_figure(fig, output_path: Path, *, title: str, subject: str) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_path,
        format="pdf",
        bbox_inches="tight",
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
    configure_paper_style()
    figure_height = 3.75 if len(generated_classes) > 4 else 3.25
    fig, ax = plt.subplots(figsize=(7.16, figure_height))
    image = _draw_confusion_matrix(
        ax,
        metrics,
        generated_classes,
        detected_classes,
    )
    ax.set_xlabel("Detected IP-ID Selection Strategy")
    ax.set_ylabel("Generating IP-ID\nSelection Strategy")
    colorbar = fig.colorbar(image, ax=ax, pad=0.04, fraction=0.045, ticks=np.arange(0, 101, 20))
    colorbar.set_label("Percentage [%]")
    fig.subplots_adjust(
        left=0.22 if len(generated_classes) > 4 else 0.19,
        right=0.88,
        bottom=0.32,
        top=0.98,
    )
    return _save_figure(
        fig,
        output_path,
        title=title,
        subject="Synthetic IP-ID classifier confusion matrix",
    )


def plot_impaired_confusion_matrices(
    lossy_metrics: dict,
    reordered_metrics: dict,
    output_path: Path,
) -> Path:
    configure_paper_style()
    fig, axes = plt.subplots(
        nrows=2,
        sharex=True,
        sharey=True,
        figsize=(7.16, 5.4),
        gridspec_kw={"hspace": 0.38},
    )
    image = _draw_confusion_matrix(
        axes[0],
        lossy_metrics,
        FIXED_STRATEGIES,
        FIXED_DETECTED_STRATEGIES,
        title="Lossy Dataset",
    )
    _draw_confusion_matrix(
        axes[1],
        reordered_metrics,
        FIXED_STRATEGIES,
        FIXED_DETECTED_STRATEGIES,
        title="Lossy+Reordered Dataset",
    )
    axes[0].tick_params(axis="x", bottom=False, labelbottom=False)
    axes[-1].set_xlabel("Detected IP-ID Selection Strategy")
    fig.supylabel("Generating IP-ID Selection Strategy", x=0.025)
    fig.subplots_adjust(left=0.20, right=0.86, bottom=0.22, top=0.95)
    colorbar_axis = fig.add_axes([0.885, 0.30, 0.018, 0.48])
    colorbar = fig.colorbar(image, cax=colorbar_axis, ticks=np.arange(0, 101, 20))
    colorbar.set_label("Percentage [%]")
    return _save_figure(
        fig,
        output_path,
        title="Fixed-interval classifier validation under loss and reordering",
        subject="Synthetic 4x25 IP-ID classifier confusion matrices",
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
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
) -> dict[str, Path]:
    """Generate synthetic datasets, classify them, and write plots and metrics."""
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    seed_sequence = np.random.SeedSequence(seed)
    rt_rng, rt_out_of_scope_rng, fixed_rng, fixed_out_of_scope_rng, impairment_rng = [
        np.random.default_rng(child) for child in seed_sequence.spawn(5)
    ]

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
        strategy: _strategy_names(_classify_mass(values))
        for strategy, values in fixed_sequences.items()
    }
    fixed_out_of_scope_sequences = generate_fixed_out_of_scope_sequences(
        samples_per_strategy,
        fixed_out_of_scope_rng,
    )
    fixed_out_of_scope_detections = {
        strategy: _strategy_names(_classify_mass(values))
        for strategy, values in fixed_out_of_scope_sequences.items()
    }

    fixed_matrix = np.concatenate(
        [fixed_sequences[strategy] for strategy in FIXED_STRATEGIES],
        axis=0,
    )
    fixed_loss_mask, lossy_matrix, reordered_matrix = apply_fixed_interval_impairments(
        fixed_matrix,
        impairment_rng,
    )
    class_slices = {}
    class_offset = 0
    for strategy in FIXED_STRATEGIES:
        next_offset = class_offset + len(fixed_sequences[strategy])
        class_slices[strategy] = slice(class_offset, next_offset)
        class_offset = next_offset
    lossy_sequences = {
        strategy: lossy_matrix[class_slices[strategy]] for strategy in FIXED_STRATEGIES
    }
    reordered_sequences = {
        strategy: reordered_matrix[class_slices[strategy]] for strategy in FIXED_STRATEGIES
    }
    loss_masks = {
        strategy: fixed_loss_mask[class_slices[strategy]] for strategy in FIXED_STRATEGIES
    }
    lossy_detections = {
        strategy: _strategy_names(_classify_mass(values, loss_masks[strategy]))
        for strategy, values in lossy_sequences.items()
    }
    reordered_detections = {
        strategy: _strategy_names(_classify_mass(values, loss_masks[strategy]))
        for strategy, values in reordered_sequences.items()
    }

    def flatten_labels(
        detections: dict[str, list[str]],
        classes: tuple[str, ...],
    ) -> tuple[list[str], list[str]]:
        expected = [strategy for strategy in classes for _ in range(len(detections[strategy]))]
        detected = [prediction for strategy in classes for prediction in detections[strategy]]
        return expected, detected

    rt_expected, rt_detected = flatten_labels(rt_detections, RT_STRATEGIES)
    fixed_expected, fixed_detected = flatten_labels(fixed_detections, FIXED_STRATEGIES)
    lossy_expected, lossy_detected = flatten_labels(lossy_detections, FIXED_STRATEGIES)
    reordered_expected, reordered_detected = flatten_labels(
        reordered_detections,
        FIXED_STRATEGIES,
    )
    rt_metrics = _confusion_metrics(
        rt_expected,
        rt_detected,
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
        FIXED_STRATEGIES,
        FIXED_DETECTED_STRATEGIES,
    )
    reordered_metrics = _confusion_metrics(
        reordered_expected,
        reordered_detected,
        FIXED_STRATEGIES,
        FIXED_DETECTED_STRATEGIES,
    )
    out_of_scope_metrics = {
        "rt_based": _rejection_metrics(rt_out_of_scope_detections),
        "fixed_interval": _rejection_metrics(fixed_out_of_scope_detections),
    }

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
        expected_strategies={"MULTI": "UNCLASSIFIED"},
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
        dataset=FIXED_OUT_OF_SCOPE_DATASET,
        sequences=fixed_out_of_scope_sequences,
        detections=fixed_out_of_scope_detections,
        requests_per_connection=FIXED_REQUESTS_PER_CONNECTION,
        address_offset=next_address,
        expected_strategies={"SINGLE": "UNCLASSIFIED"},
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
    dataset_path = processed_dir / "synthetic-classifier-validation.pq"
    _write_parquet(pa.table(columns, schema=VALIDATION_SCHEMA), dataset_path)

    common_metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "classifier_version": CLASSIFIER_VERSION,
        "random_structure_score": {
            "version": RANDOM_STRUCTURE_SCORE_VERSION,
            "threshold": RANDOM_STRUCTURE_MIN_SCORE,
            "applies_after": ["CONSTANT", "MULTI"],
        },
        "synthetic_generator_version": "2",
        "seed": seed,
        "samples_per_strategy": samples_per_strategy,
        "trivial_samples_per_strategy": TRIVIAL_SAMPLES_PER_STRATEGY,
        "trivial_strategies": sorted(TRIVIAL_STRATEGIES),
        "synthetic_generator_parameters": SYNTHETIC_GENERATOR_PARAMETERS,
        "samples_by_dataset_and_strategy": {
            RT_DATASET: {strategy: len(rt_sequences[strategy]) for strategy in RT_STRATEGIES},
            FIXED_IDEAL_DATASET: {
                strategy: len(fixed_sequences[strategy]) for strategy in FIXED_STRATEGIES
            },
            RT_OUT_OF_SCOPE_DATASET: {
                strategy: len(rt_out_of_scope_sequences[strategy])
                for strategy in RT_OUT_OF_SCOPE_STRATEGIES
            },
            FIXED_OUT_OF_SCOPE_DATASET: {
                strategy: len(fixed_out_of_scope_sequences[strategy])
                for strategy in FIXED_OUT_OF_SCOPE_STRATEGIES
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
    rt_json = figure_dir / "rt-based-4x4-classifier-confusion.json"
    fixed_json = figure_dir / "fixed-interval-4x25-classifier-confusion.json"
    impaired_json = figure_dir / "fixed-interval-4x25-impaired-classifier-confusion.json"
    out_of_scope_json = figure_dir / "out-of-scope-classifier-rejection.json"
    _write_json(
        {
            **common_metadata,
            "dataset": RT_DATASET,
            "shape": "4x4",
            "metrics": rt_metrics,
        },
        rt_json,
    )
    _write_json(
        {
            **common_metadata,
            "dataset": FIXED_IDEAL_DATASET,
            "shape": "4x25",
            "metrics": fixed_metrics,
        },
        fixed_json,
    )
    _write_json(
        {
            **common_metadata,
            "shape": "4x25",
            "loss_fraction": 0.20,
            "present_ipids_per_sequence": 80,
            "paired_loss_masks": True,
            "reordered_fraction_of_present": 0.20,
            "reordered_ipids_per_sequence": reordered_count,
            "datasets": {
                "lossy": {"name": FIXED_LOSSY_DATASET, "metrics": lossy_metrics},
                "lossy_reordered": {
                    "name": FIXED_REORDERED_DATASET,
                    "metrics": reordered_metrics,
                },
            },
        },
        impaired_json,
    )
    _write_json(
        {
            **common_metadata,
            "purpose": (
                "Validate that real generating strategies outside each measurement "
                "classifier's supported label space are rejected as UNCLASSIFIED."
            ),
            "tests": {
                "rt_based": {
                    "dataset": RT_OUT_OF_SCOPE_DATASET,
                    "generating_strategies": list(RT_OUT_OF_SCOPE_STRATEGIES),
                    "metrics": out_of_scope_metrics["rt_based"],
                },
                "fixed_interval": {
                    "dataset": FIXED_OUT_OF_SCOPE_DATASET,
                    "generating_strategies": list(FIXED_OUT_OF_SCOPE_STRATEGIES),
                    "metrics": out_of_scope_metrics["fixed_interval"],
                },
            },
        },
        out_of_scope_json,
    )

    rt_pdf = figure_dir / "rt-based-4x4-classifier-confusion.pdf"
    fixed_pdf = figure_dir / "fixed-interval-4x25-classifier-confusion.pdf"
    impaired_pdf = figure_dir / "fixed-interval-4x25-impaired-classifier-confusion.pdf"
    plot_ideal_confusion_matrix(
        rt_metrics,
        RT_STRATEGIES,
        RT_DETECTED_STRATEGIES,
        rt_pdf,
        title="RT-based 4x4 classifier validation",
    )
    plot_ideal_confusion_matrix(
        fixed_metrics,
        FIXED_STRATEGIES,
        FIXED_DETECTED_STRATEGIES,
        fixed_pdf,
        title="Fixed-interval 4x25 classifier validation",
    )
    plot_impaired_confusion_matrices(lossy_metrics, reordered_metrics, impaired_pdf)

    return {
        "dataset": dataset_path,
        "rt_based_pdf": rt_pdf,
        "rt_based_json": rt_json,
        "fixed_interval_pdf": fixed_pdf,
        "fixed_interval_json": fixed_json,
        "impaired_pdf": impaired_pdf,
        "impaired_json": impaired_json,
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
) -> None:
    outputs = validate_classifier(
        samples_per_strategy=samples_per_strategy,
        seed=seed,
    )
    for name, path in outputs.items():
        typer.echo(f"{name}: {path}")


if __name__ == "__main__":
    app()
