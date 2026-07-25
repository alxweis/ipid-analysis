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
REQUEST_IP_IDS = np.asarray([18933, 18932, 3717, 3718, 3719], dtype=np.int64)

RT_DATASET = "rt-based-4x4-ideal"
FIXED_IDEAL_DATASET = "fixed-interval-4x25-ideal"
FIXED_LOSSY_DATASET = "fixed-interval-4x25-lossy"
FIXED_REORDERED_DATASET = "fixed-interval-4x25-lossy-reordered"

RT_STRATEGIES = (
    "REFLECTION",
    "CONSTANT",
    "SINGLE",
    "PER_CONNECTION",
    "PER_DESTINATION",
    "PER_BUCKET",
    "UNCLASSIFIED",
)
FIXED_STRATEGIES = ("CONSTANT", "MULTI", "RANDOM", "UNCLASSIFIED")

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

    destination_base = rng.integers(0, 20_000, size=n, dtype=np.int64)
    per_destination = np.empty((n, length), dtype=np.uint16)
    per_destination[:, 0::2] = (destination_base[:, None] + np.arange(length // 2)) % MODULUS
    per_destination[:, 1::2] = (
        destination_base[:, None] + 30_000 + np.arange(length // 2)
    ) % MODULUS

    connection_base = rng.integers(0, 4_000, size=n, dtype=np.int64)
    connection_starts = (
        connection_base[:, None] + np.asarray([0, 10_000, 30_000, 50_000])
    ) % MODULUS
    per_connection_cube = (
        connection_starts[:, None, :]
        + np.arange(RT_REQUESTS_PER_CONNECTION, dtype=np.int64)[None, :, None]
    ) % MODULUS
    per_connection = _round_connection_flatten(per_connection_cube)

    single_starts = rng.integers(0, MODULUS, size=n, dtype=np.int64)
    single_increments = rng.integers(2, 2_001, size=(n, length - 1), dtype=np.int64)
    single = _cumulative_sequences(single_starts, single_increments)

    bucket_base = rng.integers(0, 3_000, size=n, dtype=np.int64)
    bucket_starts = (bucket_base[:, None] + np.asarray([0, 30_000, 1_000, 31_000])) % MODULUS
    bucket_increments = rng.integers(
        2,
        2_001,
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

    unknown_base = rng.integers(0, 4_000, size=n, dtype=np.int64)
    unknown_cube = np.empty(
        (n, RT_REQUESTS_PER_CONNECTION, CONNECTION_COUNT),
        dtype=np.int64,
    )
    for request_index in range(RT_REQUESTS_PER_CONNECTION):
        for connection_index in range(CONNECTION_COUNT):
            cluster = (request_index + connection_index) % 2
            unknown_cube[:, request_index, connection_index] = (
                unknown_base
                + 30_000 * cluster
                + request_index * CONNECTION_COUNT
                + connection_index
            )
    unclassified = _round_connection_flatten(unknown_cube % MODULUS)

    return config, {
        "REFLECTION": reflection,
        "CONSTANT": constant,
        "SINGLE": single,
        "PER_CONNECTION": per_connection,
        "PER_DESTINATION": per_destination,
        "PER_BUCKET": per_bucket,
        "UNCLASSIFIED": unclassified,
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

    constant_values = rng.integers(0, MODULUS, size=n, dtype=np.uint16)
    constant = np.repeat(constant_values[:, None], length, axis=1)

    multi_base = rng.integers(0, 2_000, size=n, dtype=np.int64)
    multi_starts = (multi_base[:, None] + np.asarray([0, 16_000, 32_000, 48_000])) % MODULUS
    multi_increments = rng.integers(
        1,
        17,
        size=(n, CONNECTION_COUNT, FIXED_REQUESTS_PER_CONNECTION - 1),
        dtype=np.int64,
    )
    multi_connections = np.concatenate(
        [
            multi_starts[:, :, None],
            multi_starts[:, :, None] + np.cumsum(multi_increments, axis=2, dtype=np.int64),
        ],
        axis=2,
    )
    multi = _round_connection_flatten(multi_connections.transpose(0, 2, 1) % MODULUS)

    random = np.empty((n, length), dtype=np.uint16)
    values_per_quartile = length // 4
    for row_index in range(n):
        quartiles = [
            rng.integers(
                quartile * (MODULUS // 4),
                (quartile + 1) * (MODULUS // 4),
                size=values_per_quartile,
                dtype=np.uint16,
            )
            for quartile in range(4)
        ]
        random[row_index] = rng.permutation(np.concatenate(quartiles))

    unknown_starts = rng.integers(1_000, 50_000, size=n, dtype=np.int64)
    unknown_increments = rng.integers(1, 5, size=(n, length - 1), dtype=np.int64)
    unclassified = _cumulative_sequences(unknown_starts, unknown_increments)

    return {
        "CONSTANT": constant,
        "MULTI": multi,
        "RANDOM": random,
        "UNCLASSIFIED": unclassified,
    }


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
                    int(value)
                    for column_index, value in enumerate(row)
                    if not loss_mask[row_index, column_index]
                ]
            )
    return classify_batch_mass(pa.array(rows, type=pa.list_(pa.int64())))


def _strategy_names(codes: np.ndarray) -> list[str]:
    return [IPIDStrategy(int(code)).name for code in codes]


def _confusion_metrics(
    expected: list[str],
    detected: list[str],
    classes: tuple[str, ...],
) -> dict:
    class_index = {name: index for index, name in enumerate(classes)}
    unexpected = sorted((set(expected) | set(detected)) - set(classes))
    if unexpected:
        raise ValueError(f"strategies outside validation matrix: {unexpected}")

    counts = np.zeros((len(classes), len(classes)), dtype=np.int64)
    for truth, prediction in zip(expected, detected, strict=True):
        counts[class_index[truth], class_index[prediction]] += 1

    support = counts.sum(axis=1)
    predicted_count = counts.sum(axis=0)
    correct = np.diag(counts)
    precision = np.divide(
        correct,
        predicted_count,
        out=np.zeros(len(classes), dtype=float),
        where=predicted_count > 0,
    )
    recall = np.divide(
        correct,
        support,
        out=np.zeros(len(classes), dtype=float),
        where=support > 0,
    )
    f1 = np.divide(
        2 * precision * recall,
        precision + recall,
        out=np.zeros(len(classes), dtype=float),
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
    weights = support / total if total else np.zeros(len(classes), dtype=float)

    expected_agreement = (
        float(np.dot(support, predicted_count) / (total * total)) if total else 0.0
    )
    cohen_kappa = (
        (accuracy - expected_agreement) / (1.0 - expected_agreement)
        if expected_agreement < 1.0
        else 1.0
    )
    mcc_numerator = float(correct.sum() * total - np.dot(support, predicted_count))
    mcc_denominator = float(
        np.sqrt(
            (total**2 - np.dot(predicted_count, predicted_count))
            * (total**2 - np.dot(support, support))
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
                "predicted": int(predicted_count[index]),
                "correct": int(correct[index]),
                "precision": float(precision[index]),
                "recall": float(recall[index]),
                "f1": float(f1[index]),
            }
            for index, strategy in enumerate(classes)
        },
        "confusion_matrix": {
            "class_order": list(classes),
            "counts": counts.tolist(),
            "row_percentages": percentages.tolist(),
        },
    }


def _matrix_percentages(metrics: dict) -> np.ndarray:
    return np.asarray(metrics["confusion_matrix"]["row_percentages"], dtype=float)


def _draw_confusion_matrix(
    ax,
    metrics: dict,
    classes: tuple[str, ...],
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
    labels = [STRATEGY_PRETTY[strategy] for strategy in classes]
    ax.set_xticks(
        np.arange(len(labels)),
        labels,
        rotation=35,
        ha="right",
        rotation_mode="anchor",
    )
    ax.set_yticks(np.arange(len(labels)), labels)
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
    classes: tuple[str, ...],
    output_path: Path,
    *,
    title: str,
) -> Path:
    configure_paper_style()
    figure_height = 3.75 if len(classes) > 4 else 3.25
    fig, ax = plt.subplots(figsize=(7.16, figure_height))
    image = _draw_confusion_matrix(ax, metrics, classes)
    ax.set_xlabel("Detected IP-ID Selection Strategy")
    ax.set_ylabel("Generating IP-ID\nSelection Strategy")
    colorbar = fig.colorbar(image, ax=ax, pad=0.04, fraction=0.045, ticks=np.arange(0, 101, 20))
    colorbar.set_label("Percentage [%]")
    fig.subplots_adjust(
        left=0.22 if len(classes) > 4 else 0.19,
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
        title="Lossy Dataset",
    )
    _draw_confusion_matrix(
        axes[1],
        reordered_metrics,
        FIXED_STRATEGIES,
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
            columns["EXPECTED_STRATEGY"].append(strategy)
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
    samples_per_strategy: int = 1_000,
    seed: int = 42,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
) -> dict[str, Path]:
    """Generate synthetic datasets, classify them, and write plots and metrics."""
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    seed_sequence = np.random.SeedSequence(seed)
    rt_rng, fixed_rng, impairment_rng = [
        np.random.default_rng(child) for child in seed_sequence.spawn(3)
    ]

    rt_config, rt_sequences = generate_rt_sequences(samples_per_strategy, rt_rng)
    rt_detections = {
        strategy: _strategy_names(classify_batch(values, rt_config))
        for strategy, values in rt_sequences.items()
    }

    fixed_sequences = generate_fixed_sequences(samples_per_strategy, fixed_rng)
    fixed_detections = {
        strategy: _strategy_names(_classify_mass(values))
        for strategy, values in fixed_sequences.items()
    }

    fixed_matrix = np.concatenate(
        [fixed_sequences[strategy] for strategy in FIXED_STRATEGIES],
        axis=0,
    )
    fixed_loss_mask, lossy_matrix, reordered_matrix = apply_fixed_interval_impairments(
        fixed_matrix,
        impairment_rng,
    )
    class_slices = {
        strategy: slice(
            index * samples_per_strategy,
            (index + 1) * samples_per_strategy,
        )
        for index, strategy in enumerate(FIXED_STRATEGIES)
    }
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
    rt_metrics = _confusion_metrics(rt_expected, rt_detected, RT_STRATEGIES)
    fixed_metrics = _confusion_metrics(fixed_expected, fixed_detected, FIXED_STRATEGIES)
    lossy_metrics = _confusion_metrics(lossy_expected, lossy_detected, FIXED_STRATEGIES)
    reordered_metrics = _confusion_metrics(
        reordered_expected,
        reordered_detected,
        FIXED_STRATEGIES,
    )

    columns = _empty_validation_columns()
    next_address = _append_dataset_rows(
        columns,
        dataset=RT_DATASET,
        sequences=rt_sequences,
        detections=rt_detections,
        requests_per_connection=RT_REQUESTS_PER_CONNECTION,
        address_offset=0,
    )
    fixed_address_offset = next_address
    next_address = _append_dataset_rows(
        columns,
        dataset=FIXED_IDEAL_DATASET,
        sequences=fixed_sequences,
        detections=fixed_detections,
        requests_per_connection=FIXED_REQUESTS_PER_CONNECTION,
        address_offset=fixed_address_offset,
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
        "synthetic_generator_version": "1",
        "seed": seed,
        "samples_per_strategy": samples_per_strategy,
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

    rt_pdf = figure_dir / "rt-based-4x4-classifier-confusion.pdf"
    fixed_pdf = figure_dir / "fixed-interval-4x25-classifier-confusion.pdf"
    impaired_pdf = figure_dir / "fixed-interval-4x25-impaired-classifier-confusion.pdf"
    plot_ideal_confusion_matrix(
        rt_metrics,
        RT_STRATEGIES,
        rt_pdf,
        title="RT-based 4x4 classifier validation",
    )
    plot_ideal_confusion_matrix(
        fixed_metrics,
        FIXED_STRATEGIES,
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
    }


@app.command()
def main(
    samples_per_strategy: int = typer.Option(
        1_000,
        min=1,
        help="synthetic sequences generated for every evaluated strategy",
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
