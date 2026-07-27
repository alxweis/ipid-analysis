"""Plot production-oriented synthetic RANDOM-compatibility score CDFs.

This remains an independent classifier diagnostic and does not change the
production strategy classifier.  Unlike the more expensive increment-view
diagnostics, this score is designed to scale to large fixed-interval datasets:

* one sort of the present 16-bit IP-ID values,
* an exact discrete occupancy/collision tail probability,
* a conservative circular maximum-gap tail bound,
* an analytic 16-bin Pearson uniformity p-value,
* one linear bounded-increment support pass over full/destination/connection
  families to retain power for damaged counters.

The raw-value components are invariant to sample order.  Reordering can only
change the inexpensive bounded-increment component.
Threshold calibration is independent of the plotted strategy samples and is
cached as a versioned artifact for reproducible, inexpensive reruns.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
import json
import math
from pathlib import Path

import matplotlib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer

matplotlib.use("Agg")

from matplotlib.lines import Line2D  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import LogFormatterMathtext, MultipleLocator, NullFormatter  # noqa: E402
from scipy.special import betainc, gammaincc  # noqa: E402

from ipid_analysis.classifier_validation import apply_fixed_interval_impairments  # noqa: E402
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR  # noqa: E402
from ipid_analysis.paper_figures import configure_paper_style  # noqa: E402
from ipid_analysis.plot_chi2_pvalue_cdf import (  # noqa: E402
    CONNECTION_COUNT,
    DEFAULT_SEED,
    IDEAL_DATASET,
    IDEAL_SEQUENCE_LENGTH,
    LOSS_FRACTION,
    LOSSY_DATASET,
    LOSSY_REORDERED_DATASET,
    PLOT_STRATEGIES,
    PRESENT_SEQUENCE_LENGTH,
    REORDER_FRACTION,
    REQUESTS_PER_CONNECTION,
    TRIVIAL_SAMPLES_PER_STRATEGY,
    TRIVIAL_STRATEGIES,
    generate_chi2_sequences,
)
from ipid_analysis.strategies import (  # noqa: E402
    MAX_INC,
    MODULUS,
    STRATEGY_COLORS,
    STRATEGY_PRETTY,
)

app = typer.Typer()

SCORE_VERSION = "raw-multiset-bounded-v3"
DEFAULT_STRUCTURE_SAMPLES_PER_STRATEGY = 10_000
DEFAULT_THRESHOLD_SAMPLES = 10_000
DEFAULT_RANDOM_FALSE_REJECTION_RATE = 0.01
UNIFORMITY_BINS = 16
MIN_TEST_SAMPLES = 2
BOUNDED_INCREMENT_NULL_PROBABILITY = MAX_INC / MODULUS
X_AXIS_MAXIMUM = 1.05
X_AXIS_LEFT_PADDING_DECADES = 1
MAX_DENSE_LOG_DECADES = 25
ZERO_PANEL_WIDTH_RATIO = 0.55
LOG_PANEL_WIDTH_RATIO = 6.61
THRESHOLD_COLOR = "#C62828"
CALIBRATION_FILENAME = f"random-score-calibration-{SCORE_VERSION}.json"

SCORE_SCHEMA = pa.schema(
    [
        ("DATASET", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.string()),
        ("SAMPLE_INDEX", pa.int32()),
        ("RANDOM_COMPATIBILITY_SCORE", pa.float64()),
        ("IS_RANDOM_COMPATIBLE", pa.bool_()),
    ]
)


@dataclass(frozen=True)
class RawFeatures:
    sample_count: np.ndarray
    unique_count: np.ndarray
    maximum_gap: np.ndarray
    uniformity_pvalue: np.ndarray
    occupancy_pvalue: np.ndarray
    maximum_gap_pvalue: np.ndarray


@lru_cache(maxsize=1)
def _occupancy_cdf_table() -> np.ndarray:
    """P(D <= d) for n draws over 2**16 values, n,d <= sequence length."""
    maximum = IDEAL_SEQUENCE_LENGTH
    table = np.ones((maximum + 1, maximum + 1), dtype=float)
    distribution = np.zeros(maximum + 1, dtype=float)
    distribution[0] = 1.0

    for sample_count in range(1, maximum + 1):
        previous = distribution
        distribution = np.zeros_like(previous)
        distinct = np.arange(1, sample_count + 1)
        distribution[distinct] = (
            previous[distinct] * distinct / MODULUS
            + previous[distinct - 1] * (MODULUS - distinct + 1) / MODULUS
        )
        table[sample_count] = np.cumsum(distribution)
    return table


def calculate_raw_features(values: np.ndarray, loss_mask: np.ndarray) -> RawFeatures:
    """Calculate every score component with one row-wise sort."""
    present = ~loss_mask
    sample_count = present.sum(axis=1).astype(np.int16)
    values_u32 = values.astype(np.uint32, copy=False)
    sentinel = np.uint32(MODULUS)
    ordered = np.sort(np.where(present, values_u32, sentinel), axis=1)
    width = ordered.shape[1]

    adjacent_active = np.arange(width - 1)[None, :] < (sample_count[:, None] - 1)
    interior_gaps = ordered[:, 1:] - ordered[:, :-1]
    repeated = adjacent_active & (interior_gaps == 0)
    unique_count = sample_count - repeated.sum(axis=1)

    last_index = np.clip(sample_count - 1, 0, width - 1)
    first = ordered[:, 0]
    last = ordered[np.arange(len(ordered)), last_index]
    wrap_gap = MODULUS - last.astype(np.int64) + first.astype(np.int64)
    active_gaps = np.where(adjacent_active, interior_gaps, 0)
    maximum_gap = np.maximum(active_gaps.max(axis=1), wrap_gap).astype(np.int64)

    row_count = len(values)
    bins = (values_u32 * UNIFORMITY_BINS) // MODULUS
    rows = np.broadcast_to(np.arange(row_count)[:, None], values.shape)
    flat_bin = (rows * UNIFORMITY_BINS + bins)[present]
    counts = np.bincount(
        flat_bin,
        minlength=row_count * UNIFORMITY_BINS,
    ).reshape(row_count, UNIFORMITY_BINS)
    expected = np.where(sample_count > 0, sample_count / UNIFORMITY_BINS, 1.0)[:, None]
    chi2 = ((counts - expected) ** 2 / expected).sum(axis=1)
    uniformity_pvalue = gammaincc((UNIFORMITY_BINS - 1) / 2.0, chi2 / 2.0)

    occupancy_table = _occupancy_cdf_table()
    occupancy_pvalue = occupancy_table[
        np.clip(sample_count, 0, IDEAL_SEQUENCE_LENGTH),
        np.clip(unique_count, 0, IDEAL_SEQUENCE_LENGTH),
    ]

    gap_fraction = np.clip(maximum_gap / MODULUS, 0.0, 1.0)
    maximum_gap_pvalue = np.minimum(
        1.0,
        sample_count * np.power(1.0 - gap_fraction, np.maximum(sample_count - 1, 0)),
    )

    return RawFeatures(
        sample_count=sample_count,
        unique_count=unique_count,
        maximum_gap=maximum_gap,
        uniformity_pvalue=uniformity_pvalue,
        occupancy_pvalue=occupancy_pvalue,
        maximum_gap_pvalue=maximum_gap_pvalue,
    )


def _binomial_upper_tail(
    success_count: np.ndarray,
    sample_count: np.ndarray,
) -> np.ndarray:
    pvalues = np.ones(len(sample_count), dtype=float)
    valid = (sample_count >= MIN_TEST_SAMPLES) & (success_count > 0)
    pvalues[valid] = betainc(
        success_count[valid],
        sample_count[valid] - success_count[valid] + 1,
        BOUNDED_INCREMENT_NULL_PROBABILITY,
    )
    return pvalues


def _increment_counts(
    values: np.ndarray,
    present: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    pair_present = present[:, :-1] & present[:, 1:]
    increments = (
        values[:, 1:].astype(np.uint32) - values[:, :-1].astype(np.uint32)
    ) & 0xFFFF
    bounded = pair_present & (increments >= 1) & (increments <= MAX_INC)
    return bounded.sum(axis=1).astype(np.int64), pair_present.sum(axis=1).astype(np.int64)


def bounded_increment_pvalues(
    values: np.ndarray,
    loss_mask: np.ndarray,
) -> np.ndarray:
    """Minimum exact support p-value over three pooled logical families."""
    present = ~loss_mask
    full_success, full_count = _increment_counts(values, present)

    destination_components = [
        _increment_counts(values[:, index::2], present[:, index::2])
        for index in range(2)
    ]
    destination_success = sum(component[0] for component in destination_components)
    destination_count = sum(component[1] for component in destination_components)

    connections = values.reshape(
        len(values),
        REQUESTS_PER_CONNECTION,
        CONNECTION_COUNT,
    ).transpose(0, 2, 1)
    connection_present = present.reshape(
        len(values),
        REQUESTS_PER_CONNECTION,
        CONNECTION_COUNT,
    ).transpose(0, 2, 1)
    connection_components = [
        _increment_counts(connections[:, index], connection_present[:, index])
        for index in range(CONNECTION_COUNT)
    ]
    connection_success = sum(component[0] for component in connection_components)
    connection_count = sum(component[1] for component in connection_components)

    return np.minimum.reduce(
        [
            _binomial_upper_tail(full_success, full_count),
            _binomial_upper_tail(destination_success, destination_count),
            _binomial_upper_tail(connection_success, connection_count),
        ]
    )


def calculate_scores(values: np.ndarray, loss_mask: np.ndarray) -> np.ndarray:
    """Fast RANDOM score for fixed-interval residuals."""
    features = calculate_raw_features(values, loss_mask)
    scores = np.minimum.reduce(
        [
            features.uniformity_pvalue,
            features.occupancy_pvalue,
            features.maximum_gap_pvalue,
            bounded_increment_pvalues(values, loss_mask),
        ]
    )
    return np.clip(scores, 0.0, 1.0)


def _random_calibration_datasets(
    sample_count: int,
    sequence_rng: np.random.Generator,
    impairment_rng: np.random.Generator,
) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    ideal = sequence_rng.integers(
        0,
        MODULUS,
        size=(sample_count, IDEAL_SEQUENCE_LENGTH),
        dtype=np.uint16,
    )
    loss_mask, lossy, reordered = apply_fixed_interval_impairments(
        ideal,
        impairment_rng,
        loss_fraction=LOSS_FRACTION,
        reorder_fraction=REORDER_FRACTION,
    )
    return {
        IDEAL_DATASET: (ideal, np.zeros_like(ideal, dtype=bool)),
        LOSSY_DATASET: (lossy, loss_mask),
        LOSSY_REORDERED_DATASET: (reordered, loss_mask),
    }


def _calibration_key(
    *,
    sample_count: int,
    false_rejection_rate: float,
    seed: int,
) -> dict:
    return {
        "score_version": SCORE_VERSION,
        "sample_count_per_dataset": sample_count,
        "false_rejection_rate": false_rejection_rate,
        "seed": seed,
        "uniformity_bins": UNIFORMITY_BINS,
        "bounded_increment_maximum": MAX_INC,
        "ideal_sequence_length": IDEAL_SEQUENCE_LENGTH,
        "loss_fraction": LOSS_FRACTION,
    }


def _write_json(value: dict, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(output_path)
    return output_path


def load_or_calibrate_threshold(
    cache_path: Path,
    *,
    sample_count: int,
    false_rejection_rate: float,
    seed: int,
) -> tuple[float, dict[str, float], dict[str, int], bool]:
    if sample_count < 2:
        raise ValueError("threshold sample count must be at least 2")
    if not 0.0 < false_rejection_rate < 1.0:
        raise ValueError("false rejection rate must lie strictly between 0 and 1")

    key = _calibration_key(
        sample_count=sample_count,
        false_rejection_rate=false_rejection_rate,
        seed=seed,
    )
    if cache_path.is_file():
        cached = json.loads(cache_path.read_text())
        if cached.get("key") == key:
            return (
                float(cached["tau"]),
                {name: float(value) for name, value in cached["dataset_lower_quantiles"].items()},
                {name: int(value) for name, value in cached["random_below_tau"].items()},
                True,
            )

    threshold_rng, impairment_rng = [
        np.random.default_rng(child)
        for child in np.random.SeedSequence(seed).spawn(2)
    ]
    scores = {
        dataset: calculate_scores(values, mask)
        for dataset, (values, mask) in _random_calibration_datasets(
            sample_count,
            threshold_rng,
            impairment_rng,
        ).items()
    }
    quantiles = {
        dataset: float(np.quantile(values, false_rejection_rate, method="lower"))
        for dataset, values in scores.items()
    }
    tau = min(quantiles.values())
    below = {
        dataset: int((values < tau).sum())
        for dataset, values in scores.items()
    }
    _write_json(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "key": key,
            "tau": tau,
            "dataset_lower_quantiles": quantiles,
            "random_below_tau": below,
        },
        cache_path,
    )
    return tau, quantiles, below, False


def _log_axis_parameters(
    scores: dict[str, np.ndarray],
    threshold: float,
) -> tuple[float, np.ndarray, np.ndarray]:
    positive_values = [
        float(values[values > 0].min())
        for values in scores.values()
        if np.any(values > 0)
    ]
    positive_minimum = min(threshold, *positive_values)
    minimum_exponent = math.floor(math.log10(positive_minimum))
    padded_exponent = min(-1, minimum_exponent - X_AXIS_LEFT_PADDING_DECADES)
    exponent_span = -padded_exponent
    if exponent_span <= MAX_DENSE_LOG_DECADES:
        exponent_step = 1
    elif exponent_span <= 50:
        exponent_step = 5
    elif exponent_span <= 100:
        exponent_step = 10
    elif exponent_span <= 200:
        exponent_step = 20
    else:
        exponent_step = 40
    axis_minimum_exponent = exponent_step * math.floor(
        padded_exponent / exponent_step
    )
    major_ticks = np.power(
        10.0,
        np.arange(axis_minimum_exponent, 1, exponent_step, dtype=float),
    )
    if exponent_step == 1:
        minor_ticks = np.concatenate(
            [
                np.arange(2, 10, dtype=float) * 10.0**exponent
                for exponent in range(axis_minimum_exponent, 0)
            ]
        )
    else:
        minor_ticks = np.power(
            10.0,
            np.arange(
                axis_minimum_exponent + exponent_step / 2,
                0,
                exponent_step,
                dtype=float,
            ),
        )
    return 10.0**axis_minimum_exponent, major_ticks, minor_ticks


def _zero_percentages(scores: dict[str, np.ndarray]) -> dict[str, float]:
    """Return the exact S=0 probability mass of every strategy."""
    return {
        strategy: float(100.0 * np.mean(scores[strategy] == 0.0))
        for strategy in PLOT_STRATEGIES
    }


def _positive_ecdf_coordinates(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """CDF coordinates for S>0, starting at the probability mass at S=0."""
    positive = np.sort(values[values > 0])
    if not len(positive):
        return np.array([], dtype=float), np.array([], dtype=float)
    zero_count = int((values == 0.0).sum())
    percentages = 100.0 * (
        zero_count + np.arange(1, len(positive) + 1)
    ) / len(values)
    return (
        np.concatenate(([positive[0]], positive)),
        np.concatenate(([100.0 * zero_count / len(values)], percentages)),
    )


def plot_score_cdf(
    scores: dict[str, np.ndarray],
    threshold: float,
    output_path: Path,
    *,
    dataset_label: str,
) -> Path:
    configure_paper_style()
    fig, (zero_ax, log_ax) = plt.subplots(
        1,
        2,
        figsize=(7.16, 3.15),
        sharey=True,
        gridspec_kw={
            "width_ratios": [ZERO_PANEL_WIDTH_RATIO, LOG_PANEL_WIDTH_RATIO],
            "wspace": 0.05,
        },
    )
    zero_percentages = _zero_percentages(scores)
    zero_strategies = [
        strategy
        for strategy in PLOT_STRATEGIES
        if zero_percentages[strategy] > 0.0
    ]
    for strategy in PLOT_STRATEGIES:
        color = STRATEGY_COLORS[strategy]
        zero_percentage = zero_percentages[strategy]
        if zero_percentage > 0.0:
            zero_ax.vlines(
                0.0,
                0.0,
                zero_percentage,
                color=color,
                linewidth=1.7,
            )
        x_values, cumulative_percentages = _positive_ecdf_coordinates(
            scores[strategy]
        )
        if len(x_values):
            log_ax.step(
                x_values,
                cumulative_percentages,
                where="post",
                color=color,
                linewidth=1.7,
            )

    for index, strategy in enumerate(zero_strategies, start=1):
        zero_ax.scatter(
            [0.0],
            [
                zero_percentages[strategy]
                * index
                / (len(zero_strategies) + 1)
            ],
            color=STRATEGY_COLORS[strategy],
            edgecolors="white",
            linewidths=0.35,
            s=18,
            zorder=3,
        )

    log_ax.axvline(
        threshold,
        color=THRESHOLD_COLOR,
        linestyle="--",
        linewidth=1.2,
        zorder=1.5,
    )

    zero_ax.set_xlim(-0.5, 0.5)
    zero_ax.set_xticks([0.0])
    zero_ax.set_xticklabels(["0"])
    zero_ax.spines["right"].set_visible(False)
    zero_ax.tick_params(axis="x", which="minor", bottom=False)

    axis_minimum, major_ticks, minor_ticks = _log_axis_parameters(scores, threshold)
    log_ax.set_xscale("log")
    log_ax.set_xlim(axis_minimum, X_AXIS_MAXIMUM)
    log_ax.set_xticks(major_ticks)
    log_ax.set_xticks(minor_ticks, minor=True)
    log_ax.xaxis.set_major_formatter(LogFormatterMathtext(base=10))
    log_ax.xaxis.set_minor_formatter(NullFormatter())
    log_ax.spines["left"].set_visible(False)
    log_ax.tick_params(axis="y", which="both", left=False, labelleft=False)

    break_size = 0.012
    break_style = {
        "color": "black",
        "clip_on": False,
        "linewidth": 0.8,
    }
    zero_ax.plot(
        (1 - break_size, 1 + break_size),
        (-break_size, +break_size),
        transform=zero_ax.transAxes,
        **break_style,
    )
    zero_ax.plot(
        (1 - break_size, 1 + break_size),
        (1 - break_size, 1 + break_size),
        transform=zero_ax.transAxes,
        **break_style,
    )
    log_ax.plot(
        (-break_size, +break_size),
        (-break_size, +break_size),
        transform=log_ax.transAxes,
        **break_style,
    )
    log_ax.plot(
        (-break_size, +break_size),
        (1 - break_size, 1 + break_size),
        transform=log_ax.transAxes,
        **break_style,
    )

    zero_ax.set_ylim(0, 103)
    zero_ax.yaxis.set_major_locator(MultipleLocator(20))
    zero_ax.yaxis.set_minor_locator(MultipleLocator(10))
    zero_ax.set_ylabel("Cumulative Percentage [%]")
    for axis in (zero_ax, log_ax):
        axis.grid(
            which="major",
            color="#BDBDBD",
            linestyle="--",
            linewidth=0.5,
            alpha=0.7,
        )
        axis.grid(
            which="minor",
            axis="y",
            color="#D9D9D9",
            linestyle=":",
            linewidth=0.35,
        )

    handles = [
        Line2D(
            [0],
            [0],
            color=STRATEGY_COLORS[strategy],
            linewidth=1.7,
            marker="o" if strategy in zero_strategies else None,
            markersize=4,
            label=STRATEGY_PRETTY[strategy],
        )
        for strategy in PLOT_STRATEGIES
    ]
    handles.append(
        Line2D(
            [0],
            [0],
            color=THRESHOLD_COLOR,
            linestyle="--",
            linewidth=1.2,
            label=rf"Threshold $\tau={threshold:.1e}$",
        )
    )
    fig.legend(
        handles=handles,
        ncol=5,
        loc="upper center",
        bbox_to_anchor=(0.56, 0.995),
        frameon=False,
        columnspacing=1.0,
        handlelength=2.2,
    )
    fig.supxlabel(
        r"RANDOM-Compatibility Score $S$",
        x=0.56,
        y=0.055,
    )
    fig.subplots_adjust(left=0.12, right=0.995, bottom=0.22, top=0.70)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_path,
        format="pdf",
        bbox_inches="tight",
        metadata={
            "Title": (
                "Production-oriented RANDOM-compatibility score distributions "
                f"by IP-ID selection strategy ({dataset_label})"
            ),
            "Subject": f"Synthetic 4x25 order-invariant CDFs ({dataset_label})",
            "Creator": "ipid-analysis",
        },
    )
    plt.close(fig)
    return output_path


def _write_scores(
    datasets: dict[str, dict[str, np.ndarray]],
    threshold: float,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    with pq.ParquetWriter(temporary, SCORE_SCHEMA, compression="zstd") as writer:
        for dataset, strategy_scores in datasets.items():
            for strategy in PLOT_STRATEGIES:
                scores = strategy_scores[strategy]
                row_count = len(scores)
                writer.write_table(
                    pa.Table.from_arrays(
                        [
                            pa.array([dataset] * row_count, type=pa.string()),
                            pa.array([strategy] * row_count, type=pa.string()),
                            pa.array(np.arange(row_count, dtype=np.int32)),
                            pa.array(scores, type=pa.float64()),
                            pa.array(scores >= threshold, type=pa.bool_()),
                        ],
                        schema=SCORE_SCHEMA,
                    )
                )
    temporary.replace(output_path)
    return output_path


def render(
    *,
    samples_per_strategy: int = DEFAULT_STRUCTURE_SAMPLES_PER_STRATEGY,
    threshold_samples: int = DEFAULT_THRESHOLD_SAMPLES,
    false_rejection_rate: float = DEFAULT_RANDOM_FALSE_REJECTION_RATE,
    seed: int = DEFAULT_SEED,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
) -> tuple[Path, Path, Path, Path, Path, Path, Path]:
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    processed_dir = processed_root / "classifier-validation"
    figure_dir = figures_root / "classifier-validation"
    threshold, calibration_quantiles, calibration_below, cache_hit = (
        load_or_calibrate_threshold(
            processed_dir / CALIBRATION_FILENAME,
            sample_count=threshold_samples,
            false_rejection_rate=false_rejection_rate,
            seed=seed,
        )
    )

    sequence_rng, impairment_rng = [
        np.random.default_rng(child)
        for child in np.random.SeedSequence(seed).spawn(2)
    ]
    ideal_sequences = generate_chi2_sequences(samples_per_strategy, sequence_rng)
    datasets: dict[str, dict[str, np.ndarray]] = {
        IDEAL_DATASET: {},
        LOSSY_DATASET: {},
        LOSSY_REORDERED_DATASET: {},
    }
    for strategy in PLOT_STRATEGIES:
        ideal = ideal_sequences.pop(strategy)
        loss_mask, lossy, reordered = apply_fixed_interval_impairments(
            ideal,
            impairment_rng,
            loss_fraction=LOSS_FRACTION,
            reorder_fraction=REORDER_FRACTION,
        )
        datasets[IDEAL_DATASET][strategy] = calculate_scores(
            ideal,
            np.zeros_like(ideal, dtype=bool),
        )
        lossy_scores = calculate_scores(lossy, loss_mask)
        datasets[LOSSY_DATASET][strategy] = lossy_scores
        datasets[LOSSY_REORDERED_DATASET][strategy] = calculate_scores(
            reordered,
            loss_mask,
        )

    aggregate_path = _write_scores(
        datasets,
        threshold,
        processed_dir / "random-structure-score-cdf.pq",
    )

    labels = {
        IDEAL_DATASET: "Ideal Dataset",
        LOSSY_DATASET: "Lossy Dataset",
        LOSSY_REORDERED_DATASET: "Lossy+Reordered Dataset",
    }
    paths = {}
    for dataset, strategy_scores in datasets.items():
        pdf_path = plot_score_cdf(
            strategy_scores,
            threshold,
            figure_dir / f"random-structure-score-cdf-{dataset}.pdf",
            dataset_label=labels[dataset],
        )
        summaries = {}
        for strategy, values in strategy_scores.items():
            summaries[strategy] = {
                "minimum": float(values.min()),
                "q25": float(np.quantile(values, 0.25)),
                "median": float(np.median(values)),
                "q75": float(np.quantile(values, 0.75)),
                "maximum": float(values.max()),
                "zero_count": int((values == 0.0).sum()),
                "zero_percentage": float(100.0 * (values == 0.0).mean()),
                "below_threshold_count": int((values < threshold).sum()),
                "below_threshold_percentage": float(100.0 * (values < threshold).mean()),
                "random_compatible_count": int((values >= threshold).sum()),
                "random_compatible_percentage": float(100.0 * (values >= threshold).mean()),
            }
        metadata = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "seed": seed,
            "dataset": dataset,
            "connection_count": CONNECTION_COUNT,
            "requests_per_connection": REQUESTS_PER_CONNECTION,
            "ideal_sequence_length": IDEAL_SEQUENCE_LENGTH,
            "present_ipids_per_sequence": (
                IDEAL_SEQUENCE_LENGTH if dataset == IDEAL_DATASET else PRESENT_SEQUENCE_LENGTH
            ),
            "loss_fraction": 0.0 if dataset == IDEAL_DATASET else LOSS_FRACTION,
            "reorder_fraction_of_present": (
                REORDER_FRACTION if dataset == LOSSY_REORDERED_DATASET else 0.0
            ),
            "samples_per_nontrivial_strategy": samples_per_strategy,
            "trivial_samples_per_strategy": TRIVIAL_SAMPLES_PER_STRATEGY,
            "trivial_strategies": sorted(TRIVIAL_STRATEGIES),
            "score": {
                "version": SCORE_VERSION,
                "definition": "minimum production-oriented compatibility score",
                "components": [
                    f"analytic Pearson Chi-square p-value with {UNIFORMITY_BINS} bins",
                    "exact discrete occupancy/collision lower-tail probability",
                    "conservative circular maximum-gap upper-tail bound",
                    (
                        "exact upper-tail Binomial support for increments in "
                        f"[1, {MAX_INC}] over pooled full/destination/connection families"
                    ),
                ],
                "sorts_per_sequence": 1,
                "uniformity_bins": UNIFORMITY_BINS,
                "raw_components_reordering_invariant": True,
                "bounded_increment_null_probability": (
                    BOUNDED_INCREMENT_NULL_PROBABILITY
                ),
                "range": "[0, 1] without a positive score floor",
                "zero_score_representation": (
                    "separate linear S=0 panel; logarithmic S>0 panel"
                ),
                "random_compatible_when": "S >= tau",
            },
            "threshold": {
                "tau": threshold,
                "target_global_random_false_rejection_rate": false_rejection_rate,
                "calibration_samples_per_dataset": threshold_samples,
                "dataset_lower_quantiles": calibration_quantiles,
                "chosen_as": "minimum dataset lower quantile",
                "calibration_random_below_tau": calibration_below,
                "cache": str(processed_dir / CALIBRATION_FILENAME),
                "cache_hit": cache_hit,
            },
            "figure": str(pdf_path),
            "aggregate": str(aggregate_path),
            "summary_by_strategy": summaries,
        }
        json_path = _write_json(
            metadata,
            figure_dir / f"random-structure-score-cdf-{dataset}.json",
        )
        paths[dataset] = (pdf_path, json_path)

    return (
        paths[IDEAL_DATASET][0],
        paths[IDEAL_DATASET][1],
        paths[LOSSY_DATASET][0],
        paths[LOSSY_DATASET][1],
        paths[LOSSY_REORDERED_DATASET][0],
        paths[LOSSY_REORDERED_DATASET][1],
        aggregate_path,
    )


@app.command()
def main(
    samples_per_strategy: int = typer.Option(
        DEFAULT_STRUCTURE_SAMPLES_PER_STRATEGY,
        min=1,
        help=(
            "synthetic sequences per nontrivial strategy; REFLECTION and CONSTANT always use 1000"
        ),
    ),
    threshold_samples: int = typer.Option(
        DEFAULT_THRESHOLD_SAMPLES,
        min=2,
        help="independent RANDOM sequences per dataset for cached threshold calibration",
    ),
    false_rejection_rate: float = typer.Option(
        DEFAULT_RANDOM_FALSE_REJECTION_RATE,
        min=0.0,
        max=1.0,
        help="target global false-rejection rate for synthetic RANDOM",
    ),
    seed: int = typer.Option(DEFAULT_SEED, help="deterministic random seed"),
) -> None:
    outputs = render(
        samples_per_strategy=samples_per_strategy,
        threshold_samples=threshold_samples,
        false_rejection_rate=false_rejection_rate,
        seed=seed,
    )
    names = (
        "ideal_pdf",
        "ideal_json",
        "lossy_pdf",
        "lossy_json",
        "lossy_reordered_pdf",
        "lossy_reordered_json",
        "aggregate",
    )
    for name, path in zip(names, outputs, strict=True):
        typer.echo(f"{name}: {path}")


if __name__ == "__main__":
    app()
