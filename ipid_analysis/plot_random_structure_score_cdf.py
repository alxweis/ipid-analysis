"""Plot synthetic RANDOM-compatibility structure-score CDFs.

This is an independent classifier diagnostic.  It does not change the
production strategy classifier.  The score combines four small, complementary
tests over the raw IP-IDs and the logical full/destination/connection views:

* discrete KS-D for marginal non-uniformity,
* two-sided circular Greenwood spacings for clustering or over-regularity,
* discrete KS-D for second differences (serial structure),
* exact Binomial support for counter increments in the bounded 1..21845 range.

All component statistics are converted to empirical p-values using simulated
discrete-uniform null distributions of the matching sample length.  Their
minimum is the RANDOM-compatibility score S.  A single global threshold is
calibrated on separate synthetic RANDOM sequences and shared by all datasets.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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
from scipy.special import betainc  # noqa: E402

from ipid_analysis.classifier_validation import apply_fixed_interval_impairments  # noqa: E402
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR  # noqa: E402
from ipid_analysis.paper_figures import configure_paper_style  # noqa: E402
from ipid_analysis.plot_chi2_pvalue_cdf import (  # noqa: E402
    CONNECTION_COUNT,
    DEFAULT_SAMPLES_PER_STRATEGY,
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
    _ecdf_coordinates,
    generate_chi2_sequences,
)
from ipid_analysis.strategies import (  # noqa: E402
    MAX_INC,
    MODULUS,
    STRATEGY_COLORS,
    STRATEGY_PRETTY,
)

app = typer.Typer()

DEFAULT_NULL_SAMPLES_PER_LENGTH = 10_000
DEFAULT_THRESHOLD_SAMPLES = 10_000
DEFAULT_RANDOM_FALSE_REJECTION_RATE = 0.01
MIN_TEST_SAMPLES = 2
BOUNDED_INCREMENT_NULL_PROBABILITY = MAX_INC / MODULUS
X_AXIS_MAXIMUM = 1.05
THRESHOLD_COLOR = "#C62828"

RAW_VIEW = "raw"
INCREMENT_VIEWS = (
    "full",
    "destination-0",
    "destination-1",
    "connection-0",
    "connection-1",
    "connection-2",
    "connection-3",
)

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
class Statistic:
    sample_count: np.ndarray
    value: np.ndarray
    tail: str


def _ks_d(values: np.ndarray, present: np.ndarray) -> Statistic:
    """One-sample KS-D against the discrete uniform 16-bit distribution."""
    sample_count = present.sum(axis=1).astype(np.int16)
    ordered = np.sort(np.where(present, values, MODULUS), axis=1)
    width = values.shape[1]
    ranks = np.arange(1, width + 1, dtype=float)[None, :]
    active = ranks <= sample_count[:, None]
    denominator = np.maximum(sample_count, 1)[:, None]
    uniform_cdf = (ordered.astype(float) + 0.5) / MODULUS
    d_plus = np.where(active, ranks / denominator - uniform_cdf, -np.inf)
    d_minus = np.where(active, uniform_cdf - (ranks - 1.0) / denominator, -np.inf)
    statistic = np.maximum(d_plus.max(axis=1), d_minus.max(axis=1))
    statistic = np.where(sample_count >= MIN_TEST_SAMPLES, statistic, np.nan)
    return Statistic(sample_count, statistic, "upper")


def _greenwood(values: np.ndarray, present: np.ndarray) -> Statistic:
    """Dimensionless circular Greenwood spacing statistic."""
    sample_count = present.sum(axis=1).astype(np.int16)
    ordered = np.sort(np.where(present, values, MODULUS), axis=1)
    width = values.shape[1]
    interior = np.diff(ordered, axis=1).astype(float)
    interior_active = np.arange(width - 1)[None, :] < (sample_count[:, None] - 1)
    scaled = sample_count[:, None] * interior / MODULUS - 1.0
    statistic = np.where(interior_active, scaled * scaled, 0.0).sum(axis=1)

    last_index = np.clip(sample_count - 1, 0, width - 1)
    wrap = (MODULUS - ordered[np.arange(len(ordered)), last_index] + ordered[:, 0]).astype(float)
    wrap_scaled = sample_count * wrap / MODULUS - 1.0
    statistic += wrap_scaled * wrap_scaled
    statistic = np.where(sample_count >= MIN_TEST_SAMPLES, statistic, np.nan)
    return Statistic(sample_count, statistic, "two-sided")


def _logical_views(
    values: np.ndarray,
    present: np.ndarray,
) -> dict[str, tuple[np.ndarray, np.ndarray]]:
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
    views = {
        "full": (values, present),
        "destination-0": (values[:, 0::2], present[:, 0::2]),
        "destination-1": (values[:, 1::2], present[:, 1::2]),
    }
    views.update(
        {
            f"connection-{index}": (
                connections[:, index, :],
                connection_present[:, index, :],
            )
            for index in range(CONNECTION_COUNT)
        }
    )
    return views


def _adjacent_increments(
    values: np.ndarray,
    present: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    increment_present = present[:, :-1] & present[:, 1:]
    increments = (values[:, 1:] - values[:, :-1]) & 0xFFFF
    return increments, increment_present


def _second_differences(
    values: np.ndarray,
    present: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    difference_present = present[:, :-2] & present[:, 1:-1] & present[:, 2:]
    differences = (values[:, 2:] - 2 * values[:, 1:-1] + values[:, :-2]) & 0xFFFF
    return differences, difference_present


def _bounded_increment_support(
    increments: np.ndarray,
    present: np.ndarray,
) -> Statistic:
    """Count valid increments in the deterministic counter range."""
    sample_count = present.sum(axis=1).astype(np.int16)
    bounded = present & (increments >= 1) & (increments <= MAX_INC)
    return Statistic(
        sample_count,
        bounded.sum(axis=1).astype(float),
        "binomial-upper",
    )


def calculate_statistics(
    values: np.ndarray,
    loss_mask: np.ndarray,
) -> dict[str, Statistic]:
    """Calculate the raw and logical-view statistics for every sequence."""
    values = values.astype(np.int64, copy=False)
    present = ~loss_mask
    statistics = {
        f"{RAW_VIEW}.ks": _ks_d(values, present),
        f"{RAW_VIEW}.greenwood": _greenwood(values, present),
    }
    increments_by_view = {}
    for view_name, (view_values, view_present) in _logical_views(values, present).items():
        increments, increment_present = _adjacent_increments(view_values, view_present)
        increments_by_view[view_name] = (increments, increment_present)
        second_differences, second_present = _second_differences(
            view_values,
            view_present,
        )
        statistics[f"{view_name}.increment.ks"] = _ks_d(
            increments,
            increment_present,
        )
        statistics[f"{view_name}.increment.greenwood"] = _greenwood(
            increments,
            increment_present,
        )
        statistics[f"{view_name}.second-difference.ks"] = _ks_d(
            second_differences,
            second_present,
        )

    bounded_families = {
        "full": (increments_by_view["full"],),
        "destination": tuple(increments_by_view[f"destination-{index}"] for index in range(2)),
        "connection": tuple(
            increments_by_view[f"connection-{index}"] for index in range(CONNECTION_COUNT)
        ),
    }
    for family, components in bounded_families.items():
        family_increments = np.concatenate(
            [component[0] for component in components],
            axis=1,
        )
        family_present = np.concatenate(
            [component[1] for component in components],
            axis=1,
        )
        statistics[f"{family}.increment.bounded-support"] = _bounded_increment_support(
            family_increments, family_present
        )
    return statistics


def build_null_tables(
    samples_per_length: int,
    rng: np.random.Generator,
) -> dict[str, dict[int, np.ndarray]]:
    """Discrete-uniform reference distributions indexed by statistic and length."""
    if samples_per_length < 2:
        raise ValueError("samples_per_length must be at least 2")
    tables: dict[str, dict[int, np.ndarray]] = {"ks": {}, "greenwood": {}}
    for sample_count in range(MIN_TEST_SAMPLES, IDEAL_SEQUENCE_LENGTH + 1):
        values = rng.integers(
            0,
            MODULUS,
            size=(samples_per_length, sample_count),
            dtype=np.uint16,
        ).astype(np.int64)
        present = np.ones_like(values, dtype=bool)
        tables["ks"][sample_count] = np.sort(_ks_d(values, present).value)
        tables["greenwood"][sample_count] = np.sort(_greenwood(values, present).value)
    return tables


def _empirical_pvalues(
    statistic: Statistic,
    references: dict[int, np.ndarray],
) -> np.ndarray:
    pvalues = np.ones(len(statistic.value), dtype=float)
    valid = np.isfinite(statistic.value) & (statistic.sample_count >= MIN_TEST_SAMPLES)
    for sample_count in np.unique(statistic.sample_count[valid]):
        selected = valid & (statistic.sample_count == sample_count)
        observed = statistic.value[selected]
        reference = references[int(sample_count)]
        denominator = len(reference) + 1.0
        upper = (
            len(reference) - np.searchsorted(reference, observed, side="left") + 1.0
        ) / denominator
        if statistic.tail == "upper":
            pvalues[selected] = upper
        else:
            lower = (np.searchsorted(reference, observed, side="right") + 1.0) / denominator
            pvalues[selected] = np.minimum(1.0, 2.0 * np.minimum(lower, upper))
    return pvalues


def _bounded_support_pvalues(statistic: Statistic) -> np.ndarray:
    """Exact upper-tail Binomial p-values under 16-bit uniform increments."""
    sample_count = statistic.sample_count.astype(np.int64)
    bounded_count = statistic.value.astype(np.int64)
    pvalues = np.ones(len(bounded_count), dtype=float)
    positive = (sample_count >= MIN_TEST_SAMPLES) & (bounded_count > 0)
    pvalues[positive] = betainc(
        bounded_count[positive],
        sample_count[positive] - bounded_count[positive] + 1,
        BOUNDED_INCREMENT_NULL_PROBABILITY,
    )
    return pvalues


def calculate_scores(
    values: np.ndarray,
    loss_mask: np.ndarray,
    null_tables: dict[str, dict[int, np.ndarray]],
) -> np.ndarray:
    """Minimum empirical p-value over every valid structure test and view."""
    statistics = calculate_statistics(values, loss_mask)
    component_pvalues = []
    for name, statistic in statistics.items():
        if statistic.tail == "binomial-upper":
            component_pvalues.append(_bounded_support_pvalues(statistic))
        else:
            table_name = "greenwood" if name.endswith(".greenwood") else "ks"
            component_pvalues.append(_empirical_pvalues(statistic, null_tables[table_name]))
    return np.min(np.stack(component_pvalues, axis=1), axis=1)


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


def calibrate_threshold(
    null_tables: dict[str, dict[int, np.ndarray]],
    sample_count: int,
    false_rejection_rate: float,
    sequence_rng: np.random.Generator,
    impairment_rng: np.random.Generator,
) -> tuple[float, dict[str, np.ndarray], dict[str, float]]:
    if sample_count < 2:
        raise ValueError("threshold sample count must be at least 2")
    if not 0.0 < false_rejection_rate < 1.0:
        raise ValueError("false rejection rate must lie strictly between 0 and 1")

    scores = {
        dataset: calculate_scores(values, mask, null_tables)
        for dataset, (values, mask) in _random_calibration_datasets(
            sample_count,
            sequence_rng,
            impairment_rng,
        ).items()
    }
    quantiles = {
        dataset: float(np.quantile(values, false_rejection_rate, method="lower"))
        for dataset, values in scores.items()
    }
    # One conservative threshold shared by every impairment condition.
    return min(quantiles.values()), scores, quantiles


def calculate_strategy_scores(
    sequences: dict[str, np.ndarray],
    loss_masks: dict[str, np.ndarray],
    null_tables: dict[str, dict[int, np.ndarray]],
) -> dict[str, np.ndarray]:
    return {
        strategy: calculate_scores(sequences[strategy], loss_masks[strategy], null_tables)
        for strategy in PLOT_STRATEGIES
    }


def _log_axis_parameters(
    scores: dict[str, np.ndarray],
    threshold: float,
) -> tuple[float, np.ndarray, np.ndarray]:
    positive_minimum = min(
        threshold,
        *(float(values[values > 0].min()) for values in scores.values()),
    )
    minimum_exponent = math.floor(math.log10(positive_minimum))
    axis_minimum_exponent = min(-1, minimum_exponent)
    major_ticks = np.power(
        10.0,
        np.arange(axis_minimum_exponent, 1, dtype=float),
    )
    minor_ticks = np.concatenate(
        [
            np.arange(2, 10, dtype=float) * 10.0**exponent
            for exponent in range(axis_minimum_exponent, 0)
        ]
    )
    return 10.0**axis_minimum_exponent, major_ticks, minor_ticks


def plot_score_cdf(
    scores: dict[str, np.ndarray],
    threshold: float,
    output_path: Path,
    *,
    dataset_label: str,
) -> Path:
    configure_paper_style()
    fig, ax = plt.subplots(figsize=(7.16, 3.15))
    for strategy in PLOT_STRATEGIES:
        x_values, cumulative_percentages = _ecdf_coordinates(scores[strategy])
        ax.step(
            x_values,
            cumulative_percentages,
            where="post",
            color=STRATEGY_COLORS[strategy],
            linewidth=1.7,
        )
    ax.axvline(
        threshold,
        color=THRESHOLD_COLOR,
        linestyle="--",
        linewidth=1.2,
        zorder=1.5,
    )

    axis_minimum, major_ticks, minor_ticks = _log_axis_parameters(scores, threshold)
    ax.set_xscale("log")
    ax.set_xlim(axis_minimum, X_AXIS_MAXIMUM)
    ax.set_xticks(major_ticks)
    ax.set_xticks(minor_ticks, minor=True)
    ax.xaxis.set_major_formatter(LogFormatterMathtext(base=10))
    ax.xaxis.set_minor_formatter(NullFormatter())
    ax.set_ylim(0, 103)
    ax.yaxis.set_major_locator(MultipleLocator(20))
    ax.yaxis.set_minor_locator(MultipleLocator(10))
    ax.set_xlabel(r"RANDOM-Compatibility Score $S$")
    ax.set_ylabel("Cumulative Percentage [%]")
    ax.grid(which="major", color="#BDBDBD", linestyle="--", linewidth=0.5, alpha=0.7)
    ax.grid(which="minor", axis="y", color="#D9D9D9", linestyle=":", linewidth=0.35)

    handles = [
        Line2D(
            [0],
            [0],
            color=STRATEGY_COLORS[strategy],
            linewidth=1.7,
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
    ax.legend(
        handles=handles,
        ncol=5,
        loc="lower center",
        bbox_to_anchor=(0.5, 1.015),
        frameon=False,
        columnspacing=1.0,
        handlelength=2.2,
    )
    fig.subplots_adjust(left=0.12, right=0.995, bottom=0.22, top=0.70)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_path,
        format="pdf",
        bbox_inches="tight",
        metadata={
            "Title": (
                "RANDOM-compatibility structure-score distributions by IP-ID "
                f"selection strategy ({dataset_label})"
            ),
            "Subject": f"Synthetic 4x25 empirical CDFs ({dataset_label})",
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
    rows = [
        {
            "DATASET": dataset,
            "IPID_SELECTION_STRATEGY": strategy,
            "SAMPLE_INDEX": sample_index,
            "RANDOM_COMPATIBILITY_SCORE": float(score),
            "IS_RANDOM_COMPATIBLE": bool(score >= threshold),
        }
        for dataset, strategy_scores in datasets.items()
        for strategy in PLOT_STRATEGIES
        for sample_index, score in enumerate(strategy_scores[strategy])
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=SCORE_SCHEMA), temporary, compression="zstd")
    temporary.replace(output_path)
    return output_path


def _write_json(value: dict, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(output_path)
    return output_path


def render(
    *,
    samples_per_strategy: int = DEFAULT_SAMPLES_PER_STRATEGY,
    null_samples_per_length: int = DEFAULT_NULL_SAMPLES_PER_LENGTH,
    threshold_samples: int = DEFAULT_THRESHOLD_SAMPLES,
    false_rejection_rate: float = DEFAULT_RANDOM_FALSE_REJECTION_RATE,
    seed: int = DEFAULT_SEED,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
) -> tuple[Path, Path, Path, Path, Path, Path, Path]:
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    rngs = [np.random.default_rng(child) for child in np.random.SeedSequence(seed).spawn(5)]
    sequence_rng, impairment_rng, null_rng, threshold_rng, threshold_impairment_rng = rngs
    null_tables = build_null_tables(null_samples_per_length, null_rng)
    threshold, calibration_scores, calibration_quantiles = calibrate_threshold(
        null_tables,
        threshold_samples,
        false_rejection_rate,
        threshold_rng,
        threshold_impairment_rng,
    )

    ideal_sequences = generate_chi2_sequences(samples_per_strategy, sequence_rng)
    loss_masks = {}
    lossy_sequences = {}
    reordered_sequences = {}
    for strategy in PLOT_STRATEGIES:
        loss_mask, lossy, reordered = apply_fixed_interval_impairments(
            ideal_sequences[strategy],
            impairment_rng,
            loss_fraction=LOSS_FRACTION,
            reorder_fraction=REORDER_FRACTION,
        )
        loss_masks[strategy] = loss_mask
        lossy_sequences[strategy] = lossy
        reordered_sequences[strategy] = reordered
    ideal_masks = {
        strategy: np.zeros_like(values, dtype=bool) for strategy, values in ideal_sequences.items()
    }

    datasets = {
        IDEAL_DATASET: calculate_strategy_scores(
            ideal_sequences,
            ideal_masks,
            null_tables,
        ),
        LOSSY_DATASET: calculate_strategy_scores(
            lossy_sequences,
            loss_masks,
            null_tables,
        ),
        LOSSY_REORDERED_DATASET: calculate_strategy_scores(
            reordered_sequences,
            loss_masks,
            null_tables,
        ),
    }

    processed_dir = processed_root / "classifier-validation"
    figure_dir = figures_root / "classifier-validation"
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
                "definition": "minimum empirical p-value over all valid components",
                "raw_components": ["discrete KS-D", "two-sided circular Greenwood"],
                "increment_view_components": [
                    "discrete KS-D",
                    "two-sided circular Greenwood",
                    "discrete KS-D of second differences",
                ],
                "pooled_increment_family_components": [
                    (f"exact upper-tail Binomial support test for increments in [1, {MAX_INC}]")
                ],
                "bounded_increment_null_probability": (BOUNDED_INCREMENT_NULL_PROBABILITY),
                "increment_views": list(INCREMENT_VIEWS),
                "loss_handling": (
                    "increments and second differences use only logically adjacent "
                    "present positions; missing positions are not bridged"
                ),
                "random_compatible_when": "S >= tau",
            },
            "threshold": {
                "tau": threshold,
                "target_global_random_false_rejection_rate": false_rejection_rate,
                "calibration_samples_per_dataset": threshold_samples,
                "dataset_lower_quantiles": calibration_quantiles,
                "chosen_as": "minimum dataset lower quantile",
                "calibration_random_below_tau": {
                    name: int((values < threshold).sum())
                    for name, values in calibration_scores.items()
                },
            },
            "null_calibration": {
                "distribution": "discrete uniform over [0, 65535]",
                "samples_per_sequence_length": null_samples_per_length,
                "sequence_lengths": [MIN_TEST_SAMPLES, IDEAL_SEQUENCE_LENGTH],
                "runtime_simulation": False,
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
        DEFAULT_SAMPLES_PER_STRATEGY,
        min=1,
        help=(
            "synthetic sequences per nontrivial strategy; REFLECTION and CONSTANT always use 1000"
        ),
    ),
    null_samples_per_length: int = typer.Option(
        DEFAULT_NULL_SAMPLES_PER_LENGTH,
        min=2,
        help="discrete-uniform samples per sequence length for empirical p-values",
    ),
    threshold_samples: int = typer.Option(
        DEFAULT_THRESHOLD_SAMPLES,
        min=2,
        help="independent RANDOM sequences per dataset for threshold calibration",
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
        null_samples_per_length=null_samples_per_length,
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
