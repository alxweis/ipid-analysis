"""Plot production-oriented synthetic RANDOM-compatibility score CDFs.

The plot and the production fixed-interval classifier share this score
implementation. Unlike the more expensive increment-view diagnostics, the
score is designed to scale to large fixed-interval datasets:

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
    _ecdf_coordinates,
    generate_chi2_sequences,
)
from ipid_analysis.strategies import (  # noqa: E402
    MAX_INC,
    MODULUS,
    RANDOM_STRUCTURE_BOUNDED_INCREMENT_NULL_PROBABILITY,
    RANDOM_STRUCTURE_SCORE_VERSION,
    RANDOM_STRUCTURE_UNIFORMITY_BINS,
    STRATEGY_COLORS,
    STRATEGY_PRETTY,
    MeasurementConfig,
    RandomStructureFeatures,
    random_structure_bounded_increment_pvalues,
    random_structure_features,
    random_structure_scores,
)

app = typer.Typer()

SCORE_VERSION = RANDOM_STRUCTURE_SCORE_VERSION
DEFAULT_STRUCTURE_SAMPLES_PER_STRATEGY = 100_000
DEFAULT_THRESHOLD_SAMPLES = 1_000_000
DEFAULT_RANDOM_FALSE_REJECTION_RATE = 0.0001
UNIFORMITY_BINS = RANDOM_STRUCTURE_UNIFORMITY_BINS
MIN_COMPATIBILITY_SCORE = 1e-20
BOUNDED_INCREMENT_NULL_PROBABILITY = RANDOM_STRUCTURE_BOUNDED_INCREMENT_NULL_PROBABILITY
X_AXIS_MAXIMUM = 1.05
X_AXIS_LEFT_PADDING_DECADES = 1
X_MAJOR_EXPONENT_STEP = 2
THRESHOLD_COLOR = "#C62828"
CALIBRATION_FILENAME = f"random-score-calibration-{SCORE_VERSION}.json"
SCORE_CONFIG = MeasurementConfig(
    connection_count=CONNECTION_COUNT,
    requests_per_connection=REQUESTS_PER_CONNECTION,
    request_ip_ids=np.empty(0, dtype=np.int64),
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


RawFeatures = RandomStructureFeatures


def calculate_raw_features(
    values: np.ndarray,
    loss_mask: np.ndarray,
) -> RawFeatures:
    """Expose the production feature calculation to the evaluation plot."""
    return random_structure_features(values, ~loss_mask)


def bounded_increment_pvalues(
    values: np.ndarray,
    loss_mask: np.ndarray,
) -> np.ndarray:
    """Expose the production sequence-view component to the evaluation plot."""
    return random_structure_bounded_increment_pvalues(
        values,
        ~loss_mask,
        SCORE_CONFIG,
    )


def calculate_scores(values: np.ndarray, loss_mask: np.ndarray) -> np.ndarray:
    """Production score, clipped only to make zero values visible on log axes."""
    return np.clip(
        random_structure_scores(
            values,
            ~loss_mask,
            SCORE_CONFIG,
        ),
        MIN_COMPATIBILITY_SCORE,
        1.0,
    )


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
    positive_minimum = min(
        threshold,
        *(float(values[values > 0].min()) for values in scores.values()),
    )
    minimum_exponent = math.floor(math.log10(positive_minimum))
    axis_minimum_exponent = min(
        -1,
        minimum_exponent - X_AXIS_LEFT_PADDING_DECADES,
    )
    exponents = np.arange(axis_minimum_exponent, 1, dtype=int)
    major_mask = (exponents % X_MAJOR_EXPONENT_STEP) == 0
    major_ticks = np.power(10.0, exponents[major_mask].astype(float))
    minor_ticks = np.power(10.0, exponents[~major_mask].astype(float))
    return 10.0**axis_minimum_exponent, major_ticks, minor_ticks


def _floor_only_strategies(scores: dict[str, np.ndarray]) -> list[str]:
    """Return strategies whose complete CDF is censored at the plotting floor."""
    return [
        strategy
        for strategy in PLOT_STRATEGIES
        if np.all(scores[strategy] <= MIN_COMPATIBILITY_SCORE)
    ]


def plot_score_cdf(
    scores: dict[str, np.ndarray],
    threshold: float,
    output_path: Path,
    *,
    dataset_label: str,
) -> Path:
    configure_paper_style()
    fig, ax = plt.subplots(figsize=(7.16, 3.15))
    floor_only_strategies = _floor_only_strategies(scores)
    for strategy in PLOT_STRATEGIES:
        x_values, cumulative_percentages = _ecdf_coordinates(scores[strategy])
        ax.step(
            x_values,
            cumulative_percentages,
            where="post",
            color=STRATEGY_COLORS[strategy],
            linewidth=1.7,
        )
    if floor_only_strategies:
        marker_percentages = np.linspace(
            15.0,
            85.0,
            len(floor_only_strategies),
        )
        for strategy, percentage in zip(
            floor_only_strategies,
            marker_percentages,
            strict=True,
        ):
            ax.scatter(
                [MIN_COMPATIBILITY_SCORE],
                [percentage],
                color=STRATEGY_COLORS[strategy],
                edgecolors="white",
                linewidths=0.35,
                s=18,
                zorder=3,
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
    ax.set_xlabel(r"Random-Compatibility Score $S$")
    ax.set_ylabel("Cumulative Percentage [%]")
    ax.grid(which="major", color="#BDBDBD", linestyle="--", linewidth=0.5, alpha=0.7)
    ax.grid(which="minor", axis="y", color="#D9D9D9", linestyle=":", linewidth=0.35)

    handles = [
        Line2D(
            [0],
            [0],
            color=STRATEGY_COLORS[strategy],
            linewidth=1.7,
            marker="o" if strategy in floor_only_strategies else None,
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
            label=rf"Threshold $\tau$",
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
