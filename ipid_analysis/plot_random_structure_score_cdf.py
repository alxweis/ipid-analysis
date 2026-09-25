"""Plot the selected validation-only RANDOM-candidate score CDFs.

The ``mass-4x25-random-score-cdf-*`` artifacts exactly match the candidate selected by
the independent v2 evaluation: the minimum of raw-IPID uniformity, empirical
increment uniformity, and empirical circular gap uniformity. The production
classifier remains unchanged.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import matplotlib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer

matplotlib.use("Agg")

from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator, NullFormatter

from ipid_analysis.classifier_validation import apply_fixed_interval_impairments, apply_reordering
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.paper_figures import configure_paper_style
from ipid_analysis.plot_chi2_pvalue_cdf import (
    CONNECTION_COUNT,
    DEFAULT_SEED,
    IDEAL_SEQUENCE_LENGTH,
    LOSS_FRACTION,
    PLOT_STRATEGIES,
    PRESENT_SEQUENCE_LENGTH,
    REORDER_FRACTION,
    REQUESTS_PER_CONNECTION,
    TRIVIAL_SAMPLES_PER_STRATEGY,
    TRIVIAL_STRATEGIES,
    generate_chi2_sequences,
)
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_NULL_TABLE_SAMPLES,
    CANDIDATE_NULL_TABLE_SEED,
    CANDIDATE_NULL_TABLE_VERSION,
    CANDIDATE_RANDOM_METRICS,
    CANDIDATE_RANDOM_MIN_SCORE,
    CANDIDATE_RANDOM_SCORE_VERSION,
    CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE,
    candidate_random_scores,
)
from ipid_analysis.random_classifier_evaluation import EmpiricalNullTables
from ipid_analysis.strategies import (
    STRATEGY_COLORS,
    STRATEGY_PRETTY,
)

app = typer.Typer()

SCORE_VERSION = CANDIDATE_RANDOM_SCORE_VERSION
DEFAULT_STRUCTURE_SAMPLES_PER_STRATEGY = 100_000
DEFAULT_NULL_TABLE_SAMPLES = CANDIDATE_NULL_TABLE_SAMPLES
DEFAULT_RANDOM_FALSE_REJECTION_RATE = CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE
POSITIVE_SCORE_AXIS_MINIMUM = 1e-6
X_AXIS_MAXIMUM = 1.05
X_MAJOR_EXPONENT_STEP = 2
THRESHOLD_COLOR = "#C62828"
MASS_IDEAL_DATASET = "ideal"
MASS_LOSSY_DATASET = "lossy"
MASS_REORDERED_DATASET = "reordered"
MASS_LOSSY_REORDERED_DATASET = "lossy-reordered"

SCORE_SCHEMA = pa.schema(
    [
        ("DATASET", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.string()),
        ("SAMPLE_INDEX", pa.int32()),
        ("RANDOM_COMPATIBILITY_SCORE", pa.float64()),
        ("IS_RANDOM_COMPATIBLE", pa.bool_()),
    ]
)


def calculate_scores(
    values: np.ndarray,
    loss_mask: np.ndarray,
    null_tables: EmpiricalNullTables,
) -> np.ndarray:
    """Return the uncensored candidate score, including exact zero values."""
    return candidate_random_scores(
        values,
        ~loss_mask,
        null_tables,
    )


def _write_json(value: dict, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(output_path)
    return output_path


def _log_axis_parameters() -> tuple[float, np.ndarray, np.ndarray]:
    axis_minimum_exponent = int(np.log10(POSITIVE_SCORE_AXIS_MINIMUM))
    exponents = np.arange(axis_minimum_exponent, 1, dtype=int)
    major_mask = (exponents % X_MAJOR_EXPONENT_STEP) == 0
    major_ticks = np.power(10.0, exponents[major_mask].astype(float))
    minor_ticks = np.power(10.0, exponents[~major_mask].astype(float))
    return 10.0**axis_minimum_exponent, major_ticks, minor_ticks


def _subminimum_only_strategies(scores: dict[str, np.ndarray]) -> list[str]:
    """Return strategies entirely below the positive log-axis boundary."""
    return [
        strategy
        for strategy in PLOT_STRATEGIES
        if np.all(scores[strategy] < POSITIVE_SCORE_AXIS_MINIMUM)
    ]


def _positive_ecdf_coordinates(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """ECDF coordinates above the log boundary, retaining censored mass."""
    visible = values[values >= POSITIVE_SCORE_AXIS_MINIMUM]
    if not len(visible):
        return np.empty(0, dtype=float), np.empty(0, dtype=float)
    ordered = np.sort(visible)
    censored_count = len(values) - len(visible)
    percentages = 100.0 * (censored_count + np.arange(1, len(ordered) + 1)) / len(values)
    return (
        np.concatenate(([ordered[0]], ordered)),
        np.concatenate(([100.0 * censored_count / len(values)], percentages)),
    )


def plot_score_cdf(
    scores: dict[str, np.ndarray],
    threshold: float,
    output_path: Path,
    *,
    dataset_label: str,
) -> Path:
    configure_paper_style()
    fig = plt.figure(figsize=(6.75, 3.15))
    grid = fig.add_gridspec(1, 2, width_ratios=(0.055, 0.945), wspace=0.065)
    subminimum_ax = fig.add_subplot(grid[0, 0])
    ax = fig.add_subplot(grid[0, 1], sharey=subminimum_ax)
    subminimum_only_strategies = _subminimum_only_strategies(scores)
    for strategy in PLOT_STRATEGIES:
        values = scores[strategy]
        subminimum_percentage = (
            100.0 * np.count_nonzero(values < POSITIVE_SCORE_AXIS_MINIMUM) / len(values)
        )
        if subminimum_percentage:
            subminimum_ax.vlines(
                0.0,
                0.0,
                subminimum_percentage,
                color=STRATEGY_COLORS[strategy],
                linewidth=1.7,
            )
        x_values, cumulative_percentages = _positive_ecdf_coordinates(values)
        if len(x_values):
            ax.step(
                x_values,
                cumulative_percentages,
                where="post",
                color=STRATEGY_COLORS[strategy],
                linewidth=1.7,
            )
    if subminimum_only_strategies:
        marker_positions = np.linspace(
            -0.18,
            0.18,
            len(subminimum_only_strategies),
        )
        for strategy, position in zip(
            subminimum_only_strategies,
            marker_positions,
            strict=True,
        ):
            subminimum_ax.scatter(
                [position],
                [100.0],
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

    axis_minimum, major_ticks, minor_ticks = _log_axis_parameters()
    ax.set_xscale("log")
    ax.set_xlim(axis_minimum, X_AXIS_MAXIMUM)
    major_exponents = np.log10(major_ticks).astype(int)
    major_labels = [rf"$10^{{{exponent}}}$" for exponent in major_exponents]
    ax.set_xticks(major_ticks, labels=major_labels)
    ax.set_xticks(minor_ticks, minor=True)
    ax.xaxis.set_minor_formatter(NullFormatter())
    subminimum_ax.set_xlim(-0.5, 0.5)
    subminimum_exponent = int(np.log10(POSITIVE_SCORE_AXIS_MINIMUM))
    subminimum_ax.set_xticks([0.0], labels=[rf"$<10^{{{subminimum_exponent}}}$"])
    subminimum_ax.get_xticklabels()[0].set_horizontalalignment("right")
    ax.get_xticklabels()[0].set_horizontalalignment("left")
    for current_ax in (subminimum_ax, ax):
        current_ax.set_ylim(0, 103)
        current_ax.yaxis.set_major_locator(MultipleLocator(20))
        current_ax.yaxis.set_minor_locator(MultipleLocator(10))
    subminimum_ax.set_ylabel("Cumulative Percentage [%]")
    ax.tick_params(axis="y", which="both", left=False, labelleft=False)
    subminimum_ax.tick_params(axis="y", which="both", right=False)
    subminimum_ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    break_size = 0.025
    break_style = {"color": "black", "clip_on": False, "linewidth": 0.9}
    subminimum_ax.plot(
        (1.0 - break_size, 1.0 + break_size),
        (-break_size, break_size),
        transform=subminimum_ax.transAxes,
        **break_style,
    )
    subminimum_ax.plot(
        (1.0 - break_size, 1.0 + break_size),
        (1.0 - break_size, 1.0 + break_size),
        transform=subminimum_ax.transAxes,
        **break_style,
    )
    ax.plot(
        (-break_size, break_size),
        (-break_size, break_size),
        transform=ax.transAxes,
        **break_style,
    )
    ax.plot(
        (-break_size, break_size),
        (1.0 - break_size, 1.0 + break_size),
        transform=ax.transAxes,
        **break_style,
    )
    fig.supxlabel(r"Random-Compatibility Score $S$", y=0.045)
    subminimum_ax.grid(
        which="major", axis="y", color="#BDBDBD", linestyle="--", linewidth=0.5, alpha=0.7
    )
    subminimum_ax.grid(which="minor", axis="y", color="#D9D9D9", linestyle=":", linewidth=0.35)
    ax.grid(which="major", color="#BDBDBD", linestyle="--", linewidth=0.5, alpha=0.7)
    ax.grid(which="minor", axis="y", color="#D9D9D9", linestyle=":", linewidth=0.35)

    handles = [
        Line2D(
            [0],
            [0],
            color=STRATEGY_COLORS[strategy],
            linewidth=1.7,
            marker="o" if strategy in subminimum_only_strategies else None,
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
            label=r"Threshold $\tau$",
        )
    )
    fig.legend(
        handles=handles,
        ncol=5,
        loc="upper center",
        bbox_to_anchor=(0.53, 0.985),
        frameon=False,
        columnspacing=0.85,
        handlelength=2.0,
    )
    fig.subplots_adjust(left=0.13, right=0.995, bottom=0.23, top=0.70)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_path,
        format="pdf",
        bbox_inches="tight",
        metadata={
            "Title": (
                "Selected RANDOM-candidate score distributions "
                f"by IP-ID selection strategy ({dataset_label})"
            ),
            "Subject": f"Synthetic 4x25 candidate-score CDFs ({dataset_label})",
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
    null_table_samples: int = DEFAULT_NULL_TABLE_SAMPLES,
    null_table_seed: int = CANDIDATE_NULL_TABLE_SEED,
    threshold: float = CANDIDATE_RANDOM_MIN_SCORE,
    seed: int = DEFAULT_SEED,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
) -> tuple[Path, Path, Path, Path, Path, Path, Path, Path, Path]:
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")
    if null_table_samples < 1:
        raise ValueError("null_table_samples must be positive")
    if not 0.0 <= threshold <= 1.0:
        raise ValueError("threshold must lie in [0, 1]")

    processed_dir = processed_root / "classifier-validation"
    figure_dir = figures_root / "classifier-validation"
    for prefix in ("chi2-pvalue-cdf", "random-structure-score-cdf"):
        for dataset in ("ideal", "lossy", "lossy-reordered"):
            (figure_dir / f"{prefix}-{dataset}.pdf").unlink(missing_ok=True)
            (figure_dir / f"{prefix}-{dataset}.json").unlink(missing_ok=True)
        (processed_dir / f"{prefix}.pq").unlink(missing_ok=True)
    null_tables = EmpiricalNullTables(null_table_samples, null_table_seed)

    sequence_rng, impairment_rng, reorder_rng = [
        np.random.default_rng(child) for child in np.random.SeedSequence(seed).spawn(3)
    ]
    ideal_sequences = generate_chi2_sequences(samples_per_strategy, sequence_rng)
    datasets: dict[str, dict[str, np.ndarray]] = {
        MASS_IDEAL_DATASET: {},
        MASS_LOSSY_DATASET: {},
        MASS_REORDERED_DATASET: {},
        MASS_LOSSY_REORDERED_DATASET: {},
    }
    for strategy in PLOT_STRATEGIES:
        ideal = ideal_sequences.pop(strategy)
        loss_mask, lossy, reordered = apply_fixed_interval_impairments(
            ideal,
            impairment_rng,
            loss_fraction=LOSS_FRACTION,
            reorder_fraction=REORDER_FRACTION,
        )
        reorder_only = apply_reordering(ideal, reorder_rng, reordered_count=20)
        datasets[MASS_IDEAL_DATASET][strategy] = calculate_scores(
            ideal,
            np.zeros_like(ideal, dtype=bool),
            null_tables,
        )
        lossy_scores = calculate_scores(lossy, loss_mask, null_tables)
        datasets[MASS_LOSSY_DATASET][strategy] = lossy_scores
        datasets[MASS_REORDERED_DATASET][strategy] = calculate_scores(
            reorder_only,
            np.zeros_like(ideal, dtype=bool),
            null_tables,
        )
        datasets[MASS_LOSSY_REORDERED_DATASET][strategy] = calculate_scores(
            reordered,
            loss_mask,
            null_tables,
        )

    aggregate_path = _write_scores(
        datasets,
        threshold,
        processed_dir / "mass-4x25-random-score-cdf.pq",
    )

    labels = {
        MASS_IDEAL_DATASET: "Mass 4x25 Ideal Dataset",
        MASS_LOSSY_DATASET: "Mass 4x25 Lossy Dataset",
        MASS_REORDERED_DATASET: "Mass 4x25 Reordered Dataset",
        MASS_LOSSY_REORDERED_DATASET: "Mass 4x25 Lossy+Reordered Dataset",
    }
    paths = {}
    for dataset, strategy_scores in datasets.items():
        pdf_path = plot_score_cdf(
            strategy_scores,
            threshold,
            figure_dir / f"mass-4x25-random-score-cdf-{dataset}.pdf",
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
                IDEAL_SEQUENCE_LENGTH
                if dataset in {MASS_IDEAL_DATASET, MASS_REORDERED_DATASET}
                else PRESENT_SEQUENCE_LENGTH
            ),
            "loss_fraction": (
                LOSS_FRACTION
                if dataset in {MASS_LOSSY_DATASET, MASS_LOSSY_REORDERED_DATASET}
                else 0.0
            ),
            "reorder_fraction_of_present": (
                REORDER_FRACTION
                if dataset in {MASS_REORDERED_DATASET, MASS_LOSSY_REORDERED_DATASET}
                else 0.0
            ),
            "samples_per_nontrivial_strategy": samples_per_strategy,
            "trivial_samples_per_strategy": TRIVIAL_SAMPLES_PER_STRATEGY,
            "trivial_strategies": sorted(TRIVIAL_STRATEGIES),
            "score": {
                "version": SCORE_VERSION,
                "definition": "minimum selected RANDOM-candidate compatibility score",
                "components": list(CANDIDATE_RANDOM_METRICS),
                "combiner": "minimum",
                "raw_uniformity_bins": 16,
                "increment_uniformity_order_invariant": False,
                "gap_uniformity_order_invariant": True,
                "null_tables": {
                    "version": CANDIDATE_NULL_TABLE_VERSION,
                    "sample_count": null_table_samples,
                    "seed": null_table_seed,
                    "pvalue_resolution": 1.0 / (null_table_samples + 1.0),
                },
                "validation_only": True,
                "production_classifier_changed": False,
                "random_compatible_when": "S >= tau",
            },
            "threshold": {
                "tau": threshold,
                "target_global_random_false_rejection_rate": (DEFAULT_RANDOM_FALSE_REJECTION_RATE),
                "selected_by": "independent held-out random-classifier evaluation v2",
            },
            "figure": str(pdf_path),
            "figure_axis": {
                "subminimum_scores": (
                    f"separate categorical panel for S < {POSITIVE_SCORE_AXIS_MINIMUM}"
                ),
                "positive_scores": "logarithmic panel",
                "positive_display_minimum": POSITIVE_SCORE_AXIS_MINIMUM,
                "scores_below_display_minimum": "shown in the separate subminimum panel",
            },
            "aggregate": str(aggregate_path),
            "summary_by_strategy": summaries,
        }
        json_path = _write_json(
            metadata,
            figure_dir / f"mass-4x25-random-score-cdf-{dataset}.json",
        )
        paths[dataset] = (pdf_path, json_path)

    return (
        paths[MASS_IDEAL_DATASET][0],
        paths[MASS_IDEAL_DATASET][1],
        paths[MASS_LOSSY_DATASET][0],
        paths[MASS_LOSSY_DATASET][1],
        paths[MASS_REORDERED_DATASET][0],
        paths[MASS_REORDERED_DATASET][1],
        paths[MASS_LOSSY_REORDERED_DATASET][0],
        paths[MASS_LOSSY_REORDERED_DATASET][1],
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
    null_table_samples: int = typer.Option(
        DEFAULT_NULL_TABLE_SAMPLES,
        min=1,
        help="Monte Carlo samples per empirical candidate null table",
    ),
    null_table_seed: int = typer.Option(
        CANDIDATE_NULL_TABLE_SEED,
        help="deterministic seed for empirical candidate null tables",
    ),
    threshold: float = typer.Option(
        CANDIDATE_RANDOM_MIN_SCORE,
        min=0.0,
        max=1.0,
        help="selected candidate minimum-score threshold",
    ),
    seed: int = typer.Option(DEFAULT_SEED, help="deterministic random seed"),
) -> None:
    outputs = render(
        samples_per_strategy=samples_per_strategy,
        null_table_samples=null_table_samples,
        null_table_seed=null_table_seed,
        threshold=threshold,
        seed=seed,
    )
    names = (
        "ideal_pdf",
        "ideal_json",
        "lossy_pdf",
        "lossy_json",
        "reordered_pdf",
        "reordered_json",
        "lossy_reordered_pdf",
        "lossy_reordered_json",
        "aggregate",
    )
    for name, path in zip(names, outputs, strict=True):
        typer.echo(f"{name}: {path}")


if __name__ == "__main__":
    app()
