"""Plot the final production RANDOM-score CDFs.

The ``mass-4x25-random-score-cdf-*`` artifacts exactly match the score selected by
the final operating-point confirmation: the minimum of raw-IPID uniformity,
hierarchically aggregated multiscale increment evidence, and empirical circular gap
uniformity. This is the score used by the v7 production mass classifier.
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
from ipid_analysis.paper_figures import (
    COMPACT_PAPER_MAJOR_TICK_LENGTH,
    COMPACT_PAPER_PDF_PADDING_INCHES,
    COMPACT_PAPER_STROKE_WIDTH,
    configure_compact_validation_style,
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
    CANDIDATE_RANDOM_SELECTION,
    CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE,
    CandidateNullTables,
    candidate_random_scores,
    create_candidate_null_tables,
)
from ipid_analysis.strategies import (
    STRATEGY_COLORS,
    STRATEGY_PRETTY,
)
from ipid_analysis.synthetic_mass_sequences import (
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
    generate_mass_paper_sequences,
)

app = typer.Typer()

SCORE_VERSION = CANDIDATE_RANDOM_SCORE_VERSION
DEFAULT_STRUCTURE_SAMPLES_PER_STRATEGY = 100_000
DEFAULT_NULL_TABLE_SAMPLES = CANDIDATE_NULL_TABLE_SAMPLES
DEFAULT_RANDOM_FALSE_REJECTION_RATE = CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE
POSITIVE_SCORE_AXIS_MINIMUM = 1e-6
X_AXIS_MAXIMUM = 1.05
X_MAJOR_EXPONENT_STEP = 1
THRESHOLD_COLOR = "#C62828"
CDF_FIGURE_SIZE_INCHES = (7.0, 2.50)
CDF_GRID_LEFT = 0.075
CDF_GRID_RIGHT = 0.985
CDF_GRID_BOTTOM = 0.215
CDF_GRID_TOP = 0.720
CDF_BROKEN_AXIS_WIDTH_RATIOS = (0.052, 0.948)
CDF_BROKEN_AXIS_SPACE = 0.055
CDF_CURVE_LINEWIDTH = 1.35
CDF_THRESHOLD_LINEWIDTH = 1.0
CDF_BREAK_MARKER_HALF_WIDTH = 0.34
CDF_XLABEL_X = 0.535
CDF_XLABEL_Y = 0.070
CDF_LEGEND_ANCHOR = (0.535, 0.735)
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
    null_tables: CandidateNullTables,
) -> np.ndarray:
    """Return the uncensored production score, including exact zero values."""
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


def _log_axis_parameters(
    positive_axis_minimum: float = POSITIVE_SCORE_AXIS_MINIMUM,
) -> tuple[float, np.ndarray, np.ndarray]:
    axis_minimum_exponent = int(np.floor(np.log10(positive_axis_minimum)))
    exponents = np.arange(axis_minimum_exponent, 1, dtype=int)
    major_mask = (exponents % X_MAJOR_EXPONENT_STEP) == 0
    major_ticks = np.power(10.0, exponents[major_mask].astype(float))
    minor_ticks = np.asarray(
        [
            multiplier * 10.0**exponent
            for exponent in range(axis_minimum_exponent, 0)
            for multiplier in range(1, 10)
            if multiplier * 10.0**exponent not in major_ticks
        ],
        dtype=float,
    )
    return positive_axis_minimum, major_ticks, minor_ticks


def _subminimum_only_strategies(
    scores: dict[str, np.ndarray],
    positive_axis_minimum: float = POSITIVE_SCORE_AXIS_MINIMUM,
) -> list[str]:
    """Return strategies entirely below the positive log-axis boundary."""
    return [
        strategy
        for strategy in PLOT_STRATEGIES
        if np.all(scores[strategy] < positive_axis_minimum)
    ]


def _positive_ecdf_coordinates(
    values: np.ndarray,
    positive_axis_minimum: float = POSITIVE_SCORE_AXIS_MINIMUM,
) -> tuple[np.ndarray, np.ndarray]:
    """ECDF coordinates above the log boundary, retaining censored mass."""
    visible = values[values >= positive_axis_minimum]
    if not len(visible):
        return np.empty(0, dtype=float), np.empty(0, dtype=float)
    ordered = np.sort(visible)
    censored_count = len(values) - len(visible)
    percentages = 100.0 * (censored_count + np.arange(1, len(ordered) + 1)) / len(values)
    return (
        np.concatenate(([ordered[0]], ordered)),
        np.concatenate(([100.0 * censored_count / len(values)], percentages)),
    )


def _ecdf_marker_coordinates(
    values: np.ndarray,
    percentages: np.ndarray,
    positive_axis_minimum: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Return actual ECDF sample points for sparse, non-displacing markers."""
    ordered = np.sort(values)
    indices = np.ceil(percentages * len(ordered) / 100.0).astype(int) - 1
    indices = np.clip(indices, 0, len(ordered) - 1)
    x_values = ordered[indices]
    y_values = 100.0 * (indices + 1) / len(ordered)
    visible = x_values >= positive_axis_minimum
    return x_values[visible], y_values[visible]


def plot_score_cdf(
    scores: dict[str, np.ndarray],
    threshold: float,
    output_path: Path,
    *,
    dataset_label: str,
    x_label: str = r"Random-Compatibility Score $S$",
    method_title: str = "Production RANDOM score",
    positive_axis_minimum: float = POSITIVE_SCORE_AXIS_MINIMUM,
    separate_subminimum_panel: bool = True,
    show_curve_markers: bool = False,
) -> Path:
    configure_compact_validation_style()
    if separate_subminimum_panel:
        fig = plt.figure(figsize=CDF_FIGURE_SIZE_INCHES)
        grid = fig.add_gridspec(
            1,
            2,
            width_ratios=CDF_BROKEN_AXIS_WIDTH_RATIOS,
            wspace=CDF_BROKEN_AXIS_SPACE,
        )
        subminimum_ax = fig.add_subplot(grid[0, 0])
        ax = fig.add_subplot(grid[0, 1], sharey=subminimum_ax)
        subminimum_only_strategies = _subminimum_only_strategies(
            scores,
            positive_axis_minimum,
        )
    else:
        fig, ax = plt.subplots(figsize=CDF_FIGURE_SIZE_INCHES)
        subminimum_ax = None
        subminimum_only_strategies = []
    for strategy_index, strategy in enumerate(PLOT_STRATEGIES):
        values = scores[strategy]
        subminimum_percentage = (
            100.0 * np.count_nonzero(values < positive_axis_minimum) / len(values)
        )
        if subminimum_ax is not None and subminimum_percentage:
            subminimum_ax.vlines(
                0.0,
                0.0,
                subminimum_percentage,
                color=STRATEGY_COLORS[strategy],
                linewidth=CDF_CURVE_LINEWIDTH,
            )
        x_values, cumulative_percentages = _positive_ecdf_coordinates(
            values,
            positive_axis_minimum,
        )
        if len(x_values):
            ax.step(
                x_values,
                cumulative_percentages,
                where="post",
                color=STRATEGY_COLORS[strategy],
                linewidth=CDF_CURVE_LINEWIDTH,
            )
        if show_curve_markers:
            marker_percentages = np.asarray([12.0, 32.0, 52.0, 72.0, 92.0])
            marker_percentages += 1.5 * (strategy_index - (len(PLOT_STRATEGIES) - 1) / 2.0)
            marker_x, marker_y = _ecdf_marker_coordinates(
                values,
                marker_percentages,
                positive_axis_minimum,
            )
            ax.scatter(
                marker_x,
                marker_y,
                color=STRATEGY_COLORS[strategy],
                edgecolors="white",
                linewidths=0.30,
                s=12,
                zorder=3,
            )
    if subminimum_ax is not None and subminimum_only_strategies:
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
        linewidth=CDF_THRESHOLD_LINEWIDTH,
        zorder=1.5,
    )

    axis_minimum, major_ticks, minor_ticks = _log_axis_parameters(positive_axis_minimum)
    ax.set_xscale("log")
    major_exponents = np.log10(major_ticks).astype(int)
    major_labels = [rf"$10^{{{exponent}}}$" for exponent in major_exponents]
    ax.set_xticks(major_ticks, labels=major_labels)
    ax.set_xticks(minor_ticks, minor=True)
    ax.xaxis.set_minor_formatter(NullFormatter())
    # Matplotlib expands limits to include explicit ticks; restore the requested
    # boundary afterwards so compact, non-decade NIST limits remain effective.
    ax.set_xlim(axis_minimum, X_AXIS_MAXIMUM)
    if subminimum_ax is not None:
        subminimum_ax.set_xlim(-0.5, 0.5)
        subminimum_exponent = int(np.log10(positive_axis_minimum))
        subminimum_ax.set_xticks([0.0], labels=[rf"$<10^{{{subminimum_exponent}}}$"])
    for current_ax in (subminimum_ax, ax) if subminimum_ax is not None else (ax,):
        current_ax.set_ylim(0, 103)
        current_ax.yaxis.set_major_locator(MultipleLocator(20))
        current_ax.yaxis.set_minor_locator(MultipleLocator(10))
    if subminimum_ax is not None:
        subminimum_ax.set_ylabel("Cumulative Percentage [%]", labelpad=2.5)
        ax.tick_params(axis="y", which="both", left=False, labelleft=False)
        subminimum_ax.tick_params(axis="y", which="both", right=False)
        subminimum_ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        break_marker = [
            (-CDF_BREAK_MARKER_HALF_WIDTH, -1.0),
            (CDF_BREAK_MARKER_HALF_WIDTH, 1.0),
        ]
        break_style = {
            "marker": break_marker,
            "markersize": 2.0 * COMPACT_PAPER_MAJOR_TICK_LENGTH,
            "linestyle": "none",
            "color": "black",
            "markeredgewidth": COMPACT_PAPER_STROKE_WIDTH,
            "clip_on": False,
        }
        subminimum_ax.plot(
            [1.0, 1.0],
            [0.0, 1.0],
            transform=subminimum_ax.transAxes,
            **break_style,
        )
        ax.plot(
            [0.0, 0.0],
            [0.0, 1.0],
            transform=ax.transAxes,
            **break_style,
        )
    else:
        ax.set_ylabel("Cumulative Percentage [%]", labelpad=2.5)
    fig.supxlabel(
        x_label,
        x=CDF_XLABEL_X,
        y=CDF_XLABEL_Y,
        fontsize=plt.rcParams["axes.labelsize"],
    )
    if subminimum_ax is not None:
        subminimum_ax.grid(
            which="major",
            axis="y",
            color="#BDBDBD",
            linestyle="--",
            linewidth=0.40,
            alpha=0.7,
        )
        subminimum_ax.grid(
            which="minor",
            axis="y",
            color="#D9D9D9",
            linestyle=":",
            linewidth=0.25,
        )
    ax.grid(which="major", color="#BDBDBD", linestyle="--", linewidth=0.40, alpha=0.7)
    ax.grid(which="minor", color="#D9D9D9", linestyle=":", linewidth=0.25, alpha=0.75)

    handles = [
        Line2D(
            [0],
            [0],
            color=STRATEGY_COLORS[strategy],
            linewidth=CDF_CURVE_LINEWIDTH,
            marker=("o" if show_curve_markers or strategy in subminimum_only_strategies else None),
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
            linewidth=CDF_THRESHOLD_LINEWIDTH,
            label=r"Threshold $\tau$",
        )
    )
    fig.legend(
        handles=handles,
        ncol=5,
        loc="lower center",
        bbox_to_anchor=CDF_LEGEND_ANCHOR,
        frameon=False,
        columnspacing=1.05,
        handlelength=2.1,
        handletextpad=0.45,
        borderaxespad=0.0,
        labelspacing=0.25,
    )
    fig.subplots_adjust(
        left=CDF_GRID_LEFT,
        right=CDF_GRID_RIGHT,
        bottom=CDF_GRID_BOTTOM,
        top=CDF_GRID_TOP,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_path,
        format="pdf",
        bbox_inches="tight",
        pad_inches=COMPACT_PAPER_PDF_PADDING_INCHES,
        metadata={
            "Title": (
                f"{method_title} distributions by IP-ID selection strategy ({dataset_label})"
            ),
            "Subject": f"Synthetic 4x25 production-score CDFs ({dataset_label})",
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
    null_tables = create_candidate_null_tables(null_table_samples, null_table_seed)

    sequence_rng, impairment_rng, reorder_rng = [
        np.random.default_rng(child) for child in np.random.SeedSequence(seed).spawn(3)
    ]
    ideal_sequences = generate_mass_paper_sequences(samples_per_strategy, sequence_rng)
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
                "definition": "minimum production RANDOM-compatibility score",
                "components": list(CANDIDATE_RANDOM_METRICS),
                "combiner": "minimum",
                "raw_uniformity_bins": 16,
                "increment_uniformity_order_invariant": False,
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
                "gap_uniformity_order_invariant": True,
                "null_tables": {
                    "version": CANDIDATE_NULL_TABLE_VERSION,
                    "sample_count": null_table_samples,
                    "base_seed": null_table_seed,
                    "component_seeds": {
                        "increment_uniformity": (
                            null_table_seed + CANDIDATE_INCREMENT_NULL_TABLE_SEED_OFFSET
                        ),
                        "gap_uniformity": (
                            null_table_seed + CANDIDATE_GAP_NULL_TABLE_SEED_OFFSET
                        ),
                    },
                    "pvalue_resolution": 1.0 / (null_table_samples + 1.0),
                },
                "validation_only": False,
                "production_classifier_changed": True,
                "random_compatible_when": "S >= tau",
            },
            "threshold": {
                "tau": threshold,
                "target_global_random_false_rejection_rate": (DEFAULT_RANDOM_FALSE_REJECTION_RATE),
                "selected_by": CANDIDATE_RANDOM_SELECTION,
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
