"""Plot minimum increment-subsequence Chi-square p-value CDFs."""

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

from ipid_analysis.classifier_validation import (  # noqa: E402
    REQUEST_IP_IDS,
    SYNTHETIC_GENERATOR_PARAMETERS,
    _generate_multi_sequences,
    apply_fixed_interval_impairments,
)
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR  # noqa: E402
from ipid_analysis.paper_figures import configure_paper_style  # noqa: E402
from ipid_analysis.strategies import (  # noqa: E402
    CHI2_BINS,
    MAX_INC,
    MODULUS,
    RANDOM_MIN_P_VALUE,
    STRATEGY_COLORS,
    STRATEGY_PRETTY,
    chi2_uniformity_pvalues,
)

app = typer.Typer()

CONNECTION_COUNT = 4
REQUESTS_PER_CONNECTION = 25
IDEAL_SEQUENCE_LENGTH = CONNECTION_COUNT * REQUESTS_PER_CONNECTION
PRESENT_SEQUENCE_LENGTH = 80
LOSS_FRACTION = 0.20
REORDER_FRACTION = 0.20
DEFAULT_SAMPLES_PER_STRATEGY = 10_000
TRIVIAL_SAMPLES_PER_STRATEGY = 1_000
DEFAULT_SEED = 42
X_AXIS_MAXIMUM = 1.05
X_MAJOR_EXPONENT_STEP = 20
X_MINOR_EXPONENT_OFFSET = 10
RANDOM_MINIMUM_MARKER_COLOR = "#C62828"
RANDOM_MINIMUM_MARKER_LABEL = "Minimum Random p-value"
IDEAL_DATASET = "ideal"
LOSSY_DATASET = "lossy"
LOSSY_REORDERED_DATASET = "lossy-reordered"

PLOT_STRATEGIES = (
    "REFLECTION",
    "CONSTANT",
    "SINGLE",
    "PER_DESTINATION",
    "PER_CONNECTION",
    "PER_BUCKET",
    "MULTI",
    "RANDOM",
)
TRIVIAL_STRATEGIES = frozenset({"REFLECTION", "CONSTANT"})
INCREMENT_SUBSEQUENCES = (
    "full",
    "destination-0",
    "destination-1",
    "connection-0",
    "connection-1",
    "connection-2",
    "connection-3",
)

P_VALUE_SCHEMA = pa.schema(
    [
        ("DATASET", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.string()),
        ("SAMPLE_INDEX", pa.int32()),
        ("MINIMUM_CHI2_P_VALUE", pa.float64()),
    ]
)


def _round_connection_flatten(values: np.ndarray) -> np.ndarray:
    """Flatten (sample, request round, connection) in measurement order."""
    return values.reshape(values.shape[0], -1).astype(np.uint16)


def _cumulative_sequences(starts: np.ndarray, increments: np.ndarray) -> np.ndarray:
    cumulative = np.cumsum(increments, axis=1, dtype=np.int64)
    values = np.concatenate([starts[:, None], starts[:, None] + cumulative], axis=1)
    return (values % MODULUS).astype(np.uint16)


def generate_chi2_sequences(
    samples_per_strategy: int,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """Generate ideal, measurement-shaped 4x25 sequences for all plot strategies."""
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    n = samples_per_strategy
    trivial_n = TRIVIAL_SAMPLES_PER_STRATEGY
    request_pattern = REQUEST_IP_IDS[np.arange(IDEAL_SEQUENCE_LENGTH) % len(REQUEST_IP_IDS)]

    reflection_offsets = rng.integers(0, MODULUS, size=trivial_n, dtype=np.int64)
    reflection = ((request_pattern[None, :] + reflection_offsets[:, None]) % MODULUS).astype(
        np.uint16
    )

    constant_values = rng.integers(0, MODULUS, size=trivial_n, dtype=np.uint16)
    constant = np.repeat(constant_values[:, None], IDEAL_SEQUENCE_LENGTH, axis=1)

    single_starts = rng.integers(0, MODULUS, size=n, dtype=np.int64)
    single_increments = rng.integers(
        1,
        MAX_INC + 1,
        size=(n, IDEAL_SEQUENCE_LENGTH - 1),
        dtype=np.int64,
    )
    single = _cumulative_sequences(single_starts, single_increments)

    destination_starts = rng.integers(
        0,
        MODULUS,
        size=(n, 2),
        dtype=np.int64,
    )
    per_destination = np.empty((n, IDEAL_SEQUENCE_LENGTH), dtype=np.uint16)
    destination_steps = np.arange(IDEAL_SEQUENCE_LENGTH // 2, dtype=np.int64)
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
        + np.arange(REQUESTS_PER_CONNECTION, dtype=np.int64)[None, :, None]
    ) % MODULUS
    per_connection = _round_connection_flatten(per_connection_cube)

    bucket_starts = rng.integers(
        0,
        MODULUS,
        size=(n, CONNECTION_COUNT),
        dtype=np.int64,
    )
    bucket_increments = rng.integers(
        1,
        MAX_INC + 1,
        size=(n, CONNECTION_COUNT, REQUESTS_PER_CONNECTION - 1),
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

    multi = _generate_multi_sequences(n, IDEAL_SEQUENCE_LENGTH, rng)

    random = rng.integers(
        0,
        MODULUS,
        size=(n, IDEAL_SEQUENCE_LENGTH),
        dtype=np.uint16,
    )

    return {
        "REFLECTION": reflection,
        "CONSTANT": constant,
        "SINGLE": single,
        "PER_DESTINATION": per_destination,
        "PER_CONNECTION": per_connection,
        "PER_BUCKET": per_bucket,
        "MULTI": multi,
        "RANDOM": random,
    }


def apply_strategy_impairments(
    sequences: dict[str, np.ndarray],
    rng: np.random.Generator,
) -> tuple[
    dict[str, np.ndarray],
    dict[str, np.ndarray],
    dict[str, np.ndarray],
]:
    """Apply paired 20% loss and loss+reordering to every strategy."""
    loss_masks = {}
    lossy_sequences = {}
    reordered_sequences = {}
    for strategy in PLOT_STRATEGIES:
        loss_mask, lossy, reordered = apply_fixed_interval_impairments(
            sequences[strategy],
            rng,
            loss_fraction=LOSS_FRACTION,
            reorder_fraction=REORDER_FRACTION,
        )
        loss_masks[strategy] = loss_mask
        lossy_sequences[strategy] = lossy
        reordered_sequences[strategy] = reordered
    return loss_masks, lossy_sequences, reordered_sequences


def _increment_pvalues(
    values: np.ndarray,
    present: np.ndarray,
) -> np.ndarray:
    """Chi-square p-values of modular increments between consecutive present values."""
    sample_count, subsequence_length = values.shape
    compacted = np.zeros((sample_count, subsequence_length), dtype=np.int64)
    compacted_indices = np.cumsum(present, axis=1) - 1
    row_indices = np.broadcast_to(np.arange(sample_count)[:, None], values.shape)
    compacted[row_indices[present], compacted_indices[present]] = values[present]
    present_lengths = present.sum(axis=1)
    increments = (compacted[:, 1:] - compacted[:, :-1]) & 0xFFFF
    increment_present = np.arange(subsequence_length - 1)[None, :] < (present_lengths[:, None] - 1)
    return chi2_uniformity_pvalues(increments, increment_present)


def calculate_minimum_increment_pvalues(
    values: np.ndarray,
    loss_mask: np.ndarray,
) -> np.ndarray:
    """Minimum p-value over full, destination, and connection increments."""
    present = ~loss_mask
    component_pvalues = [
        _increment_pvalues(values, present),
        _increment_pvalues(values[:, 0::2], present[:, 0::2]),
        _increment_pvalues(values[:, 1::2], present[:, 1::2]),
    ]
    value_connections = values.reshape(
        len(values),
        REQUESTS_PER_CONNECTION,
        CONNECTION_COUNT,
    ).transpose(0, 2, 1)
    present_connections = present.reshape(
        len(values),
        REQUESTS_PER_CONNECTION,
        CONNECTION_COUNT,
    ).transpose(0, 2, 1)
    component_pvalues.extend(
        _increment_pvalues(
            value_connections[:, connection_index, :],
            present_connections[:, connection_index, :],
        )
        for connection_index in range(CONNECTION_COUNT)
    )
    return np.min(np.stack(component_pvalues, axis=1), axis=1)


def calculate_strategy_pvalues(
    sequences: dict[str, np.ndarray],
    loss_masks: dict[str, np.ndarray],
) -> dict[str, np.ndarray]:
    """Calculate the minimum increment-subsequence p-value per sequence."""
    return {
        strategy: calculate_minimum_increment_pvalues(
            sequences[strategy].astype(np.int64, copy=False),
            loss_masks[strategy],
        )
        for strategy in PLOT_STRATEGIES
    }


def _ecdf_coordinates(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    ordered = np.sort(values)
    percentages = 100.0 * np.arange(1, len(ordered) + 1) / len(ordered)
    return np.concatenate(([ordered[0]], ordered)), np.concatenate(([0.0], percentages))


def _log_axis_parameters(
    pvalues: dict[str, np.ndarray],
) -> tuple[float, np.ndarray, np.ndarray]:
    minimum = min(float(values[values > 0].min()) for values in pvalues.values())
    minimum_exponent = math.floor(math.log10(minimum))
    axis_minimum_exponent = X_MAJOR_EXPONENT_STEP * math.floor(
        minimum_exponent / X_MAJOR_EXPONENT_STEP
    )
    major_ticks = np.power(
        10.0,
        np.arange(axis_minimum_exponent, 1, X_MAJOR_EXPONENT_STEP, dtype=float),
    )
    minor_ticks = np.power(
        10.0,
        np.arange(
            axis_minimum_exponent + X_MINOR_EXPONENT_OFFSET,
            1,
            X_MAJOR_EXPONENT_STEP,
            dtype=float,
        ),
    )
    return 10.0**axis_minimum_exponent, major_ticks, minor_ticks


def plot_chi2_pvalue_cdf(
    pvalues: dict[str, np.ndarray],
    output_path: Path,
    *,
    dataset_label: str,
) -> Path:
    configure_paper_style()
    fig, ax = plt.subplots(figsize=(7.16, 3.15))
    for strategy in PLOT_STRATEGIES:
        x_values, cumulative_percentages = _ecdf_coordinates(pvalues[strategy])
        ax.step(
            x_values,
            cumulative_percentages,
            where="post",
            color=STRATEGY_COLORS[strategy],
            linewidth=1.7,
        )

    random_minimum_pvalue = float(pvalues["RANDOM"].min())
    ax.axvline(
        random_minimum_pvalue,
        color=RANDOM_MINIMUM_MARKER_COLOR,
        linestyle="--",
        linewidth=1.2,
        zorder=1.5,
    )

    axis_minimum, major_ticks, minor_ticks = _log_axis_parameters(pvalues)
    ax.set_xscale("log")
    ax.set_xlim(axis_minimum, X_AXIS_MAXIMUM)
    ax.set_xticks(major_ticks)
    ax.set_xticks(minor_ticks, minor=True)
    ax.xaxis.set_major_formatter(LogFormatterMathtext(base=10))
    ax.xaxis.set_minor_formatter(NullFormatter())
    ax.tick_params(axis="x", which="major", length=5, width=0.8)
    ax.tick_params(axis="x", which="minor", length=2.8, width=0.65)
    ax.set_ylim(0, 103)
    ax.yaxis.set_major_locator(MultipleLocator(20))
    ax.yaxis.set_minor_locator(MultipleLocator(10))
    ax.set_xlabel(r"Chi$^2$ p-value [Minimum of all Subsequences]")
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
            color=RANDOM_MINIMUM_MARKER_COLOR,
            linestyle="--",
            linewidth=1.2,
            label=RANDOM_MINIMUM_MARKER_LABEL,
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
                "Minimum increment-subsequence Chi-square p-value distributions "
                f"by IP-ID selection strategy ({dataset_label})"
            ),
            "Subject": (
                "Synthetic 4x25 IP-ID increment-subsequence empirical CDFs "
                f"({dataset_label})"
            ),
            "Creator": "ipid-analysis",
        },
    )
    plt.close(fig)
    return output_path


def _write_pvalues(
    datasets: dict[str, dict[str, np.ndarray]],
    output_path: Path,
) -> Path:
    rows = [
        {
            "DATASET": dataset,
            "IPID_SELECTION_STRATEGY": strategy,
            "SAMPLE_INDEX": sample_index,
            "MINIMUM_CHI2_P_VALUE": float(pvalue),
        }
        for dataset, pvalues in datasets.items()
        for strategy in PLOT_STRATEGIES
        for sample_index, pvalue in enumerate(pvalues[strategy])
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=P_VALUE_SCHEMA), temporary, compression="zstd"
    )
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
    seed: int = DEFAULT_SEED,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
) -> tuple[Path, Path, Path, Path, Path, Path, Path]:
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    seed_sequence = np.random.SeedSequence(seed)
    sequence_rng, impairment_rng = [
        np.random.default_rng(child) for child in seed_sequence.spawn(2)
    ]
    ideal_sequences = generate_chi2_sequences(samples_per_strategy, sequence_rng)
    loss_masks, lossy_sequences, reordered_sequences = apply_strategy_impairments(
        ideal_sequences,
        impairment_rng,
    )
    ideal_loss_masks = {
        strategy: np.zeros_like(values, dtype=bool)
        for strategy, values in ideal_sequences.items()
    }
    ideal_pvalues = calculate_strategy_pvalues(ideal_sequences, ideal_loss_masks)
    lossy_pvalues = calculate_strategy_pvalues(lossy_sequences, loss_masks)
    reordered_pvalues = calculate_strategy_pvalues(reordered_sequences, loss_masks)

    processed_dir = processed_root / "classifier-validation"
    figure_dir = figures_root / "classifier-validation"
    aggregate_path = _write_pvalues(
        {
            IDEAL_DATASET: ideal_pvalues,
            LOSSY_DATASET: lossy_pvalues,
            LOSSY_REORDERED_DATASET: reordered_pvalues,
        },
        processed_dir / "chi2-pvalue-cdf.pq",
    )
    ideal_pdf_path = plot_chi2_pvalue_cdf(
        ideal_pvalues,
        figure_dir / "chi2-pvalue-cdf-ideal.pdf",
        dataset_label="Ideal Dataset",
    )
    lossy_pdf_path = plot_chi2_pvalue_cdf(
        lossy_pvalues,
        figure_dir / "chi2-pvalue-cdf-lossy.pdf",
        dataset_label="Lossy Dataset",
    )
    reordered_pdf_path = plot_chi2_pvalue_cdf(
        reordered_pvalues,
        figure_dir / "chi2-pvalue-cdf-lossy-reordered.pdf",
        dataset_label="Lossy+Reordered Dataset",
    )
    samples_by_strategy = {
        strategy: int(len(ideal_pvalues[strategy])) for strategy in PLOT_STRATEGIES
    }

    def summarize(pvalues: dict[str, np.ndarray]) -> dict:
        summaries = {}
        for strategy in PLOT_STRATEGIES:
            values = pvalues[strategy]
            summaries[strategy] = {
                "minimum": float(values.min()),
                "q25": float(np.quantile(values, 0.25)),
                "median": float(np.median(values)),
                "q75": float(np.quantile(values, 0.75)),
                "maximum": float(values.max()),
                "below_random_threshold_count": int((values < RANDOM_MIN_P_VALUE).sum()),
                "below_random_threshold_percentage": float(
                    100.0 * (values < RANDOM_MIN_P_VALUE).mean()
                ),
            }
        return summaries

    common_metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "seed": seed,
        "connection_count": CONNECTION_COUNT,
        "requests_per_connection": REQUESTS_PER_CONNECTION,
        "ideal_sequence_length": IDEAL_SEQUENCE_LENGTH,
        "samples_per_nontrivial_strategy": samples_per_strategy,
        "trivial_samples_per_strategy": TRIVIAL_SAMPLES_PER_STRATEGY,
        "trivial_strategies": sorted(TRIVIAL_STRATEGIES),
        "synthetic_generator_parameters": SYNTHETIC_GENERATOR_PARAMETERS,
        "samples_by_strategy": samples_by_strategy,
        "chi2_uniformity_test": {
            "scope": (
                "modulo-2^16 increments between consecutive present values within each subsequence"
            ),
            "loss_handling": (
                "drop missing values within each subsequence before taking differences"
            ),
            "subsequences": list(INCREMENT_SUBSEQUENCES),
            "subsequence_aggregation": "minimum",
            "increment_modulus": MODULUS,
            "bins": CHI2_BINS,
            "degrees_of_freedom": CHI2_BINS - 1,
            "random_min_p_value": RANDOM_MIN_P_VALUE,
            "order_invariant": False,
        },
        "x_axis_maximum": X_AXIS_MAXIMUM,
        "x_axis_major_exponent_step": X_MAJOR_EXPONENT_STEP,
        "x_axis_minor_exponent_offset": X_MINOR_EXPONENT_OFFSET,
        "aggregate": str(aggregate_path),
    }
    ideal_json_path = _write_json(
        {
            **common_metadata,
            "dataset": IDEAL_DATASET,
            "present_ipids_per_sequence": IDEAL_SEQUENCE_LENGTH,
            "loss_fraction": 0.0,
            "lost_ipids_per_sequence": 0,
            "reorder_fraction_of_present": 0.0,
            "reordered_ipids_per_sequence": 0,
            "paired_loss_masks": False,
            "random_minimum_p_value_marker": float(ideal_pvalues["RANDOM"].min()),
            "figure": str(ideal_pdf_path),
            "summary_by_strategy": summarize(ideal_pvalues),
        },
        figure_dir / "chi2-pvalue-cdf-ideal.json",
    )
    lossy_json_path = _write_json(
        {
            **common_metadata,
            "dataset": LOSSY_DATASET,
            "present_ipids_per_sequence": PRESENT_SEQUENCE_LENGTH,
            "loss_fraction": LOSS_FRACTION,
            "lost_ipids_per_sequence": IDEAL_SEQUENCE_LENGTH - PRESENT_SEQUENCE_LENGTH,
            "reorder_fraction_of_present": 0.0,
            "reordered_ipids_per_sequence": 0,
            "paired_loss_masks": True,
            "random_minimum_p_value_marker": float(lossy_pvalues["RANDOM"].min()),
            "figure": str(lossy_pdf_path),
            "summary_by_strategy": summarize(lossy_pvalues),
        },
        figure_dir / "chi2-pvalue-cdf-lossy.json",
    )
    reordered_json_path = _write_json(
        {
            **common_metadata,
            "dataset": LOSSY_REORDERED_DATASET,
            "present_ipids_per_sequence": PRESENT_SEQUENCE_LENGTH,
            "loss_fraction": LOSS_FRACTION,
            "lost_ipids_per_sequence": IDEAL_SEQUENCE_LENGTH - PRESENT_SEQUENCE_LENGTH,
            "reorder_fraction_of_present": REORDER_FRACTION,
            "reordered_ipids_per_sequence": round(
                PRESENT_SEQUENCE_LENGTH * REORDER_FRACTION
            ),
            "paired_loss_masks": True,
            "random_minimum_p_value_marker": float(reordered_pvalues["RANDOM"].min()),
            "figure": str(reordered_pdf_path),
            "summary_by_strategy": summarize(reordered_pvalues),
        },
        figure_dir / "chi2-pvalue-cdf-lossy-reordered.json",
    )
    return (
        ideal_pdf_path,
        ideal_json_path,
        lossy_pdf_path,
        lossy_json_path,
        reordered_pdf_path,
        reordered_json_path,
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
    seed: int = typer.Option(DEFAULT_SEED, help="deterministic random seed"),
) -> None:
    (
        ideal_pdf_path,
        ideal_json_path,
        lossy_pdf_path,
        lossy_json_path,
        reordered_pdf_path,
        reordered_json_path,
        aggregate_path,
    ) = render(
        samples_per_strategy=samples_per_strategy,
        seed=seed,
    )
    typer.echo(f"ideal_pdf: {ideal_pdf_path}")
    typer.echo(f"ideal_json: {ideal_json_path}")
    typer.echo(f"lossy_pdf: {lossy_pdf_path}")
    typer.echo(f"lossy_json: {lossy_json_path}")
    typer.echo(f"lossy_reordered_pdf: {reordered_pdf_path}")
    typer.echo(f"lossy_reordered_json: {reordered_json_path}")
    typer.echo(f"aggregate: {aggregate_path}")


if __name__ == "__main__":
    app()
