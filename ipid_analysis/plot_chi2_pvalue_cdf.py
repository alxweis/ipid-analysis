"""Plot global Chi-square uniformity p-value CDFs for synthetic IP-ID strategies."""

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
from matplotlib.ticker import LogFormatterMathtext, MultipleLocator, NullLocator  # noqa: E402

from ipid_analysis.classifier_validation import REQUEST_IP_IDS  # noqa: E402
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR  # noqa: E402
from ipid_analysis.paper_figures import configure_paper_style  # noqa: E402
from ipid_analysis.strategies import (  # noqa: E402
    CHI2_BINS,
    MODULUS,
    RANDOM_MIN_P_VALUE,
    STRATEGY_COLORS,
    STRATEGY_PRETTY,
    chi2_uniformity_pvalues,
)

app = typer.Typer()

CONNECTION_COUNT = 4
REQUESTS_PER_CONNECTION = 20
SEQUENCE_LENGTH = CONNECTION_COUNT * REQUESTS_PER_CONNECTION
DEFAULT_SAMPLES_PER_STRATEGY = 10_000
TRIVIAL_SAMPLES_PER_STRATEGY = 100
DEFAULT_SEED = 42

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

P_VALUE_SCHEMA = pa.schema(
    [
        ("IPID_SELECTION_STRATEGY", pa.string()),
        ("SAMPLE_INDEX", pa.int32()),
        ("CHI2_P_VALUE", pa.float64()),
    ]
)


def _round_connection_flatten(values: np.ndarray) -> np.ndarray:
    """Flatten (sample, request round, connection) in measurement order."""
    return values.reshape(values.shape[0], -1).astype(np.uint16)


def _cumulative_sequences(starts: np.ndarray, increments: np.ndarray) -> np.ndarray:
    cumulative = np.cumsum(increments, axis=1, dtype=np.int64)
    values = np.concatenate([starts[:, None], starts[:, None] + cumulative], axis=1)
    return (values % MODULUS).astype(np.uint16)


def _separated_connection_starts(
    sample_count: int,
    rng: np.random.Generator,
    *,
    minimum_circular_gap: int = 1_200,
) -> np.ndarray:
    """Draw four starts whose circular gaps keep their short counters separate."""
    accepted: list[np.ndarray] = []
    accepted_count = 0
    while accepted_count < sample_count:
        candidate_count = max(256, 2 * (sample_count - accepted_count))
        candidates = np.sort(
            rng.integers(
                0,
                MODULUS,
                size=(candidate_count, CONNECTION_COUNT),
                dtype=np.int64,
            ),
            axis=1,
        )
        circular_gaps = np.concatenate(
            [
                np.diff(candidates, axis=1),
                (MODULUS - candidates[:, -1] + candidates[:, 0])[:, None],
            ],
            axis=1,
        )
        valid = candidates[(circular_gaps >= minimum_circular_gap).all(axis=1)]
        if len(valid):
            accepted.append(valid)
            accepted_count += len(valid)
    return np.concatenate(accepted, axis=0)[:sample_count]


def generate_chi2_sequences(
    samples_per_strategy: int,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """Generate ideal, measurement-shaped 4x20 sequences for all plot strategies."""
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    n = samples_per_strategy
    trivial_n = TRIVIAL_SAMPLES_PER_STRATEGY
    request_pattern = REQUEST_IP_IDS[np.arange(SEQUENCE_LENGTH) % len(REQUEST_IP_IDS)]

    reflection_offsets = rng.integers(0, MODULUS, size=trivial_n, dtype=np.int64)
    reflection = ((request_pattern[None, :] + reflection_offsets[:, None]) % MODULUS).astype(
        np.uint16
    )

    constant_values = rng.integers(0, MODULUS, size=trivial_n, dtype=np.uint16)
    constant = np.repeat(constant_values[:, None], SEQUENCE_LENGTH, axis=1)

    single_starts = rng.integers(0, MODULUS, size=n, dtype=np.int64)
    single_increments = rng.integers(
        2,
        2_001,
        size=(n, SEQUENCE_LENGTH - 1),
        dtype=np.int64,
    )
    single = _cumulative_sequences(single_starts, single_increments)

    destination_base = rng.integers(0, 20_000, size=n, dtype=np.int64)
    per_destination = np.empty((n, SEQUENCE_LENGTH), dtype=np.uint16)
    per_destination[:, 0::2] = (
        destination_base[:, None] + np.arange(SEQUENCE_LENGTH // 2)
    ) % MODULUS
    per_destination[:, 1::2] = (
        destination_base[:, None] + 30_000 + np.arange(SEQUENCE_LENGTH // 2)
    ) % MODULUS

    connection_starts = _separated_connection_starts(n, rng)
    per_connection_cube = (
        connection_starts[:, None, :]
        + np.arange(REQUESTS_PER_CONNECTION, dtype=np.int64)[None, :, None]
    ) % MODULUS
    per_connection = _round_connection_flatten(per_connection_cube)

    bucket_base = rng.integers(0, 3_000, size=n, dtype=np.int64)
    bucket_starts = (bucket_base[:, None] + np.asarray([0, 30_000, 1_000, 31_000])) % MODULUS
    bucket_increments = rng.integers(
        2,
        2_001,
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

    multi_starts = _separated_connection_starts(n, rng)
    multi_increments = rng.integers(
        1,
        17,
        size=(n, CONNECTION_COUNT, REQUESTS_PER_CONNECTION - 1),
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

    random = rng.integers(
        0,
        MODULUS,
        size=(n, SEQUENCE_LENGTH),
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


def calculate_strategy_pvalues(
    sequences: dict[str, np.ndarray],
) -> dict[str, np.ndarray]:
    """Calculate the classifier's one global Chi-square p-value per sequence."""
    result = {}
    for strategy in PLOT_STRATEGIES:
        values = sequences[strategy].astype(np.int64, copy=False)
        present = np.ones(values.shape, dtype=bool)
        result[strategy] = chi2_uniformity_pvalues(values, present)
    return result


def _ecdf_coordinates(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    ordered = np.sort(values)
    percentages = 100.0 * np.arange(1, len(ordered) + 1) / len(ordered)
    return np.concatenate(([ordered[0]], ordered)), np.concatenate(([0.0], percentages))


def _log_axis_parameters(pvalues: dict[str, np.ndarray]) -> tuple[float, np.ndarray]:
    minimum = min(float(values[values > 0].min()) for values in pvalues.values())
    minimum_exponent = math.floor(math.log10(minimum))
    tick_step = 10 if minimum_exponent <= -30 else 5
    axis_minimum_exponent = tick_step * math.floor(minimum_exponent / tick_step)
    ticks = np.power(
        10.0,
        np.arange(axis_minimum_exponent, 1, tick_step, dtype=float),
    )
    return 10.0**axis_minimum_exponent, ticks


def plot_chi2_pvalue_cdf(
    pvalues: dict[str, np.ndarray],
    output_path: Path,
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

    axis_minimum, major_ticks = _log_axis_parameters(pvalues)
    ax.set_xscale("log")
    ax.set_xlim(axis_minimum, 1.0)
    ax.set_xticks(major_ticks)
    ax.xaxis.set_major_formatter(LogFormatterMathtext(base=10))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.set_ylim(0, 103)
    ax.yaxis.set_major_locator(MultipleLocator(20))
    ax.yaxis.set_minor_locator(MultipleLocator(10))
    ax.set_xlabel(r"Chi$^2$ p-value")
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
    ax.legend(
        handles=handles,
        ncol=4,
        loc="lower center",
        bbox_to_anchor=(0.5, 1.015),
        frameon=False,
        columnspacing=1.3,
        handlelength=2.2,
    )
    fig.subplots_adjust(left=0.12, right=0.995, bottom=0.22, top=0.70)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_path,
        format="pdf",
        bbox_inches="tight",
        metadata={
            "Title": "Chi-square p-value distributions by IP-ID selection strategy",
            "Subject": "Synthetic 4x20 IP-ID sequence empirical CDFs",
            "Creator": "ipid-analysis",
        },
    )
    plt.close(fig)
    return output_path


def _write_pvalues(pvalues: dict[str, np.ndarray], output_path: Path) -> Path:
    rows = [
        {
            "IPID_SELECTION_STRATEGY": strategy,
            "SAMPLE_INDEX": sample_index,
            "CHI2_P_VALUE": float(pvalue),
        }
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
) -> tuple[Path, Path, Path]:
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    sequences = generate_chi2_sequences(samples_per_strategy, np.random.default_rng(seed))
    pvalues = calculate_strategy_pvalues(sequences)

    processed_dir = processed_root / "classifier-validation"
    figure_dir = figures_root / "classifier-validation"
    aggregate_path = _write_pvalues(
        pvalues,
        processed_dir / "chi2-pvalue-cdf.pq",
    )
    pdf_path = plot_chi2_pvalue_cdf(
        pvalues,
        figure_dir / "chi2-pvalue-cdf.pdf",
    )
    samples_by_strategy = {strategy: int(len(pvalues[strategy])) for strategy in PLOT_STRATEGIES}
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
    json_path = _write_json(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "seed": seed,
            "connection_count": CONNECTION_COUNT,
            "requests_per_connection": REQUESTS_PER_CONNECTION,
            "sequence_length": SEQUENCE_LENGTH,
            "samples_per_nontrivial_strategy": samples_per_strategy,
            "trivial_samples_per_strategy": TRIVIAL_SAMPLES_PER_STRATEGY,
            "trivial_strategies": sorted(TRIVIAL_STRATEGIES),
            "samples_by_strategy": samples_by_strategy,
            "chi2_uniformity_test": {
                "scope": "all IP-ID values in one sequence",
                "subsequence_aggregation": None,
                "bins": CHI2_BINS,
                "degrees_of_freedom": CHI2_BINS - 1,
                "random_min_p_value": RANDOM_MIN_P_VALUE,
            },
            "aggregate": str(aggregate_path),
            "figure": str(pdf_path),
            "summary_by_strategy": summaries,
        },
        figure_dir / "chi2-pvalue-cdf.json",
    )
    return pdf_path, json_path, aggregate_path


@app.command()
def main(
    samples_per_strategy: int = typer.Option(
        DEFAULT_SAMPLES_PER_STRATEGY,
        min=1,
        help=(
            "synthetic sequences per nontrivial strategy; REFLECTION and CONSTANT always use 100"
        ),
    ),
    seed: int = typer.Option(DEFAULT_SEED, help="deterministic random seed"),
) -> None:
    pdf_path, json_path, aggregate_path = render(
        samples_per_strategy=samples_per_strategy,
        seed=seed,
    )
    typer.echo(f"pdf: {pdf_path}")
    typer.echo(f"json: {json_path}")
    typer.echo(f"aggregate: {aggregate_path}")


if __name__ == "__main__":
    app()
