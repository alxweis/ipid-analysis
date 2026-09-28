"""Paper comparison for an adapted NIST SP 800-22 short-sequence baseline."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import zipfile

import matplotlib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from ipid_analysis.classifier_validation import (
    _format_matrix_percentage,
    apply_fixed_interval_impairments,
    apply_reordering,
)
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.nist_short_sequence import (
    NIST_BASELINE_VERSION,
    NIST_EXCLUDED_TESTS,
    NIST_PUBLICATION,
    NIST_PUBLICATION_URL,
    NIST_TEST_LABELS,
    NIST_TEST_NAMES,
    ShortNistNullTables,
    ipids_to_bitstreams,
)
from ipid_analysis.paper_figures import PERCENTAGE_CMAP, configure_paper_style
from ipid_analysis.plot_random_structure_score_cdf import plot_score_cdf
from ipid_analysis.strategies import STRATEGY_PRETTY
from ipid_analysis.synthetic_mass_sequences import (
    DEFAULT_SEED,
    IDEAL_SEQUENCE_LENGTH,
    LOSS_FRACTION,
    PLOT_STRATEGIES,
    PRESENT_SEQUENCE_LENGTH,
    REORDER_FRACTION,
    TRIVIAL_SAMPLES_PER_STRATEGY,
    TRIVIAL_STRATEGIES,
    generate_mass_paper_sequences,
)

app = typer.Typer(add_completion=False)
LOGGER = logging.getLogger(__name__)

DEFAULT_SAMPLES_PER_STRATEGY = 100_000
DEFAULT_NULL_TABLE_SAMPLES = 1_000_000
DEFAULT_NULL_TABLE_SEED = 20_260_930
DEFAULT_BATCH_SIZE = 2_000
TARGET_RANDOM_FALSE_REJECTION_RATE = 0.0001
NIST_COMPONENT_THRESHOLD = 0.01
NIST_COMBINED_THRESHOLD = TARGET_RANDOM_FALSE_REJECTION_RATE
DEFAULT_OUTPUT_DIR = PROCESSED_DATA_DIR / "classifier-validation" / "nist-baseline"
DEFAULT_FIGURE_DIR = FIGURES_DIR / "classifier-validation" / "nist-baseline"

CONDITIONS = ("ideal", "lossy", "reordered", "lossy-reordered")
CONDITION_LABELS = {
    "ideal": "Ideal",
    "lossy": "20% loss",
    "reordered": "20% reordering",
    "lossy-reordered": "20% loss + 20% reordering",
}
HEATMAP_ROWS = ("combined", *NIST_TEST_NAMES)
HEATMAP_LABELS = {"combined": "Combined baseline", **NIST_TEST_LABELS}

SCORE_SCHEMA = pa.schema(
    [
        ("DATASET", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.string()),
        ("SAMPLE_INDEX", pa.int32()),
        ("NIST_COMPATIBILITY_SCORE", pa.float64()),
        ("IS_RANDOM_COMPATIBLE", pa.bool_()),
        *[(name.upper(), pa.float64()) for name in NIST_TEST_NAMES],
    ]
)


def _condition_inputs(
    ideal: np.ndarray,
    impairment_rng: np.random.Generator,
    reorder_rng: np.random.Generator,
) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    loss_mask, lossy, lossy_reordered = apply_fixed_interval_impairments(
        ideal,
        impairment_rng,
        loss_fraction=LOSS_FRACTION,
        reorder_fraction=REORDER_FRACTION,
    )
    reordered = apply_reordering(
        ideal,
        reorder_rng,
        reordered_count=round(IDEAL_SEQUENCE_LENGTH * REORDER_FRACTION),
    )
    complete = np.ones_like(ideal, dtype=bool)
    return {
        "ideal": (ideal, complete),
        "lossy": (lossy, ~loss_mask),
        "reordered": (reordered, complete),
        "lossy-reordered": (lossy_reordered, ~loss_mask),
    }


def _write_json(value: dict, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(output_path)
    return output_path


def _create_bundle(paths: list[Path], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in paths:
            archive.write(path, arcname=path.name)
    return output_path


def _plot_test_heatmap(
    acceptance_percentages: dict[tuple[str, str, str], float],
    output_path: Path,
) -> Path:
    configure_paper_style()
    fig, axes = plt.subplots(2, 2, figsize=(7.16, 6.3), sharex=True, sharey=True)
    image = None
    for condition_index, condition in enumerate(CONDITIONS):
        axis = axes.flat[condition_index]
        matrix = np.asarray(
            [
                [
                    acceptance_percentages[(condition, row, strategy)]
                    for strategy in PLOT_STRATEGIES
                ]
                for row in HEATMAP_ROWS
            ]
        )
        image = axis.imshow(
            matrix,
            cmap=PERCENTAGE_CMAP,
            vmin=0,
            vmax=100,
            aspect="auto",
            interpolation="nearest",
        )
        axis.set_title(CONDITION_LABELS[condition], pad=5)
        axis.axhline(0.5, color="#555555", linewidth=0.55)
        for row_index in range(len(HEATMAP_ROWS)):
            for column_index in range(len(PLOT_STRATEGIES)):
                percentage = matrix[row_index, column_index]
                axis.text(
                    column_index,
                    row_index,
                    _format_matrix_percentage(float(percentage)),
                    ha="center",
                    va="center",
                    color="white" if percentage >= 50 else "#222222",
                    fontsize=5.2,
                )
    for axis in axes[-1, :]:
        axis.set_xticks(
            np.arange(len(PLOT_STRATEGIES)),
            [STRATEGY_PRETTY[strategy] for strategy in PLOT_STRATEGIES],
            rotation=45,
            ha="right",
            rotation_mode="anchor",
        )
    for axis in axes[:, 0]:
        axis.set_yticks(
            np.arange(len(HEATMAP_ROWS)),
            [HEATMAP_LABELS[name] for name in HEATMAP_ROWS],
        )
    fig.supxlabel("Generating IP-ID process", y=0.02)
    fig.supylabel("Adapted NIST component", x=0.015)
    fig.subplots_adjust(left=0.20, right=0.88, bottom=0.18, top=0.96, hspace=0.16, wspace=0.08)
    colorbar_axis = fig.add_axes((0.91, 0.25, 0.014, 0.50))
    colorbar = fig.colorbar(image, cax=colorbar_axis)
    colorbar.set_label("Classified RANDOM [%]")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_path,
        format="pdf",
        bbox_inches="tight",
        metadata={
            "Title": "Adapted NIST SP 800-22 baseline by component and IP-ID strategy",
            "Subject": "Short IP-ID-derived bitstream comparison under four impairments",
            "Creator": "ipid-analysis",
        },
    )
    plt.close(fig)
    return output_path


def render(
    *,
    samples_per_strategy: int = DEFAULT_SAMPLES_PER_STRATEGY,
    null_table_samples: int = DEFAULT_NULL_TABLE_SAMPLES,
    null_table_seed: int = DEFAULT_NULL_TABLE_SEED,
    batch_size: int = DEFAULT_BATCH_SIZE,
    seed: int = DEFAULT_SEED,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    figure_dir: Path = DEFAULT_FIGURE_DIR,
) -> dict[str, Path]:
    if min(samples_per_strategy, null_table_samples, batch_size) < 1:
        raise ValueError("sample counts and batch size must be positive")
    output_dir.mkdir(parents=True, exist_ok=True)
    figure_dir.mkdir(parents=True, exist_ok=True)

    sequence_rng = np.random.default_rng(np.random.SeedSequence([seed, 1]))
    ideal_sequences = generate_mass_paper_sequences(samples_per_strategy, sequence_rng)
    null_tables = ShortNistNullTables(null_table_samples, null_table_seed, batch_size)
    scores: dict[str, dict[str, np.ndarray]] = {condition: {} for condition in CONDITIONS}
    acceptance_counts: dict[tuple[str, str, str], int] = {}
    sample_counts: dict[tuple[str, str], int] = {}
    aggregate_path = output_dir / "mass-4x25-nist-baseline-scores.pq"
    temporary = aggregate_path.with_suffix(aggregate_path.suffix + ".part")
    temporary.unlink(missing_ok=True)

    with pq.ParquetWriter(temporary, SCORE_SCHEMA, compression="zstd") as writer:
        for strategy_index, strategy in enumerate(PLOT_STRATEGIES):
            ideal = ideal_sequences[strategy]
            conditions = _condition_inputs(
                ideal,
                np.random.default_rng(np.random.SeedSequence([seed, 2, strategy_index])),
                np.random.default_rng(np.random.SeedSequence([seed, 3, strategy_index])),
            )
            for condition in CONDITIONS:
                values, present = conditions[condition]
                score_parts = []
                for offset in range(0, len(values), batch_size):
                    stop = min(offset + batch_size, len(values))
                    bits = ipids_to_bitstreams(values[offset:stop], present[offset:stop])
                    combined, components = null_tables.combined_pvalues(bits)
                    score_parts.append(combined)
                    arrays = [
                        pa.array([condition] * len(combined), type=pa.string()),
                        pa.array([strategy] * len(combined), type=pa.string()),
                        pa.array(np.arange(offset, stop, dtype=np.int32)),
                        pa.array(combined, type=pa.float64()),
                        pa.array(combined >= NIST_COMBINED_THRESHOLD, type=pa.bool_()),
                    ]
                    arrays.extend(
                        pa.array(components[name], type=pa.float64()) for name in NIST_TEST_NAMES
                    )
                    writer.write_table(pa.Table.from_arrays(arrays, schema=SCORE_SCHEMA))
                    acceptance_counts[(condition, "combined", strategy)] = acceptance_counts.get(
                        (condition, "combined", strategy), 0
                    ) + int(np.count_nonzero(combined >= NIST_COMBINED_THRESHOLD))
                    for name in NIST_TEST_NAMES:
                        acceptance_counts[(condition, name, strategy)] = acceptance_counts.get(
                            (condition, name, strategy), 0
                        ) + int(np.count_nonzero(components[name] >= NIST_COMPONENT_THRESHOLD))
                scores[condition][strategy] = np.concatenate(score_parts)
                sample_counts[(condition, strategy)] = len(values)
    temporary.replace(aggregate_path)

    acceptance_percentages = {
        key: 100.0 * count / sample_counts[(key[0], key[2])]
        for key, count in acceptance_counts.items()
    }
    outputs: dict[str, Path] = {"aggregate": aggregate_path}
    summary_by_condition = {}
    for condition in CONDITIONS:
        pdf_path = plot_score_cdf(
            scores[condition],
            NIST_COMBINED_THRESHOLD,
            figure_dir / f"mass-4x25-nist-score-cdf-{condition}.pdf",
            dataset_label=CONDITION_LABELS[condition],
            x_label=r"Adapted NIST Compatibility Score $S_{\mathrm{NIST}}$",
            method_title="Adapted NIST SP 800-22 baseline score",
        )
        condition_summary = {
            strategy: {
                "minimum": float(values.min()),
                "median": float(np.median(values)),
                "maximum": float(values.max()),
                "classified_random_count": int(
                    np.count_nonzero(values >= NIST_COMBINED_THRESHOLD)
                ),
                "classified_random_percentage": float(
                    100.0 * np.mean(values >= NIST_COMBINED_THRESHOLD)
                ),
            }
            for strategy, values in scores[condition].items()
        }
        json_path = _write_json(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "dataset": condition,
                "dataset_label": CONDITION_LABELS[condition],
                "baseline_version": NIST_BASELINE_VERSION,
                "not_a_nist_validation_claim": True,
                "threshold": NIST_COMBINED_THRESHOLD,
                "summary_by_strategy": condition_summary,
            },
            figure_dir / f"mass-4x25-nist-score-cdf-{condition}.json",
        )
        outputs[f"{condition}_pdf"] = pdf_path
        outputs[f"{condition}_json"] = json_path
        summary_by_condition[condition] = condition_summary

    heatmap_path = _plot_test_heatmap(
        acceptance_percentages,
        figure_dir / "nist-test-strategy-heatmap.pdf",
    )
    outputs["heatmap_pdf"] = heatmap_path
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "purpose": "adapted NIST SP 800-22 baseline for short IP-ID-derived bitstreams",
        "baseline_version": NIST_BASELINE_VERSION,
        "publication": NIST_PUBLICATION,
        "publication_url": NIST_PUBLICATION_URL,
        "not_a_nist_validation_claim": True,
        "encoding": "present IP-IDs as big-endian unsigned 16-bit words in measurement order",
        "missing_values": "omitted without replacement bits",
        "ideal_bit_length": 16 * IDEAL_SEQUENCE_LENGTH,
        "lossy_bit_length": 16 * PRESENT_SEQUENCE_LENGTH,
        "conditions": list(CONDITIONS),
        "included_tests": list(NIST_TEST_NAMES),
        "excluded_tests": NIST_EXCLUDED_TESTS,
        "component_threshold": NIST_COMPONENT_THRESHOLD,
        "combined_score": "empirical left-tail p-value of the dependent minimum component p-value",
        "combined_threshold": NIST_COMBINED_THRESHOLD,
        "target_random_false_rejection_rate": TARGET_RANDOM_FALSE_REJECTION_RATE,
        "null_tables": {
            "sample_count": null_table_samples,
            "seed": null_table_seed,
            "conditioned_on_exact_bit_length": True,
            "add_one_correction": True,
        },
        "samples_per_nontrivial_strategy": samples_per_strategy,
        "trivial_samples_per_strategy": TRIVIAL_SAMPLES_PER_STRATEGY,
        "trivial_strategies": sorted(TRIVIAL_STRATEGIES),
        "seed": seed,
        "summary_by_condition": summary_by_condition,
        "component_random_compatibility_percentages": {
            "|".join(key): value for key, value in acceptance_percentages.items()
        },
    }
    summary_path = _write_json(report, output_dir / "nist-baseline-summary.json")
    outputs["summary_json"] = summary_path
    bundle_inputs = [path for name, path in outputs.items() if name != "aggregate"]
    bundle_path = _create_bundle(
        bundle_inputs,
        output_dir / "nist-baseline-review-bundle.zip",
    )
    outputs["review_bundle"] = bundle_path
    return outputs


def _configure_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[stream, file_handler], force=True)
    logging.getLogger("fontTools").setLevel(logging.WARNING)


@app.command()
def main(
    samples_per_strategy: int = typer.Option(DEFAULT_SAMPLES_PER_STRATEGY, min=1),
    null_table_samples: int = typer.Option(DEFAULT_NULL_TABLE_SAMPLES, min=1),
    null_table_seed: int = typer.Option(DEFAULT_NULL_TABLE_SEED),
    batch_size: int = typer.Option(DEFAULT_BATCH_SIZE, min=1),
    seed: int = typer.Option(DEFAULT_SEED),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR),  # noqa: B008
    figure_dir: Path = typer.Option(DEFAULT_FIGURE_DIR),  # noqa: B008
) -> None:
    log_path = output_dir / "nist-baseline.log"
    _configure_logging(log_path)
    outputs = render(
        samples_per_strategy=samples_per_strategy,
        null_table_samples=null_table_samples,
        null_table_seed=null_table_seed,
        batch_size=batch_size,
        seed=seed,
        output_dir=output_dir,
        figure_dir=figure_dir,
    )
    for handler in logging.getLogger().handlers:
        handler.flush()
    bundle_inputs = [
        path for name, path in outputs.items() if name not in {"aggregate", "review_bundle"}
    ]
    bundle_inputs.append(log_path)
    _create_bundle(bundle_inputs, outputs["review_bundle"])
    for name, path in outputs.items():
        typer.echo(f"{name}: {path}")
    typer.echo(f"run_log: {log_path}")


if __name__ == "__main__":
    app()
