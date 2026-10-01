"""Paper comparison for an adapted NIST SP 800-22 short-sequence baseline."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import math
from pathlib import Path
import zipfile

import matplotlib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.transforms import ScaledTranslation

from ipid_analysis.classifier_validation import (
    CONFUSION_NUMERIC_TEXT_UPWARD_OFFSET_POINTS,
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
from ipid_analysis.paper_figures import (
    COMPACT_PAPER_PDF_PADDING_INCHES,
    COMPACT_PAPER_STROKE_WIDTH,
    PERCENTAGE_CMAP,
    configure_compact_validation_style,
    draw_percentage_colorbar_axis,
)
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

NIST_HEATMAP_FIGURE_SIZE_INCHES = (7.0, 5.15)
NIST_HEATMAP_LEFT_INCHES = 1.55
NIST_HEATMAP_CELL_WIDTH_INCHES = 0.30
NIST_HEATMAP_CELL_HEIGHT_INCHES = 0.155
NIST_HEATMAP_HORIZONTAL_GAP_INCHES = 0.24
NIST_HEATMAP_VERTICAL_GAP_INCHES = 0.38
NIST_HEATMAP_BOTTOM_INCHES = 1.05
NIST_HEATMAP_TITLE_GAP_INCHES = 0.05
NIST_HEATMAP_XLABEL_GAP_INCHES = 0.60
NIST_HEATMAP_YLABEL_GAP_INCHES = 1.20
NIST_HEATMAP_COLORBAR_GAP_INCHES = 0.10
NIST_HEATMAP_COLORBAR_WIDTH_INCHES = 0.065
NIST_HEATMAP_CELL_FONT_SIZE = 8.0

CONDITIONS = ("ideal", "lossy", "reordered", "lossy-reordered")
CONDITION_LABELS = {
    "ideal": "Ideal",
    "lossy": "20% Lossy",
    "reordered": "20% Reordered",
    "lossy-reordered": "20% Lossy + 20% Reordered",
}
HEATMAP_ROWS = ("combined", *NIST_TEST_NAMES)
HEATMAP_LABELS = {"combined": "Combined score", **NIST_TEST_LABELS}

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


def _next_lower_power_of_ten(scores: dict[str, np.ndarray]) -> float:
    """Return the decade immediately below the smallest positive score."""
    positive_minima = [
        float(values[values > 0].min()) for values in scores.values() if np.any(values > 0)
    ]
    if not positive_minima:
        raise ValueError("NIST CDF requires at least one positive score")
    exponent = math.ceil(math.log10(min(positive_minima))) - 1
    return 10.0**exponent


def _normalize_heatmap_title_gap(axis) -> None:
    figure = axis.figure
    figure.canvas.draw()
    renderer = figure.canvas.get_renderer()
    dpi = figure.dpi
    axis_box = axis.get_window_extent(renderer=renderer)
    title_box = axis.title.get_window_extent(renderer=renderer)
    current_gap_inches = (title_box.y0 - axis_box.y1) / dpi
    title_x, title_y = axis.title.get_position()
    axis.title.set_position(
        (
            title_x,
            title_y + (NIST_HEATMAP_TITLE_GAP_INCHES - current_gap_inches) * dpi / axis_box.height,
        )
    )


def _add_heatmap_shared_labels(fig, *, panel_width: float, panel_height: float) -> None:
    figure_width, figure_height = fig.get_size_inches()
    block_width = 2 * panel_width + NIST_HEATMAP_HORIZONTAL_GAP_INCHES
    block_height = 2 * panel_height + NIST_HEATMAP_VERTICAL_GAP_INCHES
    xlabel = fig.text(
        (NIST_HEATMAP_LEFT_INCHES + block_width / 2.0) / figure_width,
        0.05,
        "Generating IP-ID process",
        ha="center",
        va="bottom",
    )
    ylabel = fig.text(
        0.05,
        (NIST_HEATMAP_BOTTOM_INCHES + block_height / 2.0) / figure_height,
        "Adapted NIST component",
        ha="center",
        va="center",
        rotation=90,
    )
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    dpi = fig.dpi
    xlabel_box = xlabel.get_window_extent(renderer=renderer)
    xlabel_x, xlabel_y = xlabel.get_position()
    xlabel.set_position(
        (
            xlabel_x,
            xlabel_y
            + (NIST_HEATMAP_BOTTOM_INCHES - NIST_HEATMAP_XLABEL_GAP_INCHES - xlabel_box.y1 / dpi)
            / figure_height,
        )
    )
    ylabel_box = ylabel.get_window_extent(renderer=renderer)
    ylabel_x, ylabel_y = ylabel.get_position()
    ylabel.set_position(
        (
            ylabel_x
            + (NIST_HEATMAP_LEFT_INCHES - NIST_HEATMAP_YLABEL_GAP_INCHES - ylabel_box.x1 / dpi)
            / figure_width,
            ylabel_y,
        )
    )


def _plot_test_heatmap(
    acceptance_percentages: dict[tuple[str, str, str], float],
    output_path: Path,
) -> Path:
    configure_compact_validation_style()
    figure_width, figure_height = NIST_HEATMAP_FIGURE_SIZE_INCHES
    panel_width = len(PLOT_STRATEGIES) * NIST_HEATMAP_CELL_WIDTH_INCHES
    panel_height = len(HEATMAP_ROWS) * NIST_HEATMAP_CELL_HEIGHT_INCHES
    fig = plt.figure(figsize=NIST_HEATMAP_FIGURE_SIZE_INCHES)
    axes = np.empty((2, 2), dtype=object)
    for row in range(2):
        for column in range(2):
            left = NIST_HEATMAP_LEFT_INCHES + column * (
                panel_width + NIST_HEATMAP_HORIZONTAL_GAP_INCHES
            )
            bottom = NIST_HEATMAP_BOTTOM_INCHES + (1 - row) * (
                panel_height + NIST_HEATMAP_VERTICAL_GAP_INCHES
            )
            axes[row, column] = fig.add_axes(
                (
                    left / figure_width,
                    bottom / figure_height,
                    panel_width / figure_width,
                    panel_height / figure_height,
                )
            )
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
        axis.pcolormesh(
            np.arange(len(PLOT_STRATEGIES) + 1),
            np.arange(len(HEATMAP_ROWS) + 1),
            matrix,
            cmap=PERCENTAGE_CMAP,
            vmin=0,
            vmax=100,
            edgecolors="white",
            linewidth=0.40,
            antialiased=False,
            shading="flat",
        )
        axis.set_xlim(0, len(PLOT_STRATEGIES))
        axis.set_ylim(len(HEATMAP_ROWS), 0)
        axis.set_xticks(np.arange(len(PLOT_STRATEGIES)) + 0.5)
        axis.set_yticks(np.arange(len(HEATMAP_ROWS)) + 0.5)
        axis.set_title(CONDITION_LABELS[condition], pad=0.0, y=1.0)
        _normalize_heatmap_title_gap(axis)
        axis.axhline(1.0, color="#555555", linewidth=0.65)
        for spine in axis.spines.values():
            spine.set_linewidth(COMPACT_PAPER_STROKE_WIDTH)
        axis.tick_params(
            which="major",
            width=COMPACT_PAPER_STROKE_WIDTH,
            length=3.0,
            pad=1.5,
        )
        for row_index in range(len(HEATMAP_ROWS)):
            for column_index in range(len(PLOT_STRATEGIES)):
                percentage = matrix[row_index, column_index]
                text_transform = axis.transData + ScaledTranslation(
                    0,
                    CONFUSION_NUMERIC_TEXT_UPWARD_OFFSET_POINTS / 72.0,
                    fig.dpi_scale_trans,
                )
                axis.text(
                    column_index + 0.5,
                    row_index + 0.5,
                    _format_matrix_percentage(float(percentage)),
                    ha="center",
                    va="center",
                    color="white" if percentage >= 50 else "#222222",
                    fontsize=NIST_HEATMAP_CELL_FONT_SIZE,
                    transform=text_transform,
                )
    xlabels = [STRATEGY_PRETTY[strategy] for strategy in PLOT_STRATEGIES]
    ylabels = [HEATMAP_LABELS[name] for name in HEATMAP_ROWS]
    for row, axis_row in enumerate(axes):
        for column, axis in enumerate(axis_row):
            if row == 1:
                axis.set_xticklabels(
                    xlabels,
                    rotation=30,
                    ha="right",
                    va="top",
                    rotation_mode="anchor",
                )
            else:
                axis.tick_params(axis="x", bottom=True, labelbottom=False)
            if column == 0:
                axis.set_yticklabels(ylabels, ha="right", va="center")
            else:
                axis.tick_params(axis="y", left=True, labelleft=False)

    _add_heatmap_shared_labels(fig, panel_width=panel_width, panel_height=panel_height)
    block_width = 2 * panel_width + NIST_HEATMAP_HORIZONTAL_GAP_INCHES
    block_height = 2 * panel_height + NIST_HEATMAP_VERTICAL_GAP_INCHES
    colorbar_axis = fig.add_axes(
        (
            (NIST_HEATMAP_LEFT_INCHES + block_width + NIST_HEATMAP_COLORBAR_GAP_INCHES)
            / figure_width,
            (NIST_HEATMAP_BOTTOM_INCHES + block_height / 2.0 - panel_height / 2.0) / figure_height,
            NIST_HEATMAP_COLORBAR_WIDTH_INCHES / figure_width,
            panel_height / figure_height,
        )
    )
    draw_percentage_colorbar_axis(
        colorbar_axis,
        label="Classified Random [%]",
        stroke_width=COMPACT_PAPER_STROKE_WIDTH,
        tick_length=3.0,
        tick_pad=1.5,
        text_upward_offset_points=CONFUSION_NUMERIC_TEXT_UPWARD_OFFSET_POINTS,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_path,
        format="pdf",
        bbox_inches="tight",
        pad_inches=COMPACT_PAPER_PDF_PADDING_INCHES,
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
        axis_minimum = _next_lower_power_of_ten(scores[condition])
        pdf_path = plot_score_cdf(
            scores[condition],
            NIST_COMBINED_THRESHOLD,
            figure_dir / f"mass-4x25-nist-score-cdf-{condition}.pdf",
            dataset_label=CONDITION_LABELS[condition],
            x_label=r"Adapted NIST Compatibility Score $S_{\mathrm{NIST}}$",
            method_title="Adapted NIST SP 800-22 baseline score",
            positive_axis_minimum=axis_minimum,
            separate_subminimum_panel=False,
            show_curve_markers=True,
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
                "figure_axis": {
                    "positive_display_minimum": axis_minimum,
                    "rule": "next lower power of ten below the minimum positive score",
                },
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
        "heatmap_combined_score_row": (
            "percentage of sequences whose empirically calibrated combined score is at least "
            "the combined threshold"
        ),
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
