"""Held-out paper diagnostics for current, NIST-derived, and candidate RANDOM scores."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import zipfile

import matplotlib
import numpy as np
import typer

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from ipid_analysis.classifier_validation import FIXED_CONFIG, _format_matrix_percentage
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.nist_short_sequence import ShortNistNullTables, ipids_to_bitstreams
from ipid_analysis.paper_figures import PERCENTAGE_CMAP, add_fixed_percentage_colorbar
from ipid_analysis.plot_nist_short_sequence_baseline import (
    DEFAULT_NULL_TABLE_SEED as NIST_NULL_TABLE_SEED,
)
from ipid_analysis.plot_nist_short_sequence_baseline import NIST_COMBINED_THRESHOLD
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_EVALUATION_SEED,
    CANDIDATE_NULL_TABLE_SAMPLES,
    CANDIDATE_NULL_TABLE_SEED,
    CANDIDATE_RANDOM_MIN_SCORE,
    candidate_random_scores,
    create_candidate_null_tables,
)
from ipid_analysis.random_classifier_evaluation import (
    ImpairmentCondition,
    _configure_evaluation_style,
    _stable_rng,
    _wilson_interval,
)
from ipid_analysis.random_classifier_evaluation_v2 import (
    GENERATOR_NAMES,
    apply_v2_impairment,
    generate_v2_sequences,
)
from ipid_analysis.strategies import (
    RANDOM_STRUCTURE_MIN_SCORE,
    RANDOM_STRUCTURE_MIN_TEST_SAMPLES,
    random_structure_scores,
)

app = typer.Typer(add_completion=False)
LOGGER = logging.getLogger(__name__)

DEFAULT_SAMPLES_PER_GENERATOR = 100_000
DEFAULT_BATCH_SIZE = 2_000
DEFAULT_OUTPUT_DIR = PROCESSED_DATA_DIR / "classifier-validation" / "paper-diagnostics"
DEFAULT_FIGURE_DIR = FIGURES_DIR / "classifier-validation"
METHODS = ("current", "nist", "candidate")
COMPACT_CONDITION_LABELS = {
    "ideal": "Ideal",
    "loss-20-random": "20% Lossy",
    "reorder-20": "20%\nReordered",
    "loss-20-random-reorder-20": "20% Lossy +\n20% Reordered",
}
METHOD_LABELS = {
    "current": "Current production score",
    "nist": "Adapted NIST baseline",
    "candidate": "Candidate: Raw + Aggregated Multiscale + Gap",
}
CONDITIONS = (
    ImpairmentCondition("ideal"),
    ImpairmentCondition("loss-20-random", loss_fraction=0.20),
    ImpairmentCondition("reorder-20", reorder_fraction=0.20),
    ImpairmentCondition(
        "loss-20-random-reorder-20",
        loss_fraction=0.20,
        reorder_fraction=0.20,
    ),
)


@dataclass
class Aggregate:
    random_count: int = 0
    sample_count: int = 0

    def add(self, decisions: np.ndarray) -> None:
        self.random_count += int(decisions.sum())
        self.sample_count += len(decisions)


def _pretty_generator(name: str) -> str:
    return name.replace("_", " ").title().replace("Per ", "Per-")


def _evaluate(
    samples_per_generator: int,
    batch_size: int,
    seed: int,
    candidate_null_samples: int,
    candidate_null_seed: int,
    nist_null_samples: int,
    nist_null_seed: int,
) -> list[dict]:
    candidate_tables = create_candidate_null_tables(candidate_null_samples, candidate_null_seed)
    nist_tables = ShortNistNullTables(nist_null_samples, nist_null_seed, batch_size)
    aggregates = {
        (method, condition.name, generator): Aggregate()
        for method in METHODS
        for condition in CONDITIONS
        for generator in GENERATOR_NAMES
    }
    produced = 0
    batch_index = 0
    while produced < samples_per_generator:
        size = min(batch_size, samples_per_generator - produced)
        LOGGER.info("Evaluating diagnostic batch %d (%d rows/generator)", batch_index + 1, size)
        generated = generate_v2_sequences(size, _stable_rng(seed, 801, batch_index), "heldout")
        for generator_index, generator in enumerate(GENERATOR_NAMES):
            ideal = generated[generator].values
            for condition_index, condition in enumerate(CONDITIONS):
                values, present, _ = apply_v2_impairment(
                    ideal,
                    condition,
                    _stable_rng(seed, 802, batch_index, generator_index, condition_index),
                )
                enough = present.sum(axis=1) >= RANDOM_STRUCTURE_MIN_TEST_SAMPLES
                current = (
                    random_structure_scores(values, present, FIXED_CONFIG)
                    >= RANDOM_STRUCTURE_MIN_SCORE
                ) & enough
                candidate = (
                    candidate_random_scores(values, present, candidate_tables)
                    >= CANDIDATE_RANDOM_MIN_SCORE
                ) & enough
                nist_bits = ipids_to_bitstreams(values, present)
                nist_score, _ = nist_tables.combined_pvalues(nist_bits)
                nist = (nist_score >= NIST_COMBINED_THRESHOLD) & enough
                aggregates[("current", condition.name, generator)].add(current)
                aggregates[("nist", condition.name, generator)].add(nist)
                aggregates[("candidate", condition.name, generator)].add(candidate)
        produced += size
        batch_index += 1

    rows = []
    for method in METHODS:
        for condition in CONDITIONS:
            for generator in GENERATOR_NAMES:
                aggregate = aggregates[(method, condition.name, generator)]
                random_truth = generator == "RANDOM"
                error_count = (
                    aggregate.sample_count - aggregate.random_count
                    if random_truth
                    else aggregate.random_count
                )
                low, high = _wilson_interval(error_count, aggregate.sample_count)
                rows.append(
                    {
                        "method": method,
                        "condition": condition.name,
                        "generator_strategy": generator,
                        "binary_truth": "RANDOM" if random_truth else "STRUCTURED",
                        "sample_count": aggregate.sample_count,
                        "random_count": aggregate.random_count,
                        "random_rate": aggregate.random_count / aggregate.sample_count,
                        "error_count": error_count,
                        "error_rate": error_count / aggregate.sample_count,
                        "error_rate_ci95_low": low,
                        "error_rate_ci95_high": high,
                    }
                )
    return rows


def _binary_metrics(rows: list[dict]) -> dict:
    counts = np.zeros((2, 2), dtype=np.int64)
    for row in rows:
        truth_index = 1 if row["binary_truth"] == "RANDOM" else 0
        counts[truth_index, 1] += row["random_count"]
        counts[truth_index, 0] += row["sample_count"] - row["random_count"]
    support = counts.sum(axis=1)
    percentages = np.divide(
        counts * 100.0,
        support[:, None],
        out=np.zeros_like(counts, dtype=float),
        where=support[:, None] > 0,
    )
    return {"counts": counts.tolist(), "row_percentages": percentages.tolist()}


def _draw_binary(axis, metrics: dict, title: str | None = None):
    percentages = np.asarray(metrics["row_percentages"], dtype=float)
    image = axis.imshow(
        percentages,
        cmap=PERCENTAGE_CMAP,
        vmin=0,
        vmax=100,
        aspect="auto",
        interpolation="nearest",
    )
    axis.set_box_aspect(0.72)
    axis.set_xticks(np.arange(2), ["Non-\nRandom", "Random"])
    axis.set_yticks(np.arange(2), ["Structured", "Random"])
    if title:
        axis.set_title(title, pad=4, fontsize=9)
    for row_index in range(2):
        for column_index in range(2):
            percentage = percentages[row_index, column_index]
            axis.text(
                column_index,
                row_index,
                _format_matrix_percentage(float(percentage)),
                ha="center",
                va="center",
                color="white" if percentage >= 50 else "#222222",
                fontsize=7,
            )
    return image


def _plot_candidate_confusion(metrics: dict, output_path: Path) -> Path:
    _configure_evaluation_style()
    fig, axes = plt.subplots(1, 4, figsize=(7.16, 2.55), sharex=True, sharey=True)
    image = None
    for column, condition in enumerate(CONDITIONS):
        image = _draw_binary(
            axes[column],
            metrics["candidate"][condition.name],
            COMPACT_CONDITION_LABELS[condition.name],
        )
        axes[column].tick_params(axis="y", labelleft=column == 0)
    axes[0].set_ylabel("Generating process")
    fig.supxlabel("Candidate decision", y=0.02)
    fig.subplots_adjust(left=0.10, right=0.88, bottom=0.24, top=0.82, wspace=0.22)
    add_fixed_percentage_colorbar(fig, image, left=0.91, center_y=0.53)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    return output_path


def _plot_method_confusion(metrics: dict, output_path: Path) -> Path:
    _configure_evaluation_style()
    fig, axes = plt.subplots(3, 4, figsize=(7.16, 5.35), sharex=True, sharey=True)
    image = None
    for row, method in enumerate(METHODS):
        for column, condition in enumerate(CONDITIONS):
            image = _draw_binary(
                axes[row, column],
                metrics[method][condition.name],
                COMPACT_CONDITION_LABELS[condition.name] if row == 0 else None,
            )
            axes[row, column].tick_params(axis="y", labelleft=column == 0)
            if column == 0:
                axes[row, column].set_ylabel(METHOD_LABELS[method])
    fig.supxlabel("Decision", y=0.015)
    fig.subplots_adjust(
        left=0.17,
        right=0.88,
        bottom=0.12,
        top=0.92,
        hspace=0.32,
        wspace=0.22,
    )
    add_fixed_percentage_colorbar(fig, image, left=0.91, center_y=0.50)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    return output_path


def _plot_candidate_by_generator(rows: list[dict], output_path: Path) -> Path:
    _configure_evaluation_style()
    lookup = {
        (row["generator_strategy"], row["condition"]): row
        for row in rows
        if row["method"] == "candidate"
    }
    matrix = np.asarray(
        [
            [
                100.0 * lookup[(generator, condition.name)]["random_rate"]
                for condition in CONDITIONS
            ]
            for generator in GENERATOR_NAMES
        ]
    )
    fig, axis = plt.subplots(figsize=(7.16, 5.8))
    image = axis.imshow(
        matrix,
        cmap=PERCENTAGE_CMAP,
        vmin=0,
        vmax=100,
        aspect="auto",
        interpolation="nearest",
    )
    axis.set_xticks(
        np.arange(len(CONDITIONS)),
        [COMPACT_CONDITION_LABELS[condition.name] for condition in CONDITIONS],
    )
    axis.set_yticks(
        np.arange(len(GENERATOR_NAMES)),
        [_pretty_generator(name) for name in GENERATOR_NAMES],
    )
    axis.set_xlabel("Synthetic measurement condition")
    axis.set_ylabel("Generating IP-ID process")
    for row_index in range(len(GENERATOR_NAMES)):
        for column_index in range(len(CONDITIONS)):
            percentage = matrix[row_index, column_index]
            axis.text(
                column_index,
                row_index,
                _format_matrix_percentage(float(percentage)),
                ha="center",
                va="center",
                color="white" if percentage >= 50 else "#222222",
                fontsize=7,
            )
    fig.subplots_adjust(left=0.30, right=0.87, bottom=0.16, top=0.98)
    add_fixed_percentage_colorbar(
        fig,
        image,
        left=0.90,
        center_y=0.50,
        label="Classified Random [%]",
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    return output_path


def _write_csv(rows: list[dict], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def _write_json(value: dict, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


def _create_bundle(paths: list[Path], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in paths:
            archive.write(path, arcname=path.name)
    return output_path


def render(
    *,
    samples_per_generator: int = DEFAULT_SAMPLES_PER_GENERATOR,
    candidate_null_samples: int = CANDIDATE_NULL_TABLE_SAMPLES,
    candidate_null_seed: int = CANDIDATE_NULL_TABLE_SEED,
    nist_null_samples: int = CANDIDATE_NULL_TABLE_SAMPLES,
    nist_null_seed: int = NIST_NULL_TABLE_SEED,
    batch_size: int = DEFAULT_BATCH_SIZE,
    seed: int = CANDIDATE_EVALUATION_SEED,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    figure_dir: Path = DEFAULT_FIGURE_DIR,
) -> dict[str, Path]:
    if min(samples_per_generator, candidate_null_samples, nist_null_samples, batch_size) < 1:
        raise ValueError("sample counts and batch size must be positive")
    rows = _evaluate(
        samples_per_generator,
        batch_size,
        seed,
        candidate_null_samples,
        candidate_null_seed,
        nist_null_samples,
        nist_null_seed,
    )
    metrics = {
        method: {
            condition.name: _binary_metrics(
                [
                    row
                    for row in rows
                    if row["method"] == method and row["condition"] == condition.name
                ]
            )
            for condition in CONDITIONS
        }
        for method in METHODS
    }
    csv_path = _write_csv(rows, output_dir / "random-classifier-paper-diagnostics.csv")
    candidate_path = _plot_candidate_confusion(
        metrics,
        figure_dir / "random-classifier-candidate-confusion.pdf",
    )
    method_path = _plot_method_confusion(
        metrics,
        figure_dir / "random-classifier-method-confusion.pdf",
    )
    generator_path = _plot_candidate_by_generator(
        rows,
        figure_dir / "random-classifier-candidate-by-generator.pdf",
    )
    report_path = _write_json(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "production_classifier_changed": False,
            "samples_per_generator_and_condition": samples_per_generator,
            "generator_profile": "heldout",
            "generators": list(GENERATOR_NAMES),
            "conditions": [condition.name for condition in CONDITIONS],
            "methods": list(METHODS),
            "thresholds": {
                "current": RANDOM_STRUCTURE_MIN_SCORE,
                "nist": NIST_COMBINED_THRESHOLD,
                "candidate": CANDIDATE_RANDOM_MIN_SCORE,
            },
            "binary_metrics": metrics,
        },
        output_dir / "random-classifier-paper-diagnostics.json",
    )
    paths = [csv_path, report_path, candidate_path, method_path, generator_path]
    bundle_path = _create_bundle(
        paths,
        output_dir / "random-classifier-paper-review-bundle.zip",
    )
    return {
        "aggregate_csv": csv_path,
        "report_json": report_path,
        "candidate_confusion_pdf": candidate_path,
        "method_confusion_pdf": method_path,
        "candidate_by_generator_pdf": generator_path,
        "review_bundle": bundle_path,
    }


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
    samples_per_generator: int = typer.Option(DEFAULT_SAMPLES_PER_GENERATOR, min=1),
    candidate_null_samples: int = typer.Option(CANDIDATE_NULL_TABLE_SAMPLES, min=1),
    candidate_null_seed: int = typer.Option(CANDIDATE_NULL_TABLE_SEED),
    nist_null_samples: int = typer.Option(CANDIDATE_NULL_TABLE_SAMPLES, min=1),
    nist_null_seed: int = typer.Option(NIST_NULL_TABLE_SEED),
    batch_size: int = typer.Option(DEFAULT_BATCH_SIZE, min=1),
    seed: int = typer.Option(CANDIDATE_EVALUATION_SEED),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR),  # noqa: B008
    figure_dir: Path = typer.Option(DEFAULT_FIGURE_DIR),  # noqa: B008
) -> None:
    log_path = output_dir / "random-classifier-paper-diagnostics.log"
    _configure_logging(log_path)
    outputs = render(
        samples_per_generator=samples_per_generator,
        candidate_null_samples=candidate_null_samples,
        candidate_null_seed=candidate_null_seed,
        nist_null_samples=nist_null_samples,
        nist_null_seed=nist_null_seed,
        batch_size=batch_size,
        seed=seed,
        output_dir=output_dir,
        figure_dir=figure_dir,
    )
    for handler in logging.getLogger().handlers:
        handler.flush()
    bundle_inputs = [path for name, path in outputs.items() if name != "review_bundle"]
    bundle_inputs.append(log_path)
    _create_bundle(bundle_inputs, outputs["review_bundle"])
    for name, path in outputs.items():
        typer.echo(f"{name}: {path}")
    typer.echo(f"run_log: {log_path}")


if __name__ == "__main__":
    app()
