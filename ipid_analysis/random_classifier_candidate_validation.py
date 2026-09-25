"""Paper figures for the selected validation-only RANDOM classifier candidate.

The experiment compares the currently deployed RANDOM decision stage with the
selected minimum of raw-, increment-, and gap-uniformity p-values.  It uses the
independent held-out v2 generators and paired ideal, 20% loss, and 20% loss plus
20% reordering conditions.  Production classification remains unchanged.
"""

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

from ipid_analysis.classifier_validation import FIXED_CONFIG
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.paper_figures import PERCENTAGE_CMAP
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_EVALUATION_SEED,
    CANDIDATE_NULL_TABLE_SAMPLES,
    CANDIDATE_NULL_TABLE_SEED,
    CANDIDATE_NULL_TABLE_VERSION,
    CANDIDATE_RANDOM_METRICS,
    CANDIDATE_RANDOM_MIN_SCORE,
    CANDIDATE_RANDOM_SCORE_VERSION,
    CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE,
    candidate_random_score_components,
)
from ipid_analysis.random_classifier_evaluation import (
    EmpiricalNullTables,
    ImpairmentCondition,
    _configure_evaluation_style,
    _stable_rng,
    _wilson_interval,
)
from ipid_analysis.random_classifier_evaluation_v2 import (
    GENERATOR_NAMES,
    generate_v2_sequences,
)
from ipid_analysis.random_classifier_evaluation_v2 import (
    apply_v2_impairment as apply_impairment,
)
from ipid_analysis.strategies import (
    RANDOM_STRUCTURE_MIN_SCORE,
    RANDOM_STRUCTURE_MIN_TEST_SAMPLES,
    RANDOM_STRUCTURE_SCORE_VERSION,
    random_structure_scores,
)

app = typer.Typer(add_completion=False)
LOGGER = logging.getLogger(__name__)

DEFAULT_SAMPLES_PER_GENERATOR = 100_000
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_OUTPUT_DIR = PROCESSED_DATA_DIR / "classifier-validation"
DEFAULT_FIGURE_DIR = FIGURES_DIR / "classifier-validation"

MODEL_NAMES = ("current", "candidate")
MODEL_LABELS = {
    "current": "Current production score",
    "candidate": "Candidate: Raw + Increment + Gap",
}
CONDITIONS = (
    ImpairmentCondition("ideal"),
    ImpairmentCondition("loss-20-random", loss_fraction=0.20),
    ImpairmentCondition(
        "loss-20-random-reorder-20",
        loss_fraction=0.20,
        reorder_fraction=0.20,
    ),
)
CONDITION_LABELS = {
    "ideal": "Ideal",
    "loss-20-random": "20% loss",
    "loss-20-random-reorder-20": "20% loss + 20% reordering",
}
BINARY_TRUTH_ORDER = ("STRUCTURED", "RANDOM")
BINARY_DECISION_ORDER = ("NON-RANDOM", "RANDOM")


@dataclass
class Aggregate:
    random_count: int = 0
    sample_count: int = 0

    def add(self, random_compatible: np.ndarray) -> None:
        self.random_count += int(random_compatible.sum())
        self.sample_count += len(random_compatible)


def _pretty_generator(name: str) -> str:
    return name.replace("_", " ").title().replace("Per ", "Per-")


def _condition_metadata(condition: ImpairmentCondition) -> dict:
    return {
        "name": condition.name,
        "label": CONDITION_LABELS[condition.name],
        "loss_fraction": condition.loss_fraction,
        "reorder_fraction_of_present": condition.reorder_fraction,
        "loss_pattern": condition.loss_pattern,
    }


def _binary_metrics(rows: list[dict]) -> dict:
    counts = np.zeros((2, 2), dtype=np.int64)
    for row in rows:
        truth_index = 1 if row["generator_strategy"] == "RANDOM" else 0
        random_count = row["random_count"]
        nonrandom_count = row["sample_count"] - random_count
        counts[truth_index, 0] += nonrandom_count
        counts[truth_index, 1] += random_count
    support = counts.sum(axis=1)
    percentages = np.divide(
        counts * 100.0,
        support[:, None],
        out=np.zeros_like(counts, dtype=float),
        where=support[:, None] > 0,
    )
    structured_total = int(support[0])
    random_total = int(support[1])
    false_random_count = int(counts[0, 1])
    false_rejection_count = int(counts[1, 0])
    false_random_rate = false_random_count / structured_total
    false_rejection_rate = false_rejection_count / random_total
    return {
        "truth_order": list(BINARY_TRUTH_ORDER),
        "decision_order": list(BINARY_DECISION_ORDER),
        "counts": counts.tolist(),
        "row_percentages": percentages.tolist(),
        "structured_sample_count": structured_total,
        "random_sample_count": random_total,
        "false_random_count": false_random_count,
        "false_random_rate": false_random_rate,
        "false_random_rate_ci95": list(_wilson_interval(false_random_count, structured_total)),
        "random_false_rejection_count": false_rejection_count,
        "random_false_rejection_rate": false_rejection_rate,
        "random_false_rejection_rate_ci95": list(
            _wilson_interval(false_rejection_count, random_total)
        ),
        "balanced_accuracy": float(
            0.5 * (counts[0, 0] / structured_total + counts[1, 1] / random_total)
        ),
    }


def _evaluation_rows(
    samples_per_generator: int,
    batch_size: int,
    seed: int,
    null_tables: EmpiricalNullTables,
    candidate_threshold: float,
) -> list[dict]:
    aggregates = {
        (model, condition.name, generator): Aggregate()
        for model in MODEL_NAMES
        for condition in CONDITIONS
        for generator in GENERATOR_NAMES
    }
    produced = 0
    batch_index = 0
    while produced < samples_per_generator:
        size = min(batch_size, samples_per_generator - produced)
        LOGGER.info(
            "Evaluating held-out batch %d (%d..%d per generator)",
            batch_index + 1,
            produced,
            produced + size - 1,
        )
        generated = generate_v2_sequences(
            size,
            _stable_rng(seed, 501, batch_index),
            "heldout",
        )
        for generator_index, generator in enumerate(GENERATOR_NAMES):
            ideal = generated[generator].values
            for condition_index, condition in enumerate(CONDITIONS):
                values, present, _ = apply_impairment(
                    ideal,
                    condition,
                    _stable_rng(
                        seed,
                        502,
                        batch_index,
                        generator_index,
                        condition_index,
                    ),
                )
                present_count = present.sum(axis=1)
                enough = present_count >= RANDOM_STRUCTURE_MIN_TEST_SAMPLES
                current = (
                    random_structure_scores(values, present, FIXED_CONFIG)
                    >= RANDOM_STRUCTURE_MIN_SCORE
                ) & enough
                candidate = (
                    candidate_random_score_components(
                        values,
                        present,
                        null_tables,
                    ).score
                    >= candidate_threshold
                ) & enough
                aggregates[("current", condition.name, generator)].add(current)
                aggregates[("candidate", condition.name, generator)].add(candidate)
        produced += size
        batch_index += 1

    rows = []
    for model in MODEL_NAMES:
        for condition in CONDITIONS:
            for generator in GENERATOR_NAMES:
                aggregate = aggregates[(model, condition.name, generator)]
                truth = "RANDOM" if generator == "RANDOM" else "STRUCTURED"
                if truth == "RANDOM":
                    error_count = aggregate.sample_count - aggregate.random_count
                    error_type = "random_false_rejection"
                else:
                    error_count = aggregate.random_count
                    error_type = "false_random"
                error_rate = error_count / aggregate.sample_count
                low, high = _wilson_interval(error_count, aggregate.sample_count)
                rows.append(
                    {
                        "model": model,
                        "condition": condition.name,
                        "generator_strategy": generator,
                        "binary_truth": truth,
                        "sample_count": aggregate.sample_count,
                        "random_count": aggregate.random_count,
                        "random_rate": aggregate.random_count / aggregate.sample_count,
                        "error_type": error_type,
                        "error_count": error_count,
                        "error_rate": error_rate,
                        "error_rate_ci95_low": low,
                        "error_rate_ci95_high": high,
                    }
                )
    return rows


def _write_csv(rows: list[dict], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def _write_json(value: dict, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    return output_path


def _draw_binary_matrix(ax, metrics: dict, *, title: str | None = None):
    percentages = np.asarray(metrics["row_percentages"], dtype=float)
    image = ax.imshow(
        percentages,
        cmap=PERCENTAGE_CMAP,
        vmin=0,
        vmax=100,
        aspect="equal",
        interpolation="nearest",
    )
    ax.set_xticks(np.arange(2), ["Non-\nRANDOM", "RANDOM"])
    ax.set_yticks(np.arange(2), ["Structured", "RANDOM"])
    if title:
        ax.set_title(title, fontsize=10, pad=5)
    for row_index in range(2):
        for column_index in range(2):
            percentage = percentages[row_index, column_index]
            ax.text(
                column_index,
                row_index,
                f"{percentage:.2f}",
                ha="center",
                va="center",
                color="white" if percentage >= 50 else "#222222",
                fontsize=8,
            )
    return image


def _save_candidate_confusion(metrics: dict, output_path: Path) -> Path:
    _configure_evaluation_style()
    fig = plt.figure(figsize=(7.16, 2.75))
    grid = fig.add_gridspec(
        1,
        4,
        width_ratios=(1, 1, 1, 0.06),
        left=0.12,
        right=0.90,
        bottom=0.25,
        top=0.88,
        wspace=0.38,
    )
    axes = [fig.add_subplot(grid[0, 0])]
    axes.extend(
        fig.add_subplot(grid[0, column_index], sharex=axes[0], sharey=axes[0])
        for column_index in range(1, 3)
    )
    image = None
    for column_index, (ax, condition) in enumerate(zip(axes, CONDITIONS, strict=True)):
        image = _draw_binary_matrix(
            ax,
            metrics["candidate"][condition.name],
            title=CONDITION_LABELS[condition.name],
        )
        ax.tick_params(
            axis="y",
            left=column_index == 0,
            labelleft=column_index == 0,
        )
    axes[0].set_ylabel("Generating process")
    fig.supxlabel("Candidate decision", y=0.02)
    colorbar_axis = fig.add_subplot(grid[0, 3])
    fig.colorbar(image, cax=colorbar_axis, label="Percentage [%]")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    return output_path


def _save_comparison_confusion(metrics: dict, output_path: Path) -> Path:
    _configure_evaluation_style()
    fig = plt.figure(figsize=(7.16, 4.65))
    grid = fig.add_gridspec(
        2,
        4,
        width_ratios=(1, 1, 1, 0.06),
        left=0.19,
        right=0.90,
        bottom=0.15,
        top=0.94,
        hspace=0.34,
        wspace=0.38,
    )
    axes = np.empty((2, 3), dtype=object)
    axes[0, 0] = fig.add_subplot(grid[0, 0])
    for row_index in range(2):
        for column_index in range(3):
            if row_index == 0 and column_index == 0:
                continue
            axes[row_index, column_index] = fig.add_subplot(
                grid[row_index, column_index],
                sharex=axes[0, 0],
                sharey=axes[0, 0],
            )
    image = None
    for row_index, model in enumerate(MODEL_NAMES):
        for column_index, condition in enumerate(CONDITIONS):
            ax = axes[row_index, column_index]
            image = _draw_binary_matrix(
                ax,
                metrics[model][condition.name],
                title=(CONDITION_LABELS[condition.name] if row_index == 0 else None),
            )
            ax.tick_params(
                axis="y",
                left=column_index == 0,
                labelleft=column_index == 0,
            )
            if column_index == 0:
                ax.set_ylabel(MODEL_LABELS[model] + "\nGenerating process")
    fig.supxlabel("Decision", y=0.02)
    colorbar_axis = fig.add_subplot(grid[:, 3])
    fig.colorbar(image, cax=colorbar_axis, label="Percentage [%]")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    return output_path


def _save_by_generator(rows: list[dict], output_path: Path) -> Path:
    _configure_evaluation_style()
    candidate_rows = {
        (row["generator_strategy"], row["condition"]): row
        for row in rows
        if row["model"] == "candidate"
    }
    matrix = np.asarray(
        [
            [
                100.0 * candidate_rows[(generator, condition.name)]["random_rate"]
                for condition in CONDITIONS
            ]
            for generator in GENERATOR_NAMES
        ]
    )
    fig, ax = plt.subplots(figsize=(7.16, 5.75))
    image = ax.imshow(
        matrix,
        cmap=PERCENTAGE_CMAP,
        vmin=0,
        vmax=100,
        aspect="auto",
        interpolation="nearest",
    )
    ax.set_xticks(
        np.arange(len(CONDITIONS)),
        [CONDITION_LABELS[condition.name] for condition in CONDITIONS],
    )
    ax.set_yticks(
        np.arange(len(GENERATOR_NAMES)),
        [_pretty_generator(name) for name in GENERATOR_NAMES],
    )
    ax.set_xlabel("Synthetic measurement condition")
    ax.set_ylabel("Generating IP-ID process")
    for row_index in range(len(GENERATOR_NAMES)):
        for column_index in range(len(CONDITIONS)):
            percentage = matrix[row_index, column_index]
            ax.text(
                column_index,
                row_index,
                f"{percentage:.1f}",
                ha="center",
                va="center",
                color="white" if percentage >= 50 else "#222222",
                fontsize=7,
            )
    colorbar = fig.colorbar(image, ax=ax, pad=0.025, fraction=0.04)
    colorbar.set_label("Classified RANDOM [%]")
    fig.subplots_adjust(left=0.30, right=0.89, bottom=0.13, top=0.98)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    return output_path


def _create_bundle(paths: list[Path], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in paths:
            archive.write(path, arcname=path.name)
    return output_path


def validate_random_classifier_candidate(
    *,
    samples_per_generator: int = DEFAULT_SAMPLES_PER_GENERATOR,
    null_table_samples: int = CANDIDATE_NULL_TABLE_SAMPLES,
    null_table_seed: int = CANDIDATE_NULL_TABLE_SEED,
    candidate_threshold: float = CANDIDATE_RANDOM_MIN_SCORE,
    batch_size: int = DEFAULT_BATCH_SIZE,
    seed: int = CANDIDATE_EVALUATION_SEED,
    output_dir: Path | None = None,
    figure_dir: Path | None = None,
) -> dict[str, Path]:
    """Generate paper and review artifacts without activating the candidate."""
    if min(samples_per_generator, null_table_samples, batch_size) < 1:
        raise ValueError("sample counts and batch size must be positive")
    if not 0.0 <= candidate_threshold <= 1.0:
        raise ValueError("candidate threshold must lie in [0, 1]")
    output_dir = output_dir or DEFAULT_OUTPUT_DIR
    figure_dir = figure_dir or DEFAULT_FIGURE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    figure_dir.mkdir(parents=True, exist_ok=True)

    specification_matches = (
        samples_per_generator == DEFAULT_SAMPLES_PER_GENERATOR
        and null_table_samples == CANDIDATE_NULL_TABLE_SAMPLES
        and null_table_seed == CANDIDATE_NULL_TABLE_SEED
        and candidate_threshold == CANDIDATE_RANDOM_MIN_SCORE
        and batch_size == DEFAULT_BATCH_SIZE
        and seed == CANDIDATE_EVALUATION_SEED
    )
    if not specification_matches:
        LOGGER.warning(
            "Exploratory parameters differ from the selected candidate specification; "
            "figures are not paper-conclusive"
        )

    null_tables = EmpiricalNullTables(null_table_samples, null_table_seed)
    rows = _evaluation_rows(
        samples_per_generator,
        batch_size,
        seed,
        null_tables,
        candidate_threshold,
    )
    metrics = {
        model: {
            condition.name: _binary_metrics(
                [
                    row
                    for row in rows
                    if row["model"] == model and row["condition"] == condition.name
                ]
            )
            for condition in CONDITIONS
        }
        for model in MODEL_NAMES
    }

    csv_path = _write_csv(
        rows,
        output_dir / "random-classifier-candidate-validation.csv",
    )
    candidate_confusion_path = _save_candidate_confusion(
        metrics,
        figure_dir / "random-classifier-candidate-confusion.pdf",
    )
    comparison_confusion_path = _save_comparison_confusion(
        metrics,
        figure_dir / "random-classifier-current-vs-candidate-confusion.pdf",
    )
    by_generator_path = _save_by_generator(
        rows,
        figure_dir / "random-classifier-candidate-by-generator.pdf",
    )
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "purpose": "validation-only paper figures before production activation",
        "production_classifier_changed": False,
        "candidate_specification_matches_selected_evaluation": specification_matches,
        "samples_per_generator_and_condition": samples_per_generator,
        "batch_size": batch_size,
        "evaluation_seed": seed,
        "generator_profile": "heldout",
        "generators": list(GENERATOR_NAMES),
        "conditions": [_condition_metadata(condition) for condition in CONDITIONS],
        "current": {
            "score_version": RANDOM_STRUCTURE_SCORE_VERSION,
            "threshold": RANDOM_STRUCTURE_MIN_SCORE,
        },
        "candidate": {
            "score_version": CANDIDATE_RANDOM_SCORE_VERSION,
            "metrics": list(CANDIDATE_RANDOM_METRICS),
            "combiner": "minimum",
            "threshold": candidate_threshold,
            "target_random_false_rejection_rate": (CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE),
            "null_tables": {
                "version": CANDIDATE_NULL_TABLE_VERSION,
                "sample_count": null_table_samples,
                "seed": null_table_seed,
                "pvalue_resolution": 1.0 / (null_table_samples + 1.0),
            },
        },
        "binary_metrics": metrics,
        "artifacts": {
            "aggregate_csv": str(csv_path),
            "candidate_confusion_pdf": str(candidate_confusion_path),
            "comparison_confusion_pdf": str(comparison_confusion_path),
            "candidate_by_generator_pdf": str(by_generator_path),
        },
    }
    json_path = _write_json(
        report,
        figure_dir / "random-classifier-candidate-validation.json",
    )
    bundle_path = _create_bundle(
        [
            csv_path,
            json_path,
            candidate_confusion_path,
            comparison_confusion_path,
            by_generator_path,
        ],
        output_dir / "random-classifier-candidate-review-bundle.zip",
    )
    return {
        "aggregate_csv": csv_path,
        "validation_json": json_path,
        "candidate_confusion_pdf": candidate_confusion_path,
        "comparison_confusion_pdf": comparison_confusion_path,
        "candidate_by_generator_pdf": by_generator_path,
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


@app.command()
def main(
    samples_per_generator: int = typer.Option(DEFAULT_SAMPLES_PER_GENERATOR, min=1),
    null_table_samples: int = typer.Option(CANDIDATE_NULL_TABLE_SAMPLES, min=1),
    null_table_seed: int = typer.Option(CANDIDATE_NULL_TABLE_SEED),
    candidate_threshold: float = typer.Option(
        CANDIDATE_RANDOM_MIN_SCORE,
        min=0.0,
        max=1.0,
    ),
    batch_size: int = typer.Option(DEFAULT_BATCH_SIZE, min=1),
    seed: int = typer.Option(CANDIDATE_EVALUATION_SEED),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR),  # noqa: B008
    figure_dir: Path = typer.Option(DEFAULT_FIGURE_DIR),  # noqa: B008
) -> None:
    log_path = output_dir / "random-classifier-candidate-validation.log"
    _configure_logging(log_path)
    outputs = validate_random_classifier_candidate(
        samples_per_generator=samples_per_generator,
        null_table_samples=null_table_samples,
        null_table_seed=null_table_seed,
        candidate_threshold=candidate_threshold,
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
