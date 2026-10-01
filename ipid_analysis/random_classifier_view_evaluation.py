"""Staged offline evaluation of view evidence for RANDOM classification.

This module deliberately does not change production classification.  It compares
the selected ``min(raw, increment, gap)`` validation candidate with alternatives
that aggregate independent destination/connection evidence, add circular-spacing
subsequence views, and vary the increment bin rule.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time
import zipfile

import matplotlib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.special import gammaincc
import typer

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from ipid_analysis.classifier_validation import FIXED_CONFIG, generate_fixed_sequences
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.increment_bin_rule_evaluation import (
    IncrementBinNullTables,
    IncrementBinRule,
    increment_view_pvalues_for_rule,
    selected_bin_counts,
)
from ipid_analysis.random_classifier_evaluation import (
    EmpiricalNullTables,
    ImpairmentCondition,
    _configure_evaluation_style,
    _right_tail_pvalues,
    _spacing_cvm_statistics,
    _stable_rng,
    _threshold_at_false_rejection,
    _wilson_interval,
)
from ipid_analysis.random_classifier_evaluation_v2 import (
    GeneratedBatch,
    apply_v2_impairment,
    generate_v2_sequences,
)
from ipid_analysis.strategies import (
    MODULUS,
    RANDOM_STRUCTURE_MIN_TEST_SAMPLES,
    random_structure_features,
)

app = typer.Typer(add_completion=False)
LOGGER = logging.getLogger(__name__)

EXPERIMENT_VERSION = "1"
VIEW_NAMES = ("full", "dst0", "dst1", "con0", "con1", "con2", "con3")
CORE_CONDITIONS = (
    ImpairmentCondition("ideal"),
    ImpairmentCondition("lossy", loss_fraction=0.20),
    ImpairmentCondition("reordered", reorder_fraction=0.20),
    ImpairmentCondition("lossy-reordered", loss_fraction=0.20, reorder_fraction=0.20),
)
PROFILE_NAMES = ("paper", "heldout")
TARGET_RANDOM_FALSE_REJECTION_RATE = 0.0001
DEFAULT_OUTPUT_DIR = (
    PROCESSED_DATA_DIR / "classifier-validation" / "random-classifier-view-evaluation"
)
DEFAULT_FIGURE_DIR = (
    FIGURES_DIR / "classifier-validation" / "random-classifier-view-evaluation"
)


@dataclass(frozen=True)
class EvaluationPreset:
    paper_samples: int
    heldout_samples: int
    calibration_samples: int
    null_samples: int
    batch_size: int


PRESETS = {
    "screening": EvaluationPreset(5_000, 5_000, 50_000, 100_000, 5_000),
    "confirmation": EvaluationPreset(100_000, 100_000, 250_000, 500_000, 10_000),
}


BIN_RULES = (
    IncrementBinRule(
        "multiscale-e2",
        "multiscale",
        2,
        "jointly calibrated 3/4/8/16-bin minimum (current candidate)",
        bin_counts=(3, 4, 8, 16),
    ),
    IncrementBinRule(
        "multiscale-e3",
        "multiscale",
        3,
        "jointly calibrated usable 3/4/8/16-bin tests with e=3",
        bin_counts=(3, 4, 8, 16),
        minimum_transition_count=9,
    ),
    IncrementBinRule(
        "singlescale-e2",
        "single",
        2,
        "one largest usable resolution from 3/4/8/16 with e=2",
        bin_counts=(3, 4, 8, 16),
    ),
    IncrementBinRule(
        "singlescale-e3",
        "single",
        3,
        "one largest usable resolution from 3/4/8/16 with e=3",
        bin_counts=(3, 4, 8, 16),
        minimum_transition_count=9,
    ),
    IncrementBinRule(
        "fixed-3",
        "fixed3",
        description="fixed thirds aligned with the MAX_INC support",
        minimum_transition_count=9,
    ),
    IncrementBinRule(
        "thirds-e3",
        "thirds",
        3,
        "one largest usable resolution from 3/6/9/12 with e=3",
        minimum_transition_count=9,
    ),
)
RULE_BY_NAME = {rule.name: rule for rule in BIN_RULES}


@dataclass(frozen=True)
class Candidate:
    name: str
    rule_name: str
    increment_mode: str
    gap_mode: str
    include_raw: bool
    include_increment: bool
    include_gap: bool


def _candidates() -> tuple[Candidate, ...]:
    candidates = []
    for rule in BIN_RULES:
        for increment_mode in ("minimum", "aggregate"):
            for gap_mode in ("full", "aggregate"):
                for include_raw in (False, True):
                    raw = "raw+" if include_raw else ""
                    candidates.append(
                        Candidate(
                            f"{raw}{rule.name}:inc-{increment_mode}:gap-{gap_mode}",
                            rule.name,
                            increment_mode,
                            gap_mode,
                            include_raw,
                            True,
                            True,
                        )
                    )
    candidates.extend(
        (
            Candidate("raw-only", "multiscale-e2", "minimum", "full", True, False, False),
            Candidate("gap-full-only", "multiscale-e2", "minimum", "full", False, False, True),
            Candidate(
                "gap-aggregate-only",
                "multiscale-e2",
                "minimum",
                "aggregate",
                False,
                False,
                True,
            ),
        )
    )
    for rule in BIN_RULES:
        for increment_mode in ("minimum", "aggregate"):
            candidates.append(
                Candidate(
                    f"{rule.name}:inc-{increment_mode}-only",
                    rule.name,
                    increment_mode,
                    "full",
                    False,
                    True,
                    False,
                )
            )
    return tuple(candidates)


ALL_CANDIDATES = _candidates()
BASELINE_NAME = "raw+multiscale-e2:inc-minimum:gap-full"


def _view_arrays(
    values: np.ndarray,
    present: np.ndarray,
) -> tuple[tuple[np.ndarray, np.ndarray], ...]:
    connection_values = values.reshape(
        len(values),
        FIXED_CONFIG.requests_per_connection,
        FIXED_CONFIG.connection_count,
    ).transpose(0, 2, 1)
    connection_present = present.reshape(
        len(values),
        FIXED_CONFIG.requests_per_connection,
        FIXED_CONFIG.connection_count,
    ).transpose(0, 2, 1)
    return (
        (values, present),
        (values[:, 0::2], present[:, 0::2]),
        (values[:, 1::2], present[:, 1::2]),
        *((connection_values[:, index], connection_present[:, index]) for index in range(4)),
    )


def gap_view_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    null_tables: EmpiricalNullTables,
) -> np.ndarray:
    """Return spacing p-values in the stable seven-view order."""
    components = [
        _gap_view_pvalues(view_values, view_present, null_tables)
        for view_values, view_present in _view_arrays(values, present)
    ]
    return np.column_stack(components)


def _gap_view_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    null_tables: EmpiricalNullTables,
) -> np.ndarray:
    counts = present.sum(axis=1).astype(np.int64)
    statistics = _spacing_cvm_statistics(values, present)
    result = np.ones(len(values), dtype=float)
    for count in np.unique(counts):
        if count < RANDOM_STRUCTURE_MIN_TEST_SAMPLES:
            continue
        rows = np.flatnonzero(counts == count)
        result[rows] = _right_tail_pvalues(
            statistics[rows],
            null_tables.spacing(int(count)),
        )
    return result


def _increment_valid_views(
    present: np.ndarray,
    rule: IncrementBinRule,
) -> np.ndarray:
    columns = []
    for _, view_present in _view_arrays(np.zeros_like(present, dtype=np.uint16), present):
        transition_counts = (view_present[:, :-1] & view_present[:, 1:]).sum(axis=1)
        columns.append(
            np.fromiter(
                (bool(selected_bin_counts(rule, int(count))) for count in transition_counts),
                dtype=bool,
                count=len(present),
            )
        )
    return np.column_stack(columns)


def _gap_valid_views(present: np.ndarray) -> np.ndarray:
    return np.column_stack(
        [
            view_present.sum(axis=1) >= RANDOM_STRUCTURE_MIN_TEST_SAMPLES
            for _, view_present in _view_arrays(np.zeros_like(present, dtype=np.uint16), present)
        ]
    )


def fisher_compatibility(pvalues: np.ndarray, valid: np.ndarray) -> np.ndarray:
    """Fisher-combine disjoint views, excluding unavailable short views."""
    clipped = np.clip(pvalues, np.finfo(float).tiny, 1.0)
    active = valid.sum(axis=1)
    log_sum = np.where(valid, np.log(clipped), 0.0).sum(axis=1)
    result = np.ones(len(pvalues), dtype=float)
    rows = active > 0
    # Fisher T=-2*sum(log(p)); chi-square survival is gammaincc(k, T/2).
    result[rows] = gammaincc(active[rows], -log_sum[rows])
    return result


def hierarchical_score(pvalues: np.ndarray, valid: np.ndarray) -> np.ndarray:
    """Minimum of full, combined destination, and combined connection evidence."""
    full = np.where(valid[:, 0], pvalues[:, 0], 1.0)
    destinations = fisher_compatibility(pvalues[:, 1:3], valid[:, 1:3])
    connections = fisher_compatibility(pvalues[:, 3:7], valid[:, 3:7])
    return np.minimum.reduce((full, destinations, connections))


def _primitive_scores(
    values: np.ndarray,
    present: np.ndarray,
    increment_tables: IncrementBinNullTables,
    spacing_tables: EmpiricalNullTables,
) -> dict[str, np.ndarray]:
    result = {
        "raw": random_structure_features(values, present).uniformity_pvalue,
        "gap_views": gap_view_pvalues(values, present, spacing_tables),
    }
    gap_valid = _gap_valid_views(present)
    result["gap_minimum"] = result["gap_views"].min(axis=1)
    result["gap_full"] = result["gap_views"][:, 0]
    result["gap_aggregate"] = hierarchical_score(result["gap_views"], gap_valid)
    for rule in BIN_RULES:
        views = increment_view_pvalues_for_rule(values, present, rule, increment_tables)
        valid = _increment_valid_views(present, rule)
        result[f"inc_views:{rule.name}"] = views
        result[f"inc_minimum:{rule.name}"] = views.min(axis=1)
        result[f"inc_aggregate:{rule.name}"] = hierarchical_score(views, valid)
    return result


def _component_scores(
    primitive: dict[str, np.ndarray],
    candidate: Candidate,
) -> dict[str, np.ndarray]:
    components = {}
    if candidate.include_increment:
        components["increment"] = primitive[
            f"inc_{candidate.increment_mode}:{candidate.rule_name}"
        ]
    if candidate.include_gap:
        components["gap"] = primitive[f"gap_{candidate.gap_mode}"]
    if candidate.include_raw:
        components["raw"] = primitive["raw"]
    return components


def candidate_scores(
    primitive: dict[str, np.ndarray],
    candidates: tuple[Candidate, ...],
) -> np.ndarray:
    return np.column_stack(
        [
            np.minimum.reduce(tuple(_component_scores(primitive, item).values()))
            for item in candidates
        ]
    )


def _paper_batches(sample_count: int, rng: np.random.Generator) -> dict[str, GeneratedBatch]:
    return {
        name: GeneratedBatch(
            values,
            np.full(sample_count, -1, dtype=np.int32),
            np.zeros(sample_count, dtype=np.int16),
        )
        for name, values in generate_fixed_sequences(sample_count, rng).items()
    }


def _generate_profile(
    profile: str,
    sample_count: int,
    rng: np.random.Generator,
) -> dict[str, GeneratedBatch]:
    return _paper_batches(sample_count, rng) if profile == "paper" else generate_v2_sequences(
        sample_count, rng, "heldout"
    )


def _calibrate(
    sample_count: int,
    batch_size: int,
    seed: int,
    candidates: tuple[Candidate, ...],
    increment_tables: IncrementBinNullTables,
    spacing_tables: EmpiricalNullTables,
) -> dict[str, dict]:
    by_condition: list[list[np.ndarray]] = [[] for _ in CORE_CONDITIONS]
    for condition_index, condition in enumerate(CORE_CONDITIONS):
        LOGGER.info("Calibrating %s (%d RANDOM sequences)", condition.name, sample_count)
        for offset in range(0, sample_count, batch_size):
            size = min(batch_size, sample_count - offset)
            ideal = _stable_rng(seed, 101, condition_index, offset).integers(
                0,
                MODULUS,
                size=(size, FIXED_CONFIG.sequence_length),
                dtype=np.uint16,
            )
            values, present, _ = apply_v2_impairment(
                ideal,
                condition,
                _stable_rng(seed, 102, condition_index, offset),
            )
            primitive = _primitive_scores(values, present, increment_tables, spacing_tables)
            by_condition[condition_index].append(
                candidate_scores(primitive, candidates).astype(np.float32)
            )
    arrays = [np.concatenate(parts) for parts in by_condition]
    calibrated = {}
    for candidate_index, candidate in enumerate(candidates):
        thresholds = {
            condition.name: _threshold_at_false_rejection(
                arrays[index][:, candidate_index],
                TARGET_RANDOM_FALSE_REJECTION_RATE,
            )
            for index, condition in enumerate(CORE_CONDITIONS)
        }
        threshold = min(thresholds.values())
        calibrated[candidate.name] = {
            "threshold": threshold,
            "threshold_by_condition": thresholds,
            "false_rejection_by_condition": {
                condition.name: float((arrays[index][:, candidate_index] < threshold).mean())
                for index, condition in enumerate(CORE_CONDITIONS)
            },
        }
    return calibrated


def _score_schema() -> pa.Schema:
    fields = [
        ("PROFILE", pa.string()),
        ("CONDITION", pa.string()),
        ("GENERATOR_STRATEGY", pa.string()),
        ("SAMPLE_ID", pa.int64()),
        ("PRESENT_COUNT", pa.int16()),
        ("RAW", pa.float32()),
    ]
    fields.extend((f"GAP_{name.upper()}", pa.float32()) for name in VIEW_NAMES)
    for rule in BIN_RULES:
        fields.extend(
            (f"INC_{rule.name.upper().replace('-', '_')}_{name.upper()}", pa.float32())
            for name in VIEW_NAMES
        )
    return pa.schema(fields)


def _write_score_batch(
    writer: pq.ParquetWriter,
    profile: str,
    condition: str,
    strategy: str,
    sample_offset: int,
    present: np.ndarray,
    primitive: dict[str, np.ndarray],
) -> None:
    size = len(present)
    columns: dict[str, object] = {
        "PROFILE": [profile] * size,
        "CONDITION": [condition] * size,
        "GENERATOR_STRATEGY": [strategy] * size,
        "SAMPLE_ID": np.arange(sample_offset, sample_offset + size, dtype=np.int64),
        "PRESENT_COUNT": present.sum(axis=1).astype(np.int16),
        "RAW": primitive["raw"].astype(np.float32),
    }
    for index, name in enumerate(VIEW_NAMES):
        columns[f"GAP_{name.upper()}"] = primitive["gap_views"][:, index].astype(np.float32)
    for rule in BIN_RULES:
        key = rule.name.upper().replace("-", "_")
        views = primitive[f"inc_views:{rule.name}"]
        for index, name in enumerate(VIEW_NAMES):
            columns[f"INC_{key}_{name.upper()}"] = views[:, index].astype(np.float32)
    writer.write_table(pa.Table.from_pydict(columns, schema=writer.schema))


def _evaluate(
    preset: EvaluationPreset,
    seed: int,
    candidates: tuple[Candidate, ...],
    calibrated: dict[str, dict],
    increment_tables: IncrementBinNullTables,
    spacing_tables: EmpiricalNullTables,
    score_path: Path,
) -> tuple[list[dict], list[dict]]:
    counts: dict[tuple[str, str, str, str], list[int]] = {}
    catches: dict[tuple[str, str, str, str, str], int] = {}
    schema = _score_schema()
    with pq.ParquetWriter(score_path, schema, compression="zstd") as writer:
        for profile_index, profile in enumerate(PROFILE_NAMES):
            sample_count = preset.paper_samples if profile == "paper" else preset.heldout_samples
            for offset in range(0, sample_count, preset.batch_size):
                size = min(preset.batch_size, sample_count - offset)
                LOGGER.info("Evaluating %s samples %d..%d", profile, offset, offset + size - 1)
                generated = _generate_profile(
                    profile,
                    size,
                    _stable_rng(seed, 201, profile_index, offset),
                )
                for strategy_index, (strategy, batch) in enumerate(generated.items()):
                    for condition_index, condition in enumerate(CORE_CONDITIONS):
                        values, present, _ = apply_v2_impairment(
                            batch.values,
                            condition,
                            _stable_rng(
                                seed,
                                202,
                                profile_index,
                                offset,
                                strategy_index,
                                condition_index,
                            ),
                        )
                        primitive = _primitive_scores(
                            values,
                            present,
                            increment_tables,
                            spacing_tables,
                        )
                        _write_score_batch(
                            writer,
                            profile,
                            condition.name,
                            strategy,
                            offset,
                            present,
                            primitive,
                        )
                        scores = candidate_scores(primitive, candidates)
                        for candidate_index, candidate in enumerate(candidates):
                            threshold = calibrated[candidate.name]["threshold"]
                            random_compatible = scores[:, candidate_index] >= threshold
                            key = (profile, candidate.name, condition.name, strategy)
                            aggregate = counts.setdefault(key, [0, 0])
                            aggregate[0] += int(random_compatible.sum())
                            aggregate[1] += size

                            component = _component_scores(primitive, candidate)
                            component_names = tuple(sorted(component))
                            mask = np.zeros(size, dtype=np.uint8)
                            for bit, name in enumerate(component_names):
                                mask |= ((component[name] < threshold).astype(np.uint8) << bit)
                            unique, frequencies = np.unique(mask, return_counts=True)
                            for trigger_mask, frequency in zip(unique, frequencies, strict=True):
                                label = "+".join(
                                    name
                                    for bit, name in enumerate(component_names)
                                    if int(trigger_mask) & (1 << bit)
                                ) or "none"
                                catch_key = (
                                    profile,
                                    candidate.name,
                                    condition.name,
                                    strategy,
                                    str(label),
                                )
                                catches[catch_key] = catches.get(catch_key, 0) + int(frequency)

    detail_rows = []
    for (profile, candidate, condition, strategy), (accepted, total) in sorted(counts.items()):
        errors = total - accepted if strategy == "RANDOM" else accepted
        low, high = _wilson_interval(errors, total)
        detail_rows.append(
            {
                "profile": profile,
                "candidate": candidate,
                "condition": condition,
                "generator_strategy": strategy,
                "sample_count": total,
                "random_compatible_count": accepted,
                "error_type": "false_rejection" if strategy == "RANDOM" else "false_random",
                "error_count": errors,
                "error_rate": errors / total,
                "error_rate_ci95_low": low,
                "error_rate_ci95_high": high,
            }
        )
    catch_rows = [
        {
            "profile": profile,
            "candidate": candidate,
            "condition": condition,
            "generator_strategy": strategy,
            "triggered_components": label,
            "sample_count": count,
        }
        for (profile, candidate, condition, strategy, label), count in sorted(catches.items())
    ]
    return detail_rows, catch_rows


def _benchmark(
    seed: int,
    candidates: tuple[Candidate, ...],
    increment_tables: IncrementBinNullTables,
    spacing_tables: EmpiricalNullTables,
    sample_count: int = 2_000,
) -> dict[str, float]:
    ideal = _stable_rng(seed, 301).integers(
        0,
        MODULUS,
        size=(sample_count, FIXED_CONFIG.sequence_length),
        dtype=np.uint16,
    )
    present = np.ones_like(ideal, dtype=bool)
    timings = {}
    for candidate in candidates:
        rule = RULE_BY_NAME[candidate.rule_name]

        def calculate(
            rule: IncrementBinRule = rule,
            candidate: Candidate = candidate,
        ) -> np.ndarray:
            components = []
            if candidate.include_increment:
                increment_views = increment_view_pvalues_for_rule(
                    ideal, present, rule, increment_tables
                )
                increment_valid = _increment_valid_views(present, rule)
                increment = (
                    increment_views.min(axis=1)
                    if candidate.increment_mode == "minimum"
                    else hierarchical_score(increment_views, increment_valid)
                )
                components.append(increment)
            if candidate.include_gap and candidate.gap_mode == "full":
                full_values, full_present = _view_arrays(ideal, present)[0]
                components.append(_gap_view_pvalues(full_values, full_present, spacing_tables))
            elif candidate.include_gap:
                gap_views = gap_view_pvalues(ideal, present, spacing_tables)
                components.append(hierarchical_score(gap_views, _gap_valid_views(present)))
            if candidate.include_raw:
                components.append(random_structure_features(ideal, present).uniformity_pvalue)
            return np.minimum.reduce(components)

        calculate()
        started = time.perf_counter()
        calculate()
        timings[candidate.name] = (time.perf_counter() - started) * 1000.0 * 10_000 / sample_count
    return timings


def _summaries(
    detail_rows: list[dict],
    calibrated: dict[str, dict],
    runtimes: dict[str, float],
    candidates: tuple[Candidate, ...],
) -> list[dict]:
    rows = []
    for profile in PROFILE_NAMES:
        for candidate in candidates:
            selected = [
                row
                for row in detail_rows
                if row["profile"] == profile and row["candidate"] == candidate.name
            ]
            random_rows = [row for row in selected if row["generator_strategy"] == "RANDOM"]
            structured = [row for row in selected if row["generator_strategy"] != "RANDOM"]
            structured_rates = [row["error_rate"] for row in structured]
            worst = max(structured, key=lambda row: row["error_rate"])
            per_bucket_lr = next(
                (
                    row["error_rate"]
                    for row in structured
                    if row["condition"] == "lossy-reordered"
                    and row["generator_strategy"] == "PER_BUCKET"
                ),
                float("nan"),
            )
            rows.append(
                {
                    "profile": profile,
                    "candidate": candidate.name,
                    "bin_rule": candidate.rule_name,
                    "increment_mode": candidate.increment_mode,
                    "gap_mode": candidate.gap_mode,
                    "include_raw": candidate.include_raw,
                    "include_increment": candidate.include_increment,
                    "include_gap": candidate.include_gap,
                    "threshold": calibrated[candidate.name]["threshold"],
                    "random_false_rejection_rate": sum(r["error_count"] for r in random_rows)
                    / sum(r["sample_count"] for r in random_rows),
                    "structured_false_random_rate": sum(r["error_count"] for r in structured)
                    / sum(r["sample_count"] for r in structured),
                    "structured_p95_false_random_rate": float(np.quantile(structured_rates, 0.95)),
                    "structured_worst_false_random_rate": worst["error_rate"],
                    "structured_worst_scenario": (
                        f"{worst['condition']}:{worst['generator_strategy']}"
                    ),
                    "per_bucket_lossy_reordered_false_random_rate": per_bucket_lr,
                    "runtime_ms_per_10k": runtimes[candidate.name],
                }
            )
    return rows


def _pareto(summary_rows: list[dict]) -> list[dict]:
    heldout = [row for row in summary_rows if row["profile"] == "heldout"]
    frontier = []
    for row in heldout:
        dominated = any(
            other["structured_false_random_rate"] <= row["structured_false_random_rate"]
            and other["structured_p95_false_random_rate"]
            <= row["structured_p95_false_random_rate"]
            and other["structured_worst_false_random_rate"]
            <= row["structured_worst_false_random_rate"]
            and other["runtime_ms_per_10k"] <= row["runtime_ms_per_10k"]
            and any(
                other[key] < row[key]
                for key in (
                    "structured_false_random_rate",
                    "structured_p95_false_random_rate",
                    "structured_worst_false_random_rate",
                    "runtime_ms_per_10k",
                )
            )
            for other in heldout
        )
        if not dominated:
            frontier.append(row)
    return sorted(frontier, key=lambda row: row["structured_false_random_rate"])


def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _plot_accuracy(summary_rows: list[dict], path: Path) -> None:
    _configure_evaluation_style()
    heldout = sorted(
        (row for row in summary_rows if row["profile"] == "heldout"),
        key=lambda row: row["structured_false_random_rate"],
    )[:16]
    fig, axis = plt.subplots(figsize=(8.4, 4.8))
    y = np.arange(len(heldout))
    axis.barh(y, [100 * row["structured_false_random_rate"] for row in heldout], color="#4c78a8")
    axis.set_yticks(y, [row["candidate"] for row in heldout])
    axis.invert_yaxis()
    axis.set_xlabel("Structured sequences classified Random [%]")
    axis.grid(axis="x", color="0.88", linewidth=0.6)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def _plot_per_bucket(detail_rows: list[dict], summary_rows: list[dict], path: Path) -> None:
    _configure_evaluation_style()
    top = [
        row["candidate"]
        for row in sorted(
            (row for row in summary_rows if row["profile"] == "heldout"),
            key=lambda row: row["structured_false_random_rate"],
        )[:12]
    ]
    selected = {
        row["candidate"]: row["error_rate"]
        for row in detail_rows
        if row["profile"] == "paper"
        and row["condition"] == "lossy-reordered"
        and row["generator_strategy"] == "PER_BUCKET"
        and row["candidate"] in top
    }
    fig, axis = plt.subplots(figsize=(8.4, 4.2))
    names = [name for name in top if name in selected]
    axis.barh(np.arange(len(names)), [100 * selected[name] for name in names], color="#72b7b2")
    axis.set_yticks(np.arange(len(names)), names)
    axis.invert_yaxis()
    axis.set_xlabel("Per-Bucket classified Random under Lossy + Reordered [%]")
    axis.grid(axis="x", color="0.88", linewidth=0.6)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def _recommendations(summary_rows: list[dict], path: Path, preset_name: str) -> list[str]:
    heldout = sorted(
        (row for row in summary_rows if row["profile"] == "heldout"),
        key=lambda row: (
            row["structured_false_random_rate"],
            row["structured_p95_false_random_rate"],
            row["structured_worst_false_random_rate"],
            row["runtime_ms_per_10k"],
        ),
    )
    shortlist = [row["candidate"] for row in heldout[:6]]
    baseline = next(row for row in heldout if row["candidate"] == BASELINE_NAME)
    lines = [
        "RANDOM classifier view-evidence evaluation",
        f"Preset: {preset_name}",
        "",
        "This is an offline comparison; production classification is unchanged.",
        (
            "Accuracy is ranked before runtime. Screening selects candidates; "
            "it does not provide final estimates."
        ),
        "Global reordering is used for both paper and held-out profiles.",
        (
            "Screening tail observations per condition: "
            f"{PRESETS[preset_name].calibration_samples * TARGET_RANDOM_FALSE_REJECTION_RATE:.1f}."
        ),
        "",
        "Reference baseline:",
        (
            f"  {BASELINE_NAME}: false-Random={baseline['structured_false_random_rate']:.6%}, "
            f"p95={baseline['structured_p95_false_random_rate']:.6%}, "
            f"worst={baseline['structured_worst_false_random_rate']:.6%}"
        ),
        "",
        "Accuracy-first shortlist:",
    ]
    for rank, row in enumerate(heldout[:10], 1):
        lines.append(
            f"  {rank}. {row['candidate']} "
            f"| false-Random={row['structured_false_random_rate']:.6%} "
            f"| p95={row['structured_p95_false_random_rate']:.6%} "
            f"| worst={row['structured_worst_false_random_rate']:.6%} "
            f"| Per-Bucket LR={row['per_bucket_lossy_reordered_false_random_rate']:.6%} "
            f"| {row['runtime_ms_per_10k']:.2f} ms/10k"
        )
    lines.extend(
        [
            "",
            "Confirmation command candidate list:",
            "  " + ",".join(shortlist),
            "",
            "Interpretation guardrails:",
            "  - Compare confidence intervals and per-scenario rows, not only aggregate rank.",
            "  - Fisher aggregation is used only within disjoint destination/connection groups.",
            (
                "  - The overlapping full/destination/connection family minimum is calibrated "
                "as part of the final score."
            ),
            "  - No synthetic evaluation substitutes for a real-world ground-truth dataset.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return shortlist


def _review_bundle(output_dir: Path, figure_dir: Path) -> Path:
    path = output_dir / "random-classifier-view-review-bundle.zip"
    names = (
        "summary.json",
        "variant-results.csv",
        "variant-by-scenario.csv",
        "catch-attribution.csv",
        "pareto-frontier.csv",
        "recommendations.txt",
        "run.log",
    )
    figures = ("variant-accuracy-summary.pdf", "per-bucket-lossy-reordered.pdf")
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in names:
            candidate = output_dir / name
            if candidate.exists():
                archive.write(candidate, name)
        for name in figures:
            candidate = figure_dir / name
            if candidate.exists():
                archive.write(candidate, name)
    return path


@app.command()
def main(
    preset: str = typer.Option("screening", help="screening or confirmation"),
    variants: str = typer.Option("", help="Optional comma-separated candidate names"),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR),  # noqa: B008
    figure_dir: Path = typer.Option(DEFAULT_FIGURE_DIR),  # noqa: B008
    seed: int = typer.Option(20260927),
) -> None:
    """Run the staged, production-independent view-evidence experiment."""
    if preset not in PRESETS:
        raise typer.BadParameter(f"unknown preset {preset!r}; choose from {tuple(PRESETS)}")
    selected = ALL_CANDIDATES
    if variants.strip():
        requested = {name.strip() for name in variants.split(",") if name.strip()}
        requested.add(BASELINE_NAME)
        selected = tuple(candidate for candidate in ALL_CANDIDATES if candidate.name in requested)
        missing = requested - {candidate.name for candidate in selected}
        if missing:
            raise typer.BadParameter(f"unknown candidate(s): {sorted(missing)}")
    if not selected:
        raise typer.BadParameter("at least one candidate is required")

    config = PRESETS[preset]
    output_dir = output_dir / preset
    figure_dir = figure_dir / preset
    output_dir.mkdir(parents=True, exist_ok=True)
    figure_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "run.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, mode="w", encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )
    LOGGER.info("Starting view-evidence evaluation preset=%s candidates=%d", preset, len(selected))
    expected_tail = config.calibration_samples * TARGET_RANDOM_FALSE_REJECTION_RATE
    if expected_tail < 20:
        LOGGER.warning(
            "Exploratory screening: only %.1f expected calibration observations per condition "
            "lie in the target lower tail; use confirmation for final threshold estimates",
            expected_tail,
        )
    increment_tables = IncrementBinNullTables(config.null_samples, seed + 11)
    spacing_tables = EmpiricalNullTables(config.null_samples, seed + 23)
    calibrated = _calibrate(
        config.calibration_samples,
        config.batch_size,
        seed,
        selected,
        increment_tables,
        spacing_tables,
    )
    detail_rows, catch_rows = _evaluate(
        config,
        seed,
        selected,
        calibrated,
        increment_tables,
        spacing_tables,
        output_dir / "view-scores.pq",
    )
    runtimes = _benchmark(seed, selected, increment_tables, spacing_tables)
    summary_rows = _summaries(detail_rows, calibrated, runtimes, selected)
    frontier = _pareto(summary_rows)

    _write_csv(output_dir / "variant-results.csv", summary_rows)
    _write_csv(output_dir / "variant-by-scenario.csv", detail_rows)
    _write_csv(output_dir / "catch-attribution.csv", catch_rows)
    _write_csv(output_dir / "pareto-frontier.csv", frontier)
    _plot_accuracy(summary_rows, figure_dir / "variant-accuracy-summary.pdf")
    _plot_per_bucket(
        detail_rows,
        summary_rows,
        figure_dir / "per-bucket-lossy-reordered.pdf",
    )
    shortlist = _recommendations(summary_rows, output_dir / "recommendations.txt", preset)
    metadata = {
        "experiment_version": EXPERIMENT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "preset": preset,
        "configuration": config.__dict__,
        "target_random_false_rejection_rate": TARGET_RANDOM_FALSE_REJECTION_RATE,
        "expected_calibration_tail_observations_per_condition": expected_tail,
        "exploratory_tail_calibration": expected_tail < 20,
        "conditions": [condition.__dict__ for condition in CORE_CONDITIONS],
        "bin_rules": [rule.__dict__ for rule in BIN_RULES],
        "candidates": [candidate.__dict__ for candidate in selected],
        "calibration": calibrated,
        "accuracy_first_shortlist": shortlist,
        "production_classifier_changed": False,
        "score_parquet": str(output_dir / "view-scores.pq"),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    bundle = _review_bundle(output_dir, figure_dir)
    LOGGER.info("Review bundle: %s", bundle)
    for key, value in {
        "summary": output_dir / "summary.json",
        "results": output_dir / "variant-results.csv",
        "scenarios": output_dir / "variant-by-scenario.csv",
        "catch_attribution": output_dir / "catch-attribution.csv",
        "view_scores": output_dir / "view-scores.pq",
        "recommendations": output_dir / "recommendations.txt",
        "review_bundle": bundle,
    }.items():
        typer.echo(f"{key}: {value}")


if __name__ == "__main__":
    app()
