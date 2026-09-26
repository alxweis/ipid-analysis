"""Offline ablation of increment-uniformity bin-selection rules.

The experiment keeps the selected validation score structure

    min(raw uniformity, increment uniformity, gap uniformity)

fixed and changes only the increment-uniformity bin rule.  Every complete
candidate is calibrated on independent true-RANDOM sequences before it is
compared on paper-compatible and threshold-independent counter generators.
Nothing in this module changes the production classifier.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import math
from pathlib import Path
import time

import matplotlib
import numpy as np
import typer

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from ipid_analysis.classifier_validation import FIXED_CONFIG, generate_fixed_sequences
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.random_classifier_evaluation import (
    EmpiricalNullTables,
    _configure_evaluation_style,
    _create_review_bundle,
    _right_tail_pvalues,
    _stable_rng,
    _threshold_at_false_rejection,
    _wilson_interval,
    gap_uniformity_pvalues,
)
from ipid_analysis.random_classifier_evaluation_v2 import (
    CONDITIONS,
    GENERATOR_NAMES,
    STEP_BIN_EDGES,
    GeneratedBatch,
    apply_v2_impairment,
    generate_v2_sequences,
)
from ipid_analysis.strategies import MODULUS, random_structure_features

app = typer.Typer(add_completion=False)
LOGGER = logging.getLogger(__name__)

EXPERIMENT_VERSION = "1"
DEFAULT_PAPER_SAMPLES_PER_STRATEGY = 100_000
DEFAULT_SELECTION_SAMPLES_PER_STRATEGY = 50_000
DEFAULT_TEST_SAMPLES_PER_STRATEGY = 100_000
DEFAULT_CALIBRATION_SAMPLES_PER_CONDITION = 250_000
DEFAULT_NULL_TABLE_SAMPLES = 500_000
DEFAULT_TARGET_RANDOM_FALSE_REJECTION_RATE = 0.0001
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_OUTPUT_DIR = (
    PROCESSED_DATA_DIR / "classifier-validation" / "increment-bin-rule-evaluation"
)
DEFAULT_FIGURE_DIR = FIGURES_DIR / "classifier-validation" / "increment-bin-rule-evaluation"
MIN_INCREMENT_TRANSITIONS = 10
MAX_POWER_OF_TWO_BINS = 16
MAX_THIRDS_BINS = 12
MULTISCALE_BINS = (3, 4, 8, 16)


@dataclass(frozen=True)
class IncrementBinRule:
    name: str
    family: str
    target_expected_per_bin: int | None = None
    description: str = ""


BIN_RULES = (
    IncrementBinRule(
        "power2-e5",
        "power2",
        5,
        "current baseline: largest power of two with about five expected transitions/bin",
    ),
    IncrementBinRule(
        "power2-e3",
        "power2",
        3,
        "finer neutral rule: largest power of two with about three expected transitions/bin",
    ),
    IncrementBinRule(
        "fixed-3",
        "fixed3",
        description="fixed thirds aligned with the MAX_INC one-third support",
    ),
    IncrementBinRule(
        "thirds-e5",
        "thirds",
        5,
        "largest of 3/6/9/12 with about five expected transitions/bin",
    ),
    IncrementBinRule(
        "thirds-e3",
        "thirds",
        3,
        "largest of 3/6/9/12 with about three expected transitions/bin",
    ),
    IncrementBinRule(
        "thirds-e2",
        "thirds",
        2,
        "largest of 3/6/9/12 with about two expected transitions/bin",
    ),
    IncrementBinRule(
        "multiscale-3-4-8-16",
        "multiscale",
        2,
        "jointly calibrated minimum across usable 3/4/8/16-bin tests",
    ),
)
BIN_RULE_BY_NAME = {rule.name: rule for rule in BIN_RULES}
PROFILE_NAMES = ("paper", "selection", "heldout")


def selected_bin_counts(rule: IncrementBinRule, transition_count: int) -> tuple[int, ...]:
    """Return the test resolutions used by ``rule`` for one increment view."""
    if transition_count < MIN_INCREMENT_TRANSITIONS:
        return ()
    if rule.family == "fixed3":
        return (3,)
    if rule.family == "power2":
        maximum = min(
            MAX_POWER_OF_TWO_BINS,
            transition_count // int(rule.target_expected_per_bin),
        )
        if maximum < 2:
            return ()
        return (1 << math.floor(math.log2(maximum)),)
    if rule.family == "thirds":
        maximum = min(
            MAX_THIRDS_BINS,
            transition_count // int(rule.target_expected_per_bin),
        )
        candidates = tuple(count for count in (3, 6, 9, 12) if count <= maximum)
        return candidates[-1:] if candidates else ()
    if rule.family == "multiscale":
        maximum = transition_count // int(rule.target_expected_per_bin)
        return tuple(count for count in MULTISCALE_BINS if count <= maximum)
    raise ValueError(f"unknown bin-rule family: {rule.family}")


def _discrete_bin_probabilities(bin_count: int) -> np.ndarray:
    """Exact bin probabilities for floor(delta*k/65536), including thirds."""
    boundaries = (
        np.arange(bin_count + 1, dtype=np.int64) * MODULUS + bin_count - 1
    ) // bin_count
    return np.diff(boundaries).astype(float) / MODULUS


def _pearson_statistics(counts: np.ndarray, probabilities: np.ndarray) -> np.ndarray:
    if np.all(probabilities == probabilities[0]):
        # Preserve the established power-of-two baseline bit-for-bit.  This
        # also avoids empirical-rank changes caused only by floating rounding
        # at tied Pearson statistics.
        expected = counts.sum(axis=1) / len(probabilities)
        return np.square(counts - expected[:, None]).sum(axis=1) / expected
    expected = counts.sum(axis=1, keepdims=True) * probabilities[None, :]
    return (np.square(counts - expected) / expected).sum(axis=1)


def _left_tail_pvalues(observed: np.ndarray, sorted_null: np.ndarray) -> np.ndarray:
    """Conservative empirical P(T <= observed), with add-one correction."""
    right = np.searchsorted(sorted_null, observed, side="right")
    return (right + 1.0) / (len(sorted_null) + 1.0)


class IncrementBinNullTables:
    """Null tables for arbitrary discrete bins and a joint multiscale test."""

    def __init__(self, sample_count: int, seed: int, batch_size: int = 20_000):
        if sample_count < 1:
            raise ValueError("null-table sample count must be positive")
        self.sample_count = sample_count
        self.seed = seed
        self.batch_size = batch_size
        self._single: dict[tuple[int, int], np.ndarray] = {}
        self._joint: dict[tuple[int, tuple[int, ...]], np.ndarray] = {}

    def single(self, transition_count: int, bin_count: int) -> np.ndarray:
        key = (transition_count, bin_count)
        if key not in self._single:
            probabilities = _discrete_bin_probabilities(bin_count)
            LOGGER.info(
                "Generating increment-bin null table m=%d, bins=%d (%d samples)",
                transition_count,
                bin_count,
                self.sample_count,
            )
            counts = _stable_rng(self.seed, 11, transition_count, bin_count).multinomial(
                transition_count,
                probabilities,
                size=self.sample_count,
            )
            statistics = _pearson_statistics(counts, probabilities)
            statistics.sort()
            self._single[key] = statistics
        return self._single[key]

    def joint(self, transition_count: int, bin_counts: tuple[int, ...]) -> np.ndarray:
        """Null distribution of the minimum component p-value across scales."""
        key = (transition_count, bin_counts)
        if key not in self._joint:
            LOGGER.info(
                "Generating joint increment-bin null table m=%d, bins=%s (%d samples)",
                transition_count,
                "/".join(map(str, bin_counts)),
                self.sample_count,
            )
            minimum_pvalues = np.ones(self.sample_count, dtype=float)
            offset = 0
            batch_index = 0
            common_bin_count = math.lcm(*bin_counts)
            common_probabilities = _discrete_bin_probabilities(common_bin_count)
            while offset < self.sample_count:
                size = min(self.batch_size, self.sample_count - offset)
                common_counts = _stable_rng(
                    self.seed,
                    29,
                    transition_count,
                    sum(bin_counts),
                    batch_index,
                ).multinomial(
                    transition_count,
                    common_probabilities,
                    size=size,
                )
                batch_minimum = np.ones(size, dtype=float)
                for bin_count in bin_counts:
                    counts = common_counts.reshape(
                        size,
                        bin_count,
                        common_bin_count // bin_count,
                    ).sum(axis=2)
                    statistics = _pearson_statistics(
                        counts,
                        _discrete_bin_probabilities(bin_count),
                    )
                    component = _right_tail_pvalues(
                        statistics,
                        self.single(transition_count, bin_count),
                    )
                    batch_minimum = np.minimum(batch_minimum, component)
                minimum_pvalues[offset : offset + size] = batch_minimum
                offset += size
                batch_index += 1
            minimum_pvalues.sort()
            self._joint[key] = minimum_pvalues
        return self._joint[key]


def _view_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    rule: IncrementBinRule,
    null_tables: IncrementBinNullTables,
) -> np.ndarray:
    if values.shape[1] < 2:
        return np.ones(len(values), dtype=float)
    pair_present = present[:, :-1] & present[:, 1:]
    transition_counts = pair_present.sum(axis=1).astype(np.int64)
    increments = (values[:, 1:].astype(np.int64) - values[:, :-1].astype(np.int64)) % MODULUS
    result = np.ones(len(values), dtype=float)

    for transition_count in np.unique(transition_counts):
        bin_counts = selected_bin_counts(rule, int(transition_count))
        if not bin_counts:
            continue
        rows = np.flatnonzero(transition_counts == transition_count)
        active = pair_present[rows]
        component_pvalues = []
        for bin_count in bin_counts:
            bins = (increments[rows] * bin_count) // MODULUS
            row_ids = np.broadcast_to(np.arange(len(rows))[:, None], bins.shape)
            flat = (row_ids * bin_count + bins)[active]
            counts = np.bincount(
                flat,
                minlength=len(rows) * bin_count,
            ).reshape(len(rows), bin_count)
            statistics = _pearson_statistics(
                counts,
                _discrete_bin_probabilities(bin_count),
            )
            component_pvalues.append(
                _right_tail_pvalues(
                    statistics,
                    null_tables.single(int(transition_count), bin_count),
                )
            )
        minimum = np.minimum.reduce(component_pvalues)
        if len(bin_counts) > 1:
            result[rows] = _left_tail_pvalues(
                minimum,
                null_tables.joint(int(transition_count), bin_counts),
            )
        else:
            result[rows] = minimum
    return result


def increment_uniformity_pvalues_for_rule(
    values: np.ndarray,
    present: np.ndarray,
    rule: IncrementBinRule,
    null_tables: IncrementBinNullTables,
) -> np.ndarray:
    """Minimum calibrated view p-value for one candidate bin rule."""
    if values.shape[1] != FIXED_CONFIG.sequence_length:
        raise ValueError(
            f"expected {FIXED_CONFIG.sequence_length} fixed positions, got {values.shape[1]}"
        )
    components = [_view_pvalues(values, present, rule, null_tables)]
    components.extend(
        _view_pvalues(values[:, offset::2], present[:, offset::2], rule, null_tables)
        for offset in range(2)
    )
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
    components.extend(
        _view_pvalues(
            connection_values[:, connection],
            connection_present[:, connection],
            rule,
            null_tables,
        )
        for connection in range(FIXED_CONFIG.connection_count)
    )
    return np.minimum.reduce(components)


def candidate_scores_by_bin_rule(
    values: np.ndarray,
    present: np.ndarray,
    increment_tables: IncrementBinNullTables,
    spacing_tables: EmpiricalNullTables,
) -> np.ndarray:
    """Return Raw+Increment+Gap minimum scores for all bin-rule candidates."""
    raw = random_structure_features(values, present).uniformity_pvalue
    gap = gap_uniformity_pvalues(values, present, spacing_tables)
    scores = []
    for rule in BIN_RULES:
        increment = increment_uniformity_pvalues_for_rule(
            values,
            present,
            rule,
            increment_tables,
        )
        scores.append(np.minimum.reduce([raw, increment, gap]))
    return np.clip(np.column_stack(scores), 0.0, 1.0)


def _calibrate(
    samples_per_condition: int,
    batch_size: int,
    target: float,
    seed: int,
    increment_tables: IncrementBinNullTables,
    spacing_tables: EmpiricalNullTables,
) -> dict[str, dict]:
    score_batches: list[list[np.ndarray]] = [[] for _ in CONDITIONS]
    for condition_index, condition in enumerate(CONDITIONS):
        LOGGER.info("Calibrating bin rules for %s", condition.name)
        for offset in range(0, samples_per_condition, batch_size):
            size = min(batch_size, samples_per_condition - offset)
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
            score_batches[condition_index].append(
                candidate_scores_by_bin_rule(
                    values,
                    present,
                    increment_tables,
                    spacing_tables,
                ).astype(np.float32)
            )
    by_condition = [np.concatenate(batches) for batches in score_batches]
    calibrated = {}
    for rule_index, rule in enumerate(BIN_RULES):
        condition_thresholds = {
            condition.name: _threshold_at_false_rejection(
                by_condition[index][:, rule_index],
                target,
            )
            for index, condition in enumerate(CONDITIONS)
        }
        threshold = min(condition_thresholds.values())
        calibrated[rule.name] = {
            "threshold": threshold,
            "threshold_by_condition": condition_thresholds,
            "false_rejection_by_condition": {
                condition.name: float(
                    (by_condition[index][:, rule_index] < threshold).mean()
                )
                for index, condition in enumerate(CONDITIONS)
            },
        }
    return calibrated


def _paper_batches(sample_count: int, rng: np.random.Generator) -> dict[str, GeneratedBatch]:
    generated = generate_fixed_sequences(sample_count, rng)
    return {
        name: GeneratedBatch(
            values,
            np.full(sample_count, -1, dtype=np.int32),
            np.zeros(sample_count, dtype=np.int16),
        )
        for name, values in generated.items()
    }


def _evaluate_profile(
    profile: str,
    samples_per_strategy: int,
    batch_size: int,
    seed: int,
    calibrated: dict[str, dict],
    increment_tables: IncrementBinNullTables,
    spacing_tables: EmpiricalNullTables,
) -> tuple[dict, dict]:
    counts: dict[tuple[str, str, str], list[int]] = {}
    step_counts: dict[tuple[str, str, int], list[int]] = {}
    for offset in range(0, samples_per_strategy, batch_size):
        size = min(batch_size, samples_per_strategy - offset)
        LOGGER.info(
            "Evaluating %s bin rules (%d..%d per strategy)",
            profile,
            offset,
            offset + size - 1,
        )
        rng = _stable_rng(seed, 201, PROFILE_NAMES.index(profile), offset)
        generated = (
            _paper_batches(size, rng)
            if profile == "paper"
            else generate_v2_sequences(size, rng, profile)
        )
        for strategy_index, (strategy, batch) in enumerate(generated.items()):
            for condition_index, condition in enumerate(CONDITIONS):
                values, present, _ = apply_v2_impairment(
                    batch.values,
                    condition,
                    _stable_rng(
                        seed,
                        202,
                        PROFILE_NAMES.index(profile),
                        offset,
                        strategy_index,
                        condition_index,
                    ),
                )
                scores = candidate_scores_by_bin_rule(
                    values,
                    present,
                    increment_tables,
                    spacing_tables,
                )
                for rule_index, rule in enumerate(BIN_RULES):
                    accepted = scores[:, rule_index] >= calibrated[rule.name]["threshold"]
                    aggregate = counts.setdefault((rule.name, condition.name, strategy), [0, 0])
                    aggregate[0] += int(accepted.sum())
                    aggregate[1] += size
                    valid_steps = batch.base_step >= 0
                    if strategy != "RANDOM" and np.any(valid_steps):
                        step_bins = np.digitize(
                            batch.base_step,
                            STEP_BIN_EDGES[1:-1],
                            right=False,
                        )
                        for step_bin in np.unique(step_bins[valid_steps]):
                            selected = valid_steps & (step_bins == step_bin)
                            parameter = step_counts.setdefault(
                                (rule.name, strategy, int(step_bin)),
                                [0, 0],
                            )
                            parameter[0] += int(accepted[selected].sum())
                            parameter[1] += int(selected.sum())
    return counts, step_counts


def _detail_rows(profile: str, counts: dict) -> list[dict]:
    rows = []
    for (rule, condition, strategy), (accepted, total) in sorted(counts.items()):
        errors = total - accepted if strategy == "RANDOM" else accepted
        low, high = _wilson_interval(errors, total)
        rows.append(
            {
                "profile": profile,
                "bin_rule": rule,
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
    return rows


def _summary_rows(
    details: list[dict],
    calibrated: dict[str, dict],
    runtimes: dict[str, float],
    target: float,
) -> list[dict]:
    summaries = []
    for profile in PROFILE_NAMES:
        for rule in BIN_RULES:
            selected = [
                row for row in details if row["profile"] == profile and row["bin_rule"] == rule.name
            ]
            random_rows = [row for row in selected if row["generator_strategy"] == "RANDOM"]
            structured_rows = [row for row in selected if row["generator_strategy"] != "RANDOM"]
            random_errors = sum(row["error_count"] for row in random_rows)
            random_total = sum(row["sample_count"] for row in random_rows)
            structured_errors = sum(row["error_count"] for row in structured_rows)
            structured_total = sum(row["sample_count"] for row in structured_rows)
            rates = [row["error_rate"] for row in structured_rows]
            worst = max(structured_rows, key=lambda row: row["error_rate"])
            worst_random = max(random_rows, key=lambda row: row["error_rate"])
            random_low, random_high = _wilson_interval(random_errors, random_total)
            worst_random_low, worst_random_high = _wilson_interval(
                worst_random["error_count"],
                worst_random["sample_count"],
            )
            summaries.append(
                {
                    "profile": profile,
                    "bin_rule": rule.name,
                    "threshold": calibrated[rule.name]["threshold"],
                    "target_random_false_rejection_rate": target,
                    "random_false_rejection_rate": random_errors / random_total,
                    "random_false_rejection_rate_ci95_low": random_low,
                    "random_false_rejection_rate_ci95_high": random_high,
                    "worst_condition_random_false_rejection_rate": worst_random["error_rate"],
                    "worst_condition_random_false_rejection_rate_ci95_low": worst_random_low,
                    "worst_condition_random_false_rejection_rate_ci95_high": worst_random_high,
                    "worst_random_condition": worst_random["condition"],
                    "no_significant_random_frr_exceedance": worst_random_low <= target,
                    "structured_false_random_rate": structured_errors / structured_total,
                    "p95_scenario_false_random_rate": float(np.quantile(rates, 0.95)),
                    "worst_scenario_false_random_rate": worst["error_rate"],
                    "worst_scenario": f"{worst['condition']}/{worst['generator_strategy']}",
                    "runtime_ms_per_10000": runtimes[rule.name],
                }
            )
    return summaries


def _step_rows(profile: str, step_counts: dict) -> list[dict]:
    rows = []
    for (rule, strategy, step_bin), (accepted, total) in sorted(step_counts.items()):
        rows.append(
            {
                "profile": profile,
                "bin_rule": rule,
                "generator_strategy": strategy,
                "step_bin_index": step_bin,
                "step_lower_inclusive": int(STEP_BIN_EDGES[step_bin]),
                "step_upper_exclusive": int(STEP_BIN_EDGES[step_bin + 1]),
                "sample_count": total,
                "false_random_count": accepted,
                "false_random_rate": accepted / total,
            }
        )
    return rows


def _benchmark(
    sample_count: int,
    seed: int,
    increment_tables: IncrementBinNullTables,
    spacing_tables: EmpiricalNullTables,
) -> dict[str, float]:
    values = _stable_rng(seed, 301).integers(
        0,
        MODULUS,
        size=(sample_count, FIXED_CONFIG.sequence_length),
        dtype=np.uint16,
    )
    present = np.ones_like(values, dtype=bool)
    timings = {}
    for rule in BIN_RULES:
        increment_uniformity_pvalues_for_rule(values, present, rule, increment_tables)
        gap_uniformity_pvalues(values, present, spacing_tables)
        observations = []
        for _ in range(3):
            started = time.perf_counter()
            raw = random_structure_features(values, present).uniformity_pvalue
            gap = gap_uniformity_pvalues(values, present, spacing_tables)
            increment = increment_uniformity_pvalues_for_rule(
                values,
                present,
                rule,
                increment_tables,
            )
            np.minimum.reduce([raw, increment, gap])
            observations.append((time.perf_counter() - started) * 1000.0)
        timings[rule.name] = float(np.median(observations) * 10_000 / sample_count)
    return timings


def _write_csv(rows: list[dict], path: Path) -> Path:
    if not rows:
        raise ValueError("cannot write an empty CSV")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _plot_heatmap(details: list[dict], output_path: Path) -> Path:
    _configure_evaluation_style()
    heldout = [row for row in details if row["profile"] == "heldout"]
    strategies = [name for name in GENERATOR_NAMES if name != "RANDOM"]
    matrix = np.asarray(
        [
            [
                np.mean(
                    [
                        row["error_rate"]
                        for row in heldout
                        if row["bin_rule"] == rule.name
                        and row["generator_strategy"] == strategy
                    ]
                )
                for strategy in strategies
            ]
            for rule in BIN_RULES
        ]
    )
    figure, axis = plt.subplots(figsize=(11.0, 4.2))
    image = axis.imshow(matrix * 100.0, aspect="auto", cmap="magma_r", vmin=0.0, vmax=100.0)
    axis.set_xticks(range(len(strategies)), [name.replace("_", " ").title() for name in strategies])
    axis.set_yticks(range(len(BIN_RULES)), [rule.name for rule in BIN_RULES])
    axis.tick_params(axis="x", rotation=55)
    axis.set_xlabel("Held-out generator")
    axis.set_ylabel("Increment bin rule")
    figure.colorbar(image, ax=axis, label="False-RANDOM [%]")
    figure.tight_layout()
    figure.savefig(output_path)
    plt.close(figure)
    return output_path


def _plot_summary(summaries: list[dict], details: list[dict], output_path: Path) -> Path:
    _configure_evaluation_style()
    names = [rule.name for rule in BIN_RULES]
    heldout = {
        row["bin_rule"]: row for row in summaries if row["profile"] == "heldout"
    }
    paper = {row["bin_rule"]: row for row in summaries if row["profile"] == "paper"}
    x = np.arange(len(names))
    figure, axes = plt.subplots(1, 3, figsize=(12.0, 3.5), sharex=True)
    axes[0].bar(x, [100 * heldout[name]["structured_false_random_rate"] for name in names])
    axes[0].set_ylabel("False-RANDOM [%]")
    axes[0].set_title("Held-out aggregate")
    axes[1].bar(x, [100 * heldout[name]["worst_scenario_false_random_rate"] for name in names])
    axes[1].set_title("Held-out worst scenario")
    paper_details = [
        row
        for row in details
        if row["profile"] == "paper" and row["generator_strategy"] == "PER_BUCKET"
    ]
    axes[2].bar(
        x,
        [
            100
            * np.mean(
                [row["error_rate"] for row in paper_details if row["bin_rule"] == name]
            )
            for name in names
        ],
    )
    axes[2].set_title("Paper Per-Bucket")
    for axis in axes:
        axis.grid(axis="y", color="#d0d0d0", linewidth=0.6)
        axis.set_xticks(x, names, rotation=55, ha="right")
    # Reference the paper summary so static analysis catches profile coverage.
    assert set(paper) == set(names)
    figure.tight_layout()
    figure.savefig(output_path)
    plt.close(figure)
    return output_path


def _write_recommendations(summaries: list[dict], details: list[dict], output_path: Path) -> Path:
    heldout = [row for row in summaries if row["profile"] == "heldout"]
    ranked = sorted(
        heldout,
        key=lambda row: (
            not row["no_significant_random_frr_exceedance"],
            row["worst_scenario_false_random_rate"],
            row["p95_scenario_false_random_rate"],
            row["structured_false_random_rate"],
            row["runtime_ms_per_10000"],
        ),
    )
    lines = [
        "Increment-uniformity bin-rule ablation",
        "",
        "Accuracy-first ranking (held-out full-domain generators):",
    ]
    for index, row in enumerate(ranked, start=1):
        paper_bucket = np.mean(
            [
                detail["error_rate"]
                for detail in details
                if detail["profile"] == "paper"
                and detail["bin_rule"] == row["bin_rule"]
                and detail["generator_strategy"] == "PER_BUCKET"
            ]
        )
        lines.extend(
            [
                f"{index}. {row['bin_rule']}",
                f"   true-RANDOM FRR: {row['random_false_rejection_rate']:.6%}",
                f"   worst-condition true-RANDOM FRR: {row['worst_condition_random_false_rejection_rate']:.6%} ({row['worst_random_condition']})",
                f"   no significant target exceedance: {row['no_significant_random_frr_exceedance']}",
                f"   held-out aggregate False-RANDOM: {row['structured_false_random_rate']:.6%}",
                f"   held-out p95 scenario: {row['p95_scenario_false_random_rate']:.6%}",
                f"   held-out worst scenario: {row['worst_scenario_false_random_rate']:.6%} ({row['worst_scenario']})",
                f"   paper Per-Bucket False-RANDOM: {paper_bucket:.6%}",
                f"   runtime per 10k sequences: {row['runtime_ms_per_10000']:.3f} ms",
            ]
        )
    lines.extend(
        [
            "",
            "Decision rule:",
            "  1. verify the calibrated and held-out true-RANDOM false-rejection rates;",
            "  2. prioritize held-out worst-case, p95, and aggregate accuracy;",
            "  3. inspect paper Per-Bucket and step-sensitivity results for MAX_INC overfitting;",
            "  4. use runtime only to break materially similar accuracy results;",
            "  5. do not change production before reviewing the complete scenario table.",
            "",
        ]
    )
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def evaluate_increment_bin_rules(
    *,
    paper_samples_per_strategy: int = DEFAULT_PAPER_SAMPLES_PER_STRATEGY,
    selection_samples_per_strategy: int = DEFAULT_SELECTION_SAMPLES_PER_STRATEGY,
    test_samples_per_strategy: int = DEFAULT_TEST_SAMPLES_PER_STRATEGY,
    calibration_samples_per_condition: int = DEFAULT_CALIBRATION_SAMPLES_PER_CONDITION,
    null_table_samples: int = DEFAULT_NULL_TABLE_SAMPLES,
    target_random_frr: float = DEFAULT_TARGET_RANDOM_FALSE_REJECTION_RATE,
    batch_size: int = DEFAULT_BATCH_SIZE,
    seed: int = 42,
    output_dir: Path | None = None,
    figure_dir: Path | None = None,
    benchmark_sample_count: int = 10_000,
) -> dict[str, Path]:
    sample_counts = (
        paper_samples_per_strategy,
        selection_samples_per_strategy,
        test_samples_per_strategy,
        calibration_samples_per_condition,
        null_table_samples,
        benchmark_sample_count,
    )
    if any(value < 1 for value in sample_counts):
        raise ValueError("sample counts must be positive")
    if not 0.0 <= target_random_frr < 1.0:
        raise ValueError("target RANDOM false-rejection rate must be in [0, 1)")
    output_dir = output_dir or DEFAULT_OUTPUT_DIR
    figure_dir = figure_dir or DEFAULT_FIGURE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    figure_dir.mkdir(parents=True, exist_ok=True)

    started = time.perf_counter()
    increment_tables = IncrementBinNullTables(null_table_samples, seed + 1)
    spacing_tables = EmpiricalNullTables(null_table_samples, seed + 2)
    calibrated = _calibrate(
        calibration_samples_per_condition,
        batch_size,
        target_random_frr,
        seed + 3,
        increment_tables,
        spacing_tables,
    )
    profile_sizes = {
        "paper": paper_samples_per_strategy,
        "selection": selection_samples_per_strategy,
        "heldout": test_samples_per_strategy,
    }
    all_details = []
    all_steps = []
    for profile_index, profile in enumerate(PROFILE_NAMES):
        counts, step_counts = _evaluate_profile(
            profile,
            profile_sizes[profile],
            batch_size,
            seed + 4 + profile_index,
            calibrated,
            increment_tables,
            spacing_tables,
        )
        all_details.extend(_detail_rows(profile, counts))
        all_steps.extend(_step_rows(profile, step_counts))
    runtimes = _benchmark(
        benchmark_sample_count,
        seed + 9,
        increment_tables,
        spacing_tables,
    )
    summaries = _summary_rows(all_details, calibrated, runtimes, target_random_frr)

    result_path = _write_csv(summaries, output_dir / "bin-rule-results.csv")
    detail_path = _write_csv(all_details, output_dir / "bin-rule-by-scenario.csv")
    step_path = _write_csv(all_steps, output_dir / "bin-rule-step-sensitivity.csv")
    recommendation_path = _write_recommendations(
        summaries,
        all_details,
        output_dir / "recommendations.txt",
    )
    heatmap_path = _plot_heatmap(
        all_details,
        figure_dir / "bin-rule-false-random-heatmap.pdf",
    )
    summary_plot_path = _plot_summary(
        summaries,
        all_details,
        figure_dir / "bin-rule-accuracy-summary.pdf",
    )
    elapsed = time.perf_counter() - started
    quality_warnings = []
    if 1.0 / (null_table_samples + 1.0) > target_random_frr:
        quality_warnings.append(
            "null-table p-value resolution is coarser than the target RANDOM false-rejection rate"
        )
    if calibration_samples_per_condition * target_random_frr < 20:
        quality_warnings.append(
            "fewer than 20 expected calibration observations per condition lie in the target lower tail"
        )
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "experiment_version": EXPERIMENT_VERSION,
        "production_classifier_changed": False,
        "candidate_score": "min(raw_uniformity, increment_uniformity, gap_uniformity)",
        "seed": seed,
        "samples_per_strategy": profile_sizes,
        "calibration_samples_per_condition": calibration_samples_per_condition,
        "null_table_samples": null_table_samples,
        "target_random_false_rejection_rate": target_random_frr,
        "quality_warnings": quality_warnings,
        "elapsed_seconds": elapsed,
        "minimum_increment_transitions": MIN_INCREMENT_TRANSITIONS,
        "thirds_bin_cap": MAX_THIRDS_BINS,
        "multiscale_bins": list(MULTISCALE_BINS),
        "bin_rules": [rule.__dict__ for rule in BIN_RULES],
        "exact_discrete_bin_probabilities": True,
        "multiscale_calibration": (
            "independent Monte-Carlo null distribution of the minimum single-scale empirical p-value"
        ),
        "conditions": [condition.__dict__ for condition in CONDITIONS],
        "calibration": calibrated,
        "runtime_ms_per_10000": runtimes,
        "artifacts": {
            "results": str(result_path),
            "by_scenario": str(detail_path),
            "step_sensitivity": str(step_path),
            "recommendations": str(recommendation_path),
            "heatmap": str(heatmap_path),
            "accuracy_summary": str(summary_plot_path),
        },
    }
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    bundle_path = _create_review_bundle(
        [
            summary_path,
            result_path,
            detail_path,
            step_path,
            recommendation_path,
            heatmap_path,
            summary_plot_path,
        ],
        output_dir / "increment-bin-rule-review-bundle.zip",
    )
    LOGGER.info("Increment bin-rule evaluation completed in %.1f seconds", elapsed)
    return {
        "summary": summary_path,
        "results": result_path,
        "by_scenario": detail_path,
        "step_sensitivity": step_path,
        "recommendations": recommendation_path,
        "heatmap": heatmap_path,
        "accuracy_summary": summary_plot_path,
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
    paper_samples_per_strategy: int = typer.Option(DEFAULT_PAPER_SAMPLES_PER_STRATEGY, min=1),
    selection_samples_per_strategy: int = typer.Option(
        DEFAULT_SELECTION_SAMPLES_PER_STRATEGY,
        min=1,
    ),
    test_samples_per_strategy: int = typer.Option(DEFAULT_TEST_SAMPLES_PER_STRATEGY, min=1),
    calibration_samples_per_condition: int = typer.Option(
        DEFAULT_CALIBRATION_SAMPLES_PER_CONDITION,
        min=1,
    ),
    null_table_samples: int = typer.Option(DEFAULT_NULL_TABLE_SAMPLES, min=1),
    target_random_frr: float = typer.Option(
        DEFAULT_TARGET_RANDOM_FALSE_REJECTION_RATE,
        min=0.0,
        max=0.999999,
    ),
    batch_size: int = typer.Option(DEFAULT_BATCH_SIZE, min=1),
    seed: int = typer.Option(42),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR),  # noqa: B008
    figure_dir: Path = typer.Option(DEFAULT_FIGURE_DIR),  # noqa: B008
) -> None:
    log_path = output_dir / "run.log"
    _configure_logging(log_path)
    outputs = evaluate_increment_bin_rules(
        paper_samples_per_strategy=paper_samples_per_strategy,
        selection_samples_per_strategy=selection_samples_per_strategy,
        test_samples_per_strategy=test_samples_per_strategy,
        calibration_samples_per_condition=calibration_samples_per_condition,
        null_table_samples=null_table_samples,
        target_random_frr=target_random_frr,
        batch_size=batch_size,
        seed=seed,
        output_dir=output_dir,
        figure_dir=figure_dir,
    )
    for handler in logging.getLogger().handlers:
        handler.flush()
    bundle_inputs = [path for name, path in outputs.items() if name != "review_bundle"]
    bundle_inputs.append(log_path)
    _create_review_bundle(bundle_inputs, outputs["review_bundle"])
    for name, path in outputs.items():
        typer.echo(f"{name}: {path}")
    typer.echo(f"run_log: {log_path}")


if __name__ == "__main__":
    app()
