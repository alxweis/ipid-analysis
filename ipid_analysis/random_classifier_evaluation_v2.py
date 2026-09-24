"""Accuracy-first offline evaluation for fixed-interval RANDOM classification.

Version 2 removes classifier-threshold leakage from the structured generators,
separates model selection from held-out validation, and evaluates every nonempty
subset of the six RANDOM metrics with several empirically calibrated p-value
combiners.  It deliberately does not modify production classification.
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
import pyarrow as pa
import pyarrow.parquet as pq
import typer

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from ipid_analysis.classifier_validation import FIXED_CONFIG
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.random_classifier_evaluation import (
    CONDITIONS as V1_CONDITIONS,
)
from ipid_analysis.random_classifier_evaluation import (
    METRIC_DESCRIPTIONS,
    METRIC_LABELS,
    METRIC_NAMES,
    EmpiricalNullTables,
    ImpairmentCondition,
    _benchmark_metrics,
    _benchmark_subsets,
    _configure_evaluation_style,
    _create_review_bundle,
    _stable_rng,
    _subset_indices,
    _subset_name,
    _threshold_at_false_rejection,
    _wilson_interval,
    compute_metric_scores,
)
from ipid_analysis.strategies import MODULUS

app = typer.Typer(add_completion=False)
LOGGER = logging.getLogger(__name__)

EXPERIMENT_VERSION = "2"
DEFAULT_SELECTION_SAMPLES_PER_STRATEGY = 50_000
DEFAULT_TEST_SAMPLES_PER_STRATEGY = 100_000
DEFAULT_WEIGHT_TRAINING_SAMPLES_PER_STRATEGY = 10_000
DEFAULT_CALIBRATION_SAMPLES_PER_CONDITION = 250_000
DEFAULT_NULL_TABLE_SAMPLES = 500_000
DEFAULT_TARGET_RANDOM_FALSE_REJECTION_RATE = 0.0001
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_OUTPUT_DIR = (
    PROCESSED_DATA_DIR / "classifier-validation" / "random-classifier-evaluation-v2"
)
DEFAULT_FIGURE_DIR = FIGURES_DIR / "classifier-validation" / "random-classifier-evaluation-v2"

COMBINER_NAMES = ("minimum", "fisher", "cauchy", "weighted_minimum")
DATASET_NAMES = ("selection", "heldout")
GENERATOR_NAMES = (
    "REFLECTION",
    "CONSTANT",
    "SINGLE_FIXED",
    "SINGLE_JITTERED",
    "SINGLE_DRIFT",
    "SINGLE_BURSTY",
    "SINGLE_MIXED",
    "PER_DESTINATION",
    "PER_CONNECTION",
    "PER_BUCKET",
    "MULTI_CLUSTERED",
    "MULTI_COUNTER_2",
    "MULTI_COUNTER_4",
    "MULTI_COUNTER_8",
    "MULTI_COUNTER_16",
    "RANDOM",
)
STRUCTURED_GENERATORS = tuple(name for name in GENERATOR_NAMES if name != "RANDOM")
PRODUCTION_PREFILTERED_GENERATORS = frozenset({"CONSTANT", "MULTI_CLUSTERED"})

# These bins describe results only.  They are not used to generate sequences.
STEP_BIN_EDGES = np.asarray(
    [1, 4, 16, 64, 256, 1024, 4096, 16384, 32768, 49152, MODULUS],
    dtype=np.int64,
)

CONDITIONS = (
    *V1_CONDITIONS,
    ImpairmentCondition("loss-20-burst", loss_fraction=0.20, loss_pattern="burst"),
    ImpairmentCondition(
        "loss-20-burst-reorder-20",
        loss_fraction=0.20,
        reorder_fraction=0.20,
        loss_pattern="burst",
    ),
)


@dataclass(frozen=True)
class GeneratedBatch:
    values: np.ndarray
    base_step: np.ndarray
    counter_count: np.ndarray


def _empty_metadata(sample_count: int) -> tuple[np.ndarray, np.ndarray]:
    return (
        np.full(sample_count, -1, dtype=np.int32),
        np.zeros(sample_count, dtype=np.int16),
    )


def _sample_steps(
    shape: int | tuple[int, ...],
    rng: np.random.Generator,
    profile: str,
) -> np.ndarray:
    """Sample the full 16-bit step domain without classifier constants."""
    if profile == "selection":
        samples = np.exp(rng.uniform(0.0, math.log(MODULUS), size=shape))
        return np.clip(np.rint(samples), 1, MODULUS - 1).astype(np.int64)
    if profile == "heldout":
        linear = rng.integers(1, MODULUS, size=shape, dtype=np.int64)
        logarithmic = np.clip(
            np.rint(np.exp(rng.uniform(0.0, math.log(MODULUS), size=shape))),
            1,
            MODULUS - 1,
        ).astype(np.int64)
        choose_linear = rng.random(shape) < 0.55
        return np.where(choose_linear, linear, logarithmic)
    raise ValueError(f"unknown generator profile: {profile}")


def _cumulative_sequences(starts: np.ndarray, increments: np.ndarray) -> np.ndarray:
    cumulative = np.cumsum(increments, axis=1, dtype=np.int64)
    values = np.concatenate([starts[:, None], starts[:, None] + cumulative], axis=1)
    return (values % MODULUS).astype(np.uint16)


def _single_sequences(
    sample_count: int,
    rng: np.random.Generator,
    profile: str,
    mode: str,
) -> GeneratedBatch:
    width = FIXED_CONFIG.sequence_length
    starts = rng.integers(0, MODULUS, size=sample_count, dtype=np.int64)
    base = _sample_steps(sample_count, rng, profile)
    increments = np.repeat(base[:, None], width - 1, axis=1)

    if mode == "jittered":
        maximum_fraction = 0.30 if profile == "selection" else 0.75
        fractions = rng.uniform(0.01, maximum_fraction, size=sample_count)
        radius = np.maximum(1, np.rint(base * fractions).astype(np.int64))
        noise = np.rint((2.0 * rng.random(increments.shape) - 1.0) * radius[:, None]).astype(
            np.int64
        )
        increments = np.clip(increments + noise, 1, MODULUS - 1)
    elif mode == "drift":
        maximum_fraction = 0.50 if profile == "selection" else 1.25
        drift = np.rint(
            base * rng.uniform(-maximum_fraction, maximum_fraction, size=sample_count)
        ).astype(np.int64)
        phase = np.linspace(-0.5, 0.5, width - 1)
        increments = np.clip(
            increments + np.rint(drift[:, None] * phase[None, :]).astype(np.int64),
            1,
            MODULUS - 1,
        )
    elif mode == "bursty":
        probability = 0.08 if profile == "selection" else 0.18
        bursts = rng.random(increments.shape) < probability
        factors = rng.integers(2, 17 if profile == "selection" else 33, size=increments.shape)
        increments = np.where(bursts, increments * factors, increments)
        increments = np.clip(increments, 1, MODULUS - 1)
    elif mode == "mixed":
        independent = _sample_steps(increments.shape, rng, profile)
        keep_base_probability = 0.75 if profile == "selection" else 0.55
        increments = np.where(
            rng.random(increments.shape) < keep_base_probability,
            increments,
            independent,
        )
    elif mode != "fixed":
        raise ValueError(f"unknown single-counter mode: {mode}")

    return GeneratedBatch(
        _cumulative_sequences(starts, increments),
        base.astype(np.int32),
        np.ones(sample_count, dtype=np.int16),
    )


def _scoped_counter_sequences(
    sample_count: int,
    rng: np.random.Generator,
    profile: str,
    scope: str,
) -> GeneratedBatch:
    width = FIXED_CONFIG.sequence_length
    values = np.empty((sample_count, width), dtype=np.uint16)
    if scope == "destination":
        groups = [np.arange(offset, width, 2) for offset in range(2)]
        variable = False
    elif scope in {"connection", "bucket"}:
        groups = [
            connection
            + FIXED_CONFIG.connection_count * np.arange(FIXED_CONFIG.requests_per_connection)
            for connection in range(FIXED_CONFIG.connection_count)
        ]
        variable = scope == "bucket"
    else:
        raise ValueError(f"unknown counter scope: {scope}")

    group_steps = _sample_steps((sample_count, len(groups)), rng, profile)
    for group_index, positions in enumerate(groups):
        starts = rng.integers(0, MODULUS, size=sample_count, dtype=np.int64)
        base = group_steps[:, group_index]
        increments = np.repeat(base[:, None], len(positions) - 1, axis=1)
        if variable:
            radius = np.maximum(1, np.rint(base * 0.35).astype(np.int64))
            noise = np.rint((2.0 * rng.random(increments.shape) - 1.0) * radius[:, None]).astype(
                np.int64
            )
            increments = np.clip(increments + noise, 1, MODULUS - 1)
        values[:, positions] = _cumulative_sequences(starts, increments)

    return GeneratedBatch(
        values,
        np.median(group_steps, axis=1).astype(np.int32),
        np.full(sample_count, len(groups), dtype=np.int16),
    )


def _clustered_multi_sequences(
    sample_count: int,
    rng: np.random.Generator,
    profile: str,
) -> GeneratedBatch:
    width = FIXED_CONFIG.sequence_length
    maximum_clusters = 16
    cluster_count = rng.integers(2, maximum_clusters + 1, size=sample_count)
    starts = rng.integers(
        0,
        MODULUS,
        size=(sample_count, maximum_clusters),
        dtype=np.int64,
    )
    labels = np.floor(rng.random((sample_count, width)) * cluster_count[:, None]).astype(int)
    labels[:, :2] = np.asarray([0, 1])
    width_scale = _sample_steps(sample_count, rng, profile)
    # Cluster widths cover the full scale but are capped to retain a meaningful
    # clustered family; the cap is an evaluator parameter, not a classifier one.
    cap = MODULUS // (4 if profile == "selection" else 2)
    width_scale = np.clip(width_scale, 1, cap)
    offsets = np.floor(rng.random((sample_count, width)) * (width_scale[:, None] + 1)).astype(
        np.int64
    )
    selected_starts = np.take_along_axis(starts, labels, axis=1)
    values = ((selected_starts + offsets) % MODULUS).astype(np.uint16)
    return GeneratedBatch(
        values,
        width_scale.astype(np.int32),
        cluster_count.astype(np.int16),
    )


def _multi_counter_sequences(
    sample_count: int,
    counter_count: int,
    rng: np.random.Generator,
    profile: str,
) -> GeneratedBatch:
    width = FIXED_CONFIG.sequence_length
    alpha = 1.0 if profile == "selection" else 0.35
    weights = rng.dirichlet(np.full(counter_count, alpha), size=sample_count)
    cumulative_weights = np.cumsum(weights, axis=1)
    labels = (rng.random((sample_count, width, 1)) > cumulative_weights[:, None, :]).sum(axis=2)
    labels[:, :counter_count] = np.arange(counter_count)[None, :]
    starts = rng.integers(
        0,
        MODULUS,
        size=(sample_count, counter_count),
        dtype=np.int64,
    )
    steps = _sample_steps((sample_count, counter_count), rng, profile)
    values = np.empty((sample_count, width), dtype=np.int64)
    for counter in range(counter_count):
        selected = labels == counter
        occurrence = np.cumsum(selected, axis=1, dtype=np.int64) - 1
        generated = starts[:, counter, None] + occurrence * steps[:, counter, None]
        values[selected] = generated[selected]
    return GeneratedBatch(
        (values % MODULUS).astype(np.uint16),
        np.median(steps, axis=1).astype(np.int32),
        np.full(sample_count, counter_count, dtype=np.int16),
    )


def generate_v2_sequences(
    sample_count: int,
    rng: np.random.Generator,
    profile: str,
) -> dict[str, GeneratedBatch]:
    """Generate threshold-independent selection or held-out sequences."""
    if sample_count < 1:
        raise ValueError("sample_count must be positive")
    width = FIXED_CONFIG.sequence_length
    empty_step, empty_counter = _empty_metadata(sample_count)

    request_ids = np.asarray(FIXED_CONFIG.request_ip_ids, dtype=np.int64)
    request_pattern = request_ids[np.arange(width) % len(request_ids)]
    reflection_offset = rng.integers(0, MODULUS, size=sample_count, dtype=np.int64)
    reflection = ((request_pattern[None, :] + reflection_offset[:, None]) % MODULUS).astype(
        np.uint16
    )
    constant_value = rng.integers(0, MODULUS, size=sample_count, dtype=np.uint16)
    constant = np.repeat(constant_value[:, None], width, axis=1)

    generated = {
        "REFLECTION": GeneratedBatch(reflection, empty_step.copy(), empty_counter.copy()),
        "CONSTANT": GeneratedBatch(constant, empty_step.copy(), empty_counter.copy()),
        "SINGLE_FIXED": _single_sequences(sample_count, rng, profile, "fixed"),
        "SINGLE_JITTERED": _single_sequences(sample_count, rng, profile, "jittered"),
        "SINGLE_DRIFT": _single_sequences(sample_count, rng, profile, "drift"),
        "SINGLE_BURSTY": _single_sequences(sample_count, rng, profile, "bursty"),
        "SINGLE_MIXED": _single_sequences(sample_count, rng, profile, "mixed"),
        "PER_DESTINATION": _scoped_counter_sequences(sample_count, rng, profile, "destination"),
        "PER_CONNECTION": _scoped_counter_sequences(sample_count, rng, profile, "connection"),
        "PER_BUCKET": _scoped_counter_sequences(sample_count, rng, profile, "bucket"),
        "MULTI_CLUSTERED": _clustered_multi_sequences(sample_count, rng, profile),
        "MULTI_COUNTER_2": _multi_counter_sequences(sample_count, 2, rng, profile),
        "MULTI_COUNTER_4": _multi_counter_sequences(sample_count, 4, rng, profile),
        "MULTI_COUNTER_8": _multi_counter_sequences(sample_count, 8, rng, profile),
        "MULTI_COUNTER_16": _multi_counter_sequences(sample_count, 16, rng, profile),
    }
    random_values = rng.integers(
        0,
        MODULUS,
        size=(sample_count, width),
        dtype=np.uint16,
    )
    generated["RANDOM"] = GeneratedBatch(
        random_values,
        empty_step.copy(),
        empty_counter.copy(),
    )
    return generated


def apply_v2_impairment(
    ideal: np.ndarray,
    condition: ImpairmentCondition,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray, int]:
    """Apply random, concentrated, or burst loss while preserving positions."""
    if ideal.ndim != 2 or ideal.shape[1] != FIXED_CONFIG.sequence_length:
        raise ValueError("ideal sequences must use the fixed 4x25 layout")
    row_count, width = ideal.shape
    loss_count = round(width * condition.loss_fraction)
    loss_mask = np.zeros((row_count, width), dtype=bool)

    if loss_count:
        if condition.loss_pattern == "burst":
            starts = rng.integers(0, width, size=row_count)
            selected = (starts[:, None] + np.arange(loss_count)[None, :]) % width
        elif condition.loss_pattern == "random":
            selected = np.argpartition(rng.random((row_count, width)), loss_count - 1, axis=1)[
                :, :loss_count
            ]
        elif condition.loss_pattern == "one_connection":
            connection = rng.integers(0, FIXED_CONFIG.connection_count, size=row_count)
            candidates = (
                connection[:, None]
                + FIXED_CONFIG.connection_count
                * np.arange(FIXED_CONFIG.requests_per_connection)[None, :]
            )
            order = np.argpartition(rng.random(candidates.shape), loss_count - 1, axis=1)[
                :, :loss_count
            ]
            selected = np.take_along_axis(candidates, order, axis=1)
        elif condition.loss_pattern == "one_destination":
            destination = rng.integers(0, 2, size=row_count)
            candidates = destination[:, None] + 2 * np.arange(width // 2)[None, :]
            order = np.argpartition(rng.random(candidates.shape), loss_count - 1, axis=1)[
                :, :loss_count
            ]
            selected = np.take_along_axis(candidates, order, axis=1)
        else:
            raise ValueError(f"unknown loss pattern: {condition.loss_pattern}")
        loss_mask[np.arange(row_count)[:, None], selected] = True

    values = ideal.copy()
    present = ~loss_mask
    reorder_count = round((width - loss_count) * condition.reorder_fraction)
    if reorder_count > 1:
        keys = rng.random((row_count, width))
        keys[~present] = np.inf
        targets = np.argpartition(keys, reorder_count - 1, axis=1)[:, :reorder_count]
        source_order = np.argsort(rng.random((row_count, reorder_count)), axis=1)
        sources = np.take_along_axis(targets, source_order, axis=1)
        values[np.arange(row_count)[:, None], targets] = values[
            np.arange(row_count)[:, None], sources
        ]
    return values, present, reorder_count


def combine_metric_scores(
    scores: np.ndarray,
    indices: np.ndarray,
    combiner: str,
    metric_weights: np.ndarray | None = None,
) -> np.ndarray:
    """Return a statistic where smaller values are less RANDOM-compatible."""
    selected = np.clip(scores[:, indices].astype(float), 1e-12, 1.0 - 1e-12)
    if combiner == "minimum":
        return selected.min(axis=1)
    if combiner == "fisher":
        return np.log(selected).sum(axis=1)
    if combiner == "cauchy":
        evidence = np.tan((0.5 - selected) * np.pi).mean(axis=1)
        return -evidence
    if combiner == "weighted_minimum":
        if metric_weights is None:
            raise ValueError("weighted_minimum requires metric weights")
        weights = np.asarray(metric_weights, dtype=float)[indices]
        weights = weights / weights.sum()
        return (selected / weights[None, :]).min(axis=1)
    raise ValueError(f"unknown combiner: {combiner}")


def _calibration_metric_scores(
    samples_per_condition: int,
    batch_size: int,
    seed: int,
    null_tables: EmpiricalNullTables,
) -> tuple[np.ndarray, np.ndarray]:
    batches = []
    condition_batches = []
    for condition_index, condition in enumerate(CONDITIONS):
        LOGGER.info("Calibrating v2 condition %s", condition.name)
        produced = 0
        batch_index = 0
        while produced < samples_per_condition:
            size = min(batch_size, samples_per_condition - produced)
            rng = _stable_rng(seed, 101, condition_index, batch_index)
            ideal = rng.integers(
                0,
                MODULUS,
                size=(size, FIXED_CONFIG.sequence_length),
                dtype=np.uint16,
            )
            values, present, _ = apply_v2_impairment(
                ideal,
                condition,
                _stable_rng(seed, 102, condition_index, batch_index),
            )
            batches.append(compute_metric_scores(values, present, null_tables).astype(np.float32))
            condition_batches.append(np.full(size, condition_index, dtype=np.int8))
            produced += size
            batch_index += 1
    return np.concatenate(batches), np.concatenate(condition_batches)


def _singleton_thresholds(
    calibration_scores: np.ndarray,
    calibration_conditions: np.ndarray,
    target: float,
) -> np.ndarray:
    thresholds = np.empty(len(METRIC_NAMES), dtype=float)
    for metric_index in range(len(METRIC_NAMES)):
        thresholds[metric_index] = min(
            _threshold_at_false_rejection(
                calibration_scores[calibration_conditions == condition_index, metric_index],
                target,
            )
            for condition_index in range(len(CONDITIONS))
        )
    return thresholds


def _learn_metric_weights(
    samples_per_strategy: int,
    batch_size: int,
    seed: int,
    null_tables: EmpiricalNullTables,
    singleton_thresholds: np.ndarray,
) -> tuple[np.ndarray, dict[str, float]]:
    detected = np.zeros(len(METRIC_NAMES), dtype=np.int64)
    total = 0
    produced = 0
    batch_index = 0
    while produced < samples_per_strategy:
        size = min(batch_size, samples_per_strategy - produced)
        generated = generate_v2_sequences(
            size,
            _stable_rng(seed, 201, batch_index),
            "selection",
        )
        for strategy_index, strategy in enumerate(STRUCTURED_GENERATORS):
            batch = generated[strategy]
            for condition_index, condition in enumerate(CONDITIONS):
                values, present, _ = apply_v2_impairment(
                    batch.values,
                    condition,
                    _stable_rng(seed, 202, batch_index, strategy_index, condition_index),
                )
                scores = compute_metric_scores(values, present, null_tables)
                detected += (scores < singleton_thresholds[None, :]).sum(axis=0)
                total += size
        produced += size
        batch_index += 1
    power = detected / total
    # Keep every metric eligible while letting selection-set power allocate more
    # of the weighted-minimum false-rejection budget to useful components.
    raw_weights = np.maximum(power, 0.02)
    weights = raw_weights / raw_weights.sum()
    return weights, {name: float(value) for name, value in zip(METRIC_NAMES, power, strict=True)}


def _calibrate_candidates(
    calibration_scores: np.ndarray,
    calibration_conditions: np.ndarray,
    target: float,
    metric_weights: np.ndarray,
) -> dict[tuple[str, int], dict]:
    calibrated = {}
    for combiner in COMBINER_NAMES:
        for mask in range(1, 1 << len(METRIC_NAMES)):
            indices = _subset_indices(mask)
            combined = combine_metric_scores(
                calibration_scores,
                indices,
                combiner,
                metric_weights,
            )
            condition_thresholds = {
                condition.name: _threshold_at_false_rejection(
                    combined[calibration_conditions == condition_index], target
                )
                for condition_index, condition in enumerate(CONDITIONS)
            }
            threshold = min(condition_thresholds.values())
            rejected = combined < threshold
            calibrated[(combiner, mask)] = {
                "threshold": threshold,
                "calibration_threshold_by_condition": condition_thresholds,
                "calibration_random_false_rejection_rate": float(rejected.mean()),
                "calibration_random_false_rejection_rate_by_condition": {
                    condition.name: float(
                        rejected[calibration_conditions == condition_index].mean()
                    )
                    for condition_index, condition in enumerate(CONDITIONS)
                },
            }
    return calibrated


SCORE_SCHEMA = pa.schema(
    [
        ("DATASET", pa.string()),
        ("CONDITION", pa.string()),
        ("GENERATOR_STRATEGY", pa.string()),
        ("SAMPLE_ID", pa.int64()),
        ("PRESENT_COUNT", pa.int16()),
        ("REORDERED_COUNT", pa.int16()),
        ("BASE_STEP", pa.int32()),
        ("COUNTER_COUNT", pa.int16()),
        *((name.upper(), pa.float32()) for name in METRIC_NAMES),
    ]
)


def _write_score_batch(
    writer: pq.ParquetWriter,
    dataset: str,
    condition: ImpairmentCondition,
    strategy: str,
    sample_offset: int,
    present: np.ndarray,
    reorder_count: int,
    generated: GeneratedBatch,
    scores: np.ndarray,
) -> None:
    size = len(scores)
    arrays = [
        pa.array([dataset] * size, type=pa.string()),
        pa.array([condition.name] * size, type=pa.string()),
        pa.array([strategy] * size, type=pa.string()),
        pa.array(np.arange(sample_offset, sample_offset + size), type=pa.int64()),
        pa.array(present.sum(axis=1).astype(np.int16), type=pa.int16()),
        pa.array(np.full(size, reorder_count, dtype=np.int16), type=pa.int16()),
        pa.array(generated.base_step, type=pa.int32()),
        pa.array(generated.counter_count, type=pa.int16()),
    ]
    arrays.extend(
        pa.array(scores[:, index], type=pa.float32()) for index in range(len(METRIC_NAMES))
    )
    writer.write_batch(pa.record_batch(arrays, schema=SCORE_SCHEMA))


def _evaluate_dataset(
    dataset: str,
    samples_per_strategy: int,
    batch_size: int,
    seed: int,
    null_tables: EmpiricalNullTables,
    calibrated: dict[tuple[str, int], dict],
    metric_weights: np.ndarray,
    writer: pq.ParquetWriter | None,
) -> tuple[dict[tuple[str, int, str, str], list[int]], dict[tuple[str, int, str, int], list[int]]]:
    counts: dict[tuple[str, int, str, str], list[int]] = {}
    step_counts: dict[tuple[str, int, str, int], list[int]] = {}
    profile = dataset
    produced = 0
    batch_index = 0
    while produced < samples_per_strategy:
        size = min(batch_size, samples_per_strategy - produced)
        LOGGER.info(
            "Evaluating %s batch %d (%d..%d per strategy)",
            dataset,
            batch_index + 1,
            produced,
            produced + size - 1,
        )
        generated = generate_v2_sequences(
            size,
            _stable_rng(seed, 301, batch_index),
            profile,
        )
        for strategy_index, strategy in enumerate(GENERATOR_NAMES):
            batch = generated[strategy]
            for condition_index, condition in enumerate(CONDITIONS):
                values, present, reorder_count = apply_v2_impairment(
                    batch.values,
                    condition,
                    _stable_rng(seed, 302, batch_index, strategy_index, condition_index),
                )
                scores = compute_metric_scores(values, present, null_tables)
                if writer is not None:
                    _write_score_batch(
                        writer,
                        dataset,
                        condition,
                        strategy,
                        produced,
                        present,
                        reorder_count,
                        batch,
                        scores,
                    )
                step_bins = np.digitize(batch.base_step, STEP_BIN_EDGES[1:-1], right=False)
                for (combiner, mask), calibration in calibrated.items():
                    combined = combine_metric_scores(
                        scores,
                        _subset_indices(mask),
                        combiner,
                        metric_weights,
                    )
                    random_compatible = combined >= calibration["threshold"]
                    key = (combiner, mask, condition.name, strategy)
                    aggregate = counts.setdefault(key, [0, 0])
                    aggregate[0] += int(random_compatible.sum())
                    aggregate[1] += size
                    if strategy != "RANDOM" and np.any(batch.base_step >= 0):
                        for step_bin in np.unique(step_bins[batch.base_step >= 0]):
                            selected = (step_bins == step_bin) & (batch.base_step >= 0)
                            parameter_key = (combiner, mask, strategy, int(step_bin))
                            parameter = step_counts.setdefault(parameter_key, [0, 0])
                            parameter[0] += int(random_compatible[selected].sum())
                            parameter[1] += int(selected.sum())
        produced += size
        batch_index += 1
    return counts, step_counts


def _merge_count_maps(*maps: dict) -> dict:
    merged = {}
    for values in maps:
        for key, counts in values.items():
            target = merged.setdefault(key, [0, 0])
            target[0] += counts[0]
            target[1] += counts[1]
    return merged


def _benchmark_combiners(
    seed: int,
    metric_weights: np.ndarray,
    sample_count: int,
) -> dict[tuple[str, int], float]:
    scores = _stable_rng(seed, 401).random((sample_count, len(METRIC_NAMES)))
    timings = {}
    for combiner in COMBINER_NAMES:
        for mask in range(1, 1 << len(METRIC_NAMES)):
            indices = _subset_indices(mask)
            combine_metric_scores(scores, indices, combiner, metric_weights)
            observations = []
            for _ in range(3):
                started = time.perf_counter()
                combine_metric_scores(scores, indices, combiner, metric_weights)
                observations.append((time.perf_counter() - started) * 1000.0)
            timings[(combiner, mask)] = float(np.median(observations) * 10_000 / sample_count)
    return timings


def _summarize_dataset(
    dataset: str,
    calibrated: dict[tuple[str, int], dict],
    counts: dict[tuple[str, int, str, str], list[int]],
    metric_timings: dict[int, float],
    combiner_timings: dict[tuple[str, int], float],
    target: float,
) -> tuple[list[dict], list[dict]]:
    summaries = []
    details = []
    for (combiner, mask), calibration in calibrated.items():
        metric_names = [METRIC_NAMES[index] for index in _subset_indices(mask)]
        random_rejected = 0
        random_total = 0
        condition_random = {}
        structured_accepted = 0
        structured_total = 0
        residual_accepted = 0
        residual_total = 0
        structured_rates = []
        worst_rate = -1.0
        worst_scenario = ""
        for condition in CONDITIONS:
            for strategy in GENERATOR_NAMES:
                accepted, total = counts[(combiner, mask, condition.name, strategy)]
                if strategy == "RANDOM":
                    errors = total - accepted
                    rate = errors / total
                    random_rejected += errors
                    random_total += total
                    condition_random[condition.name] = (errors, total)
                    error_type = "false_rejection"
                else:
                    errors = accepted
                    rate = accepted / total
                    structured_accepted += accepted
                    structured_total += total
                    structured_rates.append(rate)
                    if strategy not in PRODUCTION_PREFILTERED_GENERATORS:
                        residual_accepted += accepted
                        residual_total += total
                    if rate > worst_rate:
                        worst_rate = rate
                        worst_scenario = f"{condition.name}/{strategy}"
                    error_type = "false_random"
                low, high = _wilson_interval(errors, total)
                details.append(
                    {
                        "dataset": dataset,
                        "combiner": combiner,
                        "subset_mask": mask,
                        "metrics": "+".join(metric_names),
                        "condition": condition.name,
                        "generator_strategy": strategy,
                        "sample_count": total,
                        "random_compatible_count": accepted,
                        "error_type": error_type,
                        "error_count": errors,
                        "error_rate": rate,
                        "error_rate_ci95_low": low,
                        "error_rate_ci95_high": high,
                    }
                )
        random_rate = random_rejected / random_total
        random_low, random_high = _wilson_interval(random_rejected, random_total)
        condition_rates = {
            name: errors / total for name, (errors, total) in condition_random.items()
        }
        worst_condition = max(condition_rates, key=condition_rates.get)
        worst_condition_rate = condition_rates[worst_condition]
        worst_errors, worst_total = condition_random[worst_condition]
        worst_condition_low, worst_condition_high = _wilson_interval(worst_errors, worst_total)
        structured_rate = structured_accepted / structured_total
        structured_low, structured_high = _wilson_interval(structured_accepted, structured_total)
        residual_rate = residual_accepted / residual_total
        summaries.append(
            {
                "dataset": dataset,
                "combiner": combiner,
                "subset_mask": mask,
                "metrics": "+".join(metric_names),
                "metric_count": len(metric_names),
                "threshold": calibration["threshold"],
                "target_random_false_rejection_rate": target,
                "calibration_random_false_rejection_rate": calibration[
                    "calibration_random_false_rejection_rate"
                ],
                "test_random_false_rejection_rate": random_rate,
                "test_random_false_rejection_rate_ci95_low": random_low,
                "test_random_false_rejection_rate_ci95_high": random_high,
                "worst_condition_random_false_rejection_rate": worst_condition_rate,
                "worst_condition_random_false_rejection_rate_ci95_low": worst_condition_low,
                "worst_condition_random_false_rejection_rate_ci95_high": worst_condition_high,
                "worst_random_condition": worst_condition,
                "structured_false_random_rate": structured_rate,
                "structured_false_random_rate_ci95_low": structured_low,
                "structured_false_random_rate_ci95_high": structured_high,
                "residual_like_false_random_rate": residual_rate,
                "p95_strategy_condition_false_random_rate": float(
                    np.quantile(structured_rates, 0.95)
                ),
                "p99_strategy_condition_false_random_rate": float(
                    np.quantile(structured_rates, 0.99)
                ),
                "worst_strategy_condition_false_random_rate": worst_rate,
                "worst_strategy_condition": worst_scenario,
                "measured_metric_runtime_ms_per_10000": metric_timings[mask],
                "measured_combiner_runtime_ms_per_10000": combiner_timings[(combiner, mask)],
                "measured_total_runtime_ms_per_10000": metric_timings[mask]
                + combiner_timings[(combiner, mask)],
                "meets_observed_worst_condition_target": worst_condition_rate <= target,
                "no_significant_condition_exceedance": worst_condition_low <= target,
                "meets_accuracy_constraint": (
                    random_rate <= target and worst_condition_low <= target
                ),
            }
        )
    return summaries, details


def _step_rows(dataset: str, step_counts: dict) -> list[dict]:
    rows = []
    for (combiner, mask, strategy, step_bin), (accepted, total) in step_counts.items():
        rows.append(
            {
                "dataset": dataset,
                "combiner": combiner,
                "subset_mask": mask,
                "metrics": _subset_name(mask),
                "generator_strategy": strategy,
                "step_bin_index": step_bin,
                "step_lower_inclusive": int(STEP_BIN_EDGES[step_bin]),
                "step_upper_exclusive": int(STEP_BIN_EDGES[step_bin + 1]),
                "sample_count": total,
                "false_random_count": accepted,
                "false_random_rate": accepted / total,
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            row["dataset"],
            row["combiner"],
            row["subset_mask"],
            row["generator_strategy"],
            row["step_bin_index"],
        ),
    )


def _accuracy_frontier(rows: list[dict]) -> list[dict]:
    objectives = (
        "test_random_false_rejection_rate",
        "worst_strategy_condition_false_random_rate",
        "p95_strategy_condition_false_random_rate",
        "residual_like_false_random_rate",
        "structured_false_random_rate",
        "measured_total_runtime_ms_per_10000",
    )
    frontier = []
    for candidate in rows:
        dominated = False
        for other in rows:
            if other is candidate:
                continue
            if all(other[key] <= candidate[key] for key in objectives) and any(
                other[key] < candidate[key] for key in objectives
            ):
                dominated = True
                break
        if not dominated:
            frontier.append(candidate)
    return sorted(
        frontier,
        key=lambda row: (
            not row["meets_accuracy_constraint"],
            row["worst_strategy_condition_false_random_rate"],
            row["p95_strategy_condition_false_random_rate"],
            row["residual_like_false_random_rate"],
            row["structured_false_random_rate"],
            row["measured_total_runtime_ms_per_10000"],
        ),
    )


def _write_csv(rows: list[dict], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _write_json(value: dict, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    return path


def _plot_metric_heatmap(details: list[dict], output_path: Path) -> Path:
    _configure_evaluation_style()
    matrix = np.zeros((len(METRIC_NAMES), len(STRUCTURED_GENERATORS)))
    for metric_index, _ in enumerate(METRIC_NAMES):
        mask = 1 << metric_index
        for strategy_index, strategy in enumerate(STRUCTURED_GENERATORS):
            rates = [
                row["error_rate"]
                for row in details
                if row["dataset"] == "heldout"
                and row["combiner"] == "minimum"
                and row["subset_mask"] == mask
                and row["generator_strategy"] == strategy
            ]
            matrix[metric_index, strategy_index] = max(rates)
    fig, ax = plt.subplots(figsize=(11.5, 3.8))
    image = ax.imshow(matrix * 100.0, cmap="magma", vmin=0, vmax=100, aspect="auto")
    ax.set_xticks(
        np.arange(len(STRUCTURED_GENERATORS)),
        [name.replace("_", " ").title() for name in STRUCTURED_GENERATORS],
        rotation=35,
        ha="right",
    )
    ax.set_yticks(
        np.arange(len(METRIC_NAMES)),
        [METRIC_LABELS[name] for name in METRIC_NAMES],
    )
    ax.set_xlabel("Held-out structured generator")
    ax.set_ylabel("Single metric")
    colorbar = fig.colorbar(image, ax=ax)
    colorbar.set_label("Worst-condition False-RANDOM [%]")
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    return output_path


def _plot_accuracy_tradeoff(rows: list[dict], frontier: list[dict], output_path: Path) -> Path:
    _configure_evaluation_style()
    fig, ax = plt.subplots(figsize=(7.16, 4.3))
    runtime = np.asarray([row["measured_total_runtime_ms_per_10000"] for row in rows])
    p95 = np.asarray([row["p95_strategy_condition_false_random_rate"] * 100 for row in rows])
    average = np.asarray([row["residual_like_false_random_rate"] * 100 for row in rows])
    points = ax.scatter(runtime, p95, c=average, cmap="viridis", s=28, alpha=0.8)
    ax.scatter(
        [row["measured_total_runtime_ms_per_10000"] for row in frontier],
        [row["p95_strategy_condition_false_random_rate"] * 100 for row in frontier],
        marker="D",
        facecolors="none",
        edgecolors="black",
        s=52,
        linewidths=0.8,
        label="Held-out Pareto frontier",
    )
    ax.set_xlabel("Runtime [ms / 10,000 sequences]")
    ax.set_ylabel("Held-out p95 False-RANDOM [%]")
    ax.grid(True, alpha=0.2)
    ax.legend(loc="best")
    colorbar = fig.colorbar(points, ax=ax)
    colorbar.set_label("Residual-like False-RANDOM [%]")
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    return output_path


def _plot_step_sensitivity(step_rows: list[dict], output_path: Path) -> Path:
    _configure_evaluation_style()
    fig, axes = plt.subplots(2, 2, figsize=(7.16, 6.2), sharex=True, sharey=True)
    strategies = ("SINGLE_FIXED", "SINGLE_JITTERED", "SINGLE_DRIFT", "SINGLE_BURSTY")
    singleton_metrics = (
        (1, "Raw uniformity"),
        (8, "Bounded increment"),
        (16, "Increment uniformity"),
        (32, "Gap uniformity"),
    )
    for ax, strategy in zip(axes.flat, strategies, strict=True):
        for mask, label in singleton_metrics:
            rows = [
                row
                for row in step_rows
                if row["dataset"] == "heldout"
                and row["combiner"] == "minimum"
                and row["subset_mask"] == mask
                and row["generator_strategy"] == strategy
            ]
            rows.sort(key=lambda row: row["step_bin_index"])
            x = [
                math.sqrt(row["step_lower_inclusive"] * row["step_upper_exclusive"])
                for row in rows
            ]
            y = [row["false_random_rate"] * 100 for row in rows]
            ax.plot(x, y, marker="o", markersize=2.5, linewidth=1.0, label=label)
        ax.set_xscale("log")
        ax.set_title(strategy.replace("_", " ").title())
        ax.grid(True, alpha=0.2)
    for ax in axes[-1]:
        ax.set_xlabel("Generated base step")
    for ax in axes[:, 0]:
        ax.set_ylabel("False-RANDOM [%]")
    axes[0, 0].legend(fontsize=7, loc="best")
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    return output_path


def _write_recommendations(frontier: list[dict], target: float, output_path: Path) -> Path:
    lines = [
        "RANDOM classifier v2 accuracy-first candidates",
        "==============================================",
        "",
        "Offline validation only; production classification is unchanged.",
        "Generators do not import classifier increment or clustering thresholds.",
        f"Target RANDOM false-rejection rate: {target:.8g}",
        "",
        "Held-out Pareto candidates in accuracy-first review order:",
    ]
    for rank, row in enumerate(frontier[:20], start=1):
        lines.extend(
            [
                "",
                f"{rank}. {row['combiner']}: {row['metrics']}",
                f"   threshold: {row['threshold']:.12g}",
                f"   pooled RANDOM false rejection: {row['test_random_false_rejection_rate']:.8g}",
                (
                    "   worst-condition RANDOM false rejection: "
                    f"{row['worst_condition_random_false_rejection_rate']:.8g} "
                    f"({row['worst_random_condition']})"
                ),
                (
                    "   worst structured scenario False-RANDOM: "
                    f"{row['worst_strategy_condition_false_random_rate']:.8g} "
                    f"({row['worst_strategy_condition']})"
                ),
                f"   p95 structured scenario False-RANDOM: {row['p95_strategy_condition_false_random_rate']:.8g}",
                f"   residual-like False-RANDOM: {row['residual_like_false_random_rate']:.8g}",
                f"   aggregate False-RANDOM: {row['structured_false_random_rate']:.8g}",
                f"   runtime [ms/10k]: {row['measured_total_runtime_ms_per_10000']:.3f}",
                f"   accuracy constraint: {row['meets_accuracy_constraint']}",
            ]
        )
    lines.extend(
        [
            "",
            "Review rule:",
            "  1. reject candidates with a statistically significant RANDOM-FRR exceedance;",
            "  2. prioritize worst-case, p95, and residual-like False-RANDOM accuracy;",
            "  3. use aggregate error next;",
            "  4. use runtime only after accuracy unless costs differ materially;",
            "  5. inspect scenario and step-sensitivity tables before production changes.",
            "",
        ]
    )
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def evaluate_random_classifier_metrics_v2(
    *,
    selection_samples_per_strategy: int = DEFAULT_SELECTION_SAMPLES_PER_STRATEGY,
    test_samples_per_strategy: int = DEFAULT_TEST_SAMPLES_PER_STRATEGY,
    weight_training_samples_per_strategy: int = DEFAULT_WEIGHT_TRAINING_SAMPLES_PER_STRATEGY,
    calibration_samples_per_condition: int = DEFAULT_CALIBRATION_SAMPLES_PER_CONDITION,
    null_table_samples: int = DEFAULT_NULL_TABLE_SAMPLES,
    target_random_frr: float = DEFAULT_TARGET_RANDOM_FALSE_REJECTION_RATE,
    batch_size: int = DEFAULT_BATCH_SIZE,
    seed: int = 42,
    output_dir: Path | None = None,
    figure_dir: Path | None = None,
    benchmark_sample_count: int = 10_000,
) -> dict[str, Path]:
    """Run threshold-independent selection and held-out metric evaluation."""
    sample_counts = (
        selection_samples_per_strategy,
        test_samples_per_strategy,
        weight_training_samples_per_strategy,
        calibration_samples_per_condition,
        null_table_samples,
    )
    if any(value < 1 for value in sample_counts):
        raise ValueError("sample counts must be positive")
    if not 0.0 <= target_random_frr < 1.0:
        raise ValueError("target RANDOM false-rejection rate must be in [0, 1)")
    output_dir = output_dir or DEFAULT_OUTPUT_DIR
    figure_dir = figure_dir or DEFAULT_FIGURE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    figure_dir.mkdir(parents=True, exist_ok=True)

    quality_warnings = []
    null_resolution = 1.0 / (null_table_samples + 1.0)
    if null_resolution > target_random_frr:
        quality_warnings.append(
            "null-table p-value resolution is coarser than the target RANDOM false-rejection rate"
        )
    expected_tail = calibration_samples_per_condition * target_random_frr
    if expected_tail < 20:
        quality_warnings.append(
            "fewer than 20 expected calibration observations per condition lie in the target tail"
        )
    for warning in quality_warnings:
        LOGGER.warning("Exploratory sample budget: %s", warning)

    started = time.perf_counter()
    null_tables = EmpiricalNullTables(null_table_samples, seed + 1)
    calibration_scores, calibration_conditions = _calibration_metric_scores(
        calibration_samples_per_condition,
        batch_size,
        seed + 2,
        null_tables,
    )
    singleton_thresholds = _singleton_thresholds(
        calibration_scores,
        calibration_conditions,
        target_random_frr,
    )
    metric_weights, singleton_selection_power = _learn_metric_weights(
        weight_training_samples_per_strategy,
        batch_size,
        seed + 3,
        null_tables,
        singleton_thresholds,
    )
    calibrated = _calibrate_candidates(
        calibration_scores,
        calibration_conditions,
        target_random_frr,
        metric_weights,
    )
    del calibration_scores
    del calibration_conditions

    score_path = output_dir / "heldout-metric-scores.pq"
    writer = pq.ParquetWriter(score_path, SCORE_SCHEMA, compression="zstd")
    try:
        selection_counts, selection_step_counts = _evaluate_dataset(
            "selection",
            selection_samples_per_strategy,
            batch_size,
            seed + 4,
            null_tables,
            calibrated,
            metric_weights,
            None,
        )
        heldout_counts, heldout_step_counts = _evaluate_dataset(
            "heldout",
            test_samples_per_strategy,
            batch_size,
            seed + 5,
            null_tables,
            calibrated,
            metric_weights,
            writer,
        )
    finally:
        writer.close()

    standalone_timings = _benchmark_metrics(seed + 6, null_tables, benchmark_sample_count)
    subset_timings = _benchmark_subsets(seed + 6, null_tables, benchmark_sample_count)
    combiner_timings = _benchmark_combiners(
        seed + 7,
        metric_weights,
        benchmark_sample_count,
    )
    selection_summaries, selection_details = _summarize_dataset(
        "selection",
        calibrated,
        selection_counts,
        subset_timings,
        combiner_timings,
        target_random_frr,
    )
    heldout_summaries, heldout_details = _summarize_dataset(
        "heldout",
        calibrated,
        heldout_counts,
        subset_timings,
        combiner_timings,
        target_random_frr,
    )
    all_summaries = selection_summaries + heldout_summaries
    all_details = selection_details + heldout_details
    step_rows = _step_rows("selection", selection_step_counts) + _step_rows(
        "heldout", heldout_step_counts
    )
    frontier = _accuracy_frontier(heldout_summaries)

    result_path = _write_csv(all_summaries, output_dir / "combination-results.csv")
    detail_path = _write_csv(all_details, output_dir / "combination-by-scenario.csv")
    step_path = _write_csv(step_rows, output_dir / "step-sensitivity.csv")
    frontier_path = _write_csv(frontier, output_dir / "heldout-pareto-frontier.csv")
    recommendation_path = _write_recommendations(
        frontier,
        target_random_frr,
        output_dir / "recommendations.txt",
    )
    heatmap_path = _plot_metric_heatmap(
        all_details,
        figure_dir / "metric-false-random-heatmap.pdf",
    )
    tradeoff_path = _plot_accuracy_tradeoff(
        heldout_summaries,
        frontier,
        figure_dir / "combination-accuracy-tradeoff.pdf",
    )
    step_plot_path = _plot_step_sensitivity(
        step_rows,
        figure_dir / "metric-step-sensitivity.pdf",
    )
    elapsed = time.perf_counter() - started
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "experiment_version": EXPERIMENT_VERSION,
        "production_classifier_changed": False,
        "seed": seed,
        "selection_samples_per_strategy_and_condition": selection_samples_per_strategy,
        "heldout_samples_per_strategy_and_condition": test_samples_per_strategy,
        "weight_training_samples_per_strategy_and_condition": (
            weight_training_samples_per_strategy
        ),
        "calibration_samples_per_condition": calibration_samples_per_condition,
        "null_table_samples": null_table_samples,
        "target_random_false_rejection_rate": target_random_frr,
        "null_table_pvalue_resolution": null_resolution,
        "expected_calibration_tail_observations_per_condition": expected_tail,
        "quality_warnings": quality_warnings,
        "batch_size": batch_size,
        "elapsed_seconds": elapsed,
        "metric_order": list(METRIC_NAMES),
        "metric_descriptions": METRIC_DESCRIPTIONS,
        "combiner_order": list(COMBINER_NAMES),
        "combiner_definitions": {
            "minimum": "minimum component p-value (Tippett-style)",
            "fisher": "sum(log(p)); lower values provide more evidence against RANDOM",
            "cauchy": "negative mean tan((0.5-p)*pi); lower values provide more evidence",
            "weighted_minimum": "minimum p_i/w_i with selection-set singleton-power weights",
        },
        "generator_threshold_independence": (
            "v2 generators span the full 1..65535 step domain and do not import "
            "classifier increment or clustering thresholds"
        ),
        "generator_profiles": {
            "selection": "log-uniform base steps with moderate perturbations",
            "heldout": "independent linear/log mixture with broader perturbations and imbalance",
        },
        "evaluation_generators": list(GENERATOR_NAMES),
        "production_prefiltered_generators": sorted(PRODUCTION_PREFILTERED_GENERATORS),
        "conditions": [condition.__dict__ for condition in CONDITIONS],
        "step_result_bins": STEP_BIN_EDGES.tolist(),
        "singleton_selection_detection_power": singleton_selection_power,
        "weighted_minimum_metric_weights": {
            name: float(weight) for name, weight in zip(METRIC_NAMES, metric_weights, strict=True)
        },
        "standalone_metric_runtime_ms_per_10000": standalone_timings,
        "candidate_count_per_dataset": len(COMBINER_NAMES) * ((1 << len(METRIC_NAMES)) - 1),
        "heldout_pareto_candidate_count": len(frontier),
        "heldout_pareto_candidates": frontier,
        "artifacts": {
            "heldout_metric_scores": str(score_path),
            "combination_results": str(result_path),
            "combination_by_scenario": str(detail_path),
            "step_sensitivity": str(step_path),
            "heldout_pareto_frontier": str(frontier_path),
            "recommendations": str(recommendation_path),
            "metric_heatmap": str(heatmap_path),
            "combination_tradeoff": str(tradeoff_path),
            "step_sensitivity_plot": str(step_plot_path),
        },
    }
    summary_path = _write_json(summary, output_dir / "summary.json")
    bundle_path = _create_review_bundle(
        [
            summary_path,
            result_path,
            detail_path,
            step_path,
            frontier_path,
            recommendation_path,
            heatmap_path,
            tradeoff_path,
            step_plot_path,
        ],
        output_dir / "random-classifier-review-bundle-v2.zip",
    )
    LOGGER.info("Evaluation v2 completed in %.1f seconds", elapsed)
    return {
        "summary": summary_path,
        "combination_results": result_path,
        "combination_by_scenario": detail_path,
        "step_sensitivity": step_path,
        "heldout_pareto_frontier": frontier_path,
        "recommendations": recommendation_path,
        "heldout_metric_scores": score_path,
        "metric_heatmap": heatmap_path,
        "combination_tradeoff": tradeoff_path,
        "step_sensitivity_plot": step_plot_path,
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
    selection_samples_per_strategy: int = typer.Option(
        DEFAULT_SELECTION_SAMPLES_PER_STRATEGY,
        min=1,
        help="selection-set samples per strategy and impairment condition",
    ),
    test_samples_per_strategy: int = typer.Option(
        DEFAULT_TEST_SAMPLES_PER_STRATEGY,
        min=1,
        help="independent held-out samples per strategy and impairment condition",
    ),
    weight_training_samples_per_strategy: int = typer.Option(
        DEFAULT_WEIGHT_TRAINING_SAMPLES_PER_STRATEGY,
        min=1,
        help="selection-profile samples used only for weighted-minimum metric weights",
    ),
    calibration_samples_per_condition: int = typer.Option(
        DEFAULT_CALIBRATION_SAMPLES_PER_CONDITION,
        min=1,
        help="independent RANDOM samples per condition for candidate thresholds",
    ),
    null_table_samples: int = typer.Option(
        DEFAULT_NULL_TABLE_SAMPLES,
        min=1,
        help="Monte-Carlo samples per increment/spacing lookup table",
    ),
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
    outputs = evaluate_random_classifier_metrics_v2(
        selection_samples_per_strategy=selection_samples_per_strategy,
        test_samples_per_strategy=test_samples_per_strategy,
        weight_training_samples_per_strategy=weight_training_samples_per_strategy,
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
    bundle_inputs = [
        path
        for name, path in outputs.items()
        if name not in {"heldout_metric_scores", "review_bundle"}
    ]
    bundle_inputs.append(log_path)
    _create_review_bundle(bundle_inputs, outputs["review_bundle"])
    for name, path in outputs.items():
        typer.echo(f"{name}: {path}")
    typer.echo(f"run_log: {log_path}")


if __name__ == "__main__":
    app()
