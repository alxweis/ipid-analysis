"""Offline metric-selection experiment for fixed-interval RANDOM classification.

This module deliberately does not change the production classifier.  It evaluates
the four production RANDOM-score components and two candidate replacements on
held-out synthetic 4x25 data:

* raw IP-ID uniformity
* occupancy/collisions
* circular maximum gap
* bounded increments
* increment uniformity over full, destination, and connection views
* circular gap-distribution uniformity

All 63 non-empty metric subsets are calibrated as complete decision rules.  This
is important because the minimum of multiple component p-values is not itself a
uniform p-value.  Increment and spacing null distributions are generated once
offline; production-like scoring only performs table lookups.
"""

from __future__ import annotations

from collections.abc import Callable
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import math
from pathlib import Path
import time
import zipfile

import matplotlib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer

matplotlib.use("Agg")

from matplotlib.colors import Normalize
import matplotlib.pyplot as plt
from scipy.special import gammaincc

from ipid_analysis.classifier_validation import (
    FIXED_CONFIG,
    FIXED_STRATEGIES,
    generate_fixed_sequences,
)
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.paper_figures import configure_paper_style
from ipid_analysis.strategies import (
    MODULUS,
    RANDOM_STRUCTURE_MIN_TEST_SAMPLES,
    random_structure_bounded_increment_pvalues,
    random_structure_features,
)

app = typer.Typer(add_completion=False)
LOGGER = logging.getLogger(__name__)

DEFAULT_SAMPLES_PER_STRATEGY = 100_000
DEFAULT_CALIBRATION_SAMPLES_PER_CONDITION = 100_000
DEFAULT_NULL_TABLE_SAMPLES = 200_000
DEFAULT_TARGET_RANDOM_FALSE_REJECTION_RATE = 0.0001
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_OUTPUT_DIR = PROCESSED_DATA_DIR / "classifier-validation" / "random-classifier-evaluation"
DEFAULT_FIGURE_DIR = FIGURES_DIR / "classifier-validation" / "random-classifier-evaluation"
MIN_INCREMENT_TRANSITIONS = 10
MAX_INCREMENT_BINS = 16
MIN_EXPECTED_INCREMENT_BIN_COUNT = 5

METRIC_NAMES = (
    "raw_uniformity",
    "occupancy",
    "maximum_gap",
    "bounded_increment",
    "increment_uniformity",
    "gap_uniformity",
)
METRIC_LABELS = {
    "raw_uniformity": "Raw uniformity",
    "occupancy": "Occupancy",
    "maximum_gap": "Maximum gap",
    "bounded_increment": "Bounded increment",
    "increment_uniformity": "Increment uniformity",
    "gap_uniformity": "Gap uniformity",
}
METRIC_DESCRIPTIONS = {
    "raw_uniformity": "16-bin Pearson p-value over present raw IP-IDs",
    "occupancy": "exact lower-tail collision/occupancy p-value",
    "maximum_gap": "conservative upper-tail circular maximum-gap probability",
    "bounded_increment": "production bounded-increment upper-tail p-value",
    "increment_uniformity": (
        "minimum empirical Pearson p-value over adjacency-preserving full, "
        "two destination, and four connection increment views"
    ),
    "gap_uniformity": (
        "empirical upper-tail Cramer-von-Mises score over all circular spacings, "
        "including zero and wraparound gaps"
    ),
}
EVALUATION_GENERATORS = (
    *FIXED_STRATEGIES,
    "SINGLE_CONSTANT_LOW",
    "SINGLE_CONSTANT_MEDIUM",
    "SINGLE_CONSTANT_HIGH",
    "SINGLE_JITTERED_HIGH",
    "MULTI_COUNTER_2",
    "MULTI_COUNTER_4",
    "MULTI_COUNTER_8",
)


@dataclass(frozen=True)
class ImpairmentCondition:
    name: str
    loss_fraction: float = 0.0
    reorder_fraction: float = 0.0
    loss_pattern: str = "random"


CONDITIONS = (
    ImpairmentCondition("ideal"),
    ImpairmentCondition("loss-05-random", loss_fraction=0.05),
    ImpairmentCondition("loss-10-random", loss_fraction=0.10),
    ImpairmentCondition("loss-20-random", loss_fraction=0.20),
    ImpairmentCondition(
        "loss-20-random-reorder-20",
        loss_fraction=0.20,
        reorder_fraction=0.20,
    ),
    ImpairmentCondition(
        "loss-20-one-destination",
        loss_fraction=0.20,
        loss_pattern="one_destination",
    ),
    ImpairmentCondition(
        "loss-20-one-connection",
        loss_fraction=0.20,
        loss_pattern="one_connection",
    ),
)

SCORE_SCHEMA = pa.schema(
    [
        ("CONDITION", pa.string()),
        ("GENERATOR_STRATEGY", pa.string()),
        ("SAMPLE_ID", pa.int64()),
        ("PRESENT_COUNT", pa.int16()),
        ("REORDERED_COUNT", pa.int16()),
        *((name.upper(), pa.float32()) for name in METRIC_NAMES),
    ]
)


def _stable_rng(seed: int, *parts: int) -> np.random.Generator:
    return np.random.default_rng(np.random.SeedSequence([seed, *parts]))


def _power_of_two_bin_count(sample_count: int) -> int:
    """Largest power-of-two bin count retaining five expected samples/bin."""
    maximum = min(MAX_INCREMENT_BINS, sample_count // MIN_EXPECTED_INCREMENT_BIN_COUNT)
    if maximum < 2:
        return 0
    return 1 << math.floor(math.log2(maximum))


def _right_tail_pvalues(observed: np.ndarray, sorted_null: np.ndarray) -> np.ndarray:
    """Conservative empirical P(T >= observed), with an add-one correction."""
    left = np.searchsorted(sorted_null, observed, side="left")
    return (len(sorted_null) - left + 1.0) / (len(sorted_null) + 1.0)


def _spacing_cvm_statistics(values: np.ndarray, present: np.ndarray) -> np.ndarray:
    """C-v-M discrepancy of circular spacings from the IID-uniform spacing law."""
    sample_counts = present.sum(axis=1).astype(np.int64)
    result = np.zeros(len(values), dtype=float)
    if not len(values):
        return result

    sentinel = np.uint32(MODULUS)
    ordered = np.sort(
        np.where(present, values, sentinel).astype(np.uint32, copy=False),
        axis=1,
    )
    for sample_count in np.unique(sample_counts):
        if sample_count < 2:
            continue
        rows = np.flatnonzero(sample_counts == sample_count)
        selected = ordered[rows, :sample_count].astype(np.float64)
        interior = np.diff(selected, axis=1)
        wrap = (MODULUS - selected[:, -1] + selected[:, 0])[:, None]
        spacings = np.concatenate([interior, wrap], axis=1) / MODULUS

        # A circular spacing of IID continuous uniform points has marginal
        # Beta(1, n-1).  The PIT makes the marginal target Uniform(0, 1); the
        # dependence between spacings and 16-bit discreteness are retained by
        # the empirical null calibration below.
        transformed = 1.0 - np.power(
            np.clip(1.0 - spacings, 0.0, 1.0),
            sample_count - 1,
        )
        transformed.sort(axis=1)
        target = (2.0 * np.arange(1, sample_count + 1) - 1.0) / (2.0 * sample_count)
        result[rows] = 1.0 / (12.0 * sample_count) + np.square(transformed - target[None, :]).sum(
            axis=1
        )
    return result


class EmpiricalNullTables:
    """Lazily generated, deterministic null tables for candidate metrics."""

    def __init__(self, sample_count: int, seed: int, batch_size: int = 20_000):
        if sample_count < 1:
            raise ValueError("null-table sample count must be positive")
        self.sample_count = sample_count
        self.seed = seed
        self.batch_size = batch_size
        self._increment: dict[tuple[int, int], np.ndarray] = {}
        self._spacing: dict[int, np.ndarray] = {}

    def increment(self, transition_count: int, bin_count: int) -> np.ndarray:
        key = (transition_count, bin_count)
        if key not in self._increment:
            LOGGER.info(
                "Generating increment null table m=%d, bins=%d (%d samples)",
                transition_count,
                bin_count,
                self.sample_count,
            )
            rng = _stable_rng(self.seed, 11, transition_count, bin_count)
            counts = rng.multinomial(
                transition_count,
                np.full(bin_count, 1.0 / bin_count),
                size=self.sample_count,
            )
            expected = transition_count / bin_count
            statistics = np.square(counts - expected).sum(axis=1) / expected
            statistics.sort()
            self._increment[key] = statistics
        return self._increment[key]

    def spacing(self, sample_count: int) -> np.ndarray:
        if sample_count not in self._spacing:
            LOGGER.info(
                "Generating spacing null table n=%d (%d samples)",
                sample_count,
                self.sample_count,
            )
            statistics = np.empty(self.sample_count, dtype=float)
            offset = 0
            batch_index = 0
            while offset < self.sample_count:
                size = min(self.batch_size, self.sample_count - offset)
                rng = _stable_rng(self.seed, 23, sample_count, batch_index)
                values = rng.integers(
                    0,
                    MODULUS,
                    size=(size, sample_count),
                    dtype=np.uint16,
                )
                statistics[offset : offset + size] = _spacing_cvm_statistics(
                    values,
                    np.ones_like(values, dtype=bool),
                )
                offset += size
                batch_index += 1
            statistics.sort()
            self._spacing[sample_count] = statistics
        return self._spacing[sample_count]


def _increment_view_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    null_tables: EmpiricalNullTables,
) -> np.ndarray:
    """Pearson uniformity p-values using only originally adjacent positions."""
    if values.shape[1] < 2:
        return np.ones(len(values), dtype=float)
    pair_present = present[:, :-1] & present[:, 1:]
    transitions = pair_present.sum(axis=1).astype(np.int64)
    increments = (values[:, 1:].astype(np.int64) - values[:, :-1].astype(np.int64)) % MODULUS
    result = np.ones(len(values), dtype=float)

    for transition_count in np.unique(transitions):
        if transition_count < MIN_INCREMENT_TRANSITIONS:
            continue
        bin_count = _power_of_two_bin_count(int(transition_count))
        if bin_count < 2:
            continue
        rows = np.flatnonzero(transitions == transition_count)
        bins = (increments[rows] * bin_count) // MODULUS
        active = pair_present[rows]
        row_ids = np.broadcast_to(np.arange(len(rows))[:, None], bins.shape)
        flat = (row_ids * bin_count + bins)[active]
        counts = np.bincount(flat, minlength=len(rows) * bin_count).reshape(len(rows), bin_count)
        expected = transition_count / bin_count
        statistics = np.square(counts - expected).sum(axis=1) / expected
        result[rows] = _right_tail_pvalues(
            statistics,
            null_tables.increment(int(transition_count), bin_count),
        )
    return result


def increment_uniformity_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    null_tables: EmpiricalNullTables,
) -> np.ndarray:
    """Minimum p-value over full, two destination, and four connection views."""
    if values.shape[1] != FIXED_CONFIG.sequence_length:
        raise ValueError(
            f"expected {FIXED_CONFIG.sequence_length} fixed positions, got {values.shape[1]}"
        )
    components = [_increment_view_pvalues(values, present, null_tables)]
    components.extend(
        _increment_view_pvalues(values[:, offset::2], present[:, offset::2], null_tables)
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
        _increment_view_pvalues(
            connection_values[:, connection],
            connection_present[:, connection],
            null_tables,
        )
        for connection in range(FIXED_CONFIG.connection_count)
    )
    return np.minimum.reduce(components)


def gap_uniformity_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    null_tables: EmpiricalNullTables,
) -> np.ndarray:
    """Empirical compatibility p-value for the complete circular gap distribution."""
    counts = present.sum(axis=1).astype(np.int64)
    statistics = _spacing_cvm_statistics(values, present)
    result = np.ones(len(values), dtype=float)
    for sample_count in np.unique(counts):
        if sample_count < RANDOM_STRUCTURE_MIN_TEST_SAMPLES:
            continue
        rows = np.flatnonzero(counts == sample_count)
        result[rows] = _right_tail_pvalues(
            statistics[rows],
            null_tables.spacing(int(sample_count)),
        )
    return result


def compute_metric_scores(
    values: np.ndarray,
    present: np.ndarray,
    null_tables: EmpiricalNullTables,
) -> np.ndarray:
    """Return one RANDOM-compatibility score per row and metric."""
    features = random_structure_features(values, present)
    scores = np.column_stack(
        [
            features.uniformity_pvalue,
            features.occupancy_pvalue,
            features.maximum_gap_pvalue,
            random_structure_bounded_increment_pvalues(values, present, FIXED_CONFIG),
            increment_uniformity_pvalues(values, present, null_tables),
            gap_uniformity_pvalues(values, present, null_tables),
        ]
    )
    return np.clip(scores, 0.0, 1.0)


def _raw_uniformity_pvalues(values: np.ndarray, present: np.ndarray) -> np.ndarray:
    """Production 16-bin raw-IPID Pearson component without an unnecessary sort."""
    row_count = len(values)
    sample_count = present.sum(axis=1)
    bin_count = 16
    bins = (values.astype(np.int64) * bin_count) // MODULUS
    rows = np.broadcast_to(np.arange(row_count)[:, None], values.shape)
    flat = (rows * bin_count + bins)[present]
    counts = np.bincount(flat, minlength=row_count * bin_count).reshape(row_count, bin_count)
    expected = np.where(sample_count > 0, sample_count / bin_count, 1.0)[:, None]
    statistic = np.square(counts - expected).sum(axis=1) / expected[:, 0]
    return np.where(
        sample_count > 0,
        gammaincc((bin_count - 1) / 2.0, statistic / 2.0),
        1.0,
    )


def apply_impairment(
    ideal: np.ndarray,
    condition: ImpairmentCondition,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray, int]:
    """Apply a loss mask and optional value reordering without compacting positions."""
    if ideal.ndim != 2 or ideal.shape[1] != FIXED_CONFIG.sequence_length:
        raise ValueError("ideal sequences must use the fixed 4x25 layout")
    row_count, width = ideal.shape
    loss_count = round(width * condition.loss_fraction)
    loss_mask = np.zeros((row_count, width), dtype=bool)

    if loss_count:
        if condition.loss_pattern == "random":
            keys = rng.random((row_count, width))
            selected = np.argpartition(keys, loss_count - 1, axis=1)[:, :loss_count]
        elif condition.loss_pattern == "one_connection":
            if loss_count > FIXED_CONFIG.requests_per_connection:
                raise ValueError("loss does not fit within one connection")
            connection = rng.integers(0, FIXED_CONFIG.connection_count, size=row_count)
            candidates = (
                connection[:, None]
                + FIXED_CONFIG.connection_count
                * np.arange(FIXED_CONFIG.requests_per_connection)[None, :]
            )
            order = np.argpartition(
                rng.random(candidates.shape),
                loss_count - 1,
                axis=1,
            )[:, :loss_count]
            selected = np.take_along_axis(candidates, order, axis=1)
        elif condition.loss_pattern == "one_destination":
            destination = rng.integers(0, 2, size=row_count)
            candidates = destination[:, None] + 2 * np.arange(width // 2)[None, :]
            order = np.argpartition(
                rng.random(candidates.shape),
                loss_count - 1,
                axis=1,
            )[:, :loss_count]
            selected = np.take_along_axis(candidates, order, axis=1)
        else:
            raise ValueError(f"unknown loss pattern: {condition.loss_pattern}")
        loss_mask[np.arange(row_count)[:, None], selected] = True

    values = ideal.copy()
    present = ~loss_mask
    present_count = width - loss_count
    reorder_count = round(present_count * condition.reorder_fraction)
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


def _constant_counter_sequences(
    sample_count: int,
    rng: np.random.Generator,
    minimum_step: int,
    maximum_step: int,
    *,
    jitter: int = 0,
) -> np.ndarray:
    starts = rng.integers(0, MODULUS, size=sample_count, dtype=np.int64)
    base_steps = rng.integers(
        minimum_step,
        maximum_step + 1,
        size=sample_count,
        dtype=np.int64,
    )
    increments = np.repeat(
        base_steps[:, None],
        FIXED_CONFIG.sequence_length - 1,
        axis=1,
    )
    if jitter:
        increments += rng.integers(
            -jitter,
            jitter + 1,
            size=increments.shape,
            dtype=np.int64,
        )
        increments = np.clip(increments, 1, MODULUS - 1)
    cumulative = np.cumsum(increments, axis=1, dtype=np.int64)
    values = np.concatenate([starts[:, None], starts[:, None] + cumulative], axis=1)
    return (values % MODULUS).astype(np.uint16)


def _interleaved_counter_sequences(
    sample_count: int,
    counter_count: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Interleave independently advancing counters without creating value clusters."""
    width = FIXED_CONFIG.sequence_length
    labels = rng.integers(0, counter_count, size=(sample_count, width))
    # Guarantee that each counter contributes at least once without imposing a
    # fixed round-robin order on the remaining positions.
    labels[:, :counter_count] = np.arange(counter_count)[None, :]
    starts = rng.integers(
        0,
        MODULUS,
        size=(sample_count, counter_count),
        dtype=np.int64,
    )
    steps = rng.integers(
        1,
        4097,
        size=(sample_count, counter_count),
        dtype=np.int64,
    )
    values = np.empty((sample_count, width), dtype=np.int64)
    for counter in range(counter_count):
        selected = labels == counter
        occurrence = np.cumsum(selected, axis=1, dtype=np.int64) - 1
        generated = starts[:, counter, None] + occurrence * steps[:, counter, None]
        values[selected] = generated[selected]
    return (values % MODULUS).astype(np.uint16)


def generate_evaluation_sequences(
    sample_count: int,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """Ideal strategy generators plus counter-rate and non-clustered MULTI variants."""
    generated = generate_fixed_sequences(sample_count, rng)
    generated.update(
        {
            "SINGLE_CONSTANT_LOW": _constant_counter_sequences(sample_count, rng, 1, 32),
            "SINGLE_CONSTANT_MEDIUM": _constant_counter_sequences(sample_count, rng, 256, 4096),
            "SINGLE_CONSTANT_HIGH": _constant_counter_sequences(
                sample_count, rng, 21_846, MODULUS - 1
            ),
            "SINGLE_JITTERED_HIGH": _constant_counter_sequences(
                sample_count,
                rng,
                21_846,
                MODULUS - 1,
                jitter=1024,
            ),
            "MULTI_COUNTER_2": _interleaved_counter_sequences(sample_count, 2, rng),
            "MULTI_COUNTER_4": _interleaved_counter_sequences(sample_count, 4, rng),
            "MULTI_COUNTER_8": _interleaved_counter_sequences(sample_count, 8, rng),
        }
    )
    return generated


def _subset_indices(mask: int) -> np.ndarray:
    return np.flatnonzero([(mask >> index) & 1 for index in range(len(METRIC_NAMES))])


def _subset_name(mask: int) -> str:
    return "+".join(METRIC_NAMES[index] for index in _subset_indices(mask))


def _threshold_at_false_rejection(scores: np.ndarray, target: float) -> float:
    """Threshold whose strict lower tail contains no more than target fraction."""
    ordered = np.sort(scores)
    index = min(math.floor(target * len(ordered)), len(ordered) - 1)
    return float(ordered[index])


def _calibration_scores(
    samples_per_condition: int,
    batch_size: int,
    seed: int,
    null_tables: EmpiricalNullTables,
) -> tuple[np.ndarray, np.ndarray]:
    score_batches = []
    condition_batches = []
    for condition_index, condition in enumerate(CONDITIONS):
        LOGGER.info("Calibrating condition %s", condition.name)
        produced = 0
        batch_index = 0
        while produced < samples_per_condition:
            size = min(batch_size, samples_per_condition - produced)
            value_rng = _stable_rng(seed, 101, condition_index, batch_index)
            ideal = value_rng.integers(
                0,
                MODULUS,
                size=(size, FIXED_CONFIG.sequence_length),
                dtype=np.uint16,
            )
            impairment_rng = _stable_rng(seed, 102, condition_index, batch_index)
            values, present, _ = apply_impairment(ideal, condition, impairment_rng)
            score_batches.append(
                compute_metric_scores(values, present, null_tables).astype(np.float32)
            )
            condition_batches.append(np.full(size, condition_index, dtype=np.int8))
            produced += size
            batch_index += 1
    return np.concatenate(score_batches), np.concatenate(condition_batches)


def _calibrate_subsets(
    calibration_scores: np.ndarray,
    calibration_conditions: np.ndarray,
    target: float,
) -> dict[int, dict]:
    calibrated = {}
    for mask in range(1, 1 << len(METRIC_NAMES)):
        indices = _subset_indices(mask)
        combined = calibration_scores[:, indices].min(axis=1)
        condition_thresholds = {
            condition.name: _threshold_at_false_rejection(
                combined[calibration_conditions == index], target
            )
            for index, condition in enumerate(CONDITIONS)
        }
        # A single production threshold must satisfy every supported impairment
        # condition.  Lower thresholds reject fewer true RANDOM sequences.
        threshold = min(condition_thresholds.values())
        rejected = combined < threshold
        by_condition = {
            condition.name: float(rejected[calibration_conditions == index].mean())
            for index, condition in enumerate(CONDITIONS)
        }
        calibrated[mask] = {
            "threshold": threshold,
            "calibration_threshold_by_condition": condition_thresholds,
            "calibration_random_false_rejection_rate": float(rejected.mean()),
            "calibration_random_false_rejection_rate_by_condition": by_condition,
        }
    return calibrated


def _write_score_batch(
    writer: pq.ParquetWriter,
    condition: ImpairmentCondition,
    strategy: str,
    sample_offset: int,
    present: np.ndarray,
    reorder_count: int,
    scores: np.ndarray,
) -> None:
    size = len(scores)
    arrays = [
        pa.array([condition.name] * size, type=pa.string()),
        pa.array([strategy] * size, type=pa.string()),
        pa.array(np.arange(sample_offset, sample_offset + size), type=pa.int64()),
        pa.array(present.sum(axis=1).astype(np.int16), type=pa.int16()),
        pa.array(np.full(size, reorder_count, dtype=np.int16), type=pa.int16()),
    ]
    arrays.extend(
        pa.array(scores[:, index], type=pa.float32()) for index in range(len(METRIC_NAMES))
    )
    writer.write_batch(pa.record_batch(arrays, schema=SCORE_SCHEMA))


def _evaluate_test_data(
    samples_per_strategy: int,
    batch_size: int,
    seed: int,
    null_tables: EmpiricalNullTables,
    calibrated: dict[int, dict],
    score_path: Path,
) -> dict[tuple[int, str, str], list[int]]:
    counts: dict[tuple[int, str, str], list[int]] = {}
    score_path.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(score_path, SCORE_SCHEMA, compression="zstd")
    try:
        produced = 0
        batch_index = 0
        while produced < samples_per_strategy:
            size = min(batch_size, samples_per_strategy - produced)
            LOGGER.info(
                "Evaluating test batch %d (%d..%d per strategy)",
                batch_index + 1,
                produced,
                produced + size - 1,
            )
            generator_rng = _stable_rng(seed, 201, batch_index)
            generated = generate_evaluation_sequences(size, generator_rng)
            for strategy_index, strategy in enumerate(EVALUATION_GENERATORS):
                ideal = generated[strategy]
                for condition_index, condition in enumerate(CONDITIONS):
                    impairment_rng = _stable_rng(
                        seed,
                        202,
                        batch_index,
                        strategy_index,
                        condition_index,
                    )
                    values, present, reorder_count = apply_impairment(
                        ideal,
                        condition,
                        impairment_rng,
                    )
                    scores = compute_metric_scores(values, present, null_tables)
                    _write_score_batch(
                        writer,
                        condition,
                        strategy,
                        produced,
                        present,
                        reorder_count,
                        scores,
                    )
                    for mask, calibration in calibrated.items():
                        combined = scores[:, _subset_indices(mask)].min(axis=1)
                        random_compatible = combined >= calibration["threshold"]
                        key = (mask, condition.name, strategy)
                        if key not in counts:
                            counts[key] = [0, 0]
                        counts[key][0] += int(random_compatible.sum())
                        counts[key][1] += size
            produced += size
            batch_index += 1
    finally:
        writer.close()
    return counts


def _benchmark_metrics(
    seed: int,
    null_tables: EmpiricalNullTables,
    sample_count: int = 10_000,
) -> dict[str, float]:
    rng = _stable_rng(seed, 301)
    values = rng.integers(
        0,
        MODULUS,
        size=(sample_count, FIXED_CONFIG.sequence_length),
        dtype=np.uint16,
    )
    present = np.ones_like(values, dtype=bool)
    functions: dict[str, Callable[[], np.ndarray]] = {
        "raw_uniformity": lambda: _raw_uniformity_pvalues(values, present),
        "occupancy": lambda: random_structure_features(values, present).occupancy_pvalue,
        "maximum_gap": lambda: random_structure_features(values, present).maximum_gap_pvalue,
        "bounded_increment": lambda: random_structure_bounded_increment_pvalues(
            values, present, FIXED_CONFIG
        ),
        "increment_uniformity": lambda: increment_uniformity_pvalues(values, present, null_tables),
        "gap_uniformity": lambda: gap_uniformity_pvalues(values, present, null_tables),
    }
    timings = {}
    for name, function in functions.items():
        function()  # warm caches and code paths; offline table construction is excluded
        observations = []
        for _ in range(3):
            started = time.perf_counter()
            function()
            observations.append((time.perf_counter() - started) * 1000.0)
        timings[name] = float(np.median(observations) * 10_000 / sample_count)
    return timings


def _benchmark_subsets(
    seed: int,
    null_tables: EmpiricalNullTables,
    sample_count: int = 10_000,
) -> dict[int, float]:
    rng = _stable_rng(seed, 302)
    values = rng.integers(
        0,
        MODULUS,
        size=(sample_count, FIXED_CONFIG.sequence_length),
        dtype=np.uint16,
    )
    present = np.ones_like(values, dtype=bool)

    def calculate(mask: int) -> np.ndarray:
        selected = {METRIC_NAMES[index] for index in _subset_indices(mask)}
        components = []
        features = None
        if selected & {"occupancy", "maximum_gap"}:
            # One production feature pass can share its sort across both
            # components and also supplies raw uniformity when requested.
            features = random_structure_features(values, present)
        if "raw_uniformity" in selected:
            components.append(
                features.uniformity_pvalue
                if features is not None
                else _raw_uniformity_pvalues(values, present)
            )
        if "occupancy" in selected:
            components.append(features.occupancy_pvalue)
        if "maximum_gap" in selected:
            components.append(features.maximum_gap_pvalue)
        if "bounded_increment" in selected:
            components.append(
                random_structure_bounded_increment_pvalues(values, present, FIXED_CONFIG)
            )
        if "increment_uniformity" in selected:
            components.append(increment_uniformity_pvalues(values, present, null_tables))
        if "gap_uniformity" in selected:
            components.append(gap_uniformity_pvalues(values, present, null_tables))
        return np.minimum.reduce(components)

    timings = {}
    for mask in range(1, 1 << len(METRIC_NAMES)):
        calculate(mask)
        observations = []
        for _ in range(3):
            started = time.perf_counter()
            calculate(mask)
            observations.append((time.perf_counter() - started) * 1000.0)
        timings[mask] = float(np.median(observations) * 10_000 / sample_count)
    return timings


def _wilson_interval(
    successes: int, total: int, z: float = 1.959963984540054
) -> tuple[float, float]:
    if total == 0:
        return 0.0, 0.0
    proportion = successes / total
    denominator = 1.0 + z * z / total
    center = (proportion + z * z / (2.0 * total)) / denominator
    radius = (
        z
        * math.sqrt(proportion * (1.0 - proportion) / total + z * z / (4.0 * total * total))
        / denominator
    )
    return max(0.0, center - radius), min(1.0, center + radius)


def _summarize_subsets(
    calibrated: dict[int, dict],
    counts: dict[tuple[int, str, str], list[int]],
    timings: dict[str, float],
    subset_timings: dict[int, float],
    target_random_frr: float,
) -> tuple[list[dict], list[dict]]:
    detail_rows = []
    summary_rows = []
    for mask, calibration in calibrated.items():
        metric_indices = _subset_indices(mask)
        metric_names = [METRIC_NAMES[index] for index in metric_indices]
        random_rejected = 0
        random_total = 0
        random_frr_by_condition = {}
        structured_accepted = 0
        structured_total = 0
        worst_false_random = 0.0
        worst_false_random_scenario = ""
        for condition in CONDITIONS:
            for strategy in EVALUATION_GENERATORS:
                accepted, total = counts[(mask, condition.name, strategy)]
                if strategy == "RANDOM":
                    false_count = total - accepted
                    rate = false_count / total
                    random_rejected += false_count
                    random_total += total
                    random_frr_by_condition[condition.name] = rate
                    error_name = "false_rejection"
                else:
                    false_count = accepted
                    rate = accepted / total
                    structured_accepted += accepted
                    structured_total += total
                    if rate > worst_false_random:
                        worst_false_random = rate
                        worst_false_random_scenario = f"{condition.name}/{strategy}"
                    error_name = "false_random"
                interval_low, interval_high = _wilson_interval(false_count, total)
                detail_rows.append(
                    {
                        "subset_mask": mask,
                        "metrics": "+".join(metric_names),
                        "condition": condition.name,
                        "generator_strategy": strategy,
                        "sample_count": total,
                        "random_compatible_count": accepted,
                        "error_type": error_name,
                        "error_count": false_count,
                        "error_rate": rate,
                        "error_rate_ci95_low": interval_low,
                        "error_rate_ci95_high": interval_high,
                    }
                )
        random_frr = random_rejected / random_total
        worst_random_frr = max(random_frr_by_condition.values())
        false_random_rate = structured_accepted / structured_total
        random_ci_low, random_ci_high = _wilson_interval(random_rejected, random_total)
        structured_ci_low, structured_ci_high = _wilson_interval(
            structured_accepted, structured_total
        )
        runtime = sum(timings[name] for name in metric_names)
        summary_rows.append(
            {
                "subset_mask": mask,
                "metrics": "+".join(metric_names),
                "metric_count": len(metric_names),
                "threshold": calibration["threshold"],
                "target_random_false_rejection_rate": target_random_frr,
                "calibration_random_false_rejection_rate": calibration[
                    "calibration_random_false_rejection_rate"
                ],
                "test_random_false_rejection_rate": random_frr,
                "test_random_false_rejection_rate_ci95_low": random_ci_low,
                "test_random_false_rejection_rate_ci95_high": random_ci_high,
                "worst_condition_random_false_rejection_rate": worst_random_frr,
                "structured_false_random_rate": false_random_rate,
                "structured_false_random_rate_ci95_low": structured_ci_low,
                "structured_false_random_rate_ci95_high": structured_ci_high,
                "worst_strategy_condition_false_random_rate": worst_false_random,
                "worst_strategy_condition": worst_false_random_scenario,
                "standalone_runtime_sum_ms_per_10000": runtime,
                "measured_subset_runtime_ms_per_10000": subset_timings[mask],
                "meets_test_random_frr_target": worst_random_frr <= target_random_frr,
            }
        )
    return summary_rows, detail_rows


def _pareto_frontier(rows: list[dict]) -> list[dict]:
    objectives = (
        "test_random_false_rejection_rate",
        "worst_condition_random_false_rejection_rate",
        "structured_false_random_rate",
        "worst_strategy_condition_false_random_rate",
        "measured_subset_runtime_ms_per_10000",
        "metric_count",
    )
    frontier = []
    for candidate in rows:
        dominated = False
        for other in rows:
            if other is candidate:
                continue
            no_worse = all(other[key] <= candidate[key] for key in objectives)
            strictly_better = any(other[key] < candidate[key] for key in objectives)
            if no_worse and strictly_better:
                dominated = True
                break
        if not dominated:
            frontier.append(candidate)
    return sorted(
        frontier,
        key=lambda row: (
            not row["meets_test_random_frr_target"],
            row["worst_strategy_condition_false_random_rate"],
            row["structured_false_random_rate"],
            row["metric_count"],
            row["measured_subset_runtime_ms_per_10000"],
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


def _configure_evaluation_style() -> None:
    """Use paper fonts when installed, while keeping validation runnable anywhere."""
    try:
        configure_paper_style()
    except RuntimeError as exc:
        LOGGER.warning("Paper font setup unavailable; using Matplotlib defaults: %s", exc)
        plt.rcdefaults()


def _plot_metric_heatmap(detail_rows: list[dict], output_path: Path) -> Path:
    _configure_evaluation_style()
    singleton_masks = [1 << index for index in range(len(METRIC_NAMES))]
    structured = [strategy for strategy in EVALUATION_GENERATORS if strategy != "RANDOM"]
    matrix = np.zeros((len(singleton_masks), len(structured)), dtype=float)
    for metric_index, mask in enumerate(singleton_masks):
        for strategy_index, strategy in enumerate(structured):
            rates = [
                row["error_rate"]
                for row in detail_rows
                if row["subset_mask"] == mask and row["generator_strategy"] == strategy
            ]
            matrix[metric_index, strategy_index] = max(rates)

    fig, ax = plt.subplots(figsize=(11.0, 3.8))
    image = ax.imshow(matrix * 100.0, cmap="magma", vmin=0, vmax=100, aspect="auto")
    ax.set_xticks(
        np.arange(len(structured)), [name.replace("_", " ").title() for name in structured]
    )
    ax.set_yticks(np.arange(len(METRIC_NAMES)), [METRIC_LABELS[name] for name in METRIC_NAMES])
    ax.tick_params(axis="x", rotation=35)
    ax.set_xlabel("Structured generator")
    ax.set_ylabel("Single metric")
    colorbar = fig.colorbar(image, ax=ax)
    colorbar.set_label("Worst-condition False-RANDOM [%]")
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    return output_path


def _plot_subset_tradeoff(rows: list[dict], frontier: list[dict], output_path: Path) -> Path:
    _configure_evaluation_style()
    frontier_masks = {row["subset_mask"] for row in frontier}
    fig, ax = plt.subplots(figsize=(7.16, 4.2))
    runtime = np.asarray([row["measured_subset_runtime_ms_per_10000"] for row in rows])
    false_random = np.asarray(
        [max(row["worst_strategy_condition_false_random_rate"] * 100.0, 1e-6) for row in rows]
    )
    random_frr = np.asarray([row["test_random_false_rejection_rate"] * 100.0 for row in rows])
    sizes = np.asarray([20 + 12 * row["metric_count"] for row in rows])
    normalization = Normalize(vmin=0.0, vmax=float(random_frr.max()) or 1.0)
    points = ax.scatter(
        runtime,
        false_random,
        c=random_frr,
        cmap="viridis",
        norm=normalization,
        s=sizes,
        edgecolors="none",
        alpha=0.85,
    )
    frontier_rows = [row for row in rows if row["subset_mask"] in frontier_masks]
    ax.scatter(
        [row["measured_subset_runtime_ms_per_10000"] for row in frontier_rows],
        [
            max(row["worst_strategy_condition_false_random_rate"] * 100.0, 1e-6)
            for row in frontier_rows
        ],
        marker="D",
        facecolors="none",
        edgecolors="black",
        s=[28 + 12 * row["metric_count"] for row in frontier_rows],
        linewidths=0.8,
        label="Pareto frontier",
    )
    ax.set_yscale("log")
    ax.set_xlabel("Measured subset runtime [ms / 10,000 sequences]")
    ax.set_ylabel("Worst-condition False-RANDOM [%]")
    ax.grid(True, which="both", alpha=0.2)
    ax.legend(loc="best")
    colorbar = fig.colorbar(points, ax=ax)
    colorbar.set_label("True-RANDOM false rejection [%]")
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    return output_path


def _write_recommendations(
    frontier: list[dict],
    target: float,
    output_path: Path,
) -> Path:
    lines = [
        "RANDOM classifier metric-selection candidates",
        "=============================================",
        "",
        "This is an offline validation result; it does not change production classification.",
        f"Target RANDOM false-rejection rate: {target:.8g}",
        "",
        "Pareto candidates in review order:",
    ]
    for rank, row in enumerate(frontier[:15], start=1):
        lines.extend(
            [
                "",
                f"{rank}. {row['metrics']}",
                f"   metrics: {row['metric_count']}",
                f"   threshold: {row['threshold']:.12g}",
                f"   test RANDOM false-rejection: {row['test_random_false_rejection_rate']:.8g}",
                (
                    "   test RANDOM false-rejection 95% CI: "
                    f"[{row['test_random_false_rejection_rate_ci95_low']:.8g}, "
                    f"{row['test_random_false_rejection_rate_ci95_high']:.8g}]"
                ),
                (
                    "   worst-condition RANDOM false-rejection: "
                    f"{row['worst_condition_random_false_rejection_rate']:.8g}"
                ),
                f"   structured False-RANDOM: {row['structured_false_random_rate']:.8g}",
                (
                    "   structured False-RANDOM 95% CI: "
                    f"[{row['structured_false_random_rate_ci95_low']:.8g}, "
                    f"{row['structured_false_random_rate_ci95_high']:.8g}]"
                ),
                (
                    "   worst strategy/condition False-RANDOM: "
                    f"{row['worst_strategy_condition_false_random_rate']:.8g} "
                    f"({row['worst_strategy_condition']})"
                ),
                (
                    "   measured subset runtime [ms/10k]: "
                    f"{row['measured_subset_runtime_ms_per_10000']:.3f}"
                ),
                f"   meets strict test target: {row['meets_test_random_frr_target']}",
            ]
        )
    lines.extend(
        [
            "",
            "Selection rule:",
            "  1. enforce the acceptable RANDOM false-rejection bound;",
            "  2. minimize worst-case and aggregate False-RANDOM;",
            "  3. among statistically similar candidates choose the simplest and fastest;",
            "  4. inspect per-scenario CSV rows before changing production.",
            "",
        ]
    )
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def _create_review_bundle(paths: list[Path], output_path: Path) -> Path:
    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in paths:
            archive.write(path, arcname=path.name)
    return output_path


def evaluate_random_classifier_metrics(
    *,
    samples_per_strategy: int = DEFAULT_SAMPLES_PER_STRATEGY,
    calibration_samples_per_condition: int = DEFAULT_CALIBRATION_SAMPLES_PER_CONDITION,
    null_table_samples: int = DEFAULT_NULL_TABLE_SAMPLES,
    target_random_frr: float = DEFAULT_TARGET_RANDOM_FALSE_REJECTION_RATE,
    batch_size: int = DEFAULT_BATCH_SIZE,
    seed: int = 42,
    output_dir: Path | None = None,
    figure_dir: Path | None = None,
    benchmark_sample_count: int = 10_000,
) -> dict[str, Path]:
    """Run calibration, held-out evaluation, Pareto analysis, and reporting."""
    if samples_per_strategy < 1 or calibration_samples_per_condition < 1:
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
    expected_calibration_tail = calibration_samples_per_condition * target_random_frr
    if expected_calibration_tail < 20:
        quality_warnings.append(
            "fewer than 20 expected calibration observations per condition lie in the "
            "target lower tail"
        )
    for warning in quality_warnings:
        LOGGER.warning("Exploratory sample budget: %s", warning)

    null_tables = EmpiricalNullTables(null_table_samples, seed + 1)
    started = time.perf_counter()
    calibration_scores, calibration_conditions = _calibration_scores(
        calibration_samples_per_condition,
        batch_size,
        seed + 2,
        null_tables,
    )
    calibrated = _calibrate_subsets(
        calibration_scores,
        calibration_conditions,
        target_random_frr,
    )

    score_path = output_dir / "metric-scores.pq"
    counts = _evaluate_test_data(
        samples_per_strategy,
        batch_size,
        seed + 3,
        null_tables,
        calibrated,
        score_path,
    )
    timings = _benchmark_metrics(seed + 4, null_tables, benchmark_sample_count)
    subset_timings = _benchmark_subsets(seed + 4, null_tables, benchmark_sample_count)
    summary_rows, detail_rows = _summarize_subsets(
        calibrated,
        counts,
        timings,
        subset_timings,
        target_random_frr,
    )
    frontier = _pareto_frontier(summary_rows)

    subset_path = _write_csv(summary_rows, output_dir / "subset-results.csv")
    detail_path = _write_csv(detail_rows, output_dir / "subset-by-scenario.csv")
    pareto_path = _write_csv(frontier, output_dir / "pareto-frontier.csv")
    recommendation_path = _write_recommendations(
        frontier,
        target_random_frr,
        output_dir / "recommendations.txt",
    )
    heatmap_path = _plot_metric_heatmap(
        detail_rows,
        figure_dir / "metric-false-random-heatmap.pdf",
    )
    tradeoff_path = _plot_subset_tradeoff(
        summary_rows,
        frontier,
        figure_dir / "subset-pareto-tradeoff.pdf",
    )
    elapsed = time.perf_counter() - started
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "experiment_version": "1",
        "production_classifier_changed": False,
        "seed": seed,
        "samples_per_strategy_and_condition": samples_per_strategy,
        "calibration_samples_per_condition": calibration_samples_per_condition,
        "null_table_samples": null_table_samples,
        "target_random_false_rejection_rate": target_random_frr,
        "null_table_pvalue_resolution": null_resolution,
        "expected_calibration_tail_observations_per_condition": expected_calibration_tail,
        "quality_warnings": quality_warnings,
        "batch_size": batch_size,
        "elapsed_seconds": elapsed,
        "metric_order": list(METRIC_NAMES),
        "metric_descriptions": METRIC_DESCRIPTIONS,
        "evaluation_generators": list(EVALUATION_GENERATORS),
        "increment_uniformity": {
            "views": ["full", "destination-0", "destination-1", "connection-0..3"],
            "missing_value_policy": "originally adjacent present positions only",
            "minimum_transitions": MIN_INCREMENT_TRANSITIONS,
            "bin_rule": (
                "largest power of two <= floor(transitions/5), capped at 16; "
                "views below 10 transitions are uninformative"
            ),
            "view_combination": "minimum empirical right-tail p-value",
        },
        "gap_uniformity": {
            "statistic": "Cramer-von-Mises over PIT-transformed circular spacings",
            "includes_zero_gaps": True,
            "includes_wraparound_gap": True,
            "order_invariant": True,
            "null": "empirical IID discrete-uniform 16-bit table conditional on present count",
        },
        "conditions": [condition.__dict__ for condition in CONDITIONS],
        "metric_benchmark_ms_per_10000": timings,
        "subset_benchmark_ms_per_10000": {
            _subset_name(mask): value for mask, value in subset_timings.items()
        },
        "subset_count": len(summary_rows),
        "pareto_subset_count": len(frontier),
        "pareto_candidates": frontier,
        "artifacts": {
            "metric_scores": str(score_path),
            "subset_results": str(subset_path),
            "subset_by_scenario": str(detail_path),
            "pareto_frontier": str(pareto_path),
            "recommendations": str(recommendation_path),
            "metric_heatmap": str(heatmap_path),
            "subset_tradeoff": str(tradeoff_path),
        },
    }
    summary_path = _write_json(summary, output_dir / "summary.json")
    compact_paths = [
        summary_path,
        subset_path,
        detail_path,
        pareto_path,
        recommendation_path,
        heatmap_path,
        tradeoff_path,
    ]
    bundle_path = _create_review_bundle(
        compact_paths,
        output_dir / "random-classifier-review-bundle.zip",
    )
    LOGGER.info("Evaluation completed in %.1f seconds", elapsed)
    return {
        "summary": summary_path,
        "subset_results": subset_path,
        "subset_by_scenario": detail_path,
        "pareto_frontier": pareto_path,
        "recommendations": recommendation_path,
        "metric_scores": score_path,
        "metric_heatmap": heatmap_path,
        "subset_tradeoff": tradeoff_path,
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
    samples_per_strategy: int = typer.Option(
        DEFAULT_SAMPLES_PER_STRATEGY,
        min=1,
        help="held-out samples per strategy and impairment condition",
    ),
    calibration_samples_per_condition: int = typer.Option(
        DEFAULT_CALIBRATION_SAMPLES_PER_CONDITION,
        min=1,
        help="independent RANDOM samples per condition for subset thresholds",
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
        help="maximum calibration rejection rate for true RANDOM sequences",
    ),
    batch_size: int = typer.Option(DEFAULT_BATCH_SIZE, min=1),
    seed: int = typer.Option(42),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR),  # noqa: B008
    figure_dir: Path = typer.Option(DEFAULT_FIGURE_DIR),  # noqa: B008
) -> None:
    log_path = output_dir / "run.log"
    _configure_logging(log_path)
    outputs = evaluate_random_classifier_metrics(
        samples_per_strategy=samples_per_strategy,
        calibration_samples_per_condition=calibration_samples_per_condition,
        null_table_samples=null_table_samples,
        target_random_frr=target_random_frr,
        batch_size=batch_size,
        seed=seed,
        output_dir=output_dir,
        figure_dir=figure_dir,
    )
    # Add the completed log after all handlers have flushed the final message.
    for handler in logging.getLogger().handlers:
        handler.flush()
    bundle_inputs = [
        path for name, path in outputs.items() if name not in {"metric_scores", "review_bundle"}
    ]
    bundle_inputs.append(log_path)
    _create_review_bundle(bundle_inputs, outputs["review_bundle"])
    for name, path in outputs.items():
        typer.echo(f"{name}: {path}")
    typer.echo(f"run_log: {log_path}")


if __name__ == "__main__":
    app()
