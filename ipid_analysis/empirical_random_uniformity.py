"""Shared empirical null models for discrete 16-bit RANDOM sequences.

This module deliberately has no plotting or synthetic-validation dependencies.
Production and offline evaluation therefore use the same deterministic spacing
statistics and Monte Carlo tables without pulling paper tooling into the mass
classifier.
"""

from __future__ import annotations

import logging

import numpy as np

from ipid_analysis.strategies import MODULUS, RANDOM_STRUCTURE_MIN_TEST_SAMPLES

LOGGER = logging.getLogger(__name__)


def _stable_rng(seed: int, *parts: int) -> np.random.Generator:
    return np.random.default_rng(np.random.SeedSequence([seed, *parts]))


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
        # Beta(1, n-1). The PIT makes the marginal target Uniform(0, 1); the
        # dependence and 16-bit discreteness remain in the empirical null.
        transformed = 1.0 - np.power(
            np.clip(1.0 - spacings, 0.0, 1.0),
            sample_count - 1,
        )
        transformed.sort(axis=1)
        target = (2.0 * np.arange(1, sample_count + 1) - 1.0) / (2.0 * sample_count)
        result[rows] = 1.0 / (12.0 * sample_count) + np.square(
            transformed - target[None, :]
        ).sum(axis=1)
    return result


class EmpiricalNullTables:
    """Lazily generated deterministic null tables for discrete metrics."""

    def __init__(self, sample_count: int, seed: int, batch_size: int = 20_000):
        if sample_count < 1:
            raise ValueError("null-table sample count must be positive")
        self.sample_count = sample_count
        self.seed = seed
        self.batch_size = batch_size
        self._increment: dict[tuple[int, int], np.ndarray] = {}
        self._spacing: dict[int, np.ndarray] = {}

    def increment(self, transition_count: int, bin_count: int) -> np.ndarray:
        """Return the historical single-scale increment null table."""
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
        """Return the circular-spacing null table for one observed length."""
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


def gap_uniformity_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    null_tables: EmpiricalNullTables,
) -> np.ndarray:
    """Empirical compatibility p-value for the full circular gap distribution."""
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
