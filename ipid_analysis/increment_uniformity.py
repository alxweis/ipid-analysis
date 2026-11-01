"""Single-resolution Pearson tests for the seven mass increment views.

Each view uses k = min(16, floor(m / 5)) bins for m observed transitions.
Views with fewer than three bins are unavailable and contribute a neutral one
to the minimum. Missing replies never create transitions across missing slots.
"""

from __future__ import annotations

import numpy as np
from scipy.special import gammaincc

from ipid_analysis.strategies import MODULUS, MeasurementConfig

MIN_EXPECTED_PER_BIN = 5
MIN_BINS = 3
MAX_BINS = 16
MIN_TRANSITIONS = MIN_EXPECTED_PER_BIN * MIN_BINS
VIEW_NAMES = ("full", "dst1", "dst2", "con1", "con2", "con3", "con4")


def _view_pvalues(values: np.ndarray, present: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    pairs = present[:, :-1] & present[:, 1:]
    transitions = pairs.sum(axis=1)
    bin_counts = np.minimum(MAX_BINS, transitions // MIN_EXPECTED_PER_BIN)
    valid = bin_counts >= MIN_BINS
    result = np.ones(len(values), dtype=float)
    increments = (values[:, 1:].astype(np.int64) - values[:, :-1].astype(np.int64)) % MODULUS

    for bin_count in np.unique(bin_counts[valid]):
        rows = np.flatnonzero(valid & (bin_counts == bin_count))
        bins = increments[rows] * bin_count // MODULUS
        row_ids = np.broadcast_to(np.arange(len(rows))[:, None], bins.shape)
        counts = np.bincount(
            (row_ids * bin_count + bins)[pairs[rows]],
            minlength=len(rows) * bin_count,
        ).reshape(len(rows), bin_count)
        # floor(delta*k/65536) can have unequal bin widths, notably for k=3.
        boundaries = (np.arange(bin_count + 1) * MODULUS + bin_count - 1) // bin_count
        probabilities = np.diff(boundaries) / MODULUS
        expected = transitions[rows, None] * probabilities[None, :]
        statistic = (np.square(counts - expected) / expected).sum(axis=1)
        result[rows] = gammaincc((bin_count - 1) / 2.0, statistic / 2.0)
    return result, valid


def increment_uniformity_view_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    config: MeasurementConfig,
) -> tuple[np.ndarray, np.ndarray]:
    """Return p-values and availability in VIEW_NAMES order for a 4x25 layout."""
    if config.connection_count != 4 or config.requests_per_connection != 25:
        raise ValueError("RANDOM increment tests require 4x25 mass sequences")
    if values.ndim != 2 or values.shape[1] != config.sequence_length:
        raise ValueError(f"expected {config.sequence_length} fixed measurement positions")
    if present.shape != values.shape:
        raise ValueError("presence mask must match the measurement positions")

    views = [_view_pvalues(values, present)]
    views.extend(_view_pvalues(values[:, i::2], present[:, i::2]) for i in range(2))
    views.extend(_view_pvalues(values[:, i::4], present[:, i::4]) for i in range(4))
    return (
        np.column_stack([view[0] for view in views]),
        np.column_stack([view[1] for view in views]),
    )


def increment_uniformity_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    config: MeasurementConfig,
) -> np.ndarray:
    """Minimum available view p-value; overlapping tests are not independent."""
    pvalues, _ = increment_uniformity_view_pvalues(values, present, config)
    return pvalues.min(axis=1)
