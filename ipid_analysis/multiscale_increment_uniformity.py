"""Selected multiscale increment-uniformity test for mass measurements.

The test evaluates the full sequence, both destination subsequences, and all
connection subsequences.  Within each view it combines every usable resolution
from 3/4/8/16 bins and calibrates that minimum against a joint empirical null
distribution. Missing observations never create artificial transitions.

The final production score combines the two disjoint destination views
and the four disjoint connection views with Fisher's method. It then takes the
minimum of the full-sequence evidence and those two group-level values. The
final production threshold calibrates that overlapping three-way minimum.
"""

from __future__ import annotations

import logging
import math

import numpy as np
from scipy.special import gammaincc

from ipid_analysis.strategies import MODULUS, MeasurementConfig

LOGGER = logging.getLogger(__name__)

MULTISCALE_INCREMENT_BINS = (3, 4, 8, 16)
MULTISCALE_TARGET_EXPECTED_PER_BIN = 2
MIN_INCREMENT_TRANSITIONS = 10


def _stable_rng(seed: int, *parts: int) -> np.random.Generator:
    return np.random.default_rng(np.random.SeedSequence([seed, *parts]))


def _right_tail_pvalues(observed: np.ndarray, sorted_null: np.ndarray) -> np.ndarray:
    """Conservative empirical P(T >= observed), with add-one correction."""
    left = np.searchsorted(sorted_null, observed, side="left")
    return (len(sorted_null) - left + 1.0) / (len(sorted_null) + 1.0)


def _left_tail_pvalues(observed: np.ndarray, sorted_null: np.ndarray) -> np.ndarray:
    """Conservative empirical P(T <= observed), with add-one correction."""
    right = np.searchsorted(sorted_null, observed, side="right")
    return (right + 1.0) / (len(sorted_null) + 1.0)


def discrete_bin_probabilities(bin_count: int) -> np.ndarray:
    """Exact bin probabilities for floor(delta*k/65536)."""
    boundaries = (np.arange(bin_count + 1, dtype=np.int64) * MODULUS + bin_count - 1) // bin_count
    return np.diff(boundaries).astype(float) / MODULUS


def pearson_statistics(counts: np.ndarray, probabilities: np.ndarray) -> np.ndarray:
    """Return Pearson statistics for rows of multinomial bin counts."""
    if np.all(probabilities == probabilities[0]):
        expected = counts.sum(axis=1) / len(probabilities)
        return np.square(counts - expected[:, None]).sum(axis=1) / expected
    expected = counts.sum(axis=1, keepdims=True) * probabilities[None, :]
    return (np.square(counts - expected) / expected).sum(axis=1)


class MultiscaleIncrementNullTables:
    """Empirical null tables for single resolutions and their joint minimum."""

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
            probabilities = discrete_bin_probabilities(bin_count)
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
            statistics = pearson_statistics(counts, probabilities)
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
            common_bin_count = math.lcm(*bin_counts)
            common_probabilities = discrete_bin_probabilities(common_bin_count)
            offset = 0
            batch_index = 0
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
                    statistics = pearson_statistics(
                        counts,
                        discrete_bin_probabilities(bin_count),
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


def selected_bin_counts(
    transition_count: int,
    *,
    bin_counts: tuple[int, ...] = MULTISCALE_INCREMENT_BINS,
    target_expected_per_bin: int = MULTISCALE_TARGET_EXPECTED_PER_BIN,
) -> tuple[int, ...]:
    """Return all usable resolutions for one increment view."""
    if transition_count < MIN_INCREMENT_TRANSITIONS:
        return ()
    maximum = transition_count // target_expected_per_bin
    return tuple(count for count in bin_counts if count <= maximum)


def _view_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    null_tables: MultiscaleIncrementNullTables,
    *,
    bin_counts: tuple[int, ...],
    target_expected_per_bin: int,
) -> tuple[np.ndarray, np.ndarray]:
    if values.shape[1] < 2:
        return np.ones(len(values), dtype=float), np.zeros(len(values), dtype=bool)
    pair_present = present[:, :-1] & present[:, 1:]
    transition_counts = pair_present.sum(axis=1).astype(np.int64)
    increments = (values[:, 1:].astype(np.int64) - values[:, :-1].astype(np.int64)) % MODULUS
    result = np.ones(len(values), dtype=float)
    valid = np.zeros(len(values), dtype=bool)

    for transition_count in np.unique(transition_counts):
        active_bin_counts = selected_bin_counts(
            int(transition_count),
            bin_counts=bin_counts,
            target_expected_per_bin=target_expected_per_bin,
        )
        if not active_bin_counts:
            continue
        rows = np.flatnonzero(transition_counts == transition_count)
        valid[rows] = True
        active = pair_present[rows]
        component_pvalues = []
        for bin_count in active_bin_counts:
            bins = (increments[rows] * bin_count) // MODULUS
            row_ids = np.broadcast_to(np.arange(len(rows))[:, None], bins.shape)
            flat = (row_ids * bin_count + bins)[active]
            counts = np.bincount(
                flat,
                minlength=len(rows) * bin_count,
            ).reshape(len(rows), bin_count)
            statistics = pearson_statistics(
                counts,
                discrete_bin_probabilities(bin_count),
            )
            component_pvalues.append(
                _right_tail_pvalues(
                    statistics,
                    null_tables.single(int(transition_count), bin_count),
                )
            )
        minimum = np.minimum.reduce(component_pvalues)
        if len(active_bin_counts) > 1:
            result[rows] = _left_tail_pvalues(
                minimum,
                null_tables.joint(int(transition_count), active_bin_counts),
            )
        else:
            result[rows] = minimum
    return result, valid


def multiscale_increment_uniformity_view_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    config: MeasurementConfig,
    null_tables: MultiscaleIncrementNullTables,
    *,
    bin_counts: tuple[int, ...] = MULTISCALE_INCREMENT_BINS,
    target_expected_per_bin: int = MULTISCALE_TARGET_EXPECTED_PER_BIN,
) -> tuple[np.ndarray, np.ndarray]:
    """Return p-values and validity for the seven stable increment views.

    Columns are ordered as ``full, dst0, dst1, con0, con1, con2, con3``.
    Views without a usable bin resolution retain the neutral p-value one and a
    false validity flag.
    """
    if values.shape[1] != config.sequence_length:
        raise ValueError(
            f"expected {config.sequence_length} fixed positions, got {values.shape[1]}"
        )

    def view(data: np.ndarray, mask: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        return _view_pvalues(
            data,
            mask,
            null_tables,
            bin_counts=bin_counts,
            target_expected_per_bin=target_expected_per_bin,
        )

    components = [view(values, present)]
    components.extend(view(values[:, offset::2], present[:, offset::2]) for offset in range(2))
    connection_values = values.reshape(
        len(values),
        config.requests_per_connection,
        config.connection_count,
    ).transpose(0, 2, 1)
    connection_present = present.reshape(
        len(values),
        config.requests_per_connection,
        config.connection_count,
    ).transpose(0, 2, 1)
    components.extend(
        view(connection_values[:, connection], connection_present[:, connection])
        for connection in range(config.connection_count)
    )
    return (
        np.column_stack([component[0] for component in components]),
        np.column_stack([component[1] for component in components]),
    )


def fisher_compatibility(pvalues: np.ndarray, valid: np.ndarray) -> np.ndarray:
    """Fisher-combine disjoint view p-values, excluding unavailable views."""
    clipped = np.clip(pvalues, np.finfo(float).tiny, 1.0)
    active = valid.sum(axis=1)
    log_sum = np.where(valid, np.log(clipped), 0.0).sum(axis=1)
    result = np.ones(len(pvalues), dtype=float)
    rows = active > 0
    result[rows] = gammaincc(active[rows], -log_sum[rows])
    return result


def aggregate_increment_view_evidence(
    pvalues: np.ndarray,
    valid: np.ndarray,
) -> np.ndarray:
    """Combine full, destination, and connection increment evidence.

    Destination and connection p-values are combined only inside their
    respective disjoint groups. The final minimum contains overlapping views
    and therefore receives no standalone p-value interpretation; the complete
    RANDOM-candidate threshold is calibrated for this exact score.
    """
    if pvalues.ndim != 2 or pvalues.shape[1] != 7:
        raise ValueError("expected seven increment-view p-value columns")
    if valid.shape != pvalues.shape:
        raise ValueError("validity mask must match increment-view p-values")
    full = np.where(valid[:, 0], pvalues[:, 0], 1.0)
    destinations = fisher_compatibility(pvalues[:, 1:3], valid[:, 1:3])
    connections = fisher_compatibility(pvalues[:, 3:7], valid[:, 3:7])
    return np.minimum.reduce((full, destinations, connections))


def multiscale_increment_uniformity_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    config: MeasurementConfig,
    null_tables: MultiscaleIncrementNullTables,
    *,
    bin_counts: tuple[int, ...] = MULTISCALE_INCREMENT_BINS,
    target_expected_per_bin: int = MULTISCALE_TARGET_EXPECTED_PER_BIN,
) -> np.ndarray:
    """Return the minimum calibrated p-value across all seven increment views."""
    pvalues, _ = multiscale_increment_uniformity_view_pvalues(
        values,
        present,
        config,
        null_tables,
        bin_counts=bin_counts,
        target_expected_per_bin=target_expected_per_bin,
    )
    return pvalues.min(axis=1)


def multiscale_increment_evidence_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    config: MeasurementConfig,
    null_tables: MultiscaleIncrementNullTables,
    *,
    bin_counts: tuple[int, ...] = MULTISCALE_INCREMENT_BINS,
    target_expected_per_bin: int = MULTISCALE_TARGET_EXPECTED_PER_BIN,
) -> np.ndarray:
    """Return the selected hierarchical increment-evidence compatibility."""
    pvalues, valid = multiscale_increment_uniformity_view_pvalues(
        values,
        present,
        config,
        null_tables,
        bin_counts=bin_counts,
        target_expected_per_bin=target_expected_per_bin,
    )
    return aggregate_increment_view_evidence(pvalues, valid)
