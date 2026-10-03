"""Final RANDOM classifier selected by the operating-point confirmation.

The module centralizes the exact score specification shared by production,
paper validation, and held-out diagnostics so those paths cannot silently
diverge.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING

import numpy as np

from ipid_analysis.empirical_random_uniformity import (
    EmpiricalNullTables,
    gap_uniformity_pvalues,
)
from ipid_analysis.multiscale_increment_uniformity import (
    MIN_INCREMENT_TRANSITIONS,
    MULTISCALE_INCREMENT_BINS,
    MULTISCALE_TARGET_EXPECTED_PER_BIN,
    MultiscaleIncrementNullTables,
    multiscale_increment_evidence_pvalues,
)
from ipid_analysis.strategies import random_structure_features

if TYPE_CHECKING:
    from ipid_analysis.strategies import MeasurementConfig

CANDIDATE_RANDOM_SCORE_VERSION = "raw-multiscale-increment-evidence-gap-min-v3"
CANDIDATE_RANDOM_METRICS = (
    "raw_uniformity",
    "increment_uniformity",
    "gap_uniformity",
)

# Selected by the seed-20260927 final operating-point confirmation using a
# target true-RANDOM false-rejection rate of 0.05%. This threshold is meaningful
# only for hierarchical increment-evidence aggregation and the null-table
# specification recorded alongside it.
CANDIDATE_RANDOM_MIN_SCORE = 7.89999176049605e-05
CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE = 0.0005
CANDIDATE_RANDOM_SELECTION = (
    "seed-20260927 final operating-point confirmation at 0.05% "
    "target true-RANDOM false-rejection"
)
CANDIDATE_NULL_TABLE_VERSION = "empirical-discrete-16bit-multiscale-v2"
CANDIDATE_NULL_TABLE_SAMPLES = 1_000_000
CANDIDATE_NULL_TABLE_SEED = 20_260_927
CANDIDATE_INCREMENT_NULL_TABLE_SEED_OFFSET = 11
CANDIDATE_GAP_NULL_TABLE_SEED_OFFSET = 23
CANDIDATE_INCREMENT_NULL_TABLE_SEED = (
    CANDIDATE_NULL_TABLE_SEED + CANDIDATE_INCREMENT_NULL_TABLE_SEED_OFFSET
)
CANDIDATE_GAP_NULL_TABLE_SEED = (
    CANDIDATE_NULL_TABLE_SEED + CANDIDATE_GAP_NULL_TABLE_SEED_OFFSET
)
CANDIDATE_EVALUATION_SEED = CANDIDATE_NULL_TABLE_SEED
CANDIDATE_INCREMENT_BIN_COUNTS = MULTISCALE_INCREMENT_BINS
CANDIDATE_INCREMENT_TARGET_EXPECTED_PER_BIN = MULTISCALE_TARGET_EXPECTED_PER_BIN
CANDIDATE_INCREMENT_MIN_TRANSITIONS = MIN_INCREMENT_TRANSITIONS
CANDIDATE_INCREMENT_SUBSEQUENCE_AGGREGATION = "hierarchical-fisher-disjoint-v1"


@dataclass(frozen=True)
class CandidateNullTables:
    """Separately seeded null tables for the two empirical components."""

    increment: MultiscaleIncrementNullTables
    gap: EmpiricalNullTables


def create_candidate_null_tables(
    sample_count: int = CANDIDATE_NULL_TABLE_SAMPLES,
    seed: int = CANDIDATE_NULL_TABLE_SEED,
) -> CandidateNullTables:
    """Build the exact null-table pair used by the selected candidate."""
    return CandidateNullTables(
        increment=MultiscaleIncrementNullTables(
            sample_count,
            seed + CANDIDATE_INCREMENT_NULL_TABLE_SEED_OFFSET,
        ),
        gap=EmpiricalNullTables(
            sample_count,
            seed + CANDIDATE_GAP_NULL_TABLE_SEED_OFFSET,
        ),
    )


@lru_cache(maxsize=1)
def production_candidate_null_tables() -> CandidateNullTables:
    """Return the process-wide production tables with the confirmed seeds.

    The table containers generate individual sample-count tables lazily.  A
    process-wide cache prevents rebuilding them for every streamed mass batch.
    """
    return create_candidate_null_tables()


@dataclass(frozen=True)
class CandidateRandomScoreComponents:
    """Candidate component p-values and their minimum score."""

    raw_uniformity: np.ndarray
    increment_uniformity: np.ndarray
    gap_uniformity: np.ndarray
    score: np.ndarray


def candidate_random_score_components(
    values: np.ndarray,
    present: np.ndarray,
    null_tables: CandidateNullTables,
    config: MeasurementConfig | None = None,
) -> CandidateRandomScoreComponents:
    """Calculate the final score components for one measurement shape."""
    # Historical validation callers omit the shape because all confirmation
    # data use the fixed 4x25 layout. Production always passes its snapshot
    # configuration explicitly and therefore has no validation dependency.
    if config is None:
        from ipid_analysis.classifier_validation import FIXED_CONFIG

        config = FIXED_CONFIG
    if config.connection_count != 4 or config.requests_per_connection != 25:
        raise ValueError(
            "final RANDOM score is calibrated only for 4x25 fixed-interval mass sequences"
        )
    raw = random_structure_features(values, present).uniformity_pvalue
    increment = multiscale_increment_evidence_pvalues(
        values,
        present,
        config,
        null_tables.increment,
        bin_counts=CANDIDATE_INCREMENT_BIN_COUNTS,
        target_expected_per_bin=CANDIDATE_INCREMENT_TARGET_EXPECTED_PER_BIN,
    )
    gap = gap_uniformity_pvalues(values, present, null_tables.gap)
    score = np.clip(np.minimum.reduce([raw, increment, gap]), 0.0, 1.0)
    return CandidateRandomScoreComponents(
        raw_uniformity=raw,
        increment_uniformity=increment,
        gap_uniformity=gap,
        score=score,
    )


def candidate_random_scores(
    values: np.ndarray,
    present: np.ndarray,
    null_tables: CandidateNullTables,
    config: MeasurementConfig | None = None,
) -> np.ndarray:
    """Return only the final minimum RANDOM-compatibility score."""
    return candidate_random_score_components(
        values,
        present,
        null_tables,
        config,
    ).score
