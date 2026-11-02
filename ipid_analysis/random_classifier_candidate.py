"""Production RANDOM score: minimum increment and circular-gap compatibility.

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
from ipid_analysis.increment_uniformity import (
    MAX_BINS,
    MIN_BINS,
    MIN_EXPECTED_PER_BIN,
    MIN_TRANSITIONS,
    increment_uniformity_pvalues,
)
from ipid_analysis.strategies import (
    RANDOM_STRUCTURE_CALIBRATION_REPORT,
    RANDOM_STRUCTURE_MIN_SCORE,
    RANDOM_STRUCTURE_SCORE_VERSION,
    RANDOM_STRUCTURE_THRESHOLD_STATUS,
)

if TYPE_CHECKING:
    from ipid_analysis.strategies import MeasurementConfig

CANDIDATE_RANDOM_SCORE_VERSION = RANDOM_STRUCTURE_SCORE_VERSION
CANDIDATE_RANDOM_METRICS = (
    "increment_uniformity",
    "gap_uniformity",
)

CANDIDATE_RANDOM_MIN_SCORE = RANDOM_STRUCTURE_MIN_SCORE
CANDIDATE_RANDOM_THRESHOLD_STATUS = RANDOM_STRUCTURE_THRESHOLD_STATUS
CANDIDATE_RANDOM_CALIBRATION_REPORT = RANDOM_STRUCTURE_CALIBRATION_REPORT
CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE = 0.0005
CANDIDATE_RANDOM_SELECTION = (
    "seed-20261102 production-score confirmation at 0.05% target true-RANDOM false-rejection"
)
CANDIDATE_NULL_TABLE_VERSION = "empirical-discrete-16bit-circular-cvm-v1"
CANDIDATE_NULL_TABLE_SAMPLES = 1_000_000
CANDIDATE_NULL_TABLE_SEED = 20_260_927
# Historical offline view experiments still import these seed constants.
# Production creates no increment null table.
CANDIDATE_INCREMENT_NULL_TABLE_SEED_OFFSET = 11
CANDIDATE_GAP_NULL_TABLE_SEED_OFFSET = 23
CANDIDATE_INCREMENT_NULL_TABLE_SEED = (
    CANDIDATE_NULL_TABLE_SEED + CANDIDATE_INCREMENT_NULL_TABLE_SEED_OFFSET
)
CANDIDATE_GAP_NULL_TABLE_SEED = CANDIDATE_NULL_TABLE_SEED + CANDIDATE_GAP_NULL_TABLE_SEED_OFFSET
CANDIDATE_EVALUATION_SEED = CANDIDATE_NULL_TABLE_SEED
CANDIDATE_INCREMENT_BIN_COUNTS = tuple(range(MIN_BINS, MAX_BINS + 1))
CANDIDATE_INCREMENT_TARGET_EXPECTED_PER_BIN = MIN_EXPECTED_PER_BIN
CANDIDATE_INCREMENT_MIN_TRANSITIONS = MIN_TRANSITIONS
CANDIDATE_INCREMENT_SUBSEQUENCE_AGGREGATION = "minimum-available-views"


def candidate_random_threshold_status(
    threshold: float, null_table_samples: int, null_table_seed: int
) -> str:
    """Apply the calibration status only to the matching production specification."""
    if (threshold, null_table_samples, null_table_seed) == (
        CANDIDATE_RANDOM_MIN_SCORE,
        CANDIDATE_NULL_TABLE_SAMPLES,
        CANDIDATE_NULL_TABLE_SEED,
    ):
        return CANDIDATE_RANDOM_THRESHOLD_STATUS
    return "custom-uncalibrated"


@dataclass(frozen=True)
class CandidateNullTables:
    """Only the unchanged circular-gap component needs a null table."""

    gap: EmpiricalNullTables


def create_candidate_null_tables(
    sample_count: int = CANDIDATE_NULL_TABLE_SAMPLES,
    seed: int = CANDIDATE_NULL_TABLE_SEED,
) -> CandidateNullTables:
    """Build the unchanged, separately seeded circular-gap null tables."""
    return CandidateNullTables(
        gap=EmpiricalNullTables(
            sample_count,
            seed + CANDIDATE_GAP_NULL_TABLE_SEED_OFFSET,
        ),
    )


@lru_cache(maxsize=1)
def production_candidate_null_tables() -> CandidateNullTables:
    """Return process-wide gap tables with the existing seed.

    The table containers generate individual sample-count tables lazily.  A
    process-wide cache prevents rebuilding them for every streamed mass batch.
    """
    return create_candidate_null_tables()


@dataclass(frozen=True)
class CandidateRandomScoreComponents:
    """Candidate component p-values and their minimum score."""

    increment_uniformity: np.ndarray
    gap_uniformity: np.ndarray
    score: np.ndarray


def candidate_random_score_components(
    values: np.ndarray,
    present: np.ndarray,
    null_tables: CandidateNullTables,
    config: MeasurementConfig | None = None,
) -> CandidateRandomScoreComponents:
    """Calculate S=min(p_inc, p_gap) independently of the decision threshold."""
    # Historical validation callers omit the shape because all confirmation
    # data use the fixed 4x25 layout. Production always passes its snapshot
    # configuration explicitly and therefore has no validation dependency.
    if config is None:
        from ipid_analysis.classifier_validation import FIXED_CONFIG

        config = FIXED_CONFIG
    if config.connection_count != 4 or config.requests_per_connection != 25:
        raise ValueError("RANDOM score supports only 4x25 fixed-interval mass sequences")
    increment = increment_uniformity_pvalues(values, present, config)
    gap = gap_uniformity_pvalues(values, present, null_tables.gap)
    score = np.clip(np.minimum(increment, gap), 0.0, 1.0)
    return CandidateRandomScoreComponents(
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
