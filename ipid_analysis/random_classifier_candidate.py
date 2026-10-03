"""Validation-only RANDOM classifier selected by final operating-point confirmation.

The module centralizes the exact candidate specification so paper validation
and a later production implementation cannot silently diverge.  Importing it
does not modify the production classifier in :mod:`ipid_analysis.strategies`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from ipid_analysis.multiscale_increment_uniformity import (
    MIN_INCREMENT_TRANSITIONS,
    MULTISCALE_INCREMENT_BINS,
    MULTISCALE_TARGET_EXPECTED_PER_BIN,
    MultiscaleIncrementNullTables,
    multiscale_increment_evidence_pvalues,
)
from ipid_analysis.strategies import random_structure_features

if TYPE_CHECKING:
    from ipid_analysis.random_classifier_evaluation import EmpiricalNullTables

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
    from ipid_analysis.random_classifier_evaluation import EmpiricalNullTables

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
) -> CandidateRandomScoreComponents:
    """Calculate the validation candidate without changing production state."""
    # Imported lazily because the offline evaluator reuses the established
    # classifier-validation generators. Keeping that dependency out of module
    # initialization lets the established validator import this candidate.
    from ipid_analysis.classifier_validation import FIXED_CONFIG
    from ipid_analysis.random_classifier_evaluation import (
        gap_uniformity_pvalues,
    )

    raw = random_structure_features(values, present).uniformity_pvalue
    increment = multiscale_increment_evidence_pvalues(
        values,
        present,
        FIXED_CONFIG,
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
) -> np.ndarray:
    """Return only the validation candidate's minimum compatibility score."""
    return candidate_random_score_components(values, present, null_tables).score
