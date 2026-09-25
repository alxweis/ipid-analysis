"""Validation-only RANDOM classifier candidate selected by evaluation v2.

The module centralizes the exact candidate specification so paper validation
and a later production implementation cannot silently diverge.  Importing it
does not modify the production classifier in :mod:`ipid_analysis.strategies`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from ipid_analysis.strategies import random_structure_features

if TYPE_CHECKING:
    from ipid_analysis.random_classifier_evaluation import EmpiricalNullTables

CANDIDATE_RANDOM_SCORE_VERSION = "raw-increment-gap-min-v1"
CANDIDATE_RANDOM_METRICS = (
    "raw_uniformity",
    "increment_uniformity",
    "gap_uniformity",
)

# Selected by the independent held-out v2 evaluation using a target true-RANDOM
# false-rejection rate of 0.01%.  This threshold is meaningful only with the
# null-table specification recorded alongside it.
CANDIDATE_RANDOM_MIN_SCORE = 8.999991223392587e-06
CANDIDATE_RANDOM_TARGET_FALSE_REJECTION_RATE = 0.0001
CANDIDATE_NULL_TABLE_VERSION = "empirical-discrete-16bit-v1"
CANDIDATE_NULL_TABLE_SAMPLES = 1_000_000
CANDIDATE_NULL_TABLE_SEED = 20_260_926
CANDIDATE_EVALUATION_SEED = 20_260_925


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
    null_tables: EmpiricalNullTables,
) -> CandidateRandomScoreComponents:
    """Calculate the validation candidate without changing production state."""
    # Imported lazily because the offline evaluator reuses the established
    # classifier-validation generators. Keeping that dependency out of module
    # initialization lets the established validator import this candidate.
    from ipid_analysis.random_classifier_evaluation import (
        gap_uniformity_pvalues,
        increment_uniformity_pvalues,
    )

    raw = random_structure_features(values, present).uniformity_pvalue
    increment = increment_uniformity_pvalues(values, present, null_tables)
    gap = gap_uniformity_pvalues(values, present, null_tables)
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
    null_tables: EmpiricalNullTables,
) -> np.ndarray:
    """Return only the validation candidate's minimum compatibility score."""
    return candidate_random_score_components(values, present, null_tables).score
