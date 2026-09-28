"""Shared paper generators for synthetic 4x25 Mass IP-ID sequences.

The module deliberately contains no statistical test.  It provides one stable
input population for the selected RANDOM-score figures and for independent
baseline methods such as the adapted NIST SP 800-22 comparison.
"""

from __future__ import annotations

import numpy as np

from ipid_analysis.classifier_validation import (
    CONNECTION_COUNT,
    FIXED_REQUESTS_PER_CONNECTION,
    generate_fixed_sequences,
)

REQUESTS_PER_CONNECTION = FIXED_REQUESTS_PER_CONNECTION
IDEAL_SEQUENCE_LENGTH = CONNECTION_COUNT * REQUESTS_PER_CONNECTION
PRESENT_SEQUENCE_LENGTH = 80
LOSS_FRACTION = 0.20
REORDER_FRACTION = 0.20
DEFAULT_SEED = 42
TRIVIAL_SAMPLES_PER_STRATEGY = 1_000
PLOT_STRATEGIES = (
    "REFLECTION",
    "CONSTANT",
    "SINGLE",
    "PER_DESTINATION",
    "PER_CONNECTION",
    "PER_BUCKET",
    "MULTI",
    "RANDOM",
)
TRIVIAL_STRATEGIES = frozenset({"REFLECTION", "CONSTANT"})


def generate_mass_paper_sequences(
    samples_per_strategy: int,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """Generate balanced, measurement-shaped sequences for paper figures.

    REFLECTION and CONSTANT are deterministic families for which 1,000 samples
    are sufficient.  The remaining strategy families use the requested sample
    budget.  All values come from the classifier-validation generators so that
    every paper method is evaluated on exactly the same strategy definitions.
    """
    if samples_per_strategy < 1:
        raise ValueError("samples_per_strategy must be positive")

    generated_count = max(samples_per_strategy, TRIVIAL_SAMPLES_PER_STRATEGY)
    generated = generate_fixed_sequences(generated_count, rng)
    return {
        strategy: generated[strategy][
            : (
                TRIVIAL_SAMPLES_PER_STRATEGY
                if strategy in TRIVIAL_STRATEGIES
                else samples_per_strategy
            )
        ]
        for strategy in PLOT_STRATEGIES
    }
