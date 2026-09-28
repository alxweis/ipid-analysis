"""Adapted NIST SP 800-22 baseline for short IP-ID-derived bitstreams.

This is a comparison baseline, not a claim of NIST validation.  Every present
IP-ID is encoded as an unsigned 16-bit big-endian word in measurement order.
Only tests whose published input-size recommendations are met by both the
1,600-bit complete stream and the 1,280-bit 20%-loss stream are included.
The dependent component minimum is calibrated against an empirical IID-bit
null distribution for the exact stream length.
"""

from __future__ import annotations

from functools import cache
import logging
import math

import numpy as np
from scipy.special import erfc, gammaincc, ndtr

LOGGER = logging.getLogger(__name__)

NIST_BASELINE_VERSION = "adapted-nist-sp800-22r1a-short-ipid-v1"
NIST_PUBLICATION = "NIST SP 800-22 Rev. 1a (2010)"
NIST_PUBLICATION_URL = "https://doi.org/10.6028/NIST.SP.800-22r1a"
NIST_BLOCK_SIZE = 32
NIST_SERIAL_PATTERN_LENGTH = 4
NIST_APPROXIMATE_ENTROPY_PATTERN_LENGTH = 4
NIST_TEST_NAMES = (
    "frequency",
    "block_frequency_m32",
    "runs",
    "longest_run_m8",
    "spectral_dft",
    "serial_m4_p1",
    "serial_m4_p2",
    "approximate_entropy_m4",
    "cumulative_sums_forward",
    "cumulative_sums_reverse",
)
NIST_TEST_LABELS = {
    "frequency": "Frequency",
    "block_frequency_m32": "Block Frequency",
    "runs": "Runs",
    "longest_run_m8": "Longest Run",
    "spectral_dft": "Spectral DFT",
    "serial_m4_p1": "Serial p1",
    "serial_m4_p2": "Serial p2",
    "approximate_entropy_m4": "Approx. Entropy",
    "cumulative_sums_forward": "Cusum Forward",
    "cumulative_sums_reverse": "Cusum Reverse",
}
NIST_EXCLUDED_TESTS = {
    "binary_matrix_rank": "requires at least 38,912 bits for 32x32 matrices",
    "non_overlapping_template": "recommended parameters target much longer streams",
    "overlapping_template": "published parameterization requires about 1,000,000 bits",
    "maurer_universal": "requires at least 387,840 bits",
    "linear_complexity": "requires at least 1,000,000 bits and 200 blocks",
    "random_excursions": "requires at least 1,000,000 bits",
    "random_excursions_variant": "requires at least 1,000,000 bits",
}


def ipids_to_bitstreams(values: np.ndarray, present: np.ndarray) -> np.ndarray:
    """Encode present IP-IDs as big-endian 16-bit words in measurement order."""
    if values.shape != present.shape or values.ndim != 2:
        raise ValueError("values and present must be equally shaped two-dimensional arrays")
    counts = present.sum(axis=1)
    if len(counts) and np.any(counts != counts[0]):
        raise ValueError("all rows must contain the same number of present IP-IDs")
    present_count = int(counts[0]) if len(counts) else 0
    if present_count == 0:
        return np.empty((len(values), 0), dtype=np.uint8)
    selected = values[present].reshape(len(values), present_count).astype(np.uint16, copy=False)
    octets = np.empty((len(values), present_count, 2), dtype=np.uint8)
    octets[:, :, 0] = selected >> 8
    octets[:, :, 1] = selected & 0xFF
    return np.unpackbits(octets.reshape(len(values), -1), axis=1, bitorder="big")


def _frequency(bits: np.ndarray) -> np.ndarray:
    n = bits.shape[1]
    sums = (2 * bits.astype(np.int32) - 1).sum(axis=1)
    return erfc(np.abs(sums) / math.sqrt(2.0 * n))


def _block_frequency(bits: np.ndarray, block_size: int = NIST_BLOCK_SIZE) -> np.ndarray:
    n = bits.shape[1]
    block_count = n // block_size
    if n < 100 or block_size < 20 or block_size <= 0.01 * n or block_count >= 100:
        raise ValueError("block-frequency parameters violate SP 800-22 recommendations")
    blocks = bits[:, : block_count * block_size].reshape(len(bits), block_count, block_size)
    proportions = blocks.mean(axis=2)
    chi_square = 4.0 * block_size * np.square(proportions - 0.5).sum(axis=1)
    return gammaincc(block_count / 2.0, chi_square / 2.0)


def _runs(bits: np.ndarray) -> np.ndarray:
    n = bits.shape[1]
    proportions = bits.mean(axis=1)
    prerequisite = np.abs(proportions - 0.5) < (2.0 / math.sqrt(n))
    runs = 1 + np.count_nonzero(bits[:, 1:] != bits[:, :-1], axis=1)
    expected = 2.0 * n * proportions * (1.0 - proportions)
    denominator = 2.0 * math.sqrt(2.0 * n) * proportions * (1.0 - proportions)
    result = np.zeros(len(bits), dtype=float)
    valid = prerequisite & (denominator > 0)
    result[valid] = erfc(np.abs(runs[valid] - expected[valid]) / denominator[valid])
    return result


def _longest_run(bits: np.ndarray) -> np.ndarray:
    n = bits.shape[1]
    if n < 128:
        raise ValueError("longest-run test requires at least 128 bits")
    block_size = 8
    block_count = n // block_size
    blocks = bits[:, : block_count * block_size].reshape(len(bits), block_count, block_size)
    current = np.zeros((len(bits), block_count), dtype=np.int16)
    longest = np.zeros_like(current)
    for offset in range(block_size):
        current = np.where(blocks[:, :, offset] == 1, current + 1, 0)
        longest = np.maximum(longest, current)
    counts = np.stack(
        [
            np.count_nonzero(longest <= 1, axis=1),
            np.count_nonzero(longest == 2, axis=1),
            np.count_nonzero(longest == 3, axis=1),
            np.count_nonzero(longest >= 4, axis=1),
        ],
        axis=1,
    )
    probabilities = np.asarray([0.2148, 0.3672, 0.2305, 0.1875])
    expected = block_count * probabilities
    chi_square = (np.square(counts - expected[None, :]) / expected[None, :]).sum(axis=1)
    return gammaincc(3.0 / 2.0, chi_square / 2.0)


def _spectral_dft(bits: np.ndarray) -> np.ndarray:
    n = bits.shape[1]
    if n < 1_000:
        raise ValueError("spectral DFT test requires at least 1,000 bits")
    signs = 2.0 * bits.astype(float) - 1.0
    magnitudes = np.abs(np.fft.fft(signs, axis=1))[:, : n // 2]
    threshold = math.sqrt(math.log(1.0 / 0.05) * n)
    observed_below = np.count_nonzero(magnitudes < threshold, axis=1)
    expected_below = 0.95 * n / 2.0
    normalized = (observed_below - expected_below) / math.sqrt(n * 0.95 * 0.05 / 4.0)
    return erfc(np.abs(normalized) / math.sqrt(2.0))


def _pattern_counts(bits: np.ndarray, pattern_length: int) -> np.ndarray:
    codes = np.zeros(bits.shape, dtype=np.int32)
    for offset in range(pattern_length):
        codes = (codes << 1) | np.roll(bits, -offset, axis=1)
    pattern_count = 1 << pattern_length
    row_ids = np.broadcast_to(np.arange(len(bits), dtype=np.int64)[:, None], bits.shape)
    flat = row_ids * pattern_count + codes
    return np.bincount(
        flat.ravel(),
        minlength=len(bits) * pattern_count,
    ).reshape(len(bits), pattern_count)


def _psi2(bits: np.ndarray, pattern_length: int) -> np.ndarray:
    if pattern_length == 0:
        return np.zeros(len(bits), dtype=float)
    counts = _pattern_counts(bits, pattern_length)
    n = bits.shape[1]
    return (1 << pattern_length) * np.square(counts).sum(axis=1) / n - n


def _serial(
    bits: np.ndarray, pattern_length: int = NIST_SERIAL_PATTERN_LENGTH
) -> tuple[np.ndarray, np.ndarray]:
    n = bits.shape[1]
    if pattern_length >= math.floor(math.log2(n)) - 2:
        raise ValueError("serial-test pattern length violates SP 800-22 recommendation")
    psi_m = _psi2(bits, pattern_length)
    psi_m1 = _psi2(bits, pattern_length - 1)
    psi_m2 = _psi2(bits, pattern_length - 2)
    delta1 = psi_m - psi_m1
    delta2 = psi_m - 2.0 * psi_m1 + psi_m2
    return (
        gammaincc(2 ** (pattern_length - 2), delta1 / 2.0),
        gammaincc(2 ** (pattern_length - 3), delta2 / 2.0),
    )


def _phi(bits: np.ndarray, pattern_length: int) -> np.ndarray:
    counts = _pattern_counts(bits, pattern_length).astype(float)
    probabilities = counts / bits.shape[1]
    logarithms = np.zeros_like(probabilities)
    np.log(probabilities, out=logarithms, where=probabilities > 0)
    return (probabilities * logarithms).sum(axis=1)


def _approximate_entropy(
    bits: np.ndarray,
    pattern_length: int = NIST_APPROXIMATE_ENTROPY_PATTERN_LENGTH,
) -> np.ndarray:
    n = bits.shape[1]
    if pattern_length >= math.floor(math.log2(n)) - 5:
        raise ValueError("approximate-entropy pattern length violates SP 800-22 recommendation")
    approximate_entropy = _phi(bits, pattern_length) - _phi(bits, pattern_length + 1)
    chi_square = 2.0 * n * (math.log(2.0) - approximate_entropy)
    return gammaincc(2 ** (pattern_length - 1), chi_square / 2.0)


@cache
def _cusum_pvalue_table(bit_length: int) -> np.ndarray:
    root_n = math.sqrt(bit_length)
    table = np.ones(bit_length + 1, dtype=float)
    for maximum in range(1, bit_length + 1):
        first = 0.0
        lower = math.floor((-bit_length / maximum + 1.0) / 4.0)
        upper = math.floor((bit_length / maximum - 1.0) / 4.0)
        for k in range(lower, upper + 1):
            first += ndtr((4 * k + 1) * maximum / root_n) - ndtr((4 * k - 1) * maximum / root_n)
        second = 0.0
        lower = math.floor((-bit_length / maximum - 3.0) / 4.0)
        upper = math.floor((bit_length / maximum - 1.0) / 4.0)
        for k in range(lower, upper + 1):
            second += ndtr((4 * k + 3) * maximum / root_n) - ndtr((4 * k + 1) * maximum / root_n)
        table[maximum] = np.clip(1.0 - first + second, 0.0, 1.0)
    return table


def _cumulative_sums(bits: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    signs = 2 * bits.astype(np.int32) - 1
    forward_maximum = np.abs(np.cumsum(signs, axis=1)).max(axis=1)
    reverse_maximum = np.abs(np.cumsum(signs[:, ::-1], axis=1)).max(axis=1)
    table = _cusum_pvalue_table(bits.shape[1])
    return table[forward_maximum], table[reverse_maximum]


def nist_component_pvalues(bits: np.ndarray) -> dict[str, np.ndarray]:
    """Return all applicable SP 800-22 component p-values for each row."""
    if bits.ndim != 2 or bits.shape[1] < 1_000:
        raise ValueError("short-sequence baseline requires a rectangular stream of >=1000 bits")
    serial_p1, serial_p2 = _serial(bits)
    cusum_forward, cusum_reverse = _cumulative_sums(bits)
    components = {
        "frequency": _frequency(bits),
        "block_frequency_m32": _block_frequency(bits),
        "runs": _runs(bits),
        "longest_run_m8": _longest_run(bits),
        "spectral_dft": _spectral_dft(bits),
        "serial_m4_p1": serial_p1,
        "serial_m4_p2": serial_p2,
        "approximate_entropy_m4": _approximate_entropy(bits),
        "cumulative_sums_forward": cusum_forward,
        "cumulative_sums_reverse": cusum_reverse,
    }
    return {name: np.clip(components[name], 0.0, 1.0) for name in NIST_TEST_NAMES}


def nist_minimum_pvalues(bits: np.ndarray) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    components = nist_component_pvalues(bits)
    return np.minimum.reduce([components[name] for name in NIST_TEST_NAMES]), components


class ShortNistNullTables:
    """Lazily calibrate the dependent minimum for each exact bit length."""

    def __init__(self, sample_count: int, seed: int, batch_size: int = 2_000):
        if min(sample_count, batch_size) < 1:
            raise ValueError("sample_count and batch_size must be positive")
        self.sample_count = sample_count
        self.seed = seed
        self.batch_size = batch_size
        self._minimum: dict[int, np.ndarray] = {}

    def minimum(self, bit_length: int) -> np.ndarray:
        if bit_length not in self._minimum:
            LOGGER.info(
                "Generating adapted-NIST null table n=%d (%d samples)",
                bit_length,
                self.sample_count,
            )
            values = np.empty(self.sample_count, dtype=float)
            offset = 0
            rng = np.random.default_rng(np.random.SeedSequence([self.seed, bit_length]))
            while offset < self.sample_count:
                size = min(self.batch_size, self.sample_count - offset)
                bits = rng.integers(0, 2, size=(size, bit_length), dtype=np.uint8)
                minimum, _ = nist_minimum_pvalues(bits)
                values[offset : offset + size] = minimum
                offset += size
            values.sort()
            self._minimum[bit_length] = values
        return self._minimum[bit_length]

    def combined_pvalues(self, bits: np.ndarray) -> tuple[np.ndarray, dict[str, np.ndarray]]:
        minimum, components = nist_minimum_pvalues(bits)
        null = self.minimum(bits.shape[1])
        right = np.searchsorted(null, minimum, side="right")
        combined = (right + 1.0) / (len(null) + 1.0)
        return combined, components
