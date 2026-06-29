#!/usr/bin/env python3
"""Map every IP_ADDR to an IPID selection strategy.

Designed for very large inputs (>100 GB, >300M rows). The core idea: every IPID
sequence has a fixed length of 16, so a batch of N rows is a dense (N, 16) uint16
matrix. All classifiers are vectorised over the whole batch instead of running
per row -- that turns billions of tiny numpy calls into a handful of large ones.

Pipeline:
  DuckDB scans the parquet, splits the comma string and casts to uint16 in C++
  (multithreaded, streaming) -> Arrow record batches -> dense (N, 16) matrix ->
  vectorised classifiers -> dictionary-encoded output column -> parquet.

Only IP_ADDR and IPID_SEQUENCE are read; the timestamp columns are not used by
any classifier and are therefore never touched (large I/O saving).
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from enum import IntEnum
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

try:
    from scipy.stats import chi2 as _chi2_dist  # only needed for is_random
except Exception:  # pragma: no cover
    _chi2_dist = None

# --- domain constants -----------------------------------------------------
MIN_STEPS_BEFORE_WRAPAROUND = 3
MAX_INC = math.ceil(65536 / MIN_STEPS_BEFORE_WRAPAROUND) - 1  # 21845

CONNECTION_COUNT = 4
REQUESTS_PER_CONNECTION = 4
SEQUENCE_LENGTH = CONNECTION_COUNT * REQUESTS_PER_CONNECTION  # 16
REQUEST_IP_IDS = np.array([18933, 18932, 3717, 3718, 3719], dtype=np.int64)

MULTI_MAX_INC = 800
MULTI_MAX_CLUSTERS = 16
RANDOM_MIN_CHI2_VALUE = 1e-9
RANDOM_CHI2_BINS = 8  # bins for the uniformity test (see classify_batch notes)


class Strategy(IntEnum):
    """Priority order == evaluation order. The integer value is the output code."""

    REFLECTION = 0
    CONSTANT = 1
    PER_DESTINATION = 2
    PER_CONNECTION = 3
    SINGLE = 4
    PER_BUCKET = 5
    MULTI = 6
    RANDOM = 7
    UNCLASSIFIED = 8


# Categories for the dictionary-encoded output column; index == Strategy value.
_CATEGORIES = pa.array([s.name for s in sorted(Strategy, key=int)], type=pa.string())
_STRATEGY_DTYPE = pa.dictionary(pa.int8(), pa.string())
OUTPUT_SCHEMA = pa.schema(
    [("IP_ADDR", pa.string()), ("IPID_SELECTION_STRATEGY", _STRATEGY_DTYPE)]
)

# Reflection reference pattern, expanded to the full sequence length once.
_REFLECTION_PATTERN = REQUEST_IP_IDS[np.arange(SEQUENCE_LENGTH) % REQUEST_IP_IDS.size]


# --------------------------------------------------------------------------
# Vectorised classifiers. Everything operates on the (N, 16) matrix and derived
# arrays, so there is no Python-level per-row work.
# --------------------------------------------------------------------------
def _chi2_uniform_p(inc: np.ndarray, n_bins: int) -> np.ndarray:
    """Per-row p-value of a chi-square uniformity test on `inc` (N, L) over the
    16-bit range. Returns (N,). Reference implementation -- see classify_batch."""
    if _chi2_dist is None:
        return np.ones(inc.shape[0])  # scipy missing -> never reject
    n, length = inc.shape
    if length == 0:
        return np.ones(n)
    bin_idx = (inc.astype(np.int64) * n_bins) // 65536  # (N, L) in [0, n_bins)
    flat = bin_idx + np.arange(n)[:, None] * n_bins
    counts = np.bincount(flat.ravel(), minlength=n * n_bins).reshape(n, n_bins)
    expected = length / n_bins
    stat = ((counts - expected) ** 2 / expected).sum(axis=1)
    return _chi2_dist.sf(stat, df=n_bins - 1)


def classify_batch(matrix: np.ndarray) -> np.ndarray:
    """matrix: (N, 16) uint16. Returns (N,) int8 strategy codes (Strategy values)."""
    n = matrix.shape[0]
    if n == 0:
        return np.empty(0, dtype=np.int8)

    m_wide = matrix.astype(np.int64)  # unambiguous modular arithmetic for reflection

    # All deltas are computed on uint16, so subtraction wraps mod 2**16 for free.
    inc_all = np.diff(matrix, axis=1)                     # (N, 15)
    src1_inc = np.diff(matrix[:, 0::2], axis=1)           # (N, 7)
    src2_inc = np.diff(matrix[:, 1::2], axis=1)           # (N, 7)
    # Undo the round-robin interleaving: connection j == columns j, j+4, j+8, j+12.
    con = matrix.reshape(n, REQUESTS_PER_CONNECTION, CONNECTION_COUNT).transpose(0, 2, 1)
    con_inc = np.diff(con, axis=2)                        # (N, CONNECTION_COUNT, 3)

    # reflection: every IPID equals the request pattern shifted by a constant offset.
    offset = (m_wide[:, 0] - _REFLECTION_PATTERN[0]) % 65536
    expected = (_REFLECTION_PATTERN[None, :] + offset[:, None]) % 65536
    refl = np.all(m_wide == expected, axis=1)

    const = np.all(inc_all == 0, axis=1)

    pdest = np.all(src1_inc == 1, axis=1) & np.all(src2_inc == 1, axis=1)

    pconn = np.all(con_inc == 1, axis=(1, 2))

    single = np.all((inc_all >= 1) & (inc_all <= MAX_INC), axis=1)

    pbucket = np.all((con_inc >= 1) & (con_inc <= MAX_INC), axis=(1, 2))

    # multi: distinct IPID counters -> values fall into a few proximity clusters.
    # Greedy 1-D clustering: a new cluster starts where the sorted gap exceeds
    # MULTI_MAX_INC. (Assumed semantics for the original get_clusters; verify.)
    gaps = np.diff(np.sort(matrix, axis=1).astype(np.int64), axis=1)
    n_clusters = 1 + np.count_nonzero(gaps > MULTI_MAX_INC, axis=1)
    multi = (n_clusters > 1) & (n_clusters <= MULTI_MAX_CLUSTERS)

    # random: increments look uniform across every sub-sequence group. Reject
    # "random" if any group's uniformity p-value is implausibly small.
    p_min = _chi2_uniform_p(inc_all, RANDOM_CHI2_BINS)
    p_min = np.minimum(p_min, _chi2_uniform_p(src1_inc, RANDOM_CHI2_BINS))
    p_min = np.minimum(p_min, _chi2_uniform_p(src2_inc, RANDOM_CHI2_BINS))
    for j in range(CONNECTION_COUNT):
        p_min = np.minimum(p_min, _chi2_uniform_p(con_inc[:, j, :], RANDOM_CHI2_BINS))
    rand = p_min >= RANDOM_MIN_CHI2_VALUE

    # First match wins, in Strategy priority order; default UNCLASSIFIED.
    return np.select(
        [refl, const, pdest, pconn, single, pbucket, multi, rand],
        [s.value for s in (Strategy.REFLECTION, Strategy.CONSTANT, Strategy.PER_DESTINATION,
                           Strategy.PER_CONNECTION, Strategy.SINGLE, Strategy.PER_BUCKET,
                           Strategy.MULTI, Strategy.RANDOM)],
        default=Strategy.UNCLASSIFIED.value,
    ).astype(np.int8)


# --------------------------------------------------------------------------
def _batch_codes(ip_count: int, ipids: pa.ListArray) -> np.ndarray:
    """Build (N,) int8 codes for one Arrow batch. Rows whose sequence length is
    not exactly 16 cannot be classified and become UNCLASSIFIED (order preserved)."""
    codes = np.full(ip_count, Strategy.UNCLASSIFIED.value, dtype=np.int8)

    valid = pc.fill_null(pc.equal(pc.list_value_length(ipids), SEQUENCE_LENGTH), False)
    valid_np = valid.to_numpy(zero_copy_only=False)
    if not valid_np.any():
        return codes

    sub = ipids.filter(valid)  # compacts the child buffer to the selected rows
    flat = sub.values.to_numpy(zero_copy_only=False).astype(np.uint16, copy=False)
    matrix = flat.reshape(-1, SEQUENCE_LENGTH)
    codes[valid_np] = classify_batch(matrix)
    return codes


def process(
        input_path: Path,
        output_path: Path,
        batch_size: int,
        compression: str | None,
        threads: int,
        log_every: int,
) -> None:
    import duckdb

    con = duckdb.connect()
    if threads:
        con.execute(f"PRAGMA threads={threads}")

    # Split + cast happen vectorised in DuckDB; CAST (not TRY_CAST) surfaces
    # malformed data loudly instead of producing nulls inside the matrix.
    relation = con.execute(
        "SELECT IP_ADDR, "
        "list_transform(string_split(IPID_SEQUENCE, ','), x -> CAST(x AS USMALLINT)) AS ipids "
        "FROM read_parquet($path)",
        {"path": str(input_path)},
    )
    reader = relation.to_arrow_reader(batch_size)

    writer = pq.ParquetWriter(output_path, OUTPUT_SCHEMA, compression=compression)
    processed = 0
    last_log = 0
    start = time.monotonic()
    try:
        for batch in reader:
            ip_addr = batch.column("IP_ADDR")
            codes = _batch_codes(batch.num_rows, batch.column("ipids"))

            strategy_col = pa.DictionaryArray.from_arrays(
                pa.array(codes, type=pa.int8()), _CATEGORIES
            )
            writer.write_batch(
                pa.record_batch([ip_addr, strategy_col], schema=OUTPUT_SCHEMA)
            )

            processed += batch.num_rows
            if log_every and processed - last_log >= log_every:
                rate = processed / (time.monotonic() - start)
                print(f"{processed:,} IPs ({rate:,.0f}/s)", file=sys.stderr)
                last_log = processed
    finally:
        writer.close()
        con.close()

    elapsed = time.monotonic() - start
    print(f"done: {processed:,} IPs in {elapsed:.1f}s -> {output_path}", file=sys.stderr)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("input", type=Path, help="input parquet (e.g. ipid.pq)")
    p.add_argument("output", type=Path, help="output parquet")
    p.add_argument("--batch-size", type=int, default=1_000_000,
                   help="rows per batch; bounds peak memory (default: 1_000_000)")
    p.add_argument("--compression", default="zstd",
                   choices=["zstd", "snappy", "gzip", "lz4", "none"])
    p.add_argument("--threads", type=int, default=0,
                   help="DuckDB scan threads (0 = DuckDB default = all cores)")
    p.add_argument("--log-every", type=int, default=5_000_000,
                   help="log progress every N IPs to stderr (0 = off)")
    args = p.parse_args()

    process(
        args.input, args.output,
        batch_size=args.batch_size,
        compression=None if args.compression == "none" else args.compression,
        threads=args.threads,
        log_every=args.log_every,
    )


if __name__ == "__main__":
    main()