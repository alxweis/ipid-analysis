#!/usr/bin/env python3
"""IPID selection-strategy classification.

Usage:
    python3 classify_ipid.py ipid/icmp_2026-06-29_15-56-56

The single argument is a measurement key. Paths are derived from it:

    input  : ./data/raw/<key>/ipid.pq
    config : ./data/raw/<key>/ipid.snapshot.yaml
    output : ./data/processed/<key>/strategies.pq        (created automatically)

(The data root defaults to ./data and can be changed with --data-root.)

The snapshot YAML supplies the measurement layout (connection_count,
requests_per_connection, request_ip_ids); the classifier thresholds below are
tuning parameters and stay in code.

Design for scale (>100 GB / >300M rows):
  * DuckDB streams the file and splits/casts the comma-separated IPID strings in
    C++ across all cores -- no per-row Python parsing.
  * Only IP_ADDR and IPID_SEQUENCE are read; the timestamp columns are never
    touched (the current rules do not use them -> saves most of the I/O).
  * Each sequence has a fixed length, so a whole batch becomes one (N, L) uint16
    matrix and every rule runs vectorized over the batch. No per-IP objects.
  * The strategy column is dictionary-encoded (int8 + small dictionary), so it
    costs ~1 byte/row instead of a string per row.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from scipy.special import gammaincc  # vectorized chi-square survival function

MODULUS = 1 << 16  # IPIDs are 16-bit

# --- classifier thresholds (tuning, measurement-independent) ---------------
MIN_STEPS_BEFORE_WRAPAROUND = 3
MAX_INC = math.ceil(MODULUS / MIN_STEPS_BEFORE_WRAPAROUND) - 1  # 21845
MULTI_MAX_INC = 800
MULTI_MAX_CLUSTERS = 16
RANDOM_MIN_P_VALUE = 1e-9  # reject "random" if any uniformity p-value is below this
CHI2_BINS = 4  # bins for the increment-uniformity test (see chi2_pvalue note)

DATA_ROOT = Path("data")
INPUT_NAME = "ipid.pq"
SNAPSHOT_NAME = "ipid.snapshot.yaml"
OUTPUT_NAME = "strategies.pq"


class IPIDStrategy(IntEnum):
    """Values double as the dictionary codes; ORDER == classification priority."""

    REFLECTION = 0
    CONSTANT = 1
    PER_DESTINATION = 2
    PER_CONNECTION = 3
    SINGLE = 4
    PER_BUCKET = 5
    MULTI = 6
    RANDOM = 7
    UNCLASSIFIED = 8


STRATEGY_NAMES = [s.name for s in IPIDStrategy]
STRATEGY_DICT = pa.array(STRATEGY_NAMES, type=pa.string())

OUTPUT_SCHEMA = pa.schema(
    [
        ("IP_ADDR", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.dictionary(pa.int8(), pa.string())),
    ]
)

# DuckDB does the heavy lifting: scan + split + cast, multithreaded in C++.
READ_SQL = """
           SELECT
               IP_ADDR,
               list_transform(string_split(IPID_SEQUENCE, ','), x -> CAST(x AS INTEGER)) AS ipid
           FROM read_parquet($input) \
           """


# ---------------------------------------------------------------------------
# Measurement configuration (from the snapshot YAML).
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class MeasurementConfig:
    connection_count: int
    requests_per_connection: int
    request_ip_ids: np.ndarray  # int64

    @property
    def sequence_length(self) -> int:
        return self.connection_count * self.requests_per_connection


def load_config(snapshot_path: Path) -> MeasurementConfig:
    with snapshot_path.open() as fh:
        data = yaml.safe_load(fh)
    try:
        return MeasurementConfig(
            connection_count=int(data["connection_count"]),
            requests_per_connection=int(data["requests_per_connection"]),
            request_ip_ids=np.asarray(data["request_ip_ids"], dtype=np.int64),
        )
    except (KeyError, TypeError) as exc:
        raise ValueError(f"{snapshot_path}: missing/invalid measurement fields ({exc})") from exc


# ---------------------------------------------------------------------------
# Helpers used by the rules. All operate on whole batches.
# ---------------------------------------------------------------------------
def _all_in_range(inc: np.ndarray, lo: int, hi: int, axis) -> np.ndarray:
    """Per-row: are all increments within [lo, hi]?  inc is already mod-2**16."""
    return ((inc >= lo) & (inc <= hi)).all(axis=axis)


def chi2_pvalue(inc: np.ndarray, n_bins: int) -> np.ndarray:
    """Chi-square goodness-of-fit p-value (per row) that the increments are
    uniform over [0, 2**16).

    NOTE (assumption -- the original chi2_test was undefined): the increments of
    a random IPID source are uniform mod 2**16, so this bins each row's
    increments into `n_bins` equal-width bins and tests against a uniform
    expectation. Swap this body if your intended test differs. Returns a p-value
    in [0, 1]; small => clearly non-uniform.
    """
    m, length = inc.shape
    if length == 0:
        return np.ones(m)
    bins = (inc.astype(np.int64) * n_bins) // MODULUS  # 0..n_bins-1
    flat = np.arange(m).repeat(length) * n_bins + bins.ravel()
    counts = np.bincount(flat, minlength=m * n_bins).reshape(m, n_bins)
    expected = length / n_bins
    chi2 = ((counts - expected) ** 2 / expected).sum(axis=1)
    df = n_bins - 1
    return gammaincc(df / 2.0, chi2 / 2.0)


def cluster_counts(seq: np.ndarray, max_diff: int) -> np.ndarray:
    """Per-row number of clusters: sort the row, then start a new cluster wherever
    the gap between consecutive sorted values exceeds `max_diff`.

    NOTE (assumption -- get_clusters was undefined): linear (non-wrapping)
    single-link clustering on the raw IPIDs. Replace if you need circular
    (mod-2**16) clustering.
    """
    ordered = np.sort(seq, axis=1).astype(np.int64)
    gaps = np.diff(ordered, axis=1)
    return 1 + (gaps > max_diff).sum(axis=1)


# ---------------------------------------------------------------------------
# Vectorized classifier. S: (N, L) uint16 -> (N,) int8 codes.
# Each mask mirrors one of the original is_* predicates.
# ---------------------------------------------------------------------------
def classify_batch(S: np.ndarray, cfg: MeasurementConfig) -> np.ndarray:
    n = S.shape[0]
    S64 = S.astype(np.int64)
    conn, req = cfg.connection_count, cfg.requests_per_connection
    req_ids = cfg.request_ip_ids

    # increments, all mod 2**16 via uint16 wraparound
    inc_all = np.diff(S, axis=1)
    inc_src1 = np.diff(S[:, 0::2], axis=1)   # source A (interface a)
    inc_src2 = np.diff(S[:, 1::2], axis=1)   # source B (interface b)
    # connection j = positions [j, j+conn, j+2*conn, ...] -> reshape + swap axes
    con = S.reshape(n, req, conn).transpose(0, 2, 1)
    inc_con = np.diff(con, axis=2)           # (N, conn, req-1)

    # REFLECTION: sequence equals the request pattern shifted by a constant offset
    pattern = req_ids[np.arange(cfg.sequence_length) % req_ids.size]
    offset = (S64[:, 0] - pattern[0]) % MODULUS
    expected = (pattern[None, :] + offset[:, None]) % MODULUS
    m_reflection = (S64 == expected).all(axis=1)

    m_constant = (inc_all == 0).all(axis=1)
    m_per_dest = _all_in_range(inc_src1, 1, 1, 1) & _all_in_range(inc_src2, 1, 1, 1)
    m_per_conn = _all_in_range(inc_con, 1, 1, (1, 2))
    m_single = _all_in_range(inc_all, 1, MAX_INC, 1)
    m_per_bucket = _all_in_range(inc_con, 1, MAX_INC, (1, 2))

    n_clusters = cluster_counts(S, MULTI_MAX_INC)
    m_multi = (n_clusters > 1) & (n_clusters <= MULTI_MAX_CLUSTERS)

    # Resolve the cheap, deterministic rules first; -1 marks rows that still need
    # the expensive RANDOM test, so chi2 runs only on the residual.
    det_masks = [m_reflection, m_constant, m_per_dest, m_per_conn, m_single, m_per_bucket, m_multi]
    codes = np.select(det_masks, list(range(len(det_masks))), default=-1).astype(np.int8)

    residual = np.flatnonzero(codes == -1)
    if residual.size:
        # RANDOM: increments look uniform on every view (whole seq, both sources,
        # each connection). Reject if the smallest p-value is below the threshold.
        p_min = np.minimum.reduce(
            [
                chi2_pvalue(inc_all[residual], CHI2_BINS),
                chi2_pvalue(inc_src1[residual], CHI2_BINS),
                chi2_pvalue(inc_src2[residual], CHI2_BINS),
                *[chi2_pvalue(inc_con[residual, j, :], CHI2_BINS) for j in range(conn)],
            ]
        )
        codes[residual] = np.where(
            p_min >= RANDOM_MIN_P_VALUE,
            int(IPIDStrategy.RANDOM),
            int(IPIDStrategy.UNCLASSIFIED),
            )
    return codes


# ---------------------------------------------------------------------------
def _batch_to_matrix(ipid_list: pa.ListArray, seq_len: int) -> tuple[np.ndarray, np.ndarray]:
    """Turn a list<int32> column into an (M, seq_len) uint16 matrix for the rows
    that have exactly seq_len entries. Returns (valid_mask, matrix)."""
    lengths = ipid_list.value_lengths().to_numpy(zero_copy_only=False)
    if lengths.size == 0:
        return np.zeros(0, dtype=bool), np.empty((0, seq_len), dtype=np.uint16)

    valid = lengths == seq_len
    flat = ipid_list.flatten().to_numpy(zero_copy_only=False)
    starts = np.empty(len(lengths), dtype=np.int64)
    starts[0] = 0
    np.cumsum(lengths[:-1], out=starts[1:])

    idx = starts[valid][:, None] + np.arange(seq_len)
    matrix = flat[idx].astype(np.uint16, copy=False)
    return valid, matrix


def process(
        input_path: Path,
        output_path: Path,
        cfg: MeasurementConfig,
        batch_size: int,
        compression: str | None,
        threads: int,
        log_every: int,
) -> None:
    con = duckdb.connect(config={"threads": threads} if threads else {})
    reader = con.execute(READ_SQL, {"input": str(input_path)}).to_arrow_reader(batch_size)
    writer = pq.ParquetWriter(output_path, OUTPUT_SCHEMA, compression=compression)

    processed = 0
    last_log = 0
    start = time.monotonic()
    try:
        for batch in reader:
            ip_addr = batch.column("IP_ADDR").cast(pa.string())
            valid, matrix = _batch_to_matrix(batch.column("ipid"), cfg.sequence_length)

            codes = np.full(len(valid), int(IPIDStrategy.UNCLASSIFIED), dtype=np.int8)
            if matrix.shape[0]:
                codes[valid] = classify_batch(matrix, cfg)

            strategy = pa.DictionaryArray.from_arrays(pa.array(codes), STRATEGY_DICT)
            writer.write_batch(pa.record_batch([ip_addr, strategy], schema=OUTPUT_SCHEMA))

            processed += len(valid)
            if log_every and processed - last_log >= log_every:
                rate = processed / (time.monotonic() - start)
                print(f"{processed:,} IPs processed ({rate:,.0f}/s)", file=sys.stderr)
                last_log = processed
    finally:
        writer.close()
        con.close()

    elapsed = time.monotonic() - start
    print(f"Done: {processed:,} IPs in {elapsed:.1f}s -> {output_path}", file=sys.stderr)


def main() -> None:
    p = argparse.ArgumentParser(description="Classify IPID selection strategies.")
    p.add_argument("measurement", help="measurement key, e.g. ipid/icmp_2026-06-29_15-56-56")
    p.add_argument("--data-root", type=Path, default=DATA_ROOT,
                   help="data root (default: ./data)")
    p.add_argument("--batch-size", type=int, default=1_000_000,
                   help="rows per batch (default: 1000000)")
    p.add_argument("--compression", default="zstd",
                   choices=["zstd", "snappy", "gzip", "lz4", "none"])
    p.add_argument("--threads", type=int, default=0,
                   help="DuckDB threads (0 = all cores)")
    p.add_argument("--log-every", type=int, default=5_000_000,
                   help="log progress every N IPs to stderr (0 = off)")
    args = p.parse_args()

    raw_dir = args.data_root / "raw" / args.measurement
    input_path = raw_dir / INPUT_NAME
    snapshot_path = raw_dir / SNAPSHOT_NAME
    output_path = args.data_root / "processed" / args.measurement / OUTPUT_NAME

    for path in (input_path, snapshot_path):
        if not path.is_file():
            sys.exit(f"error: not found: {path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cfg = load_config(snapshot_path)
    print(
        f"input   : {input_path}\n"
        f"config  : {snapshot_path} "
        f"({cfg.connection_count}x{cfg.requests_per_connection} = {cfg.sequence_length} IPIDs, "
        f"{cfg.request_ip_ids.size} request IDs)\n"
        f"output  : {output_path}",
        file=sys.stderr,
    )

    process(
        input_path,
        output_path,
        cfg,
        batch_size=args.batch_size,
        compression=None if args.compression == "none" else args.compression,
        threads=args.threads,
        log_every=args.log_every,
    )


if __name__ == "__main__":
    main()