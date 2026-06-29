"""IPID selection-strategy classification.

Reads a measurement's IPID sequences from ``data/raw/<measurement>/ipid.pq`` and
writes a per-IP strategy label to ``data/processed/<measurement>/strategies.pq``.

Run it on a measurement key (relative to the data dirs)::

    python ipid_analysis/strategies.py ipid/icmp_2026-06-29_15-56-56

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

import math
import re
import time
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer
import yaml
from loguru import logger
from scipy.special import gammaincc  # vectorized chi-square survival function
from tqdm import tqdm

from ipid_analysis.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

app = typer.Typer()

MODULUS = 1 << 16  # IPIDs are 16-bit

# --- classifier thresholds (tuning, measurement-independent) ---------------
MIN_STEPS_BEFORE_WRAPAROUND = 3
MAX_INC = math.ceil(MODULUS / MIN_STEPS_BEFORE_WRAPAROUND) - 1  # 21845
MULTI_MAX_INC = 800
MULTI_MAX_CLUSTERS = 16
RANDOM_MIN_P_VALUE = 1e-9  # reject "random" if any uniformity p-value is below this
CHI2_BINS = 4  # bins for the increment-uniformity test (see chi2_pvalue note)

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
           SELECT IP_ADDR,
                  list_transform(string_split(IPID_SEQUENCE, ','), x - > CAST(x AS INTEGER)) AS ipid
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
    increments into ``n_bins`` equal-width bins and tests against a uniform
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
    the gap between consecutive sorted values exceeds ``max_diff``.

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
def classify_batch(S: np.ndarray, cfg: MeasurementConfig, skip_first: bool = False) -> np.ndarray:
    conn, req = cfg.connection_count, cfg.requests_per_connection
    req_ids = cfg.request_ip_ids

    # Pattern over the original positions; trimmed identically to S below.
    pattern = req_ids[np.arange(cfg.sequence_length) % req_ids.size]

    if skip_first:
        # TCP: the first IPID of each connection belongs to the handshake's last
        # packet. With round-robin interleaving that is exactly the first round
        # (positions 0..conn-1), so drop it from every view.
        S = S[:, conn:]
        pattern = pattern[conn:]
        req -= 1

    n = S.shape[0]
    S64 = S.astype(np.int64)

    # increments, all mod 2**16 via uint16 wraparound
    inc_all = np.diff(S, axis=1)
    inc_src1 = np.diff(S[:, 0::2], axis=1)  # source A (interface a)
    inc_src2 = np.diff(S[:, 1::2], axis=1)  # source B (interface b)
    # connection j = positions [j, j+conn, j+2*conn, ...] -> reshape + swap axes
    con = S.reshape(n, req, conn).transpose(0, 2, 1)
    inc_con = np.diff(con, axis=2)  # (N, conn, req-1)

    # REFLECTION: sequence equals the request pattern shifted by a constant offset
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
        skip_first: bool,
        batch_size: int,
        compression: str | None,
        threads: int,
) -> int:
    """Stream input_path through the classifier into output_path. Returns the
    number of IPs written."""
    total = pq.ParquetFile(input_path).metadata.num_rows
    con = duckdb.connect(config={"threads": threads} if threads else {})
    reader = con.execute(READ_SQL, {"input": str(input_path)}).to_arrow_reader(batch_size)
    writer = pq.ParquetWriter(output_path, OUTPUT_SCHEMA, compression=compression)

    processed = 0
    try:
        with tqdm(total=total, unit="IP", desc="classifying") as bar:
            for batch in reader:
                ip_addr = batch.column("IP_ADDR").cast(pa.string())
                valid, matrix = _batch_to_matrix(batch.column("ipid"), cfg.sequence_length)

                codes = np.full(len(valid), int(IPIDStrategy.UNCLASSIFIED), dtype=np.int8)
                if matrix.shape[0]:
                    codes[valid] = classify_batch(matrix, cfg, skip_first)

                strategy = pa.DictionaryArray.from_arrays(pa.array(codes), STRATEGY_DICT)
                writer.write_batch(pa.record_batch([ip_addr, strategy], schema=OUTPUT_SCHEMA))

                processed += len(valid)
                bar.update(len(valid))
    finally:
        writer.close()
        con.close()
    return processed


def resolve_protocol(measurement: str, protocol: str) -> str:
    """'auto' derives the protocol from the measurement leaf (tcp-80 -> tcp)."""
    if protocol != "auto":
        return protocol.lower()
    leaf = measurement.rstrip("/").split("/")[-1]
    return re.split(r"[-_]", leaf, maxsplit=1)[0].lower()


@app.command()
def main(
        measurement: str = typer.Argument(
            ..., help="measurement key, e.g. ipid/icmp_2026-06-29_15-56-56"
        ),
        protocol: str = typer.Option(
            "auto", help="auto|tcp|udp|icmp; tcp skips the first IPID of each connection"
        ),
        batch_size: int = typer.Option(1_000_000, help="rows per batch"),
        compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
        threads: int = typer.Option(0, help="DuckDB threads (0 = all cores)"),
) -> None:
    raw_dir = RAW_DATA_DIR / measurement
    input_path = raw_dir / INPUT_NAME
    snapshot_path = raw_dir / SNAPSHOT_NAME
    output_path = PROCESSED_DATA_DIR / measurement / OUTPUT_NAME

    for path in (input_path, snapshot_path):
        if not path.is_file():
            logger.error(f"not found: {path}")
            raise typer.Exit(code=1)

    cfg = load_config(snapshot_path)
    proto = resolve_protocol(measurement, protocol)
    skip_first = proto == "tcp"

    logger.info(
        f"{measurement}: {proto}, "
        f"{cfg.connection_count}x{cfg.requests_per_connection}={cfg.sequence_length} IPIDs"
        + (", skipping first IPID per connection" if skip_first else "")
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    n = process(
        input_path,
        output_path,
        cfg,
        skip_first,
        batch_size=batch_size,
        compression=None if compression == "none" else compression,
        threads=threads,
    )
    logger.success(f"classified {n:,} IPs in {time.monotonic() - start:.1f}s -> {output_path}")


if __name__ == "__main__":
    app()
