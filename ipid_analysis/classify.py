from __future__ import annotations

import math
import time
from enum import IntEnum
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer
from loguru import logger
from scipy.special import gammaincc  # vectorized chi-square survival function
from tqdm import tqdm

from ipid_analysis.config import PROCESSED_DATA_DIR, IPID_MEASURE_NAME, IPID_CONFIG_SNAPSHOT_NAME, \
    STRATEGY_DATA_NAME, IP_ADDR, IPID_SELECTION_STRATEGY, load_config, IPID_DATA_DIR, MeasurementConfig

app = typer.Typer()

MODULUS = 1 << 16  # IPIDs are 16-bit

# --- Classifier thresholds -------------------------------------------------
MIN_STEPS_BEFORE_WRAPAROUND = 3
MAX_INC = math.ceil(MODULUS / MIN_STEPS_BEFORE_WRAPAROUND) - 1  # 21845
MULTI_MAX_INC = 800
MULTI_MAX_CLUSTERS = 16
RANDOM_MIN_P_VALUE = 1e-9
CHI2_BINS = 4


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
        (IP_ADDR, pa.string()),
        (IPID_SELECTION_STRATEGY, pa.dictionary(pa.int8(), pa.string())),
    ]
)

READ_SQL = """
           SELECT IP_ADDR,
                  CAST(string_split(IPID_SEQUENCE, ',') AS INTEGER[]) AS ipid
           FROM read_parquet($input) \
           """


# ---------------------------------------------------------------------------
# Helpers used by the rules. All operate on whole batches.
# ---------------------------------------------------------------------------
def _all_in_range(inc: np.ndarray, lo: int, hi: int, axis) -> np.ndarray:
    """Per-row: are all increments within [lo, hi]?  inc is already mod-2**16."""
    return ((inc >= lo) & (inc <= hi)).all(axis=axis)


def chi2_pvalue(inc: np.ndarray, n_bins: int) -> np.ndarray:
    """Chi-square goodness-of-fit p-value (per row) that the increments are uniform over [0, 2**16)."""
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
    """Per-row cluster count, circular (mod 2**16): sort each row, count gaps above max_diff, including the wrap gap
    from the largest value back to the smallest. On a circle, #clusters == #large gaps (or 1 if none)."""
    ordered = np.sort(seq, axis=1).astype(np.int64)
    interior = (np.diff(ordered, axis=1) > max_diff).sum(axis=1)
    wrap = MODULUS - (ordered[:, -1] - ordered[:, 0])  # gap max -> min over the wraparound
    k = interior + (wrap > max_diff)
    return np.where(k == 0, 1, k)


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
        # TCP: the first IPID of each connection belongs to the handshake's last packet. With round-robin interleaving
        # that is exactly the first round (positions 0..conn-1), so drop it from every view.
        S = S[:, conn:]
        pattern = pattern[conn:]
        req -= 1

    n = S.shape[0]
    S64 = S.astype(np.int64)

    # increments, all mod 2**16 via uint16 wraparound
    inc_all = np.diff(S, axis=1)
    inc_src1 = np.diff(S[:, 0::2], axis=1)  # source A
    inc_src2 = np.diff(S[:, 1::2], axis=1)  # source B
    # connection i = positions [i, i+conn, i+2*conn, ...] -> reshape + swap axes
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

    # Resolve the cheap, deterministic rules first; -1 marks rows that still need the expensive RANDOM test, so chi2
    # runs only on the residual.
    det_masks = [m_reflection, m_constant, m_per_dest, m_per_conn, m_single, m_per_bucket, m_multi]
    codes = np.select(det_masks, list(range(len(det_masks))), default=-1).astype(np.int8)

    residual = np.flatnonzero(codes == -1)
    if residual.size:
        # RANDOM: increments look uniform on every view (whole seq, both sources, each connection). Reject if the
        # smallest p-value is below the threshold.
        p_min = np.minimum.reduce(
            [
                chi2_pvalue(inc_all[residual], CHI2_BINS),
                chi2_pvalue(inc_src1[residual], CHI2_BINS),
                chi2_pvalue(inc_src2[residual], CHI2_BINS),
                *[chi2_pvalue(inc_con[residual, i, :], CHI2_BINS) for i in range(conn)],
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
    """Turn a list<int32> column into an (M, seq_len) uint16 matrix for the rows that have exactly seq_len entries.
    Returns (valid_mask, matrix)."""
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
    """Stream input_path through the classifier into output_path. Returns the number of IPs written."""
    total = pq.ParquetFile(input_path).metadata.num_rows
    con = duckdb.connect(config={"threads": threads} if threads else {})
    reader = con.execute(READ_SQL, {"input": str(input_path)}).to_arrow_reader(batch_size)
    writer = pq.ParquetWriter(output_path, OUTPUT_SCHEMA, compression=compression)

    processed = 0
    try:
        with tqdm(total=total, unit="IP", desc="classifying") as bar:
            for batch in reader:
                ip_addr = batch.column(IP_ADDR).cast(pa.string())
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


@app.command()
def main(
        measurement_id: str = typer.Argument(
            ..., help="measurement id, e.g. tcp-80_YYYY-MM-DD_HH-MM-SS"
        ),
        batch_size: int = typer.Option(1_000_000, help="rows per batch"),
        compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
        threads: int = typer.Option(0, help="DuckDB threads (0 = all cores)"),
) -> None:
    raw_dir = IPID_DATA_DIR / measurement_id
    input_path = raw_dir / IPID_MEASURE_NAME
    snapshot_path = raw_dir / IPID_CONFIG_SNAPSHOT_NAME
    output_path = PROCESSED_DATA_DIR / measurement_id / STRATEGY_DATA_NAME

    for path in (input_path, snapshot_path):
        if not path.is_file():
            logger.error(f"not found: {path}")
            raise typer.Exit(code=1)

    cfg = load_config(snapshot_path)
    proto = cfg.zmap.protocol
    skip_first = proto == "tcp"

    logger.info(
        f"{measurement_id}: {proto}, "
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
