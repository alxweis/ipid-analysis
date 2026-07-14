"""IPID selection-strategy classification.

Reads a measurement's IPID sequences from ``data/raw/<measurement>/ipid.pq`` and
writes a per-IP strategy label to ``data/processed/<measurement>/strategies.pq``.

Run it on a measurement key (relative to the data dirs)::

    python ipid_analysis/strategies.py tcp.ipid.nec.rt.base

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

from dataclasses import dataclass
from enum import IntEnum
import math
from pathlib import Path
import re
import time

import duckdb
from loguru import logger
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.special import gammaincc  # vectorized chi-square survival function
from tqdm import tqdm
import typer
import yaml

from ipid_analysis.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement, load_manifest, resolve

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

DEFAULT_MANIFEST = RAW_DATA_DIR / "manifest.json"


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
    -- Missing/non-numeric IPIDs (e.g. '-' for a probe without reply) become NULL
    -- via TRY_CAST; any such row is emitted as an empty list so it falls through
    -- to UNCLASSIFIED (the length != SEQUENCE_LENGTH path) instead of crashing.
    CASE WHEN len(list_filter(ints, v -> v IS NULL)) = 0 THEN ints ELSE CAST([] AS INTEGER[]) END AS ipid
FROM (
    SELECT IP_ADDR,
           list_transform(string_split(IPID_SEQUENCE, ','), x -> TRY_CAST(x AS INTEGER)) AS ints
    FROM read_parquet($input)
)
"""

# Mass measurements are not the fixed 4x4 structure (up to 4x25 = 80..100 values,
# with '-' for lost replies). Drop the '-' and classify the *present* values with
# position-independent rules only. Batches are capped to bound the padded matrix.
READ_SQL_MASS = """
SELECT
    IP_ADDR,
    list_filter(
        list_transform(string_split(IPID_SEQUENCE, ','), x -> TRY_CAST(x AS INTEGER)),
        v -> v IS NOT NULL
    ) AS ipid
FROM read_parquet($input)
"""

MASS_BATCH_CAP = 250_000  # rows/batch for the (N x <=100) padded mass path


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


def _mass_padded(ipid_list: pa.ListArray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Ragged list<int> -> (lengths, present_mask, values) padded to the batch's
    max present length. `values` uses -1 for padding; `present_mask` marks the
    real entries."""
    lengths = ipid_list.value_lengths().to_numpy(zero_copy_only=False).astype(np.int64)
    n = len(lengths)
    w = int(lengths.max()) if n and lengths.max() > 0 else 0
    if w == 0:
        return lengths, np.zeros((n, 0), bool), np.full((n, 0), -1, np.int64)

    flat = ipid_list.flatten().to_numpy(zero_copy_only=False).astype(np.int64)
    starts = np.empty(n, dtype=np.int64)
    starts[0] = 0
    np.cumsum(lengths[:-1], out=starts[1:])

    col = np.arange(w)
    present = col[None, :] < lengths[:, None]
    gather = np.clip(starts[:, None] + col[None, :], 0, max(flat.size - 1, 0))
    values = np.where(present, flat[gather] if flat.size else -1, -1)
    return lengths, present, values


def _cluster_counts_mass(values: np.ndarray, present: np.ndarray, lengths: np.ndarray) -> np.ndarray:
    """Circular single-link cluster count per row over the present values."""
    n, w = values.shape
    big = 1 << 20
    ordered = np.sort(np.where(present, values, big), axis=1)
    gaps = np.diff(ordered, axis=1)
    gap_present = np.arange(w - 1)[None, :] < (lengths[:, None] - 1)
    interior = np.where(gap_present, gaps > MULTI_MAX_INC, False).sum(axis=1)
    idx_max = np.clip(lengths - 1, 0, w - 1)
    span = ordered[np.arange(n), idx_max] - ordered[:, 0]  # max - min of present
    wrap_big = ((MODULUS - span) > MULTI_MAX_INC) & (lengths >= 1)
    k = interior + wrap_big
    return np.where(lengths >= 1, np.where(k == 0, 1, k), 0)


def _chi2_pvalue_mass(diff: np.ndarray, inc_present: np.ndarray) -> np.ndarray:
    """Per-row uniformity p-value of the present increments (see chi2_pvalue)."""
    m = diff.shape[0]
    if m == 0:
        return np.ones(0)
    n_inc = inc_present.sum(axis=1)
    bins = (diff * CHI2_BINS) // MODULUS
    rows = np.broadcast_to(np.arange(m)[:, None], diff.shape)
    flat_bin = (rows * CHI2_BINS + bins)[inc_present]
    counts = np.bincount(flat_bin, minlength=m * CHI2_BINS).reshape(m, CHI2_BINS)
    exp = np.where(n_inc > 0, n_inc / CHI2_BINS, 1.0)[:, None]
    chi2 = ((counts - exp) ** 2 / exp).sum(axis=1)
    p = gammaincc((CHI2_BINS - 1) / 2.0, chi2 / 2.0)
    return np.where(n_inc > 0, p, 1.0)


def classify_batch_mass(ipid_list: pa.ListArray) -> np.ndarray:
    """Position-independent classification for mass measurements ('-' already
    dropped). Only CONSTANT, SINGLE, MULTI, RANDOM apply; empty -> UNCLASSIFIED."""
    lengths, present, values = _mass_padded(ipid_list)
    n = len(lengths)
    codes = np.full(n, int(IPIDStrategy.UNCLASSIFIED), dtype=np.int8)
    if values.shape[1] == 0:
        return codes

    diff = (values[:, 1:] - values[:, :-1]) & 0xFFFF  # consecutive present, mod 2**16
    inc_present = np.arange(values.shape[1] - 1)[None, :] < (lengths[:, None] - 1)
    has_inc = lengths >= 2

    m_constant = (lengths >= 1) & np.where(inc_present, diff == 0, True).all(axis=1)
    m_single = has_inc & np.where(inc_present, (diff >= 1) & (diff <= MAX_INC), True).all(axis=1)
    n_clusters = _cluster_counts_mass(values, present, lengths)
    m_multi = (n_clusters > 1) & (n_clusters <= MULTI_MAX_CLUSTERS)

    codes = np.select(
        [m_constant, m_single, m_multi],
        [int(IPIDStrategy.CONSTANT), int(IPIDStrategy.SINGLE), int(IPIDStrategy.MULTI)],
        default=-1,
    ).astype(np.int8)

    residual = np.flatnonzero(codes == -1)
    if residual.size:
        p = _chi2_pvalue_mass(diff[residual], inc_present[residual])
        # rows with no increments (length < 2) can't be RANDOM -> UNCLASSIFIED
        is_random = (p >= RANDOM_MIN_P_VALUE) & (lengths[residual] >= 2)
        codes[residual] = np.where(
            is_random, int(IPIDStrategy.RANDOM), int(IPIDStrategy.UNCLASSIFIED)
        )
    return codes


def process(
    input_path: Path,
    output_path: Path,
    cfg: MeasurementConfig,
    skip_first: bool,
    mass: bool,
    batch_size: int,
    compression: str | None,
    threads: int,
) -> int:
    """Stream input_path through the classifier into output_path. Returns the
    number of IPs written. `mass` selects the position-independent, variable-length
    path (READ_SQL_MASS); otherwise the fixed 4x4 path."""
    total = pq.ParquetFile(input_path).metadata.num_rows
    con = duckdb.connect(config={"threads": threads} if threads else {})
    read_sql = READ_SQL_MASS if mass else READ_SQL
    reader_batch = min(batch_size, MASS_BATCH_CAP) if mass else batch_size
    reader = con.execute(read_sql, {"input": str(input_path)}).to_arrow_reader(reader_batch)
    writer = pq.ParquetWriter(output_path, OUTPUT_SCHEMA, compression=compression)

    processed = 0
    try:
        with tqdm(total=total, unit="IP", desc="classifying") as bar:
            for batch in reader:
                ip_addr = batch.column("IP_ADDR").cast(pa.string())
                if mass:
                    codes = classify_batch_mass(batch.column("ipid"))
                else:
                    valid, matrix = _batch_to_matrix(batch.column("ipid"), cfg.sequence_length)
                    codes = np.full(len(valid), int(IPIDStrategy.UNCLASSIFIED), dtype=np.int8)
                    if matrix.shape[0]:
                        codes[valid] = classify_batch(matrix, cfg, skip_first)

                strategy = pa.DictionaryArray.from_arrays(pa.array(codes), STRATEGY_DICT)
                writer.write_batch(pa.record_batch([ip_addr, strategy], schema=OUTPUT_SCHEMA))

                processed += len(codes)
                bar.update(len(codes))
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


def strategies_output_path(m: IpidMeasurement) -> Path:
    """data/processed/<zmap_id>/<proto>-ipid-<mode>-<interval>-<scale>_strategies.pq"""
    if not m.zmap_id:
        raise ValueError(f"{m.target}: no zmap id in manifest (needed for the output path)")
    return PROCESSED_DATA_DIR / m.zmap_id / m.output_name("strategies")


def classify_measurement(
    m: IpidMeasurement,
    batch_size: int = 1_000_000,
    compression: str | None = "zstd",
    threads: int = 0,
) -> Path:
    """Classify one ipid measurement and write its strategies.pq into the
    campaign directory (data/processed/<zmap_id>/). Returns the output path."""
    raw_dir = RAW_DATA_DIR / "ipid" / m.measurement_id
    input_path = raw_dir / INPUT_NAME
    snapshot_path = raw_dir / SNAPSHOT_NAME
    output_path = strategies_output_path(m)

    for path in (input_path, snapshot_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    cfg = load_config(snapshot_path)
    mass = m.scale == "mass"
    skip_first = (m.protocol == "tcp") and not mass  # handshake skip is a 4x4-only concept

    if mass:
        logger.info(f"[{m.target}] {m.measurement_id}: mass, position-independent rules only")
    else:
        logger.info(
            f"[{m.target}] {m.measurement_id}: "
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
        mass,
        batch_size=batch_size,
        compression=compression,
        threads=threads,
    )
    logger.success(f"[{m.target}] {n:,} IPs in {time.monotonic() - start:.1f}s -> {output_path}")
    return output_path


@app.command()
def main(
    target: str = typer.Argument(..., help="dotted target, e.g. tcp.ipid.nec.rt.base"),
    manifest: Path = typer.Option(DEFAULT_MANIFEST, help="measurement manifest JSON"),
    batch_size: int = typer.Option(1_000_000, help="rows per batch"),
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
    threads: int = typer.Option(0, help="DuckDB threads (0 = all cores)"),
) -> None:
    m = resolve(load_manifest(manifest), target)
    if m is None:
        logger.error(f"{target}: not present in {manifest}")
        raise typer.Exit(code=1)
    try:
        classify_measurement(
            m,
            batch_size=batch_size,
            compression=None if compression == "none" else compression,
            threads=threads,
        )
    except FileNotFoundError as exc:
        logger.error(f"not found: {exc}")
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    app()
