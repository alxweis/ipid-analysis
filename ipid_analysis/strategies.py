"""IPID selection-strategy classification.

Reads a measurement's IPID sequences from ``data/raw/<measurement>/ipid.pq`` and
writes a per-IP strategy label to ``data/processed/<measurement>/strategies.pq``.

Run it on a measurement key (relative to the data dirs)::

    python ipid_analysis/strategies.py tcp.ipid.no-connection.rt-based.base

Design for scale (>100 GB / >300M rows):
  * DuckDB streams the file and splits/casts the comma-separated IPID strings in
    C++ across all cores -- no per-row Python parsing.
  * Only IP_ADDR and IPID_SEQUENCE are read; the timestamp columns are never
    touched (the current rules do not use them -> saves most of the I/O).
  * Base sequences have a fixed length, so a whole batch becomes one (N, L)
    uint16 matrix. Mass sequences retain their fixed measurement positions and
    mark missing replies explicitly. All rules run vectorized over each batch.
  * The strategy column is dictionary-encoded (int8 + small dictionary), so it
    costs ~1 byte/row instead of a string per row.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from functools import lru_cache
import math
from pathlib import Path
import re
import shutil
import time

import duckdb
from loguru import logger
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.special import betainc, gammaincc
from tqdm import tqdm
import typer
import yaml

from ipid_analysis.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement, load_manifest, resolve

app = typer.Typer()

MODULUS = 1 << 16  # IPIDs are 16-bit
CLASSIFIER_VERSION = "4"

# --- classifier thresholds (tuning, measurement-independent) ---------------
MIN_STEPS_BEFORE_WRAPAROUND = 3
MAX_INC = math.ceil(MODULUS / MIN_STEPS_BEFORE_WRAPAROUND) - 1  # 21845
MULTI_MAX_INC = 800
MULTI_MAX_CLUSTERS = 16
# Retained for the legacy Chi-square diagnostic plot; production RANDOM uses S.
RANDOM_MIN_P_VALUE = 1e-9
CHI2_BINS = 4  # equal-width bins for the IPID-value uniformity test
RANDOM_STRUCTURE_SCORE_VERSION = "raw-multiset-bounded-v2"
RANDOM_STRUCTURE_MIN_SCORE = 0.002018328854246871
RANDOM_STRUCTURE_UNIFORMITY_BINS = 16
RANDOM_STRUCTURE_MIN_TEST_SAMPLES = 2
RANDOM_STRUCTURE_BOUNDED_INCREMENT_NULL_PROBABILITY = MAX_INC / MODULUS

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
    NOT_ENOUGH_SAMPLES = 9


STRATEGY_NAMES = [s.name for s in IPIDStrategy]
STRATEGY_DICT = pa.array(STRATEGY_NAMES, type=pa.string())

# One display name and one fixed color per strategy, used consistently across all
# figures. Edit here to change them everywhere.
STRATEGY_PRETTY = {
    "REFLECTION": "Reflection",
    "CONSTANT": "Constant",
    "PER_DESTINATION": "Per-Destination",
    "PER_CONNECTION": "Per-Connection",
    "SINGLE": "Single",
    "PER_BUCKET": "Per-Bucket",
    "MULTI": "Multi",
    "RANDOM": "Random",
    "UNCLASSIFIED": "Unclassified",
    "NOT_ENOUGH_SAMPLES": "Not Enough Samples",
}

STRATEGY_COLORS = {
    "REFLECTION": "#ECD37E",  # light yellow
    "CONSTANT": "#799ABE",  # light blue
    "PER_DESTINATION": "#BC9AE6",  # light purple
    "PER_CONNECTION": "#EDA7D2",  # light pink
    "SINGLE": "#ED9393",  # light red
    "PER_BUCKET": "#82D486",  # light green
    "MULTI": "#9AD4EC",  # light cyan
    "RANDOM": "#F5AA60",  # light orange
    "UNCLASSIFIED": "#E2E2E2",  # light grey
    "NOT_ENOUGH_SAMPLES": "#B0B0B0",  # medium-light grey
}

OUTPUT_SCHEMA = pa.schema(
    [
        ("IP_ADDR", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.dictionary(pa.int8(), pa.string())),
    ],
    metadata={
        b"classifier_version": CLASSIFIER_VERSION.encode(),
        b"random_structure_score_version": RANDOM_STRUCTURE_SCORE_VERSION.encode(),
        b"random_structure_min_score": str(RANDOM_STRUCTURE_MIN_SCORE).encode(),
    },
)

# DuckDB does the heavy lifting: scan + split + cast, multithreaded in C++.
READ_SQL = """
SELECT
    IP_ADDR,
    -- Missing/non-numeric IPIDs (e.g. '-' for a probe without reply) become NULL
    -- via TRY_CAST. Such rows are emitted as empty lists so the fixed-position
    -- classifier assigns UNCLASSIFIED instead of using incomplete positions.
    CASE WHEN len(list_filter(ints, v -> v IS NULL)) = 0 THEN ints ELSE CAST([] AS INTEGER[]) END AS ipid
FROM (
    SELECT IP_ADDR,
           list_transform(string_split(IPID_SEQUENCE, ','), x -> TRY_CAST(x AS INTEGER)) AS ints
    FROM read_parquet($input)
)
"""

# Mass measurements are not the fixed 4x4 structure (4x25 positions with '-'
# for lost replies). Preserve every position as either an IP-ID or -1 so the
# RANDOM score can evaluate full/destination/connection subsequences. CONSTANT
# and MULTI still operate only on present IP-IDs.
READ_SQL_MASS = """
SELECT
    IP_ADDR,
    list_transform(
        string_split(IPID_SEQUENCE, ','),
        x -> COALESCE(TRY_CAST(x AS INTEGER), -1)
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


# ---------------------------------------------------------------------------
# Vectorized classifier. S: (N, L) uint16 -> (N,) int8 codes.
# Each mask mirrors one of the original is_* predicates.
# ---------------------------------------------------------------------------
def increment_views(
    S: np.ndarray, cfg: MeasurementConfig, skip_first: bool
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Shared subsequence structure for a base (N, L) matrix: applies the TCP
    handshake skip, then returns (pattern, inc_all, inc_src1, inc_src2, inc_con).
    Used by both the classifier and the increment extractor so they stay in sync."""
    conn, req = cfg.connection_count, cfg.requests_per_connection
    pattern = cfg.request_ip_ids[np.arange(cfg.sequence_length) % cfg.request_ip_ids.size]

    if skip_first:
        # TCP: first IPID of each connection is the handshake's last packet ->
        # drop the first round (positions 0..conn-1) from every view.
        S = S[:, conn:]
        pattern = pattern[conn:]
        req -= 1

    n = S.shape[0]
    inc_all = np.diff(S, axis=1)
    inc_src1 = np.diff(S[:, 0::2], axis=1)  # source A (interface a)
    inc_src2 = np.diff(S[:, 1::2], axis=1)  # source B (interface b)
    con = S.reshape(n, req, conn).transpose(0, 2, 1)  # (N, conn, req)
    inc_con = np.diff(con, axis=2)  # (N, conn, req-1)
    return pattern, inc_all, inc_src1, inc_src2, inc_con


def classify_batch(S: np.ndarray, cfg: MeasurementConfig, skip_first: bool = False) -> np.ndarray:
    conn = cfg.connection_count
    pattern, inc_all, inc_src1, inc_src2, inc_con = increment_views(S, cfg, skip_first)

    # trim S identically to the views for the REFLECTION comparison
    S64 = (S[:, conn:] if skip_first else S).astype(np.int64)

    # REFLECTION: sequence equals the request pattern shifted by a constant offset
    offset = (S64[:, 0] - pattern[0]) % MODULUS
    expected = (pattern[None, :] + offset[:, None]) % MODULUS
    m_reflection = (S64 == expected).all(axis=1)

    m_constant = (inc_all == 0).all(axis=1)
    m_per_dest = _all_in_range(inc_src1, 1, 1, 1) & _all_in_range(inc_src2, 1, 1, 1)
    m_per_conn = _all_in_range(inc_con, 1, 1, (1, 2))
    m_single = _all_in_range(inc_all, 1, MAX_INC, 1)
    m_per_bucket = _all_in_range(inc_con, 1, MAX_INC, (1, 2))

    # Base measurements intentionally stop after the position-dependent and
    # cheaply determined rules. MULTI and RANDOM require the larger mass sample.
    masks = [m_reflection, m_constant, m_per_dest, m_per_conn, m_single, m_per_bucket]
    return np.select(
        masks,
        [
            int(IPIDStrategy.REFLECTION),
            int(IPIDStrategy.CONSTANT),
            int(IPIDStrategy.PER_DESTINATION),
            int(IPIDStrategy.PER_CONNECTION),
            int(IPIDStrategy.SINGLE),
            int(IPIDStrategy.PER_BUCKET),
        ],
        default=int(IPIDStrategy.UNCLASSIFIED),
    ).astype(np.int8)


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


def _mass_padded(
    ipid_list: pa.ListArray,
    sequence_length: int | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Ragged list<int> -> (present counts, present mask, position matrix).

    Input -1 values and padded cells both represent missing replies. When
    ``sequence_length`` is supplied, every row retains exactly that many
    measurement positions.
    """
    list_lengths = ipid_list.value_lengths().to_numpy(zero_copy_only=False).astype(np.int64)
    n = len(list_lengths)
    maximum_list_length = int(list_lengths.max()) if n and list_lengths.max() > 0 else 0
    if sequence_length is not None and maximum_list_length > sequence_length:
        raise ValueError(
            f"mass sequence contains {maximum_list_length} positions, "
            f"expected at most {sequence_length}"
        )
    w = sequence_length if sequence_length is not None else maximum_list_length
    if w == 0:
        return (
            np.zeros(n, dtype=np.int64),
            np.zeros((n, 0), bool),
            np.full((n, 0), -1, np.int64),
        )

    flat = ipid_list.flatten().to_numpy(zero_copy_only=False).astype(np.int64)
    starts = np.empty(n, dtype=np.int64)
    starts[0] = 0
    np.cumsum(list_lengths[:-1], out=starts[1:])

    col = np.arange(w)
    in_list = col[None, :] < list_lengths[:, None]
    gather = np.clip(starts[:, None] + col[None, :], 0, max(flat.size - 1, 0))
    values = np.where(in_list, flat[gather] if flat.size else -1, -1)
    present = in_list & (values >= 0)
    lengths = present.sum(axis=1).astype(np.int64)
    return lengths, present, values


def _sorted_present_values(
    values: np.ndarray,
    present: np.ndarray,
) -> np.ndarray:
    """Sort present 16-bit values and place missing positions at the end."""
    sentinel = np.uint32(MODULUS)
    values_u32 = np.where(present, values, 0).astype(np.uint32, copy=False)
    return np.sort(np.where(present, values_u32, sentinel), axis=1)


def _cluster_counts_mass(
    values: np.ndarray,
    present: np.ndarray,
    lengths: np.ndarray,
    *,
    ordered: np.ndarray | None = None,
) -> np.ndarray:
    """Circular single-link cluster count per row over the present values."""
    n, w = values.shape
    if ordered is None:
        ordered = _sorted_present_values(values, present)
    gaps = np.diff(ordered, axis=1)
    gap_present = np.arange(w - 1)[None, :] < (lengths[:, None] - 1)
    interior = np.where(gap_present, gaps > MULTI_MAX_INC, False).sum(axis=1)
    idx_max = np.clip(lengths - 1, 0, w - 1)
    span = ordered[np.arange(n), idx_max] - ordered[:, 0]  # max - min of present
    wrap_big = ((MODULUS - span) > MULTI_MAX_INC) & (lengths >= 1)
    k = interior + wrap_big
    return np.where(lengths >= 1, np.where(k == 0, 1, k), 0)


def chi2_uniformity_pvalues(values: np.ndarray, present: np.ndarray) -> np.ndarray:
    """Per-row uniformity p-value of the present IPID values.

    Operating on values instead of consecutive increments makes RANDOM
    classification invariant to sample order and robust to missing replies.
    """
    m = values.shape[0]
    if m == 0:
        return np.ones(0)
    sample_count = present.sum(axis=1)
    bins = (values * CHI2_BINS) // MODULUS
    rows = np.broadcast_to(np.arange(m)[:, None], values.shape)
    flat_bin = (rows * CHI2_BINS + bins)[present]
    counts = np.bincount(flat_bin, minlength=m * CHI2_BINS).reshape(m, CHI2_BINS)
    exp = np.where(sample_count > 0, sample_count / CHI2_BINS, 1.0)[:, None]
    chi2 = ((counts - exp) ** 2 / exp).sum(axis=1)
    p = gammaincc((CHI2_BINS - 1) / 2.0, chi2 / 2.0)
    return np.where(sample_count > 0, p, 1.0)


@dataclass(frozen=True)
class RandomStructureFeatures:
    sample_count: np.ndarray
    unique_count: np.ndarray
    maximum_gap: np.ndarray
    uniformity_pvalue: np.ndarray
    occupancy_pvalue: np.ndarray
    maximum_gap_pvalue: np.ndarray


@lru_cache(maxsize=None)
def _occupancy_cdf_table(maximum: int) -> np.ndarray:
    """Exact P(D <= d) for n draws over the 16-bit IP-ID space."""
    table = np.ones((maximum + 1, maximum + 1), dtype=float)
    distribution = np.zeros(maximum + 1, dtype=float)
    distribution[0] = 1.0

    for sample_count in range(1, maximum + 1):
        previous = distribution
        distribution = np.zeros_like(previous)
        distinct = np.arange(1, sample_count + 1)
        distribution[distinct] = (
            previous[distinct] * distinct / MODULUS
            + previous[distinct - 1] * (MODULUS - distinct + 1) / MODULUS
        )
        table[sample_count] = np.cumsum(distribution)
    return table


def random_structure_features(
    values: np.ndarray,
    present: np.ndarray,
    *,
    ordered: np.ndarray | None = None,
) -> RandomStructureFeatures:
    """Calculate raw RANDOM-compatibility components with one row-wise sort."""
    sample_count = present.sum(axis=1).astype(np.int16)
    values_u32 = np.where(present, values, 0).astype(np.uint32, copy=False)
    if ordered is None:
        ordered = _sorted_present_values(values, present)
    width = ordered.shape[1]
    if width == 0:
        empty = np.zeros(len(values), dtype=float)
        return RandomStructureFeatures(
            sample_count=sample_count,
            unique_count=sample_count.copy(),
            maximum_gap=np.zeros(len(values), dtype=np.int64),
            uniformity_pvalue=np.ones(len(values), dtype=float),
            occupancy_pvalue=np.ones(len(values), dtype=float),
            maximum_gap_pvalue=empty,
        )

    adjacent_active = np.arange(width - 1)[None, :] < (sample_count[:, None] - 1)
    interior_gaps = ordered[:, 1:] - ordered[:, :-1]
    repeated = adjacent_active & (interior_gaps == 0)
    unique_count = sample_count - repeated.sum(axis=1)

    last_index = np.clip(sample_count - 1, 0, width - 1)
    first = ordered[:, 0]
    last = ordered[np.arange(len(ordered)), last_index]
    wrap_gap = MODULUS - last.astype(np.int64) + first.astype(np.int64)
    active_gaps = np.where(adjacent_active, interior_gaps, 0)
    maximum_gap = np.maximum(active_gaps.max(axis=1), wrap_gap).astype(np.int64)

    row_count = len(values)
    bins = (values_u32 * RANDOM_STRUCTURE_UNIFORMITY_BINS) // MODULUS
    rows = np.broadcast_to(np.arange(row_count)[:, None], values.shape)
    flat_bin = (rows * RANDOM_STRUCTURE_UNIFORMITY_BINS + bins)[present]
    counts = np.bincount(
        flat_bin,
        minlength=row_count * RANDOM_STRUCTURE_UNIFORMITY_BINS,
    ).reshape(row_count, RANDOM_STRUCTURE_UNIFORMITY_BINS)
    expected = np.where(
        sample_count > 0,
        sample_count / RANDOM_STRUCTURE_UNIFORMITY_BINS,
        1.0,
    )[:, None]
    chi2 = ((counts - expected) ** 2 / expected).sum(axis=1)
    uniformity_pvalue = gammaincc(
        (RANDOM_STRUCTURE_UNIFORMITY_BINS - 1) / 2.0,
        chi2 / 2.0,
    )

    occupancy_table = _occupancy_cdf_table(width)
    occupancy_pvalue = occupancy_table[
        np.clip(sample_count, 0, width),
        np.clip(unique_count, 0, width),
    ]

    gap_fraction = np.clip(maximum_gap / MODULUS, 0.0, 1.0)
    maximum_gap_pvalue = np.minimum(
        1.0,
        sample_count * np.power(1.0 - gap_fraction, np.maximum(sample_count - 1, 0)),
    )

    return RandomStructureFeatures(
        sample_count=sample_count,
        unique_count=unique_count,
        maximum_gap=maximum_gap,
        uniformity_pvalue=uniformity_pvalue,
        occupancy_pvalue=occupancy_pvalue,
        maximum_gap_pvalue=maximum_gap_pvalue,
    )


def _binomial_upper_tail(
    success_count: np.ndarray,
    sample_count: np.ndarray,
) -> np.ndarray:
    pvalues = np.ones(len(sample_count), dtype=float)
    valid = (sample_count >= RANDOM_STRUCTURE_MIN_TEST_SAMPLES) & (success_count > 0)
    pvalues[valid] = betainc(
        success_count[valid],
        sample_count[valid] - success_count[valid] + 1,
        RANDOM_STRUCTURE_BOUNDED_INCREMENT_NULL_PROBABILITY,
    )
    return pvalues


def _increment_counts(
    values: np.ndarray,
    present: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    pair_present = present[:, :-1] & present[:, 1:]
    increments = (values[:, 1:] - values[:, :-1]) & 0xFFFF
    bounded = pair_present & (increments >= 1) & (increments <= MAX_INC)
    return (
        bounded.sum(axis=1).astype(np.int64),
        pair_present.sum(axis=1).astype(np.int64),
    )


def random_structure_bounded_increment_pvalues(
    values: np.ndarray,
    present: np.ndarray,
    cfg: MeasurementConfig,
) -> np.ndarray:
    """Minimum bounded-increment support over full/destination/connection views."""
    if values.shape[1] != cfg.sequence_length:
        raise ValueError(
            f"RANDOM score requires {cfg.sequence_length} positions, got {values.shape[1]}"
        )

    full_success, full_count = _increment_counts(values, present)

    destination_components = [
        _increment_counts(values[:, index::2], present[:, index::2]) for index in range(2)
    ]
    destination_success = sum(component[0] for component in destination_components)
    destination_count = sum(component[1] for component in destination_components)

    connections = values.reshape(
        len(values),
        cfg.requests_per_connection,
        cfg.connection_count,
    ).transpose(0, 2, 1)
    connection_present = present.reshape(
        len(values),
        cfg.requests_per_connection,
        cfg.connection_count,
    ).transpose(0, 2, 1)
    connection_components = [
        _increment_counts(connections[:, index], connection_present[:, index])
        for index in range(cfg.connection_count)
    ]
    connection_success = sum(component[0] for component in connection_components)
    connection_count = sum(component[1] for component in connection_components)

    return np.minimum.reduce(
        [
            _binomial_upper_tail(full_success, full_count),
            _binomial_upper_tail(destination_success, destination_count),
            _binomial_upper_tail(connection_success, connection_count),
        ]
    )


def random_structure_scores(
    values: np.ndarray,
    present: np.ndarray,
    cfg: MeasurementConfig,
    *,
    ordered: np.ndarray | None = None,
) -> np.ndarray:
    """Production RANDOM-compatibility score shared with synthetic evaluation."""
    features = random_structure_features(values, present, ordered=ordered)
    return np.clip(
        np.minimum.reduce(
            [
                features.uniformity_pvalue,
                features.occupancy_pvalue,
                features.maximum_gap_pvalue,
                random_structure_bounded_increment_pvalues(
                    values,
                    present,
                    cfg,
                ),
            ]
        ),
        0.0,
        1.0,
    )


def classify_batch_mass(
    ipid_list: pa.ListArray,
    cfg: MeasurementConfig,
) -> np.ndarray:
    """Fixed-interval mass classification.

    CONSTANT and MULTI retain their existing priority and definitions. Only
    residual rows are evaluated with the RANDOM structure score.
    """
    lengths, present, values = _mass_padded(ipid_list, cfg.sequence_length)
    n = len(lengths)
    codes = np.full(n, int(IPIDStrategy.UNCLASSIFIED), dtype=np.int8)
    if values.shape[1] == 0:
        return codes

    # CONSTANT and MULTI are intentionally evaluated before RANDOM and retain
    # their existing definitions over the multiset of received IP-IDs.
    ordered = _sorted_present_values(values, present)
    last_index = np.clip(lengths - 1, 0, values.shape[1] - 1)
    m_constant = (lengths >= 1) & (ordered[:, 0] == ordered[np.arange(n), last_index])
    n_clusters = _cluster_counts_mass(
        values,
        present,
        lengths,
        ordered=ordered,
    )
    m_multi = (n_clusters > 1) & (n_clusters <= MULTI_MAX_CLUSTERS)

    codes = np.select(
        [m_constant, m_multi],
        [int(IPIDStrategy.CONSTANT), int(IPIDStrategy.MULTI)],
        default=-1,
    ).astype(np.int8)

    residual = np.flatnonzero(codes == -1)
    if residual.size:
        score = random_structure_scores(
            values[residual],
            present[residual],
            cfg,
            ordered=ordered[residual],
        )
        is_random = (score >= RANDOM_STRUCTURE_MIN_SCORE) & (
            lengths[residual] >= RANDOM_STRUCTURE_MIN_TEST_SAMPLES
        )
        codes[residual] = np.where(
            is_random,
            int(IPIDStrategy.RANDOM),
            int(IPIDStrategy.UNCLASSIFIED),
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
    number of IPs written. ``mass`` selects the fixed-interval path that retains
    missing positions (READ_SQL_MASS); otherwise the fixed 4x4 path."""
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
                    codes = classify_batch_mass(batch.column("ipid"), cfg)
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


def strategies_output_path(
    m: IpidMeasurement,
    processed_root: Path = PROCESSED_DATA_DIR,
) -> Path:
    """Return the canonical campaign path for a strategies parquet."""
    return m.artifact_path(processed_root, "strategies")


def _publish_persisted_strategies(source_path: Path, output_path: Path) -> int:
    """Atomically copy a persisted workflow classification into processed data."""
    parquet = pq.ParquetFile(source_path)
    if parquet.schema_arrow.names != OUTPUT_SCHEMA.names:
        raise ValueError(f"{source_path}: invalid strategies schema")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    try:
        shutil.copyfile(source_path, temporary)
        temporary.replace(output_path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return parquet.metadata.num_rows


def classify_paths(
    input_path: Path,
    snapshot_path: Path,
    output_path: Path,
    *,
    protocol: str,
    mass: bool = False,
    batch_size: int = 1_000_000,
    compression: str | None = "zstd",
    threads: int = 0,
) -> Path:
    """Classify explicit input paths without requiring a campaign manifest.

    This is the entry point used by the S3 workflow worker. It deliberately
    reuses the same classifier as normal manifest-driven postprocessing.
    """
    for path in (input_path, snapshot_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    cfg = load_config(snapshot_path)
    skip_first = protocol.lower() == "tcp" and not mass
    output_path.parent.mkdir(parents=True, exist_ok=True)
    process(
        input_path,
        output_path,
        cfg,
        skip_first,
        mass,
        batch_size=batch_size,
        compression=compression,
        threads=threads,
    )
    return output_path


def classify_measurement(
    m: IpidMeasurement,
    batch_size: int = 1_000_000,
    compression: str | None = "zstd",
    threads: int = 0,
    reclassify: bool = False,
    raw_root: Path = RAW_DATA_DIR,
    processed_root: Path = PROCESSED_DATA_DIR,
) -> Path:
    """Classify one ipid measurement and write its strategies.pq into the
    campaign directory. Existing processed or workflow classifications are
    reused unless ``reclassify`` is true. Returns the output path."""
    raw_dir = raw_root / "ipid" / m.measurement_id
    input_path = raw_dir / INPUT_NAME
    snapshot_path = raw_dir / SNAPSHOT_NAME
    persisted_path = raw_dir / OUTPUT_NAME
    output_path = strategies_output_path(m, processed_root)

    if persisted_path.is_file() and not reclassify:
        n = _publish_persisted_strategies(persisted_path, output_path)
        logger.success(
            f"[{m.target}] reused {n:,} workflow-classified IPs "
            f"from {persisted_path} -> {output_path}"
        )
        return output_path

    if output_path.is_file() and not reclassify:
        n = pq.ParquetFile(output_path).metadata.num_rows
        logger.info(f"[{m.target}] reusing {n:,} classified IPs -> {output_path}")
        return output_path

    mass = m.scale == "mass"
    cfg = load_config(snapshot_path)
    skip_first = (m.protocol == "tcp") and not mass

    if mass:
        logger.info(
            f"[{m.target}] {m.measurement_id}: mass, "
            "CONSTANT/MULTI followed by RANDOM structure score"
        )
    else:
        logger.info(
            f"[{m.target}] {m.measurement_id}: "
            f"{cfg.connection_count}x{cfg.requests_per_connection}={cfg.sequence_length} IPIDs"
            + (", skipping first IPID per connection" if skip_first else "")
        )

    start = time.monotonic()
    classify_paths(
        input_path,
        snapshot_path,
        output_path,
        protocol=m.protocol,
        mass=mass,
        batch_size=batch_size,
        compression=compression,
        threads=threads,
    )
    n = pq.ParquetFile(output_path).metadata.num_rows
    logger.success(f"[{m.target}] {n:,} IPs in {time.monotonic() - start:.1f}s -> {output_path}")
    return output_path


@app.command()
def main(
    target: str = typer.Argument(
        ..., help="dotted target, e.g. tcp.ipid.no-connection.rt-based.base"
    ),
    manifest: Path = typer.Option(DEFAULT_MANIFEST, help="measurement manifest JSON"),
    batch_size: int = typer.Option(1_000_000, help="rows per batch"),
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
    threads: int = typer.Option(0, help="DuckDB threads (0 = all cores)"),
    reclassify: bool = typer.Option(
        False,
        "--reclassify",
        help="ignore persisted strategies and classify the measurement again",
    ),
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
            reclassify=reclassify,
        )
    except FileNotFoundError as exc:
        logger.error(f"not found: {exc}")
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    app()
