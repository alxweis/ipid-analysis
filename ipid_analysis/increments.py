"""Per-IP IPID increments, selected by the detected strategy.

For each IP the increments of the subsequences that belong to its strategy are
stored:

    PER_DESTINATION           -> increments of the two source subsequences
    PER_CONNECTION/PER_BUCKET -> increments of the connection subsequences
    everything else (fallback) -> increments of the whole sequence

Mass measurements only have position-independent strategies, so their increments
are always over the whole (present) sequence.

    python ipid_analysis/increments.py tcp.ipid.no-connection.fixed-interval.base
    -> data/processed/<zmap_id>/no-connection/fixed-interval-base/n-fi-b_increments.pq
"""

from __future__ import annotations

from pathlib import Path
import time

import duckdb
from loguru import logger
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer

from ipid_analysis.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement, load_manifest, resolve
from ipid_analysis.strategies import (
    DEFAULT_MANIFEST,
    INPUT_NAME,
    MASS_BATCH_CAP,
    READ_SQL,
    READ_SQL_MASS,
    SNAPSHOT_NAME,
    STRATEGY_DICT,
    IPIDStrategy,
    _batch_to_matrix,
    _mass_padded,
    classify_batch,
    classify_batch_mass,
    increment_views,
    load_config,
)

app = typer.Typer()

OUTPUT_SCHEMA = pa.schema(
    [
        ("IP_ADDR", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.dictionary(pa.int8(), pa.string())),
        ("INCREMENTS", pa.list_(pa.int32())),
    ]
)

_SRC_CODES = (int(IPIDStrategy.PER_DESTINATION),)
_CON_CODES = (int(IPIDStrategy.PER_CONNECTION), int(IPIDStrategy.PER_BUCKET))


def _scatter(values: np.ndarray, offsets: np.ndarray, mask: np.ndarray, view: np.ndarray) -> None:
    """Write view[r] into values[offsets[r]:offsets[r]+w] for each masked row r."""
    rows = np.flatnonzero(mask)
    if rows.size == 0 or view.shape[1] == 0:
        return
    w = view.shape[1]
    dst = (offsets[rows][:, None] + np.arange(w)[None, :]).ravel()
    values[dst] = view[rows].ravel().astype(np.int32)


def base_increments(matrix: np.ndarray, cfg, skip_first: bool, codes: np.ndarray):
    """(offsets, values) of the strategy-selected increments for base rows."""
    _, inc_all, inc_src1, inc_src2, inc_con = increment_views(matrix, cfg, skip_first)
    n = matrix.shape[0]
    inc_src = np.concatenate([inc_src1, inc_src2], axis=1)
    inc_conf = inc_con.reshape(n, -1)

    is_src = np.isin(codes, _SRC_CODES)
    is_con = np.isin(codes, _CON_CODES)
    is_all = ~(is_src | is_con)

    widths = np.full(n, inc_all.shape[1], dtype=np.int64)
    widths[is_src] = inc_src.shape[1]
    widths[is_con] = inc_conf.shape[1]
    offsets = np.zeros(n + 1, dtype=np.int64)
    np.cumsum(widths, out=offsets[1:])

    values = np.empty(int(offsets[-1]), dtype=np.int32)
    _scatter(values, offsets, is_all, inc_all)
    _scatter(values, offsets, is_src, inc_src)
    _scatter(values, offsets, is_con, inc_conf)
    return offsets, values


def mass_increments(ipid_list: pa.ListArray):
    """(offsets, values) of the whole-sequence increments over present values."""
    lengths, _, vals = _mass_padded(ipid_list)
    if vals.shape[1] == 0:
        return np.zeros(len(lengths) + 1, dtype=np.int64), np.empty(0, dtype=np.int32)
    diff = (vals[:, 1:] - vals[:, :-1]) & 0xFFFF
    inc_present = np.arange(vals.shape[1] - 1)[None, :] < (lengths[:, None] - 1)
    values = diff[inc_present].astype(np.int32)  # present increments, row-major
    widths = np.clip(lengths - 1, 0, None)
    offsets = np.zeros(len(lengths) + 1, dtype=np.int64)
    np.cumsum(widths, out=offsets[1:])
    return offsets, values


def increments_output_path(m: IpidMeasurement) -> Path:
    return m.artifact_path(PROCESSED_DATA_DIR, "increments")


def extract_increments(
    m: IpidMeasurement,
    batch_size: int = 1_000_000,
    compression: str | None = "zstd",
    threads: int = 0,
) -> Path:
    """Write the canonical increments parquet for one measurement."""
    input_path = RAW_DATA_DIR / "ipid" / m.measurement_id / INPUT_NAME
    snapshot_path = RAW_DATA_DIR / "ipid" / m.measurement_id / SNAPSHOT_NAME
    for path in (input_path, snapshot_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    cfg = load_config(snapshot_path)
    mass = m.scale == "mass"
    skip_first = (m.protocol == "tcp") and not mass
    output_path = increments_output_path(m)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    read_sql = READ_SQL_MASS if mass else READ_SQL
    reader_batch = min(batch_size, MASS_BATCH_CAP) if mass else batch_size
    con = duckdb.connect(config={"threads": threads} if threads else {})
    reader = con.execute(read_sql, {"input": str(input_path)}).to_arrow_reader(reader_batch)
    writer = pq.ParquetWriter(output_path, OUTPUT_SCHEMA, compression=compression)

    logger.info(
        f"[{m.target}] {m.measurement_id}: extracting increments ({'mass' if mass else 'base'})"
    )
    start = time.monotonic()
    try:
        for batch in reader:
            ip_addr = batch.column("IP_ADDR").cast(pa.string())
            n = len(ip_addr)

            if mass:
                codes = classify_batch_mass(batch.column("ipid"))
                offsets, values = mass_increments(batch.column("ipid"))
            else:
                valid, matrix = _batch_to_matrix(batch.column("ipid"), cfg.sequence_length)
                codes = np.full(n, int(IPIDStrategy.UNCLASSIFIED), dtype=np.int8)
                full_offsets = np.zeros(n + 1, dtype=np.int64)
                values = np.empty(0, dtype=np.int32)
                if matrix.shape[0]:
                    codes[valid] = classify_batch(matrix, cfg, skip_first)
                    voff, values = base_increments(matrix, cfg, skip_first, codes[valid])
                    widths = np.zeros(n, dtype=np.int64)
                    widths[valid] = np.diff(voff)
                    np.cumsum(widths, out=full_offsets[1:])
                offsets = full_offsets

            increments = pa.ListArray.from_arrays(
                pa.array(offsets, type=pa.int32()), pa.array(values, type=pa.int32())
            )
            strategy = pa.DictionaryArray.from_arrays(pa.array(codes), STRATEGY_DICT)
            writer.write_batch(
                pa.record_batch([ip_addr, strategy, increments], schema=OUTPUT_SCHEMA)
            )
    finally:
        writer.close()
        con.close()

    logger.success(f"[{m.target}] increments in {time.monotonic() - start:.1f}s -> {output_path}")
    return output_path


@app.command()
def main(
    target: str = typer.Argument(
        ..., help="dotted target, e.g. tcp.ipid.no-connection.fixed-interval.base"
    ),
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
        extract_increments(
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
