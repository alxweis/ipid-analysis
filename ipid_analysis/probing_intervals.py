"""Probing-interval extraction.

For one ipid measurement, compute the probing intervals -- the consecutive
deltas of SEND_TIMESTAMP_SEQUENCE (microseconds) -- per IP and write them to the
campaign directory::

    python ipid_analysis/probing_intervals.py tcp.ipid.nec.fi.base
    -> data/processed/<zmap_id>/<proto>-ipid-<mode>-<interval>-<scale>_probing_intervals.pq

The whole thing runs in DuckDB (split -> cast -> list-diff), streaming and
multithreaded; no per-row Python. Output schema: IP_ADDR, PROBING_INTERVALS
(list<bigint>). Rows are kept even if a sequence has a different length.
"""

from __future__ import annotations

from pathlib import Path
import time

import duckdb
from loguru import logger
import typer

from ipid_analysis.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement, load_manifest, resolve
from ipid_analysis.strategies import DEFAULT_MANIFEST, INPUT_NAME

app = typer.Typer()

# Split + cast the send timestamps, then take consecutive deltas via a list
# comprehension (DuckDB list indexing is 1-based).
INTERVALS_SQL = """
COPY (
    SELECT
        IP_ADDR,
        [ts[i + 1] - ts[i] FOR i IN range(1, len(ts))] AS PROBING_INTERVALS
    FROM (
        SELECT IP_ADDR,
               CAST(string_split(SEND_TIMESTAMP_SEQUENCE, ',') AS BIGINT[]) AS ts
        FROM read_parquet($input)
    )
) TO $output (FORMAT parquet, COMPRESSION $compression)
"""


def probing_intervals_output_path(m: IpidMeasurement) -> Path:
    if not m.zmap_id:
        raise ValueError(f"{m.target}: no zmap id in manifest (needed for the output path)")
    return PROCESSED_DATA_DIR / m.zmap_id / m.output_name("probing_intervals")


def extract_probing_intervals(
    m: IpidMeasurement, compression: str = "zstd", threads: int = 0
) -> Path:
    """Write <...>_probing_intervals.pq for one measurement. Returns the path."""
    input_path = RAW_DATA_DIR / "ipid" / m.measurement_id / INPUT_NAME
    if not input_path.is_file():
        raise FileNotFoundError(input_path)
    output_path = probing_intervals_output_path(m)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"[{m.target}] {m.measurement_id}: extracting probing intervals")
    start = time.monotonic()
    con = duckdb.connect(config={"threads": threads} if threads else {})
    con.execute(
        INTERVALS_SQL,
        {"input": str(input_path), "output": str(output_path), "compression": compression},
    )
    con.close()
    logger.success(f"[{m.target}] intervals in {time.monotonic() - start:.1f}s -> {output_path}")
    return output_path


@app.command()
def main(
    target: str = typer.Argument(..., help="dotted target, e.g. tcp.ipid.nec.fi.base"),
    manifest: Path = typer.Option(DEFAULT_MANIFEST, help="measurement manifest JSON"),
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4"),
    threads: int = typer.Option(0, help="DuckDB threads (0 = all cores)"),
) -> None:
    m = resolve(load_manifest(manifest), target)
    if m is None:
        logger.error(f"{target}: not present in {manifest}")
        raise typer.Exit(code=1)
    try:
        extract_probing_intervals(m, compression=compression, threads=threads)
    except FileNotFoundError as exc:
        logger.error(f"not found: {exc}")
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    app()
