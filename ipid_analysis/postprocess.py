"""Manifest-driven postprocessing: run every step for every ipid measurement
present in the manifest.

    python ipid_analysis/postprocess.py manifest.json

Per measurement:
  1. strategies.pq          (ipid_analysis.strategies)
  2. probing_intervals.pq   (ipid_analysis.probing_intervals)

Missing measurements (combination not in the manifest, or raw data absent) are
skipped with a warning instead of aborting the run.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger
import typer

from ipid_analysis.manifest import iter_ipid_measurements, load_manifest
from ipid_analysis.probing_intervals import extract_probing_intervals
from ipid_analysis.strategies import classify_measurement

app = typer.Typer()


@app.command()
def main(
    manifest_path: Path = typer.Argument(..., help="measurement manifest JSON"),
    batch_size: int = typer.Option(1_000_000, help="rows per batch (strategies)"),
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
    threads: int = typer.Option(0, help="DuckDB threads (0 = all cores)"),
) -> None:
    measurements = iter_ipid_measurements(load_manifest(manifest_path))
    logger.info(f"{len(measurements)} ipid measurement(s) in {manifest_path}")

    comp = None if compression == "none" else compression
    ok, skipped = 0, 0
    for m in measurements:
        try:
            classify_measurement(m, batch_size=batch_size, compression=comp, threads=threads)
            extract_probing_intervals(m, compression=comp or "zstd", threads=threads)
            ok += 1
        except FileNotFoundError as exc:
            logger.warning(f"[{m.target}] missing input ({exc}) -- skipped")
            skipped += 1

    logger.success(f"postprocessing done: {ok} ok, {skipped} skipped")


if __name__ == "__main__":
    app()
