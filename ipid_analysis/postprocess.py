"""Manifest-driven postprocessing + plotting: run every step for every ipid
measurement present in the manifest.

    python ipid_analysis/postprocess.py data.json

Per measurement:
  1. strategies.pq          (ipid_analysis.strategies)
  2. probing-intervals.pq   (ipid_analysis.probing_intervals)
  3. increments.pq          (ipid_analysis.increments)
  4. strategies PDF + JSON          (ipid_analysis.plot_strategies)
  5. probing-intervals PDF + JSON   (ipid_analysis.plot_probing_intervals)
  6. increments CDF PDF + JSON      (ipid_analysis.plot_increments)

Missing measurements (combination not in the manifest, or raw data absent) are
skipped with a warning instead of aborting the run.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger
import typer

from ipid_analysis.increments import extract_increments
from ipid_analysis.manifest import iter_ipid_measurements, load_manifest
from ipid_analysis.plot_increments import render as render_increments_plot
from ipid_analysis.plot_probing_intervals import render as render_intervals_plot
from ipid_analysis.plot_strategies import render as render_strategies_plot
from ipid_analysis.probing_intervals import extract_probing_intervals
from ipid_analysis.strategies import classify_measurement

app = typer.Typer()


@app.command()
def main(
    manifest_path: Path = typer.Argument(..., help="measurement manifest JSON (e.g. data.json)"),
    batch_size: int = typer.Option(1_000_000, help="rows per batch"),
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
            extract_increments(m, batch_size=batch_size, compression=comp, threads=threads)
            render_strategies_plot(m)
            render_intervals_plot(m)
            render_increments_plot(m)
            ok += 1
        except FileNotFoundError as exc:
            logger.warning(f"[{m.target}] missing input ({exc}) -- skipped")
            skipped += 1

    logger.success(f"postprocessing + plotting done: {ok} ok, {skipped} skipped")


if __name__ == "__main__":
    app()
