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

After the individual measurements, every available no-connection RT-base and
fixed-interval-mass pair is merged and its strategy distribution is plotted.
The same pair also produces a paper plot showing how the RT-based
``UNCLASSIFIED`` population is refined by the fixed-interval mass measurement.
TCP campaigns with an RT-based connection-oriented base measurement produce an
additional version with that measurement's strategy distribution as a third bar.
Protocol campaigns with an OS measurement also produce a row-normalized heatmap of
merged IP-ID selection strategies by general-purpose and network OS.
TCP campaigns also produce a paper plot of the merged strategy distribution
split by the ZMap ``synack`` and ``rst`` reply classifications.
Every available RT-base/fixed-interval-base pair is also compared with the
three compact paper figures in :mod:`ipid_analysis.paper_figures`.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger
import typer

from ipid_analysis.comparison import iter_base_comparisons
from ipid_analysis.increments import extract_increments
from ipid_analysis.manifest import iter_ipid_measurements, load_manifest, resolve
from ipid_analysis.paper_figures import (
    default_maxmind_database,
    render_increment_comparison,
    render_probing_interval_comparison,
    render_strategy_intersection,
)
from ipid_analysis.plot_increments import render as render_increments_plot
from ipid_analysis.plot_os_strategy import (
    render as render_os_strategy_plot,
)
from ipid_analysis.plot_os_strategy import (
    resolve_os_measurement_id,
)
from ipid_analysis.plot_probing_intervals import render as render_intervals_plot
from ipid_analysis.plot_strategies import (
    render as render_strategies_plot,
)
from ipid_analysis.plot_strategies import (
    render_merged as render_merged_strategies_plot,
)
from ipid_analysis.plot_strategy_refinement import (
    render as render_strategy_refinement_plot,
)
from ipid_analysis.plot_strategy_refinement import (
    render_with_connection as render_strategy_refinement_with_connection_plot,
)
from ipid_analysis.plot_tcp_flags_strategy import render as render_tcp_flags_strategy_plot
from ipid_analysis.probing_intervals import extract_probing_intervals
from ipid_analysis.strategies import classify_measurement
from ipid_analysis.strategy_merge import iter_strategy_merges, merge_strategies

app = typer.Typer()


@app.command()
def main(
    manifest_path: Path = typer.Argument(..., help="measurement manifest JSON (e.g. data.json)"),
    batch_size: int = typer.Option(1_000_000, help="rows per batch"),
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
    threads: int = typer.Option(0, help="DuckDB threads (0 = all cores)"),
    maxmind_db: Path | None = typer.Option(
        None,
        help="GeoLite2/GeoIP2 .mmdb for continent plots; also read from IPID_MAXMIND_DB",
    ),
) -> None:
    manifest = load_manifest(manifest_path)
    measurements = iter_ipid_measurements(manifest)
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

    merged_ok, merged_skipped = 0, 0
    for merge in iter_strategy_merges(manifest):
        try:
            output, stats = merge_strategies(
                merge,
                batch_size=batch_size,
                compression=comp,
                threads=threads,
            )
            render_merged_strategies_plot(merge)
            refinement_pdf, _, _ = render_strategy_refinement_plot(
                merge,
                compression=comp,
                threads=threads,
            )
            os_pdf = None
            os_measurement_id = resolve_os_measurement_id(manifest, merge.protocol)
            if os_measurement_id is not None:
                try:
                    os_pdf, _, _ = render_os_strategy_plot(
                        merge,
                        os_measurement_id,
                        compression=comp,
                        threads=threads,
                    )
                except (FileNotFoundError, ValueError) as exc:
                    logger.warning(
                        f"[{merge.target}] OS strategy heatmap failed ({exc}) -- skipped"
                    )
            tcp_flags_pdf = None
            connection_pdf = None
            if merge.protocol == "tcp":
                tcp_flags_pdf, _, _ = render_tcp_flags_strategy_plot(
                    merge,
                    compression=comp,
                    threads=threads,
                )
                connection = resolve(
                    manifest,
                    "tcp.ipid.connection.rt-based.base",
                )
                if connection is not None:
                    connection_pdf, _, _ = render_strategy_refinement_with_connection_plot(
                        merge,
                        connection,
                        compression=comp,
                        threads=threads,
                    )
            connection_message = (
                f"; connection-oriented refinement -> {connection_pdf}"
                if connection_pdf is not None
                else ""
            )
            os_message = f"; OS strategy heatmap -> {os_pdf}" if os_pdf is not None else ""
            tcp_flags_message = (
                f"; TCP flags by strategy -> {tcp_flags_pdf}" if tcp_flags_pdf is not None else ""
            )
            logger.success(
                f"[{merge.target}] {stats.rows:,} merged IPs, "
                f"{stats.not_enough_samples:,} not enough samples -> {output}; "
                f"strategy refinement -> {refinement_pdf}"
                f"{connection_message}{tcp_flags_message}{os_message}"
            )
            merged_ok += 1
        except FileNotFoundError as exc:
            logger.warning(f"[{merge.target}] missing merge input ({exc}) -- skipped")
            merged_skipped += 1

    comparison_ok, comparison_skipped = 0, 0
    continent_database = maxmind_db or default_maxmind_database()
    for comparison in iter_base_comparisons(manifest):
        completed = 0
        for label, renderer in (
            ("increment distributions", render_increment_comparison),
            ("strategy intersection", render_strategy_intersection),
        ):
            try:
                output, _, _ = renderer(
                    comparison,
                    compression=comp,
                    threads=threads,
                )
            except (FileNotFoundError, ValueError) as exc:
                logger.warning(f"[{comparison.target}] missing {label} input ({exc}) -- skipped")
            else:
                logger.success(f"[{comparison.target}] {label} -> {output}")
                completed += 1

        if continent_database is None or not continent_database.is_file():
            logger.warning(
                f"[{comparison.target}] no MaxMind .mmdb configured; "
                "continent probing-interval figure skipped"
            )
        else:
            try:
                output, _, _ = render_probing_interval_comparison(
                    comparison,
                    maxmind_database=continent_database,
                    compression=comp,
                    threads=threads,
                )
            except (FileNotFoundError, RuntimeError, ValueError) as exc:
                logger.warning(f"[{comparison.target}] continent figure failed ({exc}) -- skipped")
            else:
                logger.success(f"[{comparison.target}] probing intervals by continent -> {output}")
                completed += 1

        if completed:
            comparison_ok += 1
        else:
            comparison_skipped += 1

    logger.success(
        f"postprocessing + plotting done: {ok} measurements ok, {skipped} skipped; "
        f"{merged_ok} merges ok, {merged_skipped} skipped; "
        f"{comparison_ok} comparisons produced output, {comparison_skipped} skipped"
    )


if __name__ == "__main__":
    app()
