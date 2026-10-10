"""Fast rerun command for CAIDA/RIPE network-role artifacts only."""

from __future__ import annotations

from pathlib import Path

from loguru import logger
import typer

from ipid_analysis.caida_itdk import (
    DEFAULT_RELEASE as DEFAULT_ITDK_RELEASE,
)
from ipid_analysis.caida_itdk import (
    DEFAULT_TOPOLOGY as DEFAULT_ITDK_TOPOLOGY,
)
from ipid_analysis.caida_itdk import (
    prepare_itdk,
)
from ipid_analysis.config import PROCESSED_DATA_DIR
from ipid_analysis.manifest import load_manifest, resolve
from ipid_analysis.plot_itdk_os import render_itdk_os_analysis
from ipid_analysis.plot_itdk_strategy import render_itdk_analysis
from ipid_analysis.plot_os_group_strategy import resolve_os_measurement_id
from ipid_analysis.plot_ripe_atlas_os import render_ripe_atlas_os_analysis
from ipid_analysis.plot_ripe_atlas_strategy import render_ripe_atlas_analysis
from ipid_analysis.ripe_atlas import (
    DEFAULT_DUMP_SAMPLES,
    DEFAULT_PREPROCESS_WORKERS,
    DEFAULT_SOURCE,
    infer_campaign_start,
    parse_datetime,
    prepare_ripe_atlas,
)
from ipid_analysis.strategy_merge import iter_strategy_merges

app = typer.Typer()


@app.command()
def main(
    manifest_path: Path = typer.Argument(..., help="completed analysis-job manifest"),
    threads: int = typer.Option(0, min=0, help="DuckDB threads; 0 uses all cores"),
    compression: str = typer.Option("zstd", help="OS-group Parquet compression"),
    itdk_release: str = typer.Option(DEFAULT_ITDK_RELEASE, envvar="IPID_ANALYSIS_ITDK_RELEASE"),
    itdk_topology: str = typer.Option(DEFAULT_ITDK_TOPOLOGY, envvar="IPID_ANALYSIS_ITDK_TOPOLOGY"),
    itdk_source_dir: Path | None = typer.Option(None, envvar="IPID_ANALYSIS_ITDK_SOURCE_DIR"),
    itdk_ifaces: Path | None = typer.Option(None, envvar="IPID_ANALYSIS_ITDK_IFACES"),
    ripe_source: str = typer.Option(DEFAULT_SOURCE, envvar="IPID_ANALYSIS_RIPE_SOURCE"),
    ripe_measurement_ids: Path | None = typer.Option(
        None, envvar="IPID_ANALYSIS_RIPE_MEASUREMENT_IDS"
    ),
    ripe_input_dir: Path | None = typer.Option(None, envvar="IPID_ANALYSIS_RIPE_INPUT_DIR"),
    ripe_campaign_start: str | None = typer.Option(
        None, envvar="IPID_ANALYSIS_RIPE_CAMPAIGN_START"
    ),
    ripe_dump_samples: int = typer.Option(
        DEFAULT_DUMP_SAMPLES, min=1, envvar="IPID_ANALYSIS_RIPE_DUMP_SAMPLES"
    ),
    ripe_preprocess_workers: int = typer.Option(
        DEFAULT_PREPROCESS_WORKERS, min=1, envvar="IPID_ANALYSIS_RIPE_WORKERS"
    ),
    skip_itdk: bool = typer.Option(False, "--skip-itdk"),
    skip_ripe: bool = typer.Option(False, "--skip-ripe"),
) -> None:
    """Reuse strategies/OS data and regenerate only network-role outputs."""
    manifest = load_manifest(manifest_path)
    itdk = None
    if not skip_itdk:
        itdk = prepare_itdk(
            release=itdk_release,
            topology=itdk_topology,
            source_dir=itdk_source_dir,
            ifaces=itdk_ifaces,
        )
    ripe = None
    if not skip_ripe:
        campaign_start = (
            parse_datetime(ripe_campaign_start)
            if ripe_campaign_start is not None
            else infer_campaign_start(manifest)
        )
        ripe = prepare_ripe_atlas(
            campaign_start=campaign_start,
            source=ripe_source,
            measurement_ids_file=ripe_measurement_ids,
            input_dir=ripe_input_dir,
            dump_samples=ripe_dump_samples,
            preprocess_workers=ripe_preprocess_workers,
        )

    sources = list(iter_strategy_merges(manifest))
    connection = resolve(manifest, "tcp.ipid.connection.rt-based.base")
    if connection is not None:
        sources.append(connection)
    if not sources:
        raise typer.BadParameter("manifest has no canonical merged or TCP connection source")

    produced = 0
    parquet_compression = None if compression == "none" else compression
    for source in sources:
        strategies = source.artifact_path(PROCESSED_DATA_DIR, "strategies")
        if not strategies.is_file():
            logger.warning(
                f"[{source.target}] missing existing strategies ({strategies}) -- skipped"
            )
            continue
        if itdk is not None:
            outputs = render_itdk_analysis(source, itdk, threads=threads)
            logger.success(f"[{source.target}] CAIDA role outputs -> {outputs.role_pdf}")
            produced += 1
        if ripe is not None:
            outputs = render_ripe_atlas_analysis(source, ripe, itdk=itdk, threads=threads)
            logger.success(f"[{source.target}] RIPE role outputs -> {outputs.role_pdf}")
            produced += 1
        os_measurement_id = resolve_os_measurement_id(manifest, source.protocol)
        if os_measurement_id is not None:
            if itdk is not None:
                outputs = render_itdk_os_analysis(
                    source,
                    itdk,
                    os_measurement_id,
                    compression=parquet_compression,
                    threads=threads,
                )
                logger.success(f"[{source.target}] CAIDA OS role output -> {outputs.role_pdf}")
            if ripe is not None:
                outputs = render_ripe_atlas_os_analysis(
                    source,
                    ripe,
                    os_measurement_id,
                    compression=parquet_compression,
                    threads=threads,
                )
                logger.success(f"[{source.target}] RIPE OS role output -> {outputs.role_pdf}")
    logger.success(f"network-role analysis complete: {produced} strategy-role output set(s)")


if __name__ == "__main__":
    app()
