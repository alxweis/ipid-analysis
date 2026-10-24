"""Manifest-driven postprocessing + plotting: run every step for every ipid
measurement present in the manifest.

    python ipid_analysis/postprocess.py data.json

Per measurement:
  1. coverage.json          (ipid_analysis.coverage)
  2. strategies.pq          (ipid_analysis.strategies)
  3. probing-intervals.pq   (ipid_analysis.probing_intervals)
  4. increments.pq          (ipid_analysis.increments)
  5. strategies PDF + JSON          (ipid_analysis.plot_strategies)
  6. probing-intervals PDF + JSON   (ipid_analysis.plot_probing_intervals)
  7. increments CDF PDF + JSON      (ipid_analysis.plot_increments)

Missing measurements (combination not in the manifest, or raw data absent) are
skipped with a warning instead of aborting the run.

After the individual measurements, every available no-connection RT-base and
fixed-interval-mass pair is merged and its strategy distribution is plotted.
The same pair also produces a paper plot showing how the RT-based
``UNCLASSIFIED`` population is refined by the fixed-interval mass measurement.
TCP campaigns with an RT-based connection-oriented base measurement produce an
additional version with that measurement's strategy distribution as a third bar.
Protocol campaigns with an OS measurement also produce a processed per-IP
OS-group Parquet file and a row-normalized heatmap of merged IP-ID selection
strategies by general-purpose, network/appliance, and embedded/RTOS group.
TCP campaigns with a connection-oriented RT-based base measurement produce the
same OS heatmap for that individual strategy result.
TCP campaigns also produce a paper plot of the merged strategy distribution
split by the ZMap ``synack`` and ``rst`` reply classifications.
Canonical merged results and the TCP RT-based connection result are joined to
the configured CAIDA ITDK release to produce observed transit-role plots and
cross-interface strategy-consistency artifacts. When the protocol campaign also
contains an OS measurement, the same ITDK roles are joined to resolved canonical
OS groups and rendered as a second compact observed-role distribution.
The same canonical IP-ID results are joined to observed roles extracted from
the 28 complete UTC days of RIPE Atlas traceroutes preceding the campaign.
Every available RT-base/fixed-interval-base pair is also compared with the
three compact paper figures in :mod:`ipid_analysis.paper_figures`.
"""

from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import subprocess

from loguru import logger
import typer

from ipid_analysis.artifacts import assert_complete, fingerprints, provenance, write_text
from ipid_analysis.caida_itdk import (
    DEFAULT_RELEASE as DEFAULT_ITDK_RELEASE,
)
from ipid_analysis.caida_itdk import (
    DEFAULT_TOPOLOGY as DEFAULT_ITDK_TOPOLOGY,
)
from ipid_analysis.caida_itdk import prepare_itdk
from ipid_analysis.comparison import iter_base_comparisons
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.coverage import write_coverage
from ipid_analysis.increments import extract_increments
from ipid_analysis.manifest import (
    iter_ipid_measurements,
    iter_random_reproducibility,
    load_manifest,
    resolve,
)
from ipid_analysis.missing_reply_analysis import analyze_missing_replies
from ipid_analysis.paper_figures import (
    default_maxmind_database,
    render_increment_comparison,
    render_probing_interval_comparison,
    render_strategy_intersection,
)
from ipid_analysis.plot_increments import render as render_increments_plot
from ipid_analysis.plot_itdk_os import render_itdk_os_analysis
from ipid_analysis.plot_itdk_strategy import render_itdk_analysis
from ipid_analysis.plot_os_group_strategy import (
    render as render_os_strategy_plot,
)
from ipid_analysis.plot_os_group_strategy import (
    render_measurement as render_os_strategy_measurement_plot,
)
from ipid_analysis.plot_os_group_strategy import (
    resolve_os_measurement_id,
)
from ipid_analysis.plot_probing_intervals import render as render_intervals_plot
from ipid_analysis.plot_ripe_atlas_os import render_ripe_atlas_os_analysis
from ipid_analysis.plot_ripe_atlas_strategy import render_ripe_atlas_analysis
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
from ipid_analysis.random_reproducibility_analysis import (
    evaluate_random_reproducibility,
)
from ipid_analysis.ripe_atlas import (
    DEFAULT_DUMP_SAMPLES,
    DEFAULT_PREPROCESS_WORKERS,
    infer_campaign_start,
    parse_datetime,
    prepare_ripe_atlas,
)
from ipid_analysis.ripe_atlas import (
    DEFAULT_SOURCE as DEFAULT_RIPE_SOURCE,
)
from ipid_analysis.runtime import start_run, validate_storage
from ipid_analysis.strategies import classify_measurement
from ipid_analysis.strategy_merge import iter_strategy_merges, merge_strategies

app = typer.Typer()


def required_core_artifacts(
    manifest: dict, *, processed_root=PROCESSED_DATA_DIR, figures_root=FIGURES_DIR
):
    """Shared worker/CLI contract for the mandatory canonical measurement outputs."""
    paths = []
    for measurement in iter_ipid_measurements(manifest):
        paths.extend(
            measurement.artifact_path(processed_root, kind, ext)
            for kind, ext in (
                ("coverage", "json"),
                ("strategies", "pq"),
                ("increments", "pq"),
                ("probing-intervals", "pq"),
            )
        )
        paths.extend(
            measurement.artifact_path(figures_root, kind, ext)
            for kind in ("strategies", "increments", "probing-intervals")
            for ext in ("pdf", "json")
        )
    for merge in iter_strategy_merges(manifest):
        paths.append(merge.artifact_path(processed_root, "strategies"))
        paths.append(merge.artifact_path(figures_root, "strategies", "pdf"))
    return paths


@contextmanager
def record_stage(stages, name, required=False, target=None):
    record = {"stage": name, "required": required, "target": target, "status": "running"}
    stages.append(record)
    try:
        yield
    except Exception as exc:
        record.update(status="failed", error=str(exc))
        if not required and "no fixed-interval Mass measurement" in str(exc):
            record["status"] = "skipped"
        raise
    else:
        record["status"] = "completed"


@app.command()
def main(
    manifest_path: Path = typer.Argument(..., help="measurement manifest JSON (e.g. data.json)"),
    strict: bool = typer.Option(False, help="fail if any mandatory analysis is incomplete"),
    status_file: Path | None = typer.Option(None, help="machine-readable completion report"),
    batch_size: int = typer.Option(1_000_000, help="rows per batch"),
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
    threads: int = typer.Option(0, help="DuckDB threads (0 = all cores)"),
    reclassify: bool = typer.Option(
        False,
        "--reclassify",
        help="ignore persisted strategies and classify every measurement again",
    ),
    maxmind_db: Path | None = typer.Option(
        None,
        help="GeoLite2/GeoIP2 .mmdb for continent plots; also read from IPID_MAXMIND_DB",
    ),
    itdk_release: str = typer.Option(
        DEFAULT_ITDK_RELEASE,
        envvar="IPID_ANALYSIS_ITDK_RELEASE",
        help="pinned CAIDA ITDK release",
    ),
    itdk_topology: str = typer.Option(
        DEFAULT_ITDK_TOPOLOGY,
        envvar="IPID_ANALYSIS_ITDK_TOPOLOGY",
        help="CAIDA IPv4 topology prefix",
    ),
    itdk_source_dir: Path | None = typer.Option(
        None,
        envvar="IPID_ANALYSIS_ITDK_SOURCE_DIR",
        help="local ITDK release directory instead of public download",
    ),
    itdk_ifaces: Path | None = typer.Option(
        None,
        envvar="IPID_ANALYSIS_ITDK_IFACES",
        help="local ITDK .ifaces.bz2 instead of public download",
    ),
    skip_itdk: bool = typer.Option(
        False,
        "--skip-itdk",
        envvar="IPID_ANALYSIS_SKIP_ITDK",
        help="skip automatic CAIDA ITDK analysis",
    ),
    ripe_source: str = typer.Option(
        DEFAULT_RIPE_SOURCE,
        envvar="IPID_ANALYSIS_RIPE_SOURCE",
        help="RIPE Atlas source: daily-dumps or api",
    ),
    ripe_measurement_ids: Path | None = typer.Option(
        None,
        envvar="IPID_ANALYSIS_RIPE_MEASUREMENT_IDS",
        help="measurement ID file required for the RIPE API source",
    ),
    ripe_input_dir: Path | None = typer.Option(
        None,
        envvar="IPID_ANALYSIS_RIPE_INPUT_DIR",
        help="local RIPE JSONL/BZip2 directory instead of downloading",
    ),
    ripe_campaign_start: str | None = typer.Option(
        None,
        envvar="IPID_ANALYSIS_RIPE_CAMPAIGN_START",
        help="override campaign start (ISO-8601); normally inferred from the manifest",
    ),
    ripe_dump_samples: int = typer.Option(
        DEFAULT_DUMP_SAMPLES,
        min=1,
        envvar="IPID_ANALYSIS_RIPE_DUMP_SAMPLES",
        help="evenly distributed hourly daily-dump files in the RIPE window",
    ),
    ripe_preprocess_workers: int = typer.Option(
        DEFAULT_PREPROCESS_WORKERS,
        min=1,
        envvar="IPID_ANALYSIS_RIPE_WORKERS",
        help="parallel RIPE Atlas preprocessing processes",
    ),
    skip_ripe: bool = typer.Option(
        False,
        "--skip-ripe",
        envvar="IPID_ANALYSIS_SKIP_RIPE",
        help="skip automatic RIPE Atlas analysis",
    ),
) -> None:
    start_run()
    validate_storage()
    warnings = []
    stages = []
    sink = logger.add(
        lambda message: warnings.append(str(message.record["message"])),
        level="WARNING",
        filter=lambda record: record["level"].no >= 30,
    )
    required_extra = []
    reproducibility_ok = 0
    missing_ok = False
    manifest = load_manifest(manifest_path)
    measurements = iter_ipid_measurements(manifest)
    logger.info(f"{len(measurements)} ipid measurement(s) in {manifest_path}")

    itdk = None
    if skip_itdk:
        logger.info("CAIDA ITDK analysis disabled by --skip-itdk")
    else:
        try:
            with record_stage(stages, "prepare_itdk", required=False, target=None):
                itdk = prepare_itdk(
                    release=itdk_release,
                    topology=itdk_topology,
                    source_dir=itdk_source_dir,
                    ifaces=itdk_ifaces,
                )
        except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
            logger.warning(f"external role preparation failed: {exc}")

    ripe = None
    if skip_ripe:
        logger.info("RIPE Atlas analysis disabled by --skip-ripe")
    else:
        try:
            with record_stage(stages, "prepare_ripe_atlas", required=False, target=None):
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
        except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
            logger.warning(f"external role preparation failed: {exc}")

    comp = None if compression == "none" else compression
    ok, skipped = 0, 0
    for m in measurements:
        try:
            with record_stage(stages, "measurement_pipeline", required=True, target=m.target):
                write_coverage(m, manifest)
                classify_measurement(
                    m,
                    batch_size=batch_size,
                    compression=comp,
                    threads=threads,
                    reclassify=reclassify,
                )
                extract_probing_intervals(m, compression=comp or "zstd", threads=threads)
                extract_increments(m, batch_size=batch_size, compression=comp, threads=threads)
                render_strategies_plot(m)
                render_intervals_plot(m)
                render_increments_plot(m)
                ok += 1
        except FileNotFoundError as exc:
            logger.warning(f"[{m.target}] missing input ({exc}) -- skipped")
            skipped += 1

    try:
        with record_stage(
            stages,
            "analyze_missing_replies",
            required=any(
                m.interval == "fixed-interval" and m.scale == "mass" for m in measurements
            ),
            target=None,
        ):
            missing_outputs = analyze_missing_replies(
                manifest_path,
                compression=comp,
                threads=threads,
            )
            missing_ok = True
            required_extra.extend(missing_outputs.values())
            logger.success(f"automatic Mass missing-reply analysis -> {missing_outputs['pdf']}")
    except (FileNotFoundError, ValueError) as exc:
        logger.warning(f"automatic Mass missing-reply analysis failed ({exc}) -- skipped")

    for specification in iter_random_reproducibility(manifest):
        cohort_path = (
            RAW_DATA_DIR
            / "ipid"
            / specification.baseline_measurement_id
            / specification.cohort_file
        )
        try:
            with record_stage(
                stages, "evaluate_random_reproducibility", required=True, target=None
            ):
                outputs = evaluate_random_reproducibility(
                    manifest_path,
                    list(specification.repeat_ids),
                    target=specification.baseline_target,
                    cohort_path=cohort_path,
                )
                reproducibility_ok += 1
                required_extra.extend(outputs.values())
                logger.success(
                    f"[{specification.baseline_target}] automatic RANDOM reproducibility "
                    f"-> {outputs['pdf']}"
                )
        except (FileNotFoundError, ValueError) as exc:
            logger.warning(
                f"[{specification.baseline_target}] automatic RANDOM reproducibility "
                f"failed ({exc}) -- skipped"
            )

    connection = resolve(
        manifest,
        "tcp.ipid.connection.rt-based.base",
    )
    if connection is not None:
        if ripe is not None:
            try:
                with record_stage(
                    stages, "render_ripe_atlas_analysis", required=False, target=connection.target
                ):
                    outputs = render_ripe_atlas_analysis(
                        connection,
                        ripe,
                        itdk=itdk,
                        threads=threads,
                    )
                    logger.success(
                        f"[{connection.target}] RIPE Atlas role plot -> {outputs.role_pdf}"
                    )
            except (FileNotFoundError, ValueError) as exc:
                logger.warning(
                    f"[{connection.target}] RIPE Atlas role analysis failed ({exc}) -- skipped"
                )
        if itdk is not None:
            try:
                with record_stage(
                    stages, "render_itdk_analysis", required=False, target=connection.target
                ):
                    outputs = render_itdk_analysis(connection, itdk, threads=threads)
                    logger.success(
                        f"[{connection.target}] CAIDA ITDK role plot -> {outputs.role_pdf}; "
                        f"cross-interface consistency -> {outputs.consistency_json}"
                    )
            except FileNotFoundError as exc:
                logger.warning(
                    f"[{connection.target}] missing CAIDA ITDK analysis input ({exc}) -- skipped"
                )
        os_measurement_id = resolve_os_measurement_id(manifest, "tcp")
        if os_measurement_id is not None:
            try:
                with record_stage(
                    stages,
                    "render_os_strategy_measurement_plot",
                    required=False,
                    target=connection.target,
                ):
                    connection_os_pdf, _, _ = render_os_strategy_measurement_plot(
                        connection,
                        os_measurement_id,
                        compression=comp,
                        threads=threads,
                    )
                    logger.success(
                        f"[{connection.target}] connection-oriented OS strategy heatmap "
                        f"-> {connection_os_pdf}"
                    )
            except (FileNotFoundError, ValueError) as exc:
                logger.warning(
                    f"[{connection.target}] connection-oriented OS strategy heatmap "
                    f"failed ({exc}) -- skipped"
                )
            if itdk is not None:
                try:
                    with record_stage(
                        stages, "render_itdk_os_analysis", required=False, target=connection.target
                    ):
                        outputs = render_itdk_os_analysis(
                            connection,
                            itdk,
                            os_measurement_id,
                            compression=comp,
                            threads=threads,
                        )
                        logger.success(
                            f"[{connection.target}] connection-oriented CAIDA ITDK OS role plot "
                            f"-> {outputs.role_pdf}"
                        )
                except (FileNotFoundError, ValueError) as exc:
                    logger.warning(
                        f"[{connection.target}] connection-oriented CAIDA ITDK OS role plot "
                        f"failed ({exc}) -- skipped"
                    )
            if ripe is not None:
                try:
                    with record_stage(
                        stages,
                        "render_ripe_atlas_os_analysis",
                        required=False,
                        target=connection.target,
                    ):
                        ripe_os_outputs = render_ripe_atlas_os_analysis(
                            connection,
                            ripe,
                            os_measurement_id,
                            compression=comp,
                            threads=threads,
                        )
                        logger.success(
                            f"[{connection.target}] connection-oriented RIPE Atlas OS role plot "
                            f"-> {ripe_os_outputs.role_pdf}"
                        )
                except (FileNotFoundError, ValueError) as exc:
                    logger.warning(
                        f"[{connection.target}] connection-oriented RIPE Atlas OS role plot "
                        f"failed ({exc}) -- skipped"
                    )

    merged_ok, merged_skipped = 0, 0
    for merge in iter_strategy_merges(manifest):
        try:
            with record_stage(stages, "merge_pipeline", required=True, target=merge.target):
                output, stats = merge_strategies(
                    merge,
                    batch_size=batch_size,
                    compression=comp,
                    threads=threads,
                )
                render_merged_strategies_plot(merge)
                itdk_message = ""
                if itdk is not None:
                    try:
                        with record_stage(
                            stages, "render_itdk_analysis", required=False, target=merge.target
                        ):
                            itdk_outputs = render_itdk_analysis(merge, itdk, threads=threads)
                            itdk_message = (
                                f"; CAIDA ITDK role plot -> {itdk_outputs.role_pdf}; "
                                f"cross-interface consistency -> {itdk_outputs.consistency_json}"
                            )
                    except (OSError, ValueError) as exc:
                        logger.warning(f"[{merge.target}] CAIDA ITDK enrichment failed: {exc}")
                ripe_message = ""
                if ripe is not None:
                    try:
                        with record_stage(
                            stages,
                            "render_ripe_atlas_analysis",
                            required=False,
                            target=merge.target,
                        ):
                            ripe_outputs = render_ripe_atlas_analysis(
                                merge,
                                ripe,
                                itdk=itdk,
                                threads=threads,
                            )
                            ripe_message = f"; RIPE Atlas role plot -> {ripe_outputs.role_pdf}"
                    except (FileNotFoundError, ValueError) as exc:
                        logger.warning(
                            f"[{merge.target}] RIPE Atlas role analysis failed ({exc}) -- skipped"
                        )
                refinement_pdf, _, _ = render_strategy_refinement_plot(
                    merge,
                    compression=comp,
                    threads=threads,
                )
                os_pdf = None
                itdk_os_pdf = None
                ripe_os_pdf = None
                os_measurement_id = resolve_os_measurement_id(manifest, merge.protocol)
                if os_measurement_id is not None:
                    try:
                        with record_stage(
                            stages, "render_os_strategy_plot", required=False, target=merge.target
                        ):
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
                    if itdk is not None:
                        try:
                            with record_stage(
                                stages,
                                "render_itdk_os_analysis",
                                required=False,
                                target=merge.target,
                            ):
                                itdk_os_outputs = render_itdk_os_analysis(
                                    merge,
                                    itdk,
                                    os_measurement_id,
                                    compression=comp,
                                    threads=threads,
                                )
                                itdk_os_pdf = itdk_os_outputs.role_pdf
                        except (FileNotFoundError, ValueError) as exc:
                            logger.warning(
                                f"[{merge.target}] CAIDA ITDK OS role plot failed ({exc}) -- skipped"
                            )
                    if ripe is not None:
                        try:
                            with record_stage(
                                stages,
                                "render_ripe_atlas_os_analysis",
                                required=False,
                                target=merge.target,
                            ):
                                ripe_os_outputs = render_ripe_atlas_os_analysis(
                                    merge,
                                    ripe,
                                    os_measurement_id,
                                    compression=comp,
                                    threads=threads,
                                )
                                ripe_os_pdf = ripe_os_outputs.role_pdf
                        except (FileNotFoundError, ValueError) as exc:
                            logger.warning(
                                f"[{merge.target}] RIPE Atlas OS role plot failed ({exc}) -- skipped"
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
                itdk_os_message = (
                    f"; CAIDA ITDK OS role plot -> {itdk_os_pdf}"
                    if itdk_os_pdf is not None
                    else ""
                )
                ripe_os_message = (
                    f"; RIPE Atlas OS role plot -> {ripe_os_pdf}"
                    if ripe_os_pdf is not None
                    else ""
                )
                tcp_flags_message = (
                    f"; TCP flags by strategy -> {tcp_flags_pdf}"
                    if tcp_flags_pdf is not None
                    else ""
                )
                logger.success(
                    f"[{merge.target}] {stats.rows:,} merged IPs, "
                    f"{stats.not_enough_samples:,} not enough samples -> {output}; "
                    f"strategy refinement -> {refinement_pdf}"
                    f"{connection_message}{tcp_flags_message}{os_message}"
                    f"{itdk_message}{itdk_os_message}{ripe_message}{ripe_os_message}"
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
                with record_stage(stages, "comparison", required=True, target=comparison.target):
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
                with record_stage(
                    stages,
                    "render_probing_interval_comparison",
                    required=False,
                    target=comparison.target,
                ):
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

    required = list(required_extra) + required_core_artifacts(
        manifest, processed_root=PROCESSED_DATA_DIR, figures_root=FIGURES_DIR
    )
    missing = []
    for artifact in dict.fromkeys(required):
        try:
            if not artifact.is_file():
                raise FileNotFoundError(artifact)
            assert_complete(artifact)
        except (OSError, ValueError) as exc:
            missing.append(str(exc))
    expected_reproducibility = len(list(iter_random_reproducibility(manifest)))
    if reproducibility_ok != expected_reproducibility:
        missing.append("RANDOM reproducibility analysis incomplete")
    if (
        any(m.interval == "fixed-interval" and m.scale == "mass" for m in measurements)
        and not missing_ok
    ):
        missing.append("Mass missing-reply analysis incomplete")
    required_complete = (
        bool(measurements)
        and ok == len(measurements)
        and not missing
        and all(stage["status"] == "completed" for stage in stages if stage["required"])
    )
    report = {
        "version": 1,
        "required_complete": required_complete,
        "manifest": fingerprints([manifest_path]),
        "artifacts": [str(path.resolve()) for path in dict.fromkeys(required) if path.is_file()],
        "missing": missing,
        "warnings": warnings,
        "stages": stages,
        "optional": {
            "itdk": "skipped" if skip_itdk else "prepared" if itdk else "failed",
            "ripe": "skipped" if skip_ripe else "prepared" if ripe else "failed",
        },
        **provenance(),
    }
    logger.remove(sink)
    if status_file is not None:
        write_text(status_file, json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if strict and not required_complete:
        raise RuntimeError(
            "mandatory postprocessing incomplete: "
            + "; ".join(missing or ["measurements skipped"])
        )

    logger.success(
        f"postprocessing + plotting done: {ok} measurements ok, {skipped} skipped; "
        f"{merged_ok} merges ok, {merged_skipped} skipped; "
        f"{comparison_ok} comparisons produced output, {comparison_skipped} skipped"
    )


if __name__ == "__main__":
    app()
