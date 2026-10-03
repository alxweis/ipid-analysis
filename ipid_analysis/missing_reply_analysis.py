"""Observed missing-reply distribution of persisted FI Mass measurements.

The measurement stage retains the fixed position of every request in
``IPID_SEQUENCE`` and writes a non-numeric marker for requests without a usable
reply.  This module counts those positions for every stored fixed-interval Mass
sequence.  It intentionally describes *observed missing replies*, not network
packet loss: a missing value may also result from filtering, rate limiting,
capture loss, or a timeout.

Run for every FI Mass target present in a manifest::

    python -m ipid_analysis.missing_reply_analysis data.json

The command writes a compact aggregate Parquet file plus JSON metadata and a
two-panel histogram/ECDF figure.  Measurement attempts rejected before upload
because they did not meet the configured minimum reply rate are not present in
``ipid.pq``; consequently, the reported distribution is conditioned on the
persisted population and excludes the high-missingness tail.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path

import duckdb
from loguru import logger
import matplotlib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer
import yaml

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator

from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.manifest import (
    IpidMeasurement,
    iter_ipid_measurements,
    load_manifest,
)
from ipid_analysis.paper_figures import (
    COMPACT_PAPER_MAJOR_TICK_LENGTH,
    COMPACT_PAPER_MINOR_TICK_LENGTH,
    COMPACT_PAPER_PDF_PADDING_INCHES,
    COMPACT_PAPER_STROKE_WIDTH,
    configure_paper_style,
)
from ipid_analysis.strategies import (
    INPUT_NAME,
    SNAPSHOT_NAME,
    load_config,
)

app = typer.Typer()

OUTPUT_DIRECTORY = "missing-replies"
OUTPUT_STEM = "mass-missing-replies"
MANIFEST_ARGUMENT = typer.Argument(..., help="measurement manifest JSON")

OUTPUT_SCHEMA = pa.schema(
    [
        ("TARGET", pa.string()),
        ("PROTOCOL", pa.string()),
        ("CONNECTION_MODE", pa.string()),
        ("MEASUREMENT_ID", pa.string()),
        ("SEQUENCE_LENGTH", pa.int16()),
        ("MISSING_REPLY_COUNT", pa.int16()),
        ("REPLY_COUNT", pa.int16()),
        ("SEQUENCE_COUNT", pa.int64()),
        ("TOTAL_SEQUENCE_COUNT", pa.int64()),
        ("PERCENTAGE", pa.float64()),
        ("CUMULATIVE_COUNT", pa.int64()),
        ("CUMULATIVE_PERCENTAGE", pa.float64()),
    ]
)


def _write_table(table: pa.Table, output_path: Path, compression: str | None) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    pq.write_table(table, temporary, compression=compression)
    temporary.replace(output_path)
    return output_path


def _write_json(output_path: Path, value: dict) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(output_path)
    return output_path


def _minimum_reply_rate(snapshot_path: Path) -> float | None:
    """Return the measurement-stage FI retention threshold when recorded."""
    data = yaml.safe_load(snapshot_path.read_text())
    try:
        value = data["fixed_interval"]["minimum_reply_rate"]
    except (KeyError, TypeError):
        return None
    rate = float(value)
    if not 0.0 <= rate <= 1.0:
        raise ValueError(f"{snapshot_path}: invalid fixed_interval.minimum_reply_rate={rate}")
    return rate


def _fixed_interval_mass_measurements(manifest: dict) -> list[IpidMeasurement]:
    return [
        measurement
        for measurement in iter_ipid_measurements(manifest)
        if measurement.interval == "fixed-interval" and measurement.scale == "mass"
    ]


def _measurement_distribution(
    measurement: IpidMeasurement,
    *,
    raw_root: Path,
    threads: int,
) -> tuple[list[dict], dict]:
    raw_directory = raw_root / measurement.input_key
    input_path = raw_directory / INPUT_NAME
    snapshot_path = raw_directory / SNAPSHOT_NAME
    for path in (input_path, snapshot_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    config = load_config(snapshot_path)
    sequence_length = config.sequence_length
    if sequence_length < 1:
        raise ValueError(f"{snapshot_path}: sequence length must be positive")

    con = duckdb.connect(config={"threads": threads} if threads else {})
    try:
        rows = con.execute(
            """
            WITH parsed AS (
                SELECT
                    IP_ADDR,
                    string_split(IPID_SEQUENCE, ',') AS values
                FROM read_parquet($input)
            ), counted AS (
                SELECT
                    IP_ADDR,
                    len(values)::INTEGER AS sequence_length,
                    len(list_filter(values, value -> TRY_CAST(value AS INTEGER) IS NULL))
                        ::INTEGER AS missing_replies
                FROM parsed
            )
            SELECT sequence_length, missing_replies, count(*)::BIGINT AS sequence_count
            FROM counted
            GROUP BY sequence_length, missing_replies
            ORDER BY sequence_length, missing_replies
            """,
            {"input": str(input_path)},
        ).fetchall()
        total_rows, distinct_ips = map(
            int,
            con.execute(
                """
                SELECT count(*), count(DISTINCT IP_ADDR)
                FROM read_parquet($input)
                """,
                {"input": str(input_path)},
            ).fetchone(),
        )
    finally:
        con.close()

    if total_rows == 0:
        raise ValueError(f"{input_path}: Mass measurement contains no persisted sequences")
    if total_rows != distinct_ips:
        raise ValueError(f"{input_path}: duplicate IP addresses in Mass measurement")

    observed_lengths = sorted({int(length) for length, _, _ in rows})
    if observed_lengths != [sequence_length]:
        raise ValueError(
            f"{input_path}: expected {sequence_length} fixed positions per sequence, "
            f"observed lengths {observed_lengths}"
        )

    observed_counts = {int(missing): int(count) for _, missing, count in rows}
    minimum_reply_rate = _minimum_reply_rate(snapshot_path)
    configured_minimum_replies = (
        math.ceil(minimum_reply_rate * sequence_length) if minimum_reply_rate is not None else None
    )
    configured_maximum_missing = (
        sequence_length - configured_minimum_replies
        if configured_minimum_replies is not None
        else None
    )
    maximum_observed_missing = max(observed_counts)
    maximum_reported_missing = max(
        maximum_observed_missing,
        configured_maximum_missing or 0,
    )

    output_rows: list[dict] = []
    cumulative_count = 0
    for missing_replies in range(maximum_reported_missing + 1):
        count = observed_counts.get(missing_replies, 0)
        cumulative_count += count
        output_rows.append(
            {
                "TARGET": measurement.target,
                "PROTOCOL": measurement.protocol,
                "CONNECTION_MODE": measurement.connection_mode,
                "MEASUREMENT_ID": measurement.measurement_id,
                "SEQUENCE_LENGTH": sequence_length,
                "MISSING_REPLY_COUNT": missing_replies,
                "REPLY_COUNT": sequence_length - missing_replies,
                "SEQUENCE_COUNT": count,
                "TOTAL_SEQUENCE_COUNT": total_rows,
                "PERCENTAGE": 100.0 * count / total_rows,
                "CUMULATIVE_COUNT": cumulative_count,
                "CUMULATIVE_PERCENTAGE": 100.0 * cumulative_count / total_rows,
            }
        )

    complete_count = observed_counts.get(0, 0)
    incomplete_count = total_rows - complete_count
    weighted_missing = sum(missing * count for missing, count in observed_counts.items())
    below_configured_minimum_count = (
        sum(
            count
            for missing, count in observed_counts.items()
            if missing > configured_maximum_missing
        )
        if configured_maximum_missing is not None
        else None
    )
    metadata = {
        "target": measurement.target,
        "protocol": measurement.protocol,
        "connection_mode": measurement.connection_mode,
        "measurement_id": measurement.measurement_id,
        "input": str(input_path),
        "snapshot": str(snapshot_path),
        "connection_count": config.connection_count,
        "requests_per_connection": config.requests_per_connection,
        "sequence_length": sequence_length,
        "configured_minimum_reply_rate": minimum_reply_rate,
        "configured_minimum_replies": configured_minimum_replies,
        "configured_maximum_missing_replies": configured_maximum_missing,
        "persisted_sequence_count": total_rows,
        "complete_sequence_count": complete_count,
        "complete_sequence_percentage": 100.0 * complete_count / total_rows,
        "incomplete_sequence_count": incomplete_count,
        "incomplete_sequence_percentage": 100.0 * incomplete_count / total_rows,
        "mean_missing_replies": weighted_missing / total_rows,
        "maximum_observed_missing_replies": maximum_observed_missing,
        "sequences_below_configured_minimum_count": below_configured_minimum_count,
        "distribution": {
            str(row["MISSING_REPLY_COUNT"]): {
                "sequence_count": row["SEQUENCE_COUNT"],
                "percentage": row["PERCENTAGE"],
                "cumulative_percentage": row["CUMULATIVE_PERCENTAGE"],
            }
            for row in output_rows
        },
    }
    return output_rows, metadata


def _measurement_label(metadata: dict) -> str:
    connection = (
        "No connection" if metadata["connection_mode"] == "no-connection" else "Connection"
    )
    return f"{metadata['protocol'].upper()} · {connection}"


def render_missing_reply_figure(
    table: pa.Table,
    metadata: dict,
    output_path: Path,
) -> Path:
    """Render exact and cumulative missing-reply percentages."""
    configure_paper_style()
    figure, axes = plt.subplots(1, 2, figsize=(7.16, 2.35), sharex=True)
    colors = plt.get_cmap("tab10")

    rows = table.to_pylist()
    measurement_count = len(metadata["measurements"])
    bar_width = min(0.8 / measurement_count, 0.7)
    for index, measurement in enumerate(metadata["measurements"]):
        target = measurement["target"]
        target_rows = [row for row in rows if row["TARGET"] == target]
        x = np.asarray([row["MISSING_REPLY_COUNT"] for row in target_rows])
        percentages = np.asarray([row["PERCENTAGE"] for row in target_rows])
        cumulative = np.asarray([row["CUMULATIVE_PERCENTAGE"] for row in target_rows])
        label = _measurement_label(measurement)
        color = colors(index % 10)
        bar_offset = (index - (measurement_count - 1) / 2.0) * bar_width
        axes[0].bar(
            x + bar_offset,
            percentages,
            width=bar_width,
            label=label,
            color=color,
            edgecolor="white",
            linewidth=0.25,
        )
        axes[1].step(x, cumulative, where="post", linewidth=1.15, label=label, color=color)

    maximum_missing = max(int(row["MISSING_REPLY_COUNT"]) for row in rows)
    for axis in axes:
        axis.set_xlim(-0.35, maximum_missing + 0.35)
        axis.set_ylim(bottom=0.0)
        axis.xaxis.set_major_locator(MultipleLocator(2 if maximum_missing > 10 else 1))
        axis.xaxis.set_minor_locator(MultipleLocator(1))
        axis.yaxis.set_minor_locator(MultipleLocator(5))
        axis.grid(True, which="major", color="#D0D0D0", linewidth=0.45, linestyle="--")
        axis.grid(True, which="minor", color="#E7E7E7", linewidth=0.35, linestyle=":")
        axis.tick_params(
            which="major",
            width=COMPACT_PAPER_STROKE_WIDTH,
            length=COMPACT_PAPER_MAJOR_TICK_LENGTH,
        )
        axis.tick_params(
            which="minor",
            width=COMPACT_PAPER_STROKE_WIDTH,
            length=COMPACT_PAPER_MINOR_TICK_LENGTH,
        )
        for spine in axis.spines.values():
            spine.set_linewidth(COMPACT_PAPER_STROKE_WIDTH)

    axes[0].set_ylabel("Sequences [%]")
    axes[1].set_ylabel("Cumulative percentage [%]")
    figure.supxlabel("Missing replies per stored Mass sequence", y=0.015)
    axes[1].set_ylim(0.0, 101.0)

    handles, labels = axes[0].get_legend_handles_labels()
    figure.legend(
        handles,
        labels,
        loc="upper center",
        ncol=min(len(labels), 3),
        frameon=False,
        bbox_to_anchor=(0.5, 1.015),
    )
    figure.subplots_adjust(left=0.075, right=0.995, bottom=0.23, top=0.84, wspace=0.27)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(
        output_path,
        bbox_inches="tight",
        pad_inches=COMPACT_PAPER_PDF_PADDING_INCHES,
        metadata={
            "Title": "Observed missing replies in persisted FI Mass sequences",
            "Subject": "Exact and cumulative missing-reply distribution",
            "Creator": "ipid-analysis",
        },
    )
    plt.close(figure)
    return output_path


def analyze_missing_replies(
    manifest_path: Path,
    *,
    raw_root: Path = RAW_DATA_DIR,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    compression: str | None = "zstd",
    threads: int = 0,
) -> dict[str, Path]:
    """Aggregate all FI Mass targets in ``manifest_path`` and render outputs."""
    manifest = load_manifest(manifest_path)
    measurements = _fixed_interval_mass_measurements(manifest)
    if not measurements:
        raise ValueError(f"{manifest_path}: no fixed-interval Mass measurement in manifest")

    rows: list[dict] = []
    measurement_metadata: list[dict] = []
    for measurement in measurements:
        logger.info(f"[{measurement.target}] counting observed missing replies")
        measurement_rows, metadata = _measurement_distribution(
            measurement,
            raw_root=raw_root,
            threads=threads,
        )
        rows.extend(measurement_rows)
        measurement_metadata.append(metadata)

    table = pa.Table.from_pylist(rows, schema=OUTPUT_SCHEMA)
    aggregate_path = processed_root / OUTPUT_DIRECTORY / f"{OUTPUT_STEM}.pq"
    json_path = figures_root / OUTPUT_DIRECTORY / f"{OUTPUT_STEM}.json"
    pdf_path = figures_root / OUTPUT_DIRECTORY / f"{OUTPUT_STEM}.pdf"
    _write_table(table, aggregate_path, compression)

    metadata = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "manifest": str(manifest_path),
        "aggregate": str(aggregate_path),
        "semantics": {
            "missing_reply": (
                "a fixed IPID_SEQUENCE position whose token cannot be parsed as an integer"
            ),
            "population": "persisted fixed-interval Mass sequences only",
            "censoring": (
                "measurement attempts rejected before persistence because they did not meet "
                "the configured minimum reply rate are absent"
            ),
            "causal_scope": (
                "missing replies are observed outcomes and are not attributed specifically "
                "to forward-path or return-path packet loss"
            ),
        },
        "measurements": measurement_metadata,
    }
    _write_json(json_path, metadata)
    render_missing_reply_figure(table, metadata, pdf_path)
    return {"aggregate": aggregate_path, "json": json_path, "pdf": pdf_path}


@app.command()
def main(
    manifest_path: Path = MANIFEST_ARGUMENT,
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
    threads: int = typer.Option(0, help="DuckDB threads (0 = all cores)"),
) -> None:
    outputs = analyze_missing_replies(
        manifest_path,
        compression=None if compression == "none" else compression,
        threads=threads,
    )
    for name, path in outputs.items():
        typer.echo(f"{name}: {path}")


if __name__ == "__main__":
    app()
