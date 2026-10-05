"""Diagnose Base-UNCLASSIFIED sequences later classified deterministically in Mass.

The fixed-interval Mass measurement targets the RT-based Base ``UNCLASSIFIED``
population.  This module joins the same address across RT-based Base,
fixed-interval Base, and fixed-interval Mass, then evaluates every sequence
against the deterministic rule selected by the Mass classifier.  The result
distinguishes a near-match with one or two disturbed transitions from a genuine
change in observed IP-ID behaviour.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import typer

from ipid_analysis.config import PROCESSED_DATA_DIR, PROJ_ROOT, RAW_DATA_DIR
from ipid_analysis.inspect_sequences import parse_sequence
from ipid_analysis.manifest import IpidMeasurement, load_manifest, resolve
from ipid_analysis.strategies import (
    INPUT_NAME,
    MAX_INC,
    MODULUS,
    OUTPUT_NAME,
    MeasurementConfig,
    load_config,
)

app = typer.Typer(add_completion=False)

DETERMINISTIC_STRATEGIES = (
    "REFLECTION",
    "CONSTANT",
    "PER_DESTINATION",
    "PER_CONNECTION",
    "SINGLE",
    "PER_BUCKET",
)
KIND = "base-unclassified-to-mass-deterministic"
DEFAULT_DATA_DIR = PROCESSED_DATA_DIR / "deterministic-transitions"
DEFAULT_FIGURE_DIR = PROJ_ROOT / "reports" / "figures" / "deterministic-transitions"
MANIFEST_ARGUMENT = typer.Argument(..., help="measurement manifest JSON")
DATA_DIR_OPTION = typer.Option(DEFAULT_DATA_DIR, help="Parquet output directory")
FIGURE_DIR_OPTION = typer.Option(
    DEFAULT_FIGURE_DIR,
    help="CSV, JSON, and sequence-plot output directory",
)
PLOTS_OPTION = typer.Option(True, "--plots/--no-plots", help="render per-IP PNGs")


@dataclass(frozen=True)
class MeasurementTriplet:
    rt_base: IpidMeasurement
    fixed_base: IpidMeasurement | None
    mass: IpidMeasurement

    def __post_init__(self) -> None:
        if self.rt_base.interval != "rt-based" or self.rt_base.scale != "base":
            raise ValueError("transition analysis requires an RT-based Base measurement")
        if self.mass.interval != "fixed-interval" or self.mass.scale != "mass":
            raise ValueError("transition analysis requires a fixed-interval Mass measurement")
        if self.rt_base.protocol != self.mass.protocol:
            raise ValueError("Base and Mass protocols differ")
        if self.rt_base.connection_mode != self.mass.connection_mode:
            raise ValueError("Base and Mass connection modes differ")
        if self.rt_base.zmap_id != self.mass.zmap_id:
            raise ValueError("Base and Mass measurements belong to different campaigns")
        if self.fixed_base is not None and (
            self.fixed_base.interval != "fixed-interval"
            or self.fixed_base.scale != "base"
            or self.fixed_base.protocol != self.rt_base.protocol
            or self.fixed_base.connection_mode != self.rt_base.connection_mode
            or self.fixed_base.zmap_id != self.rt_base.zmap_id
        ):
            raise ValueError("fixed-interval Base measurement is incompatible")

    @property
    def protocol(self) -> str:
        return self.rt_base.protocol

    @property
    def connection_mode(self) -> str:
        return self.rt_base.connection_mode


@dataclass(frozen=True)
class RuleDiagnostics:
    present_count: int
    missing_positions: int
    evaluated_constraints: int
    missing_constraints: int
    rule_violations: int
    exact_match: bool


@dataclass(frozen=True)
class JoinedTransition:
    protocol: str
    connection_mode: str
    ip_addr: str
    rt_base_class: str
    fixed_base_class: str | None
    mass_class: str
    rt_base_sequence: str
    fixed_base_sequence: str | None
    mass_sequence: str


def iter_measurement_triplets(manifest: dict) -> list[MeasurementTriplet]:
    """Return every compatible RT-Base/FI-Base/FI-Mass set in stable order."""
    triplets: list[MeasurementTriplet] = []
    for protocol, section in manifest.items():
        if not isinstance(section, dict):
            continue
        for connection_mode in ("no-connection", "connection"):
            rt_base = resolve(
                manifest,
                f"{protocol}.ipid.{connection_mode}.rt-based.base",
            )
            mass = resolve(
                manifest,
                f"{protocol}.ipid.{connection_mode}.fixed-interval.mass",
            )
            if rt_base is None or mass is None:
                continue
            fixed_base = resolve(
                manifest,
                f"{protocol}.ipid.{connection_mode}.fixed-interval.base",
            )
            triplets.append(MeasurementTriplet(rt_base, fixed_base, mass))
    return triplets


def _raw_path(measurement: IpidMeasurement, raw_root: Path) -> Path:
    return raw_root / measurement.input_key / INPUT_NAME


def _snapshot_path(measurement: IpidMeasurement, raw_root: Path) -> Path:
    return raw_root / measurement.input_key / "ipid.snapshot.yaml"


def _strategy_path(measurement: IpidMeasurement, processed_root: Path) -> Path:
    return measurement.artifact_path(processed_root, Path(OUTPUT_NAME).stem)


def _required_paths(
    triplet: MeasurementTriplet,
    raw_root: Path,
    processed_root: Path,
) -> list[Path]:
    measurements = [triplet.rt_base, triplet.mass]
    if triplet.fixed_base is not None:
        measurements.append(triplet.fixed_base)
    return [
        path
        for measurement in measurements
        for path in (
            _raw_path(measurement, raw_root),
            _snapshot_path(measurement, raw_root),
            _strategy_path(measurement, processed_root),
        )
    ]


def load_transitions(
    triplet: MeasurementTriplet,
    *,
    raw_root: Path = RAW_DATA_DIR,
    processed_root: Path = PROCESSED_DATA_DIR,
) -> list[JoinedTransition]:
    """Join raw sequences for RT-Base UNCLASSIFIED -> Mass deterministic rows."""
    for path in _required_paths(triplet, raw_root, processed_root):
        if not path.is_file():
            raise FileNotFoundError(path)

    fixed_select = (
        "CAST(fi_s.IPID_SELECTION_STRATEGY AS VARCHAR) AS FI_BASE_CLASS, "
        "CAST(fi_r.IPID_SEQUENCE AS VARCHAR) AS FI_BASE_SEQUENCE"
        if triplet.fixed_base is not None
        else "NULL::VARCHAR AS FI_BASE_CLASS, NULL::VARCHAR AS FI_BASE_SEQUENCE"
    )
    fixed_joins = ""
    parameters: dict[str, str | list[str]] = {
        "rt_raw": str(_raw_path(triplet.rt_base, raw_root)),
        "rt_strategies": str(_strategy_path(triplet.rt_base, processed_root)),
        "mass_raw": str(_raw_path(triplet.mass, raw_root)),
        "mass_strategies": str(_strategy_path(triplet.mass, processed_root)),
        "deterministic": list(DETERMINISTIC_STRATEGIES),
    }
    if triplet.fixed_base is not None:
        fixed_joins = """
            LEFT JOIN read_parquet($fi_raw) AS fi_r USING (IP_ADDR)
            LEFT JOIN read_parquet($fi_strategies) AS fi_s USING (IP_ADDR)
        """
        parameters.update(
            {
                "fi_raw": str(_raw_path(triplet.fixed_base, raw_root)),
                "fi_strategies": str(_strategy_path(triplet.fixed_base, processed_root)),
            }
        )

    connection = duckdb.connect()
    try:
        rows = connection.execute(
            f"""
            SELECT
                CAST(m_s.IP_ADDR AS VARCHAR) AS IP_ADDR,
                CAST(rt_s.IPID_SELECTION_STRATEGY AS VARCHAR) AS RT_BASE_CLASS,
                CAST(m_s.IPID_SELECTION_STRATEGY AS VARCHAR) AS MASS_CLASS,
                CAST(rt_r.IPID_SEQUENCE AS VARCHAR) AS RT_BASE_SEQUENCE,
                CAST(m_r.IPID_SEQUENCE AS VARCHAR) AS MASS_SEQUENCE,
                {fixed_select}
            FROM read_parquet($mass_strategies) AS m_s
            INNER JOIN read_parquet($mass_raw) AS m_r USING (IP_ADDR)
            INNER JOIN read_parquet($rt_strategies) AS rt_s USING (IP_ADDR)
            INNER JOIN read_parquet($rt_raw) AS rt_r USING (IP_ADDR)
            {fixed_joins}
            WHERE CAST(rt_s.IPID_SELECTION_STRATEGY AS VARCHAR) = 'UNCLASSIFIED'
              AND CAST(m_s.IPID_SELECTION_STRATEGY AS VARCHAR) IN (
                  SELECT unnest($deterministic)
              )
            ORDER BY MASS_CLASS, IP_ADDR
            """,
            parameters,
        ).fetchall()
    finally:
        connection.close()

    return [
        JoinedTransition(
            protocol=triplet.protocol,
            connection_mode=triplet.connection_mode,
            ip_addr=str(row[0]),
            rt_base_class=str(row[1]),
            fixed_base_class=None if row[5] is None else str(row[5]),
            mass_class=str(row[2]),
            rt_base_sequence=str(row[3]),
            fixed_base_sequence=None if row[6] is None else str(row[6]),
            mass_sequence=str(row[4]),
        )
        for row in rows
    ]


def _trimmed_sequence_and_pattern(
    sequence: np.ndarray,
    cfg: MeasurementConfig,
    *,
    skip_first: bool,
) -> tuple[np.ndarray, np.ndarray]:
    pattern = cfg.request_ip_ids[np.arange(cfg.sequence_length) % cfg.request_ip_ids.size]
    if skip_first:
        return sequence[cfg.connection_count :], pattern[cfg.connection_count :]
    return sequence, pattern


def _increment_constraints(
    sequence: np.ndarray,
    groups: Iterable[np.ndarray],
    *,
    lower: int,
    upper: int,
) -> tuple[int, int, int]:
    evaluated = 0
    missing = 0
    violations = 0
    for indices in groups:
        left = sequence[indices[:-1]]
        right = sequence[indices[1:]]
        valid = (left >= 0) & (right >= 0)
        increments = (right[valid] - left[valid]) % MODULUS
        evaluated += int(valid.sum())
        missing += int((~valid).sum())
        violations += int(((increments < lower) | (increments > upper)).sum())
    return evaluated, missing, violations


def rule_diagnostics(
    sequence: np.ndarray,
    cfg: MeasurementConfig,
    strategy: str,
    *,
    skip_first: bool,
) -> RuleDiagnostics:
    """Measure distance from one exact deterministic classifier rule."""
    if strategy not in DETERMINISTIC_STRATEGIES:
        raise ValueError(f"{strategy!r} is not an exact deterministic strategy")
    if sequence.shape != (cfg.sequence_length,):
        raise ValueError(f"sequence shape is {sequence.shape}, expected ({cfg.sequence_length},)")

    present_count = int((sequence >= 0).sum())
    missing_positions = cfg.sequence_length - present_count
    trimmed, pattern = _trimmed_sequence_and_pattern(
        sequence,
        cfg,
        skip_first=skip_first,
    )
    trimmed_positions = np.arange(cfg.sequence_length)
    if skip_first:
        trimmed_positions = trimmed_positions[cfg.connection_count :]

    if strategy == "REFLECTION":
        valid = trimmed >= 0
        offsets = (trimmed[valid] - pattern[valid]) % MODULUS
        if offsets.size:
            _, frequency = np.unique(offsets, return_counts=True)
            violations = int(offsets.size - frequency.max())
        else:
            violations = 0
        evaluated = int(valid.sum())
        missing_constraints = int((~valid).sum())
    elif strategy == "CONSTANT":
        observed = trimmed[trimmed >= 0]
        if observed.size:
            _, frequency = np.unique(observed, return_counts=True)
            violations = int(observed.size - frequency.max())
        else:
            violations = 0
        evaluated = int(observed.size)
        missing_constraints = int(trimmed.size - observed.size)
    else:
        if strategy == "PER_DESTINATION":
            groups = (trimmed_positions[0::2], trimmed_positions[1::2])
            lower, upper = 1, 1
        elif strategy == "PER_CONNECTION":
            groups = tuple(
                trimmed_positions[index :: cfg.connection_count]
                for index in range(cfg.connection_count)
            )
            lower, upper = 1, 1
        elif strategy == "SINGLE":
            groups = (trimmed_positions,)
            lower, upper = 1, MAX_INC
        else:  # PER_BUCKET
            groups = tuple(
                trimmed_positions[index :: cfg.connection_count]
                for index in range(cfg.connection_count)
            )
            lower, upper = 1, MAX_INC
        evaluated, missing_constraints, violations = _increment_constraints(
            sequence,
            groups,
            lower=lower,
            upper=upper,
        )

    complete = missing_positions == 0
    return RuleDiagnostics(
        present_count=present_count,
        missing_positions=missing_positions,
        evaluated_constraints=evaluated,
        missing_constraints=missing_constraints,
        rule_violations=violations,
        exact_match=complete and violations == 0,
    )


def _measurement_values(
    transition: JoinedTransition,
    triplet: MeasurementTriplet,
    raw_root: Path,
) -> tuple[list[tuple[str, str | None, str | None, MeasurementConfig, bool]], dict[str, str]]:
    rt_cfg = load_config(_snapshot_path(triplet.rt_base, raw_root))
    mass_cfg = load_config(_snapshot_path(triplet.mass, raw_root))
    fixed_cfg = None
    if triplet.fixed_base is not None:
        fixed_cfg = load_config(_snapshot_path(triplet.fixed_base, raw_root))
    values = [
        (
            "RT_BASE",
            transition.rt_base_class,
            transition.rt_base_sequence,
            rt_cfg,
            triplet.protocol == "tcp",
        ),
        (
            "FIXED_BASE",
            transition.fixed_base_class,
            transition.fixed_base_sequence,
            fixed_cfg or rt_cfg,
            triplet.protocol == "tcp",
        ),
        (
            "MASS",
            transition.mass_class,
            transition.mass_sequence,
            mass_cfg,
            False,
        ),
    ]
    measurement_ids = {
        "RT_BASE_MEASUREMENT_ID": triplet.rt_base.measurement_id,
        "FIXED_BASE_MEASUREMENT_ID": (
            "" if triplet.fixed_base is None else triplet.fixed_base.measurement_id
        ),
        "MASS_MEASUREMENT_ID": triplet.mass.measurement_id,
    }
    return values, measurement_ids


def transition_row(
    transition: JoinedTransition,
    triplet: MeasurementTriplet,
    *,
    raw_root: Path = RAW_DATA_DIR,
) -> tuple[dict[str, object], dict[str, tuple[np.ndarray | None, RuleDiagnostics | None]]]:
    """Create one flat artifact row and plotting payload."""
    measurements, measurement_ids = _measurement_values(transition, triplet, raw_root)
    row: dict[str, object] = {
        "PROTOCOL": transition.protocol,
        "CONNECTION_MODE": transition.connection_mode,
        "IP_ADDR": transition.ip_addr,
        "MASS_CLASS": transition.mass_class,
        **measurement_ids,
    }
    plotting: dict[str, tuple[np.ndarray | None, RuleDiagnostics | None]] = {}
    for label, observed_class, raw_sequence, cfg, skip_first in measurements:
        row[f"{label}_CLASS"] = observed_class or ""
        row[f"{label}_SEQUENCE"] = raw_sequence or ""
        if raw_sequence is None:
            for suffix in (
                "PRESENT_COUNT",
                "MISSING_POSITIONS",
                "EVALUATED_CONSTRAINTS",
                "MISSING_CONSTRAINTS",
                "RULE_VIOLATIONS",
            ):
                row[f"{label}_{suffix}"] = None
            row[f"{label}_EXACT_MATCH_TO_MASS_CLASS"] = None
            plotting[label] = (None, None)
            continue
        sequence = parse_sequence(raw_sequence, cfg.sequence_length)
        diagnostics = rule_diagnostics(
            sequence,
            cfg,
            transition.mass_class,
            skip_first=skip_first,
        )
        for key, value in asdict(diagnostics).items():
            suffix = key.upper()
            if suffix == "EXACT_MATCH":
                suffix = "EXACT_MATCH_TO_MASS_CLASS"
            row[f"{label}_{suffix}"] = value
        plotting[label] = (sequence, diagnostics)
    return row, plotting


def _plot_sequence_axis(
    axis: plt.Axes,
    sequence: np.ndarray | None,
    cfg: MeasurementConfig,
    *,
    label: str,
    observed_class: str | None,
    mass_class: str,
    diagnostics: RuleDiagnostics | None,
) -> None:
    if sequence is None or diagnostics is None:
        axis.text(0.5, 0.5, "No matching measurement row", ha="center", va="center")
        axis.set_axis_off()
        axis.set_title(f"{label}: not observed")
        return
    positions = np.arange(cfg.sequence_length)
    visible = sequence.astype(float)
    visible[sequence < 0] = np.nan
    axis.plot(positions, visible, color="0.72", linewidth=0.8, zorder=1)
    colors = plt.get_cmap("tab10").colors
    for connection_index in range(cfg.connection_count):
        selected = (positions % cfg.connection_count == connection_index) & np.isfinite(visible)
        axis.scatter(
            positions[selected],
            visible[selected],
            s=18,
            color=colors[connection_index % len(colors)],
            zorder=2,
        )
    missing = ~np.isfinite(visible)
    if missing.any():
        axis.scatter(
            positions[missing],
            np.full(missing.sum(), 0.02),
            transform=axis.get_xaxis_transform(),
            marker="x",
            color="black",
            s=20,
            clip_on=False,
        )
    axis.set(
        xlim=(-1, cfg.sequence_length),
        ylim=(-1000, MODULUS + 1000),
        ylabel="IP-ID",
    )
    axis.grid(alpha=0.25)
    axis.set_title(
        f"{label}: {observed_class or 'not classified'} | "
        f"replies={diagnostics.present_count}/{cfg.sequence_length} | "
        f"violations vs {mass_class}={diagnostics.rule_violations} | "
        f"missing constraints={diagnostics.missing_constraints}",
        fontsize=10,
    )


def render_transition_plot(
    transition: JoinedTransition,
    triplet: MeasurementTriplet,
    plotting: dict[str, tuple[np.ndarray | None, RuleDiagnostics | None]],
    output_path: Path,
    *,
    raw_root: Path = RAW_DATA_DIR,
) -> None:
    """Render RT-Base, FI-Base, and FI-Mass for one address."""
    rt_cfg = load_config(_snapshot_path(triplet.rt_base, raw_root))
    fixed_cfg = (
        rt_cfg
        if triplet.fixed_base is None
        else load_config(_snapshot_path(triplet.fixed_base, raw_root))
    )
    mass_cfg = load_config(_snapshot_path(triplet.mass, raw_root))
    definitions = (
        ("RT-based Base", "RT_BASE", transition.rt_base_class, rt_cfg),
        (
            "Fixed-Interval Base",
            "FIXED_BASE",
            transition.fixed_base_class,
            fixed_cfg,
        ),
        ("Fixed-Interval Mass", "MASS", transition.mass_class, mass_cfg),
    )
    figure, axes = plt.subplots(3, 1, figsize=(12, 8.5), constrained_layout=True)
    figure.suptitle(
        f"{transition.protocol.upper()} {transition.ip_addr}: "
        f"RT-Base UNCLASSIFIED -> Mass {transition.mass_class.replace('_', '-')}\n"
        "Rule violations are evaluated against the final Mass class",
        fontsize=13,
    )
    for axis, (title, key, observed_class, cfg) in zip(axes, definitions):
        sequence, diagnostics = plotting[key]
        _plot_sequence_axis(
            axis,
            sequence,
            cfg,
            label=title,
            observed_class=observed_class,
            mass_class=transition.mass_class,
            diagnostics=diagnostics,
        )
    axes[-1].set_xlabel("Measurement index")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, dpi=160)
    plt.close(figure)


def _arrow_table(rows: list[dict[str, object]]) -> pa.Table:
    if not rows:
        return pa.table(
            {
                "PROTOCOL": pa.array([], type=pa.string()),
                "CONNECTION_MODE": pa.array([], type=pa.string()),
                "IP_ADDR": pa.array([], type=pa.string()),
                "MASS_CLASS": pa.array([], type=pa.string()),
            }
        )
    return pa.Table.from_pylist(rows)


def analyze_deterministic_transitions(
    manifest_path: Path,
    *,
    raw_root: Path = RAW_DATA_DIR,
    processed_root: Path = PROCESSED_DATA_DIR,
    data_dir: Path = DEFAULT_DATA_DIR,
    figure_dir: Path = DEFAULT_FIGURE_DIR,
    render_plots: bool = True,
) -> dict[str, Path]:
    """Write per-transition data, metadata, and paired diagnostic plots."""
    manifest = load_manifest(manifest_path)
    triplets = iter_measurement_triplets(manifest)
    if not triplets:
        raise ValueError(f"{manifest_path}: no RT-Base/FI-Mass measurement pair")

    rows: list[dict[str, object]] = []
    plot_count = 0
    measurement_metadata: list[dict[str, object]] = []
    for triplet in triplets:
        transitions = load_transitions(
            triplet,
            raw_root=raw_root,
            processed_root=processed_root,
        )
        class_counts = Counter(transition.mass_class for transition in transitions)
        measurement_metadata.append(
            {
                "protocol": triplet.protocol,
                "connection_mode": triplet.connection_mode,
                "rt_base_measurement_id": triplet.rt_base.measurement_id,
                "fixed_base_measurement_id": (
                    None if triplet.fixed_base is None else triplet.fixed_base.measurement_id
                ),
                "mass_measurement_id": triplet.mass.measurement_id,
                "transition_count": len(transitions),
                "mass_class_counts": dict(sorted(class_counts.items())),
            }
        )
        for transition in transitions:
            row, plotting = transition_row(transition, triplet, raw_root=raw_root)
            rows.append(row)
            if render_plots:
                safe_ip = transition.ip_addr.replace(":", "_")
                output_path = (
                    figure_dir
                    / "sequences"
                    / transition.protocol
                    / transition.mass_class.lower().replace("_", "-")
                    / f"{safe_ip}.png"
                )
                render_transition_plot(
                    transition,
                    triplet,
                    plotting,
                    output_path,
                    raw_root=raw_root,
                )
                plot_count += 1

    data_dir.mkdir(parents=True, exist_ok=True)
    figure_dir.mkdir(parents=True, exist_ok=True)
    table = _arrow_table(rows)
    parquet_path = data_dir / f"{KIND}.pq"
    csv_path = figure_dir / f"{KIND}.csv"
    json_path = figure_dir / f"{KIND}.json"
    pq.write_table(table, parquet_path)
    pacsv.write_csv(table, csv_path)

    total_by_class = Counter(str(row["MASS_CLASS"]) for row in rows)
    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "manifest": str(manifest_path),
        "methodology": {
            "population": (
                "addresses classified UNCLASSIFIED in RT-based Base and as a "
                "deterministic strategy in fixed-interval Mass; exact-match "
                "eligibility is reported separately"
            ),
            "comparison": (
                "each available RT-based Base, fixed-interval Base, and "
                "fixed-interval Mass raw sequence is joined by IP_ADDR"
            ),
            "rule_distance": (
                "observed deterministic-rule violations are counted against the "
                "strategy assigned by the Mass classifier; missing constraints are "
                "reported separately; Reflection and Constant use the closest modal "
                "offset or value"
            ),
        },
        "transition_count": len(rows),
        "plot_count": plot_count,
        "mass_class_counts": dict(sorted(total_by_class.items())),
        "measurements": measurement_metadata,
        "artifacts": {
            "parquet": str(parquet_path),
            "csv": str(csv_path),
            "sequence_plot_directory": str(figure_dir / "sequences"),
        },
    }
    json_path.write_text(json.dumps(metadata, indent=2) + "\n")
    return {
        "aggregate": parquet_path,
        "csv": csv_path,
        "json": json_path,
        "plots": figure_dir / "sequences",
    }


@app.command()
def main(
    manifest: Path = MANIFEST_ARGUMENT,
    data_dir: Path = DATA_DIR_OPTION,
    figure_dir: Path = FIGURE_DIR_OPTION,
    plots: bool = PLOTS_OPTION,
) -> None:
    """Analyze RT-Base UNCLASSIFIED -> deterministic Mass transitions."""
    artifacts = analyze_deterministic_transitions(
        manifest,
        data_dir=data_dir,
        figure_dir=figure_dir,
        render_plots=plots,
    )
    for name, path in artifacts.items():
        typer.echo(f"{name}: {path}")


if __name__ == "__main__":
    app()
