"""Prepare and evaluate repeated measurements of Mass RANDOM decisions.

The preparation step freezes the complete Mass ``UNCLASSIFIED`` population and
an equally sized, stratified ``RANDOM`` control population.  It writes a
ZMap-compatible target parquet without changing any persisted classifications.

The evaluation step reads one or more repeated raw IP-ID measurements directly,
applies the production classifier in memory, and reports class/component
stability plus distance from every exact deterministic rule.  Existing
``strategies.pq`` files are never read for repetitions and never overwritten.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
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

from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.deterministic_transition_analysis import (
    DETERMINISTIC_STRATEGIES,
    rule_diagnostics,
)
from ipid_analysis.inspect_sequences import parse_sequence
from ipid_analysis.manifest import IpidMeasurement, load_manifest, resolve
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_RANDOM_MIN_SCORE,
    CandidateNullTables,
    candidate_random_score_components,
    production_candidate_null_tables,
)
from ipid_analysis.strategies import (
    INPUT_NAME,
    MULTI_MAX_CLUSTERS,
    OUTPUT_NAME,
    IPIDStrategy,
    MeasurementConfig,
    _cluster_counts_mass,
    _sorted_present_values,
    classify_batch_mass,
    load_config,
    random_structure_features,
)

app = typer.Typer(add_completion=False)

DEFAULT_TARGET = "icmp.ipid.no-connection.fixed-interval.mass"
DEFAULT_MAXIMUM_TARGETS = 10_000
TARGET_FILE_NAME = "random-reproducibility-targets.pq"
COHORT_FILE_NAME = "random-reproducibility-cohort.pq"
PREPARE_METADATA_FILE_NAME = "random-reproducibility-prepare.json"
COMPONENTS = ("RAW", "INCREMENT", "GAP")
MANIFEST_ARGUMENT = typer.Argument(..., help="baseline measurement manifest JSON")
REPEAT_IDS_OPTION = typer.Option(
    ...,
    "--repeat-id",
    help="raw repeated IP-ID measurement id; pass once per repetition",
)
COHORT_OPTION = typer.Option(None, help="prepared cohort parquet")


def _measurement_paths(
    manifest_path: Path,
    target: str,
    *,
    raw_root: Path,
    processed_root: Path,
) -> tuple[IpidMeasurement, Path, Path, Path]:
    measurement = resolve(load_manifest(manifest_path), target)
    if measurement is None:
        raise ValueError(f"{target!r} is not present in {manifest_path}")
    if measurement.interval != "fixed-interval" or measurement.scale != "mass":
        raise ValueError("RANDOM reproducibility requires a fixed-interval Mass target")
    raw_path = raw_root / measurement.input_key / INPUT_NAME
    snapshot_path = raw_root / measurement.input_key / "ipid.snapshot.yaml"
    strategy_path = measurement.artifact_path(processed_root, Path(OUTPUT_NAME).stem)
    for path in (raw_path, snapshot_path, strategy_path):
        if not path.is_file():
            raise FileNotFoundError(path)
    return measurement, raw_path, snapshot_path, strategy_path


def _load_classified_sequences(raw_path: Path, strategy_path: Path) -> list[tuple[str, str, str]]:
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT
                CAST(raw.IP_ADDR AS VARCHAR),
                CAST(classified.IPID_SELECTION_STRATEGY AS VARCHAR),
                CAST(raw.IPID_SEQUENCE AS VARCHAR)
            FROM read_parquet($raw) AS raw
            INNER JOIN read_parquet($strategies) AS classified USING (IP_ADDR)
            WHERE CAST(classified.IPID_SELECTION_STRATEGY AS VARCHAR)
                  IN ('UNCLASSIFIED', 'RANDOM')
            ORDER BY IP_ADDR
            """,
            {"raw": str(raw_path), "strategies": str(strategy_path)},
        ).fetchall()
    finally:
        connection.close()
    return [(str(ip), str(strategy), str(sequence)) for ip, strategy, sequence in rows]


def _stable_key(ip_addr: str, seed: int) -> bytes:
    return hashlib.sha256(f"{seed}:{ip_addr}".encode()).digest()


def _diagnostics(
    sequence: np.ndarray,
    cfg: MeasurementConfig,
    null_tables: CandidateNullTables,
) -> dict[str, object]:
    values = sequence[None, :]
    present = values >= 0
    lengths = present.sum(axis=1).astype(np.int64)
    ordered = _sorted_present_values(values, present)
    features = random_structure_features(values, present, ordered=ordered)
    components = candidate_random_score_components(
        values,
        present,
        null_tables,
        config=cfg,
    )
    cluster_count = int(
        _cluster_counts_mass(values, present, lengths, ordered=ordered)[0]
    )
    raw = float(components.raw_uniformity[0])
    increment = float(components.increment_uniformity[0])
    gap = float(components.gap_uniformity[0])
    component_values = {"RAW": raw, "INCREMENT": increment, "GAP": gap}

    row: dict[str, object] = {
        "PRESENT_COUNT": int(features.sample_count[0]),
        "MISSING_COUNT": int(cfg.sequence_length - features.sample_count[0]),
        "UNIQUE_COUNT": int(features.unique_count[0]),
        "DUPLICATE_COUNT": int(features.sample_count[0] - features.unique_count[0]),
        "CLUSTER_COUNT": cluster_count,
        "MULTI_COMPATIBLE": 1 < cluster_count <= MULTI_MAX_CLUSTERS,
        "MAXIMUM_GAP": int(features.maximum_gap[0]),
        "P_RAW": raw,
        "P_INCREMENT": increment,
        "P_GAP": gap,
        "RANDOM_SCORE": float(components.score[0]),
        "LIMITING_COMPONENT": min(component_values, key=component_values.get),
    }
    for name, value in component_values.items():
        row[f"{name}_REJECTS_RANDOM"] = value < CANDIDATE_RANDOM_MIN_SCORE
        row[f"{name}_LOG10_MARGIN"] = math.log10(
            max(value, np.finfo(float).tiny) / CANDIDATE_RANDOM_MIN_SCORE
        )

    closest: tuple[float, int, str] | None = None
    for strategy in DETERMINISTIC_STRATEGIES:
        diagnostics = rule_diagnostics(sequence, cfg, strategy, skip_first=False)
        prefix = f"RULE_{strategy}"
        rate = (
            diagnostics.rule_violations / diagnostics.evaluated_constraints
            if diagnostics.evaluated_constraints
            else 1.0
        )
        row[f"{prefix}_VIOLATIONS"] = diagnostics.rule_violations
        row[f"{prefix}_EVALUATED"] = diagnostics.evaluated_constraints
        row[f"{prefix}_MISSING_CONSTRAINTS"] = diagnostics.missing_constraints
        row[f"{prefix}_VIOLATION_RATE"] = rate
        candidate = (rate, diagnostics.rule_violations, strategy)
        if closest is None or candidate < closest:
            closest = candidate
    assert closest is not None
    row["CLOSEST_DETERMINISTIC_CLASS"] = closest[2]
    row["CLOSEST_DETERMINISTIC_VIOLATIONS"] = closest[1]
    row["CLOSEST_DETERMINISTIC_VIOLATION_RATE"] = closest[0]
    return row


def _classify_sequences(
    sequences: list[np.ndarray],
    cfg: MeasurementConfig,
    null_tables: CandidateNullTables,
) -> list[str]:
    if not sequences:
        return []
    ipid_lists = pa.array(
        [
            [int(value) for value in sequence]
            for sequence in sequences
        ],
        type=pa.list_(pa.int64()),
    )
    codes = classify_batch_mass(ipid_lists, cfg, random_null_tables=null_tables)
    return [IPIDStrategy(int(code)).name for code in codes]


def _write_table(rows: list[dict[str, object]], path: Path) -> pa.Table:
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        keys = sorted({key for row in rows for key in row})
        normalized = [{key: row.get(key) for key in keys} for row in rows]
        table = pa.Table.from_pylist(normalized)
    else:
        table = pa.table({"IP_ADDR": pa.array([], pa.string())})
    pq.write_table(table, path)
    return table


def _artifact_stem(measurement: IpidMeasurement) -> str:
    protocol = measurement.protocol.replace("-", "_")
    mode = measurement.connection_mode.replace("-", "_")
    return f"{protocol}-{mode}-mass-random-reproducibility"


def _default_output_directories(
    measurement: IpidMeasurement,
    *,
    processed_root: Path,
    figures_root: Path = FIGURES_DIR,
) -> tuple[Path, Path]:
    if not measurement.zmap_id:
        raise ValueError(f"{measurement.target}: zmap id is required for output paths")
    relative = (
        Path(measurement.zmap_id)
        / measurement.artifact_directory
        / "random-reproducibility"
    )
    return processed_root / relative, figures_root / relative


def _proportional_stratified_sample(
    rows: list[dict[str, object]],
    maximum: int,
    seed: int,
) -> list[dict[str, object]]:
    """Deterministically sample up to ``maximum`` rows by limiting component."""
    if maximum < 0:
        raise ValueError("maximum must be non-negative")
    if len(rows) <= maximum:
        selected = list(rows)
    elif maximum == 0:
        selected = []
    else:
        groups = {
            component: [row for row in rows if row["LIMITING_COMPONENT"] == component]
            for component in COMPONENTS
        }
        groups = {component: values for component, values in groups.items() if values}
        exact = {
            component: maximum * len(values) / len(rows)
            for component, values in groups.items()
        }
        quotas = {
            component: min(len(groups[component]), math.floor(value))
            for component, value in exact.items()
        }
        remaining = maximum - sum(quotas.values())
        order = sorted(
            groups,
            key=lambda component: (
                -(exact[component] - math.floor(exact[component])),
                component,
            ),
        )
        while remaining:
            progressed = False
            for component in order:
                if quotas[component] >= len(groups[component]):
                    continue
                quotas[component] += 1
                remaining -= 1
                progressed = True
                if not remaining:
                    break
            if not progressed:
                break
        selected = []
        for component, values in groups.items():
            selected.extend(
                sorted(
                    values,
                    key=lambda row: _stable_key(str(row["IP_ADDR"]), seed),
                )[: quotas[component]]
            )

    population = Counter(str(row["LIMITING_COMPONENT"]) for row in rows)
    sample = Counter(str(row["LIMITING_COMPONENT"]) for row in selected)
    for row in selected:
        component = str(row["LIMITING_COMPONENT"])
        probability = sample[component] / population[component]
        row["SELECTION_PROBABILITY"] = probability
        row["SELECTION_WEIGHT"] = 1.0 / probability
    return selected


def prepare_random_reproducibility_paths(
    raw_path: Path,
    snapshot_path: Path,
    strategy_path: Path,
    *,
    measurement_id: str,
    target: str,
    cohort_path: Path,
    target_path: Path,
    json_path: Path,
    csv_path: Path | None = None,
    control_count: int | None = None,
    maximum_targets: int = DEFAULT_MAXIMUM_TARGETS,
    seed: int = 42,
    null_tables: CandidateNullTables | None = None,
) -> dict[str, Path]:
    """Freeze a bounded, reproducible Mass cohort from explicit input paths."""
    for path in (raw_path, snapshot_path, strategy_path):
        if not path.is_file():
            raise FileNotFoundError(path)
    if maximum_targets < 0:
        raise ValueError("maximum_targets must be non-negative")

    cfg = load_config(snapshot_path)
    tables = production_candidate_null_tables() if null_tables is None else null_tables
    classified = _load_classified_sequences(raw_path, strategy_path)
    all_rows: list[dict[str, object]] = []
    for ip_addr, strategy, raw_sequence in classified:
        sequence = parse_sequence(raw_sequence, cfg.sequence_length)
        all_rows.append(
            {
                "IP_ADDR": ip_addr,
                "COHORT": strategy,
                "BASELINE_CLASS": strategy,
                "BASELINE_MEASUREMENT_ID": measurement_id,
                "BASELINE_SEQUENCE": raw_sequence,
                **_diagnostics(sequence, cfg, tables),
            }
        )

    unclassified_population = [row for row in all_rows if row["COHORT"] == "UNCLASSIFIED"]
    random_rows = [row for row in all_rows if row["COHORT"] == "RANDOM"]
    maximum_unclassified = maximum_targets // 2
    unclassified = _proportional_stratified_sample(
        unclassified_population,
        maximum_unclassified,
        seed,
    )
    wanted = len(unclassified) if control_count is None else control_count
    if wanted < 0:
        raise ValueError("control_count must be non-negative")
    wanted = min(wanted, len(random_rows), maximum_targets - len(unclassified))
    near_count = (wanted + 1) // 2
    near = sorted(random_rows, key=lambda row: (row["RANDOM_SCORE"], row["IP_ADDR"]))[
        :near_count
    ]
    near_ips = {str(row["IP_ADDR"]) for row in near}
    far_candidates = [row for row in random_rows if str(row["IP_ADDR"]) not in near_ips]
    far = sorted(far_candidates, key=lambda row: _stable_key(str(row["IP_ADDR"]), seed))[
        : wanted - near_count
    ]
    for row in near:
        row["CONTROL_STRATUM"] = "NEAR_THRESHOLD"
        row["SELECTION_PROBABILITY"] = None
        row["SELECTION_WEIGHT"] = None
    for row in far:
        row["CONTROL_STRATUM"] = "RANDOM_SAMPLE"
        row["SELECTION_PROBABILITY"] = None
        row["SELECTION_WEIGHT"] = None
    for row in unclassified:
        row["CONTROL_STRATUM"] = ""
    selected = sorted(unclassified + near + far, key=lambda row: str(row["IP_ADDR"]))

    table = _write_table(selected, cohort_path)
    if csv_path is not None:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        pacsv.write_csv(table, csv_path)

    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_table = pa.table(
        {
            "IP_ADDR": pa.array([str(row["IP_ADDR"]) for row in selected], pa.string()),
            "REPLY_TYPE": pa.array([""] * len(selected), pa.string()),
        }
    )
    pq.write_table(target_table, target_path)

    component_population = Counter(
        str(row["LIMITING_COMPONENT"]) for row in unclassified_population
    )
    component_sample = Counter(str(row["LIMITING_COMPONENT"]) for row in unclassified)
    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target": target,
        "measurement_id": measurement_id,
        "threshold": CANDIDATE_RANDOM_MIN_SCORE,
        "selection": {
            "maximum_targets": maximum_targets,
            "unclassified_population": len(unclassified_population),
            "unclassified_selected": len(unclassified),
            "random_population": len(random_rows),
            "random_controls": len(near) + len(far),
            "near_threshold_controls": len(near),
            "random_sample_controls": len(far),
            "unclassified_by_component_population": dict(sorted(component_population.items())),
            "unclassified_by_component_selected": dict(sorted(component_sample.items())),
            "seed": seed,
        },
        "methodology": {
            "population": "bounded Mass UNCLASSIFIED sample plus equally sized stratified Mass RANDOM controls",
            "unclassified": "deterministic proportional sample by limiting component",
            "near_threshold": "lowest production RANDOM scores among accepted RANDOM rows",
            "random_sample": "deterministic hash sample from remaining RANDOM rows",
            "safety": "preparation reads classifications and never modifies them",
        },
        "artifacts": {
            "cohort": str(cohort_path),
            "targets": str(target_path),
            "csv": str(csv_path) if csv_path is not None else None,
        },
    }
    _write_json_atomic(json_path, metadata)
    result = {"cohort": cohort_path, "targets": target_path, "json": json_path}
    if csv_path is not None:
        result["csv"] = csv_path
    return result


def _write_json_atomic(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def prepare_random_reproducibility(
    manifest_path: Path,
    *,
    target: str = DEFAULT_TARGET,
    control_count: int | None = None,
    maximum_targets: int = DEFAULT_MAXIMUM_TARGETS,
    seed: int = 42,
    raw_root: Path = RAW_DATA_DIR,
    processed_root: Path = PROCESSED_DATA_DIR,
    data_dir: Path | None = None,
    figure_dir: Path | None = None,
    null_tables: CandidateNullTables | None = None,
) -> dict[str, Path]:
    """Freeze UNCLASSIFIED plus stratified RANDOM controls and target parquet."""
    measurement, raw_path, snapshot_path, strategy_path = _measurement_paths(
        manifest_path,
        target,
        raw_root=raw_root,
        processed_root=processed_root,
    )
    if data_dir is None or figure_dir is None:
        default_data, default_figures = _default_output_directories(
            measurement,
            processed_root=processed_root,
        )
        data_dir = data_dir or default_data
        figure_dir = figure_dir or default_figures
    stem = _artifact_stem(measurement)
    cohort_path = data_dir / f"{stem}-cohort.pq"
    target_path = data_dir / f"{stem}-targets.pq"
    csv_path = figure_dir / f"{stem}-cohort.csv"
    json_path = figure_dir / f"{stem}-prepare.json"
    outputs = prepare_random_reproducibility_paths(
        raw_path,
        snapshot_path,
        strategy_path,
        measurement_id=measurement.measurement_id,
        target=target,
        cohort_path=cohort_path,
        target_path=target_path,
        json_path=json_path,
        csv_path=csv_path,
        control_count=control_count,
        maximum_targets=maximum_targets,
        seed=seed,
        null_tables=null_tables,
    )
    metadata = json.loads(json_path.read_text())
    metadata["manifest"] = str(manifest_path)
    _write_json_atomic(json_path, metadata)
    return outputs


def _read_repeat_sequences(raw_path: Path, cohort_ips: list[str]) -> dict[str, str]:
    if not raw_path.is_file():
        raise FileNotFoundError(raw_path)
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT CAST(IP_ADDR AS VARCHAR), CAST(IPID_SEQUENCE AS VARCHAR)
            FROM read_parquet($raw)
            WHERE CAST(IP_ADDR AS VARCHAR) IN (SELECT unnest($ips))
            ORDER BY IP_ADDR
            """,
            {"raw": str(raw_path), "ips": cohort_ips},
        ).fetchall()
    finally:
        connection.close()
    return {str(ip): str(sequence) for ip, sequence in rows}


def _summary_rows(long_rows: list[dict[str, object]], cohort: pa.Table) -> list[dict[str, object]]:
    cohort_lookup = {str(row["IP_ADDR"]): row for row in cohort.to_pylist()}
    by_ip: dict[str, list[dict[str, object]]] = {}
    for row in long_rows:
        by_ip.setdefault(str(row["IP_ADDR"]), []).append(row)
    summaries: list[dict[str, object]] = []
    for ip_addr in sorted(cohort_lookup):
        base = cohort_lookup[ip_addr]
        rows = by_ip.get(ip_addr, [])
        observed = [row for row in rows if row["OBSERVED"]]
        class_counts = Counter(str(row["CLASS"]) for row in observed)
        total = len(rows)
        n = len(observed)
        mode_class, mode_count = ("NO_MEASUREMENT", 0)
        if class_counts:
            mode_class, mode_count = min(
                class_counts.items(), key=lambda item: (-item[1], item[0])
            )
        probabilities = np.asarray(list(class_counts.values()), dtype=float)
        if probabilities.size:
            probabilities /= probabilities.sum()
        entropy = float(-(probabilities * np.log2(probabilities)).sum()) if n else 0.0
        summary: dict[str, object] = {
            "IP_ADDR": ip_addr,
            "COHORT": base["COHORT"],
            "CONTROL_STRATUM": base.get("CONTROL_STRATUM", ""),
            "BASELINE_CLASS": base["BASELINE_CLASS"],
            "REPEAT_COUNT": total,
            "OBSERVED_COUNT": n,
            "OBSERVED_FRACTION": n / total if total else 0.0,
            "MODE_CLASS": mode_class,
            "MODE_CLASS_FRACTION": mode_count / n if n else 0.0,
            "CLASS_ENTROPY_BITS": entropy,
            "BASELINE_CLASS_AGREEMENT": (
                sum(row["CLASS"] == base["BASELINE_CLASS"] for row in observed) / n
                if n
                else 0.0
            ),
            "CLASS_COUNTS": json.dumps(dict(sorted(class_counts.items()))),
        }
        for component in COMPONENTS:
            summary[f"{component}_REJECTION_FRACTION"] = (
                sum(bool(row[f"{component}_REJECTS_RANDOM"]) for row in observed) / n
                if n
                else 0.0
            )
            summary[f"{component}_LIMITING_FRACTION"] = (
                sum(row["LIMITING_COMPONENT"] == component for row in observed) / n
                if n
                else 0.0
            )
            values = [float(row[f"P_{component}"]) for row in observed]
            summary[f"{component}_P_MEDIAN"] = float(np.median(values)) if values else None
        deterministic = Counter(
            str(row["CLOSEST_DETERMINISTIC_CLASS"]) for row in observed
        )
        if deterministic:
            closest, count = min(
                deterministic.items(), key=lambda item: (-item[1], item[0])
            )
            summary["MODAL_CLOSEST_DETERMINISTIC_CLASS"] = closest
            summary["MODAL_CLOSEST_DETERMINISTIC_FRACTION"] = count / n
        else:
            summary["MODAL_CLOSEST_DETERMINISTIC_CLASS"] = ""
            summary["MODAL_CLOSEST_DETERMINISTIC_FRACTION"] = 0.0
        summaries.append(summary)
    return summaries


def _render_summary(
    long_rows: list[dict[str, object]],
    summary_rows: list[dict[str, object]],
    output_path: Path,
) -> None:
    cohorts = ("UNCLASSIFIED", "RANDOM")
    observed = [row for row in long_rows if row["OBSERVED"]]
    figure, axes = plt.subplots(1, 3, figsize=(12, 3.6), constrained_layout=True)

    class_names = [strategy.name for strategy in IPIDStrategy if strategy != IPIDStrategy.NOT_ENOUGH_SAMPLES]
    bottoms = np.zeros(len(cohorts))
    for class_name in class_names:
        values = []
        for cohort in cohorts:
            selected = [row for row in observed if row["COHORT"] == cohort]
            values.append(
                100.0 * sum(row["CLASS"] == class_name for row in selected) / len(selected)
                if selected
                else 0.0
            )
        axes[0].bar(cohorts, values, bottom=bottoms, label=class_name.replace("_", " ").title())
        bottoms += np.asarray(values)
    axes[0].set(ylabel="Repeated outcomes [%]", ylim=(0, 100), title="Classification")
    axes[0].tick_params(axis="x", rotation=20)

    width = 0.22
    positions = np.arange(len(cohorts))
    for index, component in enumerate(COMPONENTS):
        values = []
        for cohort in cohorts:
            selected = [row for row in observed if row["COHORT"] == cohort]
            values.append(
                100.0
                * sum(bool(row[f"{component}_REJECTS_RANDOM"]) for row in selected)
                / len(selected)
                if selected
                else 0.0
            )
        axes[1].bar(positions + (index - 1) * width, values, width, label=component.title())
    axes[1].set_xticks(positions, cohorts, rotation=20)
    axes[1].set(ylabel="Component rejection [%]", ylim=(0, 100), title="RANDOM components")

    for cohort, color in zip(cohorts, ("#808080", "#F5AA60")):
        values = [
            100.0 * float(row["MODE_CLASS_FRACTION"])
            for row in summary_rows
            if row["COHORT"] == cohort
        ]
        axes[2].hist(values, bins=np.arange(-2.5, 105, 5), alpha=0.65, label=cohort.title(), color=color)
    axes[2].set(
        xlabel="Modal-class share per address [%]",
        ylabel="Addresses",
        xlim=(0, 100),
        title="Per-address stability",
    )
    for axis in axes:
        axis.grid(axis="y", alpha=0.25)
    axes[0].legend(fontsize=7, loc="center left", bbox_to_anchor=(1.0, 0.5))
    axes[1].legend(fontsize=8)
    axes[2].legend(fontsize=8)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path)
    plt.close(figure)


def evaluate_random_reproducibility(
    manifest_path: Path,
    repeat_ids: list[str],
    *,
    target: str = DEFAULT_TARGET,
    cohort_path: Path | None = None,
    raw_root: Path = RAW_DATA_DIR,
    processed_root: Path = PROCESSED_DATA_DIR,
    data_dir: Path | None = None,
    figure_dir: Path | None = None,
    null_tables: CandidateNullTables | None = None,
) -> dict[str, Path]:
    """Classify repeated raw Mass runs and report per-address stability."""
    if not repeat_ids:
        raise ValueError("at least one repeat measurement id is required")
    measurement, _, baseline_snapshot, _ = _measurement_paths(
        manifest_path,
        target,
        raw_root=raw_root,
        processed_root=processed_root,
    )
    if data_dir is None or figure_dir is None:
        default_data, default_figures = _default_output_directories(
            measurement,
            processed_root=processed_root,
        )
        data_dir = data_dir or default_data
        figure_dir = figure_dir or default_figures
    stem = _artifact_stem(measurement)
    cohort_path = cohort_path or data_dir / f"{stem}-cohort.pq"
    if not cohort_path.is_file():
        raise FileNotFoundError(cohort_path)
    cohort = pq.read_table(cohort_path)
    cohort_rows = cohort.to_pylist()
    cohort_ips = [str(row["IP_ADDR"]) for row in cohort_rows]
    cohort_by_ip = {str(row["IP_ADDR"]): row for row in cohort_rows}
    baseline_cfg = load_config(baseline_snapshot)
    tables = production_candidate_null_tables() if null_tables is None else null_tables

    long_rows: list[dict[str, object]] = []
    repeat_metadata: list[dict[str, object]] = []
    for repeat_index, repeat_id in enumerate(repeat_ids, start=1):
        directory = raw_root / "ipid" / repeat_id
        raw_path = directory / INPUT_NAME
        snapshot_path = directory / "ipid.snapshot.yaml"
        if not snapshot_path.is_file():
            raise FileNotFoundError(snapshot_path)
        cfg = load_config(snapshot_path)
        if (
            cfg.connection_count != baseline_cfg.connection_count
            or cfg.requests_per_connection != baseline_cfg.requests_per_connection
        ):
            raise ValueError(
                f"{repeat_id}: shape {cfg.connection_count}x{cfg.requests_per_connection} "
                f"differs from baseline {baseline_cfg.connection_count}x{baseline_cfg.requests_per_connection}"
            )
        raw_sequences = _read_repeat_sequences(raw_path, cohort_ips)
        observed_ips = sorted(raw_sequences)
        sequences = [parse_sequence(raw_sequences[ip], cfg.sequence_length) for ip in observed_ips]
        classes = _classify_sequences(sequences, cfg, tables)
        classified = dict(zip(observed_ips, classes))
        repeat_metadata.append(
            {
                "repeat_index": repeat_index,
                "measurement_id": repeat_id,
                "observed": len(observed_ips),
                "missing": len(cohort_ips) - len(observed_ips),
            }
        )
        for ip_addr in cohort_ips:
            base = cohort_by_ip[ip_addr]
            common: dict[str, object] = {
                "IP_ADDR": ip_addr,
                "COHORT": base["COHORT"],
                "CONTROL_STRATUM": base.get("CONTROL_STRATUM", ""),
                "BASELINE_CLASS": base["BASELINE_CLASS"],
                "REPEAT_INDEX": repeat_index,
                "MEASUREMENT_ID": repeat_id,
                "OBSERVED": ip_addr in raw_sequences,
            }
            if ip_addr not in raw_sequences:
                long_rows.append({**common, "CLASS": "NO_MEASUREMENT"})
                continue
            sequence = parse_sequence(raw_sequences[ip_addr], cfg.sequence_length)
            long_rows.append(
                {
                    **common,
                    "CLASS": classified[ip_addr],
                    "IPID_SEQUENCE": raw_sequences[ip_addr],
                    **_diagnostics(sequence, cfg, tables),
                }
            )

    summary_rows = _summary_rows(long_rows, cohort)
    long_path = data_dir / f"{stem}-repetitions.pq"
    summary_path = data_dir / f"{stem}-summary.pq"
    long_csv = figure_dir / f"{stem}-repetitions.csv"
    summary_csv = figure_dir / f"{stem}-summary.csv"
    json_path = figure_dir / f"{stem}-evaluation.json"
    pdf_path = figure_dir / f"{stem}-summary.pdf"
    long_table = _write_table(long_rows, long_path)
    summary_table = _write_table(summary_rows, summary_path)
    figure_dir.mkdir(parents=True, exist_ok=True)
    pacsv.write_csv(long_table, long_csv)
    pacsv.write_csv(summary_table, summary_csv)
    _render_summary(long_rows, summary_rows, pdf_path)

    observed = [row for row in long_rows if row["OBSERVED"]]
    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "manifest": str(manifest_path),
        "target": target,
        "baseline_measurement_id": measurement.measurement_id,
        "threshold": CANDIDATE_RANDOM_MIN_SCORE,
        "cohort_size": len(cohort_ips),
        "repeat_count": len(repeat_ids),
        "repeats": repeat_metadata,
        "component_rejections": {
            component.lower(): sum(
                bool(row[f"{component}_REJECTS_RANDOM"]) for row in observed
            )
            for component in COMPONENTS
        },
        "methodology": {
            "classification": "production v7 classifier applied directly to repeated raw rows",
            "component_stability": "all below-threshold components are retained; limiting component is additional metadata",
            "near_match": "distance to every exact deterministic rule is reported as violation count and rate",
            "missing_targets": "cohort addresses absent from a repeated ipid.pq are NO_MEASUREMENT and excluded from p-value fractions",
            "safety": "evaluation does not create or overwrite strategies.pq",
        },
        "artifacts": {
            "repetitions": str(long_path),
            "summary": str(summary_path),
            "repetitions_csv": str(long_csv),
            "summary_csv": str(summary_csv),
            "figure": str(pdf_path),
        },
    }
    json_path.write_text(json.dumps(metadata, indent=2) + "\n")
    return {
        "repetitions": long_path,
        "summary": summary_path,
        "repetitions_csv": long_csv,
        "summary_csv": summary_csv,
        "json": json_path,
        "pdf": pdf_path,
    }


@app.command("prepare")
def prepare_command(
    manifest: Path = MANIFEST_ARGUMENT,
    target: str = typer.Option(DEFAULT_TARGET, help="fixed-interval Mass manifest target"),
    control_count: int | None = typer.Option(
        None,
        min=0,
        help="RANDOM controls; default equals the UNCLASSIFIED population",
    ),
    maximum_targets: int = typer.Option(
        DEFAULT_MAXIMUM_TARGETS,
        min=0,
        help="hard cap across UNCLASSIFIED and RANDOM controls",
    ),
    seed: int = typer.Option(42, help="deterministic control-sampling seed"),
) -> None:
    """Create the frozen cohort, offline diagnostics, and measurement targets."""
    artifacts = prepare_random_reproducibility(
        manifest,
        target=target,
        control_count=control_count,
        maximum_targets=maximum_targets,
        seed=seed,
    )
    for name, path in artifacts.items():
        typer.echo(f"{name}: {path}")


@app.command("evaluate")
def evaluate_command(
    manifest: Path = MANIFEST_ARGUMENT,
    repeat_id: list[str] = REPEAT_IDS_OPTION,
    target: str = typer.Option(DEFAULT_TARGET, help="fixed-interval Mass manifest target"),
    cohort: Path | None = COHORT_OPTION,
) -> None:
    """Evaluate repeated raw measurements without overwriting classifications."""
    artifacts = evaluate_random_reproducibility(
        manifest,
        repeat_id,
        target=target,
        cohort_path=cohort,
    )
    for name, path in artifacts.items():
        typer.echo(f"{name}: {path}")


if __name__ == "__main__":
    app()
