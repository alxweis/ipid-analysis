"""Build and analyse inter-protocol IP-ID measurements.

The module deliberately reuses the three existing deterministic counter rules.
It does not try to infer hash inputs or bucket identities: it only asks whether
the observed sequence remains compatible with the already assigned strategy
when protocols are combined.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import math
from pathlib import Path
import time

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer
import yaml

from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.manifest import load_manifest
from ipid_analysis.paper_figures import (
    COMPACT_PAPER_PDF_PADDING_INCHES,
    COMPACT_PAPER_STROKE_WIDTH,
    configure_compact_validation_style,
)
from ipid_analysis.strategies import MAX_INC, MODULUS, STRATEGY_COLORS, STRATEGY_PRETTY
from ipid_analysis.strategy_merge import iter_strategy_merges

app = typer.Typer(no_args_is_help=True)

CLASSIFIER_VERSION = "1"
COUNTER_STRATEGIES = ("PER_DESTINATION", "SINGLE", "PER_BUCKET")
GROUPS = {
    "icmp-tcp": ("icmp", "tcp"),
    "icmp-udp": ("icmp", "udp"),
    "tcp-udp": ("tcp", "udp"),
    "icmp-tcp-udp": ("icmp", "tcp", "udp"),
}
RUN_MANIFEST_VERSION = 1
INTERPROTOCOL_FIGURES_DIR = FIGURES_DIR / "interprotocol"

DEPLOYMENT_PLOT_CATEGORIES = (
    "PROTOCOL_SHARED",
    "PROTOCOL_ISOLATED",
    "NOT_ENOUGH_SAMPLES",
    "STRATEGY_NOT_CONFIRMED",
)
DEPLOYMENT_PLOT_LABELS = {
    "PROTOCOL_SHARED": "Protocol-Shared",
    "PROTOCOL_ISOLATED": "Protocol-Isolated",
    "NOT_ENOUGH_SAMPLES": "Not Enough Samples",
    "STRATEGY_NOT_CONFIRMED": "Strategy Not Confirmed",
}
DEPLOYMENT_PLOT_COLORS = {
    "PROTOCOL_SHARED": STRATEGY_COLORS["PER_BUCKET"],
    "PROTOCOL_ISOLATED": STRATEGY_COLORS["RANDOM"],
    "NOT_ENOUGH_SAMPLES": STRATEGY_COLORS["NOT_ENOUGH_SAMPLES"],
    "STRATEGY_NOT_CONFIRMED": STRATEGY_COLORS["PER_DESTINATION"],
}
GROUP_INTERSECTION_LABELS = {
    "icmp-tcp": r"ICMP$\cap$TCP",
    "icmp-udp": r"ICMP$\cap$UDP",
    "tcp-udp": r"TCP$\cap$UDP",
    "icmp-tcp-udp": r"ICMP$\cap$TCP$\cap$UDP",
}
INTERSECTION_XLABEL = "Protocol Intersection (#IP Addr.)"

TARGET_SCHEMA = pa.schema([("IP_ADDR", pa.string()), ("IPID_SELECTION_STRATEGY", pa.string())])
RESULT_SCHEMA = pa.schema(
    [
        ("IP_ADDR", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.string()),
        ("PROTOCOL_GROUP", pa.string()),
        ("DEPLOYMENT", pa.string()),
    ],
    metadata={b"interprotocol_classifier_version": CLASSIFIER_VERSION.encode()},
)


@dataclass(frozen=True)
class InterprotocolConfig:
    protocols: tuple[str, ...]
    connection_count: int
    requests_per_connection: int

    @property
    def sequence_length(self) -> int:
        return len(self.protocols) * self.connection_count * self.requests_per_connection

    def layout(self) -> tuple[np.ndarray, np.ndarray]:
        positions = np.arange(self.sequence_length)
        protocol = positions % len(self.protocols)
        connection = (positions // len(self.protocols)) % self.connection_count
        return protocol, connection


def load_interprotocol_config(path: Path) -> InterprotocolConfig:
    data = yaml.safe_load(path.read_text())
    section = data.get("interprotocol", {})
    raw_protocols = section["protocols"] if "protocols" in section else data["protocols"]
    protocols = tuple(str(p).replace("udp-dns", "udp") for p in raw_protocols)
    if protocols not in GROUPS.values():
        raise ValueError(f"unsupported protocol order: {protocols}")
    return InterprotocolConfig(
        protocols=protocols,
        connection_count=int(data["connection_count"]),
        requests_per_connection=int(data["requests_per_connection"]),
    )


def _strategy_file(manifest_path: Path, protocol: str, processed_root: Path) -> Path:
    manifest = load_manifest(manifest_path)
    aliases = (protocol, "udp-dns") if protocol == "udp" else (protocol,)
    candidates = [m for m in iter_strategy_merges(manifest) if m.protocol in aliases]
    if len(candidates) != 1:
        raise ValueError(
            f"{manifest_path}: expected one canonical no-connection strategy merge "
            f"for {protocol}, found {len(candidates)}"
        )
    path = candidates[0].artifact_path(processed_root, "strategies")
    if not path.is_file():
        raise FileNotFoundError(path)
    return path


def _campaign(path: Path) -> tuple[str, dict[str, Path]]:
    data = json.loads(path.read_text())
    campaign_id = str(data["campaign_id"])
    if not campaign_id or Path(campaign_id).name != campaign_id:
        raise ValueError("campaign_id must be a non-empty directory name")
    manifests = data["manifests"]
    base = path.resolve().parent
    resolved = {}
    for protocol in ("icmp", "tcp", "udp"):
        candidate = Path(manifests[protocol])
        resolved[protocol] = candidate if candidate.is_absolute() else base / candidate
    return campaign_id, resolved


def build_target_files(
    campaign_path: Path,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    output_root: Path | None = None,
    figures_root: Path = INTERPROTOCOL_FIGURES_DIR,
    threads: int = 0,
) -> Path:
    """Create exclusive pair cohorts and one triple cohort.

    Pair cohorts exclude only addresses in the same-strategy triple cohort.
    An address with TCP=UDP=SINGLE and ICMP=PER_DESTINATION therefore remains
    in the TCP/UDP cohort, exactly as intended.
    """
    campaign_id, manifests = _campaign(campaign_path)
    inputs = {
        protocol: _strategy_file(manifest, protocol, processed_root)
        for protocol, manifest in manifests.items()
    }
    output_dir = (output_root or processed_root / "interprotocol") / campaign_id
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(config={"threads": threads} if threads else {})
    for protocol, path in inputs.items():
        escaped_input = str(path).replace("'", "''")
        con.execute(
            f"CREATE VIEW {protocol} AS SELECT IP_ADDR, "
            "CAST(IPID_SELECTION_STRATEGY AS VARCHAR) AS strategy "
            f"FROM read_parquet('{escaped_input}') WHERE "
            "CAST(IPID_SELECTION_STRATEGY AS VARCHAR) "
            f"IN ({','.join(repr(s) for s in COUNTER_STRATEGIES)})"
        )
    triple_sql = """
        SELECT i.IP_ADDR, i.strategy AS IPID_SELECTION_STRATEGY
        FROM icmp i JOIN tcp t USING (IP_ADDR) JOIN udp u USING (IP_ADDR)
        WHERE i.strategy = t.strategy AND i.strategy = u.strategy
    """
    con.execute(f"CREATE TEMP TABLE triple AS {triple_sql}")
    queries = {
        "icmp-tcp": """SELECT i.IP_ADDR, i.strategy AS IPID_SELECTION_STRATEGY
            FROM icmp i JOIN tcp t USING (IP_ADDR)
            LEFT JOIN triple x ON x.IP_ADDR=i.IP_ADDR AND x.IPID_SELECTION_STRATEGY=i.strategy
            WHERE i.strategy=t.strategy AND x.IP_ADDR IS NULL""",
        "icmp-udp": """SELECT i.IP_ADDR, i.strategy AS IPID_SELECTION_STRATEGY
            FROM icmp i JOIN udp u USING (IP_ADDR)
            LEFT JOIN triple x ON x.IP_ADDR=i.IP_ADDR AND x.IPID_SELECTION_STRATEGY=i.strategy
            WHERE i.strategy=u.strategy AND x.IP_ADDR IS NULL""",
        "tcp-udp": """SELECT t.IP_ADDR, t.strategy AS IPID_SELECTION_STRATEGY
            FROM tcp t JOIN udp u USING (IP_ADDR)
            LEFT JOIN triple x ON x.IP_ADDR=t.IP_ADDR AND x.IPID_SELECTION_STRATEGY=t.strategy
            WHERE t.strategy=u.strategy AND x.IP_ADDR IS NULL""",
        "icmp-tcp-udp": "SELECT * FROM triple",
    }
    counts: dict[str, dict[str, int]] = {}
    for group, query in queries.items():
        target = output_dir / f"{group}-targets.pq"
        escaped = str(target).replace("'", "''")
        con.execute(
            f"COPY ({query} ORDER BY IP_ADDR) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        rows = con.execute(
            "SELECT IPID_SELECTION_STRATEGY, count(*) FROM read_parquet(?) GROUP BY 1",
            [str(target)],
        ).fetchall()
        counts[group] = {str(strategy): int(count) for strategy, count in rows}
    input_rows = {
        protocol: int(
            con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
        )
        for protocol, path in inputs.items()
    }
    con.close()
    summary = {
        "campaign_id": campaign_id,
        "inputs": {key: str(value) for key, value in inputs.items()},
        "input_rows": input_rows,
        "strategies": list(COUNTER_STRATEGIES),
        "pair_semantics": "pair intersection minus same-strategy triple",
        "targets": counts,
    }
    (output_dir / "target-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    figure_dir = figures_root / campaign_id
    figure_dir.mkdir(parents=True, exist_ok=True)
    plot_target_population(counts, figure_dir / "target-population.pdf")
    (output_dir / "target-population.pdf").unlink(missing_ok=True)
    return output_dir


def _increments_ok(values: np.ndarray, upper: int) -> bool:
    if values.size < 2:
        return False
    increments = np.diff(values.astype(np.int64)) % MODULUS
    return bool(((increments >= 1) & (increments <= upper)).all())


def compatible(
    sequence: np.ndarray, cfg: InterprotocolConfig, strategy: str, subset: tuple[str, ...]
) -> bool:
    protocol_index, connection = cfg.layout()
    wanted = np.isin(protocol_index, [cfg.protocols.index(p) for p in subset])
    if strategy == "SINGLE":
        return _increments_ok(sequence[wanted], MAX_INC)
    key = connection % 2 if strategy == "PER_DESTINATION" else connection
    upper = 1 if strategy == "PER_DESTINATION" else MAX_INC
    return all(
        _increments_ok(sequence[wanted & (key == value)], upper) for value in np.unique(key)
    )


def partition_compatible(
    sequence: np.ndarray,
    cfg: InterprotocolConfig,
    strategy: str,
    partition: tuple[tuple[str, ...], ...],
) -> bool:
    return all(compatible(sequence, cfg, strategy, group) for group in partition)


def classify_sequence(sequence: np.ndarray, cfg: InterprotocolConfig, strategy: str) -> str:
    if strategy not in COUNTER_STRATEGIES:
        return "STRATEGY_NOT_SUPPORTED"
    if sequence.size != cfg.sequence_length or (sequence < 0).any():
        return "NOT_ENOUGH_SAMPLES"
    if compatible(sequence, cfg, strategy, cfg.protocols):
        return "SHARED_" + "_".join(p.upper() for p in cfg.protocols)
    isolated = tuple((p,) for p in cfg.protocols)
    if len(cfg.protocols) == 2:
        return (
            "PROTOCOL_ISOLATED"
            if partition_compatible(sequence, cfg, strategy, isolated)
            else "STRATEGY_NOT_CONFIRMED"
        )
    pair_partitions = (
        ("SHARED_ICMP_TCP", (("icmp", "tcp"), ("udp",))),
        ("SHARED_ICMP_UDP", (("icmp", "udp"), ("tcp",))),
        ("SHARED_TCP_UDP", (("tcp", "udp"), ("icmp",))),
    )
    matches = [
        name
        for name, partition in pair_partitions
        if partition_compatible(sequence, cfg, strategy, partition)
    ]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        return "AMBIGUOUS"
    return (
        "PROTOCOL_ISOLATED"
        if partition_compatible(sequence, cfg, strategy, isolated)
        else "STRATEGY_NOT_CONFIRMED"
    )


def _parse_sequence(value: str) -> np.ndarray:
    return np.asarray(
        [int(item) if item != "-" else -1 for item in value.split(",")], dtype=np.int64
    )


def classify_file(
    raw_path: Path,
    snapshot_path: Path,
    output_dir: Path,
    *,
    figure_dir: Path | None = None,
) -> Path:
    cfg = load_interprotocol_config(snapshot_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / "interprotocol-deployments.pq"
    temporary = output.with_suffix(".pq.part")
    temporary.unlink(missing_ok=True)
    counts: Counter[tuple[str, str]] = Counter()
    total = 0
    writer = pq.ParquetWriter(temporary, RESULT_SCHEMA, compression="zstd")
    try:
        parquet = pq.ParquetFile(raw_path)
        for batch in parquet.iter_batches(
            batch_size=100_000,
            columns=["IP_ADDR", "IPID_SELECTION_STRATEGY", "IPID_SEQUENCE"],
        ):
            output_rows = []
            for row in batch.to_pylist():
                deployment = classify_sequence(
                    _parse_sequence(row["IPID_SEQUENCE"]),
                    cfg,
                    row["IPID_SELECTION_STRATEGY"],
                )
                output_rows.append(
                    {
                        "IP_ADDR": row["IP_ADDR"],
                        "IPID_SELECTION_STRATEGY": row["IPID_SELECTION_STRATEGY"],
                        "PROTOCOL_GROUP": "-".join(cfg.protocols),
                        "DEPLOYMENT": deployment,
                    }
                )
                counts[(row["IPID_SELECTION_STRATEGY"], deployment)] += 1
            total += len(output_rows)
            if output_rows:
                writer.write_table(pa.Table.from_pylist(output_rows, schema=RESULT_SCHEMA))
    finally:
        writer.close()
    temporary.replace(output)
    summary = {
        "classifier_version": CLASSIFIER_VERSION,
        "protocols": list(cfg.protocols),
        "rows": total,
        "counts": {
            f"{strategy}:{deployment}": count for (strategy, deployment), count in counts.items()
        },
    }
    (output_dir / "interprotocol-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    figure_dir = figure_dir or INTERPROTOCOL_FIGURES_DIR / output_dir.name
    figure_dir.mkdir(parents=True, exist_ok=True)
    plot_deployments(counts, figure_dir / "interprotocol-deployments.pdf")
    (output_dir / "interprotocol-deployments.pdf").unlink(missing_ok=True)
    return output


def plot_target_population(counts: dict[str, dict[str, int]], output: Path) -> None:
    labels = list(GROUPS)
    bottom = np.zeros(len(labels))
    fig, ax = plt.subplots(figsize=(7.16, 3.2))
    for strategy in COUNTER_STRATEGIES:
        values = np.asarray([counts[group].get(strategy, 0) for group in labels])
        ax.bar(
            labels,
            values,
            bottom=bottom,
            label=STRATEGY_PRETTY[strategy],
            color=STRATEGY_COLORS[strategy],
        )
        bottom += values
    ax.set_ylabel("Target IP addresses")
    ax.legend(frameon=False)
    ax.tick_params(axis="x", rotation=20)
    fig.tight_layout()
    fig.savefig(output)
    plt.close(fig)


def plot_deployments(counts: Counter[tuple[str, str]], output: Path) -> None:
    deployments = sorted({deployment for _, deployment in counts})
    fig, axes = plt.subplots(1, len(COUNTER_STRATEGIES), figsize=(7.16, 2.8), squeeze=False)
    colors = plt.get_cmap("tab10").colors
    for ax, strategy in zip(axes[0], COUNTER_STRATEGIES, strict=True):
        strategy_counts = {
            deployment: counts[(strategy, deployment)] for deployment in deployments
        }
        total = max(1, sum(strategy_counts.values()))
        bottom = 0.0
        for index, deployment in enumerate(deployments):
            value = 100.0 * strategy_counts[deployment] / total
            ax.bar(
                [STRATEGY_PRETTY[strategy]],
                [value],
                bottom=bottom,
                color=colors[index % len(colors)],
                label=deployment,
            )
            bottom += value
        ax.set_ylim(0, 100)
        ax.tick_params(axis="x", rotation=25)
    axes[0, 0].set_ylabel("Targets [%]")
    if deployments:
        axes[0, -1].legend(frameon=False, fontsize=7, bbox_to_anchor=(1.02, 1), loc="upper left")
    fig.tight_layout()
    fig.savefig(output)
    plt.close(fig)


def load_run_manifest(path: Path) -> dict:
    data = json.loads(path.read_text())
    if data.get("version") != RUN_MANIFEST_VERSION:
        raise ValueError(
            f"unsupported inter-protocol run manifest version {data.get('version')!r}"
        )
    for field in ("campaign_id", "run_id"):
        value = str(data.get(field, ""))
        if not value or Path(value).name != value:
            raise ValueError(f"{field} must be a non-empty directory name")
    groups = data.get("groups")
    if not isinstance(groups, dict) or not groups:
        raise ValueError("run manifest must contain at least one protocol group")
    for group, specification in groups.items():
        if group not in GROUPS:
            raise ValueError(f"unsupported protocol group {group!r}")
        if specification.get("status") != "complete":
            raise ValueError(f"protocol group {group!r} is not complete")
        protocols = tuple(str(value) for value in specification.get("protocols", []))
        if protocols != GROUPS[group]:
            raise ValueError(
                f"protocol order for {group!r} is {protocols!r}, expected {GROUPS[group]!r}"
            )
        measurement_id = str(specification.get("measurement_id", ""))
        if not measurement_id.startswith(f"interprotocol-{group}_"):
            raise ValueError(f"invalid measurement id for {group!r}: {measurement_id!r}")
    return data


def _campaign_counts(rows: list[dict]) -> dict[str, Counter[tuple[str, str]]]:
    counts: dict[str, Counter[tuple[str, str]]] = {}
    for row in rows:
        group = str(row["PROTOCOL_GROUP"])
        counts.setdefault(group, Counter())[
            (str(row["IPID_SELECTION_STRATEGY"]), str(row["DEPLOYMENT"]))
        ] += 1
    return counts


def plot_campaign_deployments(counts: dict[str, Counter[tuple[str, str]]], output: Path) -> None:
    groups = [group for group in GROUPS if group in counts]
    deployments = sorted(
        {deployment for group_counts in counts.values() for _, deployment in group_counts}
    )
    colors = {
        deployment: plt.get_cmap("tab10").colors[index % 10]
        for index, deployment in enumerate(deployments)
    }
    columns = min(2, len(groups))
    rows = math.ceil(len(groups) / columns)
    fig, axes = plt.subplots(rows, columns, figsize=(7.16, 3.0 * rows), squeeze=False)
    for ax, group in zip(axes.flat, groups, strict=False):
        bottom = np.zeros(len(COUNTER_STRATEGIES))
        group_counts = counts[group]
        for deployment in deployments:
            values = []
            for strategy in COUNTER_STRATEGIES:
                total = sum(
                    count
                    for (candidate, _), count in group_counts.items()
                    if candidate == strategy
                )
                values.append(100 * group_counts[(strategy, deployment)] / total if total else 0)
            ax.bar(
                [STRATEGY_PRETTY[strategy] for strategy in COUNTER_STRATEGIES],
                values,
                bottom=bottom,
                color=colors[deployment],
                label=deployment,
            )
            bottom += values
        ax.set_title(group)
        ax.set_ylim(0, 100)
        ax.tick_params(axis="x", rotation=25)
    for ax in list(axes.flat)[len(groups) :]:
        ax.set_visible(False)
    axes[0, 0].set_ylabel("Targets [%]")
    if deployments:
        axes[0, -1].legend(frameon=False, fontsize=7, bbox_to_anchor=(1.02, 1), loc="upper left")
    fig.tight_layout()
    fig.savefig(output)
    plt.close(fig)


def plot_campaign_missing(counts: dict[str, Counter[tuple[str, str]]], output: Path) -> None:
    groups = [group for group in GROUPS if group in counts]
    x = np.arange(len(groups))
    width = 0.24
    fig, ax = plt.subplots(figsize=(7.16, 3.2))
    for index, strategy in enumerate(COUNTER_STRATEGIES):
        values = []
        for group in groups:
            group_counts = counts[group]
            total = sum(
                count for (candidate, _), count in group_counts.items() if candidate == strategy
            )
            missing = group_counts[(strategy, "NOT_ENOUGH_SAMPLES")]
            values.append(100 * missing / total if total else 0)
        ax.bar(
            x + (index - 1) * width,
            values,
            width,
            label=STRATEGY_PRETTY[strategy],
            color=STRATEGY_COLORS[strategy],
        )
    ax.set_xticks(x, groups, rotation=20)
    ax.set_ylabel("Not enough samples [%]")
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(output)
    plt.close(fig)


def _deployment_plot_category(deployment: str) -> str:
    if deployment.startswith("SHARED_"):
        return "PROTOCOL_SHARED"
    if deployment == "PROTOCOL_ISOLATED":
        return deployment
    if deployment == "NOT_ENOUGH_SAMPLES":
        return deployment
    # AMBIGUOUS and unsupported/unexpected outcomes are not evidence for
    # either shared or isolated state, so the plot includes them as unconfirmed.
    return "STRATEGY_NOT_CONFIRMED"


def _strategy_deployment_percentages(
    counts: dict[str, Counter[tuple[str, str]]], strategy: str
) -> dict[str, dict[str, float]]:
    percentages = {}
    for group in GROUPS:
        grouped = Counter()
        for (candidate, deployment), count in counts.get(group, Counter()).items():
            if candidate == strategy:
                grouped[_deployment_plot_category(deployment)] += count
        total = sum(grouped.values())
        percentages[group] = {
            category: 100.0 * grouped[category] / total if total else 0.0
            for category in DEPLOYMENT_PLOT_CATEGORIES
        }
    return percentages


def _strategy_group_labels(
    counts: dict[str, Counter[tuple[str, str]]], strategy: str
) -> list[str]:
    labels = []
    for group in GROUPS:
        targets = sum(
            count
            for (candidate, _), count in counts.get(group, Counter()).items()
            if candidate == strategy
        )
        labels.append(f"{GROUP_INTERSECTION_LABELS[group]}\n({targets})")
    return labels


def plot_campaign_strategy_deployments(
    counts: dict[str, Counter[tuple[str, str]]], output_dir: Path
) -> list[Path]:
    """Write one four-group percentage plot for each counter strategy."""
    output_dir.mkdir(parents=True, exist_ok=True)
    configure_compact_validation_style()
    outputs = []
    for strategy in COUNTER_STRATEGIES:
        fig, ax = plt.subplots(figsize=(6.5, 2.55))
        x = np.arange(len(GROUPS)) * 0.84
        bottom = np.zeros(len(GROUPS))
        percentages = _strategy_deployment_percentages(counts, strategy)
        for category in DEPLOYMENT_PLOT_CATEGORIES:
            values = [percentages[group][category] for group in GROUPS]
            ax.bar(
                x,
                values,
                bottom=bottom,
                color=DEPLOYMENT_PLOT_COLORS[category],
                edgecolor="white",
                linewidth=COMPACT_PAPER_STROKE_WIDTH,
                width=0.48,
                label=DEPLOYMENT_PLOT_LABELS[category],
            )
            bottom += values
        ax.set_xticks(x, _strategy_group_labels(counts, strategy))
        ax.set_xlim(x[0] - 0.34, x[-1] + 0.34)
        ax.set_ylabel("Percentage [%]")
        ax.set_xlabel(INTERSECTION_XLABEL)
        ax.set_ylim(0, 100)
        ax.set_yticks(np.arange(0, 101, 20))
        ax.set_axisbelow(True)
        ax.grid(axis="y", color="#D9D9D9", linewidth=COMPACT_PAPER_STROKE_WIDTH)
        ax.tick_params(axis="x", rotation=0, pad=1.5)
        ax.legend(
            frameon=False,
            loc="lower center",
            bbox_to_anchor=(0.5, 1.01),
            ncol=4,
            columnspacing=1.0,
            handlelength=1.25,
            handletextpad=0.45,
            borderaxespad=0,
        )
        fig.subplots_adjust(left=0.095, right=0.995, bottom=0.28, top=0.79)
        slug = strategy.lower().replace("_", "-")
        output = output_dir / f"interprotocol-campaign-{slug}.pdf"
        fig.savefig(
            output,
            format="pdf",
            bbox_inches="tight",
            pad_inches=COMPACT_PAPER_PDF_PADDING_INCHES,
            metadata={
                "Title": f"Inter-protocol deployment for {STRATEGY_PRETTY[strategy]}",
                "Subject": "Inter-protocol IP-ID deployment percentages",
                "Creator": "ipid-analysis",
            },
        )
        plt.close(fig)
        outputs.append(output)
    return outputs


def classify_campaign(
    run_manifest_path: Path,
    *,
    raw_root: Path,
    output_root: Path = PROCESSED_DATA_DIR / "interprotocol",
    figures_root: Path = INTERPROTOCOL_FIGURES_DIR,
) -> Path:
    manifest = load_run_manifest(run_manifest_path)
    output_dir = output_root / manifest["campaign_id"] / "runs" / manifest["run_id"]
    output_dir.mkdir(parents=True, exist_ok=True)
    figure_dir = figures_root / manifest["campaign_id"] / "runs" / manifest["run_id"]
    figure_dir.mkdir(parents=True, exist_ok=True)
    tables = []
    measurement_ids = {}
    for group in GROUPS:
        specification = manifest["groups"].get(group)
        if specification is None:
            continue
        measurement_id = specification["measurement_id"]
        measurement_ids[group] = measurement_id
        measurement_dir = raw_root / measurement_id
        result = classify_file(
            measurement_dir / "interprotocol.pq",
            measurement_dir / "interprotocol.snapshot.yaml",
            output_dir / group,
            figure_dir=figure_dir / group,
        )
        tables.append(pq.read_table(result).cast(RESULT_SCHEMA))
    combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    combined_path = output_dir / "interprotocol-campaign-deployments.pq"
    temporary = combined_path.with_suffix(".pq.part")
    pq.write_table(combined, temporary, compression="zstd")
    temporary.replace(combined_path)
    output_rows = combined.to_pylist()
    counts = _campaign_counts(output_rows)
    for group in measurement_ids:
        counts.setdefault(group, Counter())
    summary = {
        "classifier_version": CLASSIFIER_VERSION,
        "campaign_id": manifest["campaign_id"],
        "run_id": manifest["run_id"],
        "groups": {
            group: {
                "measurement_id": measurement_ids[group],
                "rows": sum(group_counts.values()),
                "counts": {
                    f"{strategy}:{deployment}": count
                    for (strategy, deployment), count in sorted(group_counts.items())
                },
            }
            for group, group_counts in counts.items()
        },
        "rows": len(output_rows),
    }
    (output_dir / "interprotocol-campaign-summary.json").write_text(
        json.dumps(summary, indent=2) + "\n"
    )
    analysis_run = {
        "classifier_version": CLASSIFIER_VERSION,
        "source_manifest": str(run_manifest_path),
        "campaign_id": manifest["campaign_id"],
        "run_id": manifest["run_id"],
        "measurements": measurement_ids,
    }
    (output_dir / "interprotocol-campaign-run.json").write_text(
        json.dumps(analysis_run, indent=2) + "\n"
    )
    plot_campaign_deployments(counts, figure_dir / "interprotocol-campaign-deployments.pdf")
    plot_campaign_missing(counts, figure_dir / "interprotocol-campaign-missing.pdf")
    plot_campaign_strategy_deployments(counts, figure_dir)
    (output_dir / "interprotocol-campaign-deployments.pdf").unlink(missing_ok=True)
    (output_dir / "interprotocol-campaign-missing.pdf").unlink(missing_ok=True)
    return output_dir


def synthetic_sequence(
    cfg: InterprotocolConfig, strategy: str, partition: tuple[tuple[str, ...], ...], seed: int
) -> np.ndarray:
    rng = np.random.default_rng(seed)
    protocol_index, connection = cfg.layout()
    owners = {}
    for owner, protocols in enumerate(partition):
        for protocol in protocols:
            owners[protocol] = owner
    state: dict[tuple[int, int], int] = {}
    out = np.empty(cfg.sequence_length, dtype=np.int64)
    for i in range(cfg.sequence_length):
        protocol = cfg.protocols[int(protocol_index[i])]
        owner = owners[protocol]
        context = 0
        if strategy == "PER_DESTINATION":
            context = int(connection[i] % 2)
        elif strategy == "PER_BUCKET":
            context = int(connection[i])
        key = (owner, context)
        if key not in state:
            state[key] = (owner * 25_000 + context * 3_000 + int(rng.integers(0, 100))) % MODULUS
        step = 1 if strategy != "PER_BUCKET" else int(rng.integers(2, 40))
        state[key] = (state[key] + step) % MODULUS
        out[i] = state[key]
    return out


def validate_classifier(
    processed_dir: Path,
    figure_dir: Path,
    samples: int = 1000,
    seed: int = 42,
) -> Path:
    processed_dir.mkdir(parents=True, exist_ok=True)
    figure_dir.mkdir(parents=True, exist_ok=True)
    cases = []
    for protocols in GROUPS.values():
        cfg = InterprotocolConfig(protocols, 4, 4)
        partitions = [(protocols,)]
        if len(protocols) == 2:
            partitions.append(tuple((p,) for p in protocols))
        else:
            partitions.extend(
                [
                    (("icmp", "tcp"), ("udp",)),
                    (("icmp", "udp"), ("tcp",)),
                    (("tcp", "udp"), ("icmp",)),
                    (("icmp",), ("tcp",), ("udp",)),
                ]
            )
        for strategy in COUNTER_STRATEGIES:
            for partition in partitions:
                sequence = synthetic_sequence(cfg, strategy, partition, seed + len(cases))
                expected = "SHARED_" + "_".join(protocol.upper() for protocol in partition[0])
                if all(len(part) == 1 for part in partition):
                    expected = "PROTOCOL_ISOLATED"
                observed = classify_sequence(sequence, cfg, strategy)
                cases.append(("-".join(protocols), strategy, expected, observed))
                wrapped = (sequence + (MODULUS - 100)) % MODULUS
                cases.append(
                    (
                        "-".join(protocols),
                        strategy,
                        expected,
                        classify_sequence(wrapped, cfg, strategy),
                    )
                )
                with_loss = sequence.copy()
                with_loss[len(with_loss) // 2] = -1
                cases.append(
                    (
                        "-".join(protocols),
                        strategy,
                        "NOT_ENOUGH_SAMPLES",
                        classify_sequence(with_loss, cfg, strategy),
                    )
                )
    start = time.perf_counter()
    perf_cfg = InterprotocolConfig(("icmp", "tcp", "udp"), 4, 4)
    perf_sequence = synthetic_sequence(perf_cfg, "SINGLE", (perf_cfg.protocols,), seed)
    for _ in range(samples):
        classify_sequence(perf_sequence, perf_cfg, "SINGLE")
    elapsed = time.perf_counter() - start
    correct = sum(expected == observed for _, _, expected, observed in cases)
    report = {
        "classifier_version": CLASSIFIER_VERSION,
        "cases": len(cases),
        "correct": correct,
        "accuracy": correct / len(cases),
        "performance": {
            "sequences": samples,
            "seconds": elapsed,
            "sequences_per_second": samples / elapsed,
        },
        "confusion": dict(
            Counter(f"{expected}->{observed}" for _, _, expected, observed in cases)
        ),
    }
    path = processed_dir / "interprotocol-validation.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    plot_confusion(
        [case for case in cases if case[0].count("-") == 1],
        figure_dir / "interprotocol-validation-pair-confusion.pdf",
    )
    plot_confusion(
        [case for case in cases if case[0].count("-") == 2],
        figure_dir / "interprotocol-validation-triple-confusion.pdf",
    )
    if correct != len(cases):
        raise RuntimeError(f"synthetic validation failed: {correct}/{len(cases)}")
    return path


def plot_confusion(cases: list[tuple[str, str, str, str]], output: Path) -> None:
    labels = sorted({value for case in cases for value in case[2:]})
    matrix = np.zeros((len(labels), len(labels)), dtype=int)
    for _, _, expected, observed in cases:
        matrix[labels.index(expected), labels.index(observed)] += 1
    fig, ax = plt.subplots(figsize=(7.16, 4.0))
    image = ax.imshow(matrix, cmap="Blues")
    ax.set_xticks(range(len(labels)), labels, rotation=35, ha="right")
    ax.set_yticks(range(len(labels)), labels)
    ax.set_xlabel("Observed")
    ax.set_ylabel("Synthetic ground truth")
    for row in range(len(labels)):
        for column in range(len(labels)):
            ax.text(column, row, str(matrix[row, column]), ha="center", va="center")
    fig.colorbar(image, ax=ax)
    fig.tight_layout()
    fig.savefig(output)
    plt.close(fig)


@app.command("build-targets")
def build_targets_command(
    campaign: Path,
    processed_root: Path = PROCESSED_DATA_DIR,
    output_root: Path | None = None,
    figures_root: Path = INTERPROTOCOL_FIGURES_DIR,
    threads: int = 0,
) -> None:
    typer.echo(
        build_target_files(
            campaign,
            processed_root=processed_root,
            output_root=output_root,
            figures_root=figures_root,
            threads=threads,
        )
    )


@app.command("classify")
def classify_command(
    raw: Path,
    snapshot: Path,
    output_dir: Path,
    figure_dir: Path | None = None,
) -> None:
    typer.echo(classify_file(raw, snapshot, output_dir, figure_dir=figure_dir))


@app.command("analyse-campaign")
def analyse_campaign_command(
    run_manifest: Path,
    raw_root: Path = RAW_DATA_DIR / "ipid",
    output_root: Path = PROCESSED_DATA_DIR / "interprotocol",
    figures_root: Path = INTERPROTOCOL_FIGURES_DIR,
) -> None:
    typer.echo(
        classify_campaign(
            run_manifest,
            raw_root=raw_root,
            output_root=output_root,
            figures_root=figures_root,
        )
    )


@app.command("validate")
def validate_command(
    processed_dir: Path = PROCESSED_DATA_DIR / "interprotocol-validation",
    figure_dir: Path = FIGURES_DIR / "interprotocol-validation",
    samples: int = 1000,
    seed: int = 42,
) -> None:
    typer.echo(
        validate_classifier(
            processed_dir,
            figure_dir,
            samples=samples,
            seed=seed,
        )
    )


if __name__ == "__main__":
    app()
