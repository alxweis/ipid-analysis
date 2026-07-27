"""Interactively inspect raw IP-ID sequences for one classified strategy.

The inspector joins a measurement's raw ``ipid.pq`` with its processed
``strategies.pq``, deterministically samples matching addresses, and shows one
sequence at a time. Closing the figure window advances to the next sample.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import typer

from ipid_analysis.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.manifest import load_manifest, resolve
from ipid_analysis.strategies import (
    INPUT_NAME,
    MAX_INC,
    MODULUS,
    OUTPUT_NAME,
    RANDOM_STRUCTURE_MIN_SCORE,
    STRATEGY_NAMES,
    MeasurementConfig,
    _cluster_counts_mass,
    _sorted_present_values,
    load_config,
    random_structure_bounded_increment_pvalues,
    random_structure_features,
    random_structure_scores,
)

app = typer.Typer(add_completion=False)


@dataclass(frozen=True)
class SequenceSample:
    ip_addr: str
    raw_sequence: str


@dataclass(frozen=True)
class SequenceDiagnostics:
    present_count: int
    unique_count: int
    cluster_count: int
    maximum_gap: int
    uniformity_pvalue: float
    occupancy_pvalue: float
    maximum_gap_pvalue: float
    bounded_increment_pvalue: float
    random_score: float
    limiting_component: str


def sample_sequences(
    raw_path: Path,
    strategies_path: Path,
    *,
    strategy: str,
    sample_count: int,
    seed: int,
) -> tuple[int, list[SequenceSample]]:
    """Return the matching population size and a deterministic random sample."""
    if strategy not in STRATEGY_NAMES:
        raise ValueError(f"unknown strategy {strategy!r}; choose one of {STRATEGY_NAMES}")
    if sample_count < 1:
        raise ValueError("sample_count must be at least 1")
    for path in (raw_path, strategies_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    connection = duckdb.connect()
    try:
        total = connection.execute(
            """
            SELECT count(*)
            FROM read_parquet($strategies)
            WHERE CAST(IPID_SELECTION_STRATEGY AS VARCHAR) = $strategy
            """,
            {"strategies": str(strategies_path), "strategy": strategy},
        ).fetchone()[0]
        rows = connection.execute(
            """
            SELECT raw.IP_ADDR, raw.IPID_SEQUENCE
            FROM read_parquet($raw) AS raw
            INNER JOIN read_parquet($strategies) AS classified USING (IP_ADDR)
            WHERE CAST(classified.IPID_SELECTION_STRATEGY AS VARCHAR) = $strategy
            ORDER BY hash(raw.IP_ADDR || ':' || CAST($seed AS VARCHAR))
            LIMIT $sample_count
            """,
            {
                "raw": str(raw_path),
                "strategies": str(strategies_path),
                "strategy": strategy,
                "seed": int(seed),
                "sample_count": int(sample_count),
            },
        ).fetchall()
    finally:
        connection.close()

    return int(total), [SequenceSample(str(ip_addr), str(sequence)) for ip_addr, sequence in rows]


def parse_sequence(raw_sequence: str, sequence_length: int) -> np.ndarray:
    """Parse the persisted comma-separated sequence, preserving missing positions."""
    parsed: list[int] = []
    for token in raw_sequence.split(","):
        try:
            value = int(token)
        except ValueError:
            value = -1
        parsed.append(value if 0 <= value < MODULUS else -1)

    if len(parsed) > sequence_length:
        raise ValueError(
            f"sequence contains {len(parsed)} positions, expected at most {sequence_length}"
        )
    parsed.extend([-1] * (sequence_length - len(parsed)))
    return np.asarray(parsed, dtype=np.int64)


def sequence_diagnostics(sequence: np.ndarray, cfg: MeasurementConfig) -> SequenceDiagnostics:
    """Calculate the same RANDOM-score components used by production classification."""
    if sequence.shape != (cfg.sequence_length,):
        raise ValueError(
            f"sequence shape is {sequence.shape}, expected ({cfg.sequence_length},)"
        )

    values = sequence[None, :]
    present = values >= 0
    lengths = present.sum(axis=1).astype(np.int64)
    ordered = _sorted_present_values(values, present)
    features = random_structure_features(values, present, ordered=ordered)
    bounded = random_structure_bounded_increment_pvalues(values, present, cfg)
    score = random_structure_scores(values, present, cfg, ordered=ordered)
    clusters = _cluster_counts_mass(values, present, lengths, ordered=ordered)

    components = {
        "uniformity": float(features.uniformity_pvalue[0]),
        "occupancy": float(features.occupancy_pvalue[0]),
        "maximum gap": float(features.maximum_gap_pvalue[0]),
        f"bounded increments (1..{MAX_INC})": float(bounded[0]),
    }
    return SequenceDiagnostics(
        present_count=int(features.sample_count[0]),
        unique_count=int(features.unique_count[0]),
        cluster_count=int(clusters[0]),
        maximum_gap=int(features.maximum_gap[0]),
        uniformity_pvalue=components["uniformity"],
        occupancy_pvalue=components["occupancy"],
        maximum_gap_pvalue=components["maximum gap"],
        bounded_increment_pvalue=components[f"bounded increments (1..{MAX_INC})"],
        random_score=float(score[0]),
        limiting_component=min(components, key=components.get),
    )


def _plot_sample(
    sample: SequenceSample,
    sequence: np.ndarray,
    diagnostics: SequenceDiagnostics,
    cfg: MeasurementConfig,
    *,
    strategy: str,
    sample_index: int,
    sample_count: int,
    population_count: int,
) -> tuple[plt.Figure, dict[str, int | bool]]:
    position = np.arange(cfg.sequence_length)
    visible = sequence.astype(float)
    visible[sequence < 0] = np.nan
    connection = position % cfg.connection_count
    colors = plt.get_cmap("tab10").colors

    figure, (full_axis, destination_axis, connection_axis) = plt.subplots(
        3,
        1,
        figsize=(12, 9.2),
        constrained_layout=True,
    )
    figure.suptitle(
        f"{strategy}: {sample.ip_addr}  "
        f"[{sample_index + 1}/{sample_count}; population={population_count:,}]\n"
        f"RANDOM score S={diagnostics.random_score:.3e} "
        f"(threshold={RANDOM_STRUCTURE_MIN_SCORE:.3e}; "
        f"limited by {diagnostics.limiting_component})",
        fontsize=13,
    )

    full_axis.plot(position, visible, color="0.72", linewidth=0.8, zorder=1)
    for connection_index in range(cfg.connection_count):
        selected = (connection == connection_index) & np.isfinite(visible)
        full_axis.scatter(
            position[selected],
            visible[selected],
            s=22,
            color=colors[connection_index % len(colors)],
            label=f"Connection {connection_index + 1}",
            zorder=2,
        )
    missing = ~np.isfinite(visible)
    if missing.any():
        full_axis.scatter(
            position[missing],
            np.full(missing.sum(), 0.02),
            transform=full_axis.get_xaxis_transform(),
            marker="x",
            color="black",
            s=22,
            label="Missing reply",
            clip_on=False,
        )
    full_axis.set(
        xlabel="Measurement index",
        ylabel="IP-ID",
        xlim=(-1, cfg.sequence_length),
        ylim=(-1000, MODULUS + 1000),
    )
    full_axis.grid(alpha=0.25)
    full_axis.legend(ncol=min(cfg.connection_count + int(missing.any()), 5), fontsize=9)

    for destination_index in range(2):
        subsequence = visible[destination_index::2]
        destination_axis.plot(
            np.arange(len(subsequence)),
            subsequence,
            marker="o",
            markersize=3,
            linewidth=1,
            label=f"Destination {destination_index + 1}",
        )
    destination_axis.set(
        xlabel="Index within destination subsequence",
        ylabel="IP-ID",
        ylim=(-1000, MODULUS + 1000),
    )
    destination_axis.grid(alpha=0.25)
    destination_axis.legend(fontsize=9)

    request_index = np.arange(cfg.requests_per_connection)
    for connection_index in range(cfg.connection_count):
        subsequence = visible[connection_index :: cfg.connection_count]
        connection_axis.plot(
            request_index,
            subsequence,
            marker="o",
            markersize=3,
            linewidth=1,
            color=colors[connection_index % len(colors)],
            label=f"Connection {connection_index + 1}",
        )
    connection_axis.set(
        xlabel="Request index within connection",
        ylabel="IP-ID",
        xlim=(-0.5, cfg.requests_per_connection - 0.5),
        ylim=(-1000, MODULUS + 1000),
    )
    connection_axis.grid(alpha=0.25)

    diagnostics_text = (
        f"Replies: {diagnostics.present_count}/{cfg.sequence_length}\n"
        f"Unique IP-IDs: {diagnostics.unique_count}\n"
        f"Circular clusters: {diagnostics.cluster_count}\n"
        f"Maximum circular gap: {diagnostics.maximum_gap}\n"
        f"Uniformity p: {diagnostics.uniformity_pvalue:.3e}\n"
        f"Occupancy p: {diagnostics.occupancy_pvalue:.3e}\n"
        f"Maximum-gap p: {diagnostics.maximum_gap_pvalue:.3e}\n"
        f"Bounded-increment p: {diagnostics.bounded_increment_pvalue:.3e}\n\n"
        "Close window / Right / Space: next\n"
        "Left: previous    Q / Esc: quit"
    )
    connection_axis.text(
        1.01,
        0.5,
        diagnostics_text,
        transform=connection_axis.transAxes,
        va="center",
        fontsize=9,
        family="monospace",
    )

    action: dict[str, int | bool] = {"step": 1, "quit": False}

    def on_key(event) -> None:
        if event.key in {"right", " ", "n", "enter"}:
            action["step"] = 1
            plt.close(figure)
        elif event.key in {"left", "p", "backspace"}:
            action["step"] = -1
            plt.close(figure)
        elif event.key in {"q", "escape"}:
            action["quit"] = True
            plt.close(figure)

    figure.canvas.mpl_connect("key_press_event", on_key)
    return figure, action


@app.command()
def main(
    target: str = typer.Argument(
        ...,
        help="dotted target, e.g. icmp.ipid.no-connection.fixed-interval.mass",
    ),
    manifest: Path = typer.Option(
        RAW_DATA_DIR / "manifest.json",
        help="measurement manifest JSON",
    ),
    strategy: str = typer.Option(
        "UNCLASSIFIED",
        help="strategy whose raw sequences should be sampled",
    ),
    samples: int = typer.Option(10, min=1, help="number of sampled sequences"),
    seed: int = typer.Option(42, help="deterministic sampling seed"),
    save_dir: Path | None = typer.Option(
        None,
        help="save PNGs here instead of opening interactive windows",
    ),
) -> None:
    """Show sampled sequences; closing each window advances to the next one."""
    strategy = strategy.upper().replace("-", "_")
    measurement = resolve(load_manifest(manifest), target)
    if measurement is None:
        raise typer.BadParameter(f"{target!r} is not present in {manifest}")
    if measurement.scale != "mass":
        raise typer.BadParameter("this inspector currently supports mass measurements")

    raw_dir = RAW_DATA_DIR / measurement.input_key
    raw_path = raw_dir / INPUT_NAME
    snapshot_path = raw_dir / "ipid.snapshot.yaml"
    strategies_path = measurement.artifact_path(
        PROCESSED_DATA_DIR,
        Path(OUTPUT_NAME).stem,
    )
    cfg = load_config(snapshot_path)

    population_count, selected = sample_sequences(
        raw_path,
        strategies_path,
        strategy=strategy,
        sample_count=samples,
        seed=seed,
    )
    if not selected:
        typer.echo(f"No {strategy} rows found in {strategies_path}")
        raise typer.Exit()

    typer.echo(
        f"Sampled {len(selected)} of {population_count:,} {strategy} rows. "
        + (f"Writing figures to {save_dir}." if save_dir else "Close each window to advance.")
    )
    if save_dir is not None:
        save_dir.mkdir(parents=True, exist_ok=True)
        for index, sample in enumerate(selected):
            sequence = parse_sequence(sample.raw_sequence, cfg.sequence_length)
            diagnostics = sequence_diagnostics(sequence, cfg)
            figure, _ = _plot_sample(
                sample,
                sequence,
                diagnostics,
                cfg,
                strategy=strategy,
                sample_index=index,
                sample_count=len(selected),
                population_count=population_count,
            )
            safe_ip = sample.ip_addr.replace(":", "_")
            output_path = save_dir / f"{index + 1:02d}-{safe_ip}.png"
            figure.savefig(output_path, dpi=160)
            plt.close(figure)
            typer.echo(output_path)
        return

    index = 0
    while 0 <= index < len(selected):
        sample = selected[index]
        sequence = parse_sequence(sample.raw_sequence, cfg.sequence_length)
        diagnostics = sequence_diagnostics(sequence, cfg)
        _, action = _plot_sample(
            sample,
            sequence,
            diagnostics,
            cfg,
            strategy=strategy,
            sample_index=index,
            sample_count=len(selected),
            population_count=population_count,
        )
        plt.show()
        if action["quit"]:
            break
        index = max(0, index + int(action["step"]))


if __name__ == "__main__":
    app()
