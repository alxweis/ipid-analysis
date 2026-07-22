"""Plot an individual or merged IPID selection-strategy distribution.

    python ipid_analysis/plot_strategies.py tcp.ipid.no-connection.fixed-interval.base

    python ipid_analysis/plot_strategies.py \
        tcp.ipid.no-connection.rt-based.base \
        tcp.ipid.no-connection.fixed-interval.mass
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from loguru import logger
import typer

from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR
from ipid_analysis.coverage import coverage_for_measurement, ipid_measurement_coverage
from ipid_analysis.manifest import IpidMeasurement, load_manifest, resolve
from ipid_analysis.plots import plot_strategy_distribution, strategy_counts, strategy_percentages
from ipid_analysis.strategies import DEFAULT_MANIFEST
from ipid_analysis.strategy_merge import StrategyMerge, resolve_strategy_merge

app = typer.Typer()


def _meta(m: IpidMeasurement) -> dict:
    return {
        "target": m.target,
        "protocol": m.protocol,
        "connection_mode": m.connection_mode,
        "interval": m.interval,
        "scale": m.scale,
        "measurement_id": m.measurement_id,
        "zmap_id": m.zmap_id,
    }


def _render_paths(
    strategies_path: Path,
    pdf_path: Path,
    json_path: Path,
    metadata: dict,
    title: str,
    coverage: float,
) -> tuple[Path, Path]:
    if not strategies_path.is_file():
        raise FileNotFoundError(strategies_path)

    counts = strategy_counts(strategies_path)
    total = sum(counts.values())
    percentages = strategy_percentages(counts, total)

    info = {
        **metadata,
        "source": str(strategies_path),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_ips": total,
        "ipid_measurement_coverage": coverage,
        "counts": counts,
        "percentages": percentages,
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(info, indent=2) + "\n")

    plot_strategy_distribution(percentages, pdf_path, title=title)
    return pdf_path, json_path


def render(
    m: IpidMeasurement,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    raw_root: Path = RAW_DATA_DIR,
) -> tuple[Path, Path]:
    """Write the strategy PDF + JSON for one measurement. Returns (pdf, json)."""
    return _render_paths(
        m.artifact_path(processed_root, "strategies"),
        m.artifact_path(figures_root, "strategies", "pdf"),
        m.artifact_path(figures_root, "strategies", "json"),
        _meta(m),
        f"IPID strategy distribution — {m.stem}",
        coverage_for_measurement(m, processed_root=processed_root, raw_root=raw_root),
    )


def render_merged(
    merge: StrategyMerge,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    raw_root: Path = RAW_DATA_DIR,
) -> tuple[Path, Path]:
    """Write the strategy PDF + JSON for a merged base/mass artifact."""
    metadata = {
        "target": merge.target,
        "protocol": merge.protocol,
        "connection_mode": merge.connection_mode,
        "zmap_id": merge.zmap_id,
        "base_target": merge.base.target,
        "base_measurement_id": merge.base.measurement_id,
        "mass_target": merge.mass.target,
        "mass_measurement_id": merge.mass.measurement_id,
    }
    strategies_path = merge.artifact_path(processed_root, "strategies")
    return _render_paths(
        strategies_path,
        merge.artifact_path(figures_root, "strategies", "pdf"),
        merge.artifact_path(figures_root, "strategies", "json"),
        metadata,
        f"Merged IPID strategy distribution — {merge.stem}",
        ipid_measurement_coverage(
            strategies_path,
            raw_root / "zmap" / merge.zmap_id / "zmap.pq",
        ),
    )


@app.command()
def main(
    targets: list[str] = typer.Argument(
        ...,
        help="one measurement target, or base and mass targets for a merged plot",
    ),
    manifest: Path = typer.Option(DEFAULT_MANIFEST, help="measurement manifest JSON"),
) -> None:
    data = load_manifest(manifest)
    try:
        if len(targets) == 1:
            target = targets[0]
            measurement = resolve(data, target)
            if measurement is None:
                raise ValueError(f"{target}: not present in {manifest}")
            label = measurement.target
            pdf_path, json_path = render(measurement)
        elif len(targets) == 2:
            merge = resolve_strategy_merge(data, targets[0], targets[1])
            label = merge.target
            pdf_path, json_path = render_merged(merge)
        else:
            raise ValueError("provide either one measurement target or one base/mass target pair")
    except (FileNotFoundError, ValueError) as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1) from exc
    logger.success(f"[{label}] -> {pdf_path}  +  {json_path.name}")


if __name__ == "__main__":
    app()
