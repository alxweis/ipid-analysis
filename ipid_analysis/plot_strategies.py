"""Plot the IPID selection-strategy distribution of one measurement.

    python ipid_analysis/plot_strategies.py tcp.ipid.no-connection.fixed-interval.base
    -> reports/figures/<zmap_id>/no-connection/fixed-interval-base/n-fi-b_strategies.pdf
    -> reports/figures/<zmap_id>/no-connection/fixed-interval-base/n-fi-b_strategies.json
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from loguru import logger
import typer

from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement, load_manifest, resolve
from ipid_analysis.plots import plot_strategy_distribution, strategy_counts, strategy_percentages
from ipid_analysis.strategies import DEFAULT_MANIFEST

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


def render(m: IpidMeasurement) -> tuple[Path, Path]:
    """Write the strategy PDF + JSON for one measurement. Returns (pdf, json)."""
    strategies_path = m.artifact_path(PROCESSED_DATA_DIR, "strategies")
    if not strategies_path.is_file():
        raise FileNotFoundError(strategies_path)

    pdf_path = m.artifact_path(FIGURES_DIR, "strategies", "pdf")
    json_path = m.artifact_path(FIGURES_DIR, "strategies", "json")

    counts = strategy_counts(strategies_path)
    total = sum(counts.values())
    percentages = strategy_percentages(counts, total)

    info = {
        **_meta(m),
        "source": str(strategies_path),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_ips": total,
        "counts": counts,
        "percentages": percentages,
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(info, indent=2) + "\n")

    plot_strategy_distribution(percentages, pdf_path, title=f"IPID strategy distribution — {m.stem}")
    return pdf_path, json_path


@app.command()
def main(
    target: str = typer.Argument(
        ..., help="dotted target, e.g. tcp.ipid.no-connection.fixed-interval.base"
    ),
    manifest: Path = typer.Option(DEFAULT_MANIFEST, help="measurement manifest JSON"),
) -> None:
    m = resolve(load_manifest(manifest), target)
    if m is None:
        logger.error(f"{target}: not present in {manifest}")
        raise typer.Exit(code=1)
    try:
        pdf_path, json_path = render(m)
    except FileNotFoundError as exc:
        logger.error(f"not found: {exc} (run strategies.py first)")
        raise typer.Exit(code=1) from exc
    logger.success(f"[{m.target}] -> {pdf_path}  +  {json_path.name}")


if __name__ == "__main__":
    app()
