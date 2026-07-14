"""Plot the IPID selection-strategy distribution of one measurement.

    python ipid_analysis/plot_strategies.py tcp.ipid.nec.fi.base
    -> reports/figures/<zmap_id>/<stem>_strategies.pdf
    -> reports/figures/<zmap_id>/<stem>_strategies.json
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
        "conn_mode": m.conn_mode,
        "interval": m.interval,
        "scale": m.scale,
        "measurement_id": m.measurement_id,
        "zmap_id": m.zmap_id,
    }


@app.command()
def main(
    target: str = typer.Argument(..., help="dotted target, e.g. tcp.ipid.nec.fi.base"),
    manifest: Path = typer.Option(DEFAULT_MANIFEST, help="measurement manifest JSON"),
) -> None:
    m = resolve(load_manifest(manifest), target)
    if m is None:
        logger.error(f"{target}: not present in {manifest}")
        raise typer.Exit(code=1)

    strategies_path = PROCESSED_DATA_DIR / m.zmap_id / m.output_name("strategies")
    if not strategies_path.is_file():
        logger.error(f"not found: {strategies_path} (run strategies.py first)")
        raise typer.Exit(code=1)

    fig_dir = FIGURES_DIR / m.zmap_id
    pdf_path = fig_dir / m.artifact_name("strategies", "pdf")
    json_path = fig_dir / m.artifact_name("strategies", "json")

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
    logger.success(f"[{m.target}] -> {pdf_path}  +  {json_path.name}")


if __name__ == "__main__":
    app()
