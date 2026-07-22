"""Plot the IPID-increment CDF (one line per strategy) of one measurement.

    python ipid_analysis/plot_increments.py tcp.ipid.no-connection.fixed-interval.base
    -> reports/figures/<zmap_id>/no-connection/fixed-interval-base/n-fi-b_increments.pdf
    -> reports/figures/<zmap_id>/no-connection/fixed-interval-base/n-fi-b_increments.json
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from loguru import logger
import typer

from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.coverage import coverage_for_measurement
from ipid_analysis.manifest import IpidMeasurement, load_manifest, resolve
from ipid_analysis.plots import increment_cdf, plot_increment_cdf
from ipid_analysis.strategies import DEFAULT_MANIFEST

app = typer.Typer()

KIND = "increments"


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
    """Write the increment-CDF PDF + JSON for one measurement. Returns (pdf, json)."""
    increments_path = m.artifact_path(PROCESSED_DATA_DIR, KIND)
    if not increments_path.is_file():
        raise FileNotFoundError(increments_path)

    pdf_path = m.artifact_path(FIGURES_DIR, KIND, "pdf")
    json_path = m.artifact_path(FIGURES_DIR, KIND, "json")

    cdf = increment_cdf(increments_path)
    info = {
        **_meta(m),
        "source": str(increments_path),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ipid_measurement_coverage": coverage_for_measurement(m),
        "strategies": cdf,
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(info, indent=2) + "\n")

    plot_increment_cdf(cdf, pdf_path, title=f"IP-ID increment CDF — {m.stem}")
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
        logger.error(f"not found: {exc} (run increments.py first)")
        raise typer.Exit(code=1) from exc
    logger.success(f"[{m.target}] -> {pdf_path}  +  {json_path.name}")


if __name__ == "__main__":
    app()
