"""Plot the probing-interval histogram of one measurement.

    python ipid_analysis/plot_probing_intervals.py tcp.ipid.nec.fi.base
    -> reports/figures/<zmap_id>/<stem>_probing-intervals.pdf
    -> reports/figures/<zmap_id>/<stem>_probing-intervals.json
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from loguru import logger
import typer

from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR
from ipid_analysis.manifest import IpidMeasurement, load_manifest, resolve
from ipid_analysis.plots import interval_stats, plot_probing_intervals
from ipid_analysis.strategies import DEFAULT_MANIFEST

app = typer.Typer()

KIND = "probing-intervals"  # output kind (hyphen); input file uses probing_intervals


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
    bins: int = typer.Option(50, help="histogram bins"),
    clip_quantile: float = typer.Option(0.99, help="clip the histogram range at this quantile"),
) -> None:
    m = resolve(load_manifest(manifest), target)
    if m is None:
        logger.error(f"{target}: not present in {manifest}")
        raise typer.Exit(code=1)

    intervals_path = PROCESSED_DATA_DIR / m.zmap_id / m.output_name("probing_intervals")
    if not intervals_path.is_file():
        logger.error(f"not found: {intervals_path} (run probing_intervals.py first)")
        raise typer.Exit(code=1)

    fig_dir = FIGURES_DIR / m.zmap_id
    pdf_path = fig_dir / m.artifact_name(KIND, "pdf")
    json_path = fig_dir / m.artifact_name(KIND, "json")

    stats = interval_stats(intervals_path, n_bins=bins, clip_quantile=clip_quantile)

    info = {
        **_meta(m),
        "source": str(intervals_path),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        **stats,
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(info, indent=2) + "\n")

    plot_probing_intervals(stats, pdf_path, title=f"Probing intervals — {m.stem}")
    logger.success(f"[{m.target}] -> {pdf_path}  +  {json_path.name}")


if __name__ == "__main__":
    app()
