"""Plot the IPID selection-strategy distribution of a classified measurement."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
from loguru import logger
import matplotlib.pyplot as plt
import typer

from ipid_analysis.strategies import OUTPUT_NAME, STRATEGY_NAMES
from ipid_analysis.config import FIGURES_DIR, PROCESSED_DATA_DIR

app = typer.Typer()


def strategy_distribution(strategies_path: Path) -> dict[str, float]:
    """Percentage share per strategy over all rows.

    Uses a streaming ``GROUP BY`` in DuckDB, so it never loads the file into
    memory and works on the full >100 GB output. Strategies are returned in the
    canonical order, missing ones as 0.0 (so plots are comparable across runs).
    """
    con = duckdb.connect()
    rows = con.execute(
        "SELECT IPID_SELECTION_STRATEGY AS s, count(*) AS n "
        "FROM read_parquet($p) GROUP BY 1",
        {"p": str(strategies_path)},
    ).fetchall()
    con.close()

    counts = dict(rows)
    total = sum(counts.values())
    if not total:
        return {name: 0.0 for name in STRATEGY_NAMES}
    return {name: 100.0 * counts.get(name, 0) / total for name in STRATEGY_NAMES}


def plot_strategy_distribution(
    dist: dict[str, float], output_path: Path, title: Optional[str] = None
) -> plt.Figure:
    """Bar chart of ``dist`` (x = IPID strategy, y = share in %). Saves to
    ``output_path`` (PDF by extension) and returns the figure."""
    labels = list(dist)
    values = [dist[k] for k in labels]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(labels, values, color="#4C72B0")
    ax.set_xlabel("IPID selection strategy")
    ax.set_ylabel("Share of IPs (%)")
    ax.set_title(title or "IPID selection-strategy distribution")
    ax.set_ylim(0, max(values) * 1.15 if any(values) else 1)
    for x, v in enumerate(values):
        if v > 0:
            ax.text(x, v, f"{v:.1f}%", ha="center", va="bottom", fontsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    return fig


@app.command()
def main(
    measurement: str = typer.Argument(
        ..., help="measurement key, e.g. ipid/icmp_2026-06-29_15-56-56"
    ),
    output_path: Optional[Path] = typer.Option(
        None, help="output PDF (default: reports/figures/<name>_strategy_distribution.pdf)"
    ),
) -> None:
    strategies_path = PROCESSED_DATA_DIR / measurement / OUTPUT_NAME
    if not strategies_path.is_file():
        logger.error(f"not found: {strategies_path}")
        raise typer.Exit(code=1)

    name = Path(measurement).name
    if output_path is None:
        output_path = FIGURES_DIR / f"{name}_strategy_distribution.pdf"

    logger.info(f"reading {strategies_path}")
    dist = strategy_distribution(strategies_path)
    plot_strategy_distribution(dist, output_path, title=f"IPID strategy distribution — {name}")
    logger.success(f"figure saved -> {output_path}")


if __name__ == "__main__":
    app()
