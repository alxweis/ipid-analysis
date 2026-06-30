from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
import matplotlib.pyplot as plt
import typer

from ipid_analysis.classify import STRATEGY_NAMES

app = typer.Typer()


def strategy_distribution(strategies_path: Path) -> dict[str, float]:
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


def plot_strategy_distribution(dist: dict[str, float], output_path: Path, title: Optional[str] = None) -> plt.Figure:
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
def main(measurement: str) -> None:
    pass


if __name__ == "__main__":
    app()
