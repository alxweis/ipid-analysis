from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import matplotlib.pyplot as plt
import typer
from loguru import logger
from matplotlib.figure import Figure

from ipid_analysis.classify import STRATEGY_NAMES
from ipid_analysis.config import IPID_STRATEGY_DIST_JSON_NAME, FIGURES_DIR, IPID_STRATEGY_DIST_PDF_NAME, \
    PROCESSED_DATA_DIR, IPID_STRATEGY_DATA_NAME


def _strategy_counts(strategies_path: Path) -> dict[str, int]:
    with duckdb.connect() as con:
        rows = con.execute(
            "SELECT IPID_SELECTION_STRATEGY AS s, count(*) AS n "
            "FROM read_parquet($p) GROUP BY 1",
            {"p": str(strategies_path)},
        ).fetchall()

    counts = dict(rows)
    return {name: int(counts.get(name, 0)) for name in STRATEGY_NAMES}


def _strategy_percentages(counts: dict[str, int], total: int) -> dict[str, float]:
    if not total:
        return {name: 0.0 for name in STRATEGY_NAMES}
    return {name: 100.0 * counts[name] / total for name in STRATEGY_NAMES}


def strategy_distribution(measurement: str) -> tuple[dict[str, int], int, dict[str, float]]:
    strategies_path = PROCESSED_DATA_DIR / Path(measurement) / IPID_STRATEGY_DATA_NAME
    if not strategies_path.is_file():
        logger.error(f"not found: {strategies_path}")
        raise typer.Exit(code=1)

    counts = _strategy_counts(strategies_path)
    total = sum(counts.values())
    percentages = _strategy_percentages(counts, total)

    return counts, total, percentages


def info_strategy_distribution(
        measurement: str,
        counts: dict[str, int],
        total: int,
        percentages: dict[str, float]
) -> dict[str, Any]:
    info = {
        "source": measurement,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_ips": total,
        "counts": counts,
        "percentages": percentages,
    }

    output_path = FIGURES_DIR / Path(measurement) / IPID_STRATEGY_DIST_JSON_NAME
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(info, indent=2) + "\n")
    return info


def plot_strategy_distribution(measurement: str, percentages: dict[str, float]) -> tuple[Figure, Path]:
    labels = list(percentages)
    values = [percentages[k] for k in labels]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(labels, values, color="#4C72B0")
    ax.set_xlabel("IPID selection strategy")
    ax.set_ylabel("Share of IPs (%)")
    ax.set_title("IPID selection-strategy distribution")
    ax.set_ylim(0, max(values) * 1.15 if any(values) else 1)
    for x, v in enumerate(values):
        if v > 0:
            ax.text(x, v, f"{v:.1f}%", ha="center", va="bottom", fontsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    output_path = FIGURES_DIR / Path(measurement) / IPID_STRATEGY_DIST_PDF_NAME
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    return fig, output_path
