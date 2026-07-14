"""Reusable plotting + stats for postprocessed measurements.

Functions take explicit paths and know nothing about the manifest -- the CLIs
(plot_strategies.py, plot_probing_intervals.py) resolve dotted targets to paths.
All aggregation runs in DuckDB (streaming), so it scales to the full output.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib
import numpy as np

matplotlib.use("Agg")  # headless: safe for CLI/servers

import matplotlib.pyplot as plt  # noqa: E402

from ipid_analysis.strategies import STRATEGY_NAMES  # noqa: E402

BAR_COLOR = "#4C72B0"
ACCENT = "#DD8452"


# --------------------------------------------------------------------------
# Strategy distribution
# --------------------------------------------------------------------------
def strategy_counts(strategies_path: Path) -> dict[str, int]:
    """Absolute count per strategy (canonical order, missing ones as 0)."""
    con = duckdb.connect()
    rows = con.execute(
        "SELECT IPID_SELECTION_STRATEGY AS s, count(*) AS n "
        "FROM read_parquet($p) GROUP BY 1",
        {"p": str(strategies_path)},
    ).fetchall()
    con.close()
    counts = dict(rows)
    return {name: int(counts.get(name, 0)) for name in STRATEGY_NAMES}


def strategy_percentages(counts: dict[str, int], total: int) -> dict[str, float]:
    if not total:
        return {name: 0.0 for name in STRATEGY_NAMES}
    return {name: 100.0 * counts[name] / total for name in STRATEGY_NAMES}


def plot_strategy_distribution(
    percentages: dict[str, float], output_pdf: Path, title: str | None = None
) -> Path:
    """Bar chart: x = IPID strategy, y = share of IP addresses (%)."""
    labels = list(percentages)
    values = [percentages[k] for k in labels]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(labels, values, color=BAR_COLOR)
    ax.set_xlabel("IPID selection strategy")
    ax.set_ylabel("Share of IP addresses (%)")
    ax.set_title(title or "IPID selection-strategy distribution")
    ax.set_ylim(0, max(values) * 1.15 if any(values) else 1)
    for x, v in enumerate(values):
        if v > 0:
            ax.text(x, v, f"{v:.1f}%", ha="center", va="bottom", fontsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_pdf, bbox_inches="tight")
    plt.close(fig)
    return output_pdf


# --------------------------------------------------------------------------
# Probing intervals
# --------------------------------------------------------------------------
def interval_stats(intervals_path: Path, n_bins: int = 50, clip_quantile: float = 0.99) -> dict:
    """Summary stats + a binned histogram of all probing intervals (microseconds).

    The histogram range is clipped at ``clip_quantile`` so a long tail of outliers
    (e.g. retransmits) doesn't squash the bulk. Everything is computed in DuckDB
    over the unnested list column -- no full materialization in Python."""
    con = duckdb.connect()
    unnest = "SELECT unnest(PROBING_INTERVALS) AS iv FROM read_parquet($p)"
    n, lo, hi, mean, std, p50, p90, pc = con.execute(
        f"SELECT count(*), min(iv), max(iv), avg(iv), stddev_pop(iv), "
        f"approx_quantile(iv, 0.5), approx_quantile(iv, 0.9), approx_quantile(iv, {clip_quantile}) "
        f"FROM ({unnest})",
        {"p": str(intervals_path)},
    ).fetchone()

    if not n:
        con.close()
        return {"count": 0, "unit": "microseconds", "histogram": {"bin_edges": [], "counts": []}}

    hi_clip = pc if pc > lo else hi
    bw = max((hi_clip - lo) / n_bins, 1)
    rows = con.execute(
        f"SELECT least(floor((iv - {lo}) / {bw}), {n_bins - 1})::INT AS b, count(*) AS c "
        f"FROM ({unnest}) WHERE iv BETWEEN {lo} AND {hi_clip} GROUP BY b ORDER BY b",
        {"p": str(intervals_path)},
    ).fetchall()
    con.close()

    counts = [0] * n_bins
    for b, c in rows:
        counts[int(b)] = int(c)
    edges = [lo + i * bw for i in range(n_bins + 1)]

    return {
        "count": int(n),
        "unit": "microseconds",
        "min": int(lo),
        "max": int(hi),
        "mean": float(mean),
        "stddev": float(std),
        "p50": int(p50),
        "p90": int(p90),
        f"p{int(clip_quantile * 100)}": int(pc),
        "histogram": {
            "bin_edges": edges,
            "counts": counts,
            "range": [int(lo), float(hi_clip)],
            "clipped_beyond_range": int(n - sum(counts)),
        },
    }


def plot_probing_intervals(stats: dict, output_pdf: Path, title: str | None = None) -> Path:
    """Histogram of probing intervals (x = interval in µs, y = count)."""
    hist = stats.get("histogram", {})
    edges = hist.get("bin_edges", [])
    counts = hist.get("counts", [])

    fig, ax = plt.subplots(figsize=(10, 5))
    if counts:
        centers = [(edges[i] + edges[i + 1]) / 2 for i in range(len(counts))]
        width = (edges[1] - edges[0]) * 0.95 if len(edges) > 1 else 1
        ax.bar(centers, counts, width=width, color=BAR_COLOR, align="center")
        if "p50" in stats:
            ax.axvline(stats["p50"], color=ACCENT, ls="--", lw=1,
                       label=f"median {stats['p50']:,} µs")
            ax.legend()
    ax.set_xlabel("Probing interval (µs)")
    ax.set_ylabel("Count")
    ax.set_title(title or "Probing-interval distribution")
    ax.spines[["top", "right"]].set_visible(False)

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_pdf, bbox_inches="tight")
    plt.close(fig)
    return output_pdf


# --------------------------------------------------------------------------
# Increment CDF (one line per strategy)
# --------------------------------------------------------------------------
def increment_cdf(increments_path: Path, n_points: int = 200) -> dict:
    """Per-strategy empirical CDF of the IPID increments, as (increment,
    cumulative_pct) points. Computed with DuckDB approx_quantile over the
    unnested INCREMENTS column (sketch-based, streaming -> scales to the full
    output)."""
    probs = np.linspace(0.0, 1.0, n_points)
    probs_sql = "[" + ",".join(f"{p:.5f}" for p in probs) + "]"
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT CAST(IPID_SELECTION_STRATEGY AS VARCHAR) AS s, count(*) AS n, "
        f"approx_quantile(iv, {probs_sql}) AS qs "
        f"FROM (SELECT IPID_SELECTION_STRATEGY, unnest(INCREMENTS) AS iv "
        f"      FROM read_parquet($p)) GROUP BY 1",
        {"p": str(increments_path)},
    ).fetchall()
    con.close()

    pct = [round(float(p) * 100.0, 3) for p in probs]
    out = {}
    for s, n, qs in rows:
        if n:
            out[s] = {
                "count": int(n),
                "increment": [int(x) for x in qs],
                "cumulative_pct": pct,
            }
    return out


def plot_increment_cdf(cdf: dict, output_pdf: Path, title: str | None = None) -> Path:
    """CDF line per strategy: x = IP-ID increment (log, powers of 10),
    y = cumulative percentage [%]. Zero increments are clipped to 1 for the log
    axis."""
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for name in STRATEGY_NAMES:  # stable legend order
        d = cdf.get(name)
        if not d:
            continue
        x = np.maximum(np.asarray(d["increment"], dtype=float), 1.0)
        ax.plot(x, d["cumulative_pct"], label=f"{name} (n={d['count']:,})", linewidth=1.6)

    ax.set_xscale("log")
    ax.set_xlabel("IP-ID Increment")
    ax.set_ylabel("Cumulative Percentage [%]")
    ax.set_ylim(0, 100)
    ax.set_title(title or "IP-ID increment CDF")
    ax.grid(True, which="both", ls=":", alpha=0.4)
    if cdf:
        ax.legend(fontsize=8)

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_pdf, bbox_inches="tight")
    plt.close(fig)
    return output_pdf
