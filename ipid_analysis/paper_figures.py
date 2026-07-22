"""ACM-paper figures comparing RT-based and fixed-interval base measurements.

The module creates compact, reproducible artifacts for three comparisons:

* probing intervals by continent (split violin),
* IP-ID increment distributions (paired empirical CDFs), and
* detected strategy intersections (row-normalized heatmap).

Each figure has a compact aggregate Parquet next to the processed data and a
JSON sidecar containing inputs, methodology, and summary statistics.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import duckdb
from loguru import logger
import matplotlib
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import typer

matplotlib.use("Agg")

from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

from ipid_analysis.comparison import (  # noqa: E402
    BaseComparison,
    resolve_base_comparison,
)
from ipid_analysis.config import (  # noqa: E402
    FIGURES_DIR,
    PROCESSED_DATA_DIR,
    RAW_DATA_DIR,
    REFERENCES_DIR,
)
from ipid_analysis.coverage import coverage_for_measurement  # noqa: E402
from ipid_analysis.manifest import load_manifest  # noqa: E402
from ipid_analysis.strategies import (  # noqa: E402
    DEFAULT_MANIFEST,
    STRATEGY_COLORS,
    STRATEGY_PRETTY,
)

app = typer.Typer()

PROBING_INTERVAL_KIND = "probing-intervals-by-continent"
CONTINENT_MAP_KIND = "ip-continents"
INCREMENT_KIND = "increment-distributions"
INTERSECTION_KIND = "strategy-intersection"

RT_MODE = "RT-based"
FIXED_MODE = "Fixed-Interval"
MODES = (RT_MODE, FIXED_MODE)
MODE_COLORS = {RT_MODE: "#6BAED6", FIXED_MODE: "#E78B8E"}
MODE_LINESTYLES = {RT_MODE: "--", FIXED_MODE: ":"}

INCREMENT_STRATEGIES = ("SINGLE", "PER_DESTINATION", "PER_CONNECTION", "PER_BUCKET")
INTERSECTION_STRATEGIES = (
    "REFLECTION",
    "CONSTANT",
    "SINGLE",
    "PER_CONNECTION",
    "PER_DESTINATION",
    "PER_BUCKET",
    "UNCLASSIFIED",
)

CONTINENT_ORDER = ("NA", "AS", "EU", "SA", "AF", "OC", "AN")
CONTINENT_NAMES = {
    "NA": "North America",
    "AS": "Asia",
    "EU": "Europe",
    "SA": "South America",
    "AF": "Africa",
    "OC": "Oceania",
    "AN": "Antarctica",
}

INTERVAL_SCHEMA = pa.schema(
    [
        ("CONTINENT_CODE", pa.string()),
        ("CONTINENT", pa.string()),
        ("MODE", pa.string()),
        ("IP_COUNT_MODE", pa.int64()),
        ("IP_COUNT_UNION", pa.int64()),
        ("P99_5_MS", pa.float64()),
        ("QUANTILES_MS", pa.list_(pa.float64())),
    ]
)

INCREMENT_SCHEMA = pa.schema(
    [
        ("MODE", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.string()),
        ("INCREMENT", pa.int32()),
        ("COUNT", pa.int64()),
        ("CUMULATIVE_COUNT", pa.int64()),
        ("CUMULATIVE_PERCENTAGE", pa.float64()),
        ("TOTAL_COUNT", pa.int64()),
        ("RETAINED_COUNT", pa.int64()),
        ("CLIPPED_COUNT", pa.int64()),
    ]
)

INTERSECTION_SCHEMA = pa.schema(
    [
        ("RT_BASED_STRATEGY", pa.string()),
        ("FIXED_INTERVAL_STRATEGY", pa.string()),
        ("COUNT", pa.int64()),
        ("RT_BASED_TOTAL", pa.int64()),
        ("PERCENTAGE", pa.float64()),
    ]
)


def configure_paper_style() -> None:
    """Apply one compact, serif style to all paper figures."""
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Linux Libertine O", "Libertinus Serif", "DejaVu Serif"],
            "font.size": 8,
            "axes.labelsize": 8,
            "legend.fontsize": 7,
            "xtick.labelsize": 7,
            "ytick.labelsize": 7,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )


def default_maxmind_database() -> Path | None:
    """Resolve a MaxMind country/city database from env or ``references/``."""
    configured = os.getenv("IPID_MAXMIND_DB")
    candidates = []
    if configured:
        candidates.append(Path(configured).expanduser())
    candidates.extend(
        [
            REFERENCES_DIR / "GeoLite2-Country.mmdb",
            REFERENCES_DIR / "GeoLite2-City.mmdb",
            REFERENCES_DIR / "GeoIP2-Country.mmdb",
            REFERENCES_DIR / "GeoIP2-City.mmdb",
        ]
    )
    return next((path for path in candidates if path.is_file()), None)


class MaxMindContinentLookup:
    """Small context-managed adapter around ``geoip2.database.Reader``."""

    def __init__(self, database: Path):
        try:
            import geoip2.database
            from geoip2.errors import AddressNotFoundError
        except ModuleNotFoundError as exc:
            raise RuntimeError("geoip2 is required for the continent figure") from exc
        self._not_found = AddressNotFoundError
        self._reader = geoip2.database.Reader(str(database))
        database_type = self._reader.metadata().database_type
        self._lookup = self._reader.city if "City" in database_type else self._reader.country

    def __call__(self, ip_address: str) -> tuple[str, str] | None:
        try:
            continent = self._lookup(ip_address).continent
        except (self._not_found, ValueError):
            return None
        if not continent.code:
            return None
        name = continent.name or CONTINENT_NAMES.get(continent.code, continent.code)
        return continent.code, name

    def close(self) -> None:
        self._reader.close()

    def __enter__(self) -> MaxMindContinentLookup:
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


def _write_table(table: pa.Table, output_path: Path, compression: str | None) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    pq.write_table(table, temporary, compression=compression)
    temporary.replace(output_path)
    return output_path


def _write_json(output_path: Path, value: dict) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(output_path)
    return output_path


def _require_files(*paths: Path) -> None:
    for path in paths:
        if not path.is_file():
            raise FileNotFoundError(path)


def _common_metadata(comparison: BaseComparison, sources: dict[str, Path]) -> dict:
    return {
        "target": comparison.target,
        "protocol": comparison.protocol,
        "connection_mode": comparison.connection_mode,
        "zmap_id": comparison.zmap_id,
        "measurements": {
            "rt_based": comparison.rt_based.measurement_id,
            "fixed_interval": comparison.fixed_interval.measurement_id,
        },
        "sources": {key: str(path) for key, path in sources.items()},
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def _comparison_coverage(
    comparison: BaseComparison,
    *,
    processed_root: Path,
    raw_root: Path,
) -> dict[str, float]:
    return {
        name: coverage_for_measurement(
            measurement, processed_root=processed_root, raw_root=raw_root
        )
        for name, measurement in (
            ("rt_based", comparison.rt_based),
            ("fixed_interval", comparison.fixed_interval),
        )
    }


def _save_figure(fig, output_path: Path, *, title: str, subject: str) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_path,
        bbox_inches="tight",
        pad_inches=0.02,
        metadata={"Title": title, "Subject": subject, "Creator": "ipid-analysis"},
    )
    plt.close(fig)
    return output_path


def build_continent_map(
    input_paths: tuple[Path, Path],
    output_path: Path,
    lookup: Callable[[str], tuple[str, str] | None],
    *,
    batch_size: int = 250_000,
    compression: str | None = "zstd",
    threads: int = 0,
) -> dict[str, int]:
    """Map the union of both measurements' IPs to MaxMind continents."""
    _require_files(*input_paths)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)
    schema = pa.schema(
        [("IP_ADDR", pa.string()), ("CONTINENT_CODE", pa.string()), ("CONTINENT", pa.string())]
    )
    con = duckdb.connect(config={"threads": threads} if threads else {})
    reader = con.execute(
        "SELECT DISTINCT IP_ADDR FROM ("
        "SELECT IP_ADDR FROM read_parquet($rt) UNION ALL "
        "SELECT IP_ADDR FROM read_parquet($fixed))",
        {"rt": str(input_paths[0]), "fixed": str(input_paths[1])},
    ).to_arrow_reader(batch_size)
    writer = pq.ParquetWriter(temporary, schema, compression=compression)
    mapped = 0
    unmapped = 0
    try:
        for batch in reader:
            addresses = batch.column("IP_ADDR").to_pylist()
            codes = []
            names = []
            for address in addresses:
                result = lookup(address)
                if result is None:
                    codes.append("ZZ")
                    names.append("Unknown")
                    unmapped += 1
                else:
                    code, name = result
                    codes.append(code)
                    names.append(name)
                    mapped += 1
            writer.write_table(
                pa.table(
                    {"IP_ADDR": addresses, "CONTINENT_CODE": codes, "CONTINENT": names},
                    schema=schema,
                )
            )
    except Exception:
        writer.close()
        temporary.unlink(missing_ok=True)
        raise
    else:
        writer.close()
        temporary.replace(output_path)
    finally:
        con.close()
    return {"mapped_ip_count": mapped, "unmapped_ip_count": unmapped}


def aggregate_probing_intervals(
    rt_path: Path,
    fixed_path: Path,
    continent_map_path: Path,
    output_path: Path,
    *,
    quantile_points: int = 256,
    compression: str | None = "zstd",
    threads: int = 0,
) -> dict:
    """Aggregate one per-IP interval median into continent/mode quantiles."""
    _require_files(rt_path, fixed_path, continent_map_path)
    probabilities = np.linspace(0.0, 0.995, quantile_points)
    probabilities_sql = "[" + ",".join(f"{p:.8f}" for p in probabilities) + "]"
    con = duckdb.connect(config={"threads": threads} if threads else {})
    table = con.execute(
        f"""
        WITH per_ip AS (
            SELECT '{RT_MODE}' AS MODE, IP_ADDR,
                   list_aggregate(PROBING_INTERVALS, 'median') / 1000.0 AS INTERVAL_MS
            FROM read_parquet($rt)
            UNION ALL
            SELECT '{FIXED_MODE}' AS MODE, IP_ADDR,
                   list_aggregate(PROBING_INTERVALS, 'median') / 1000.0 AS INTERVAL_MS
            FROM read_parquet($fixed)
        ),
        located AS (
            SELECT g.CONTINENT_CODE, g.CONTINENT, p.MODE, p.INTERVAL_MS
            FROM per_ip p JOIN read_parquet($geo) g USING (IP_ADDR)
            WHERE g.CONTINENT_CODE <> 'ZZ' AND p.INTERVAL_MS IS NOT NULL AND p.INTERVAL_MS >= 0
        ),
        union_counts AS (
            SELECT CONTINENT_CODE, count(*)::BIGINT AS IP_COUNT_UNION
            FROM read_parquet($geo) WHERE CONTINENT_CODE <> 'ZZ' GROUP BY 1
        )
        SELECT l.CONTINENT_CODE, any_value(l.CONTINENT) AS CONTINENT, l.MODE,
               count(*)::BIGINT AS IP_COUNT_MODE, u.IP_COUNT_UNION,
               approx_quantile(l.INTERVAL_MS, 0.995)::DOUBLE AS P99_5_MS,
               approx_quantile(l.INTERVAL_MS, {probabilities_sql})::DOUBLE[] AS QUANTILES_MS
        FROM located l JOIN union_counts u USING (CONTINENT_CODE)
        GROUP BY l.CONTINENT_CODE, l.MODE, u.IP_COUNT_UNION
        ORDER BY l.CONTINENT_CODE, l.MODE
        """,
        {"rt": str(rt_path), "fixed": str(fixed_path), "geo": str(continent_map_path)},
    ).to_arrow_table()
    con.close()
    table = table.cast(INTERVAL_SCHEMA)
    _write_table(table, output_path, compression)
    return {
        "located_ip_count_union": sum(
            {row["CONTINENT_CODE"]: row["IP_COUNT_UNION"] for row in table.to_pylist()}.values()
        ),
        "groups": table.num_rows,
        "quantile_points": quantile_points,
    }


def _abbreviate_count(value: int) -> str:
    if value >= 1_000_000:
        result = f"{value / 1_000_000:.1f}".rstrip("0").rstrip(".")
        return f"{result}M"
    if value >= 1_000:
        decimals = 1 if value < 100_000 else 0
        result = f"{value / 1_000:.{decimals}f}".rstrip("0").rstrip(".")
        return f"{result}k"
    return str(value)


def plot_probing_intervals_by_continent(aggregate_path: Path, output_path: Path) -> Path:
    """Render the split-violin probing-interval figure."""
    rows = pq.read_table(aggregate_path).to_pylist()
    if not rows:
        raise ValueError(f"{aggregate_path}: no located probing-interval groups")
    by_key = {(row["CONTINENT_CODE"], row["MODE"]): row for row in rows}
    present = {row["CONTINENT_CODE"] for row in rows}
    codes = [code for code in CONTINENT_ORDER if code in present]
    codes.extend(sorted(present - set(codes)))

    configure_paper_style()
    fig, ax = plt.subplots(figsize=(7.16, 2.65))
    positions = np.arange(len(codes), dtype=float)
    for position, code in zip(positions, codes):
        for mode, side in ((RT_MODE, "left"), (FIXED_MODE, "right")):
            row = by_key.get((code, mode))
            if not row or not row["QUANTILES_MS"]:
                continue
            parts = ax.violinplot(
                [row["QUANTILES_MS"]],
                positions=[position],
                widths=0.82,
                showmeans=False,
                showmedians=False,
                showextrema=False,
            )
            for body in parts["bodies"]:
                vertices = body.get_paths()[0].vertices
                if side == "left":
                    vertices[:, 0] = np.minimum(vertices[:, 0], position)
                else:
                    vertices[:, 0] = np.maximum(vertices[:, 0], position)
                body.set_facecolor(MODE_COLORS[mode])
                body.set_edgecolor(MODE_COLORS[mode])
                body.set_linewidth(0.35)
                body.set_alpha(0.92)

    labels = []
    for code in codes:
        candidates = [row for row in rows if row["CONTINENT_CODE"] == code]
        count = int(candidates[0]["IP_COUNT_UNION"])
        name = candidates[0]["CONTINENT"] or CONTINENT_NAMES.get(code, code)
        labels.append(f"{name}\n({_abbreviate_count(count)})")
    ax.set_xticks(positions, labels)
    ax.set_xlabel("Continent (#IP Addr.)")
    ax.set_ylabel("Probing Interval [ms]")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", which="major", color="#BDBDBD", linestyle="--", linewidth=0.5, alpha=0.7)
    ax.grid(axis="y", which="minor", color="#D9D9D9", linestyle=":", linewidth=0.35, alpha=0.7)
    ax.minorticks_on()
    ax.legend(
        handles=[
            Patch(facecolor=MODE_COLORS[RT_MODE], edgecolor="#666666", label=RT_MODE),
            Patch(facecolor=MODE_COLORS[FIXED_MODE], edgecolor="#666666", label=FIXED_MODE),
        ],
        loc="upper left",
        frameon=True,
        borderpad=0.4,
    )
    fig.subplots_adjust(left=0.095, right=0.995, bottom=0.25, top=0.98)
    return _save_figure(
        fig,
        output_path,
        title="Probing intervals by continent",
        subject="Split violins comparing RT-based and fixed-interval probing",
    )


def aggregate_increment_distributions(
    rt_path: Path,
    fixed_path: Path,
    output_path: Path,
    *,
    percentile: float = 0.999,
    compression: str | None = "zstd",
    threads: int = 0,
) -> dict:
    """Build exact increment histograms, clipped and renormalized at p99.9."""
    _require_files(rt_path, fixed_path)
    allowed = ",".join(f"'{strategy}'" for strategy in INCREMENT_STRATEGIES)
    con = duckdb.connect(config={"threads": threads} if threads else {})
    rows = con.execute(
        f"""
        SELECT MODE, STRATEGY, INCREMENT, count(*)::BIGINT AS N
        FROM (
            SELECT '{RT_MODE}' AS MODE,
                   CAST(IPID_SELECTION_STRATEGY AS VARCHAR) AS STRATEGY,
                   unnest(INCREMENTS)::INTEGER AS INCREMENT
            FROM read_parquet($rt)
            UNION ALL
            SELECT '{FIXED_MODE}' AS MODE,
                   CAST(IPID_SELECTION_STRATEGY AS VARCHAR) AS STRATEGY,
                   unnest(INCREMENTS)::INTEGER AS INCREMENT
            FROM read_parquet($fixed)
        )
        WHERE STRATEGY IN ({allowed}) AND INCREMENT > 0
        GROUP BY MODE, STRATEGY, INCREMENT
        ORDER BY MODE, STRATEGY, INCREMENT
        """,
        {"rt": str(rt_path), "fixed": str(fixed_path)},
    ).fetchall()
    con.close()

    grouped: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
    for mode, strategy, increment, count in rows:
        grouped[(mode, strategy)].append((int(increment), int(count)))

    output_rows = []
    summaries = {}
    for mode in MODES:
        for strategy in INCREMENT_STRATEGIES:
            histogram = grouped.get((mode, strategy), [])
            if not histogram:
                continue
            total = sum(count for _, count in histogram)
            threshold = percentile * total
            cumulative = 0
            cutoff_index = len(histogram) - 1
            for index, (_, count) in enumerate(histogram):
                cumulative += count
                if cumulative >= threshold:
                    cutoff_index = index
                    break
            retained = sum(count for _, count in histogram[: cutoff_index + 1])
            clipped = total - retained
            cumulative = 0
            for increment, count in histogram[: cutoff_index + 1]:
                cumulative += count
                output_rows.append(
                    {
                        "MODE": mode,
                        "IPID_SELECTION_STRATEGY": strategy,
                        "INCREMENT": increment,
                        "COUNT": count,
                        "CUMULATIVE_COUNT": cumulative,
                        "CUMULATIVE_PERCENTAGE": 100.0 * cumulative / retained,
                        "TOTAL_COUNT": total,
                        "RETAINED_COUNT": retained,
                        "CLIPPED_COUNT": clipped,
                    }
                )
            summaries[f"{mode}:{strategy}"] = {
                "total_count": total,
                "retained_count": retained,
                "clipped_count": clipped,
                "cutoff_increment": histogram[cutoff_index][0],
            }
    table = pa.Table.from_pylist(output_rows, schema=INCREMENT_SCHEMA)
    _write_table(table, output_path, compression)
    return {"percentile": percentile, "series": summaries}


def plot_increment_distributions(aggregate_path: Path, output_path: Path) -> Path:
    """Render paired RT/FI empirical CDFs for the four positional strategies."""
    rows = pq.read_table(aggregate_path).to_pylist()
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        grouped[(row["MODE"], row["IPID_SELECTION_STRATEGY"])].append(row)

    configure_paper_style()
    fig, ax = plt.subplots(figsize=(7.16, 2.65))
    for strategy in INCREMENT_STRATEGIES:
        for mode in MODES:
            series = grouped.get((mode, strategy), [])
            if not series:
                continue
            ax.step(
                [row["INCREMENT"] for row in series],
                [row["CUMULATIVE_PERCENTAGE"] for row in series],
                where="post",
                color=STRATEGY_COLORS[strategy],
                linestyle=MODE_LINESTYLES[mode],
                linewidth=1.45,
            )
    ax.set_xscale("log")
    ax.set_xlabel("IP-ID Increment")
    ax.set_ylabel("Cumulative Percentage [%]")
    ax.set_ylim(0, 103)
    ax.grid(which="major", color="#BDBDBD", linestyle="--", linewidth=0.5, alpha=0.7)
    ax.grid(which="minor", color="#D9D9D9", linestyle=":", linewidth=0.35, alpha=0.75)
    strategy_handles = [
        Line2D(
            [0],
            [0],
            color=STRATEGY_COLORS[strategy],
            linewidth=1.7,
            label=STRATEGY_PRETTY[strategy],
        )
        for strategy in INCREMENT_STRATEGIES
    ]
    mode_handles = [
        Line2D(
            [0],
            [0],
            color="#777777",
            linewidth=1.5,
            linestyle=MODE_LINESTYLES[mode],
            label=mode,
        )
        for mode in MODES
    ]
    ax.legend(
        handles=strategy_handles + mode_handles,
        ncol=3,
        loc="lower center",
        bbox_to_anchor=(0.5, 1.015),
        frameon=False,
        columnspacing=1.4,
        handlelength=2.7,
    )
    fig.subplots_adjust(left=0.095, right=0.995, bottom=0.22, top=0.70)
    return _save_figure(
        fig,
        output_path,
        title="IP-ID increment distributions",
        subject="Empirical CDFs comparing RT-based and fixed-interval probing",
    )


def aggregate_strategy_intersection(
    rt_path: Path,
    fixed_path: Path,
    output_path: Path,
    *,
    compression: str | None = "zstd",
    threads: int = 0,
) -> dict:
    """Count and row-normalize strategies for IPs present in both measurements."""
    _require_files(rt_path, fixed_path)
    allowed = ",".join(f"'{strategy}'" for strategy in INTERSECTION_STRATEGIES)
    con = duckdb.connect(config={"threads": threads} if threads else {})
    total_intersection = int(
        con.execute(
            "SELECT count(*) FROM read_parquet($rt) r INNER JOIN read_parquet($fixed) f "
            "USING (IP_ADDR)",
            {"rt": str(rt_path), "fixed": str(fixed_path)},
        ).fetchone()[0]
    )
    rows = con.execute(
        f"""
        SELECT CAST(r.IPID_SELECTION_STRATEGY AS VARCHAR) AS RT_STRATEGY,
               CAST(f.IPID_SELECTION_STRATEGY AS VARCHAR) AS FIXED_STRATEGY,
               count(*)::BIGINT AS N
        FROM read_parquet($rt) r INNER JOIN read_parquet($fixed) f USING (IP_ADDR)
        WHERE CAST(r.IPID_SELECTION_STRATEGY AS VARCHAR) IN ({allowed})
          AND CAST(f.IPID_SELECTION_STRATEGY AS VARCHAR) IN ({allowed})
        GROUP BY 1, 2
        """,
        {"rt": str(rt_path), "fixed": str(fixed_path)},
    ).fetchall()
    con.close()

    counts = {(rt, fixed): int(count) for rt, fixed, count in rows}
    row_totals = {
        rt: sum(counts.get((rt, fixed), 0) for fixed in INTERSECTION_STRATEGIES)
        for rt in INTERSECTION_STRATEGIES
    }
    output_rows = []
    for rt in INTERSECTION_STRATEGIES:
        for fixed in INTERSECTION_STRATEGIES:
            count = counts.get((rt, fixed), 0)
            total = row_totals[rt]
            output_rows.append(
                {
                    "RT_BASED_STRATEGY": rt,
                    "FIXED_INTERVAL_STRATEGY": fixed,
                    "COUNT": count,
                    "RT_BASED_TOTAL": total,
                    "PERCENTAGE": 100.0 * count / total if total else 0.0,
                }
            )
    table = pa.Table.from_pylist(output_rows, schema=INTERSECTION_SCHEMA)
    _write_table(table, output_path, compression)
    included = sum(row_totals.values())
    return {
        "intersection_ip_count": total_intersection,
        "included_ip_count": included,
        "excluded_strategy_ip_count": total_intersection - included,
        "normalization": "row-wise by RT-based strategy",
    }


def plot_strategy_intersection(aggregate_path: Path, output_path: Path) -> Path:
    """Render the row-normalized strategy intersection heatmap."""
    rows = pq.read_table(aggregate_path).to_pylist()
    values = {
        (row["RT_BASED_STRATEGY"], row["FIXED_INTERVAL_STRATEGY"]): row["PERCENTAGE"]
        for row in rows
    }
    matrix = np.asarray(
        [
            [values.get((rt, fixed), 0.0) for fixed in INTERSECTION_STRATEGIES]
            for rt in INTERSECTION_STRATEGIES
        ],
        dtype=float,
    )

    configure_paper_style()
    fig, ax = plt.subplots(figsize=(7.16, 3.15))
    image = ax.imshow(matrix, cmap="Blues", vmin=0, vmax=100, aspect="auto")
    labels = [STRATEGY_PRETTY[strategy] for strategy in INTERSECTION_STRATEGIES]
    ax.set_xticks(np.arange(len(labels)), labels, rotation=34, ha="right", rotation_mode="anchor")
    ax.set_yticks(np.arange(len(labels)), labels)
    ax.set_xlabel("Detected IP-ID Selection Strategy using Fixed-Interval probing")
    ax.set_ylabel("Detected IP-ID Selection\nStrategy using RT-based\nprobing")
    for row_index in range(matrix.shape[0]):
        for column_index in range(matrix.shape[1]):
            value = matrix[row_index, column_index]
            text = "–" if value == 0 else f"{value:.1f}"
            ax.text(
                column_index,
                row_index,
                text,
                ha="center",
                va="center",
                color="white" if value >= 50 else "#222222",
                fontsize=7,
            )
    colorbar = fig.colorbar(image, ax=ax, pad=0.04, fraction=0.045, ticks=np.arange(0, 101, 20))
    colorbar.set_label("Percentage [%]")
    fig.subplots_adjust(left=0.25, right=0.88, bottom=0.31, top=0.98)
    return _save_figure(
        fig,
        output_path,
        title="IP-ID strategy intersection",
        subject="Row-normalized comparison of RT-based and fixed-interval classifications",
    )


def render_probing_interval_comparison(
    comparison: BaseComparison,
    *,
    maxmind_database: Path | None = None,
    continent_lookup: Callable[[str], tuple[str, str] | None] | None = None,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    raw_root: Path = RAW_DATA_DIR,
    compression: str | None = "zstd",
    threads: int = 0,
) -> tuple[Path, Path, Path]:
    """Create probing-interval aggregate PQ, compact PDF, and metadata JSON."""
    rt_path = comparison.rt_based.artifact_path(processed_root, "probing-intervals")
    fixed_path = comparison.fixed_interval.artifact_path(processed_root, "probing-intervals")
    map_path = comparison.artifact_path(processed_root, CONTINENT_MAP_KIND)
    aggregate_path = comparison.artifact_path(processed_root, PROBING_INTERVAL_KIND)
    pdf_path = comparison.artifact_path(figures_root, PROBING_INTERVAL_KIND, "pdf")
    json_path = comparison.artifact_path(figures_root, PROBING_INTERVAL_KIND, "json")

    if continent_lookup is not None:
        map_stats = build_continent_map(
            (rt_path, fixed_path),
            map_path,
            continent_lookup,
            compression=compression,
            threads=threads,
        )
        database_label = "custom lookup"
    else:
        database = maxmind_database or default_maxmind_database()
        if database is None or not database.is_file():
            raise FileNotFoundError(maxmind_database or REFERENCES_DIR / "GeoLite2-Country.mmdb")
        with MaxMindContinentLookup(database) as lookup:
            map_stats = build_continent_map(
                (rt_path, fixed_path),
                map_path,
                lookup,
                compression=compression,
                threads=threads,
            )
        database_label = str(database)

    aggregate_stats = aggregate_probing_intervals(
        rt_path,
        fixed_path,
        map_path,
        aggregate_path,
        compression=compression,
        threads=threads,
    )
    plot_probing_intervals_by_continent(aggregate_path, pdf_path)
    metadata = {
        **_common_metadata(
            comparison,
            {"rt_based": rt_path, "fixed_interval": fixed_path, "continent_map": map_path},
        ),
        "figure": PROBING_INTERVAL_KIND,
        "aggregate": str(aggregate_path),
        "maxmind_database": database_label,
        "ipid_measurement_coverage": _comparison_coverage(
            comparison,
            processed_root=processed_root,
            raw_root=raw_root,
        ),
        "methodology": {
            "per_ip_statistic": "median of consecutive probing intervals",
            "input_unit": "microseconds",
            "figure_unit": "milliseconds",
            "grouping": "MaxMind continent",
            "display_limit": "99.5th percentile separately per continent and probing mode",
            "ip_weighting": "each located IP address contributes one value per probing mode",
            "unmapped_ips": "excluded from the figure and reported in metadata",
        },
        **map_stats,
        **aggregate_stats,
    }
    _write_json(json_path, metadata)
    return pdf_path, json_path, aggregate_path


def render_increment_comparison(
    comparison: BaseComparison,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    raw_root: Path = RAW_DATA_DIR,
    compression: str | None = "zstd",
    threads: int = 0,
) -> tuple[Path, Path, Path]:
    """Create increment-distribution aggregate PQ, PDF, and metadata JSON."""
    rt_path = comparison.rt_based.artifact_path(processed_root, "increments")
    fixed_path = comparison.fixed_interval.artifact_path(processed_root, "increments")
    aggregate_path = comparison.artifact_path(processed_root, INCREMENT_KIND)
    pdf_path = comparison.artifact_path(figures_root, INCREMENT_KIND, "pdf")
    json_path = comparison.artifact_path(figures_root, INCREMENT_KIND, "json")
    stats = aggregate_increment_distributions(
        rt_path,
        fixed_path,
        aggregate_path,
        compression=compression,
        threads=threads,
    )
    plot_increment_distributions(aggregate_path, pdf_path)
    _write_json(
        json_path,
        {
            **_common_metadata(comparison, {"rt_based": rt_path, "fixed_interval": fixed_path}),
            "figure": INCREMENT_KIND,
            "aggregate": str(aggregate_path),
            "ipid_measurement_coverage": _comparison_coverage(
                comparison,
                processed_root=processed_root,
                raw_root=raw_root,
            ),
            "strategies": list(INCREMENT_STRATEGIES),
            "methodology": {
                "distribution": "empirical CDF of positive IP-ID increments",
                "display_limit": "99.9th percentile separately per strategy and probing mode",
                "normalization": "retained samples are renormalized to 100%",
                "x_scale": "logarithmic",
            },
            **stats,
        },
    )
    return pdf_path, json_path, aggregate_path


def render_strategy_intersection(
    comparison: BaseComparison,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
    raw_root: Path = RAW_DATA_DIR,
    compression: str | None = "zstd",
    threads: int = 0,
) -> tuple[Path, Path, Path]:
    """Create strategy-intersection aggregate PQ, PDF, and metadata JSON."""
    rt_path = comparison.rt_based.artifact_path(processed_root, "strategies")
    fixed_path = comparison.fixed_interval.artifact_path(processed_root, "strategies")
    aggregate_path = comparison.artifact_path(processed_root, INTERSECTION_KIND)
    pdf_path = comparison.artifact_path(figures_root, INTERSECTION_KIND, "pdf")
    json_path = comparison.artifact_path(figures_root, INTERSECTION_KIND, "json")
    stats = aggregate_strategy_intersection(
        rt_path,
        fixed_path,
        aggregate_path,
        compression=compression,
        threads=threads,
    )
    plot_strategy_intersection(aggregate_path, pdf_path)
    _write_json(
        json_path,
        {
            **_common_metadata(comparison, {"rt_based": rt_path, "fixed_interval": fixed_path}),
            "figure": INTERSECTION_KIND,
            "aggregate": str(aggregate_path),
            "ipid_measurement_coverage": _comparison_coverage(
                comparison,
                processed_root=processed_root,
                raw_root=raw_root,
            ),
            "strategies": list(INTERSECTION_STRATEGIES),
            "methodology": {
                "population": "inner intersection of IP addresses in both strategy files",
                "normalization": "each RT-based strategy row sums to 100% when non-empty",
                "excluded_strategies": ["MULTI", "RANDOM", "NOT_ENOUGH_SAMPLES"],
            },
            **stats,
        },
    )
    return pdf_path, json_path, aggregate_path


@app.command()
def main(
    rt_target: str = typer.Argument(..., help="RT-based base measurement target"),
    fixed_target: str = typer.Argument(..., help="fixed-interval base measurement target"),
    manifest: Path = typer.Option(DEFAULT_MANIFEST, help="measurement manifest JSON"),
    maxmind_db: Path | None = typer.Option(
        None,
        help="GeoLite2/GeoIP2 Country or City .mmdb; also read from IPID_MAXMIND_DB",
    ),
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
    threads: int = typer.Option(0, min=0, help="DuckDB threads; 0 uses all cores"),
) -> None:
    try:
        comparison = resolve_base_comparison(load_manifest(manifest), rt_target, fixed_target)
        comp = None if compression == "none" else compression
        increment_outputs = render_increment_comparison(
            comparison, compression=comp, threads=threads
        )
        intersection_outputs = render_strategy_intersection(
            comparison, compression=comp, threads=threads
        )
        logger.success(f"[{comparison.target}] increment figure -> {increment_outputs[0]}")
        logger.success(f"[{comparison.target}] intersection figure -> {intersection_outputs[0]}")
        try:
            interval_outputs = render_probing_interval_comparison(
                comparison,
                maxmind_database=maxmind_db,
                compression=comp,
                threads=threads,
            )
        except FileNotFoundError as exc:
            logger.warning(
                f"[{comparison.target}] MaxMind/input missing ({exc}); "
                "probing-interval continent figure skipped"
            )
        else:
            logger.success(f"[{comparison.target}] continent figure -> {interval_outputs[0]}")
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    app()
