"""Cached 10-ms frequencies from per-IP medians, exclusively for Anycast plots."""

import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import paper_probing_plot as probing

BIN_MS = 10
METHOD = {
    "version": 1,
    "representation": "histogram",
    "bin_width_ms": BIN_MS,
    "bin_origin_ms": 0,
    "bin_closure": "left-closed, right-open",
    "denominator": "all finite nonnegative per-IP interval medians in the continent/mode, including the hidden tail",
    "display_limit": "entire bin containing the separately computed p99.9",
    "normalization": "each half independently: widest displayed bin has width 0.41",
    "smoothing": "none",
    "stored_bins": "all nonempty bins, including the hidden tail; empty bins are implicit zeros",
}
MAX_BYTES = 16 << 20


def schema():
    from ipid_analysis.paper_figures import PAPER_INTERVAL_SCHEMA

    return (
        PAPER_INTERVAL_SCHEMA.append(
            pa.field(
                "BINS",
                pa.list_(
                    pa.struct(
                        [
                            ("LOWER_MS", pa.int64()),
                            ("UPPER_MS", pa.int64()),
                            ("IP_COUNT", pa.int64()),
                            ("PERCENT", pa.float64()),
                        ]
                    )
                ),
            )
        )
        .append(pa.field("DISPLAY_COUNT", pa.int64()))
        .append(pa.field("TAIL_COUNT", pa.int64()))
    )


def paths(source, report, processed_root=None, *, anycast):
    aggregate, _ = probing.paths(source, report, processed_root, anycast=anycast)
    digest = hashlib.sha256(
        (inspect.getsource(aggregate_histogram) + json.dumps(METHOD, sort_keys=True)).encode()
    ).hexdigest()[:12]
    aggregate = aggregate.parent / ("histogram-10ms-v1-" + digest) / aggregate.name
    return aggregate, aggregate.with_suffix(".json")


def aggregate_histogram(sources, prefixes, rows, output):
    """Count actual observations; quantile summaries supply only cutoffs and checks."""
    from ipid_analysis.anycast_probing import filtered_continent_map
    from ipid_analysis.paper_figures import MODES
    from ipid_analysis.runtime import connect

    groups = pa.table(
        {
            "CONTINENT_CODE": [r["CONTINENT_CODE"] for r in rows],
            "MODE": [r["MODE"] for r in rows],
        }
    )
    with connect() as con:
        geo, statistics = filtered_continent_map(con, sources["continent_map"], prefixes)
        con.register("histogram_groups", groups)
        records = con.execute(
            f"""
            WITH per_ip AS (
                SELECT '{MODES[0]}' AS MODE, IP_ADDR,
                    list_aggregate(PROBING_INTERVALS, 'median') / 1000.0 AS INTERVAL_MS
                FROM read_parquet($rt)
                UNION ALL
                SELECT '{MODES[1]}' AS MODE, IP_ADDR,
                    list_aggregate(PROBING_INTERVALS, 'median') / 1000.0 AS INTERVAL_MS
                FROM read_parquet($fi)
            ), bins AS (
                SELECT g.CONTINENT_CODE, p.MODE,
                    floor(p.INTERVAL_MS / {BIN_MS})::BIGINT AS BIN_INDEX,
                    count(*)::BIGINT AS N
                FROM per_ip p JOIN {geo} g USING (IP_ADDR)
                JOIN histogram_groups h ON h.CONTINENT_CODE=g.CONTINENT_CODE AND h.MODE=p.MODE
                WHERE isfinite(p.INTERVAL_MS) AND p.INTERVAL_MS >= 0
                GROUP BY 1,2,3
            )
            SELECT CONTINENT_CODE, MODE, sum(N)::BIGINT AS TOTAL,
                list(struct_pack(BIN_INDEX := BIN_INDEX, N := N) ORDER BY BIN_INDEX) AS BINS
            FROM bins GROUP BY 1,2
        """,
            {"rt": str(sources["rt_based"]), "fi": str(sources["fixed_interval"])},
        ).fetchall()
    grouped = {(code, mode): (total, bins) for code, mode, total, bins in records}
    if set(grouped) != {(r["CONTINENT_CODE"], r["MODE"]) for r in rows}:
        raise ValueError("histogram populations differ from prepared quartiles")
    result = []
    for row in rows:
        total, bins = grouped[row["CONTINENT_CODE"], row["MODE"]]
        if total != row["IP_COUNT_MODE"]:
            raise ValueError("histogram denominator differs from the full per-IP population")
        bins = [
            dict(
                LOWER_MS=b["BIN_INDEX"] * BIN_MS,
                UPPER_MS=(b["BIN_INDEX"] + 1) * BIN_MS,
                IP_COUNT=b["N"],
                PERCENT=100 * b["N"] / total,
            )
            for b in bins or []
        ]
        shown = sum(
            b["IP_COUNT"]
            for b in bins
            if b["LOWER_MS"] <= math.floor(row["CUTOFF_MS"] / BIN_MS) * BIN_MS
        )
        result.append({**row, "BINS": bins, "DISPLAY_COUNT": shown, "TAIL_COUNT": total - shown})
    pq.write_table(pa.Table.from_pylist(result, schema=schema()), output)
    return statistics


def prepare(job, *, anycast):
    from ipid_analysis.anycast_census import load_dataset
    from ipid_analysis.artifacts import (
        cache_current,
        completion_path,
        fingerprints,
        publish_new_file,
    )
    from ipid_analysis.runtime import exclusive_lock, scratch_root

    # Existing published quartiles/impact tables are reusable and remain intact.
    probing.prepare(job, anycast=anycast)
    report = json.loads(job.source.read_text())
    legacy, legacy_meta = probing.paths(job.source, report, job.processed_root, anycast=anycast)
    rows = probing.load_series(legacy, legacy_meta, job.source, anycast=anycast)
    _, prefixes = load_dataset(anycast)
    _, mode = probing._context(job.source, report)
    sources = {
        k: probing._remap(Path(v), job.processed_root, mode)
        for k, v in report["sources"].items()
        if k in ("rt_based", "fixed_interval", "continent_map")
    }
    inputs = [job.source, legacy, legacy_meta, *sources.values(), Path(anycast), prefixes]
    aggregate, metadata = paths(job.source, report, job.processed_root, anycast=anycast)
    parameters = {
        "operation": "paper-anycast-histogram-v1",
        "histogram": METHOD,
        "calculation": aggregate.parent.name,
    }
    with exclusive_lock("anycast-histogram:" + str(aggregate.resolve())):
        if any(
            os.path.lexists(p) or os.path.lexists(completion_path(p))
            for p in (aggregate, metadata)
        ):
            if cache_current(aggregate, inputs, parameters) and cache_current(
                metadata, [aggregate], parameters
            ):
                load_series(
                    aggregate,
                    metadata,
                    job.source,
                    anycast=anycast,
                    processed_root=job.processed_root,
                )
                return "skipped-current"
            raise FileExistsError("existing/incomplete Anycast histogram left untouched")
        before = fingerprints(inputs)
        with tempfile.TemporaryDirectory(
            prefix="anycast-histogram-", dir=scratch_root()
        ) as directory:
            table, meta = Path(directory) / "histogram.pq", Path(directory) / "histogram.json"
            statistics = aggregate_histogram(sources, prefixes, rows, table)
            document = json.loads(legacy_meta.read_text())
            if any(document.get(k) != v for k, v in statistics.items()):
                raise ValueError("histogram filtering differs from prepared Anycast metadata")
            document["histogram"] = METHOD
            meta.write_text(json.dumps(document, indent=2) + "\n")
            load_series(
                table, meta, job.source, anycast=anycast, processed_root=job.processed_root
            )
            if fingerprints(inputs) != before:
                raise ValueError("source changed during histogram preparation")
            publish_new_file(table, aggregate, inputs=inputs, parameters=parameters)
            publish_new_file(meta, metadata, inputs=[aggregate], parameters=parameters)
    return "created"


def load_series(aggregate, metadata, source, *, anycast, processed_root=None):
    """Bounded, query-free read; refuse legacy quantile-only plots and corrupt bins."""
    from ipid_analysis.artifacts import assert_complete

    if not aggregate.is_file() or not metadata.is_file():
        raise ValueError(
            "10-ms Anycast histogram missing; run paper-anycast-plots with --prepare --refresh"
        )
    for p in (aggregate, metadata):
        assert_complete(p)
        if p.stat().st_size > MAX_BYTES:
            raise ValueError("expected a bounded Anycast histogram/metadata")
    report = json.loads(source.read_text())
    legacy, legacy_meta = probing.paths(source, report, processed_root, anycast=anycast)
    original_rows = probing.load_series(legacy, legacy_meta, source, anycast=anycast)
    expected = json.loads(legacy_meta.read_text()) | {"histogram": METHOD}
    if json.loads(metadata.read_text()) != expected:
        raise ValueError("histogram metadata differs from its numerical inputs")
    with pq.ParquetFile(aggregate) as file:
        if not 0 < file.metadata.num_rows <= 14 or not file.schema_arrow.equals(schema()):
            raise ValueError("unexpected Anycast histogram schema or group count")
        rows = file.read().to_pylist()
    if len(rows) != len(original_rows):
        raise ValueError("histogram groups differ from prepared summaries")
    for row, original in zip(rows, original_rows):
        if {k: row[k] for k in original} != original:
            raise ValueError("histogram population/quartiles differ from prepared summaries")
        total, bins = row["IP_COUNT_MODE"], row["BINS"]
        last, shown, all_count = -1, 0, 0
        if not bins or len(bins) > 100_000:
            raise ValueError("empty or excessive histogram bins")
        for b in bins:
            low, high, count, pct = (b[k] for k in ("LOWER_MS", "UPPER_MS", "IP_COUNT", "PERCENT"))
            if (
                low < 0
                or low % BIN_MS
                or high != low + BIN_MS
                or low <= last
                or count <= 0
                or count > total
                or not math.isfinite(pct)
                or not math.isclose(pct, 100 * count / total, rel_tol=1e-12)
            ):
                raise ValueError("invalid histogram bin/count/percentage")
            last, all_count = low, all_count + count
            if low <= math.floor(row["CUTOFF_MS"] / BIN_MS) * BIN_MS:
                shown += count
        if (
            row["DISPLAY_COUNT"] != shown
            or all_count != total
            or row["TAIL_COUNT"] != total - shown
            or not 0 < shown <= total
        ):
            raise ValueError("histogram displayed/tail counts do not conserve the population")
    return rows


def body(ax, row, center, side):
    """Filled step outline with explicit zero-frequency gaps; no KDE/interpolation."""
    from matplotlib.collections import PolyCollection

    bins = displayed_bins(row)
    maximum = max(b["PERCENT"] for b in bins)
    direction = -1 if side else 1
    vertices = [(center, bins[0]["LOWER_MS"])]
    previous = bins[0]["LOWER_MS"]
    for b in bins:
        low, high = b["LOWER_MS"], b["UPPER_MS"]
        if low > previous:
            vertices.extend(((center, previous), (center, low)))
        edge = center + direction * 0.41 * b["PERCENT"] / maximum
        vertices.extend(((edge, low), (edge, high)))
        previous = high
    vertices.append((center, previous))
    collection = PolyCollection([vertices], closed=True)
    ax.add_collection(collection)
    return collection


def quartile_edge(row, center, value):
    bins = displayed_bins(row)
    b = next((b for b in bins if b["LOWER_MS"] <= value < b["UPPER_MS"]), None)
    return center if b is None else center - 0.41 * b["PERCENT"] / max(b["PERCENT"] for b in bins)


def displayed_bins(row):
    limit = math.floor(row["CUTOFF_MS"] / BIN_MS) * BIN_MS
    return [b for b in row["BINS"] if b["LOWER_MS"] <= limit]
