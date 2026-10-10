"""Synthetic benchmark for population-scale matched-only role joins.

Example on the analysis VM::

    python benchmarks/network_role_join.py --rows 300000000 --match-stride 1000
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

import duckdb


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--match-stride", type=int, default=1_000)
    parser.add_argument("--output", type=Path, default=Path("data/benchmarks/network-roles"))
    parser.add_argument("--threads", type=int, default=0)
    arguments = parser.parse_args()
    if arguments.rows < 1 or arguments.match_stride < 1:
        parser.error("--rows and --match-stride must be positive")
    arguments.output.mkdir(parents=True, exist_ok=True)
    strategies = arguments.output / "strategies.pq"
    roles = arguments.output / "roles.pq"
    matches = arguments.output / "matches.pq"
    con = duckdb.connect(config={"threads": arguments.threads} if arguments.threads else {})
    ip = "printf('%d.%d.%d.%d', (i >> 24) & 255, (i >> 16) & 255, (i >> 8) & 255, i & 255)"
    started = time.perf_counter()
    con.execute(
        f"""
        COPY (
            SELECT {ip} AS IP_ADDR,
                   CASE hash(i) % 4 WHEN 0 THEN 'SINGLE' WHEN 1 THEN 'PER_BUCKET'
                        WHEN 2 THEN 'CONSTANT' ELSE 'RANDOM' END AS IPID_SELECTION_STRATEGY
            FROM range({arguments.rows}) AS t(i)
        ) TO '{_sql_path(strategies)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    generated_strategies_seconds = time.perf_counter() - started
    con.execute(
        f"""
        COPY (
            SELECT {ip} AS IP_ADDR, hash(i) % 2 = 0 AS T, hash(i) % 3 = 0 AS D
            FROM range(0, {arguments.rows}, {arguments.match_stride}) AS t(i)
        ) TO '{_sql_path(roles)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    started = time.perf_counter()
    con.execute(
        f"""
        COPY (
            SELECT s.IP_ADDR, s.IPID_SELECTION_STRATEGY, r.T, r.D
            FROM read_parquet('{_sql_path(strategies)}') AS s
            INNER JOIN read_parquet('{_sql_path(roles)}') AS r USING (IP_ADDR)
        ) TO '{_sql_path(matches)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    matched_join_seconds = time.perf_counter() - started
    started = time.perf_counter()
    distribution = con.execute(
        f"""
        SELECT r.T, s.IPID_SELECTION_STRATEGY, count(*)::BIGINT
        FROM read_parquet('{_sql_path(strategies)}') AS s
        INNER JOIN read_parquet('{_sql_path(roles)}') AS r USING (IP_ADDR)
        GROUP BY r.T, s.IPID_SELECTION_STRATEGY
        """
    ).fetchall()
    aggregate_seconds = time.perf_counter() - started
    matched_rows = con.execute(
        f"SELECT count(*) FROM read_parquet('{_sql_path(matches)}')"
    ).fetchone()[0]
    con.close()
    report = {
        "rows": arguments.rows,
        "match_stride": arguments.match_stride,
        "matched_rows": matched_rows,
        "generated_strategies_seconds": generated_strategies_seconds,
        "matched_join_seconds": matched_join_seconds,
        "direct_aggregate_seconds": aggregate_seconds,
        "strategy_bytes": strategies.stat().st_size,
        "role_bytes": roles.stat().st_size,
        "match_bytes": matches.stat().st_size,
        "distribution": [list(row) for row in distribution],
    }
    report_path = arguments.output / "benchmark.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
