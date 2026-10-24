"""Alternative labels, streamed from observations with bounded summaries."""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.artifacts import atomic_output, write_text
from ipid_analysis.interprotocol_stability import STABILITY_SCHEMA, STABILITY_STRATEGIES
from ipid_analysis.runtime import connect


def write_alternative_reports(stability, deployments, output_dir: Path, measurements: dict):
    """Accept existing Arrow callers or parquet paths without materializing IPs."""
    schema = STABILITY_SCHEMA.append(pa.field("DEPLOYMENT", pa.string())).append(
        pa.field("TRIGGERS", pa.list_(pa.string()))
    )
    with connect() as con:
        for name, source in (("observations", stability), ("deployments", deployments)):
            if isinstance(source, (str, Path)):
                escaped = str(source).replace("'", "''")
                con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{escaped}')")
            else:
                con.register(name, source)
        con.execute("""
            CREATE TEMP TABLE alternatives AS
            WITH changed AS (
                SELECT DISTINCT IP_ADDR, PROTOCOL_GROUP, BASELINE_STRATEGY
                FROM observations WHERE STRATEGY_STATUS = 'CHANGED'
            ), joined AS (
                SELECT s.*, d.DEPLOYMENT,
                    list_filter([
                        CASE WHEN c.IP_ADDR IS NOT NULL THEN 'CHANGED' END,
                        CASE WHEN d.DEPLOYMENT IN ('STRATEGY_NOT_CONFIRMED', 'AMBIGUOUS')
                             THEN 'STRATEGY_NOT_CONFIRMED' END
                    ], value -> value IS NOT NULL) AS TRIGGERS
                FROM observations s
                LEFT JOIN deployments d ON s.IP_ADDR = d.IP_ADDR
                    AND s.PROTOCOL_GROUP = d.PROTOCOL_GROUP
                    AND s.BASELINE_STRATEGY = d.IPID_SELECTION_STRATEGY
                LEFT JOIN changed c ON s.IP_ADDR = c.IP_ADDR
                    AND s.PROTOCOL_GROUP = c.PROTOCOL_GROUP
                    AND s.BASELINE_STRATEGY = c.BASELINE_STRATEGY
            ) SELECT * FROM joined WHERE len(TRIGGERS) > 0
        """)
        for strategy in STABILITY_STRATEGIES:
            stem = "interprotocol-campaign-" + strategy.lower().replace("_", "-") + "-alternatives"
            path = output_dir / f"{stem}.pq"
            with (
                atomic_output(path) as temporary,
                pq.ParquetWriter(temporary, schema, compression="zstd") as writer,
            ):
                reader = con.execute(
                    "SELECT * FROM alternatives WHERE BASELINE_STRATEGY = ?", [strategy]
                ).to_arrow_reader(8192)
                for batch in reader:
                    writer.write_batch(batch.cast(schema))
            groups = {}
            for group, measurement_id in measurements.items():
                params = [strategy, group]
                affected, observations, changed, unconfirmed = con.execute(
                    """
                    SELECT count(DISTINCT IP_ADDR), count(*),
                        count(DISTINCT IP_ADDR) FILTER (WHERE list_contains(TRIGGERS, 'CHANGED')),
                        count(DISTINCT IP_ADDR) FILTER (WHERE list_contains(TRIGGERS, 'STRATEGY_NOT_CONFIRMED'))
                    FROM alternatives WHERE BASELINE_STRATEGY = ? AND PROTOCOL_GROUP = ?
                """,
                    params,
                ).fetchone()
                rows = con.execute(
                    """
                    SELECT PROTOCOL, OBSERVED_STRATEGY, STRATEGY_STATUS, REASON, DEPLOYMENT, count(*)
                    FROM alternatives WHERE BASELINE_STRATEGY = ? AND PROTOCOL_GROUP = ?
                    GROUP BY 1, 2, 3, 4, 5
                """,
                    params,
                ).fetchall()
                groups[group] = {
                    "measurement_id": measurement_id,
                    "affected_ips": affected,
                    "protocol_observations": observations,
                    "trigger_ip_counts": {
                        "CHANGED": changed,
                        "STRATEGY_NOT_CONFIRMED": unconfirmed,
                    },
                    "alternatives": [
                        dict(
                            zip(
                                (
                                    "protocol",
                                    "observed_strategy",
                                    "status",
                                    "reason",
                                    "deployment",
                                    "count",
                                ),
                                row,
                                strict=True,
                            )
                        )
                        for row in sorted(rows, key=lambda row: str(row[:-1]))
                    ],
                }
            report = {
                "version": 1,
                "baseline_strategy": strategy,
                "unit": "IP/protocol observations; affected_ips counts unique IPs within each group",
                "selection": "All protocols of targets with CHANGED or unconfirmed deployment (including AMBIGUOUS)",
                "interpretation": "Unconfirmed shared-counter deployment can coexist with unchanged per-protocol labels",
                "trigger_counts_overlap": True,
                "classifier_metadata": {
                    k.decode(): v.decode() for k, v in STABILITY_SCHEMA.metadata.items()
                },
                "groups": groups,
            }
            write_text(output_dir / f"{stem}.json", json.dumps(report, indent=2) + "\n")
