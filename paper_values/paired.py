"""Exact matched-population counts from existing labels; no reclassification or raw joins."""

from contextlib import contextmanager
import json
import os


def connection_config():
    threads = int(os.environ.get("IPID_ANALYSIS_THREADS", min(os.cpu_count() or 1, 8)))
    if threads < 1:
        raise ValueError("IPID_ANALYSIS_THREADS must be positive")
    return {
        "threads": threads,
        "memory_limit": os.environ.get("IPID_ANALYSIS_DUCKDB_MEMORY", "2GB"),
        # No scratch file can fall back to /tmp, a local disk or the S3 mount.
        "temp_directory": "",
        "max_temp_directory_size": "0B",
        "preserve_insertion_order": False,
    }


@contextmanager
def connection():
    import duckdb

    try:
        with duckdb.connect(config=connection_config()) as con:
            yield con
    except duckdb.OutOfMemoryException as exc:
        raise ValueError(
            "paired calculation exhausted its RAM budget; file spilling is disabled. "
            "Increase IPID_ANALYSIS_DUCKDB_MEMORY or reduce IPID_ANALYSIS_THREADS. "
            f"DuckDB: {exc}"
        ) from exc
    except duckdb.Error as exc:
        raise ValueError(f"cannot read paired strategy populations: {exc}") from exc


def labels(con, name, path, report, classifier_version, *, materialize=False):
    import pyarrow.parquet as pq

    with pq.ParquetFile(path) as parquet:
        metadata = parquet.schema_arrow.metadata or {}
        if metadata.get(b"classifier_version", b"").decode() != classifier_version:
            raise ValueError("paired labels do not match the production classifier version")
        if parquet.metadata.num_rows != report["total_ips"]:
            raise ValueError("paired label population differs from its strategy report")
    data = con.read_parquet(str(path)).project("IP_ADDR, IPID_SELECTION_STRATEGY")
    if materialize:
        data.create(name)
    else:
        data.create_view(name)
    actual = dict(
        con.execute(f"SELECT IPID_SELECTION_STRATEGY, count(*) FROM {name} GROUP BY 1").fetchall()
    )
    expected = {key: n for key, n in report["counts"].items() if n}
    if actual != expected:
        raise ValueError("paired labels disagree with their reported strategy counts")


def unique(con, name):
    n, distinct, present = con.execute(
        f"SELECT count(*), count(DISTINCT IP_ADDR), count(IP_ADDR) FROM {name}"
    ).fetchone()
    if n != distinct or n != present:
        raise ValueError("duplicate or null IP address in paired population")
    return n


def fi_rt(paths, rt, fi, coverage, classifier_version, *, connection_mode):
    rt_path, fi_path, target_path, metadata_path = paths
    meta = json.loads(metadata_path.read_text(encoding="utf-8"))
    if (
        not isinstance(meta, dict)
        or meta.get("version") != 1
        or meta.get("zmap_id") != fi["zmap_id"]
        or meta.get("reply_type", "") != ("synack" if connection_mode else "")
        or type(meta.get("sample_rows")) is not int
        or meta["sample_rows"] < 0
    ):
        raise ValueError("target sample metadata differs from the paired measurements")
    with connection() as con:
        labels(con, "rt_source", rt_path, rt, classifier_version)
        labels(con, "fi_source", fi_path, fi, classifier_version, materialize=True)
        con.read_parquet(str(target_path)).project("IP_ADDR").create_view("target_source")
        con.execute("CREATE TEMP TABLE targets AS SELECT * FROM target_source")
        n = unique(con, "targets")
        if n != meta["sample_rows"] or n != coverage["zmap_ip_count"]:
            raise ValueError("FI target population differs from its persisted sample or coverage")
        if con.execute(
            "SELECT count(*) FROM fi_source ANTI JOIN targets USING (IP_ADDR)"
        ).fetchone()[0]:
            raise ValueError("FI results contain addresses outside the persisted target cohort")
        con.execute(
            "CREATE TEMP TABLE rt AS SELECT * FROM rt_source SEMI JOIN targets USING (IP_ADDR)"
        )
        unique(con, "rt")
        unique(con, "fi_source")
        r = con.execute(
            "SELECT count(*) FROM rt WHERE IPID_SELECTION_STRATEGY != 'NOT_ENOUGH_SAMPLES'"
        ).fetchone()[0]
        f = con.execute(
            "SELECT count(*) FROM fi_source WHERE IPID_SELECTION_STRATEGY != 'NOT_ENOUGH_SAMPLES'"
        ).fetchone()[0]
    # UNCLASSIFIED still denotes sufficient replies. This is a ratio, not an intersection rate.
    return {
        "target_count": n,
        "rt_successful_count": r,
        "fi_successful_count": f,
        "pct": 100.0 * f / r if r else None,
    }


def tcp_transition(paths, before, after, classifier_version):
    with connection() as con:
        labels(con, "before_source", paths[0], before, classifier_version)
        labels(con, "after_source", paths[1], after, classifier_version, materialize=True)
        con.execute(
            "CREATE TEMP TABLE after_valid AS SELECT * FROM after_source "
            "WHERE IPID_SELECTION_STRATEGY != 'NOT_ENOUGH_SAMPLES'"
        )
        con.execute(
            "CREATE TEMP TABLE before_cohort AS SELECT * FROM before_source "
            "SEMI JOIN after_valid USING (IP_ADDR)"
        )
        unique(con, "after_source")
        unique(con, "before_cohort")
        numerator, denominator = con.execute(
            "SELECT count(*) FILTER (WHERE b.IPID_SELECTION_STRATEGY = 'CONSTANT'), count(*) "
            "FROM before_cohort b JOIN after_valid a USING (IP_ADDR) "
            "WHERE a.IPID_SELECTION_STRATEGY = 'PER_CONNECTION' "
            "AND b.IPID_SELECTION_STRATEGY != 'NOT_ENOUGH_SAMPLES'"
        ).fetchone()
    return {
        "constant_to_per_connection_count": numerator,
        "per_connection_column_count": denominator,
        "pct": 100.0 * numerator / denominator if denominator else None,
    }
