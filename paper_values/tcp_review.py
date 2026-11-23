"""Explicit one-off EU Constant/DF and within-run SYN-ACK preparation.

Normal paper-values only reads the small prepared JSON. No reclassification,
top-N export, or replacement of the historical paired TCP transition.
"""

import argparse
from collections import Counter
import json
from pathlib import Path
import re
from time import perf_counter

from paper_values.cache import fingerprint as input_fingerprint
from paper_values.cache import verify_file
from paper_values.inputs import Inputs, select_artifacts
from paper_values.paired import connection
from paper_values.review import TCP_VERSION, percentage, read_json, record, tcp_defaults


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def prepared_path(processed_root):
    return Path(processed_root) / "eu/paper-review" / (TCP_VERSION + ".json")


def source(root, relative):
    path = Path(root) / relative
    if Path(relative).is_absolute() or not path.resolve().is_relative_to(Path(root).resolve()):
        raise ValueError("TCP review input outside campaign root")
    return path


def plans(artifacts, processed, raw):
    index = {
        (a.connection, a.phase): a
        for a in artifacts
        if a.site == "eu" and a.protocol == "tcp" and a.kind == "strategies"
    }
    result = []
    for mode, phase in (("no-connection", "rt-fi-merged"), ("connection", "rt-base")):
        a = index.get((mode, phase))
        if a is None:
            raise ValueError(f"missing canonical EU TCP {mode}/{phase} report")
        folder = Path(a.relative.removeprefix("figures/")).parent
        labels = source(processed, Path("eu") / folder / (Path(a.relative).stem + ".pq"))
        if mode == "no-connection":
            base = index[mode, "rt-base"]
            base_labels = source(
                processed,
                Path("eu") / Path(base.relative.removeprefix("figures/")).with_suffix(".pq"),
            )
            runs = (
                (a.data["base_measurement_id"], "CONSTANT", False),
                (a.data["mass_measurement_id"], "UNCLASSIFIED", True),
            )
        else:
            base_labels = None
            runs = ((a.data["measurement_id"], None, False),)
        sources = []
        import yaml

        for mid, route, mass in runs:
            if not isinstance(mid, str) or not re.fullmatch(r"tcp-80_[A-Za-z0-9_-]+", mid):
                raise ValueError("unexpected TCP measurement identity")
            raw_path = source(raw, Path("eu/ipid") / mid / "ipid.pq")
            snapshot = raw_path.with_name("ipid.snapshot.yaml")
            cfg = yaml.safe_load(snapshot.read_text())
            c, r = cfg["connection_count"], cfg["requests_per_connection"]
            if type(c) is not int or type(r) is not int or c < 2 or r < 2:
                raise ValueError("invalid TCP measurement layout")
            if mode == "connection" and c != 4:
                raise ValueError(
                    "requested handshake metric requires exactly four SYN-ACK samples"
                )
            if mode == "connection" and cfg.get("tcp", {}).get("establish_connection") is not True:
                raise ValueError(
                    "connection snapshot must confirm actual TCP connection establishment"
                )
            sources.append(
                dict(
                    raw=raw_path,
                    snapshot=snapshot,
                    width=c * r,
                    skip=0 if mass else c,
                    route=route,
                )
            )
        result.append(dict(mode=mode, report=a, labels=labels, base=base_labels, sources=sources))
    return result


def constant_counts(con, item):
    """Project only relevant columns and retain original sample positions."""
    import pyarrow.parquet as pq

    with pq.ParquetFile(item["raw"]) as parquet:
        df = "r.DF_SEQUENCE" if "DF_SEQUENCE" in parquet.schema_arrow.names else "NULL::VARCHAR"
    width, skip = item["width"], item["skip"]
    return con.execute(f"""
        WITH raw AS (
            SELECT r.IP_ADDR AS raw_ip, string_split(r.IPID_SEQUENCE, ',') AS all_ids,
                   string_split({df}, ',') AS all_df
            FROM needed n LEFT JOIN read_parquet({quote(item["raw"])}) r USING (IP_ADDR)
        ), sliced AS (
            SELECT *, list_slice(all_ids, {skip + 1}, {width}) AS ids,
                   list_slice(all_df, {skip + 1}, {width}) AS dfs FROM raw
        ), positions AS (
            SELECT *, list_filter(range(1, len(ids)+1), i -> ids[i] <> '-') AS present
            FROM sliced
        ), samples AS (
            SELECT *, list_transform(present, i -> try_cast(ids[i] AS INTEGER)) AS vals,
                   list_transform(present, i -> dfs[i]) AS flags FROM positions
        )
        SELECT list_min(vals) AS ipid,
            CASE WHEN len(all_df) IS DISTINCT FROM {width}
                   OR list_count(flags) <> len(present)
                   OR len(list_filter(flags, v -> v NOT IN ('0', '1'))) > 0
                 THEN 'UNKNOWN'
                 WHEN list_min(flags) = list_max(flags) THEN 'DF' || list_min(flags)
                 ELSE 'MIXED' END AS status,
            raw_ip IS NULL OR len(all_ids) <> {width} OR coalesce(len(present), 0) = 0
                OR list_count(vals) <> len(present) OR list_min(vals) < 0
                OR list_max(vals) > 65535 OR list_min(vals) <> list_max(vals) AS invalid,
            count(*) FROM samples GROUP BY 1,2,3
    """).fetchall()


def calculate(plan):
    from paper_values.readers import counts

    values, audit = {}, {}
    with connection() as con:
        con.execute(
            f"CREATE TEMP TABLE selected AS SELECT IP_ADDR FROM read_parquet({quote(plan['labels'])}) WHERE CAST(IPID_SELECTION_STRATEGY AS VARCHAR) = 'CONSTANT'"
        )
        n, unique = con.execute(
            "SELECT count(*), count(DISTINCT IP_ADDR) FROM selected"
        ).fetchone()
        if n != unique or n != counts(plan["report"].data)[0].get("CONSTANT", 0):
            raise ValueError("Constant labels differ from report or contain duplicate/null IPs")
        flags, zero, routed = Counter(), 0, 0
        for item in plan["sources"]:
            con.execute("DROP TABLE IF EXISTS needed")
            query = (
                "SELECT * FROM selected"
                if item["route"] is None
                else f"SELECT s.IP_ADDR FROM selected s JOIN read_parquet({quote(plan['base'])}) b USING(IP_ADDR) WHERE CAST(b.IPID_SELECTION_STRATEGY AS VARCHAR) = {quote(item['route'])}"
            )
            con.execute("CREATE TEMP TABLE needed AS " + query)
            needed, unique = con.execute(
                "SELECT count(*), count(DISTINCT IP_ADDR) FROM needed"
            ).fetchone()
            if needed != unique:
                raise ValueError("duplicate base labels in merge routing")
            routed += needed
            if not needed:
                continue
            print(f"TCP review: reading {needed:,} Constant IPs from {item['raw']}", flush=True)
            rows = constant_counts(con, item)
            if sum(row[3] for row in rows) != needed or any(row[2] is not False for row in rows):
                raise ValueError(
                    "missing/duplicate raw rows or nonconstant classifier-used samples"
                )
            for ipid, status, _, count in rows:
                flags[status] += count
                zero += count if ipid == 0 else 0
        if routed != n or sum(flags.values()) != n:
            raise ValueError("incomplete/overlapping merge routing")
        mode, phase = (
            ("no-conn", "rt-fi") if plan["mode"] == "no-connection" else ("conn", "rt-base")
        )
        prefix = f"results.eu.tcp.{mode}.{phase}.constant"
        sources = [plan["labels"], *(x["raw"] for x in plan["sources"])]
        defaults = tcp_defaults()
        for suffix, count in (
            ("df0.pct", flags["DF0"]),
            ("df1.pct", flags["DF1"]),
            ("ipid-zero.pct", zero),
        ):
            key = prefix + "." + suffix
            # Unknown/mixed remain in the denominator; never silently renormalize.
            raw = percentage(count, n)
            if suffix != "ipid-zero.pct" and flags["UNKNOWN"]:
                raw = None  # incomplete telemetry cannot establish population-wide DF shares
            values[key] = record(raw, "percent", defaults[key]["definition"], sources)
        audit.update(constant_ips=n, ipid_zero=zero, df=dict(flags))
        if plan["mode"] == "connection":
            con.execute("DROP TABLE selected")
            con.execute(
                f"CREATE TEMP TABLE selected AS SELECT IP_ADDR FROM read_parquet({quote(plan['labels'])}) WHERE CAST(IPID_SELECTION_STRATEGY AS VARCHAR) = 'PER_CONNECTION'"
            )
            denominator, unique = con.execute(
                "SELECT count(*), count(DISTINCT IP_ADDR) FROM selected"
            ).fetchone()
            if denominator != unique or denominator != counts(plan["report"].data)[0].get(
                "PER_CONNECTION", 0
            ):
                raise ValueError("invalid PER_CONNECTION population")
            raw = plan["sources"][0]["raw"]
            joined, invalid, numerator = con.execute(f"""
                WITH sequences AS (
                    SELECT r.IP_ADDR AS raw_ip, len(string_split(r.IPID_SEQUENCE, ',')) AS sample_count,
                           list_transform(list_slice(string_split(r.IPID_SEQUENCE, ','),1,4), v -> try_cast(v AS INTEGER)) AS ids
                    FROM selected s LEFT JOIN read_parquet({quote(raw)}) r USING(IP_ADDR)
                ) SELECT count(*), count(*) FILTER (WHERE raw_ip IS NULL OR sample_count <> {plan["sources"][0]["width"]} OR len(ids) <> 4
                    OR list_count(ids) <> 4 OR list_min(ids) < 0 OR list_max(ids) > 65535),
                    count(*) FILTER (WHERE list_count(ids)=4 AND list_min(ids)=list_max(ids))
                  FROM sequences
            """).fetchone()
            if joined != denominator or invalid:
                raise ValueError("missing/duplicate/invalid SYN-ACK rows in PER_CONNECTION cohort")
            key = "results.eu.tcp.conn.rt-base.per-connection.handshake.constant.pct"
            values[key] = record(
                percentage(numerator, denominator),
                "percent",
                defaults[key]["definition"],
                (plan["labels"], raw),
            )
            audit["handshake"] = dict(constant=numerator, per_connection=denominator)
    return values, audit


def read_prepared(processed_root, campaign):
    path = prepared_path(processed_root)
    if not path.is_file():
        return {}
    doc = read_json(path)
    if (
        doc.get("version") != TCP_VERSION
        or doc.get("campaign") != campaign
        or set(doc["values"]) != set(tcp_defaults())
    ):
        raise ValueError("unexpected prepared TCP review identity/keys")
    # Stat/receipt checks only; never scan raw sequences during ordinary export.
    for entry in doc["inputs"]:
        if input_fingerprint(entry["path"]) != entry:
            raise ValueError("prepared TCP review is stale; rerun prepare-paper-review")
    result = {}
    for key, value in doc["values"].items():
        raw = value["raw"]
        if raw is not None and (type(raw) not in (int, float) or not 0 <= raw <= 100):
            raise ValueError("invalid prepared TCP percentage")
        result[key] = record(
            raw, "percent", tcp_defaults()[key]["definition"], (path, *value["sources"])
        )
    return result


def prepare(campaign, reports_root, processed_root, raw_root):
    from ipid_analysis.artifacts import publish_file, temporary_path
    from paper_values.exporter import load_metrics, required_contexts

    contexts, plots = required_contexts(load_metrics()["metrics"])
    inputs = Inputs((reports_root, processed_root), contexts=contexts, plot_contexts=plots)
    try:
        artifacts, conflicts = select_artifacts(list(inputs.artifacts(campaign)))
        if inputs.errors or conflicts:
            raise ValueError(f"invalid canonical reports: {inputs.errors + conflicts}")
    finally:
        inputs.close()
    from paper_values.reference_review import prepare as prepare_references

    prepare_references(campaign, processed_root, artifacts)
    work = plans(artifacts, processed_root, raw_root)
    dependencies = sorted(
        set(
            p
            for plan in work
            for p in (
                Path(plan["report"].name),
                plan["labels"],
                *([plan["base"]] if plan["base"] else []),
                *(item[k] for item in plan["sources"] for k in ("raw", "snapshot")),
            )
        )
    )
    for path in dependencies:
        verify_file(path)
    before = [input_fingerprint(p) for p in dependencies]
    destination = prepared_path(processed_root)
    if destination.is_file():
        doc = read_json(destination)
        if (
            doc.get("version") == TCP_VERSION
            and doc.get("campaign") == campaign
            and doc.get("inputs") == before
        ):
            read_prepared(processed_root, campaign)
            print(f"TCP review: cache hit: {destination}", flush=True)
            return destination
    values, audits = {}, {}
    for plan in work:
        found, audit = calculate(plan)
        values.update(found)
        audits[plan["mode"]] = audit
    if [input_fingerprint(p) for p in dependencies] != before:
        raise ValueError("TCP review inputs changed during calculation")
    document = dict(
        version=TCP_VERSION, campaign=campaign, inputs=before, values=values, audit=audits
    )
    temporary = temporary_path(destination)
    try:
        temporary.write_text(json.dumps(document, indent=2, allow_nan=False) + "\n")
        publish_file(
            temporary, destination, inputs=dependencies, parameters={"calculation": TCP_VERSION}
        )
    finally:
        temporary.unlink(missing_ok=True)
    print(f"TCP review: prepared {destination}", flush=True)
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True)
    parser.add_argument(
        "--repository-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    for name in ("reports-root", "processed-root", "raw-root"):
        parser.add_argument("--" + name, type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", args.campaign):
        parser.error("invalid campaign")
    scope = Path("continental") / args.campaign
    started = perf_counter()
    prepare(
        args.campaign,
        args.reports_root or args.repository_root / "reports" / scope,
        args.processed_root or args.repository_root / "data/processed" / scope,
        args.raw_root or args.repository_root / "data/raw" / scope,
    )
    print(f"Completed in {perf_counter() - started:.1f}s")


if __name__ == "__main__":
    main()
