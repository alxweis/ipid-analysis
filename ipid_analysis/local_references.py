"""Additive, same-vantage controls; original cohorts and outcomes stay immutable."""

import json
import re

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from ipid_analysis.artifacts import (
    assert_complete,
    atomic_output,
    file_digest,
    validate_ip_population,
    write_text,
)
from ipid_analysis.config import PROCESSED_DATA_DIR
from ipid_analysis.continental import publish_json, read_json, source_measurement, verified_input
from ipid_analysis.interprotocol import GROUPS, InterprotocolConfig
from ipid_analysis.interprotocol_outcomes import classify_rows, prerequisites
from ipid_analysis.runtime import connect

PROTOCOLS = ("icmp", "tcp", "udp")
SAMPLES = ("base", "mass")
SCHEMA = pa.schema(
    [
        ("IP_ADDR", pa.string()),
        ("BASELINE_STRATEGY", pa.string()),
        ("LOCAL_STRATEGY", pa.string()),
        ("RECEIVED_SAMPLES", pa.int32()),
        ("EXPECTED_SAMPLES", pa.int32()),
    ]
)


def build_targets(directory):
    """Union only the original cohorts involving this protocol; never reselect."""
    inputs = [directory / f"{group}-targets.pq" for group in GROUPS]
    for protocol in PROTOCOLS:
        paths = [str(directory / f"{g}-targets.pq") for g, ps in GROUPS.items() if protocol in ps]
        with connect() as con:
            con.read_parquet(paths).create_view("cohorts")
            for sample in SAMPLES:
                output = directory / f"reference-{protocol}-{sample}-targets.pq"
                predicate = (
                    "IN ('MULTI','RANDOM')" if sample == "mass" else "NOT IN ('MULTI','RANDOM')"
                )
                with atomic_output(
                    output,
                    inputs=inputs,
                    parameters=dict(protocol=protocol, sample=sample, version=1),
                ) as temporary:
                    con.sql(
                        f"SELECT DISTINCT IP_ADDR,IPID_SELECTION_STRATEGY FROM cohorts WHERE IPID_SELECTION_STRATEGY {predicate} ORDER BY IP_ADDR"
                    ).write_parquet(str(temporary), compression="zstd")
    return [directory / f"reference-{p}-{s}-targets.pq" for p in PROTOCOLS for s in SAMPLES]


def names():
    return [f"reference-{p}-{s}" for p in PROTOCOLS for s in SAMPLES]


def tasks(objects, site, scope):
    from ipid_analysis.parallel import failure_uri, success_uri
    from ipid_analysis.scheduler import Task

    result = []
    for name in names():
        uri = f"{site}/reference-jobs/{name}/request.json"
        task = Task("parallel-reference", uri, site, scope, 10)
        if (
            uri in objects
            and success_uri(task) not in objects
            and failure_uri(task) not in objects
        ):
            result.append(task)
    uri = site + "/reference-comparison/request.json"
    task = Task("parallel-reference-comparison", uri, site, scope, 10)
    if (
        uri in objects
        and all(f"{site}/reference-jobs/{n}/done.json" in objects for n in names())
        and site + "/interprotocol-jobs/interprotocol/done.json" in objects
        and success_uri(task) not in objects
        and failure_uri(task) not in objects
    ):
        result.append(task)
    return result


def binding(client, site, work):
    from ipid_analysis.parallel import binding as protocol_binding

    campaign, config, region, _ = protocol_binding(client, site + "/protocols/udp-dns", work)
    if read_json(client, campaign + "/parallel.json", work).get("local_references") is not True:
        raise ValueError("campaign does not request local references")
    return config, region


def validate_snapshot(snapshot, protocol, sample):
    cfg = InterprotocolConfig((protocol,), 4, 4 if sample == "base" else 25)
    section = snapshot.get("interprotocol", {})
    if (
        section.get("local_reference") is not True
        or section.get("protocols") != [protocol]
        or section.get("sample") != sample
        or snapshot.get("connection_count") != cfg.connection_count
        or snapshot.get("requests_per_connection") != cfg.requests_per_connection
        or section.get("retries") != 0
        or section.get("tcp_port") != 80
        or section.get("udp_port") != 53
        or snapshot.get("tcp", {}).get("establish_connection") is not False
    ):
        raise ValueError("invalid single-protocol reference snapshot")
    interface = snapshot.get("interface", {})
    import ipaddress

    for key in ("ip_a", "ip_b"):
        if ipaddress.ip_address(interface.get(key, "")).version != 4:
            raise ValueError("reference requires two IPv4 source addresses")
    if interface["ip_a"] == interface["ip_b"]:
        raise ValueError("reference source addresses must differ")
    prerequisites(cfg, snapshot)
    return cfg


def classify_reference(raw, target, snapshot, protocol, sample, output, batch_size=2048):
    cfg = validate_snapshot(snapshot, protocol, sample)
    validate_ip_population(raw)
    validate_ip_population(target)
    with connect() as con:
        con.read_parquet(str(raw)).create_view("raw")
        con.read_parquet(str(target)).create_view("targets")
        if con.sql(
            "SELECT count(*) FROM raw r LEFT JOIN targets t USING(IP_ADDR) WHERE t.IP_ADDR IS NULL OR r.IPID_SELECTION_STRATEGY IS DISTINCT FROM t.IPID_SELECTION_STRATEGY"
        ).fetchone()[0]:
            raise ValueError("reference raw population differs from original targets")
        scratch = output.parent / "classified.pq"
        # This file lives in task scratch, not the durable scientific namespace.
        scratch.parent.mkdir(parents=True, exist_ok=True)
        try:
            with (
                pq.ParquetWriter(scratch, SCHEMA, compression="zstd") as writer,
                pq.ParquetFile(raw) as source,
            ):
                for batch in source.iter_batches(batch_size=batch_size):
                    classified = classify_rows(batch.to_pylist(), cfg, snapshot)
                    rows = [
                        dict(
                            IP_ADDR=r["IP_ADDR"],
                            BASELINE_STRATEGY=r["BASELINE_STRATEGY"],
                            LOCAL_STRATEGY=r["OBSERVED_STRATEGIES"][0],
                            RECEIVED_SAMPLES=r["RECEIVED_SAMPLES"][0],
                            EXPECTED_SAMPLES=r["EXPECTED_SAMPLES"],
                        )
                        for r in classified
                    ]
                    writer.write_table(pa.Table.from_pylist(rows, schema=SCHEMA))
            con.read_parquet(str(scratch)).create_view("classified")
            with atomic_output(
                output, inputs=[raw, target], parameters=dict(snapshot=snapshot, version=1)
            ) as temporary:
                con.sql(
                    f"SELECT t.IP_ADDR,t.IPID_SELECTION_STRATEGY AS BASELINE_STRATEGY,coalesce(c.LOCAL_STRATEGY,'NOT_ENOUGH_SAMPLES') AS LOCAL_STRATEGY,coalesce(c.RECEIVED_SAMPLES,0)::INTEGER AS RECEIVED_SAMPLES,{cfg.connection_count * cfg.requests_per_connection}::INTEGER AS EXPECTED_SAMPLES FROM targets t LEFT JOIN classified c USING(IP_ADDR) ORDER BY IP_ADDR"
                ).write_parquet(str(temporary), compression="zstd")
        finally:
            scratch.unlink(missing_ok=True)
    return output


def analyze(client, task, work, batch_size):
    from ipid_analysis.parallel import success_uri

    config, site = binding(client, task.prefix, work)
    event = read_json(client, task.uri, work)
    protocol, sample, mid = (
        event.get("protocol"),
        event.get("sample"),
        event.get("measurement_id", ""),
    )
    raw = f"{config['raw_root'].rstrip('/')}/continental/{config['campaign_id']}/{site}/ipid"
    name = f"reference-{protocol}-{sample}"
    expected_target = (
        f"{task.prefix}/interprotocol-target-jobs/interprotocol/targets/{name}-targets.pq"
    )
    if (
        event.get("version") != 1
        or event.get("campaign_id") != config["campaign_id"]
        or event.get("site") != site
        or protocol not in PROTOCOLS
        or sample not in SAMPLES
        or event.get("raw_prefix") != raw
        or task.uri != f"{task.prefix}/reference-jobs/{name}/request.json"
        or not re.fullmatch(
            rf"interprotocol-{protocol}_\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}-\d{{2}}_{sample}",
            mid,
        )
        or event.get("target", {}).get("uri") != expected_target
    ):
        raise ValueError("reference event crosses its ownership boundary")
    target = verified_input(client, expected_target, event["target"], work / "target")
    source, _ = source_measurement(client, raw, mid, "interprotocol.pq", work)
    config_path, _ = source_measurement(client, raw, mid, "interprotocol.snapshot.yaml", work)
    snapshot = yaml.safe_load(config_path.read_text())
    result = classify_reference(
        source,
        target,
        snapshot,
        protocol,
        sample,
        work / "result" / "strategies.pq",
        min(batch_size, 2048),
    )
    destination = (
        PROCESSED_DATA_DIR / "interprotocol" / "local-references" / name / "strategies.pq"
    )
    from ipid_analysis.artifacts import publish_file

    publish_file(
        result, destination, inputs=[source, target], parameters=dict(snapshot=snapshot, version=1)
    )
    publish_json(
        client,
        success_uri(task),
        dict(
            version=1,
            measurement_id=mid,
            result=str(destination),
            sha256=file_digest(destination),
            rows=pq.read_metadata(destination).num_rows,
            snapshot=snapshot,
            target=event["target"],
        ),
        work,
    )


def compare_files(outcomes, references, output):
    """Keep every original interleaved row/protocol, including missing controls."""
    with connect() as con:
        parts = []
        for (protocol, sample), path in references.items():
            view = f"ref_{protocol}_{sample}"
            con.read_parquet(str(path)).create_view(view)
            parts.append(f"SELECT *, '{protocol}' AS PROTOCOL,'{sample}' AS SAMPLE FROM {view}")
        con.sql(" UNION ALL ".join(parts)).create_view("refs")
        con.read_parquet(str(outcomes)).create_view("outcomes")
        con.sql(
            "SELECT o.IP_ADDR,o.PROTOCOL_GROUP,o.BASELINE_STRATEGY,o.SAMPLE,o.OUTCOME,unnest(o.PROTOCOLS) AS PROTOCOL,unnest(o.OBSERVED_STRATEGIES) AS INTERLEAVED_STRATEGY,unnest(o.RECEIVED_SAMPLES) AS INTERLEAVED_RECEIVED FROM outcomes o"
        ).create_view("expanded")
        with atomic_output(
            output, inputs=[outcomes, *references.values()], parameters=dict(version=1)
        ) as temporary:
            con.sql(
                "SELECT e.*,coalesce(r.LOCAL_STRATEGY,'NOT_ENOUGH_SAMPLES') AS LOCAL_STRATEGY,coalesce(r.RECEIVED_SAMPLES,0)::INTEGER AS LOCAL_RECEIVED,r.IP_ADDR IS NOT NULL AS CONTROL_PRESENT, CASE WHEN r.LOCAL_STRATEGY IS NULL OR r.LOCAL_STRATEGY='NOT_ENOUGH_SAMPLES' OR e.INTERLEAVED_STRATEGY='NOT_ENOUGH_SAMPLES' THEN 'NOT_ENOUGH_SAMPLES' WHEN r.LOCAL_STRATEGY=e.INTERLEAVED_STRATEGY THEN 'UNCHANGED' ELSE 'CHANGED' END AS LOCAL_TO_INTERLEAVED FROM expanded e LEFT JOIN refs r USING(IP_ADDR,PROTOCOL,SAMPLE,BASELINE_STRATEGY)"
            ).write_parquet(str(temporary), compression="zstd")
        counts = con.execute(
            "SELECT PROTOCOL,SAMPLE,BASELINE_STRATEGY,LOCAL_STRATEGY,INTERLEAVED_STRATEGY,LOCAL_TO_INTERLEAVED,count(*) AS TARGETS FROM read_parquet(?) GROUP BY ALL ORDER BY ALL",
            [str(output)],
        ).fetchall()
    write_text(
        output.with_suffix(".json"),
        json.dumps(
            dict(
                version=1,
                unit="original cohort IP/protocol/group",
                columns=[
                    "protocol",
                    "sample",
                    "baseline",
                    "local",
                    "interleaved",
                    "transition",
                    "targets",
                ],
                counts=counts,
            ),
            indent=2,
        )
        + "\n",
    )
    return output


def compare(client, task, work):
    from ipid_analysis.interprotocol import load_run_manifest, measurement_items
    from ipid_analysis.parallel import success_uri

    config, site = binding(client, task.prefix, work)
    if read_json(client, task.uri, work) != dict(
        version=1, campaign_id=config["campaign_id"], site=site
    ):
        raise ValueError("invalid reference comparison request")
    references, snapshots = {}, {}
    for protocol in PROTOCOLS:
        for sample in SAMPLES:
            done = read_json(
                client,
                f"{task.prefix}/reference-jobs/reference-{protocol}-{sample}/done.json",
                work,
            )
            path = (
                PROCESSED_DATA_DIR
                / "interprotocol"
                / "local-references"
                / f"reference-{protocol}-{sample}"
                / "strategies.pq"
            )
            assert_complete(path)
            if str(path) != done.get("result") or file_digest(path) != done.get("sha256"):
                raise ValueError("local reference result changed after completion")
            references[protocol, sample] = path
            snapshots[protocol, sample] = done["snapshot"]
    read_json(client, task.prefix + "/interprotocol-jobs/interprotocol/done.json", work)
    manifest_path = work / "interprotocol-manifest.json"
    client.download(task.prefix + "/interprotocol-jobs/interprotocol/manifest.json", manifest_path)
    manifest = load_run_manifest(manifest_path)
    raw = f"{config['raw_root'].rstrip('/')}/continental/{config['campaign_id']}/{site}/ipid"
    for group, spec in manifest["groups"].items():
        for sample, mid in measurement_items(spec):
            path, _ = source_measurement(client, raw, mid, "interprotocol.snapshot.yaml", work)
            snapshot = yaml.safe_load(path.read_text())
            for protocol in GROUPS[group]:
                control = snapshots[protocol, sample]
                assert_comparable(snapshot, control, protocol, sample)
    outcomes = (
        PROCESSED_DATA_DIR
        / "interprotocol"
        / "interprotocol"
        / "runs"
        / "interprotocol"
        / "interprotocol-campaign-outcomes.pq"
    )
    assert_complete(outcomes)
    output = PROCESSED_DATA_DIR / "interprotocol" / "local-references" / "comparison.pq"
    compare_files(outcomes, references, output)
    publish_json(
        client,
        success_uri(task),
        dict(
            version=1,
            result=str(output),
            sha256=file_digest(output),
            rows=pq.read_metadata(output).num_rows,
        ),
        work,
    )


def assert_comparable(snapshot, control, protocol, sample):
    validate_snapshot(control, protocol, sample)
    fields = (
        "interface",
        "request_ip_ids",
        "connection_count",
        "requests_per_connection",
        "measurement_mode",
        "fixed_interval",
        "tcp",
        "maximum_tolerated_rtt",
        "bandwidth",
        "packets_per_second",
        "number_of_inflight_probes",
    )
    section = snapshot.get("interprotocol", {})
    reference_section = control["interprotocol"]
    if (
        any(snapshot.get(key) != control.get(key) for key in fields)
        or any(
            section.get(key) != reference_section.get(key)
            for key in ("sample", "request_ipid_index", "retries", "tcp_port", "udp_port")
        )
        or protocol not in section.get("protocols", [])
        or section.get("local_reference", False)
    ):
        raise ValueError(
            "reference and interleaving use different source addresses or probe configuration"
        )


def retry(client, uri, workflow):
    import time

    from ipid_analysis.parallel import failure_uri, success_uri
    from ipid_analysis.runtime import cleanup_work, control_root, work_path
    from ipid_analysis.scheduler import Task

    match = re.fullmatch(
        re.escape(workflow.rstrip("/"))
        + r"/continental/([A-Za-z0-9][A-Za-z0-9_-]{0,79})/sites/(eu|as|af|na|sa|au)/(reference-jobs/reference-(icmp|tcp|udp)-(base|mass)|reference-comparison)/request.json",
        uri,
    )
    if match is None:
        return False
    site = uri.split("/reference-", 1)[0]
    kind = (
        "parallel-reference-comparison"
        if "/reference-comparison/" in uri
        else "parallel-reference"
    )
    task = Task(kind, uri, site)
    root = work_path(uri, category="retry")
    try:
        binding(client, site, root)
        read_json(client, uri, root)
        failure = failure_uri(task)
        if client.exists(success_uri(task)) or not client.exists(failure):
            raise ValueError("retry requires a terminal failure without success")
        path = root / "failed.json"
        client.download(failure, path)
        client.upload(path, failure.rsplit("/", 1)[0] + f"/attempts/{time.time_ns()}/failed.json")
        client.delete_failure(failure)
        return True
    finally:
        cleanup_work(root, root=control_root() / "retry")
