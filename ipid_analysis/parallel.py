"""Additive 18-VM geographic queue. Legacy six-site jobs keep their contracts.

Only immutable completed inputs unlock work. Tasks never wait for another VM;
the existing scheduler reserves CPU/RAM/scratch for their subprocesses.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys

import pyarrow.parquet as pq
import yaml

from ipid_analysis.artifacts import (
    assert_complete,
    atomic_output,
    file_digest,
    fingerprints,
)
from ipid_analysis.config import (
    FIGURES_DIR,
    PROCESSED_DATA_DIR,
    PROCESSED_STORAGE_DIR,
    RAW_DATA_DIR,
)
from ipid_analysis.continental import (
    PROTOCOLS,
    SITES,
    descriptor,
    publish_json,
    read_json,
    regional_coverage,
    source_measurement,
    verified_input,
    verify_shared_os,
)
from ipid_analysis.runtime import connect

ID = r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}"
_FLOW = re.compile(
    rf"(.+/continental/({ID})/sites/(eu|as|af|na|sa|au))/protocols/(icmp|tcp|udp-dns)$"
)


def flow_parts(flow):
    match = _FLOW.fullmatch(flow)
    if match is None:
        raise ValueError("invalid parallel protocol namespace")
    site_prefix, campaign, site, protocol = match.groups()
    return site_prefix.rsplit("/sites/", 1)[0], campaign, site, protocol


def stage_prefix(task):
    if task.kind == "parallel-discovery":
        return task.prefix + "/discovery"
    if task.kind == "parallel-union":
        return task.prefix + "/union"
    if task.kind == "parallel-site":
        return task.prefix + "/final"
    if task.kind in {"parallel-handoff", "parallel-measurement"}:
        return task.uri.rsplit("/", 1)[0]
    job = task.uri.rsplit("/", 2)[-2]
    category = {
        "parallel-strategies": "strategy-jobs",
        "parallel-core": "core-jobs",
        "parallel-roles": "role-jobs",
        "parallel-os": "os-jobs",
        "parallel-os-roles": "os-role-jobs",
        "parallel-finish": "analysis-jobs",
        "parallel-measurement-os": "measurement-os-jobs",
    }[task.kind]
    return f"{task.prefix}/{category}/{job}"


def success_uri(task):
    if task.kind == "parallel-discovery":
        return task.prefix + "/discovery-ready.json"
    if task.kind == "parallel-union":
        return task.prefix + "/os/request.json"
    return stage_prefix(task) + "/done.json"


def failure_uri(task):
    return stage_prefix(task) + "/failed.json"


def tasks(objects, prefix):
    from ipid_analysis.scheduler import Task

    result = []
    campaigns = sorted(
        u[: -len("/parallel.json")]
        for u in objects
        if u.startswith(prefix.rstrip("/") + "/continental/")
        and re.fullmatch(rf"{re.escape(prefix.rstrip('/'))}/continental/{ID}/parallel.json", u)
    )

    def add(kind, uri, flow, scope="", priority=10):
        task = Task(kind, uri, flow, scope, priority)
        if success_uri(task) not in objects and failure_uri(task) not in objects:
            result.append(task)

    for campaign in campaigns:
        name = campaign.rsplit("/", 1)[-1]
        for site in SITES:
            site_prefix = f"{campaign}/sites/{site}"
            scope = f"continental/{name}/{site}"
            for protocol in PROTOCOLS:
                flow = f"{site_prefix}/protocols/{protocol}"
                if flow + "/discovery.json" in objects:
                    add("parallel-discovery", flow + "/discovery.json", flow, scope, 1)
                for uri in sorted(objects):
                    relative = uri.removeprefix(flow + "/")
                    if relative == uri:
                        continue
                    parts = relative.split("/")
                    if len(parts) != 3 or parts[-1] != "request.json":
                        continue
                    category, job, _ = parts
                    if category == "jobs":
                        add("parallel-handoff", uri, flow, scope, 0)
                    elif category == "measurement-jobs":
                        add("parallel-measurement", uri, flow, scope)
                        if (
                            uri[: -len("request.json")] + "done.json" in objects
                            and campaign + "/os/done.json" in objects
                        ):
                            add("parallel-measurement-os", uri, flow, scope)
                    elif category == "strategy-jobs":
                        add("parallel-strategies", uri, flow, scope, 1)
                        if f"{flow}/strategy-jobs/{job}/done.json" in objects:
                            add("parallel-core", uri, flow, scope)
                        if f"{flow}/core-jobs/{job}/done.json" in objects:
                            add("parallel-roles", uri, flow, scope)
                            if campaign + "/os/done.json" in objects:
                                add("parallel-os", uri, flow, scope)
                                if f"{flow}/role-jobs/{job}/done.json" in objects:
                                    add("parallel-os-roles", uri, flow, scope)
                            os_ready = campaign + "/os/not-requested.json" in objects or (
                                f"{flow}/os-jobs/{job}/done.json" in objects
                                and f"{flow}/os-role-jobs/{job}/done.json" in objects
                            )
                            stream_requests = [
                                u
                                for u in objects
                                if u.startswith(flow + "/measurement-jobs/")
                                and u.endswith("/request.json")
                            ]
                            stream_ready = len(stream_requests) == (
                                10 if protocol == "tcp" else 8
                            ) and all(
                                u[: -len("request.json")] + "done.json" in objects
                                for u in stream_requests
                            )
                            if campaign + "/os/not-requested.json" not in objects:
                                stream_ready = stream_ready and all(
                                    u.replace("/measurement-jobs/", "/measurement-os-jobs/")[
                                        : -len("request.json")
                                    ]
                                    + "done.json"
                                    in objects
                                    for u in stream_requests
                                )
                            if (
                                os_ready
                                and stream_ready
                                and f"{flow}/role-jobs/{job}/done.json" in objects
                            ):
                                add("parallel-finish", uri, flow, scope)
            completions = [
                u
                for u in objects
                if u.startswith(site_prefix + "/protocols/")
                and re.search(r"/analysis-jobs/[^/]+/done.json$", u)
            ]
            if (
                len(completions) == 3
                and site_prefix + "/final/request.json" in objects
                and site_prefix + "/interprotocol-jobs/interprotocol/done.json" in objects
            ):
                add("parallel-site", site_prefix + "/final/request.json", site_prefix, scope)
        if (
            all(
                f"{campaign}/sites/{s}/protocols/{p}/discovery-ready.json" in objects
                for s in SITES
                for p in PROTOCOLS
            )
            and campaign + "/os/not-requested.json" not in objects
        ):
            add("parallel-union", campaign + "/union/request.json", campaign, priority=1)
    return result


def binding(client, flow, work):
    campaign, name, site, protocol = flow_parts(flow)
    layout = read_json(client, campaign + "/parallel.json", work)
    if (
        layout.get("version") != 1
        or layout.get("campaign_id") != name
        or layout.get("mode") != "parallel-protocols"
    ):
        raise ValueError("invalid parallel campaign layout")
    value = descriptor(client, campaign, work)
    return campaign, value, site, protocol


def protocol_request(client, uri, flow, work):
    from ipid_analysis.s3_workflow import AnalysisRequest, validate_analysis_manifest

    campaign, config, site, protocol = binding(client, flow, work)
    request = AnalysisRequest.parse(read_json(client, uri, work), flow)
    raw = f"{config['raw_root'].rstrip('/')}/continental/{config['campaign_id']}/{site}"
    if (
        request.protocol != protocol
        or request.zmap_prefix.rstrip("/") != raw + "/zmap"
        or request.ipid_prefix.rstrip("/") != raw + "/ipid"
        or request.os_prefix.rstrip("/")
        != ("" if config.get("os_mode") == "disabled" else raw.rsplit("/", 1)[0] + "/os")
    ):
        raise ValueError("protocol request crosses ownership boundary")
    manifest = read_json(client, request.manifest_uri, work)
    validate_analysis_manifest(manifest, request)
    disabled = config.get("os_mode") == "disabled"
    if disabled != (request.os_status == "not_requested") or (
        not disabled and manifest[protocol].get("os") != "shared-os"
    ):
        raise ValueError("parallel campaign OS policy mismatch")
    return request, manifest


def discovery(client, task, work):
    campaign, config, site, protocol = binding(client, task.prefix, work)
    event = read_json(client, task.uri, work)
    raw = f"{config['raw_root'].rstrip('/')}/continental/{config['campaign_id']}/{site}/zmap"
    from ipid_analysis.s3_workflow import measurement_protocol

    if (
        event.get("version") != 1
        or event.get("campaign_id") != config["campaign_id"]
        or event.get("site") != site
        or event.get("protocol") != protocol
        or event.get("raw_prefix") != raw
        or measurement_protocol(event.get("measurement_id", "")) != protocol
    ):
        raise ValueError("invalid discovery event")
    source, evidence = source_measurement(client, raw, event["measurement_id"], "zmap.pq", work)
    directory = (
        PROCESSED_STORAGE_DIR
        / "continental"
        / config["campaign_id"]
        / "discovery-parts"
        / site
        / protocol
    )
    output = directory / "targets.pq"
    # One independently committed partition per source; no repeated whole-union rewrite.
    with connect() as con, atomic_output(output, inputs=[source], parameters=event) as temporary:
        con.read_parquet(str(source)).project(
            "CAST(IP_ADDR AS VARCHAR) AS IP_ADDR, '' AS REPLY_TYPE"
        ).distinct().write_parquet(str(temporary), compression="zstd")
    member = dict(
        site=site,
        protocol=protocol,
        measurement_id=event["measurement_id"],
        uri=f"{raw}/{event['measurement_id']}/zmap.pq",
        sha256=evidence["sha256"],
    )
    publish_json(
        client,
        success_uri(task),
        dict(
            version=1,
            event=event,
            member=member,
            partition=str(output),
            size_bytes=output.stat().st_size,
            sha256=file_digest(output),
        ),
        work,
    )


def union(client, task, work):
    config = descriptor(client, task.prefix, work)
    members, inputs = [], []
    for site in SITES:
        for protocol in PROTOCOLS:
            flow = f"{task.prefix}/sites/{site}/protocols/{protocol}"
            ready = read_json(client, flow + "/discovery-ready.json", work)
            event = read_json(client, flow + "/discovery.json", work)
            if (
                ready.get("event") != event
                or ready.get("member", {}).get("site") != site
                or ready["member"].get("protocol") != protocol
            ):
                raise ValueError("discovery preparation binding changed")
            path = Path(ready["partition"])
            expected = (
                PROCESSED_STORAGE_DIR
                / "continental"
                / config["campaign_id"]
                / "discovery-parts"
                / site
                / protocol
                / "targets.pq"
            )
            assert_complete(path)
            if (
                path != expected
                or path.stat().st_size != ready["size_bytes"]
                or file_digest(path) != ready["sha256"]
            ):
                raise ValueError("prepared discovery changed")
            members.append(ready["member"])
            inputs.append(str(path))
    output = work / "targets.pq"
    with connect() as con, atomic_output(output, inputs=map(Path, inputs)) as temporary:
        con.read_parquet(inputs).distinct().write_parquet(str(temporary), compression="zstd")
    rows = pq.read_metadata(output).num_rows
    if rows == 0:
        raise ValueError("empty discovery union")
    target = dict(
        uri=task.prefix + "/os/targets.pq",
        size_bytes=output.stat().st_size,
        sha256=file_digest(output),
        rows=rows,
    )
    client.upload(output, target["uri"])
    reference = next(
        m["measurement_id"] for m in members if m["site"] == "eu" and m["protocol"] == "tcp"
    )
    publish_json(
        client,
        success_uri(task),
        dict(
            version=1,
            campaign_id=config["campaign_id"],
            target=target,
            raw_root=config["raw_root"],
            reference_zmap=reference,
            members=members,
        ),
        work,
    )


def measurement(client, task, work, batch_size, threads):
    from ipid_analysis.coverage import write_coverage
    from ipid_analysis.increments import extract_increments
    from ipid_analysis.manifest import IpidMeasurement
    from ipid_analysis.plot_increments import render as plot_increments
    from ipid_analysis.plot_probing_intervals import render as plot_intervals
    from ipid_analysis.plot_strategies import render as plot_strategies
    from ipid_analysis.probing_intervals import extract_probing_intervals
    from ipid_analysis.s3_workflow import measurement_protocol
    from ipid_analysis.strategies import classify_measurement, load_config

    _, config, site, protocol = binding(client, task.prefix, work)
    value = read_json(client, task.uri, work)
    raw = f"{config['raw_root'].rstrip('/')}/continental/{config['campaign_id']}/{site}"
    mid, zid, variant = value["measurement_id"], value["zmap_id"], value["variant"]
    if (
        value.get("version") != 1
        or value.get("site") != site
        or value.get("protocol") != protocol
        or value.get("campaign_id") != config["campaign_id"]
        or measurement_protocol(mid) != protocol
        or measurement_protocol(zid) != protocol
        or value["raw_prefix"] != raw + "/ipid"
        or value["zmap_prefix"] != raw + "/zmap"
        or task.uri != f"{task.prefix}/measurement-jobs/{mid}/request.json"
    ):
        raise ValueError("invalid measurement ownership")
    variants = {
        "rt-base": ("no-connection", "rt-based", "base"),
        "fi-base": ("no-connection", "fixed-interval", "base"),
        "fi-mass": ("no-connection", "fixed-interval", "mass"),
        "repeat": ("no-connection", "fixed-interval", "mass"),
        "connection-rt-base": ("connection", "rt-based", "base"),
        "connection-fi-base": ("connection", "fixed-interval", "base"),
    }
    mode, interval, scale = variants[variant]
    for name in ("ipid.pq", "ipid.snapshot.yaml"):
        source_measurement(client, raw + "/ipid", mid, name, work)
        client.download(f"{raw}/ipid/{mid}/{name}", RAW_DATA_DIR / "ipid" / mid / name)
    snapshot = yaml.safe_load((RAW_DATA_DIR / "ipid" / mid / "ipid.snapshot.yaml").read_text())
    cfg = load_config(RAW_DATA_DIR / "ipid" / mid / "ipid.snapshot.yaml")
    if (
        cfg.connection_count != 4
        or cfg.requests_per_connection != (25 if scale == "mass" else 4)
        or snapshot.get("measurement_mode") != interval
        or snapshot.get("zmap") != zid
        or bool(snapshot.get("tcp", {}).get("establish_connection")) != (mode == "connection")
    ):
        raise ValueError("snapshot differs from measurement event")
    uri = value["target"]["uri"]
    if not uri.startswith(raw + "/") or uri.rsplit("/", 1)[-1] not in {
        "zmap.pq",
        "zmap_unclassified.pq",
        "random-reproducibility-targets.pq",
        "zmap-fixed-base-sample.pq",
        "zmap-connection-sample.pq",
    }:
        raise ValueError("invalid target boundary")
    target = verified_input(client, uri, value["target"], work / "target")
    # Repetitions get additive paths; canonical baseline artifacts are never overwritten.
    # Every streaming report has its own measurement-ID directory. Canonical
    # protocol reports/merges have one owner, the protocol core job.
    m = IpidMeasurement(protocol, mode, interval, scale, mid, mid)
    persisted = f"{raw}/ipid/{mid}/strategies.pq"
    if client.exists(persisted):
        client.download(persisted, RAW_DATA_DIR / "ipid" / mid / "strategies.pq")
    write_coverage(m, {}, target_path=target)
    classify_measurement(m, batch_size=batch_size, threads=threads)
    extract_probing_intervals(m, threads=threads)
    extract_increments(m, batch_size=batch_size, threads=threads)
    plot_strategies(m)
    plot_intervals(m)
    plot_increments(m)
    publish_json(
        client,
        success_uri(task),
        dict(
            version=1,
            measurement_id=mid,
            variant=variant,
            source=fingerprints([RAW_DATA_DIR / "ipid" / mid / "ipid.pq"]),
        ),
        work,
    )


def strategies(client, task, work, batch_size, threads):
    from ipid_analysis.strategies import classify_measurement
    from ipid_analysis.strategy_merge import iter_strategy_merges, merge_strategies

    request, manifest = protocol_request(client, task.uri, task.prefix, work)
    (merge,) = iter_strategy_merges(manifest)
    for m in (merge.base, merge.mass):
        for name in ("ipid.pq", "ipid.snapshot.yaml", "strategies.pq"):
            client.download(
                f"{request.ipid_prefix.rstrip('/')}/{m.measurement_id}/{name}",
                RAW_DATA_DIR / "ipid" / m.measurement_id / name,
            )
        classify_measurement(m, batch_size=batch_size, threads=threads)
    path, stats = merge_strategies(merge, batch_size=batch_size, threads=threads)
    ready = dict(
        version=1,
        job_id=request.job_id,
        manifest_uri=request.manifest_uri,
        manifest=manifest,
        sources=fingerprints([path]),
        rows=stats.rows,
        sha256=file_digest(path),
        size_bytes=path.stat().st_size,
        scope=os.environ.get("IPID_ANALYSIS_SCOPE"),
    )
    publish_json(
        client, request.manifest_uri.rsplit("/", 1)[0] + "/strategies-ready.json", ready, work
    )
    publish_json(client, success_uri(task), ready, work)


def core(client, task, work, batch_size, threads):
    from ipid_analysis.postprocess import required_core_artifacts
    from ipid_analysis.s3_workflow import download_analysis_inputs

    request, manifest = protocol_request(client, task.uri, task.prefix, work)
    download_analysis_inputs(client, request, manifest, include_os=False)
    path = work / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    log = work / "postprocess.log"
    status = work / "postprocess-status.json"
    with log.open("w") as output:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "ipid_analysis.postprocess",
                str(path),
                "--strict",
                "--phase",
                "core",
                "--batch-size",
                str(batch_size),
                "--threads",
                str(threads),
                "--status-file",
                str(status),
            ],
            stdout=output,
            stderr=subprocess.STDOUT,
            check=True,
            timeout=int(os.environ.get("IPID_ANALYSIS_POSTPROCESS_TIMEOUT", "172800")),
        )
    value = json.loads(status.read_text())
    if (
        value.get("required_complete") is not True
        or value.get("phase") != "core"
        or value.get("manifest") != fingerprints([path])
    ):
        raise ValueError("core postprocessing incomplete")
    for artifact in required_core_artifacts(manifest):
        assert_complete(artifact)
    client.upload(log, stage_prefix(task) + "/postprocess.log")
    client.upload(status, stage_prefix(task) + "/postprocess-status.json")
    publish_json(
        client,
        success_uri(task),
        dict(
            version=1,
            job_id=request.job_id,
            manifest_uri=request.manifest_uri,
            phase="core",
            artifacts=value["artifacts"],
        ),
        work,
    )


def sources(manifest):
    from ipid_analysis.manifest import resolve
    from ipid_analysis.strategy_merge import iter_strategy_merges

    values = list(iter_strategy_merges(manifest))
    connection = resolve(manifest, "tcp.ipid.connection.rt-based.base")
    if connection:
        values.append(connection)
    return values


def roles(client, task, work, threads):
    from ipid_analysis.caida_itdk import prepare_itdk
    from ipid_analysis.plot_itdk_strategy import render_itdk_analysis
    from ipid_analysis.plot_ripe_atlas_strategy import render_ripe_atlas_analysis
    from ipid_analysis.ripe_atlas import infer_campaign_start, parse_datetime, prepare_ripe_atlas

    request, manifest = protocol_request(client, task.uri, task.prefix, work)
    errors = []
    itdk = ripe = None
    if os.environ.get("IPID_ANALYSIS_SKIP_ITDK", "0").lower() not in {"1", "true"}:
        try:
            itdk = prepare_itdk(
                release=os.environ.get("IPID_ANALYSIS_ITDK_RELEASE", "2025-08"),
                topology=os.environ.get("IPID_ANALYSIS_ITDK_TOPOLOGY", "midar-iff-snmp-tnt"),
                source_dir=Path(os.environ["IPID_ANALYSIS_ITDK_SOURCE_DIR"])
                if os.environ.get("IPID_ANALYSIS_ITDK_SOURCE_DIR")
                else None,
                ifaces=Path(os.environ["IPID_ANALYSIS_ITDK_IFACES"])
                if os.environ.get("IPID_ANALYSIS_ITDK_IFACES")
                else None,
            )
        except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
            errors.append(str(exc))
    if os.environ.get("IPID_ANALYSIS_SKIP_RIPE", "0").lower() not in {"1", "true"}:
        try:
            ripe = prepare_ripe_atlas(
                campaign_start=parse_datetime(os.environ["IPID_ANALYSIS_RIPE_CAMPAIGN_START"])
                if os.environ.get("IPID_ANALYSIS_RIPE_CAMPAIGN_START")
                else infer_campaign_start(manifest),
                source=os.environ.get("IPID_ANALYSIS_RIPE_SOURCE", "daily-dumps"),
                measurement_ids_file=Path(os.environ["IPID_ANALYSIS_RIPE_MEASUREMENT_IDS"])
                if os.environ.get("IPID_ANALYSIS_RIPE_MEASUREMENT_IDS")
                else None,
                input_dir=Path(os.environ["IPID_ANALYSIS_RIPE_INPUT_DIR"])
                if os.environ.get("IPID_ANALYSIS_RIPE_INPUT_DIR")
                else None,
                preprocess_workers=max(1, threads),
            )
        except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
            errors.append(str(exc))
    for source in sources(manifest):
        for dataset, renderer in (
            (itdk, render_itdk_analysis),
            (ripe, render_ripe_atlas_analysis),
        ):
            if dataset is None:
                continue
            try:
                kwargs = {"threads": threads}
                if dataset is ripe:
                    kwargs["itdk"] = itdk
                renderer(source, dataset, **kwargs)
            except (OSError, RuntimeError, ValueError) as exc:
                errors.append(str(exc))
    publish_json(
        client,
        success_uri(task),
        dict(version=1, job_id=request.job_id, optional_errors=errors),
        work,
    )


def os_analysis(client, task, work, threads, role_only=False):
    from ipid_analysis.plot_os_group_strategy import render, render_measurement

    request, manifest = protocol_request(client, task.uri, task.prefix, work)
    campaign = flow_parts(task.prefix)[0]
    shared, done = verify_shared_os(client, campaign, work)
    zmap, evidence = source_measurement(
        client, request.zmap_prefix, request.job_id, "zmap.pq", work
    )
    if not role_only:
        regional_coverage(
            zmap,
            shared / "os-observations.pq",
            PROCESSED_DATA_DIR / request.job_id / "shared-os-coverage.json",
            provenance=dict(
                campaign_id=done["campaign_id"],
                os_measurement_id=done["measurement_id"],
                os_vantage=done["vantage"],
                union=done["target"],
            ),
        )
        for source in sources(manifest):
            renderer = render if hasattr(source, "base") else render_measurement
            renderer(source, "shared-os", threads=threads)
    else:
        # Reuse the original persisted role joins; no external downloads in this stage.
        from datetime import datetime

        from ipid_analysis.caida_itdk import ITDKDataset
        from ipid_analysis.plot_itdk_os import render_itdk_os_analysis
        from ipid_analysis.plot_ripe_atlas_os import render_ripe_atlas_os_analysis
        from ipid_analysis.ripe_atlas import RipeAtlasDataset, RipeWindow

        for source in sources(manifest):
            for role, kind, renderer in [
                ("itdk", "itdk", render_itdk_os_analysis),
                ("ripe_atlas", "ripe-atlas", render_ripe_atlas_os_analysis),
            ]:
                metadata = source.artifact_path(FIGURES_DIR, kind + "-role-by-strategy", "json")
                joined = source.artifact_path(PROCESSED_DATA_DIR, kind + "-strategy-join")
                if not metadata.is_file() or not joined.is_file():
                    continue
                assert_complete(metadata)
                assert_complete(joined)
                value = json.loads(metadata.read_text())[role]
                kwargs = {"threads": threads}
                if role == "itdk":
                    dataset = ITDKDataset(
                        value["release"],
                        value["topology"],
                        joined.parent,
                        joined,
                        joined,
                        Path(value["metadata"]),
                        joined,
                    )
                else:
                    window = RipeWindow(
                        datetime.fromisoformat(value["window_start"]),
                        datetime.fromisoformat(value["window_end"]),
                    )
                    dataset = RipeAtlasDataset(
                        value["source"], window, joined.parent, joined, Path(value["metadata"])
                    )
                    kwargs["role_matches_path"] = joined
                renderer(source, dataset, "shared-os", **kwargs)
    publish_json(
        client, success_uri(task), dict(version=1, job_id=request.job_id, shared_os=done), work
    )


def finish(client, task, work):
    request, _ = protocol_request(client, task.uri, task.prefix, work)
    campaign = flow_parts(task.prefix)[0]
    disabled = descriptor(client, campaign, work).get("os_mode") == "disabled"
    stages = ["core-jobs", "role-jobs"] + ([] if disabled else ["os-jobs", "os-role-jobs"])
    values = {
        stage: read_json(client, f"{task.prefix}/{stage}/{request.job_id}/done.json", work)
        for stage in stages
    }
    if any(v.get("job_id") != request.job_id for v in values.values()):
        raise ValueError("analysis completion binding mismatch")
    publish_json(
        client,
        success_uri(task),
        dict(
            version=1,
            job_id=request.job_id,
            manifest_uri=request.manifest_uri,
            stages=values,
            os_status="not_requested" if disabled else "completed",
        ),
        work,
    )


def finish_site(client, task, work):
    event = read_json(client, task.uri, work)
    campaign = task.prefix.rsplit("/sites/", 1)[0]
    site = task.prefix.rsplit("/", 1)[-1]
    if event != dict(
        version=1,
        campaign_id=campaign.rsplit("/", 1)[-1],
        site=site,
        interprotocol_job="interprotocol",
    ):
        raise ValueError("invalid site completion event")
    completed = {}
    for p in PROTOCOLS:
        flow = f"{task.prefix}/protocols/{p}"
        protocol_done = read_json(client, flow + "/done.json", work)
        uri = f"{flow}/analysis-jobs/{protocol_done['job_id']}/done.json"
        completed[p] = read_json(client, uri, work)
        if completed[p].get("manifest_uri") != protocol_done["manifest_uri"]:
            raise ValueError("site analysis binding mismatch")
    inter = read_json(client, task.prefix + "/interprotocol-jobs/interprotocol/done.json", work)
    publish_json(
        client,
        success_uri(task),
        dict(
            version=1,
            campaign_id=event["campaign_id"],
            site=site,
            protocols=completed,
            interprotocol=inter,
        ),
        work,
    )


def execute(client, task, work, batch_size, threads):
    if task.kind == "parallel-measurement-os":
        return measurement_os(client, task, work, threads)
    if task.kind == "parallel-discovery":
        return discovery(client, task, work)
    if task.kind == "parallel-union":
        return union(client, task, work)
    if task.kind == "parallel-site":
        return finish_site(client, task, work)
    if task.kind == "parallel-measurement":
        return measurement(client, task, work, batch_size, threads)
    if task.kind == "parallel-strategies":
        return strategies(client, task, work, batch_size, threads)
    if task.kind == "parallel-core":
        return core(client, task, work, batch_size, threads)
    if task.kind == "parallel-roles":
        return roles(client, task, work, threads)
    if task.kind == "parallel-os":
        return os_analysis(client, task, work, threads)
    if task.kind == "parallel-os-roles":
        return os_analysis(client, task, work, threads, True)
    if task.kind == "parallel-finish":
        return finish(client, task, work)
    if task.kind == "parallel-handoff":
        from ipid_analysis.s3_workflow import Request, process_request

        _, config, site, p = binding(client, task.prefix, work)
        request = Request.parse(read_json(client, task.uri, work), task.prefix)
        raw = f"{config['raw_root'].rstrip('/')}/continental/{config['campaign_id']}/{site}/ipid"
        if request.protocol != p or request.ipid_uri != f"{raw}/{request.measurement_id}/ipid.pq":
            raise ValueError("invalid handoff ownership")
        for name in ("ipid.pq", "ipid.snapshot.yaml"):
            source_measurement(client, raw, request.measurement_id, name, work)
        return process_request(client, task.uri, task.prefix, work, batch_size, threads)
    raise ValueError("unknown parallel task")


def measurement_os(client, task, work, threads):
    """Additive OS reports for one finished measurement, independent of its sweep."""
    from ipid_analysis.artifacts import write_text
    from ipid_analysis.manifest import IpidMeasurement
    from ipid_analysis.plot_os_group_strategy import (
        aggregate_os_groups,
        plot_os_groups,
        write_os_groups,
    )
    from ipid_analysis.runtime import exclusive_lock

    campaign, config, site, p = binding(client, task.prefix, work)
    event = read_json(client, task.uri, work)
    mid = event["measurement_id"]
    if (
        event.get("campaign_id") != config["campaign_id"]
        or event.get("site") != site
        or event.get("protocol") != p
        or task.uri != f"{task.prefix}/measurement-jobs/{mid}/request.json"
    ):
        raise ValueError("invalid measurement OS source")
    measured = read_json(client, task.uri.rsplit("/", 1)[0] + "/done.json", work)
    if measured.get("measurement_id") != mid:
        raise ValueError("measurement analysis completion mismatch")
    variant = event["variant"]
    connection = "connection" if variant.startswith("connection-") else "no-connection"
    interval = "rt-based" if variant in {"rt-base", "connection-rt-base"} else "fixed-interval"
    m = IpidMeasurement(
        p, connection, interval, "mass" if variant in {"fi-mass", "repeat"} else "base", mid, mid
    )
    shared, done = verify_shared_os(client, campaign, work)
    groups = PROCESSED_DATA_DIR / "os" / "shared-os" / "os-groups.pq"
    with exclusive_lock("parallel-os-groups:" + str(groups)):
        write_os_groups(shared / "os.pq", groups, threads=threads)
    kind = "operating-system-group-by-strategy"
    aggregate = m.artifact_path(PROCESSED_DATA_DIR, kind)
    stats = aggregate_os_groups(
        m.artifact_path(PROCESSED_DATA_DIR, "strategies"), groups, aggregate, threads=threads
    )
    pdf = m.artifact_path(FIGURES_DIR, kind, "pdf")
    plot_os_groups(aggregate, pdf)
    write_text(
        m.artifact_path(FIGURES_DIR, kind, "json"),
        json.dumps(dict(measurement_id=mid, shared_os=done, statistics=stats), indent=2) + "\n",
    )
    publish_json(
        client, success_uri(task), dict(version=1, measurement_id=mid, shared_os=done), work
    )


def retry(client, uri, workflow):
    """Archive exactly one failed parallel stage, preserving all source/success data."""
    import time

    from ipid_analysis.runtime import cleanup_work, control_root, work_path
    from ipid_analysis.scheduler import Task

    pattern = rf"({re.escape(workflow.rstrip('/'))}/continental/{ID}/sites/(?:eu|as|af|na|sa|au)/protocols/(?:icmp|tcp|udp-dns))/(.+)"
    match = re.fullmatch(pattern, uri)
    if match is None:
        return False
    flow, relative = match.groups()
    root = work_path(uri, category="retry")
    try:
        binding(client, flow, root)
        if relative == "discovery.json":
            task = Task("parallel-discovery", uri, flow)
            read_json(client, uri, root)
        else:
            parts = relative.split("/")
            categories = {
                "jobs": "parallel-handoff",
                "measurement-jobs": "parallel-measurement",
                "measurement-os-jobs": "parallel-measurement-os",
                "strategy-jobs": "parallel-strategies",
                "core-jobs": "parallel-core",
                "role-jobs": "parallel-roles",
                "os-jobs": "parallel-os",
                "os-role-jobs": "parallel-os-roles",
                "analysis-jobs": "parallel-finish",
            }
            if len(parts) != 3 or parts[0] not in categories or parts[2] != "request.json":
                raise ValueError("invalid parallel retry URI")
            from ipid_analysis.s3_workflow import JOB_ID_RE

            if not JOB_ID_RE.fullmatch(parts[1]):
                raise ValueError("invalid retry job ID")
            category, mid, _ = parts
            source_uri = (
                f"{flow}/measurement-jobs/{mid}/request.json"
                if category == "measurement-os-jobs"
                else uri
                if category in {"jobs", "measurement-jobs", "strategy-jobs"}
                else f"{flow}/strategy-jobs/{mid}/request.json"
            )
            task = Task(categories[category], source_uri, flow)
            if category in {"jobs", "measurement-jobs", "measurement-os-jobs"}:
                value = read_json(client, source_uri, root)
                if value.get("measurement_id") != mid:
                    raise ValueError("retry source identity mismatch")
            else:
                protocol_request(client, source_uri, flow, root)
        failed = failure_uri(task)
        if client.exists(success_uri(task)) or not client.exists(failed):
            raise ValueError("retry requires a terminal failure without success")
        path = root / "failed.json"
        client.download(failed, path)
        client.upload(path, failed.rsplit("/", 1)[0] + f"/attempts/{time.time_ns()}/failed.json")
        client.delete_failure(failed)
        return True
    finally:
        cleanup_work(root, root=control_root() / "retry")
