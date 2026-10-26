"""Late OS stages. Core policy, science outputs and terminal markers are read-only."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

import pyarrow.parquet as pq

from ipid_analysis.artifacts import assert_complete, completion_path, file_digest
from ipid_analysis.continental import (
    ID,
    PROTOCOLS,
    SITES,
    descriptor,
    not_requested_status,
    os_disabled,
    protocol_request,
    publish_json,
    read_json,
    regional_coverage,
    union_discovery,
    verified_input,
)


def core_prefix(root, workflow):
    """Reject scoped/foreign jobs: enrichment is only for complete protocol/campaign cores."""
    base = workflow.rstrip("/") + "/"
    if not root.startswith(base) or not root.endswith("/enrichments/os"):
        raise ValueError("invalid enrichment namespace")
    core = root[: -len("/enrichments/os")]
    parts = core[len(base) :].split("/")
    if (
        len(parts) != 2
        or parts[0] not in {"continental", "analysis-jobs"}
        or not ID.fullmatch(parts[1])
    ):
        raise ValueError("enrichment requires a canonical core namespace")
    return core


def proof(client, uri, work):
    path = work / ("proof-" + hashlib.sha256(uri.encode()).hexdigest())
    client.download(uri, path)
    data = path.read_bytes()
    value = json.loads(data)
    return value, dict(uri=uri, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def intent(client, root, workflow, work):
    core = core_prefix(root, workflow)
    request = read_json(client, root + "/request.json", work)
    done, evidence = proof(client, core + "/done.json", work)
    if (
        request.get("version") != 1
        or request.get("status") != "requested"
        or request.get("core_prefix") != core
        or request.get("core_done_sha256") != evidence["sha256"]
        or not isinstance(request.get("vantage"), str)
        or not request["vantage"].strip()
        or done.get("os_status") != "not_requested"
    ):
        raise ValueError("late enrichment requires the pinned completed no-OS core")
    return core, request, evidence


def file_evidence(path):
    assert_complete(path)
    return dict(path=str(path), size_bytes=path.stat().st_size, sha256=file_digest(path))


def check_file(evidence):
    path = Path(evidence["path"])
    assert_complete(path)
    if path.stat().st_size != evidence["size_bytes"] or file_digest(path) != evidence["sha256"]:
        raise ValueError(f"core artifact changed: {path}")
    return path


def science_sources(manifest, processed, figures, status):
    from ipid_analysis.manifest import resolve
    from ipid_analysis.strategy_merge import iter_strategy_merges

    sources = list(iter_strategy_merges(manifest))
    connection = resolve(manifest, "tcp.ipid.connection.rt-based.base")
    if connection:
        sources.append(connection)
    completed_stages = {
        (stage.get("stage"), stage.get("target"))
        for stage in status.get("stages", [])
        if stage.get("status") == "completed"
    }
    result = []
    for source in sources:
        strategy = source.artifact_path(processed, "strategies")
        if not completion_path(strategy).is_file():
            raise ValueError(f"core strategies lack completion: {strategy}")
        item = dict(target=source.target, strategies=file_evidence(strategy), roles={})
        for role, kind in (("itdk", "itdk"), ("ripe_atlas", "ripe-atlas")):
            metadata = source.artifact_path(figures, kind + "-role-by-strategy", "json")
            joined = source.artifact_path(processed, kind + "-strategy-join")
            stage = "render_itdk_analysis" if role == "itdk" else "render_ripe_atlas_analysis"
            if (stage, source.target) in completed_stages:
                if not all(completion_path(p).is_file() for p in (metadata, joined)):
                    raise ValueError("completed core role analysis lacks committed artifacts")
                item["roles"][role] = dict(
                    metadata=file_evidence(metadata), joined=file_evidence(joined)
                )
        result.append(item)
    if not result:
        raise ValueError("no completed core strategy sources")
    return result


def collect_cores(client, core, workflow, work):
    from ipid_analysis.config import PROCESSED_STORAGE_DIR, REPORTS_STORAGE_DIR
    from ipid_analysis.s3_workflow import load_analysis_request, validate_analysis_manifest

    campaign = core.startswith(workflow.rstrip("/") + "/continental/")
    proofs, jobs = [], []
    if campaign:
        binding = descriptor(client, core, work)
        if not os_disabled(binding):
            raise ValueError("campaign OS policy is not disabled")
        not_requested_status(client, core, binding, work)
        for name in ("campaign.json", "os/not-requested.json"):
            _, evidence = proof(client, core + "/" + name, work)
            proofs.append(evidence)
        for site in SITES:
            final, evidence = proof(client, f"{core}/sites/{site}/final/done.json", work)
            if final.get("os_status") != "not_requested":
                raise ValueError("site core has not completed without OS")
            proofs.append(evidence)
            event, evidence = proof(client, f"{core}/sites/{site}/discovery.json", work)
            proofs.append(evidence)
            for protocol in PROTOCOLS:
                jobs.append(
                    (
                        f"{core}/sites/{site}",
                        event["zmap"][protocol],
                        f"continental/{binding['campaign_id']}/{site}",
                    )
                )
    else:
        jobs.append((workflow.rstrip("/"), core.rsplit("/", 1)[-1], ""))
    cores = []
    for prefix, job_id, scope in jobs:
        job = f"{prefix}/analysis-jobs/{job_id}"
        request = load_analysis_request(client, job + "/request.json", prefix, work)
        manifest = read_json(client, request.manifest_uri, work)
        validate_analysis_manifest(manifest, request)
        if request.os_status != "not_requested" or request.job_id != job_id:
            raise ValueError("core analysis OS intent/identity mismatch")
        if campaign:
            bound_request, bound_manifest = protocol_request(
                client, job + "/request.json", prefix, work
            )
            if bound_request != request or bound_manifest != manifest:
                raise ValueError("campaign protocol binding mismatch")
        done, evidence = proof(client, job + "/done.json", work)
        if (
            done.get("os_status") != "not_requested"
            or done.get("job_id") != job_id
            or done.get("manifest_uri") != request.manifest_uri
        ):
            raise ValueError("core analysis incomplete or manifest mismatch")
        proofs.append(evidence)
        status = read_json(client, job + "/postprocess-status.json", work)
        if (
            status.get("required_complete") is not True
            or status.get("optional", {}).get("os", {}).get(request.protocol) != "not_requested"
        ):
            raise ValueError("core scientific analysis is incomplete")
        for uri in (job + "/request.json", request.manifest_uri, job + "/postprocess-status.json"):
            _, evidence = proof(client, uri, work)
            proofs.append(evidence)
        policy_uri = job + "/os-policy.json"
        if client.exists(policy_uri):
            _, evidence = proof(client, policy_uri, work)
            proofs.append(evidence)
        processed = PROCESSED_STORAGE_DIR / scope if scope else PROCESSED_STORAGE_DIR
        figures = (REPORTS_STORAGE_DIR / scope if scope else REPORTS_STORAGE_DIR) / "figures"
        cores.append(
            dict(
                job_id=job_id,
                scope=scope,
                manifest=manifest,
                zmap_uri=f"{request.zmap_prefix.rstrip('/')}/{job_id}/zmap.pq",
                science=science_sources(manifest, processed, figures, status),
            )
        )
    return cores, proofs


def verify_pinned(client, prepared, work):
    for evidence in prepared["core_proofs"]:
        verified_input(client, evidence["uri"], evidence, work)
    for core in prepared["cores"]:
        for source in core["science"]:
            check_file(source["strategies"])
            for role in source["roles"].values():
                check_file(role["metadata"])
                check_file(role["joined"])


def prepare(client, root, workflow, work):
    core, _, core_proof = intent(client, root, workflow, work)
    if client.exists(root + "/done.json"):
        return
    if client.exists(root + "/inputs.json"):
        prepared = read_json(client, root + "/inputs.json", work)
        verify_pinned(client, prepared, work)
    else:
        cores, proofs = collect_cores(client, core, workflow, work)
        prepared = dict(
            version=1,
            cores=cores,
            core_proofs=[core_proof, *proofs],
            late_enrichment=True,
            core_os_status="not_requested",
        )
        publish_json(client, root + "/inputs.json", prepared, work)
    if core.startswith(workflow.rstrip("/") + "/continental/"):
        # Uses the identical 18-member reader and UNION DISTINCT producer.
        union_discovery(client, core, work, enrichment=True)
    else:
        from ipid_analysis.s3_workflow import measurement_protocol

        uri = prepared["cores"][0]["zmap_uri"]
        path = client.input_path(uri, work / "original-zmap.pq")
        assert_complete(path)
        rows = pq.read_metadata(path).num_rows
        if rows < 1:
            raise ValueError("empty original ZMap population")
        target = dict(
            uri=root + "/scan/targets.pq",
            size_bytes=path.stat().st_size,
            sha256=file_digest(path),
            rows=rows,
        )
        member = dict(
            protocol=measurement_protocol(prepared["cores"][0]["job_id"]),
            measurement_id=prepared["cores"][0]["job_id"],
            uri=uri,
            sha256=target["sha256"],
        )
        scan = dict(
            version=1,
            campaign_id=prepared["cores"][0]["job_id"],
            raw_root=root + "/raw",
            target=target,
            reference_zmap=member["measurement_id"],
            members=[member],
        )
        if client.exists(root + "/scan/request.json"):
            if read_json(client, root + "/scan/request.json", work) != scan:
                raise ValueError("original ZMap changed after enrichment request")
            verified_input(client, target["uri"], target, work / "target")
        else:
            client.upload(path, target["uri"])
            publish_json(client, root + "/scan/request.json", scan, work)


def verify_scan(client, root, request, work):
    scan = read_json(client, root + "/scan/request.json", work)
    done = read_json(client, root + "/scan/done.json", work)
    if (
        done.get("version") != 1
        or done.get("late_enrichment") is not True
        or done.get("campaign_id") != scan.get("campaign_id")
        or done.get("target") != scan.get("target")
        or done.get("members") != scan.get("members")
        or done.get("vantage") != request["vantage"]
        or done.get("raw_prefix") != root + "/raw"
        or set(done.get("files", {}))
        != {"os.pq", "os-observations.pq", "os-coverage.json", "os.snapshot.yaml"}
        or not re.fullmatch(
            r"tcp-80_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", done.get("measurement_id", "")
        )
    ):
        raise ValueError("late OS completion/provenance mismatch")
    for field in ("started_at", "completed_at"):
        if datetime.fromisoformat(done[field].replace("Z", "+00:00")).tzinfo is None:
            raise ValueError("OS enrichment requires actual UTC scan times")
    if datetime.fromisoformat(
        done["completed_at"].replace("Z", "+00:00")
    ) < datetime.fromisoformat(done["started_at"].replace("Z", "+00:00")):
        raise ValueError("OS completion precedes scan start")
    raw = work / "raw"
    output = raw / "os" / done["measurement_id"]
    for name, evidence in done["files"].items():
        uri = root + "/raw/" + name
        path = verified_input(client, uri, evidence, output)
        # input_path may return a mounted path; renderers need one coherent directory.
        if path != output / name:
            client.download(uri, output / name)
            if file_digest(output / name) != evidence["sha256"]:
                raise ValueError("OS local materialization integrity mismatch")
    targets = verified_input(client, scan["target"]["uri"], scan["target"], work / "targets")
    from ipid_analysis.runtime import connect

    with connect() as con:
        con.read_parquet(str(output / "os-observations.pq")).create_view("observations")
        con.read_parquet(str(targets)).create_view("targets")
        rows, distinct, invalid = con.execute(
            "SELECT count(*),count(DISTINCT IP_ADDR),count(*) FILTER(WHERE RESPONSE_MASK IS NULL OR RESPONSE_MASK NOT BETWEEN 0 AND 63 OR EVIDENCE_MASK IS NULL OR EVIDENCE_MASK NOT BETWEEN 0 AND 63 OR TAG_MASK IS NULL OR TAG_MASK NOT BETWEEN 0 AND 63 OR OS_STATUS IS NULL OR OS_STATUS NOT IN ('resolved','ambiguous','unclassified','no-evidence') OR (EVIDENCE_MASK=0)<>(OS_STATUS='no-evidence')) FROM observations"
        ).fetchone()
        missing = con.execute(
            "SELECT count(*) FROM targets ANTI JOIN observations USING(IP_ADDR)"
        ).fetchone()[0]
        target_distinct = con.execute("SELECT count(DISTINCT IP_ADDR) FROM targets").fetchone()[0]
        declared_rows = pq.read_metadata(targets).num_rows
        if (
            declared_rows != scan["target"]["rows"]
            or rows != distinct
            or rows != target_distinct
            or missing
            or invalid
        ):
            raise ValueError("OS observations do not exactly cover discovery targets")
    return raw, done


def render_core(core, raw, scan_done, processed, figures, threads):
    """Only OS producers; no classifier, merger, non-OS renderer or network fetch."""
    from ipid_analysis.caida_itdk import ITDKDataset
    from ipid_analysis.manifest import resolve
    from ipid_analysis.plot_itdk_os import render_itdk_os_analysis
    from ipid_analysis.plot_os_group_strategy import render, render_measurement
    from ipid_analysis.plot_ripe_atlas_os import render_ripe_atlas_os_analysis
    from ipid_analysis.ripe_atlas import RipeAtlasDataset, RipeWindow
    from ipid_analysis.strategy_merge import iter_strategy_merges

    sources = {s.target: s for s in iter_strategy_merges(core["manifest"])}
    connection = resolve(core["manifest"], "tcp.ipid.connection.rt-based.base")
    if connection:
        sources[connection.target] = connection
    stages = []
    for item in core["science"]:
        source = sources[item["target"]]
        # Existing science is read only; the output roots are enrichment-specific.
        from ipid_analysis.config import PROCESSED_STORAGE_DIR

        source_root = (
            PROCESSED_STORAGE_DIR / core["scope"] if core["scope"] else PROCESSED_STORAGE_DIR
        )
        kwargs = dict(
            strategy_root=source_root,
            processed_root=processed,
            figures_root=figures,
            raw_root=raw,
            threads=threads,
        )
        renderer = render_measurement if source == connection else render
        renderer(source, scan_done["measurement_id"], **kwargs)
        for role in ("itdk", "ripe_atlas"):
            pinned = item["roles"].get(role)
            if pinned is None:
                stages.append(
                    dict(target=source.target, stage=role + "_os", status="not_available_in_core")
                )
                continue
            metadata = json.loads(check_file(pinned["metadata"]).read_text(encoding="utf-8"))[role]
            joined = check_file(pinned["joined"])
            if role == "itdk":
                dataset = ITDKDataset(
                    metadata["release"],
                    metadata["topology"],
                    joined.parent,
                    joined,
                    joined,
                    Path(metadata["metadata"]),
                    joined,
                )
                render_itdk_os_analysis(source, dataset, scan_done["measurement_id"], **kwargs)
            else:
                window = RipeWindow(
                    datetime.fromisoformat(metadata["window_start"]),
                    datetime.fromisoformat(metadata["window_end"]),
                )
                dataset = RipeAtlasDataset(
                    metadata["source"], window, joined.parent, joined, Path(metadata["metadata"])
                )
                render_ripe_atlas_os_analysis(
                    source,
                    dataset,
                    scan_done["measurement_id"],
                    role_matches_path=joined,
                    **kwargs,
                )
            stages.append(dict(target=source.target, stage=role + "_os", status="completed"))
    return stages


def analyze(client, root, workflow, work, threads):
    from ipid_analysis.config import PROCESSED_STORAGE_DIR, REPORTS_STORAGE_DIR

    core, request, _ = intent(client, root, workflow, work)
    if client.exists(root + "/done.json"):
        return
    prepare(client, root, workflow, work)  # Revalidate committed discovery membership/targets.
    prepared = read_json(client, root + "/inputs.json", work)
    verify_pinned(client, prepared, work)
    raw, scan_done = verify_scan(client, root, request, work)
    identity = core[len(workflow.rstrip("/")) + 1 :]
    completed = []
    for entry in prepared["cores"]:
        relative = (
            Path("enrichments/os")
            / identity
            / (entry["scope"].rsplit("/", 1)[-1] if entry["scope"] else "protocol")
            / entry["job_id"]
        )
        processed = PROCESSED_STORAGE_DIR / relative
        figures = REPORTS_STORAGE_DIR / relative / "figures"
        job = (
            root
            + "/analyses/"
            + (entry["scope"].rsplit("/", 1)[-1] + "/" if entry["scope"] else "")
            + entry["job_id"]
        )
        if client.exists(job + "/done.json"):
            value = read_json(client, job + "/done.json", work)
            if (
                value.get("version") != 1
                or value.get("job_id") != entry["job_id"]
                or value.get("scope") != entry["scope"]
                or value.get("late_enrichment") is not True
                or value.get("scan_done") != scan_done
                or not value.get("artifacts")
            ):
                raise ValueError("enrichment analysis completion binding mismatch")
            for evidence in value["artifacts"]:
                check_file(evidence)
            completed.append(value)
            continue
        stages = render_core(entry, raw, scan_done, processed, figures, threads)
        zmap = client.input_path(entry["zmap_uri"], work / entry["job_id"] / "zmap.pq")
        regional_coverage(
            zmap,
            raw / "os" / scan_done["measurement_id"] / "os-observations.pq",
            processed / "os-coverage.json",
            provenance=dict(late_enrichment=True, core_os_status="not_requested", scan=scan_done),
        )
        artifacts = [
            file_evidence(p)
            for directory in (processed, figures)
            for p in directory.rglob("*")
            if p.is_file() and ".ipid-complete" not in p.parts
        ]
        if not artifacts or any(not completion_path(Path(a["path"])).is_file() for a in artifacts):
            raise ValueError("OS enrichment outputs lack completion markers")
        value = dict(
            version=1,
            job_id=entry["job_id"],
            scope=entry["scope"],
            artifacts=artifacts,
            stages=stages,
            late_enrichment=True,
            scan_done=scan_done,
        )
        publish_json(client, job + "/done.json", value, work)
        completed.append(value)
    verify_pinned(client, prepared, work)
    publish_json(
        client,
        root + "/done.json",
        dict(
            version=1,
            status="completed",
            core_os_status="not_requested",
            late_enrichment=True,
            request=request,
            completed_at=datetime.now(timezone.utc).isoformat(),
            analyses=completed,
            core_proofs=prepared["core_proofs"],
            scan=scan_done,
        ),
        work,
    )


def tasks(objects, workflow):
    from ipid_analysis.scheduler import Task

    result = []
    for uri in sorted(objects):
        if not uri.endswith("/enrichments/os/request.json"):
            continue
        root = uri.rsplit("/", 1)[0]
        try:
            core = core_prefix(root, workflow)
        except ValueError:
            continue
        if core + "/done.json" not in objects or any(
            root + "/" + s in objects for s in ("done.json", "failed.json", "scan/failed.json")
        ):
            continue
        if root + "/scan/request.json" not in objects:
            result.append(Task("os-enrichment-prepare", uri, workflow, priority=2))
        elif root + "/scan/done.json" in objects:
            result.append(Task("os-enrichment-analysis", uri, workflow, priority=10))
    return result


def retry(client, uri, workflow, work):
    if uri.endswith("/enrichments/os/scan/request.json"):
        root = uri[: -len("/scan/request.json")]
        stage = root + "/scan"
    elif uri.endswith("/enrichments/os/request.json"):
        root = uri[: -len("/request.json")]
        stage = root
    else:
        return False
    intent(client, root, workflow, work)
    if (
        client.exists(root + "/done.json")
        or client.exists(stage + "/done.json")
        or not client.exists(stage + "/failed.json")
    ):
        raise ValueError("retry requires enrichment failure without success")
    read_json(client, uri, work)
    failure = work / "failed.json"
    client.download(stage + "/failed.json", failure)
    import time

    client.upload(failure, stage + "/attempts/" + str(time.time_ns()) + "/failed.json")
    client.delete_failure(stage + "/failed.json")
    return True


def status(client, root):
    """Terminal markers dominate the informational, immutable running event."""
    for marker, state in (
        ("done.json", "completed"),
        ("failed.json", "failed"),
        ("scan/failed.json", "failed"),
        ("running.json", "running"),
        ("request.json", "requested"),
    ):
        if client.exists(root + "/" + marker):
            return state
    return "not_requested"


def main():
    import argparse
    import os

    from ipid_analysis.s3_workflow import S3Client

    parser = argparse.ArgumentParser(description="Read the independent late OS enrichment status")
    parser.add_argument("--s3-prefix", default=os.environ.get("IPID_ANALYSIS_S3_PREFIX"))
    identity = parser.add_mutually_exclusive_group(required=True)
    identity.add_argument("--campaign")
    identity.add_argument("--zmap")
    args = parser.parse_args()
    job_id = args.campaign or args.zmap
    if not args.s3_prefix or not args.s3_prefix.startswith("s3://") or not ID.fullmatch(job_id):
        parser.error("valid S3 workflow prefix and campaign/ZMap identity required")
    core = (
        args.s3_prefix.rstrip("/")
        + ("/continental/" if args.campaign else "/analysis-jobs/")
        + job_id
    )
    print(
        json.dumps(
            dict(core_prefix=core, enrichment_status=status(S3Client(), core + "/enrichments/os"))
        )
    )


if __name__ == "__main__":
    main()
