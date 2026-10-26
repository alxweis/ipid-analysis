"""Control-plane stages for six-site campaigns, reusing the normal producers."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re

import pyarrow.parquet as pq

from ipid_analysis.artifacts import assert_complete, atomic_output, file_digest, fingerprints
from ipid_analysis.config import PROCESSED_DATA_DIR, RAW_DATA_DIR, RAW_STORAGE_DIR
from ipid_analysis.runtime import connect, exclusive_lock

SITES = ("eu", "as", "af", "na", "sa", "au")
PROTOCOLS = ("icmp", "tcp", "udp-dns")
ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}")


def campaign_parts(uri, prefix):
    if not uri.startswith(prefix.rstrip("/") + "/continental/"):
        return None
    parts = uri[len(prefix.rstrip("/")) + 1 :].split("/")
    if len(parts) < 3 or not ID.fullmatch(parts[1]):
        return None
    if parts[2] == "sites":
        if len(parts) < 5 or parts[3] not in SITES:
            return None
        return parts[1], parts[3], parts[4:]
    return parts[1], None, parts[2:]


def read_json(client, uri, work):
    path = work / ("json-" + hashlib.sha256(uri.encode()).hexdigest() + ".json")
    client.download(uri, path)
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected an object: {uri}")
    return value


def publish_json(client, uri, value, work, *, immutable=True):
    from ipid_analysis.s3_workflow import write_json

    if immutable and client.exists(uri):
        if read_json(client, uri, work) != value:
            raise ValueError(f"immutable control object differs: {uri}")
        return
    path = work / ("publish-" + hashlib.sha256(uri.encode()).hexdigest() + ".json")
    write_json(path, value)
    client.upload(path, uri)


def os_disabled(binding):
    mode = binding.get("os_mode", "enabled")
    if mode not in ("", "enabled", "disabled"):
        raise ValueError("invalid campaign OS policy")
    return mode == "disabled"


def not_requested_status(client, campaign_prefix, binding, work):
    if not os_disabled(binding):
        raise ValueError("campaign has not disabled OS")
    value = read_json(client, campaign_prefix + "/os/not-requested.json", work)
    expected = dict(version=1, campaign_id=binding["campaign_id"], status="not_requested")
    if value != expected:
        raise ValueError("invalid not-requested OS status binding")
    return value


def descriptor(client, campaign_prefix, work):
    value = read_json(client, campaign_prefix + "/campaign.json", work)
    campaign = campaign_prefix.rsplit("/", 1)[-1]
    roots = json.loads(os.environ.get("IPID_ANALYSIS_S3_ROOTS", "{}"))
    raw = value.get("raw_root", "")
    if (
        value.get("version") != 1
        or value.get("campaign_id") != campaign
        or value.get("sites") != list(SITES)
        or value.get("protocols") != list(PROTOCOLS)
        or not isinstance(raw, str)
        or not raw.startswith("s3://")
        or (roots.get("raw") and raw.rstrip("/") != roots["raw"].rstrip("/"))
    ):
        raise ValueError("invalid campaign descriptor or raw-root binding")
    os_disabled(value)
    return value


def verified_input(client, uri, evidence, work):
    if (
        not isinstance(evidence, dict)
        or not isinstance(evidence.get("size_bytes"), int)
        or evidence["size_bytes"] < 0
        or not re.fullmatch(r"[0-9a-f]{64}", evidence.get("sha256", ""))
    ):
        raise ValueError(f"invalid integrity evidence: {uri}")
    path = client.input_path(uri, work / uri.rsplit("/", 1)[-1])
    assert_complete(path)
    if path.stat().st_size != evidence["size_bytes"] or file_digest(path) != evidence["sha256"]:
        raise ValueError(f"input integrity mismatch: {uri}")
    return path


def source_measurement(client, prefix, measurement_id, filename, work):
    from ipid_analysis.s3_workflow import JOB_ID_RE

    if not JOB_ID_RE.fullmatch(measurement_id):
        raise ValueError("invalid source measurement ID")
    marker_uri = f"{prefix.rstrip('/')}/{measurement_id}/measurement-complete.json"
    marker = read_json(client, marker_uri, work)
    if marker.get("version") != 1 or marker.get("measurement_id") != measurement_id:
        raise ValueError("source completion marker mismatch")
    evidence = marker.get("files", {}).get(filename)
    uri = f"{prefix.rstrip('/')}/{measurement_id}/{filename}"
    return verified_input(client, uri, evidence, work), evidence


def union_discovery(client, campaign_prefix, work):
    """UNION DISTINCT all 18 immutable discoveries; OS request is published last."""
    binding = descriptor(client, campaign_prefix, work)
    if os_disabled(binding):
        raise ValueError("OS not requested; discovery union/OS dispatch is disabled")
    campaign = binding["campaign_id"]
    members, inputs = [], []
    reference = None
    for site in SITES:
        event = read_json(client, f"{campaign_prefix}/sites/{site}/discovery.json", work)
        raw = f"{binding['raw_root'].rstrip('/')}/continental/{campaign}/{site}/zmap"
        if (
            event.get("version") != 1
            or event.get("campaign_id") != campaign
            or event.get("site") != site
            or event.get("raw_prefix", "").rstrip("/") != raw
            or set(event.get("zmap", {})) != set(PROTOCOLS)
        ):
            raise ValueError(f"invalid discovery binding for {site}")
        for protocol in PROTOCOLS:
            measurement_id = event["zmap"][protocol]
            from ipid_analysis.s3_workflow import measurement_protocol

            if measurement_protocol(measurement_id) != protocol:
                raise ValueError("discovery protocol mismatch")
            local = work / site / protocol
            local.mkdir(parents=True, exist_ok=True)
            path, evidence = source_measurement(client, raw, measurement_id, "zmap.pq", local)
            inputs.append(path)
            members.append(
                dict(
                    site=site,
                    protocol=protocol,
                    measurement_id=measurement_id,
                    uri=f"{raw}/{measurement_id}/zmap.pq",
                    sha256=evidence["sha256"],
                )
            )
            if site == "eu" and protocol == "tcp":
                reference = measurement_id
    if client.exists(campaign_prefix + "/os/request.json"):
        previous = read_json(client, campaign_prefix + "/os/request.json", work)
        if (
            previous.get("campaign_id") != campaign
            or previous.get("members") != members
            or previous.get("raw_root") != binding["raw_root"]
        ):
            raise ValueError("existing OS request has different sources")
        verified_input(
            client, previous["target"]["uri"], previous["target"], work / "adopted-union"
        )
        return
    output = work / "targets.pq"
    with connect() as con:
        invalid = con.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE IP_ADDR IS NULL OR NOT regexp_full_match(IP_ADDR, '[0-9]{1,3}(\\.[0-9]{1,3}){3}')",
            [[str(p) for p in inputs]],
        ).fetchone()[0]
        if invalid:
            raise ValueError("invalid discovery addresses")
        # Parameterized paths and a closed local file avoid FUSE random writes.
        with atomic_output(
            output,
            inputs=inputs,
            parameters={"operation": "six-site-discovery-union", "members": members},
        ) as temporary:
            con.read_parquet([str(p) for p in inputs]).project(
                "CAST(IP_ADDR AS VARCHAR) AS IP_ADDR, CAST('' AS VARCHAR) AS REPLY_TYPE"
            ).distinct().write_parquet(str(temporary), compression="zstd")
        rows = pq.read_metadata(output).num_rows
    if rows == 0:
        raise ValueError("empty discovery union; OS scan not started")
    target = dict(
        uri=campaign_prefix + "/os/targets.pq",
        size_bytes=output.stat().st_size,
        sha256=file_digest(output),
        rows=rows,
    )
    client.upload(output, target["uri"])
    request = dict(
        version=1,
        campaign_id=campaign,
        target=target,
        raw_root=binding["raw_root"],
        reference_zmap=reference,
        members=members,
    )
    publish_json(client, campaign_prefix + "/os/request.json", request, work)


def protocol_request(client, uri, site_prefix, work):
    from ipid_analysis.s3_workflow import AnalysisRequest, validate_analysis_manifest

    request = AnalysisRequest.parse(read_json(client, uri, work), site_prefix)
    expected_id = uri.rsplit("/", 2)[-2]
    if request.job_id != expected_id:
        raise ValueError("strategy event ID mismatch")
    parent = site_prefix.rsplit("/sites/", 1)[0]
    site = site_prefix.rsplit("/", 1)[-1]
    binding = descriptor(client, parent, work)
    raw = binding["raw_root"].rstrip("/") + f"/continental/{binding['campaign_id']}"
    if (
        request.zmap_prefix.rstrip("/") != f"{raw}/{site}/zmap"
        or request.ipid_prefix.rstrip("/") != f"{raw}/{site}/ipid"
        or request.os_prefix.rstrip("/") != ("" if os_disabled(binding) else raw + "/os")
    ):
        raise ValueError("protocol event crosses campaign/site boundaries")
    manifest = read_json(client, request.manifest_uri, work)
    validate_analysis_manifest(manifest, request)
    if os_disabled(binding) != (request.os_status == "not_requested"):
        raise ValueError("campaign/protocol OS intent mismatch")
    if not os_disabled(binding) and manifest[request.protocol].get("os") != "shared-os":
        raise ValueError("continental manifests require shared-os")
    return request, manifest


def prepare_strategies(client, uri, site_prefix, work, batch_size, threads):
    from ipid_analysis.manifest import iter_ipid_measurements
    from ipid_analysis.strategies import classify_measurement
    from ipid_analysis.strategy_merge import iter_strategy_merges, merge_strategies

    request, manifest = protocol_request(client, uri, site_prefix, work)
    ready_uri = request.manifest_uri.rsplit("/", 1)[0] + "/strategies-ready.json"
    if client.exists(ready_uri):
        ready = validate_strategy_ready(
            client, request.manifest_uri, manifest, PROCESSED_DATA_DIR, work
        )
        publish_json(client, uri.rsplit("/", 1)[0] + "/done.json", ready, work)
        return
    # Only the canonical RT and Mass merge is needed to unlock interprotocol
    # target selection. Fixed-base/connection plots remain background work.
    merges = iter_strategy_merges(manifest)
    if len(merges) != 1:
        raise ValueError("expected exactly one stateless strategy merge")
    required = {merges[0].base.measurement_id, merges[0].mass.measurement_id}
    for measurement in iter_ipid_measurements(manifest):
        if measurement.measurement_id not in required:
            continue
        local = RAW_DATA_DIR / "ipid" / measurement.measurement_id
        for name in ("ipid.pq", "ipid.snapshot.yaml", "strategies.pq"):
            client.download(
                f"{request.ipid_prefix.rstrip('/')}/{measurement.measurement_id}/{name}",
                local / name,
            )
        classify_measurement(measurement, batch_size=batch_size, threads=threads)
    path, stats = merge_strategies(merges[0], batch_size=batch_size, threads=threads)
    assert_complete(path)
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
    publish_json(client, uri.rsplit("/", 1)[0] + "/done.json", ready, work)


def validate_strategy_ready(client, manifest_uri, manifest, processed_root, work):
    from ipid_analysis.strategy_merge import iter_strategy_merges

    ready = read_json(client, manifest_uri.rsplit("/", 1)[0] + "/strategies-ready.json", work)
    merges = iter_strategy_merges(manifest)
    if len(merges) != 1:
        raise ValueError("invalid preliminary merge manifest")
    path = merges[0].artifact_path(processed_root, "strategies")
    assert_complete(path)
    if (
        ready.get("version") != 1
        or ready.get("manifest") != manifest
        or ready.get("manifest_uri") != manifest_uri
        or path.stat().st_size != ready.get("size_bytes")
        or file_digest(path) != ready.get("sha256")
    ):
        raise ValueError("preliminary strategies no longer match readiness marker")
    return ready


def verify_shared_os(client, campaign_prefix, work):
    binding = descriptor(client, campaign_prefix, work)
    if os_disabled(binding):
        raise ValueError("OS was not requested; there is no shared OS dataset to verify")
    request = read_json(client, campaign_prefix + "/os/request.json", work)
    done = read_json(client, campaign_prefix + "/os/done.json", work)
    expected = {"os.pq", "os-observations.pq", "os-coverage.json", "os.snapshot.yaml"}
    if (
        request.get("version") != 1
        or request.get("campaign_id") != binding["campaign_id"]
        or request.get("raw_root") != binding["raw_root"]
        or request.get("target", {}).get("uri") != campaign_prefix + "/os/targets.pq"
    ):
        raise ValueError("invalid shared OS request binding")
    if (
        done.get("version") != 1
        or done.get("campaign_id") != binding["campaign_id"]
        or done.get("target") != request.get("target")
        or done.get("members") != request.get("members")
        or len(done.get("members", [])) != 18
        or set(done.get("files", {})) != expected
        or done.get("vantage") != "shared-os-vm"
    ):
        raise ValueError("shared OS completion/provenance mismatch")
    raw_prefix = (
        binding["raw_root"].rstrip("/") + f"/continental/{binding['campaign_id']}/os/shared-os"
    )
    # One shared local cache in compatibility mode; production reads the one
    # mounted object in place. Hashing is locked/cached by source fingerprint.
    root = RAW_STORAGE_DIR / "continental" / binding["campaign_id"] / "os" / "shared-os"
    for name, evidence in done["files"].items():
        uri = raw_prefix + "/" + name
        mapped = client.mapped_path(uri)
        path = mapped or root / name
        if mapped is None:
            client.download(uri, path)
        else:
            path = client.input_path(uri, path)
        with exclusive_lock("shared-os-integrity:" + uri):
            cache_root = (
                Path(os.environ.get("IPID_ANALYSIS_CONTROL_ROOT", str(work.parent)))
                / "shared-os-integrity"
                / hashlib.sha256(uri.encode()).hexdigest()
            )
            cache_root.mkdir(parents=True, exist_ok=True)
            cache = cache_root / "verified.json"
            current = dict(source=fingerprints([path]), evidence=evidence)
            previous = json.loads(cache.read_text()) if cache.is_file() else None
            if current != previous:
                if (
                    path.stat().st_size != evidence["size_bytes"]
                    or file_digest(path) != evidence["sha256"]
                ):
                    raise ValueError(f"shared OS integrity mismatch: {name}")
                cache.write_text(json.dumps(current), encoding="utf-8")
    os.environ["IPID_ANALYSIS_SHARED_OS_DIR"] = str(root)
    observations = root / "os-observations.pq"
    validation = dict(target=done["target"], observations=fingerprints([observations]))
    control = Path(os.environ.get("IPID_ANALYSIS_CONTROL_ROOT", str(work.parent)))
    cache = (
        control
        / "shared-os-validation"
        / (hashlib.sha256(campaign_prefix.encode()).hexdigest() + ".json")
    )
    with exclusive_lock("shared-os-union:" + campaign_prefix):
        previous = json.loads(cache.read_text()) if cache.is_file() else None
        if previous != validation:
            union = verified_input(client, done["target"]["uri"], done["target"], work / "union")
            with connect() as con:
                con.read_parquet(str(observations)).create_view("observations")
                rows, distinct, invalid = con.execute(
                    "SELECT count(*),count(DISTINCT IP_ADDR),count(*) FILTER(WHERE RESPONSE_MASK IS NULL OR RESPONSE_MASK NOT BETWEEN 0 AND 63 OR EVIDENCE_MASK IS NULL OR EVIDENCE_MASK NOT BETWEEN 0 AND 63 OR TAG_MASK IS NULL OR TAG_MASK NOT BETWEEN 0 AND 63 OR OS_STATUS IS NULL OR OS_STATUS NOT IN ('resolved','ambiguous','unclassified','no-evidence') OR (EVIDENCE_MASK=0)<>(OS_STATUS='no-evidence')) FROM observations"
                ).fetchone()
                if rows != done["target"]["rows"] or rows != distinct or invalid:
                    raise ValueError("invalid or incomplete OS observations")
                con.read_parquet(str(union)).create_view("targets")
                missing = con.execute(
                    "SELECT count(*) FROM targets ANTI JOIN observations USING(IP_ADDR)"
                ).fetchone()[0]
                if missing:
                    raise ValueError("OS observations do not match discovery union")
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(json.dumps(validation), encoding="utf-8")
    return root, done


def regional_coverage(zmap: Path, observations: Path, output: Path, *, provenance: dict):
    """Reconstruct legacy coverage counts from exactly this protocol/site set."""
    with connect() as con:
        con.execute(
            "CREATE TEMP TABLE targets AS SELECT DISTINCT IP_ADDR FROM read_parquet(?)",
            [str(zmap)],
        )
        con.read_parquet(str(observations)).create_view("observations")
        missing = con.execute(
            "SELECT count(*) FROM targets t ANTI JOIN observations o USING(IP_ADDR)"
        ).fetchone()[0]
        if missing:
            raise ValueError(f"shared OS scan is missing {missing} regional targets")
        con.execute(
            "CREATE TEMP TABLE regional AS SELECT o.* FROM observations o JOIN targets USING(IP_ADDR)"
        )
        total, response, evidence, tagged, resolved, ambiguous, unclassified = con.execute(
            "SELECT count(*), count(*) FILTER(WHERE RESPONSE_MASK<>0), count(*) FILTER(WHERE EVIDENCE_MASK<>0),count(*) FILTER(WHERE TAG_MASK<>0),count(*) FILTER(WHERE OS_STATUS='resolved'),count(*) FILTER(WHERE OS_STATUS='ambiguous'),count(*) FILTER(WHERE OS_STATUS='unclassified') FROM regional"
        ).fetchone()

        def rate(n, d):
            return n / d if d else 0.0

        services, service_overview = {}, {}
        for index, service in enumerate(("ssh", "smb", "http", "https", "snmp", "dns")):
            bit = 1 << index
            responded, ev, tag = con.execute(
                f"SELECT count(*) FILTER(WHERE (RESPONSE_MASK & {bit})<>0),count(*) FILTER(WHERE (EVIDENCE_MASK & {bit})<>0),count(*) FILTER(WHERE (TAG_MASK & {bit})<>0) FROM regional"
            ).fetchone()
            services[service] = dict(attempted=total, responded=responded, evidence=ev, tagged=tag)
            service_overview[service] = dict(
                **services[service],
                responded_coverage=rate(responded, total),
                evidence_coverage=rate(ev, total),
                tagged_coverage=rate(tag, total),
                evidence_rate_among_responses=rate(ev, responded),
                tagged_rate_among_evidence=rate(tag, ev),
            )
        conflicts = dict(
            con.execute(
                "SELECT CONFLICT,count(*) FROM regional WHERE OS_STATUS='ambiguous' GROUP BY CONFLICT"
            ).fetchall()
        )
    source = observations.with_name("os-coverage.json")
    versions = json.loads(source.read_text()) if source.is_file() else {}
    value = dict(
        schema_version=versions.get("schema_version", "1"),
        classifier_version=versions.get("classifier_version", "unknown"),
        provenance=provenance,
        overview=dict(
            total_targets=total,
            responded_targets=response,
            responded_coverage=rate(response, total),
            evidence_targets=evidence,
            evidence_coverage=rate(evidence, total),
            tagged_targets=tagged,
            tagged_coverage=rate(tagged, total),
            resolved_targets=resolved,
            resolved_coverage=rate(resolved, total),
            ambiguous_targets=ambiguous,
            ambiguous_coverage=rate(ambiguous, total),
            unclassified_targets=unclassified,
            unclassified_coverage=rate(unclassified, total),
            resolved_rate_among_evidence=rate(resolved, evidence),
            ambiguous_rate_among_evidence=rate(ambiguous, evidence),
            unclassified_rate_among_evidence=rate(unclassified, evidence),
            services=service_overview,
        ),
        detail=dict(
            targets=dict(
                total=total,
                with_response=response,
                without_response=total - response,
                with_evidence=evidence,
                without_evidence=total - evidence,
                with_tag=tagged,
                without_tag=total - tagged,
            ),
            classification=dict(resolved=resolved, ambiguous=ambiguous, unclassified=unclassified),
            services=services,
            conflicts=conflicts,
        ),
    )
    with atomic_output(output, inputs=[zmap, observations], parameters=provenance) as temporary:
        temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    return value


def submit_site(client, site_prefix, work):
    campaign_prefix = site_prefix.rsplit("/sites/", 1)[0]
    final = read_json(client, site_prefix + "/final/request.json", work)
    if final != dict(
        version=1,
        campaign_id=campaign_prefix.rsplit("/", 1)[-1],
        site=site_prefix.rsplit("/", 1)[-1],
        interprotocol_job="interprotocol",
    ):
        raise ValueError("invalid final site completion event")
    binding = descriptor(client, campaign_prefix, work)
    disabled = os_disabled(binding)
    if disabled:
        not_requested_status(client, campaign_prefix, binding, work)
        shared = done = None
        discovery = read_json(client, site_prefix + "/discovery.json", work)
        if (
            discovery.get("version") != 1
            or discovery.get("campaign_id") != binding["campaign_id"]
            or discovery.get("site") != site_prefix.rsplit("/", 1)[-1]
            or set(discovery.get("zmap", {})) != set(PROTOCOLS)
        ):
            raise ValueError("invalid site discovery binding")
    else:
        shared, done = verify_shared_os(client, campaign_prefix, work)

    events = [
        u for u in client.objects(site_prefix + "/strategy-jobs") if u.endswith("/request.json")
    ]
    requests = [protocol_request(client, u, site_prefix, work)[0] for u in events]
    if len(requests) != 3 or {r.protocol for r in requests} != set(PROTOCOLS):
        raise ValueError("site is missing completed protocol workflows")
    for request in requests:
        if disabled:
            if discovery["zmap"].get(request.protocol) != request.job_id or discovery.get(
                "raw_prefix", ""
            ).rstrip("/") != request.zmap_prefix.rstrip("/"):
                raise ValueError("protocol does not match site discovery")
            source_measurement(
                client, request.zmap_prefix, request.job_id, "zmap.pq", work / request.job_id
            )
        else:
            members = [
                m
                for m in done["members"]
                if m["site"] == site_prefix.rsplit("/", 1)[-1]
                and m["protocol"] == request.protocol
                and m["measurement_id"] == request.job_id
            ]
            if len(members) != 1:
                raise ValueError("protocol discovery is not a shared OS source")
            zmap, evidence = source_measurement(
                client, request.zmap_prefix, request.job_id, "zmap.pq", work / request.job_id
            )
            if evidence["sha256"] != members[0]["sha256"]:
                raise ValueError("protocol discovery differs from shared OS source")
            provenance = dict(
                campaign_id=done["campaign_id"],
                site=site_prefix.rsplit("/", 1)[-1],
                protocol=request.protocol,
                os_vantage=done["vantage"],
                os_measurement_id=done["measurement_id"],
                union=done["target"],
            )
            coverage = regional_coverage(
                zmap,
                shared / "os-observations.pq",
                PROCESSED_DATA_DIR / request.job_id / "shared-os-coverage.json",
                provenance=provenance,
            )
            if coverage["detail"]["targets"]["total"] < 1:
                raise ValueError("empty site/protocol discovery")
        publish_json(
            client, request.done_uri.rsplit("/", 1)[0] + "/request.json", request.payload(), work
        )
    submitted = dict(
        version=1, campaign_id=binding["campaign_id"], requests=[r.payload() for r in requests]
    )
    if disabled:
        submitted["os_status"] = "not_requested"
    else:
        submitted["shared_os"] = done
    publish_json(client, site_prefix + "/final/submitted.json", submitted, work)


def finish_site(client, site_prefix, work):
    submitted = read_json(client, site_prefix + "/final/submitted.json", work)
    if len(submitted.get("requests", [])) != 3 or {
        r["protocol"] for r in submitted["requests"]
    } != set(PROTOCOLS):
        raise ValueError("invalid submitted site analysis inventory")
    binding = descriptor(client, site_prefix.rsplit("/sites/", 1)[0], work)
    disabled = os_disabled(binding)
    if disabled:
        not_requested_status(client, site_prefix.rsplit("/sites/", 1)[0], binding, work)
        if submitted.get("os_status") != "not_requested" or "shared_os" in submitted:
            raise ValueError("site completion OS policy mismatch")
    elif submitted.get("os_status") == "not_requested" or "shared_os" not in submitted:
        raise ValueError("site completion lost requested OS dependency")
    for request in submitted["requests"]:
        if not client.exists(request["done_uri"]):
            raise ValueError("site analysis dependency is incomplete")
        done = read_json(client, request["done_uri"], work)
        if not disabled and (
            request.get("os_status") == "not_requested" or done.get("os_status") == "not_requested"
        ):
            raise ValueError("site analysis OS intent mismatch")
        if disabled and (
            request.get("os_status") != "not_requested" or done.get("os_status") != "not_requested"
        ):
            raise ValueError("site analysis lost not-requested OS state")
        if (
            done.get("job_id") != request["job_id"]
            or done.get("manifest_uri") != request["manifest_uri"]
        ):
            raise ValueError("site analysis completion binding mismatch")
    if not client.exists(site_prefix + "/interprotocol-jobs/interprotocol/done.json"):
        raise ValueError("interprotocol analysis dependency is incomplete")
    publish_json(client, site_prefix + "/final/done.json", submitted, work)


def failure_uri(task):
    if task.kind == "union":
        return task.uri.rsplit("/", 1)[0] + "/failed.json"
    if task.kind in {"submit", "site-done"}:
        return task.uri.rsplit("/", 1)[0] + "/failed.json"
    if task.kind == "campaign-done":
        return task.uri.rsplit("/", 1)[0] + "/failed.json"
    return task.uri.rsplit("/", 1)[0] + "/failed.json"


def success_uri(task):
    job = task.uri.rsplit("/", 1)[0]
    if task.kind == "union":
        return task.uri.rsplit("/union/", 1)[0] + "/os/request.json"
    if task.kind == "submit":
        return job + "/submitted.json"
    return job + "/done.json"


def publish_task_failure(client, task, message, work):
    uri = failure_uri(task)
    success = success_uri(task)
    if client.exists(uri) or client.exists(success):
        return
    publish_json(
        client, uri, dict(version=1, kind=task.kind, request_uri=task.uri, error=message), work
    )
