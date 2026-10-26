"""Supervised child entry point. Scope/resource environment precedes imports."""

from __future__ import annotations

import ctypes
import json
import os
from pathlib import Path
import signal
import sys


def bind_parent():
    if sys.platform != "linux":
        return
    parent = os.getppid()

    def stop(signum, frame):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        os.killpg(os.getpgrp(), signal.SIGTERM)

    signal.signal(signal.SIGTERM, stop)
    # The task must terminate its entire postprocess group even if the worker
    # is SIGKILLed; no orphan writer may outlive the parent's global lease.
    if ctypes.CDLL(None).prctl(1, signal.SIGTERM, 0, 0, 0) != 0:
        raise OSError("could not register parent-death signal")
    if os.getppid() != parent or parent == 1:
        stop(signal.SIGTERM, None)


def execute(spec):
    from ipid_analysis.continental import (
        finish_site,
        prepare_strategies,
        publish_json,
        publish_task_failure,
        submit_site,
        union_discovery,
        verify_shared_os,
    )
    from ipid_analysis.runtime import exclusive_lock, scratch_root, start_run
    from ipid_analysis.s3_workflow import (
        S3Client,
        process_analysis_request,
        process_interprotocol_request,
        process_interprotocol_target_request,
        process_request,
    )
    from ipid_analysis.scheduler import Task

    task = Task(**spec["task"])
    client = S3Client()
    work = scratch_root() / "work"
    work.mkdir(exist_ok=True)
    lease = exclusive_lock("scheduled-task:" + task.key, timeout_seconds=0)
    try:
        lease.__enter__()
    except TimeoutError:
        return 75
    try:
        start_run()
        options = (spec["batch_size"], spec["cost"]["cpus"])
        if task.kind == "handoff":
            if task.scope:
                from ipid_analysis.continental import descriptor, read_json, source_measurement
                from ipid_analysis.s3_workflow import Request

                request = Request.parse(read_json(client, task.uri, work), task.prefix)
                binding = descriptor(client, task.prefix.rsplit("/sites/", 1)[0], work)
                raw_prefix = binding["raw_root"].rstrip("/") + "/" + task.scope + "/ipid"
                if request.ipid_uri != raw_prefix + "/" + request.measurement_id + "/ipid.pq":
                    raise ValueError("handoff crosses campaign/site boundaries")
                for name in ("ipid.pq", "ipid.snapshot.yaml"):
                    source_measurement(client, raw_prefix, request.measurement_id, name, work)
            process_request(client, task.uri, task.prefix, work, *options)
        elif task.kind == "strategies":
            prepare_strategies(client, task.uri, task.prefix, work, *options)
        elif task.kind == "targets":
            process_interprotocol_target_request(client, task.uri, task.prefix, work)
        elif task.kind == "analysis":
            if task.scope:
                campaign_prefix = task.prefix.rsplit("/sites/", 1)[0]
                from ipid_analysis.continental import descriptor, not_requested_status, os_disabled

                binding = descriptor(client, campaign_prefix, work)
                if os_disabled(binding):
                    not_requested_status(client, campaign_prefix, binding, work)
                    os.environ.pop("IPID_ANALYSIS_SHARED_OS_DIR", None)
                else:
                    verify_shared_os(client, campaign_prefix, work)
                from ipid_analysis.continental import protocol_request

                protocol_request(client, task.uri, task.prefix, work)
            process_analysis_request(client, task.uri, task.prefix, work, *options)
        elif task.kind == "interprotocol":
            process_interprotocol_request(client, task.uri, task.prefix, work)
        elif task.kind in {"os-enrichment-prepare", "os-enrichment-analysis"}:
            from ipid_analysis.os_enrichment import prepare, analyze

            root = task.uri.rsplit("/", 1)[0]
            if task.kind == "os-enrichment-prepare":
                prepare(client, root, task.prefix, work)
            else:
                analyze(client, root, task.prefix, work, options[1])
        elif task.kind == "union":
            union_discovery(client, task.uri.rsplit("/union/", 1)[0], work)
        elif task.kind == "submit":
            submit_site(client, task.uri.rsplit("/final/", 1)[0], work)
        elif task.kind == "site-done":
            finish_site(client, task.uri.rsplit("/final/", 1)[0], work)
        elif task.kind == "campaign-done":
            campaign_prefix = task.uri.rsplit("/", 1)[0]
            from ipid_analysis.continental import SITES, read_json

            sites = {
                s: read_json(client, f"{campaign_prefix}/sites/{s}/final/done.json", work)
                for s in SITES
            }
            from ipid_analysis.continental import descriptor, not_requested_status, os_disabled

            binding = descriptor(client, campaign_prefix, work)
            completed = dict(version=1, sites=sites)
            if os_disabled(binding):
                not_requested_status(client, campaign_prefix, binding, work)
                if any(site.get("os_status") != "not_requested" for site in sites.values()):
                    raise ValueError("campaign site completion OS intent mismatch")
                completed["os_status"] = "not_requested"
            elif any(site.get("os_status") == "not_requested" for site in sites.values()):
                raise ValueError("enabled campaign contains no-OS site completion")
            publish_json(client, campaign_prefix + "/done.json", completed, work)
        else:
            raise ValueError(f"unknown workflow task {task.kind}")
        from ipid_analysis.continental import success_uri

        if not client.exists(success_uri(task)):
            raise RuntimeError("task exited without its required completion marker")
        return 0
    except Exception as exc:
        publish_task_failure(client, task, str(exc), work)
        raise
    finally:
        lease.__exit__(None, None, None)


def main():
    bind_parent()
    spec = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, set(spec["cpu_ids"]))
    return execute(spec)


if __name__ == "__main__":
    sys.exit(main())
