# RIPE shared transfers and targeted backfill

## Transfers, cache compatibility, and budgets

The Analysis VM's default shared transfer area is
`/mnt/data01/ipid-analysis/tmp/ripe-transfers/`. It does not follow the scheduled
child's `IPID_ANALYSIS_TMP_DIR`. In mounted-S3 mode an override must remain
inside the analysis checkout, outside the persistent storage mounts, on local
storage. The scheduler requires transfers and job scratch on the same filesystem.
No VM or Ansible configuration change is needed.

`IPID_ANALYSIS_RIPE_TRANSFER_DIR` overrides that area. If only the legacy
`IPID_ANALYSIS_RIPE_WORK_DIR` is set, its `shared-transfers` subdirectory is used;
raw files already in its old `<day>/<filename>` layout are still reused and
are not deleted. New entries are under `downloads/<existing-part-key>/` and
include the dump and its HTTP state. Locks use the same shared root, so separate
job scratch roots no longer create separate locks for the same RIPE resource.
The existing PID/start-time/boot-ID stale-lock recovery is retained.

The default transfer limit is 8 GiB. The scheduler caps it to at most one eighth
of its global scratch capacity, subtracts it before calculating critical and
background job allocations, and reserves its unused capacity during admission.
Resident retained transfers are counted too. A configured reduction below
retained usage is rejected explicitly; it does not silently overcommit or erase
retained data. The normal stopped-job cleanup never removes the shared area.

Within the outer shared RIPE preparation lock, download threads reserve their
remaining bytes plus metadata headroom. They wait for parsers to free space
rather than exceeding the cap. A dump larger than the cap fails explicitly.
If only protected partials remain and no downloader/parser can free space, the
preparation reports an insufficient budget instead of waiting without progress.
Only inactive transfers from other windows in the owned `downloads/` directory
can be evicted, oldest first, when space is required. Inputs selected for the
current window and all persistent processed caches are protected. Once a part
and its cache publication commit, its raw transfer and HTTP state are reclaimed.
Thus retained failures cannot grow the raw staging area without bound. Resume
of an old evicted transfer necessarily starts again; valid persistent parts are
always checked first. This remains a single Analysis VM coordination model.

Successful prefetches are delivered in completion order. If one download fails,
already submitted successful downloads are still processed and checkpointed
before the error is propagated. No partial window is marked complete. Download
errors now record attempt, status, offset, received/expected bytes and validator;
credentials and URL query strings are excluded from diagnostics. HTTP size,
validator, range and incomplete-body checks remain in force.
Oversized response bodies are rejected before they exceed the reserved size;
their prefixes cannot be promoted as completed downloads on a subsequent call.

Hourly part version 2, window version 4, schemas, sample selection, role rules,
existing cache keys, S3 prefixes and manifest-last publication are unchanged.
Local caches and restored caches validate identity where available, typed schema,
row count, metadata markers and checksums before reuse.
Cache checksums are calculated from the actual bytes, including when a completion
marker already declares a checksum; declarations are not compared to themselves.
Markerless legacy files
without a checksum are decoded instead of being blindly accepted or rebuilt.
Window restores require the recorded window identity. Current valid caches do
not need their raw dumps and are not regenerated. Real S3/permission/network
errors and corrupt restored objects remain errors; they are not cache misses.

## What happened in geo-test-200k

All six protocol analyses select the same twelve dumps in the UTC window
`[2026-10-01, 2026-10-29)`. AS/ICMP, AS/TCP and AS/UDP ran before EU. They
checkpointed four, then another six hourly parts, but their optional RIPE stage
failed on short HTTP bodies. EU/ICMP reused those ten parts, completed the last
two and published the window; EU/TCP and EU/UDP reused that completed cache.
The old job-local scratch and ordered prefetch loop repeated some downloads
that were already complete but not yet processed. The exact historical HTTP
transport cause remains unknown because per-attempt headers and sizes were not
retained. Do not assume that this change repairs a particular RIPE server fault.

## Targeted AS backfill: commands for the operator, not executed by this PR

This does **not** run ZMap, IPID, classifiers, strategy merges, role-cache
preparation, OS analysis, or full postprocessing. It reads the already complete
RIPE/ITDK caches and four existing strategy tables: one merged table for each
protocol and TCP connection RT-base. It creates only the missing optional RIPE
and CAIDA/RIPE comparison outputs. MaxMind is not involved.

Deploy this PR first. Wait for active work to finish and stop the Analysis worker
gracefully before running this standalone writer; leave measurement VMs alone.
Do not remove core Done markers, resubmit completed core jobs, or edit their
original `postprocess-status.json`. Those files correctly describe the original
attempt. The supplemental result record below describes the later backfill.

Use the same storage roots as the normal worker. The following commands fetch
only the small original control files into a **new local** work directory:

```sh
cd /mnt/data01/ipid-analysis
unset IPID_ANALYSIS_SCOPE IPID_ANALYSIS_RUN_OWNER IPID_ANALYSIS_SHARED_OS_DIR
export IPID_ANALYSIS_STORAGE_MODE=mounted-s3
export IPID_ANALYSIS_TMP_DIR=/mnt/data01/ipid-analysis/tmp
export IPID_ANALYSIS_S3_PREFIX='s3://netds3o:aweis/ipid-2/analysis-workflow'
export IPID_ANALYSIS_S3_ROOTS='{"raw":"s3://netds3o:aweis/ipid-2/raw","processed":"s3://netds3o:aweis/ipid-2/processed","reports":"s3://netds3o:aweis/ipid-2/reports"}'
export BACKFILL_WORK="$(mktemp -d /mnt/data01/ipid-analysis/tmp/ripe-backfill-geo-test-200k-XXXXXX)"
for id in icmp_2026-10-29_01-58-38 tcp-80_2026-10-29_01-59-25 udp-dns-53_2026-10-29_02-02-02; do
  mkdir -p "$BACKFILL_WORK/$id"
  for name in request.json manifest.json done.json postprocess-status.json; do
    s3cmd get --no-progress \
      "$IPID_ANALYSIS_S3_PREFIX/continental/geo-test-200k/sites/as/analysis-jobs/$id/$name" \
      "$BACKFILL_WORK/$id/$name" || exit 1
  done
done
```

First run the Python block below with `BACKFILL_EXECUTE=0`. It validates the exact
No-OS manifests, original completion, pinned manifest hashes, shared RIPE window,
existing strategy files and existing ITDK cache; then lists the 36 new output
paths. It rejects **any** existing destination or completion marker before
writing anything. If a partial backfill already exists, inspect it separately;
do not use this recipe to overwrite it. After reviewing the plan, rerun the
same block with **only** `BACKFILL_EXECUTE=1` changed.

```sh
BACKFILL_EXECUTE=0 .venv/bin/python - <<'PY'
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

from ipid_analysis.artifacts import assert_complete, completion_path
from ipid_analysis.caida_itdk import dataset_paths
from ipid_analysis.config import PROJ_ROOT, PROCESSED_STORAGE_DIR, RAW_STORAGE_DIR, REPORTS_STORAGE_DIR
from ipid_analysis.manifest import resolve
from ipid_analysis.plot_ripe_atlas_strategy import _artifact_paths, render_ripe_atlas_analysis
from ipid_analysis.ripe_atlas import RipeAtlasDataset, campaign_window, infer_campaign_start
from ipid_analysis.ripe_cache import digest, read_metadata, validate_roles
from ipid_analysis.runtime import start_run
from ipid_analysis.s3_workflow import AnalysisRequest, validate_analysis_manifest
from ipid_analysis.strategy_merge import iter_strategy_merges

def file_digest(path):
    assert_complete(path)
    actual = digest(path)
    marker = completion_path(path)
    if marker.is_file() and json.loads(marker.read_text())["sha256"] != actual:
        raise ValueError(f"input completion checksum mismatch: {path}")
    return actual

work = Path(os.environ["BACKFILL_WORK"]).resolve()
assert (PROJ_ROOT / "tmp").resolve() in work.parents
assert not os.environ.get("IPID_ANALYSIS_SCOPE")
prefix = os.environ["IPID_ANALYSIS_S3_PREFIX"].rstrip("/") + "/continental/geo-test-200k/sites/as"
scope = Path("continental/geo-test-200k/as")
processed = PROCESSED_STORAGE_DIR / scope
figures = REPORTS_STORAGE_DIR / scope / "figures"
itdk = dataset_paths("2025-08", "midar-iff-snmp-tnt", raw_root=RAW_STORAGE_DIR)
assert_complete(itdk.interfaces_path)
assert_complete(itdk.metadata_path)
assert itdk.interfaces_path.is_file() and itdk.metadata_path.is_file()
plans, fingerprints = [], {}
ids = ["icmp_2026-10-29_01-58-38", "tcp-80_2026-10-29_01-59-25", "udp-dns-53_2026-10-29_02-02-02"]
for job in ids:
    control = work / job
    for name in ("request.json", "manifest.json", "done.json", "postprocess-status.json"):
        fingerprints[str(control / name)] = file_digest(control / name)
    request = AnalysisRequest.parse(json.loads((control / "request.json").read_text()), prefix)
    manifest_path = control / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    done = json.loads((control / "done.json").read_text())
    status = json.loads((control / "postprocess-status.json").read_text())
    validate_analysis_manifest(manifest, request)
    assert request.job_id == done["job_id"] == job
    assert done["manifest_uri"] == request.manifest_uri
    assert request.os_status == done["os_status"] == "not_requested"
    assert status["required_complete"] and status["optional"]["ripe"] == "failed"
    evidence = status["manifest"][0]
    assert manifest_path.stat().st_size == evidence["size"]
    assert file_digest(manifest_path) == evidence["sha256"]
    window = campaign_window(infer_campaign_start(manifest))
    assert window.label == "2026-10-01_2026-10-29"
    directory = PROCESSED_STORAGE_DIR / "ripe-atlas" / window.label / "daily-dumps-s12"
    dataset = RipeAtlasDataset("daily-dumps-s12", window, directory, directory / "ipv4-roles.pq", directory / "source.json")
    info = read_metadata(dataset.metadata_path)
    identity = hashlib.sha256(f"{window.label}|daily-dumps-s12|https://data-store.ripe.net/datasets/atlas-daily-dumps|https://atlas.ripe.net/api/v2".encode()).hexdigest()
    validate_roles(dataset.roles_path, info, "4", identity=identity)
    assert info["window_start"] == window.start.isoformat()
    assert info["window_end"] == window.end.isoformat()
    sources = list(iter_strategy_merges(manifest))
    connection = resolve(manifest, "tcp.ipid.connection.rt-based.base")
    if connection is not None:
        sources.append(connection)
    for source in sources:
        strategy = source.artifact_path(processed, "strategies")
        assert_complete(strategy)
        fingerprints[str(strategy)] = file_digest(strategy)
        outputs = _artifact_paths(source, processed_root=processed, figures_root=figures, with_caida=True)
        for path in asdict(outputs).values():
            assert path is not None
            if path.exists() or completion_path(path).exists():
                raise FileExistsError(f"refusing to replace existing backfill artifact: {path}")
            print(path)
        plans.append((source, dataset))
assert len(plans) == 4
fingerprints[str(itdk.interfaces_path)] = file_digest(itdk.interfaces_path)
fingerprints[str(itdk.metadata_path)] = file_digest(itdk.metadata_path)
fingerprints[str(dataset.roles_path)] = file_digest(dataset.roles_path)
fingerprints[str(dataset.metadata_path)] = file_digest(dataset.metadata_path)
record = work / "backfill-results.json"
if os.environ.get("BACKFILL_EXECUTE") == "1":
    if record.exists():
        raise FileExistsError(record)
    start_run()
    generated = []
    for source, dataset in plans:
        result = render_ripe_atlas_analysis(source, dataset, itdk=itdk, processed_root=processed, figures_root=figures, threads=4)
        for path in asdict(result).values():
            if path is not None:
                assert_complete(path)
                generated.append({"path": str(path), "size_bytes": path.stat().st_size, "sha256": file_digest(path)})
    assert all(file_digest(Path(path)) == sha for path, sha in fingerprints.items())
    record.write_text(json.dumps({"campaign_id": "geo-test-200k", "site": "as", "kind": "ripe-only-backfill", "completed_at": datetime.now(timezone.utc).isoformat(), "original_jobs": ids, "input_sha256": fingerprints, "artifacts": generated}, indent=2) + "\n", encoding="utf-8")
    print(f"Created {len(generated)} optional artifacts; supplemental provenance: {record}")
else:
    print("PLAN ONLY: four RIPE analyses / 36 new artifacts; nothing published")
PY
```

Keep `backfill-results.json` with the review/provenance material. Optionally copy
that **new** record to a distinct supplemental review location after inspecting
it; do not replace original job status or Done files. Restart the normal worker
with its previous command/environment afterward. Its completed core requests
remain completed and are not rerun. An interrupted backfill intentionally fails
the next preflight on existing files; inspect those outputs before any explicit
continuation rather than deleting them automatically.

## Validation and remaining real tests

Regression tests exercise separate child processes with distinct scratch roots,
shared-hour download/parse deduplication across windows, complete-download reuse,
restart/resume after local job cleanup, prefetch failure checkpoints, bounded
eviction/reservations, cache identity/schema/count/checksum validation, and real
error propagation using synthetic HTTP/S3 and local Parquet fixtures. They do
not change production data or execute the AS backfill.

Still test on a non-production VM/prefix: real RIPE HEAD/Range/validator behavior,
network interruption and SIGKILL during download/part publication, visibility
through the VFS-cache-off mounts, native S3 permissions and manifest ordering,
disk-pressure reservations alongside critical handoffs, and actual production
dump sizes relative to the 8 GiB transfer cap. The original three transport
failures cannot be assigned to a specific server/proxy cause retrospectively.
