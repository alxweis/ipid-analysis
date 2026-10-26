# Continental campaigns and supervised scheduling

This accompanies the six-site mode in `ipid-measure`. Existing protocol-only
campaigns and their request/Done schemas remain supported. No S3 mount or VM
configuration is changed. Keep the existing `mounted-s3`, scratch, S3 roots and
workflow-prefix environment and start `make workflow-worker` as before.

## Dependencies and storage

Each explicit campaign has six fixed sites (`eu,as,af,na,sa,au`) and three
protocol-specific ZMap discoveries per site. The six discovery events unlock
one streaming `UNION DISTINCT`, followed by a request to the separate OS VM.
This work runs asynchronously to site IPID measurements.

Each completed protocol also publishes an immutable standard manifest plus a
`strategy-jobs/<zmap-id>/request.json`. The worker restores the existing RT/Mass
classifications and builds their canonical merge without OS scans or optional
enrichment. `analysis-jobs/<zmap-id>/strategies-ready.json` unlocks interprotocol
target selection. It never replaces the meaning of `done.json`.

A site's `final/request.json` means all three regular workflows and all eight
interprotocol measurements were published. Shared OS completion then allows
its three normal strict postprocessing jobs. `final/done.json` requires those
three successful analyses and the site's interprotocol analysis. It does not
wait for other sites' IPID results. Campaign `done.json` requires all six sites.

Scientific paths are isolated below `continental/<campaign>/<site>/` under
each existing raw/processed/reports mount. Scientific measurement IDs, strategy
definitions, exclusive interprotocol cohorts and Parquet formats are preserved.
Shared RIPE/CAIDA caches stay under the physical base roots and the common
workflow cache prefix. Mount validation/URI mapping always use the three
physical roots, not their per-site subdirectories.

The OS dataset is stored once under `<raw>/continental/<campaign>/os/shared-os/`.
The worker verifies file digests, all discovery references and exact union
coverage. It reads the mounted files directly; it never copies the shared OS
bulk data into six site raw namespaces. `shared-os-coverage.json` under each
processed ZMap directory uses that protocol/site's discovery denominator and
the OS observations sidecar. It includes the real OS scan ID, shared vantage
and union checksum. This is one shared OS perspective, not six regional scans.

## Scheduling and resource accounting

The worker's default scheduling mode supervises subprocesses. S3 objects remain
the durable queue; no service or broker is added. Metadata listing and failure
publication run outside the dispatch/supervision loop. Long postprocessing,
optional enrichment or S3 failures in those children cannot occupy the polling
thread. A single global analysis-writer lease remains held by the parent.

Defaults are intentionally conservative:

| Lane | Slots | CPU reservation on 24 CPUs | RAM / scratch share per slot |
| --- | ---: | ---: | ---: |
| Critical handoffs, preliminary merges, target generation, discovery union | 2 | 4 | 1/8 |
| Final analyses, interprotocol analysis, final control stages | 1 | 16 | 3/4 |

At most one critical support job (merge or target generation) runs at a time,
leaving capacity for a handoff. The bounded discovery union uses this support
slot too, so a ready OS dispatch never queues behind a long final analysis.
Pending handoffs have first priority; ageing
within the critical queue prevents support jobs from starving. Dependencies
waiting on discovery, OS scans or another site consume no running slot.

Global budgets default to available CPUs, 80% of currently available RAM and
free local scratch minus 10 GiB. Optional explicit upper bounds are
`IPID_ANALYSIS_WORKER_CPUS`, `IPID_ANALYSIS_WORKER_MEMORY_BYTES`, and
`IPID_ANALYSIS_WORKER_SCRATCH_BYTES`. Physical availability remains an upper
bound. `--threads` continues to cap each child's CPU/thread usage. At least
three available CPUs and 3 GiB RAM/scratch are required for scheduled mode.
The old single-process dispatcher remains available as `--scheduling sequential`
for legacy jobs; continental campaigns require scheduled mode.

Admission reserves all three resources together. Child CPU affinity is
disjoint on Linux, with bounded DuckDB/BLAS/RIPE parallelism. Each job has a
dedicated scratch directory and bounded DuckDB memory/spill. The supervisor
checks aggregate process-group RSS (including postprocessing and RIPE children)
and actual scratch usage roughly once a second and terminates an overrun or
timeout. Reservations include capacity that background work cannot take.
This is application admission plus a watchdog, **not a kernel RAM/filesystem
quota**: a short allocation burst can exceed its reservation before termination.
Shared RSS pages may be counted conservatively. No privileged cgroups, disk
partitions or VM configuration changes are required. Keep RAM/disk headroom and
measure real peaks before increasing concurrency.

Logs include admitted resource budgets, job completion/failure and the local
`scheduled/<job-hash>/supervisor.log`. Retained failure diagnostics and unrelated
disk usage reduce available scratch for subsequent admissions. Operators must
manage retained local failures when capacity is exhausted; the worker does not
delete scientific data to make room. `--once` drains currently runnable stages;
it does not wait for external scans to produce missing dependencies.

## Restart, failure and explicit retry

Queue state is reconstructed from immutable S3 requests and terminal markers.
Global per-job kernel leases prevent duplicate writers after a restart even
with separate child scratch directories. On Linux, process groups and a
parent-death signal terminate the task and its descendants if the coordinator
dies; a new coordinator must not inherit orphan scientific writers.
Completion/integrity checks remain mandatory. Failure never fabricates Done.

Normal and scoped jobs use the existing explicit retry option, for example:

```sh
.venv/bin/python -m ipid_analysis.s3_workflow \
  --retry-request-uri "$IPID_ANALYSIS_S3_PREFIX/continental/ROUND/sites/eu/jobs/ID/request.json"
```

`strategy-jobs`, `analysis-jobs`, `interprotocol-target-jobs` and
`interprotocol-jobs` use their canonical scoped request URIs. New control-stage
retries use `.../continental/ROUND/union/request.json`,
`.../continental/ROUND/os/request.json`, or
`.../continental/ROUND/sites/eu/final/request.json`. The union request URI is a
synthetic control-stage identifier derived from the six discovery events.
Retry archives the terminal failure before removing only that failure marker.
Successful Done markers and raw data are preserved. For interrupted OS scans,
also follow the measure repository's local-attempt recovery instructions.

## Tests and deployment validation

Synthetic tests cover all 18 discoveries, barriers and site isolation, real
strategy merging/target selection without OS completion, shared OS checksum and
population validation, regional coverage including response-only targets,
explicit retries, concurrency/admission, global restart leases, process-group
termination and parent death (Linux). Existing scientific tests remain required.

The real ZIH S3/FUSE configuration and actual six-site/network scans are not
available in the development environment. Before a full round, use a separate
test prefix for an end-to-end run and restart/failure tests with VFS cache off.
Measure handoff latency, peak RSS/scratch and I/O with colliding site completions.
More background workers, alternative joins/engines, caching/mount changes and
more aggressive parallelism remain benchmark-dependent and are not enabled.
