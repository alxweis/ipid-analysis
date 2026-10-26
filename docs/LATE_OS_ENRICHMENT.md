# Late OS enrichment

Late enrichment applies only to an already completed, explicit No-OS core.
Deploy both companion PRs before using it. Existing OS-enabled and No-OS
workflows retain their defaults and immutable policy.

## Commands

Use the same workflow prefix on the requesting machine, Analysis VM and OS VM:

```sh
export IPID_ANALYSIS_S3_PREFIX='s3://bucket/analysis-workflow'
```

In **ipid-measure**, submit an explicit asynchronous request (no scan runs here):

```sh
make build-continental-os-worker
make run-os-enrichment ZMAP_ID=icmp_2026-10-25_01-42-26 VANTAGE=analysis-os-vm
# Alternatively: one complete six-site campaign, all 18 discoveries:
make run-os-enrichment CAMPAIGN_ID=my-campaign VANTAGE=analysis-os-vm
```

Specify exactly one identity; clear IPID_CAMPAIGN_ID for a protocol job if set.
VANTAGE names the OS scanning vantage, which may differ from the original site.
Repeated identical requests preserve their original timestamp and intent.
A changed vantage or core digest is rejected.

The **ipid-analysis** scheduled worker handles preparation and analysis:

```sh
make workflow-worker
# Read-only lifecycle status:
make os-enrichment-status ARGS="--zmap icmp_2026-10-25_01-42-26"
make os-enrichment-status ARGS="--campaign my-campaign"
```

The default scheduled mode is required; legacy sequential polling does not
dispatch enrichment stages. Preparation uses the reserved support lane and OS
analysis the single background lane, with the existing global CPU/RAM/scratch
admission, process supervision and per-task leases. Time-critical handoffs
remain independently schedulable. The 18 analyses run sequentially within that
budgeted background task and checkpoint independently.

On the **existing separate OS VM**, in ipid-measure:

```sh
make build-continental-os-worker build-os
make workflow-os-enrichment-worker ZMAP_ID=icmp_2026-10-25_01-42-26
# Alternatively, exactly one scan over the six-site union:
make workflow-os-enrichment-worker CAMPAIGN_ID=my-campaign
```

The worker waits for the enrichment scan request, runs the existing six-service
OS scanner against its explicit target file, uploads closed files and exits.
It never launches ZMap, IPID or interprotocol measurements. No SSH is involved.
Use one OS worker/VM and retain its workspace, as in the existing workflow.

## Independent state and storage

The core prefix is either:

- `<workflow>/analysis-jobs/<zmap-id>`
- `<workflow>/continental/<campaign-id>`

All new control and OS raw objects are below
`<core>/enrichments/os/`:

- `request.json`: immutable request, requested time, explicit vantage,
  core prefix and SHA-256 of original core completion.
- `inputs.json`: pinned core request/manifest/status/discovery proofs and
  existing strategy/role input checksums. The geographic mode validates all
  six completed sites and all 18 completed protocol analyses.
- `scan/targets.pq`, then `scan/request.json`: publication in that order.
  A protocol job copies its original ZMap parquet byte-for-byte. Geographic
  mode reuses the same 18-member source validation and UNION DISTINCT producer
  as normal OS-enabled campaigns, including the EU TCP reference. A committed
  union is adopted and verified, never regenerated on retry.
- `running.json`: immutable informational start event, bound to scan request
  SHA-256; not a lease and never an impediment to restart.
- `raw/{os.pq,os-observations.pq,os-coverage.json,os.snapshot.yaml}`, then
  `scan/done.json`: all four SHA-256/size checks precede OS completion.
  The completion records the later scan ID, start time, closed-file completion
  time (retained across upload retries), configured vantage and executor hostname.
- `scan/failed.json`: a terminal scan failure.
- `analyses/[<site>/]<zmap-id>/done.json`: independently committed OS-only
  analysis inventories, checksums and role availability.
- `done.json`: enrichment completed, separate from core; includes scan and
  original core proofs. `failed.json` describes preparation/analysis failure.
- `[scan/]attempts/<timestamp>/failed.json`: archived explicit retries.

Without a request, enrichment status is `not_requested`. Then it progresses
`requested -> running -> completed`, or `failed`. Terminal success/failure
markers dominate the informational running event. A running event after a
crash means unfinished work, not proof that a process is still alive.

Original manifests, OS policy, core/site/campaign Done markers, raw ZMap/IPID
data, strategies, non-OS reports and interprotocol outputs are never rewritten.
The original `os_status=not_requested` remains true even after enrichment.

New processed tables and report figures live only below
`data/processed/enrichments/os/` and `reports/enrichments/os/`, subdivided by
core identity, site (or protocol) and ZMap identity. The completion inventory
contains every exact path and checksum. Outputs include resolved OS groups,
OS-by-strategy aggregates/plots, OS-by-role joins/distributions/plots, TCP
connection RT-base OS plots where applicable, and population-specific OS
coverage. Existing artifact schemas and OS grouping/role semantics are reused.

ITDK and RIPE analyses reuse the original persisted matched role joins and their
metadata, including the original RIPE time window. No current external dataset
is fetched or substituted, and no non-OS role producer is called. If the original
optional role analysis is absent, the corresponding OS role stage is explicitly
`not_available_in_core`; no result is fabricated. Missing or changed inputs
that were pinned are failures, not optional absence.

## Restart and explicit retry

Restart the same worker with the same identity/workspace. Per-task kernel
leases prevent concurrent local writers. A closed local scan is reused after
lost exit/upload errors. A successful remote scan is never rescanned; completed
protocol analysis inventories are verified and adopted. Incomplete protocol
outputs can be rewritten only in their own enrichment namespace.

After inspecting a **scan** failure, on the OS VM:

```sh
make workflow-os-enrichment-worker CAMPAIGN_ID=my-campaign ARGS="--retry-failed"
# If the scan itself left partial files, retain/archive them and start a new ID:
make workflow-os-enrichment-worker CAMPAIGN_ID=my-campaign ARGS="--retry-failed --retry-incomplete"
```

For a protocol use ZMAP_ID instead of CAMPAIGN_ID. If an interrupted process
left no failed marker, use just `--retry-incomplete` when needed. Completed
local data are never discarded by that flag. A new partial attempt is explicit
and may repeat probes; a completed attempt is only republished.

For **preparation/analysis** failure, use the existing Analysis VM retry:

```sh
make workflow-worker ARGS="--retry-request-uri s3://bucket/analysis-workflow/continental/my-campaign/enrichments/os/request.json"
```

For a protocol substitute
`analysis-jobs/<zmap-id>/enrichments/os/request.json`. The retry archives only
the enrichment failed marker before deleting it; it never clears core failures
or successful markers. The analysis retry mechanism can also archive a scan
failure using its `.../enrichments/os/scan/request.json`, but an interrupted
scan still requires explicit `--retry-incomplete` on its OS worker.

Genuine S3, network, permission, source-integrity or visibility failures remain
errors. No OS-dependent artifacts are produced before scan Done and exact
observation-population validation. Core completion remains successful on every
enrichment failure.

## Validation still required on the actual infrastructure

Unit/integration tests use local synthetic parquet and fake object stores.
Linux CI covers the full existing test suites, race detector, shell workflow
and process-supervision tests. No production S3 data or VM/Ansible configuration
was touched during implementation.

Before operational use, run a dedicated non-production S3/FUSE/OS-VM exercise:
protocol and 18-discovery union; byte/hash comparison of core trees before/after;
real OS packet scanning; upload failure and SIGKILL recovery; analysis retry;
visibility through the VFS-cache-off mounts; disk/RAM limits and competing
handoffs. Real S3/FUSE/VM and full-scale load tests were not available in the
development environment. Multiple independent OS VMs racing on one job remain
outside the existing single-host coordination model.
