# Asynchronous analysis for 18 geographic protocol VMs

This additive queue layout is enabled by `<workflow>/continental/<campaign>/parallel.json`.
Legacy protocol-only, six-VM, No-OS, and late OS enrichment contracts retain
their existing paths and behavior. Use a new campaign ID for the new layout.
Deploy this worker before enabling protocol-specific measurement commands.

Run the existing scheduled worker with the same three configured physical S3
mounts, canonical URI mappings, tmpfs budgets and credentials. An example is:

```sh
make workflow-worker ARGS="--poll-interval 2"
```

Polling remains configurable: ready means immediately eligible, subject to the
poll interval and finite CPU/RAM/scratch/I/O capacity. Two reserved critical
slots protect RT/Mass handoffs. At most one support task uses their second lane.
Background slots default to one, preserving historical resource reservations.
Set `IPID_ANALYSIS_BACKGROUND_SLOTS` to split the existing background reservation
between more jobs; it cannot expand the total budget or consume handoff reserves.
Choose it from measured VM headroom, not the number of measurement producers.

Per protocol the durable queues below `sites/<site>/protocols/<protocol>/` are:

* `discovery.json` -> `discovery-ready.json`: verify one closed discovery and
  persist a deduplicated source partition. `discovery/failed.json` is terminal.
* `jobs/<id>/`: unchanged authoritative RT/Mass handoff schemas.
* `measurement-jobs/<id>/`: immediate per-measurement coverage/classification,
  interval/increment tables and figures, including additive repetition reports.
  Those paths use the measurement ID as their output directory, leaving the
  existing canonical baseline and merged reports to one protocol core writer.
* `strategy-jobs/<zmap>/`: canonical RT+Mass merge and the existing
  `analysis-jobs/<zmap>/strategies-ready.json` contract for interprotocol targets.
* `core-jobs/<zmap>/`: strict canonical non-OS postprocessing after its own
  preliminary merge, without waiting for any other protocol, continent or OS.
* `role-jobs/<zmap>/`: separate optional CAIDA/RIPE preparation and role analysis.
* `os-jobs/<zmap>/`: shared-OS coverage and OS strategy plots as soon as this
  core and the central OS scan are ready.
* `measurement-os-jobs/<id>/`: per-measurement OS plots as soon as that one
  measurement and shared OS are ready, even while its protocol sweep continues.
* `os-role-jobs/<zmap>/`: OS/role joins after both OS and role inputs exist.
* `analysis-jobs/<zmap>/done.json`: verifies that all required stages and
  streaming measurement reports completed. Optional role availability/errors
  remain explicit in its stage inventory.

Discovery partitions are persisted under the shared processed root at
`continental/<campaign>/discovery-parts/<site>/<protocol>/targets.pq`. The final
union uses the 18 prepared partitions, not 18 repeated scans of all raw sources.
It retains the shared OS VM's existing 18-source request/provenance schema.

Each continent's UDP VM independently requests interprotocol targets once the
three local canonical merges are ready, publishes its eight measurements and
the final event. Site completion joins only that site's protocol analyses and
interprotocol completion. There is no global scientific analysis or global
completion barrier across continents, apart from the requested shared OS scan.

All persistent publication goes through the existing completion/hash protocol.
Task scratch and subprocesses use the existing scheduler leases, resource
reservations, watchdog, upload integrity checks and bounded cleanup. Stages
have separate failure prefixes so retrying optional/background work cannot
clear a handoff failure or successful stage.

Use `--retry-request-uri` for one failed stage, for example
`.../protocols/tcp/core-jobs/<zmap>/request.json`. Background-stage request URIs
are stable task identities referring to the immutable `strategy-jobs` request;
they need not exist as duplicate S3 objects. Discovery retry uses its original
`.../protocols/tcp/discovery.json` URI. Retry archives only the failed marker,
refuses successful stages, validates ownership and preserves scientific inputs.
The shared union/OS retries retain their existing explicit retry procedure.

Synthetic tests cover independent readiness, incremental union/integrity,
publication order, and stage failure isolation. Real 18-VM/S3/FUSE load and
interruption tests still require an isolated operational test prefix; unit
tests never start Internet scans or access production S3 data.

## Local interprotocol controls

The companion reference producer pins `local_references: true` in the parallel
layout. Its explicit `local_references` target request adds six target files to
the existing target artifact inventory. The four original target files and
their original analyses keep their contents and definitions.

Within `sites/<site>/`, `reference-jobs/reference-<icmp|tcp|udp>-<base|mass>/`
contains `request.json`, `done.json` or `failed.json`. Each available reference
is independently eligible for background analysis; no OS or foreign continent
dependency exists. The worker verifies source commit markers and target hashes,
validates the single-protocol snapshot, and uses the **same classifier and
sample eligibility rules as interleaved outcomes**, including all TCP positions.
Normal TCP baseline analysis keeps its original first-sample treatment.

Additive scientific outputs below the site's processed root are:

* `interprotocol/local-references/reference-<protocol>-<sample>/strategies.pq`
  with baseline strategy, local strategy, expected and received samples for
  every original control target, including missing observations.
* `interprotocol/local-references/comparison.pq` and `comparison.json`, joining
  original baseline, local control and interleaved strategy for every original
  IP/protocol/group. The JSON contains bounded aggregated transition counts.

The comparison runs after the six controls and that continent's existing
interprotocol analysis finish. It checks the reference result digests and
source-address/probe-configuration agreement against the interleaved snapshots.
No missing control silently removes an interleaved target. Shared targets can
occur in several groups, so report counts explicitly use IP/protocol/group as
their unit. These sequential controls expose vantage/time differences; they
do not establish that interleaving alone caused any observed change.

`reference-comparison/{request,done,failed}.json` has an independent lifecycle.
The site's `final/done.json` additionally requires its comparison completion
when the campaign's `local-references.json` marker exists. Other continents,
individual measurement analyses and shared OS work continue independently.
Both reference kinds support the existing explicit `--retry-request-uri`,
archiving only their own failure marker and preserving successful science.

Empty frozen RANDOM cohorts are valid: their streaming coverage is explicitly
zero and their OS stage records `not_applicable_empty_population`. A measured
population with no resolved OS matches records `no_resolved_os_matches`.
Canonical optional OS/role plots retain their existing warning semantics;
warnings are preserved in stage completion records. Source integrity and
required scientific artifacts still fail their own task on errors.
