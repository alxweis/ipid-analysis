# ipid-analysis

Deployment, scratch storage, completion/recovery rules, compatibility changes,
and the outstanding benchmark matrix are documented in
[docs/storage-runtime.md](docs/storage-runtime.md). The worker now defaults to
`mounted-s3`; configure the explicit URI mapping before starting it.

## Inter-protocol measurement outcomes

New campaigns use the v2 manifest produced by `ipid-measure`. Update the analysis
worker before starting a v2 measurement campaign. The three-manifest
`make run-interprotocol` entry point is unchanged. All BASE groups run first,
then all MASS groups; analysis starts after both stages of every group finish.

| Baseline population | Measurement | Required replies per protocol |
| --- | --- | --- |
| Reflection, Constant, Single, Per-Destination, Per-Bucket | RT-based, 4 connections × 4 requests | 16/16 |
| Multi, Random | Fixed-interval, 4 connections × 25 requests | at least 80/100 |

Pairs send 32/200 requests and triples send 48/300 requests in BASE/MASS.
The fixed interval is between consecutive requests to the same target,
regardless of protocol; it is **not divided** by the protocol count. Every
protocol receives the same varying request-IPID pattern indexed by logical
position. Missing replies retain their original positions. Every completed
measurement retains every selected target, including zero-reply targets;
interrupted or failed measurements must not publish a completed campaign.

Target selection remains the same-strategy intersection of the canonical
stateless RT-base + FI-mass strategy merges. Pair cohorts exclude the corresponding
same-strategy triple cohort. Pair relationships inside a triple measurement stay
in the triple cohort; they are not counted as separate pair measurements.
`PER_CONNECTION` is not a baseline cohort, but can be an observed alternative.

Each IP receives one main outcome within its measured protocol group. Missing
samples are checked first, per protocol. A known alternative **or UNCLASSIFIED**
counts as a changed individual label. BASE reuses the ordinary BASE classifier,
so it cannot identify MULTI/RANDOM alternatives; MASS reuses the ordinary loss-aware
MASS classifier. Unsupported snapshots/layouts and malformed rows fail the job.

* **Counters:** full shared-model compatibility wins over individual label
  changes. Next, any matching shared-pair model with the third protocol retaining
  its baseline label yields `PARTIAL_PROTOCOL_SHARED`. Multiple matching
  partitions use that same category, with all matches retained as diagnostics.
  Otherwise all individual labels unchanged yields `PROTOCOL_ISOLATED`; remaining
  targets yield `STRATEGY_CHANGED`. In a shared-pair/changed-third case the main
  outcome is changed, but the shared pair is retained in the metadata.
* **Reflection/Constant:** after confirming every individual label, compare
  request/reply offsets modulo 65536 or constant reply values across protocols:
  `SAME_PARAMETER` when all agree, `PARTIALLY_MATCHING_PARAMETERS` when exactly
  two of three agree, and `DIFFERENT_PARAMETERS` when all differ. Pair measurements
  retain only Same/Different. Plots use Same Offset/Value, Partially Matching
  Offsets/Values, and Different Offsets/Values. Missing replies and changed
  individual strategies take precedence; matching parameters do not imply shared
  internal state.
* **Multi:** after confirming individual MULTI labels, test the combined sequence
  with the established cluster rule, excluding earlier matching rules while
  preserving the actual source/connection/request layout:
  `SAME_INTER_STRATEGY` / `DIFFERENT_INTER_STRATEGY`.
* **Random:** individual labels are reclassified; the main outcomes are
  `UNCHANGED`, `STRATEGY_CHANGED`, or `NOT_ENOUGH_SAMPLES`.
  **Joint 200/300-position RANDOM calibration is deferred.** `COMBINED_MATCH` is
  null and no Same/Different Inter-Strategy claim is made for RANDOM.

Sharing means compatibility with a counter model, not proof of internal shared
state. Changed means an observed label change, not proof of a causal effect of
interleaving. Timing, vantage point, sample length and ordinary variability also
matter. The original label may come from either part of the canonical merge.

For each group/stage, the worker writes `interprotocol-outcomes.pq` and
`interprotocol-summary.json` below
`data/processed/interprotocol/<campaign>/runs/<run>/<group>/<base|mass>/`.
Campaign outputs in `data/processed/interprotocol/<campaign>/runs/<run>/` are:

* `interprotocol-campaign-outcomes.pq`: one row per IP/group, with ordered protocol
  labels and sample counts, outcome, shared pairs, compatible partitions,
  reflection offsets/constant values, matching parameter pairs, and combined MULTI match.
* `interprotocol-campaign-summary.json`: counts per baseline/group/outcome,
  partial-model counts, matching-parameter pair counts, measurement IDs, classifier versions and the explicit
  deferred joint-RANDOM scope.
* `interprotocol-campaign-<strategy>-alternatives.json` and `.pq` for each of the
  seven baselines: targets whose **main** outcome is changed. JSON counts joint
  ordered label combinations (including UNCLASSIFIED), changed-label counts and
  resolved state counts. Percentages use all targets of that baseline/group,
  including missing targets. Parquet retains individual IPs and diagnostics.
* `interprotocol-campaign-run.json`: source manifest and stage IDs.

The campaign also creates `interprotocol-upset-<strategy>.pdf` and `.png` for
all seven strategies in the same figure directory. These compact UpSet-style
figures show outcome percentages within each measured protocol intersection,
including Not Enough Samples. Parenthesized intersection sizes sit between the
bars and membership matrix; protocol labels include set sizes within the selected
campaign population. Empty intersections show `(0)` without a filled bar.
The matrix encodes cohort membership, not inferred sharing. Pair cohorts exclude
same-strategy triple targets; single-protocol-only targets are not represented.
Use that scope in the paper caption. Counts above 10000 use rounded `k`/`M` labels;
exact counts remain in the campaign summary.

Regenerate the UpSet figures without reclassifying raw measurements:

```bash
python -m ipid_analysis.plot_interprotocol_upset \
  data/processed/interprotocol/<campaign>/runs/<run>/interprotocol-campaign-summary.json \
  reports/figures/interprotocol/<campaign>/runs/<run>/
```

`MATCHING_PARAMETER_PAIRS` records matching pairs for Reflection/Constant after
all individual labels are confirmed (all three pairs when all parameters agree).
The summary's `parameter_relationships` aggregates these by baseline, protocol
group, outcome and pair set. Outcome schema/summary version 2 adds this field and
the partial-parameter category; the measurement manifest remains version 2.

`RAW_CHANGED_PROTOCOLS` counts different individual labels before sharing takes
precedence. Counter `STATE_COUNTS` is `[shared, isolated, changed]` **after** that
precedence; for example a fully shared Per-Destination target can have three raw
label changes but state `[3,0,0]`. Insufficient rows have no changed count/state.

Seven compact paper PDFs, `interprotocol-campaign-<strategy>.pdf`, are written to
`reports/figures/interprotocol/<campaign>/runs/<run>/`. Slugs are `reflection`,
`constant`, `single`, `per-destination`, `per-bucket`, `multi`, `random`.
Bars count IPs, include Not Enough Samples, and display empty cohorts as N=0.
Partial sharing occupies only triple observations. The worker uploads tables and
JSON below `results/` and PDFs below `results/reports/figures/` as before.
Classification, campaign merging and detail export use bounded batches.

V1 manifests and snapshots without the new `interprotocol.sample` field retain
the historical 4x25 RT analysis/output path for reproducibility. V2 does not
reuse those independent stability plots or their full-table Python conversions.

Manual commands remain available:

```bash
make build-interprotocol-targets ARGS="interprotocol-campaign.json"
make analyse-interprotocol ARGS="RAW.pq SNAPSHOT.yaml OUTPUT_DIR"
make analyse-interprotocol-campaign ARGS="RUN-MANIFEST.json --raw-root data/raw/ipid"
make validate-interprotocol
```

## Paper plot font

All figures use the actual Linux Libertine OpenType family, including its
bold/italic variants and Matplotlib math text. Install it on Debian/Ubuntu
before running the analysis:

```bash
sudo apt install fonts-linuxlibertine
```

The renderer loads `LinLibertine_*.otf` or `LinLibertine_*.ttf` files directly
and fails with a clear message instead of silently substituting another family.
For a non-standard installation, set `IPID_LINUX_LIBERTINE_DIR` to the
directory containing `LinLibertine_R.otf`/`.ttf` and the other variants.

## S3 measurement handoff worker

The analysis VM can process stateless ICMP, TCP, and UDP-DNS RT measurements
produced by `ipid-measure` without any direct network connection between the
VMs. Both sides use the same S3 prefix and locally configured `s3cmd`
credentials:

```bash
export IPID_ANALYSIS_S3_PREFIX=s3://bucket/ipid-analysis-workflow/
export IPID_ANALYSIS_S3_ROOTS='{"raw":"s3://bucket/prefix/raw","processed":"s3://bucket/prefix/processed","reports":"s3://bucket/prefix/reports"}'
make workflow-worker
```

For every `jobs/<measurement-id>/request.json`, the worker:

1. reuses mapped `ipid.pq` and `ipid.snapshot.yaml` through the raw mount
   (explicit local compatibility mode downloads them),
2. runs the normal IPID selection-strategy classifier,
3. persists `strategies.pq` and additive `classification-provenance.json`
   beside the RT measurement's `ipid.pq`,
4. writes `zmap_unclassified.pq` with the ZMap-compatible columns `IP_ADDR` and
   `REPLY_TYPE`, containing only `UNCLASSIFIED` addresses,
5. uploads that parquet beside the RT measurement's `ipid.pq`, and then
   publishes `jobs/<measurement-id>/done.json` with its canonical URI, row
   count, size, and SHA-256 digest.

If processing fails, `failed.json` is uploaded instead. Requests are idempotent:
jobs with either terminal marker are skipped. Use `--once` to process the current
queue and exit, which is useful for cron; the default process polls continuously.

The corresponding `ipid-measure` run waits for the terminal marker and will not
start that protocol's 25-request fixed-interval measurement until the result has
been downloaded and verified.

The same worker also starts the complete postprocessing automatically after a
successful `make run-all-*` sweep. It polls
`analysis-jobs/<zmap-id>/request.json`, downloads the manifest and its referenced
ZMap, OS, and IP-ID references through the configured raw mount, then runs strict
manifest postprocessing. Worker control files use local `tmp/workflow/analysis/<zmap-id>/`;
successful work directories are removed after durable completion. S3 receives
`postprocess.log` followed by either
`done.json` or `failed.json`. A single worker processes the three protocol VMs'
jobs sequentially, so no additional scheduler or locking service is required.

For campaigns declaring `fixed_base_target_uri`, the worker also downloads
`zmap-fixed-base-sample.pq` and its JSON metadata from the ZMap measurement
prefix. Stateless fixed-base coverage uses this sample for ICMP, TCP, and
UDP-DNS. Historical campaigns without a fixed-base sample retain the full
ZMap denominator. RT Base and FI Mass keep their existing target populations.
Deploy this worker version before enabling ICMP/UDP-DNS fixed-base sampling
on the measurement VMs.

New TCP connection runs
declare `connection_target: zmap-connection-sample.pq` in the protocol manifest;
the worker downloads that SYN-ACK sample and its JSON metadata, and both
connection variants use it as their coverage denominator. A declared but missing
sample fails instead of silently using the full ZMap population. Historical
manifests without this declaration retain their previous coverage behavior.
Deploy this support before enabling SYN-ACK sampling on the measurement VMs.

### CAIDA ITDK role and cross-interface analysis

Normal manifest postprocessing automatically joins the canonical merged
stateless strategy result, and the TCP RT-based connection result when present,
to a pinned CAIDA IPv4 ITDK release. The default is release `2025-08` and its
`midar-iff-snmp-tnt` topology. On the first analysis job, the public
`midar-iff-snmp-tnt.ifaces.bz2` and `README.txt` are downloaded and converted to
a reusable Parquet lookup below:

```text
data/raw/caida-itdk/2025-08/midar-iff-snmp-tnt/
```

On the deployed analysis VM, `data/raw` is the S3-mounted
`/mnt/data01/ipid-analysis/data/raw`, so the large source and lookup do not use
the small root filesystem. Prepare the cache explicitly with:

```bash
make prepare-itdk ARGS="--release 2025-08 --topology midar-iff-snmp-tnt"
```

Private or not-yet-public releases can be imported without changing the
analysis code:

```bash
make prepare-itdk ARGS="--release 2026-03 \
  --source-dir /path/to/private/2026-03 \
  --topology midar-iff-snmp-tnt"
```

The worker and `make analyse` read the same settings from
`IPID_ANALYSIS_ITDK_RELEASE`, `IPID_ANALYSIS_ITDK_TOPOLOGY`,
`IPID_ANALYSIS_ITDK_SOURCE_DIR`, and `IPID_ANALYSIS_ITDK_IFACES`. Set
`IPID_ANALYSIS_SKIP_ITDK=1` only when deliberately running the legacy analysis
without ITDK output.

The role plot contains only ITDK-matched addresses and uses two exhaustive
groups: `Transit-Observed` is `T=true` with either value of `D`, while
`No Transit Evidence` is `T=false` with either value of `D`. Unmatched addresses
are reported as coverage but excluded from both bars. The JSON additionally
reports all four `T`/`D` combinations. `UNCLASSIFIED` and
`NOT_ENOUGH_SAMPLES` remain distinct in Parquet and raw JSON counts but share
one `Unclassified` segment in the compact paper plot.

When the protocol manifest contains an OS measurement, postprocessing also
creates an `Observed Network Role` by `OS Distribution` plot for the same
canonical measured population. It joins the ITDK role with the reusable
`data/processed/os/<os-id>/os-groups.pq` lookup. Only canonical
`OS_STATUS=resolved` results enter the bars; missing, ambiguous, and
unclassified OS measurements are excluded rather than mislabeled as an OS and
their role-specific coverage is recorded in the JSON sidecar. Each role is
normalized independently over its resolved-OS intersection.

The categorical OS order and color mapping are fixed across ICMP, UDP-DNS, TCP
no-connection, and TCP connection-oriented plots. If at most ten OS groups are
represented, all are shown. Otherwise, the nine groups with the largest mean
role-normalized share are retained and all other resolved groups are combined
as `Other`. Selected groups still follow the fixed manuscript order, so an OS
keeps the same relative legend position and color whenever it is present. This
keeps the two-row legend compact without discarding the tail population.

For alias-set consistency, only ITDK nodes with at least two measured,
classified interfaces are evaluated. The outputs report strict whole-node
agreement, equally node-weighted pairwise agreement, the transit-evidenced
subset, and the most frequent strategy combinations among discordant nodes.
Artifacts are stored beside their source strategy result as:

```text
*_itdk-strategy-join.pq
*_itdk-role-strategy-distribution.pq
*_itdk-role-by-strategy.{pdf,json}
*_itdk-os-role-join.pq
*_itdk-role-os-distribution.pq
*_itdk-role-by-os.{pdf,json}
*_itdk-node-consistency.pq
*_itdk-discordant-strategy-combinations.{pq,pdf}
*_itdk-consistency-summary.json
```

### RIPE Atlas observed-role analysis

Normal manifest postprocessing also derives a second observed network-role
lookup from public RIPE Atlas traceroutes. The campaign start is inferred from
the timestamped ZMap ID. The lookup always covers the 28 complete UTC days
immediately before that start; it never waits for data collected after the
IP-ID campaign:

```text
[UTC-day(campaign start) - 28 days, UTC-day(campaign start))
```

The default `daily-dumps` source downloads twelve reproducibly and evenly
distributed hourly traceroute archives from that fixed window. Current hourly
archives are commonly 2--3 GB each; downloading all 672 hours would otherwise
exceed 1.5 TB for one campaign. Up to four worker processes parse independent
dumps, with at most two downloads ahead of the bounded parser queue. BZip2 is
decompressed as a stream; no uncompressed JSON copy is written. Each parser
uses three full IPv4 bitmaps (SEEN/T/D, 1.5 GiB of address space per worker).
Only touched pages/blocks are used, and Parquet export is bounded to 262144
rows per batch. Per-address trace counts, probe/measurement-ID sets, protocol
sets and timestamps are no longer collected.

Each hour produces sorted ZSTD Parquet with `IP_V4` (uint32), `T`, `D` under
`data/processed/ripe-atlas/file-parts`. Row presence represents SEEN, including
observed addresses with both flags false. The merged window lookup also retains
the textual `IP_ADDR` column for existing plot joins. DuckDB merges flags with
`bool_or`, using four threads and an 8GB memory limit by default. Parquet is
internally compressed, not wrapped in another compressed stream.

New raw hourly downloads use a local scratch directory, defaulting to
`tmp/ripe-downloads`. Set `IPID_ANALYSIS_RIPE_WORK_DIR` to a sufficiently
large local SSD directory (not the small root filesystem or an S3 FUSE mount).
Completed scratch files are removed after their processed part is persisted;
failed/incomplete files remain resumable. Existing raw files below the following
directory are reused and never removed:

RIPE exposes these dumps as a rolling archive. Preparation reads the public
directory index before selecting files. When retention has removed the start
of the requested window but some days still overlap it, the samples are spread
over the available overlap, a warning reports the reduced day coverage, and
the exact available days and selected hours are recorded in `source.json`.
No dump at or after the campaign start is substituted. If no archived day
overlaps the requested window, use the API with an explicit measurement-ID
file or import previously retained files with `--input-dir`.

```text
data/raw/ripe-atlas/daily-dumps/YYYY-MM-DD/
```

The compact reusable lookup and complete source metadata are stored below:

```text
data/processed/ripe-atlas/<window>/daily-dumps-s12/
```

The worker automatically publishes both hourly parts and window lookups to
`<IPID_ANALYSIS_S3_PREFIX>/ripe-atlas-cache/v4/`, using the existing `s3cmd`
credentials. This also works when the workflow prefix is passed on the command
line. Override with `IPID_ANALYSIS_RIPE_CACHE_S3=s3://bucket/prefix`; an explicitly
empty value disables remote caching. Standalone preparation without either
prefix uses local caching only.

S3 Parquet objects are addressed by SHA-256 under `objects/`. Hour/window
manifests are uploaded last and contain the checksum and provenance. Restores
verify the checksum before publishing local files. A window cache is checked
before accessing the RIPE archive index; each hourly cache is checked before
requesting the raw dump. Thus the same window works after raw dumps expire,
and selected overlapping hours need not be downloaded or parsed again. New
windows still require the available-day index to select their hours. Cache v4
invalidates previous role semantics; old files are left untouched.

Resource controls (also available in manifest postprocessing):

| Setting | Default | Purpose |
| --- | --- | --- |
| `IPID_ANALYSIS_RIPE_WORKERS` | 4 | Requested parser processes, further capped by CPU and memory budget |
| `IPID_ANALYSIS_RIPE_DOWNLOADS` | 2 | Concurrent downloads, constrained to 1–2 |
| `IPID_ANALYSIS_RIPE_MEMORY_GIB` | 16 | Parser budget estimate: 2 GiB reserve plus 2 GiB per worker; minimum 4 |
| `IPID_ANALYSIS_RIPE_DUCKDB_THREADS` | 4 | Merge threads, also capped to leave one CPU available where possible |
| `IPID_ANALYSIS_RIPE_DUCKDB_MEMORY` | 8GB | DuckDB buffer-manager limit |

One resource lock under the shared processed root serializes RIPE preparation
across concurrent protocol jobs on the same VM. Parser pools use spawned
processes, avoiding fork-after-download-thread hazards. The memory budget is a
worker-sizing estimate, not an OS-level RSS limit; DuckDB allocations can also
exceed its buffer-manager limit. For a hard service-wide cap, use the VM's
service manager/cgroup. Logs report download, parsing and part/merge progress;
source manifests include elapsed parsing/preparation times. Start with these
defaults on a 120GB VM, then tune from measured CPU, I/O and RSS rather than
allocating all available memory.

For a reproducible accumulator benchmark (requires optional `psutil`):

```bash
python -m benchmarks.ripe_roles --addresses 200000 --repeats 5
```

It compares the old per-IP metadata layout with bitmap accumulation using the
same role observations; it excludes network, JSON parsing and decompression.
It reports elapsed time, sampled peak RSS and output checksums, not an end-to-end
production speedup. Cache data supports future T/D-based plots; RTT, per-probe
or other new traceroute attributes require separately retained raw sources.

Prepare a window explicitly with:

```bash
make prepare-ripe-atlas ARGS="--campaign-start 2026-09-21T02:09:12Z"
```

For explicitly selected public RIPE measurements, use the REST API instead.
The ID file may contain comma- or whitespace-separated measurement IDs:

```bash
make prepare-ripe-atlas ARGS="--campaign-start 2026-09-21T02:09:12Z \
  --source api --measurement-ids-file measurements.txt"
```

Previously downloaded or privately supplied RIPE JSONL/BZip2 files can be
imported with `--input-dir`. Automatic postprocessing reads the corresponding
`IPID_ANALYSIS_RIPE_SOURCE`, `IPID_ANALYSIS_RIPE_MEASUREMENT_IDS`,
`IPID_ANALYSIS_RIPE_INPUT_DIR`, `IPID_ANALYSIS_RIPE_DUMP_SAMPLES`, and
`IPID_ANALYSIS_RIPE_WORKERS`
settings. Set `IPID_ANALYSIS_SKIP_RIPE=1`
only when deliberately omitting the analysis.

Only public unicast IPv4 replies enter the lookup. T follows CAIDA's transit
definition: a public/private predecessor (including a known source) and a
public successor at a strictly later TTL are required; source and destination
addresses are excluded as transit within that trace. The last public response
and additional responses at the same TTL do not prove transit. If no source
address is present, a preceding observed public/private reply is required.
D means an observed responding destination, not merely the configured target.
Flags are OR-ed across traces. `Transit-Observed` is T=true, regardless of D;
`No Transit Evidence` includes both destination-only and SEEN-with-neither-flag
addresses. All four T/D counts are reported. Unmatched IP-ID targets are coverage and are
not treated as endpoints. Every matched IP address contributes once to its
role's strategy distribution, independently of its RIPE observation count.

Canonical stateless results and the TCP connection-oriented RT-base result
produce these artifacts beside the corresponding CAIDA results:

```text
*_ripe-atlas-strategy-join.pq
*_ripe-atlas-role-strategy-distribution.pq
*_ripe-atlas-role-by-strategy.{pdf,json}
*_ripe-atlas-os-role-join.pq
*_ripe-atlas-role-os-distribution.pq
*_ripe-atlas-role-by-os.{pdf,json}
*_caida-ripe-role-agreement.pq
*_caida-ripe-role-agreement.pdf
*_caida-ripe-intersection-by-strategy.pq
*_caida-ripe-intersection-role-by-strategy.{pdf,json}
```

The PDF uses the same strategy order, colors, typography, and compact legend as
the existing manuscript figures. When ITDK is enabled, the agreement artifact
reports the dual-matched populations where both, only CAIDA, only RIPE, or
neither source observed transit behavior.

All CAIDA/RIPE join artifacts are matched-only: unmatched measurement rows are
counted for coverage but are never copied into population-sized Parquet files.
Current outputs are reused when their strategy, role, and OS inputs have not
changed. To rerun only these analyses for an already completed campaign, use:

```bash
make analyse-network-roles \
  MANIFEST=data/analysis-jobs/icmp_2026-09-21_02-09-12/manifest.json
```

`make benchmark-network-roles` exercises the matched-only join design with
synthetic local Parquet files. Its timings demonstrate join/storage scaling,
not production end-to-end runtime: downloading, BZip2 decompression, JSON
parsing, RIPE preprocessing, object-storage I/O, and realistic data
compressibility are deliberately outside that benchmark.

## Strategy classification by measurement scale

Base measurements classify the position-dependent or cheaply identifiable
strategies `REFLECTION`, `CONSTANT`, `PER_DESTINATION`, `PER_CONNECTION`,
`SINGLE`, and `PER_BUCKET`. All other base sequences are `UNCLASSIFIED` and can
be passed to a mass measurement.

Complete mass-measurement sequences first use the same exact rules and priority
as base measurements: `REFLECTION`, `CONSTANT`, `PER_DESTINATION`,
`PER_CONNECTION`, `SINGLE`, and `PER_BUCKET`. The established
position-independent `CONSTANT` and `MULTI` rules then handle residual rows,
including rows with missing replies. Only the remaining sequences are tested
for `RANDOM` using the calibrated RANDOM-compatibility structure score described
below; all other sequences remain `UNCLASSIFIED`.

The exact rules require all 100 positions. Missing-reply markers are never
removed to create a shorter artificial sequence, so this does not introduce
loss-tolerant variants of the base rules. The 4 x 25 position layout and
missing-reply markers are retained for the RANDOM score's full, destination,
and connection views. Minimum reply-rate filtering is performed by
`ipid-measure` before fixed-interval rows are written, so analysis does not
duplicate that measurement-stage decision as an IPID strategy.

The classification produced by the measurement handoff is the authoritative
historical result. `make analyse data.json` first reuses an existing processed
`strategies.pq`, otherwise imports the persisted raw-measurement
`strategies.pq`, and only runs the classifier when neither exists. To
deliberately recompute all measurement classifications with the current code,
run:

```bash
make analyse data.json ARGS="--reclassify"
```

Reclassification does not replace the persisted historical input. Its result
may differ from the classification that selected the original fixed-interval
mass target when classifier rules have changed in the meantime.

To inspect why sampled mass-measurement rows retained a particular label, join
their processed strategy with the raw IP-ID sequence and step through ten
interactive plots:

```bash
make inspect-sequences ARGS="icmp.ipid.no-connection.fixed-interval.mass \
  --manifest data/analysis-jobs/<job-id>/manifest.json \
  --strategy UNCLASSIFIED --samples 10 --seed 42"
```

Each window shows the complete measurement order, both destination and all four
connection subsequences, missing replies, and the production RANDOM-score components.
Closing the window (or pressing Right/Space) advances to the next sample; Left
returns to the previous one and Q/Escape exits. On a headless SSH session, add
`--save-dir reports/sequence-inspection/<job-id>` to write the ten figures as
PNGs instead.

### Repeated RANDOM-decision analysis

Freeze a bounded sample of `UNCLASSIFIED` rows from one fixed-interval Mass
measurement together with an equally large RANDOM control cohort:

```bash
make prepare-random-reproducibility ARGS="prepare data.json \
  --target icmp.ipid.no-connection.fixed-interval.mass"
```

At most 5,000 `UNCLASSIFIED` rows are selected proportionally by limiting
component; the total cohort is capped at 10,000 targets. Half of the controls
are the accepted RANDOM rows closest to the production threshold; the remainder
is a deterministic random sample of the other RANDOM rows. The command stores
component p-values, inclusion weights for sampled `UNCLASSIFIED` strata, and
distance from every exact deterministic rule. Artifacts are written below the campaign's
`no-connection/fixed-interval-mass/random-reproducibility/` directory.

After measuring that same target file repeatedly with the established 4 x 25
fixed-interval configuration, evaluate the raw measurement ids directly:

```bash
make analyse-random-reproducibility ARGS="evaluate data.json \
  --repeat-id icmp-repeat-1 --repeat-id icmp-repeat-2"
```

The evaluator applies the production classifier in memory. It never creates or
overwrites `strategies.pq`. Per-repetition and per-address Parquet/CSV artifacts
report every component below threshold, the limiting component, classification
agreement, modal share, entropy, missing measurements, and the closest exact
deterministic rule. A compact PDF summarizes repeated class outcomes,
component rejection rates, and per-address stability.

For manifests produced by the current `ipid-measure` pipeline this workflow is
automatic: the Mass handoff prepares one frozen cohort, the manifest records
exactly five repeat measurement ids, and normal postprocessing evaluates them.
Targets missing from a repeat remain `NO_MEASUREMENT`; they are never replaced.

### Observed missing replies in Mass measurements

Analyze every fixed-interval Mass measurement present in a manifest without
reclassifying its sequences:

```bash
make analyse-missing-replies ARGS="data.json"
```

The command counts non-numeric positions in each stored `IPID_SEQUENCE` using
the same missing-value interpretation as the production classifier. It writes:

```text
data/processed/missing-replies/mass-missing-replies.pq
reports/figures/missing-replies/mass-missing-replies.json
reports/figures/missing-replies/mass-missing-replies.pdf
```

Normal manifest postprocessing runs this analysis automatically and additionally
writes measurement-specific Parquet/JSON/PDF artifacts directly below each
campaign's `no-connection/fixed-interval-mass/` directory.

The Parquet aggregate contains absolute, percentage, and cumulative counts for
every missing-reply count, separated by protocol and Mass measurement. The PDF
shows the exact distribution and ECDF. The JSON sidecar records the measurement
shape, configured minimum reply rate, complete/incomplete shares, and data
provenance.

These values are *observed missing replies*, not a causal estimate of network
packet loss. Requests can lack a stored reply because of either network
direction, target behavior, filtering, local capture loss, or a timeout.
Furthermore, `ipid-measure` persists only Mass sequences satisfying its minimum
reply rate. Measurements rejected before persistence are absent, so the output
describes the retained population (normally 0--20 missing replies for 4 x 25
sequences at an 80% minimum reply rate).

### Synthetic classifier validation

Generate reproducible, measurement-shaped IP-ID sequences and evaluate the
classifier independently of a campaign:

```bash
make validate-classifier
# Optional:
make validate-classifier ARGS="--samples-per-strategy 100000 --seed 42"
```

The paper figures and production mass classifier use the same final RANDOM score:
the minimum of raw-IPID uniformity, hierarchically aggregated multiscale
increment evidence, and full-sequence circular gap uniformity. The increment
component evaluates all usable resolutions from `{3, 4, 8, 16}` in every full,
destination, and connection view. It Fisher-combines the two disjoint
destination views and the four disjoint connection views, then takes the
minimum of full, destination, and connection evidence. The complete score and
threshold are calibrated together because those three families overlap. The
calibrated threshold and empirical-null specification are recorded in each
JSON sidecar. Production generates the deterministic empirical tables lazily
from the confirmed seeds and caches them for the lifetime of the process, so
streamed mass batches do not rebuild them.
The validation uses the same version, threshold, null-table sizes, and seeds as
production. To reproduce the pre-v7 production baseline for a historical
comparison instead, run:

```bash
python -m ipid_analysis.classifier_validation --legacy-random-score
```

By default, 100,000 sequences are generated per evaluated strategy.
`REFLECTION` and `CONSTANT` are deterministic, trivial cases and are fixed at
1,000 sequences each. `--samples-per-strategy` therefore controls the
nontrivial strategies.

Every synthetic generator samples uniformly across its complete valid
classifier domain: 16-bit starts and values use `0..65535`, `SINGLE` and
`PER_BUCKET` increments use `1..21845`, and `MULTI` uses 2 through 16
independently advancing counters with 16-bit starts, variable increments from
`1..800`, and randomly interleaved observations. Counter separation is not
enforced: natural overlap, merging, and wraparound remain in the standard
validation, and ground truth follows the generating mechanism. The previous
unambiguously separated circular-cluster model remains an implementation sanity
check but is excluded from the confusion matrices. The exact generator
parameters are recorded in every validation JSON sidecar.

The Base validation uses the real 4 x 4 round/connection interleaving and
evaluates `REFLECTION`, `CONSTANT`, `SINGLE`, `PER_CONNECTION`,
`PER_DESTINATION`, and `PER_BUCKET`. Base measurements retain only complete
16-reply results, so no Base loss case is generated. Two paper alternatives
compare the same ideal matrix against exactly three reordered positions
(18.75%) and exactly four reordered positions (25%). `MULTI` and `RANDOM` are
evaluated separately as out-of-scope Base inputs and must remain
`UNCLASSIFIED`.

The Mass validation uses 4 x 25 sequences and evaluates all eight generating
strategies under four conditions: ideal, exactly 20 missing replies, exactly 20
reordered positions, and 20 missing replies plus 16 reordered positions among
the remaining 80. Under impairment, exact-label recovery and the
safety-critical structured-as-RANDOM behavior can therefore be inspected
separately. Out-of-scope rejection rates and detected-output counts are written to
`reports/figures/classifier-validation/out-of-scope-classifier-rejection.json`
and are not mixed into the supported-strategy accuracy, precision, recall, or
F1 scores.

The raw-format synthetic sequences and detected labels are written to
`data/processed/classifier-validation/synthetic-classifier-validation.pq`.
Five focused confusion-matrix PDFs are written below
`reports/figures/classifier-validation/`. The two Base comparison JSONs retain
their metrics and reproducibility metadata; the obsolete combined Mass PDF and
JSON are removed because the three focused Mass PDFs replace them.

```text
base-4x4-classifier-confusion-reordered-3.pdf
base-4x4-classifier-confusion-reordered-4.pdf
mass-4x25-classifier-confusion-ideal.pdf
mass-4x25-classifier-confusion-lossy-vs-reordered.pdf
mass-4x25-classifier-confusion-lossy-vs-lossy-reordered.pdf
```

The four Mass datasets also evaluate the selected RANDOM-compatibility score:

```bash
make plot-mass-random-score-cdf
# Optional:
make plot-mass-random-score-cdf ARGS="--samples-per-strategy 100000 --seed 42"
```

The production score is
`S = min(raw_uniformity, increment_evidence, gap_uniformity)`. Raw uniformity
uses the established 16-bin Pearson test. Increment evidence is the empirical
multiscale test over the full, two destination, and four connection views. It
uses every resolution from `{3, 4, 8, 16}` that retains about two expected
transitions per bin, calibrates the minimum across those resolutions jointly,
uses only originally adjacent present positions, and is order-dependent. The
two destination p-values and four connection p-values are Fisher-combined only
inside their disjoint groups. Their group values and the overlapping full view
are combined by a minimum whose effect is included in the complete candidate
calibration. Gap uniformity compares the complete circular spacing distribution
of the present, sorted 16-bit IP-ID values against the same versioned discrete
empirical RANDOM null; it is order-independent and uses only the full sequence.
The fixed selected threshold is `tau = 7.89999176049605e-05`, calibrated at a
target 0.05% RANDOM false-rejection rate in the seed-20260927 final operating-point
confirmation. Increment and gap null tables use the same independent seeds as
that confirmation run, 20260938 and 20260950, respectively. A sequence is
RANDOM-compatible when `S >= tau`.

By default, the plotted nontrivial strategies use 100,000 sequences;
`REFLECTION` and `CONSTANT` remain fixed at 1,000. The exact complete-sequence
rules and the established `CONSTANT` and `MULTI` fallbacks keep precedence over
the RANDOM score. The PDFs and JSON metadata are written below
`reports/figures/classifier-validation/`; the underlying scores and decisions
are stored in
`data/processed/classifier-validation/mass-4x25-random-score-cdf.pq`. The four
PDFs are:

```text
mass-4x25-random-score-cdf-ideal.pdf
mass-4x25-random-score-cdf-lossy.pdf
mass-4x25-random-score-cdf-reordered.pdf
mass-4x25-random-score-cdf-lossy-reordered.pdf
```

The
logarithmic x-axis reserves one decade of space below the smallest score,
labels every power of ten as a major tick, and uses the customary unlabeled
logarithmic minor ticks. Strategies whose complete CDF coincides at the
numerical score floor are additionally marked in their strategy colors on that
shared vertical line.

### Adapted NIST SP 800-22 short-sequence baseline

Generate a reproducible comparison baseline derived from the established NIST
SP 800-22 Rev. 1a tests:

```bash
make plot-nist-short-sequence-baseline
# Fast smoke run, not paper-conclusive:
make plot-nist-short-sequence-baseline ARGS="--samples-per-strategy 10 --null-table-samples 100 --batch-size 10"
```

This is explicitly an adapted comparison baseline, not a claim that the short
IP-ID sequences have undergone NIST validation. Every present IP-ID is encoded
as an unsigned, big-endian 16-bit word in measurement order. Missing replies
are omitted and never replaced by artificial bits. Complete Mass sequences
therefore contain 1,600 bits; the exact 20%-loss cases contain 1,280 bits.

Only tests whose SP 800-22 input-size recommendations can be met at both
lengths are included: Frequency, Block Frequency (M=32), Runs, Longest Run
(M=8), Spectral DFT, both Serial outputs (m=4), Approximate Entropy (m=4), and
forward/reverse Cumulative Sums. Rank, template, Universal, Linear Complexity,
and excursion tests are recorded as not applicable with their exclusion reason
in the JSON report.

The minimum of the ten dependent component p-values is not treated as another
ordinary p-value. It is calibrated against IID-bit Monte Carlo null tables for
the exact 1,280- or 1,600-bit length, using an add-one correction. The combined
compatibility score is classified RANDOM at the same target 0.05% true-RANDOM
false-rejection rate used for the selected IP-ID-specific candidate. This keeps
the comparison fair without changing the published component statistics.

The command evaluates Ideal, 20% Lossy, 20% Reordered, and 20% Lossy plus 20%
Reordered. Every CDF starts at the power of ten immediately below its smallest
positive score. It writes four score CDFs and a four-panel component-by-strategy
heatmap below `reports/figures/classifier-validation/nist-baseline/`, plus a
compact review bundle and the detailed Parquet score table below
`data/processed/classifier-validation/nist-baseline/`:

```text
mass-4x25-nist-score-cdf-ideal.pdf
mass-4x25-nist-score-cdf-lossy.pdf
mass-4x25-nist-score-cdf-reordered.pdf
mass-4x25-nist-score-cdf-lossy-reordered.pdf
nist-test-strategy-heatmap.pdf
nist-baseline-review-bundle.zip
mass-4x25-nist-baseline-scores.pq
```

In the heatmap, each named NIST component row reports the percentage of
sequences whose component p-value is at least 0.01. The separated `Combined
score` row is not another NIST component or an average: it reports the
percentage whose empirically calibrated minimum-component score is at least
the combined threshold of 0.0005.

### Held-out RANDOM-classifier paper diagnostics

Compare the pre-v7 production score, the adapted NIST baseline, and the final
production score on the independent held-out v2 generators:

```bash
make plot-random-classifier-diagnostics
# Fast smoke run, not paper-conclusive:
make plot-random-classifier-diagnostics ARGS="--samples-per-generator 10 --candidate-null-samples 100 --nist-null-samples 100 --batch-size 10"
```

The diagnostics use all four impairment conditions and do not mutate classifier
state. They write:

```text
random-classifier-candidate-confusion.pdf
random-classifier-method-confusion.pdf
random-classifier-candidate-by-generator.pdf
```

The first figure isolates the binary structured-versus-RANDOM decision. The
second compares all three methods at their declared thresholds. The third
retains each held-out generator separately so difficult jittered, drifting,
bursty, per-scope, clustered, and multi-counter families are not hidden behind
near-perfect aggregate accuracy. Non-zero confusion-matrix cells below 0.1%
are rendered as `<0.1` rather than `0.0`.

### RANDOM metric-selection experiment

Reproduce the historical selection experiment over the four pre-v7 production
components plus increment-uniformity and circular gap-uniformity metrics:

```bash
# Fast pipeline/plot smoke test (not statistically conclusive)
make evaluate-random-classifier ARGS="--samples-per-strategy 1000 --calibration-samples-per-condition 2000 --null-table-samples 5000 --batch-size 1000 --seed 42"

# Full comparison run
make evaluate-random-classifier ARGS="--samples-per-strategy 100000 --calibration-samples-per-condition 250000 --null-table-samples 500000 --target-random-frr 0.0001 --batch-size 10000 --seed 42"
```

The increment test preserves the original measurement positions: an increment
is formed only when both originally adjacent positions in the selected full,
destination, or connection view are present. It never bridges a missing reply.
The number of equal-width bins is selected from the observed transition count:
the largest power of two that retains at least five expected transitions per
bin, capped at 16. Views with fewer than ten transitions are uninformative.
Pearson statistics are converted to empirical multinomial null probabilities,
so short and long views produce comparable compatibility scores.

The gap test sorts the present 16-bit values, includes duplicate zero-gaps and
the circular wraparound gap, and calculates a Cramer-von-Mises discrepancy for
the complete spacing distribution. Its empirical discrete-uniform null table
is conditioned on the number of present values, making the statistic invariant
to packet order and calibrated for each tested loss level.

The experiment covers ideal data; random 5%, 10%, and 20% loss; 20% loss plus
20% reordering of present values; and 20% loss concentrated in one destination
or one connection. In addition to the eight standard strategy generators, it
includes constant-step SINGLE counters at low, medium, and high rates, a
jittered high-rate SINGLE counter, and non-clustered interleavings of two, four,
or eight independent counters. It calculates each metric once, calibrates all
63 non-empty metric subsets as complete minimum-score decision rules, evaluates
False-RANDOM and true-RANDOM false rejection, benchmarks standalone metric
cost, and reports the non-dominated subsets. Calibration and held-out test
generators use independent deterministic random streams.

Compact reports are written to
`data/processed/classifier-validation/random-classifier-evaluation/`:

```text
summary.json
subset-results.csv
subset-by-scenario.csv
pareto-frontier.csv
recommendations.txt
run.log
random-classifier-review-bundle.zip
metric-scores.pq                 # large, detailed per-sequence scores
```

Plots are written to
`reports/figures/classifier-validation/random-classifier-evaluation/`:

```text
metric-false-random-heatmap.pdf
subset-pareto-tradeoff.pdf
```

The ZIP review bundle contains all compact CSV, JSON, text, log, and PDF
artifacts, but deliberately excludes the potentially large Parquet score table.
The report contains both standalone metric timings and measured timings for
every complete subset. Occupancy and maximum-gap candidates share their sort
and feature pass in the subset benchmark. Gap-uniformity still has its own sort;
close finalists should be benchmarked again after the selected production code
has been optimized.

### Accuracy-first RANDOM metric-selection experiment (v2)

Version 2 removes classifier-threshold leakage from the structured generators.
Counter steps span the complete `1..65535` domain without importing the
production increment or clustering thresholds. It uses independent selection
and held-out generator profiles, adds burst-loss conditions, evaluates all 63
metric subsets, and compares minimum, Fisher, Cauchy, and selection-weighted
minimum combination rules. Every rule is calibrated against dependent
true-RANDOM metric scores rather than an asymptotic combined-p-value formula.

```bash
# Fast end-to-end smoke run (not statistically conclusive)
make evaluate-random-classifier-v2 ARGS="--selection-samples-per-strategy 100 --test-samples-per-strategy 100 --weight-training-samples-per-strategy 100 --calibration-samples-per-condition 1000 --null-table-samples 5000 --target-random-frr 0.01 --batch-size 100 --seed 42"

# Accuracy-first full run
make evaluate-random-classifier-v2 ARGS="--selection-samples-per-strategy 100000 --test-samples-per-strategy 500000 --weight-training-samples-per-strategy 50000 --calibration-samples-per-condition 1000000 --null-table-samples 1000000 --target-random-frr 0.0001 --batch-size 10000 --seed 20260925"
```

The selection profile learns only the weighted-minimum allocation and provides
development-set comparisons. The independently seeded held-out profile uses a
different step distribution, broader jitter and drift, and more imbalanced
multi-counter traffic. Accuracy reports prioritize worst-case, p95, and
production-residual-like False-RANDOM rates; runtime is reported but used only
after accuracy unless costs differ materially.

Compact reports are written to
`data/processed/classifier-validation/random-classifier-evaluation-v2/`:

```text
summary.json
combination-results.csv
combination-by-scenario.csv
step-sensitivity.csv
heldout-pareto-frontier.csv
recommendations.txt
run.log
random-classifier-review-bundle-v2.zip
heldout-metric-scores.pq         # large, excluded from the review ZIP
```

Plots are written to
`reports/figures/classifier-validation/random-classifier-evaluation-v2/`:

```text
metric-false-random-heatmap.pdf
combination-accuracy-tradeoff.pdf
metric-step-sensitivity.pdf
```

The ZIP is the preferred review artifact because it contains every compact
table, the recommendations, the run log, and all three plots.

### Increment-uniformity bin-rule ablation

The bin-rule experiment that selected the production rule holds
`S = min(raw, increment, gap)` fixed and compares:

- the current power-of-two rule with about five expected transitions per bin;
- a finer power-of-two rule with about three expected transitions per bin;
- both power-of-two rules with a three-bin fallback whenever the original
  rule would select only two bins;
- fixed three-bin thirds;
- adaptive `3/6/9/12` thirds with targets of five, three, and two expected
  transitions per bin (always capped at 12 bins);
- jointly Monte-Carlo-calibrated multiscale tests over usable resolutions from
  `{3, 4, 8, 16}` and `{3, 6, 9, 12}`.

Ternary rules use the exact discrete probabilities induced by mapping all
65,536 increments into bins; they do not assume that 65,536 is divisible by
three. The multiscale candidate calibrates the minimum across scales against an
independent joint null distribution, so it is not treated as an ordinary
uncorrected p-value. Every complete Raw+Increment+Gap candidate receives its
own threshold at the same true-RANDOM false-rejection target. Production code
and constants remain unchanged.

The two multiscale candidates provide a direct practice-oriented comparison
between diverse dyadic resolutions and resolutions aligned with the one-third
MAX_INC support. Each bin set receives its own joint empirical null
distribution. The `power2-e5-min3` and `power2-e3-min3` candidates remain
single-scale tests: they select exactly one resolution per view. They differ
from their baselines only when the baseline would use two bins, in which case
they use three. This deliberately permits fewer than the nominal `e5` or `e3`
expected observations per bin for those short views. It is distinct from the
multiscale candidates, which evaluate several resolutions simultaneously and
apply a joint multiple-testing calibration.

```bash
# Fast end-to-end smoke run (not statistically conclusive)
make evaluate-increment-bin-rules ARGS="--paper-samples-per-strategy 100 --selection-samples-per-strategy 100 --test-samples-per-strategy 100 --calibration-samples-per-condition 1000 --null-table-samples 5000 --target-random-frr 0.01 --batch-size 100 --seed 20260926"

# Accuracy-first full run
make evaluate-increment-bin-rules ARGS="--paper-samples-per-strategy 500000 --selection-samples-per-strategy 100000 --test-samples-per-strategy 500000 --calibration-samples-per-condition 1000000 --null-table-samples 1000000 --target-random-frr 0.0001 --batch-size 10000 --seed 20260927"
```

The full command deliberately uses a fresh seed for the final multiscale-bin-set
comparison because the earlier seed already informed the challenger design.

The `paper` profile explicitly includes the existing MAX_INC-bounded
Per-Bucket generator. The independent `selection` and `heldout` profiles span
the complete increment domain and broader jitter, drift, burst, and
multi-counter behavior. All profiles are evaluated under ideal, random and
concentrated loss, global reordering, and combined impairments.

Compact review artifacts are written to
`data/processed/classifier-validation/increment-bin-rule-evaluation/`:

```text
summary.json
bin-rule-results.csv
bin-rule-by-scenario.csv
bin-rule-step-sensitivity.csv
recommendations.txt
run.log
increment-bin-rule-review-bundle.zip
```

Plots are written to
`reports/figures/classifier-validation/increment-bin-rule-evaluation/`:

```text
bin-rule-false-random-heatmap.pdf
bin-rule-accuracy-summary.pdf
```

For review, send the ZIP. It contains all compact tables, both plots, the
recommendations, and the generation log.

## Merging base and mass strategies

The canonical no-connection RT-base and fixed-interval-mass results can be
merged after both measurements have been classified:

```bash
python ipid_analysis/strategy_merge.py \
  tcp.ipid.no-connection.rt-based.base \
  tcp.ipid.no-connection.fixed-interval.mass

python ipid_analysis/plot_strategies.py \
  tcp.ipid.no-connection.rt-based.base \
  tcp.ipid.no-connection.fixed-interval.mass
```

A classified base strategy is retained. `UNCLASSIFIED` base rows are replaced
by their mass strategy; if their intended mass probe produced no stored row,
they become `NOT_ENOUGH_SAMPLES`. Base probe failures do not appear in the
merged result because the base strategies file defines its population.

| Base | Mass | Merged |
|---|---|---|
| classified strategy | not targeted | base strategy |
| `UNCLASSIFIED` | classified strategy | mass strategy |
| `UNCLASSIFIED` | `UNCLASSIFIED` | `UNCLASSIFIED` |
| `UNCLASSIFIED` | probe failed | `NOT_ENOUGH_SAMPLES` |
| probe failed | not targeted | omitted |

The commands above write:

```text
data/processed/<zmap-id>/no-connection/merged/rt-based-base_fixed-interval-mass/n-rt-b_fi-m_strategies.pq
reports/figures/<zmap-id>/no-connection/merged/rt-based-base_fixed-interval-mass/n-rt-b_fi-m_strategies.pdf
reports/figures/<zmap-id>/no-connection/merged/rt-based-base_fixed-interval-mass/n-rt-b_fi-m_strategies.json
```

`postprocess.py` performs these canonical merges and plots automatically after
all individual measurements in the manifest have been processed.

### RT-based to fixed-interval strategy refinement

For every canonical no-connection RT-base and fixed-interval-mass pair,
`make analyse data.json` also creates a compact ACM-width stacked-bar figure.
The upper bar contains the complete RT-based strategy distribution. The lower
bar contains the strategy distribution of the fixed-interval mass result, whose
target population is exactly the addresses classified as `UNCLASSIFIED` by the
RT-based measurement. Light guide lines expand the RT `UNCLASSIFIED` segment to
the fixed-interval bar.

The renderer validates this population relationship and fails instead of
creating a misleading figure if a fixed-interval address was not RT
`UNCLASSIFIED`. Probe failures that make the stored fixed-interval result smaller
than the target population are shown as `NOT_ENOUGH_SAMPLES`, so the lower bar
still represents every intended follow-up target and sums to 100%.

```bash
python ipid_analysis/plot_strategy_refinement.py \
  tcp.ipid.no-connection.rt-based.base \
  tcp.ipid.no-connection.fixed-interval.mass \
  --manifest data.json
```

The generated artifacts are:

```text
data/processed/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_measurement-type-by-strategy.pq

reports/figures/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_measurement-type-by-strategy.pdf
    n-rt-b_fi-m_measurement-type-by-strategy.json
```

For TCP campaigns that also contain
`tcp.ipid.connection.rt-based.base`, the same analysis run creates a second
figure with a third `RT-based & Connection-oriented` bar. The original
two-bar figure remains unchanged. The three-bar variant can also be rendered
directly:

```bash
python ipid_analysis/plot_strategy_refinement.py \
  tcp.ipid.no-connection.rt-based.base \
  tcp.ipid.no-connection.fixed-interval.mass \
  tcp.ipid.connection.rt-based.base \
  --manifest data.json
```

Its additional artifacts are:

```text
data/processed/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_measurement-type-by-strategy-with-connection.pq

reports/figures/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_measurement-type-by-strategy-with-connection.pdf
    n-rt-b_fi-m_measurement-type-by-strategy-with-connection.json
```

### TCP flags by merged strategy

For TCP, `make analyse data.json` also joins the merged RT-based-base and
fixed-interval-mass strategy result to the original ZMap `REPLY_TYPE` by
`IP_ADDR`. It creates three independently normalized bars: all recognized TCP
replies (`SYN-ACK/RST`), only `SYN-ACK`, and only `RST`. The label is `RST`
rather than `RST-ACK` because ZMap persists the reply classification `rst`, not
the complete received TCP flag set.

```bash
python ipid_analysis/plot_tcp_flags_strategy.py \
  tcp.ipid.no-connection.rt-based.base \
  tcp.ipid.no-connection.fixed-interval.mass \
  --manifest data.json
```

The generated artifacts are:

```text
data/processed/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_tcp-flags-by-strategy.pq

reports/figures/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_tcp-flags-by-strategy.pdf
    n-rt-b_fi-m_tcp-flags-by-strategy.json
```

### Operating-system groups by strategy

For every ICMP, TCP, or UDP-DNS campaign with an `os` measurement,
`make analyse data.json` joins the protocol's OS fingerprints from
`data/raw/os/<os-id>/os.pq` to that protocol's merged RT-based-base and
fixed-interval-mass strategies by `IP_ADDR`. Postprocessing validates the
current raw schema and maps every resolved `OS_TAG` to exactly one `OS_GROUP`.
It stores the per-IP result in
`data/processed/os/<os-id>/os-groups.pq` and creates one
ACM-width heatmap per protocol, split into represented `General-Purpose OS`,
`Network / Appliance OS`, and `Embedded / RTOS` sections. Every group row is
normalized independently to 100%, while its matched IP-address count is shown
beside the row label. Within each section, groups are ordered by descending
IP-address count. Exact zero cells are
displayed as `-`.
All nine IP-ID selection strategies plus the `NOT_ENOUGH_SAMPLES` follow-up
outcome remain visible even when a complete column is zero. Each OS-group row
therefore represents its complete matched merged population and still sums
to 100%.

The S3 analysis worker downloads `os-coverage.json` beside raw `os.pq`, so the
target, classification, per-service response/evidence/tag, and conflict counts
recorded by the measurement remain available for coverage reporting.

When `tcp.ipid.connection.rt-based.base` is present, the same analysis also
creates an identical heatmap for that individual connection-oriented strategy
result. It uses the same OS groups, strategy columns, row normalization,
ordering, labels, and color scale as the merged TCP plot.

The taxonomy covers every canonical OS tag emitted by `ipid-measure`. Linux
distributions, FreeBSD, OpenBSD, NetBSD, SONiC, and SonicWall retain separate
groups where their IP-ID behavior can differ. Product variants such as Cisco
IOS, IOS XE, IOS XR, NX-OS, ASA, and FTD map surjectively to the `cisco` group.
Ambiguous and unclassified rows remain quantitative measurement outcomes in
raw `os.pq`; the group population contains resolved rows only.

DuckDB performs the tag-to-group projection and strategy join using streaming
Parquet scans and bounded memory. The processed group file is reused by the TCP
connection and merged analyses when the source file and taxonomy version match.
Python receives only the compact group-by-strategy aggregate used for plotting.

```bash
make analyse data.json
```

The generated artifacts are:

```text
data/processed/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_operating-system-group-by-strategy.pq

data/processed/os/<os-id>/
  os-groups.pq
  os-groups.meta.json

reports/figures/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_operating-system-group-by-strategy.pdf
    n-rt-b_fi-m_operating-system-group-by-strategy.json

data/processed/<zmap-id>/connection/rt-based-base/
  c-rt-b_operating-system-group-by-strategy.pq

reports/figures/<zmap-id>/connection/rt-based-base/
  c-rt-b_operating-system-group-by-strategy.pdf
  c-rt-b_operating-system-group-by-strategy.json
```

## ACM comparison figures

For every protocol and connection mode that has both an RT-based base run and a
fixed-interval base run, `make analyse data.json` additionally creates three
compact, title-free paper figures:

1. split violin plots of the per-IP median probing interval by MaxMind continent
   (limited to p99.5),
2. paired empirical increment CDFs for `SINGLE`, `PER_DESTINATION`,
   `PER_CONNECTION`, and `PER_BUCKET` (limited to p99.9), and
3. a row-normalized strategy-intersection heatmap.

The MaxMind figure needs a GeoLite2/GeoIP2 Country or City database. Put it at
`references/GeoLite2-Country.mmdb`, set `IPID_MAXMIND_DB`, or pass it directly:

```bash
python ipid_analysis/postprocess.py data.json \
  --maxmind-db /path/to/GeoLite2-Country.mmdb

python ipid_analysis/paper_figures.py \
  udp-dns.ipid.connection.rt-based.base \
  udp-dns.ipid.connection.fixed-interval.base \
  --manifest data.json \
  --maxmind-db /path/to/GeoLite2-Country.mmdb
```

If no MaxMind database is configured, the increment and intersection figures
are still produced and only the continent figure is skipped with a warning.
For a connection-mode comparison, the artifacts are written as:

```text
data/processed/<zmap-id>/<connection-mode>/comparison/
  rt-based-base_fixed-interval-base/
    <n|c>-rt-b_fi-b_ip-continents.pq
    <n|c>-rt-b_fi-b_probing-intervals-by-continent.pq
    <n|c>-rt-b_fi-b_increment-distributions.pq
    <n|c>-rt-b_fi-b_strategy-intersection.pq

reports/figures/<zmap-id>/<connection-mode>/comparison/
  rt-based-base_fixed-interval-base/
    <n|c>-rt-b_fi-b_probing-intervals-by-continent.{pdf,json}
    <n|c>-rt-b_fi-b_increment-distributions.{pdf,json}
    <n|c>-rt-b_fi-b_strategy-intersection.{pdf,json}
```

The aggregate Parquets make the plotted values independently inspectable. Each
JSON sidecar records the source measurements, aggregation/normalization method,
percentile handling, population sizes, and generation timestamp.

## Manifest and artifact naming

The campaign manifest uses descriptive keys for connection and interval modes:

```json
{
  "tcp": {
    "zmap": "tcp-80_<timestamp>",
    "os": "tcp-80_<timestamp>",
    "ipid": {
      "no-connection": {
        "rt-based": {"base": "tcp-80_<timestamp>"},
        "fixed-interval": {
          "base": "tcp-80_<timestamp>",
          "mass": "tcp-80_<timestamp>"
        }
      },
      "connection": {
        "rt-based": {"base": "tcp-80_<timestamp>"},
        "fixed-interval": {"base": "tcp-80_<timestamp>"}
      }
    }
  }
}
```

CLI targets use the same names, for example
`tcp.ipid.no-connection.fixed-interval.mass`.

Every generated campaign artifact uses one shared layout below its ZMap run:

```text
<zmap-id>/
└── <no-connection|connection>/
    └── <rt-based|fixed-interval>-<base|mass>/
        └── <n|c>-<rt|fi>-<b|m>_<kind>.<pq|pdf|json>
```

For example, the mass fixed-interval strategy artifacts without established
connections are written as:

```text
data/processed/<zmap-id>/no-connection/fixed-interval-mass/n-fi-m_strategies.pq
reports/figures/<zmap-id>/no-connection/fixed-interval-mass/n-fi-m_strategies.pdf
reports/figures/<zmap-id>/no-connection/fixed-interval-mass/n-fi-m_strategies.json
```

## Project Organization

```
├── LICENSE            <- Open-source license if one is chosen
├── Makefile           <- Makefile with convenience commands like `make data` or `make train`
├── README.md          <- The top-level README for developers using this project.
├── data
│   ├── external       <- Data from third party sources.
│   ├── interim        <- Intermediate data that has been transformed.
│   ├── processed      <- The final, canonical data sets for modeling.
│   └── raw            <- The original, immutable data dump.
│
├── docs               <- A default mkdocs project; see www.mkdocs.org for details
│
├── models             <- Trained and serialized models, model predictions, or model summaries
│
├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
│                         the creator's initials, and a short `-` delimited description, e.g.
│                         `1.0-jqp-initial-data-exploration`.
│
├── pyproject.toml     <- Project configuration file with package metadata for 
│                         ipid_analysis and configuration for tools like black
│
├── references         <- Data dictionaries, manuals, and all other explanatory materials.
│
├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
│   └── figures        <- Generated graphics and figures to be used in reporting
│
├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
│                         generated with `pip freeze > requirements.txt`
│
├── setup.cfg          <- Configuration file for flake8
│
└── ipid_analysis   <- Source code for use in this project.
    │
    ├── __init__.py             <- Makes ipid_analysis a Python module
    │
    ├── config.py               <- Store useful variables and configuration
    │
    ├── dataset.py              <- Scripts to download or generate data
    │
    ├── features.py             <- Code to create features for modeling
    │
    ├── modeling                
    │   ├── __init__.py 
    │   ├── predict.py          <- Code to run model inference with trained models          
    │   └── train.py            <- Code to train models
    │
    └── plots.py                <- Code to create visualizations
```

--------

## Offline RANDOM view-evidence evaluation

The production-independent evaluator compares the selected production score
against increment evidence aggregation, circular-gap subsequences, and several
single-/multiscale increment-bin rules.  Its primary impairment model uses global
reordering. Component-only candidates are calibrated separately so the catch
attribution and redundancy comparison remain fair. Start with the bounded
screening preset:

```bash
make evaluate-random-classifier-views ARGS="--preset screening"
```

The run stores reusable primitive full/destination/connection scores in
`data/processed/classifier-validation/random-classifier-view-evaluation/screening/view-scores.pq`.
CSV/JSON summaries, plots, logs, and a compact review ZIP are written beside it.
`recommendations.txt` prints the accuracy-first shortlist.  Confirm only that
shortlist with the larger preset and a comma-separated `--variants` value:

```bash
make evaluate-random-classifier-views ARGS="--preset confirmation --variants '<candidate-a>,<candidate-b>'"
```

Both presets calibrate every complete score at RANDOM false-rejection targets
of 0.01%, 0.025%, 0.05%, and 0.1%; the confirmed 0.05% target is the primary
operating point. The evaluator also compares raw-IPID uniformity with
8, 10, 12, and 16 bins for the selected aggregated multiscale candidate. The
confirmation preset uses one million null samples per null table so its
lower-tail resolution is appropriate for the final threshold.
It computes only the increment-bin rules and gap views required by the selected
variants; the current baseline is added automatically. This keeps a focused
confirmation run practical without changing its statistical specification.
`operating-point-results.csv`, `operating-point-by-scenario.csv`, and
`raw-bin-ablation.csv` contain the focused final-tuning results, while
`final-tuning-tradeoff.pdf` visualizes the main trade-off. The summary records
the base, increment-table, and gap-table seeds explicitly. The command never
changes the production classifier.

--------



### Six-site campaigns

The additive continental workflow and the worker's reserved critical/background
scheduling are described in [docs/continental-workflow.md](docs/continental-workflow.md).

Both protocol-only and continental workflows support the producer's explicit
`SKIP_OS=1` mode, with persistent intent and non-OS completion. See
[docs/continental-workflow.md](docs/continental-workflow.md#explicit-no-os-mode-in-both-workflows).
