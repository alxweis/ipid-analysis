# S3 storage and analysis runtime

## Deployment contract

The supported production layout is three existing rclone mounts at `data/raw`,
`data/processed`, and `reports`, with local control files at `tmp` below the
repository and bounded RAM scratch on an existing tmpfs. See
[RAM and S3 processing](RAM_S3_PROCESSING.md) for the current default, budgets,
compatibility mode and migration. Nothing installs, restarts, or reconfigures a mount,
changes Ansible, or migrates/deletes existing measurement objects.

Use Python 3.12 and the frozen dependency set:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-py312.lock
export IPID_ANALYSIS_STORAGE_MODE=mounted-s3
export IPID_ANALYSIS_TMP_DIR=/mnt/data01/ipid-analysis/tmp
export IPID_ANALYSIS_S3_PREFIX=s3://bucket/workflow-prefix
export IPID_ANALYSIS_S3_ROOTS='{"raw":"s3://bucket/prefix/raw","processed":"s3://bucket/prefix/processed","reports":"s3://bucket/prefix/reports"}'
make workflow-worker
```

The URI examples are placeholders. Set the mapping to the exact canonical
`s3cmd` URI prefixes in the measurement requests. A provider-specific rclone
remote such as `s3-zih:netds3o:aweis/ipid-2/raw` is **not** necessarily a
literal `s3://` bucket/key translation. No credentials belong in this mapping.
The raw mapping is mandatory at production worker startup. All roots should
be specified to recognize identical mounted output destinations as well.

`workflow-worker` defaults to `mounted-s3`. For standalone CLI commands, set
`IPID_ANALYSIS_STORAGE_MODE=mounted-s3` explicitly. In this mode missing mounts
fail before persistent directories/files are created, and scratch cannot
overlap a persistent root or live on a FUSE filesystem. The production runtime
targets Linux with `/proc`, pidfd APIs, a same-user rclone mount process, and
access to the mount's existing rclone configuration. Ubuntu 24.04 satisfies the
kernel/Python prerequisites; actual endpoint behavior still needs the smoke
test below. A mounted command fails clearly if the owning rclone process cannot
be identified. Do not run the analysis as a different mount owner.

Historical local installations remain available with
`make workflow-worker ARGS="--storage-mode local"`. Other CLIs default to local
mode when the environment variable is absent; actual rclone roots are detected
and require explicit production mode. Existing `Path` APIs and measurement/job
schemas remain intact. Installing the lock is the reproducible Python 3.12
baseline; `requirements.txt` remains the dependency source for intentional
upgrades. Regenerate the lock deliberately and rerun the entire suite.

## Data flow and publication

Raw measurement files already present through the mount are read in place.
The worker verifies canonical URI mapping, object size through `s3cmd info`,
and FUSE visibility before reuse; it does not download a second measurement
copy or hydrate a missing mount. Legacy local mode retains downloads.

In mounted-S3 mode DuckDB spills, parser parts, HTTP resume state, ZipFile
construction and incomplete Parquet outputs use RAM scratch. Locks, task specs,
supervisor logs and retained diagnostic logs use the local control directory.
Explicit disk compatibility mode retains local ext4 data scratch. Completed
artifacts are staged **one file at a time**, validated, hashed, and published.
Local destinations use atomic replace. Remote destinations use a known-size
`rclone copyto` through the exact remote/configuration of the owning mount.
This is required for reliable publication: a terminated unknown-length FUSE
write must not replace a good object with a complete-looking truncated stream.
It is not a benchmark-based switch of the analytical read backend.

No VFS cache is enabled or assumed. In particular, `vfs-cache-mode=off` does not
provide POSIX random-write, seekable-update, rename, or multi-object transaction
semantics. Parquet footers and ZIP indexes are completed locally. After native
publication the runtime sends SIGHUP only to the identified mount via a pidfd,
clearing its directory cache, then waits for the expected size/modification time.
It neither restarts the mount nor changes its options. This cache invalidation
is necessary because writes through a separate rclone process otherwise leave
the mount's directory view stale. The behavior is documented for
[rclone mount v1.60.1](https://github.com/rclone/rclone/blob/v1.60.1/docs/content/commands/rclone_mount.md).
Native known-size S3 uploads and metadata mtime handling are implemented in
[rclone's v1.60.1 S3 backend](https://github.com/rclone/rclone/blob/v1.60.1/backend/s3/s3.go).

Each managed artifact has a small hidden companion:
`<parent>/.ipid-complete/<filename>.json`. Publication first records `writing`,
then uploads data, then records `complete` with byte count, SHA-256, observed
mtime, optional expected rows, source identities, parameters, Python/library
versions, and a digest of the analysis source code. Failed replacements retain
previous completion evidence in the writing marker. Readers reject unfinished
markers and size/mtime changes. A valid Parquet footer alone is insufficient:
a prefix containing fewer rows than expected is rejected.

This is a completion protocol for cooperating readers, not an S3 transaction
or immutable generation store. External readers must wait for the workflow
`done.json` or validate completion metadata; external writers must not modify
these objects during a run. Content is hashed at publication; normal cache
checks use marker/size/mtime without rereading every multi-GB object. Unmanaged
legacy files remain supported and are validated where imported; missing
dependency metadata causes derived caches to rebuild. Adoption records the
validation operation, not a fabricated historical software version.

The handoff preserves `strategies.pq` schema/metadata and adds portable
`classification-provenance.json` beside it. Interprotocol workflow exports use
an explicit current-run publication inventory, including PNGs, plus a portable
`artifacts.json` with sizes/hashes. Files left over from older runs and partial
files are not exported by recursive directory globbing.

## Resources and scale

All production writers share a local kernel lease; the worker's postprocess
child inherits its ownership. Locks are released by the kernel after crashes.
One sequential worker serves all protocol VMs. This is single-host coordination,
not distributed locking between independently deployed analysis VMs.

DuckDB connections are scoped/closed and own a unique local spill directory,
removed even on exceptions. CPU affinity/cgroup limits and current available
RAM bound threads, Arrow/NumPy batches, and import concurrency. DuckDB defaults
to at most half the currently available memory; its spill ceiling respects
free scratch minus a reserve. Concurrent RIPE downloads reserve estimated
scratch space before admission. These are conservative per-process estimates,
not a whole-system memory guarantee.

Useful controls (existing CLI thread/batch arguments remain supported):

| Environment | Meaning/default |
| --- | --- |
| `IPID_ANALYSIS_TMP_DIR` | dedicated local scratch; `<repo>/tmp` |
| `IPID_ANALYSIS_THREADS` | CPU budget; affinity/cgroup-derived by default |
| `IPID_ANALYSIS_DUCKDB_MEMORY` | explicit byte quantity, capped at half available memory |
| `IPID_ANALYSIS_SCRATCH_RESERVE_BYTES` | unallocated reserve; 1 GiB |
| `IPID_ANALYSIS_S3_TIMEOUT` | command timeout; ordinary s3cmd 3600s, native publication 7200s |
| `IPID_ANALYSIS_VISIBILITY_TIMEOUT` | mount visibility wait; 330s |
| `IPID_ANALYSIS_POSTPROCESS_TIMEOUT` | child deadline; 172800s |
| `IPID_ANALYSIS_CAMPAIGN_TIMEZONE` | timezone for timestamp-only measurement IDs; UTC |

Three hundred million rows must not become a Python list/dataframe. Cohort
selection uses bounded per-stratum heaps and preserves the historical stable
selection and ordering. Interprotocol summaries/alternative joins and
deterministic transition exports stream batches or use spillable DuckDB SQL.
Full diagnostics are limited to the selected bounded cohort. Increments use
the authoritative saved classifications rather than silently reclassifying
historic data. Source fingerprints invalidate strategy-dependent increments,
merges, OS and role caches when inputs change.

Streaming bounds RAM; it does **not** make arbitrary output fit a 280 GiB local
disk. A wide uncompressed CSV or concurrent parser/spill/output combination can
exhaust scratch. Current behavior is explicit failure without a completed job,
retaining control evidence and freeing failed job bulk files where possible.
Capacity must be measured on representative data. Direct multipart streaming
or new partitioned formats require separate evidence/design; they are not
silently introduced here. Offline calibration retains its exact arrays/RNG
batching/quantiles rather than replacing scientific statistics with estimates.

## Completion, failure and recovery

The worker invokes postprocess with `--strict --status-file`. Exit code zero
alone does not prove success: the report must match the manifest, contain every
mandatory canonical artifact, and show required stages completed. Missing raw
inputs, invalid snapshots, duplicate/null IP keys, invalid persisted labels,
and incomplete output rows fail explicitly. External ITDK/RIPE enrichment may
fail while core analysis completes; its stage/error appears in the status
report. Direct legacy non-strict postprocess keeps its warning/skip behavior.

Done/failed request schemas and canonical URIs are unchanged. Logs/control
records are persisted before terminal completion. Successful job scratch is
removed after durable Done; failure to remove a locked local file is nonfatal.
On failure, local config/log evidence remains; an acknowledgement indicates
whether the failure marker reached S3. A missing mount never redirects bulk
output into the underlying local directory. Interrupted publications are
recomputed/restored from valid authoritative inputs, not accepted by existence.

Explicit requeue is available after correcting the cause:

```bash
make workflow-worker ARGS="--once --retry-request-uri s3://bucket/workflow-prefix/analysis-jobs/JOB/request.json"
```

It verifies the canonical request, refuses completed jobs, archives the failed
marker under `attempts/<timestamp>/`, and only then removes that **failure
marker**. It does not delete input/result objects or automatically retry a
terminal job. The command mutates workflow metadata and must be used deliberately.

Scratch maintenance lists candidates by default:

```bash
python -m ipid_analysis.maintenance --older-than-hours 168
python -m ipid_analysis.maintenance --older-than-hours 168 --include-failed --apply
```

Only owned local scratch descendants qualify. Active process artifacts,
symlinks, persistent roots and unacknowledged failed jobs are protected. Stop
the worker first: maintenance obtains the same local leases. `make clean` only
walks source/test/benchmark directories and never mounted data/reports.

## Scientific compatibility and deliberate interface refinements

Classifier thresholds, strategy names, Parquet columns, loss semantics,
selection seed/order, synthetic null-model draws and scientific formulas are
preserved. Tests compare entire selected cohorts and column order at different
batch sizes, and verify increments against historical persisted labels.
Malformed uint16 inputs fail instead of wrapping silently. Unsupported snapshot
layouts and mismatching repeat semantics are rejected rather than compared.
Provenance records null seeds/calibration identities and source configuration.

UTC remains the interpretation of timestamp-only IDs unless a timezone is
explicitly supplied. Ambiguous local times fail; use an explicit campaign start
to disambiguate. No automatic historical reinterpretation is performed.

Manifest-wide missing-reply/transition reports now have stable
`campaign-<manifest-hash>` subdirectories to prevent cross-VM collisions.
Missing-reply legacy global names remain latest-run aliases. Explicit output
directory overrides remain available. Diagnostic transition PNGs default to a
1000-image limit; all rows remain in Parquet/CSV, and `--max-plots -1` explicitly
restores unlimited images. PNG paths include campaign and connection identity.

## Validation and remaining work

### Review traceability

These IDs refer to the architecture inventory at base commit `ff826889`.
Implementation status is separate from production validation:

| Review items | Implementation/evidence |
| --- | --- |
| N01–N04 | guarded runtime, dedicated local scratch, scoped DuckDB, CPU/RAM/batch/spill budgets and download admission; capacity envelopes still require realistic load tests |
| N05–N07 | local staging, row-count validation, native known-size publication, marker-last completion and targeted FUSE visibility refresh |
| N08–N12 | strict stage report and artifact contract, explicit retry, timeouts, dispatch isolation, single-host leases and mapped raw-input reuse |
| N13 | bounded exact cohort selection; full historical rows/column order compared at batch sizes 7 and 4096 |
| N14–N18 | dependency-bound caches, authoritative historical labels for increments, content-keyed external inputs, boundary validation, snapshot/timezone/provenance evidence |
| N19–N24 | campaign-scoped reports, validated local HTTP resume, recoverable local locks, safe cleanup, streaming legacy interprotocol and transition exports |
| N25 | listing RAM and repeated terminal lookups bounded; total recursive historical listing remains a documented protocol limitation |
| N26 | entire unittest suite in CI, storage/HTTP/process-abort/recovery regressions and opt-in real-mount smoke; real endpoint/fault/large-scale acceptance remains pending |
| N27–N28 | frozen Python 3.12 dependencies, normal venv setup, safe clean scope and this operating contract |

Implemented efficiency improvements include O01's identical-object upload skip,
O02's local restore/staging path, O04's source-bound role/OS aggregate reuse,
O05's vectorized cohort scoring and selected-only diagnostics, O07's validated
checksum reuse, and O08's once-per-triplet snapshot reads. Required canonical
secondary paths and the RIPE archival cache remain available. Removing those
copies, server-side copying across prefixes, further O03/O04 scan/aggregate
fusion, O05's individual interprotocol calculations, O06's deeper synthetic
array lifetime changes, O08's MaxMind cache and O09's scheduling changes are
follow-up profiling candidates. They add invalidation/coupling complexity or
affect scientific execution order; the existing small aggregate operations and
exact calibration arrays are retained. No such change is presented as an
unmeasured performance gain.

Run `make test` for the complete scientific, storage, recovery and HTTP suite.
CI uses Ubuntu/Python 3.12, frozen dependencies and Linux Libertine fonts.
Local mocked transport tests cover failed native writes, marker ordering,
targeted cache invalidation, missing mounts, spill cleanup, resource limits,
strict status validation, retry archiving, HTTP validators/ranges/416/short
bodies, exact cohort compatibility and authoritative persisted strategies.
Mocks cannot establish an endpoint's real publication/visibility behavior.

Before production deployment, with the worker stopped, run the opt-in smoke
test against the existing mounts:

```bash
IPID_ANALYSIS_STORAGE_MODE=mounted-s3 python benchmarks/s3_storage_smoke.py --execute
```

It writes tiny objects only in fresh `.ipid-analysis-tests/<uuid>/` namespaces
below processed/reports, checks replacement visibility/checksums/Parquet reads,
and retains them for inspection. It never deletes objects or changes raw data.
Also validate kill/timeout/restart, mount disappearance/reappearance, full
scratch and permission failures in an isolated test deployment before rollout.
These real S3/FUSE tests have not been run by the implementation task.

Benchmark-gated work remains disabled:

| Experiment | Evidence required before changing defaults |
| --- | --- |
| FUSE versus DuckDB/PyArrow native S3 reads | projected scans, joins, row-group/random reads; bytes, wall time, requests |
| VFS off versus bounded read/full caches | repeated reads, disk amplification, eviction and crash behavior |
| Parquet row groups/compression/partitioning, numeric IP keys | realistic distributions, equality of results, format compatibility and total bytes |
| Larger thread/batch limits, concurrent campaigns/imports | end-to-end throughput, peak RSS/spill, head-of-line delay, 8-CPU contention |
| Reusable persisted null/calibration caches | identical scientific results, generation identity, worthwhile saved CPU |
| Multipart/direct output streaming and wider CSV exports | scratch peak, reliable abort/recovery and stable output interfaces |

Use small, medium and representative 300-million-row fixtures; log revision,
lockfile, CPU/RAM/cgroup limits, mount/version/options, input layout, warm/cold
cache state, peak scratch/RSS, output hashes and failure behavior. Existing
`benchmarks/network_role_join.py` can measure the SQL join in isolation with
an explicit local `--output tmp/benchmarks/network-roles`; it does not establish
end-to-end S3 or preprocessing throughput.

One deliberate limit remains in the compatible request protocol: listings are
spooled to local disk and terminal HEAD checks cached, but recursive enumeration
still grows with historical request count. Eliminating it needs a producer-side
active index/retention protocol across `ipid-measure` and analysis. No queue
framework, request deletion, or incompatible producer change is introduced.
The runtime bounds listing memory, not the total listing cost.
