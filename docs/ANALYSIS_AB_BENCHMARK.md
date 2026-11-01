# Warm-cache analysis A/B benchmark

This opt-in Linux diagnostic replays classic or Geographic ICMP/TCP/UDP No-OS
postprocessing on two pinned source packages, with one Python environment and
fresh isolated result paths. It does not rerun IPID/ZMap measurements, submit
worker jobs, change production completion markers, or rebuild external caches.

| Label | Commit | Meaning |
| --- | --- | --- |
| A | `b6e142c7de41d9c6c4a5c8f4515e82abeccc97ed` | Merge of PR #113; **already includes PR #114** |
| B | `68edad13e3b0f5a4e765f8c071dea8dc2e65a0a0` | Merge of PR #117; the actual v4 code |

PR numbers are not merge order. A/B cannot attribute a difference to #114,
which is present in both. #115/#116 are standalone benchmark changes. No
production processing defaults are changed by this diagnostic.

## Run on the Analysis VM

Wait until active measurement handoffs and analyses have finished. Stop the
worker gracefully in its screen session. This standalone diagnostic is not
admitted by the scheduler; it must run alone. It holds the existing writer lease
for the duration and refuses an already active writer. Do not start measurement
VMs that require handoffs while the worker is stopped.

The production checkout can stay on main. Fetch the diagnostic branch and place
its tools in a new separate checkout (replace the branch name if appropriate):

```bash
cd /mnt/data01/ipid-analysis
git fetch origin codex/analysis-ab-classic-icmp
mkdir -p /mnt/data01/ipid-analysis/tmp/benchmarks
tools=$(mktemp -d /mnt/data01/ipid-analysis/tmp/benchmarks/ab-tools-XXXXXX)
git worktree add --detach "$tools" FETCH_HEAD
```

Keep the same three mounted roots. No VM, mount or Ansible changes are needed:

```bash
export IPID_ANALYSIS_S3_ROOTS='{
  "raw":"s3://netds3o:aweis/ipid-2/raw",
  "processed":"s3://netds3o:aweis/ipid-2/processed",
  "reports":"s3://netds3o:aweis/ipid-2/reports"
}'

/mnt/data01/ipid-analysis/.venv/bin/python \
  "$tools/benchmarks/analysis_ab.py" \
  --repo /mnt/data01/ipid-analysis --execute --order AB
```

Default request:

```text
s3://netds3o:aweis/ipid-2/analysis-workflow/continental/geo-test-v4/sites/as/analysis-jobs/tcp-80_2026-10-31_17-19-14/request.json
```

The request and matching manifest are downloaded read-only once. Their IDs,
protocol, No-OS intent and raw paths are validated by the pinned worker code.
Inputs stay on the existing S3 mounts. Real bytes are SHA-256 checked before
and after each trial; available completion-marker hashes must match. Missing
or invalid warm RIPE/ITDK caches fail the diagnostic: no HTTP download, cache
restore or cache publication is allowed.

### Classic ICMP No-OS analysis

To compare only the final analysis of the existing `make run-all-icmp SKIP_OS=1`
run, pass its classic request explicitly (the default remains Geographic AS/TCP):

```bash
/mnt/data01/ipid-analysis/.venv/bin/python \
  "$tools/benchmarks/analysis_ab.py" \
  --repo /mnt/data01/ipid-analysis --execute --order AB \
  --request-uri 's3://netds3o:aweis/ipid-2/analysis-workflow/analysis-jobs/icmp_2026-10-31_17-19-06/request.json'
```

Use the worker-stop, Git-fetch, mount configuration and isolation instructions
above. Classic inputs are read from the unscoped raw root; Geographic inputs
retain their campaign/site scope. Request IDs, protocol, canonical workflow
references and raw prefixes are checked before reading analysis inputs. Both
revisions replay the same manifest, including its fixed-interval repeats and
RANDOM reproducibility analyses. OS remains explicitly `not_requested`.

This measures final postprocessing, including plots and publication, with warm
RIPE/ITDK caches. It excludes ZMap/IPID acquisition, handoffs and scheduler wait.
It is not an end-to-end timing of `make run-all-icmp`. Missing warm caches abort
the diagnostic rather than initiating downloads. The earlier TCP timing estimate
is not a guarantee for this different protocol/population.

Use `--order ABBA` for two trials per revision, or `--order BA` for a separate
reverse-order screening. A single AB pair is preliminary and must not justify
a rollback based on a small difference. Allow roughly 40–50 minutes for AB or
80–100 minutes for ABBA, plus setup/input verification/result validation. These
are estimates from v4, not timeout guarantees.

The final line identifies a new directory under:

```text
/mnt/data01/ipid-analysis/tmp/benchmarks/analysis-ab-<unique>/
```

Restart the worker with the normal production exports and `make workflow-worker`
after the command exits. A failure also releases the writer lease. Each new
invocation uses fresh paths; it does not overwrite/resume previous diagnostics.

## Isolation and measurements

- Sources come from `git archive` of the exact SHAs; there is no production
  branch switch, requirements installation or production code edit. Both Git
  objects must be present locally; missing historical commits fail explicitly.
- Compute defaults: the same 16 allowed CPU IDs, 125 GiB RSS budget, 21 GiB
  scratch, and bounded DuckDB memory for both revisions. Thread-library settings
  and the Python environment are identical. A uses its original ext4 scratch;
  B uses private existing `/dev/shm` tmpfs. This compares their intended storage
  behavior rather than artificially making both implementations identical.
- External reference datasets remain in their original global namespaces.
  Version-specific warm-cache validation still runs. The secondary RIPE remote
  cache transport is disabled to prevent any canonical cache write or restore.
  Full input hashing precedes timing and warms reads equally; a genuinely cold
  hypervisor/network cache is neither requested nor claimed.
- Inside each diagnostic child, persistent roots are bound to the three actual
  mount points before publication modules are imported. Trial result directories
  are children of those mounts, not mount points themselves. This also applies
  to classic requests with an empty scope. The historical mount validation and
  native rclone mount discovery remain active; no production runtime is edited.
- New output objects go only to `data/processed/.ipid-analysis-tests/analysis-ab-<uuid>/<trial>/`
  and `reports/.ipid-analysis-tests/analysis-ab-<uuid>/<trial>/` on the existing
  mounts. Existing objects are never deleted or used as benchmark destinations.
  Original native rclone publication, writing/completion markers, checksums and
  FUSE visibility checks execute for these synthetic result namespaces.
- Diagnostic path redirection and timing hooks are applied only inside the
  isolated child interpreter. Python mutation checks, native output-writer checks,
  SQL COPY checks and a subprocess allowlist refuse other write destinations.
  These are fail-closed guards around trusted pinned code, not an OS sandbox for
  arbitrary hostile code.
- Logs, source snapshots, request/manifest, input fingerprints, traces and JSON
  results remain under local `tmp/benchmarks`. Only newly owned tmpfs workspaces
  are removed after their child process stops. S3 test outputs are retained.
- Preflight refuses insufficient ext4/tmpfs/RAM headroom. The supervisor samples
  process-group RSS and scratch at 0.5 seconds, enforces timeout/space/RSS limits
  and stops the process group on failure. These are monitored limits, not cgroup
  quotas; short overshoot remains possible. Existing swap configuration is not
  changed. Swap counters are recorded so affected comparisons can be rejected.

## Reading results

`summary.json` contains the measured analysis time for each revision and the
scientific-equivalence result. Per-trial `control/` contains:

| File | Contents |
| --- | --- |
| `result.json` | Timed analysis wall/CPU/child CPU, average used CPU cores, kernel RSS high-water, process I/O deltas |
| `timings.jsonl` | Individual stages, SQL execute calls, ITDK/RIPE/reproducibility functions, new null-table generation, PDF saving, native publication and rclone calls |
| `inputs.json` | Actual size/SHA-256 of each used input and external processed cache |
| `output-inventory.json` | Output byte hashes plus normalized scientific digests |
| `postprocess-status.json` | Strict core completion and optional RIPE/ITDK stage status |
| `supervision.json` | Whole-child wall time and sampled RSS/scratch peaks |
| `swap.json` | System swap-in/out counters before/after |
| `run.log`, `spec.json` | Analysis diagnostics and exact source/budget/path configuration |

The analysis timer excludes setup, source extraction, input hashing and final
validation, but includes version-specific cache checks, calculations, plotting,
publication and visibility waits. Whole-child supervision includes preparation
and validation. The kernel RSS peak also includes those untimed phases.

Timings are **nested/inclusive**: do not sum SQL, stage and publication durations
as though disjoint. Some SELECT/Arrow reads are lazy and occur after `execute()`;
the enclosing function/stage time is authoritative. SQL wall/CPU data can
identify an I/O-heavy query, but cannot independently separate FUSE/network wait
from engine scheduling. The FUSE daemon's network counters are not reported as
the Python process's I/O counters. Publication time includes mount discovery
and visibility waits; nested rclone time includes its process and transfer.

Parquets are compared by schema and sorted logical rows; CSV data rows are
order-independent with equal headers. JSON normalization excludes only
timestamps/provenance and substitutes trial-owned paths. Scientific counts,
probabilities and measurement intervals remain compared. PDFs are checked for
a valid envelope, completion integrity and matching page counts; visual equality
is not asserted. Validation targets this small test, not 300M-IP data.

This does not benchmark cold RIPE downloads/part creation, scheduling, six-site
load or full-scale capacity. If an AB difference is stable in reverse order,
use the stage traces to select a narrow follow-up; do not remove integrity
checks or change scientific calculations based only on total time.
