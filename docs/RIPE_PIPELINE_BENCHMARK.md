# RIPE parser: ext4 versus RAM

This is an **opt-in diagnostic**, not a change to the worker, cache keys,
scientific rules, storage defaults, or production publication. It measures one
real `.bz2` daily dump with the unchanged production `_build_role_part` parser.
The older `benchmarks.ripe_roles` tests accumulation only and cannot answer this
I/O/parser question.

## Run on the Analysis VM

Use the checkout containing this PR and its existing Python environment. No new
dependencies, root privileges, mounts, or VM configuration changes are needed.
Choose a maintenance window: finish active work, stop the analysis worker
gracefully, and run only one benchmark. This standalone diagnostic is **not
admitted by the worker scheduler**; its limits do not reserve capacity against
other jobs. Start the worker again afterwards. Do not interrupt time-critical
measurement handoffs to run a benchmark.

If the original complete dump still exists, prefer the read-only input option:

```bash
cd /mnt/data01/ipid-analysis
.venv/bin/python -m benchmarks.ripe_pipeline \
  --input /absolute/path/to/traceroute-2026-10-03T0000.bz2 \
  --window-start 2026-10-02 --window-end 2026-10-30 \
  --repeats 2 --profile
```

The successful production workflow normally removes temporary raw dumps after
committing their processed parts. If no complete input remains, download one
public dump **once into a new benchmark directory**:

```bash
cd /mnt/data01/ipid-analysis
.venv/bin/python -m benchmarks.ripe_pipeline \
  --url https://data-store.ripe.net/datasets/atlas-daily-dumps/2026-10-03/traceroute-2026-10-03T0000.bz2 \
  --window-start 2026-10-02 --window-end 2026-10-30 \
  --repeats 2 --profile
```

This creates `tmp/benchmarks/ripe-pipeline-<unique>/` and prints its path. The
download remains there for reuse with `--input` in subsequent experiments.
Existing source files and production caches are never changed or deleted.
There is no S3 upload, cache preparation, core reanalysis, or new IPID measurement.
The optional public HTTP download uses the existing bounded-size/resumable
downloader; the benchmark does not measure HTTP throughput as parser throughput.

The default two pairs run in order **ext4, RAM, RAM, ext4**, with a fresh parser
process each time. `--profile` adds one separate RAM profiling run. This can take
several hours for a full dump; these are single-parser trials, not the production
three/four-parser workload. For a preliminary two-trial check use `--repeats 1`.
Do not treat that preliminary pair as a stable performance estimate.

## Isolation and budgets

- Persistent diagnostics and the optional downloaded source stay in a dedicated
  descendant of this checkout's `tmp/`. The work filesystem must actually be ext4.
- The RAM trial uses a new temporary directory on an **existing tmpfs**, default
  `/dev/shm`. It does not mount/remount anything. An ordinary directory named
  `ram` is not sufficient. `--ram-dir` may select another existing tmpfs.
- Each trial receives a private input copy, scratch directory, and output. Only
  these temporary trial directories are removed. Logs, JSON, profiles, and the
  newly downloaded source are retained. Each invocation has a new result directory;
  it does not overwrite or resume earlier benchmark results.
- Defaults: `--memory-gib 4` for parser RSS, `--workspace-gib 4` for trial input
  plus intermediate files, and `--reserve-gib 2` for free memory/storage headroom.
  Before running, available memory must cover **RSS cap + tmpfs file cap + reserve**.
  Available memory uses the existing cgroup-aware helper.
- The parent monitors RSS, owned file sizes, available memory and trial timeout
  every 100 ms; a breach stops the child process group. These are monitored limits,
  not new kernel/cgroup quotas; short overshoots are possible. Input-copy writes
  are size-bounded even if the source changes. No unbounded input is buffered in RAM.
- `--timeout 7200` bounds each parser trial. Failed jobs retain logs and a failed
  `results.json`; their private trial directory is removed after the child stops.
  A forced kill of the benchmark parent can leave a uniquely named tmpfs directory;
  inspect ownership and process liveness before manually removing that directory.
- All parser trials use the same single CPU from the process's allowed affinity.
  BLAS/OpenMP/Arrow thread environment variables are bounded. The production
  algorithm, IPv4 role semantics and Parquet schema are unchanged.
- Deployment `.env` loading and inherited `IPID_ANALYSIS_*` scope/cache/publication
  settings are disabled for this process and its child. This does not alter the
  environment of a running worker or any VM configuration.

Insufficient tmpfs capacity is a clear preflight failure. Do not remount tmpfs or
increase VM limits merely to make this diagnostic run; choose an existing suitable
tmpfs or defer the comparison. A successful RAM comparison requires enough real
headroom to avoid swap. System swap-in/out deltas are recorded; any observed swap
activity adds an explicit warning and requires a repeat in a quiet window.

## Reading the output

Send the printed result directory's `results.json`, `trial-*.json`, `trial-*.log`,
`trial-*.pstats` and `trial-*.txt` for review. Exclude `source.bz2`, which can be
several GB. The `*.spec.json` files document exact trial paths and parameters.

The JSON includes:

- Unprofiled wall and process CPU time, plus ext4/RAM medians.
- Production `parse_seconds`: reading/BZip2/JSON/filtering/role calculation and
  bitmap updates. `after_parse_seconds` covers Parquet creation, source hashing,
  metadata and local completion publication. Those coarse phases are disjoint;
  the production `prepare_seconds` is an additional nested timing, not another
  phase to add to them.
- Linux `/proc/self/io` deltas, sampled RSS and file-space peaks, kernel peak RSS,
  source SHA-256, software/source provenance, output counts and canonical role hash.
  Kernel `read_bytes` can be zero for cached ext4 reads. `rchar` is logical I/O;
  neither is a network metric. Stage-copy/download times are recorded separately
  and excluded from parser timing. Startup/imports and post-run result validation
  are also outside the parser timing, but process memory is still monitored.
- With `--profile`: raw `cProfile` data, top functions by cumulative and self
  time, and matched functions for BZip2 decompression, JSON decoding, role
  derivation and bitmap updates. Some JSON implementations may not expose a
  separately matched function; inspect the full profile in that case. Cumulative
  function times can overlap and must not be added as disjoint phase percentages.
  Profiling adds overhead: that extra trial is excluded from timing medians.

Equality is checked from sorted unique `IP_V4`, `T`, `D` values, independent of
Parquet row groups/compression, plus source hash, address/trace and skip counters.
All trial outputs, including the profiling trial, must agree. Local completion
marker checksums and sizes are checked against actual bytes. Correctness failures
produce a nonzero exit and `status: failed`; `passed` establishes equivalence for
this dump, **not** a general speedup or a deployment recommendation.

## Page cache and conclusions

Copying or hashing a file warms Linux page cache. Before each ext4 trial the
benchmark fsyncs its private input, verifies its hash, and requests per-file
`POSIX_FADV_DONTNEED`. It never writes `/proc/sys/vm/drop_caches` and never evicts
an existing production source. The advisory request cannot prove cold storage:
hypervisor/backend caches and filesystem behaviour can still affect the run.
The policy is recorded verbatim in every result. Alternating order reduces order
bias; it does not eliminate background load or host throttling.

If actual ext4 read I/O is low and CPU time dominates, copying the input to tmpfs
is unlikely to remove the main cost. If tmpfs consistently improves unprofiled
wall time and the profile supports an I/O bottleneck, evaluate a budgeted RAM
path separately. This diagnostic does not benchmark native S3/FUSE streaming,
parallel parser scaling, or change the standard transfer/recovery architecture.
