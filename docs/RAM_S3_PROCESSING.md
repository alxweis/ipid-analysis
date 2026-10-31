# RAM work files and S3 persistence

## Production behavior

With `IPID_ANALYSIS_STORAGE_MODE=mounted-s3`, bulk temporary files now use an
existing Linux tmpfs by default. Source code, locks, task specifications,
supervisor logs and bounded retained diagnostic logs stay on the local volume.
No mount, service, VM or Ansible configuration is installed or changed.

The default private RAM directory is
`/dev/shm/ipid-analysis-<uid>-<checkout-path-hash>/`, mode 0700. It must be owned
by the running user, outside persistent roots and backed by tmpfs. A mount root,
symlink, ext4 override or missing tmpfs produces an error, never a disk fallback.
Job directories must stay below this root and cannot cross to an ext4 submount.

| Content | Location |
| --- | --- |
| Raw ZMap/IPID/interprotocol inputs | Existing S3/FUSE paths; read in place |
| Intermediate Parquet/CSV/JSON/PDF files | Per-job RAM scratch |
| DuckDB temporary/spill files | Per-connection RAM directories with finite size limits |
| Complete analysis results and completion markers | Existing S3 paths and schemas |
| RIPE/ITDK download `.part` and HTTP state | Bounded RAM work area |
| Complete external raw downloads | Existing shared S3 raw namespaces |
| RIPE window and processed parts | Existing S3 caches and unchanged cache keys |
| Locks, task specs and supervisor/retained logs | Local control directory (`tmp` by default) |

This is **RAM staging followed by known-size native S3 publication**, not an
unknown-length HTTP-to-S3/FUSE stream. Existing size/checksum validation,
completion-marker-last publication, mount visibility checks, upload timeouts,
`--s3-no-check-bucket` and real network/permission error handling are retained.
Seekable output formats keep normal POSIX writers on tmpfs; they do not need
individual new storage implementations or new dependencies.

RIPE checks valid window/part caches first. New daily downloads complete and
validate in shared RAM, are published under
`data/raw/ripe-atlas/daily-dumps/<day>/<filename>`, and are then parsed from S3.
The RAM copy is reclaimed only after that publication succeeds. Parser outputs
are built in RAM and published into the original processed cache namespaces.
Existing raw files and processed parts are reused; nothing deletes existing S3
data. Raw dumps persist in S3, so this intentionally increases retained raw S3
storage compared with discarding each raw download after preparing its part.
No automatic S3 retention/deletion policy is introduced.

ITDK downloads similarly stage in RAM, publish their existing raw objects, then
parse the persistent object. A committed raw ITDK object can be reused after an
interrupted cache build without repeating the HTTP transfer.

## Budgets and recovery

The scheduler reserves the **entire RAM-file pool before dividing compute RAM**
between its existing two critical slots and one background slot. Tmpfs is not
counted as free additional disk capacity. The default pool is at most 32 GiB,
further capped at one quarter of the worker memory allocation and available
tmpfs capacity. Shared RIPE transfers reserve up to one eighth of this pool
(and at most their configured 8 GiB). Remaining capacity is divided between job
slots, with critical reservations kept available during background work.

Admission counts retained files, unused active-job capacity, shared transfers,
physical tmpfs free space and available RAM. Restart admission credits an
existing task's own files instead of charging their allocation twice. Resident
RIPE files are distinguished from resident job outputs. Children inherit fixed
global/per-job byte limits; DuckDB spill is capped by remaining capacity.
HTTP chunks cannot exceed remaining RAM capacity. Output publication rejects
an exceeded budget before modifying the canonical destination. The scheduler
also supervises aggregate process RSS, job files and the total RAM pool.

These are application admission and watchdog limits, not new kernel cgroups
or per-directory filesystem quotas. Arbitrary writers can briefly overshoot a
limit between watchdog polls; full tmpfs writes fail instead of falling back to
ext4. Leave resource headroom. The OS may swap tmpfs pages under memory pressure;
the application does not disable swap or change host memory policy.

Complete S3 objects and valid processed caches survive VM restarts. RAM partial
downloads and their validator state survive a worker restart while tmpfs remains
available, but **not a reboot/removal of tmpfs**. After that, only unfinished
transfers must restart; already published raw objects/parts remain reusable.
Publication failures retain their RAM download until retry or bounded eviction.
Existing per-task leases protect live writers and stopped-job cleanup reclaims
only that task's RAM directory while retaining bounded diagnostic logs locally.
No site/campaign/job completion or scientific semantics are changed.

## Configuration and migration

| Variable | Meaning |
| --- | --- |
| `IPID_ANALYSIS_SCRATCH_STORAGE` | `ram` by default in mounted-S3 mode; `disk` by default in local compatibility mode |
| `IPID_ANALYSIS_RAM_DIR` | Optional private directory on an **existing** tmpfs; defaults to the checkout/user-specific `/dev/shm` directory |
| `IPID_ANALYSIS_RAM_BYTES` | Maximum RAM-file pool; default at most 32 GiB, with scheduler caps described above |
| `IPID_ANALYSIS_RAM_RESERVE_BYTES` | Free tmpfs headroom; default 64 MiB |
| `IPID_ANALYSIS_TMP_DIR` | Local control location in RAM production mode; retains its historical scratch meaning in explicit disk mode |
| `IPID_ANALYSIS_CONTROL_ROOT` | Stable local control/lock root passed to all children |
| `IPID_ANALYSIS_RIPE_TRANSFER_DIR` | Optional shared directory **inside the RAM root** in RAM mode |

`IPID_ANALYSIS_RAM_JOB_DIR` and job byte limits are scheduler-owned child
settings. Do not configure them globally. `IPID_ANALYSIS_WORKER_SCRATCH_BYTES`
can further reduce the scheduled RAM pool; it does not authorize extra RAM.
Local compatibility mode and an explicit `IPID_ANALYSIS_SCRATCH_STORAGE=disk`
retain the previous disk implementation. Production RAM mode has no silent
fallback. In sequential worker mode the work products also use RAM, with the
same global capacity checks but without the parallel scheduler's RSS watchdog.

For deployment, first let active jobs finish and stop the old worker normally.
Do not run old/new workers simultaneously. Preserve the existing mounts, S3
mapping and prefix; retain `IPID_ANALYSIS_TMP_DIR` for controls. If a previous
configuration points `IPID_ANALYSIS_RIPE_TRANSFER_DIR` at ext4, unset that
override (or explicitly choose disk compatibility mode). Existing complete
downloads in `tmp/ripe-transfers/downloads/<part-key>/` are checked for read-only
reuse; the legacy configured raw layout is also supported. Old ext4 partials
are left untouched and are not automatically migrated into RAM. Existing valid
S3 parts/windows need no migration or rebuilding.

A read-only preflight with the production storage environment already set:

```bash
.venv/bin/python - <<'PY'
from ipid_analysis.runtime import control_root, ram_limit, ram_root, scratch_root, validate_storage
validate_storage()
print("Control:", control_root())
print("RAM:", ram_root())
print("Data scratch:", scratch_root())
print("Requested RAM-file bytes:", ram_limit())
PY
```

Preflight creates only private empty work directories and validates mounts; it
does not publish S3 objects. Start with the existing `make workflow-worker`
command after checking the environment. No `sudo mount` or VM reconfiguration
is required. Run tests from an isolated source checkout as documented for the
earlier follow-ups; do not discover unit tests inside the live FUSE checkout.

## Validation still required on the Analysis VM

The regression suite covers path placement, publication integrity, budgets,
HTTP interruption/resume, worker-restart admission, cache reuse across windows,
and Linux real-tmpfs subprocess/concurrency behavior with synthetic data.
Real ZIH-S3/FUSE publication, interruption during native uploads, reboot
recovery and full six-site load still require an isolated VM test prefix. No
performance speedup is claimed without measurement; the existing RIPE benchmark
can compare identical parser inputs, including decompression/JSON/role costs.
