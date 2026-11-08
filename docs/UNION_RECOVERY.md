# Geographic discovery union recovery

The 18-VM `parallel-union` now streams the normalized discovery partitions into
one exact IPv4 bitmap (512 MiB). It writes the existing `IP_ADDR`, `REPLY_TYPE`
Parquet schema in bounded batches, with an empty reply type. No target is
sampled, and all discovery binding, checksum and completion checks remain in
place. Output order is numeric IPv4 order; consumers must not depend on the
previous unordered `DISTINCT` output order. The final target file still needs
scratch space proportional to its compressed size; there is no global DuckDB
hash table or spill for this step.

The supervisor also handles a process that cannot be reaped within five seconds
after termination. It keeps the process and its reservation active and retries
termination on subsequent passes. It publishes failure and reclaims scratch
only after the process has exited. The resource-limit log includes measured
usage and the job's limits before termination begins.

## Recover an interrupted campaign

1. Keep measurement VMs and the OS worker running. An analysis-only fix does
   not require rescanning or a new campaign ID.
2. Stop any old analysis worker before updating its pinned source revision.
   Verify both the scheduler and its workflow-task processes have exited.
3. Preserve the stopped job's control `task.json` and `supervisor.log`.
   A crashed supervisor can leave a large DuckDB spill in that job's RAM
   scratch. Reclaim only the scratch directory named in the verified task spec,
   under the analysis-writer and `scheduled-task:<task.key>` leases. Use the
   same `IPID_ANALYSIS_LOCK_DIR` as the worker. Do not clear the whole tmpfs,
   remove locks or delete discovery partitions/S3 objects.
4. Load `/etc/default/ipid-geographic-analysis` in the worker's fresh shell,
   clear inherited per-job resource overrides, and restore the **original**
   RIPE campaign start and source selection. Run the deployment preflight.
5. Restart `make workflow-worker ARGS="--poll-interval 2 --scheduling scheduled"`.
   Existing discovery-ready markers are reused; an unfinished union without
   a terminal failure marker is offered again. A committed OS request is not
   regenerated. If a canonical `union/failed.json` exists, inspect it and use
   the existing explicit retry operation for that stage; do not silently
   overwrite terminal status.

Watch the union's per-partition row counts, its final target count and
`os/request.json`, then the waiting RT handoffs. Completion of the union only
unblocks the OS scan; it does not imply completion of that scan or of the
campaign analyses.

## Validation

Tests compare exact union membership across duplicate batches and files,
addresses sharing bitmap bytes, the full uint32 boundary range, malformed
input rejection and atomic publication. Supervisor tests simulate delayed
SIGKILL reaping and verify that failure publication, scratch deletion and
reservation release wait for actual process exit.

A local synthetic smoke run read 20 million rows (two copies of 10 million
distinct IPv4 addresses), retained exactly 10 million targets, used about
664 MiB peak process RSS and generated no spill files. Its 18-second local
runtime is not a throughput prediction for mounted S3 or the full campaign.
