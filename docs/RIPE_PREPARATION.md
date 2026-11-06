# Prepare RIPE roles before a geographic campaign

The default remains 14 complete UTC days. Set
`IPID_ANALYSIS_RIPE_LOOKBACK_DAYS=28` in both the preparation shell and the
analysis worker environment to select 28 days. The preparation CLI also accepts
`--lookback-days`; postprocessing and network-role analysis accept
`--ripe-lookback-days`. Explicit CLI options override the environment. Values
must be positive.

Select a common campaign start before preparing. The window ends at 00:00 UTC
on that date, excluding the campaign day. For example, a campaign starting on
2026-11-07 uses `[2026-10-10T00:00:00Z, 2026-11-07T00:00:00Z)` with 28 days.
All selected dumps must already be published or cached. Preparation cannot
download an unpublished last day in advance. A later campaign date changes the
window, while overlapping hourly parts remain reusable.

Daily-dump mode samples one deterministic hour per day, not all 24 hours. Thus
28 days selects 28 hourly files. This changes temporal coverage, not the daily
sampling density. API and imported-file modes retain their existing semantics.

On the Analysis VM, use the same deployed revision, S3 mounts, credentials and
storage environment as the scheduled worker. In a screen session, after sourcing
`/etc/default/ipid-geographic-analysis`, for example:

```sh
cd /mnt/data01/ipid-analysis
source .venv/bin/activate
set -a
source /etc/default/ipid-geographic-analysis
set +a
export IPID_ANALYSIS_RIPE_LOOKBACK_DAYS=28
# Example only: replace with the actual common UTC campaign start.
export IPID_ANALYSIS_RIPE_CAMPAIGN_START=2026-11-07T18:30:00Z
/usr/local/bin/ipid-geographic-analysis-preflight
make prepare-ripe-atlas ARGS="--campaign-start $IPID_ANALYSIS_RIPE_CAMPAIGN_START --lookback-days $IPID_ANALYSIS_RIPE_LOOKBACK_DAYS"
```

Configure the same two variables in the worker's deployed environment and
restart the idle worker with that environment before submitting the campaign.
The preparation command's campaign start is explicit; the worker can otherwise
infer its own start from measurement IDs, potentially selecting another UTC day.

Prepared roles and metadata persist under the global processed RIPE root, with
reusable hourly parts and an optional content-verified S3 cache below the shared
workflow prefix. Matching complete caches are validated and reused before raw
dumps are fetched or parsed. Source mode, source options, time window and cache
version must agree. Changing only the time within one UTC day keeps the same
window. Do not use `--force` for normal reuse.

The later IPID-to-role joins and figures still need the campaign's strategy
tables. Preparing roles in advance removes raw-dump download/parsing work from
that stage; reading the compact lookup, including S3 reads, remains necessary.
Use the normal bounded RIPE resource settings. Run the standalone preparation
before the campaign while the analysis worker is idle so it does not compete
with full-size measurement analyses outside scheduler admission.
