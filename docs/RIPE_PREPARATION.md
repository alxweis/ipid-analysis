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
Preparation uses available selected dumps within this fixed window. It checks
valid retained/S3 hourly parts first, then retained raw files, then the public
archive. An unavailable selected hour (HTTP 404/410) is recorded and skipped;
no earlier date outside the window or different hour is substituted. A later
campaign date changes the window, while overlapping hourly parts remain reusable.

Daily-dump mode samples one deterministic hour per day, not all 24 hours. Thus
28 days requests 28 hourly files. If one is unavailable, the 27 available files
are sufficient to build a lookup and plots for that 28-day window. Sampling
density remains one hour per available day. API and imported-file modes retain
their existing semantics. Access-denied, transport, integrity and parsing errors
remain errors. With no usable selected inputs, preparation fails rather than
publishing an empty reference dataset.

`source.json` records requested and used slots, `missing_dump_slots` with HTTP
status/URL, actual `dump_samples`, `requested_dump_samples`, available/missing
day counts, `daily_dump_complete`, and `daily_dump_coverage_percentage`.
RIPE plot JSON reports include the same temporal coverage. A successfully
prepared lookup therefore does not claim that every requested day was present.

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
version must agree. The available-hours policy has a distinct window-cache key;
old strict-window aggregates remain untouched, and hourly part keys are unchanged.
Changing only the time within one UTC day keeps the same window.

An aggregate prepared with missing days is intentionally reused as the same
reference snapshot without new HTTP requests in every role job. To explicitly
recheck previously unavailable slots, rerun preparation with `--force` before
starting the campaign. Valid hourly parts are still reused. Avoid changing that
snapshot midway through a campaign; use the recorded coverage for reporting.

The later IPID-to-role joins and figures still need the campaign's strategy
tables. Preparing roles in advance removes raw-dump download/parsing work from
that stage; reading the compact lookup, including S3 reads, remains necessary.
Use the normal bounded RIPE resource settings. Run the standalone preparation
before the campaign while the analysis worker is idle so it does not compete
with full-size measurement analyses outside scheduler admission.
