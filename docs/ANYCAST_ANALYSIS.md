# EU Anycast sensitivity: 7–22 November 2026

The four EU probing-interval comparisons use the same immutable LACeS Census
revision `841f1285ff6f563a40f629cfe4338329388f0ac3`. Select **7 through 22 November,
inclusive**: all 16 daily IPv4 snapshots must be present. Apply the recommended
High-Confidence rule separately to each day, then union its candidate /24s:

```text
max(AB across IPv4 protocols) > 3 OR max(GCD across IPv4 protocols) > 1
```

The pinned 16-day selection contains **18,512 distinct /24 prefixes**. This is the
worldwide Census list; only its intersection with a plot's observed population
is counted as affected prefixes. Exclude every IP in those detected /24s.
Both anycast and unicast IPs may occur in the same /24, so describe the result
as IPs **within detected anycast prefixes**. This is a sensitivity analysis,
not an individual-IP Anycast proof or an estimate of all undetected Anycast.

## Preparation and explicitly requested replacement

Run on the Analysis VM in the usual activated, mounted-S3/RAM-scratch environment:

```sh
make prepare-anycast-census \
  ARGS="--month 2026-11 --start-date 2026-11-07 --end-date 2026-11-22 --revision 841f1285ff6f563a40f629cfe4338329388f0ac3"
census="$PWD/references/anycast-census/2026-11/841f1285ff6f563a40f629cfe4338329388f0ac3/20261107-20261122/dataset.json"
make paper-anycast-plots CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--census $census --sites eu --protocols icmp tcp udp --connection-mode no-connection --include-tcp-full --refresh --dry-run"
make paper-anycast-plots CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--census $census --sites eu --protocols icmp tcp udp --connection-mode no-connection --include-tcp-full --refresh --prepare --workers 3"
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"
make paper-figures-zip
```

There is no `--allow-partial`: missing days abort preparation. The old monthly
Census manifest/prefix cache stays intact; the new selection has its own dated
directory. Raw Census downloads are reused only after checking the pinned Git
blob. No external measurement is run.

Preparation sequentially reads each comparison's existing processed per-IP
intervals and stored continent map, never raw IP-ID sequences or a new
geolocation database. Quantiles and population quartiles are recomputed after
filtering, retaining the approved p99.9/256-quantile design, separately per
continent/probing mode. Rendering uses a small process pool only after the
sequential numerical preparation.

`--include-tcp-full` selects exactly the three EU stateless comparisons plus the
registered TCP Connection **Full** comparison. Its population remains jointly
successful new Full RT-based and historical Fixed-Interval IPs. It never uses
the old RT-based Connection sample or all Full RT targets as a replacement for
this paired plot population.

The user explicitly requested retaining the existing PDF names:

* `n-rt-b_fi-b_probing-intervals-by-continent__paper__anycast.pdf`, in each of the
  canonical EU ICMP/TCP-no-connection/UDP directories;
* `c-rt-b_fi-b_probing-intervals-by-continent__paper__full__anycast.pdf`, in the
  registered EU paired Full comparison directory.

Without `--refresh`, publication remains create-only. Explicit refresh first
backs up existing selected PDFs, sidecars and receipts under
`tmp/anycast-before-refresh-<UTC timestamp>/`. It prepares new content-addressed
numeric aggregates, renders elsewhere, and validates **all** staged plots,
metadata, source fingerprints and unchanged destinations before publishing
the first replacement with the existing native storage transport. Source
reports, original/unfiltered PDFs, old aggregates and campaign inventories are
preserved. A failed stage never replaces canonical PDFs. Completed reruns reuse
prepared quantiles and skip matching PDF/metadata pairs.

Do not run export or another refresh concurrently. Publication of the four PDFs
is sequential, not a multi-object S3 transaction; the backup and per-file writing
receipts remain available after interrupted publication. Successful completion
writes `finished.json` in the backup directory.

## Metadata and paper keys

Each refreshed PDF has a same-basename `.json` sidecar, copied verbatim from the
versioned numerical metadata. It contains source/Census/prefix/calculation
hashes, exact included/missing dates, per-day input URLs/blob/SHA256 information,
filter and confidence, p99.9/quartile settings and the impact table below. PDF
and sidecar publication receipts bind their bytes to the producing inputs.

The existing four `.anycast` figure keys now select the refreshed PDFs at their
unchanged paths. `paper-figures.zip` includes those PDFs. The JSON/Parquet inputs
remain in the analysis storage; the figure ZIP still contains PDFs only.

New scalar prefixes are:

```text
results.eu.icmp.rt-vs-fi-base.anycast
results.eu.tcp.no-conn.rt-vs-fi-base.anycast
results.eu.udp.rt-vs-fi-base.anycast
results.eu.tcp.conn.rt-vs-fi-base.anycast
```

Under each prefix export these values both globally and under
`continent.{na,as,eu,sa,af,oc,an}` for every present target continent:

| Suffix | Definition |
| --- | --- |
| `addresses.original.count` | Unique original IPs in this comparison's stored union continent map |
| `addresses.excluded.count`, `.pct` | IPs in detected /24s; percent of original IPs in the same scope |
| `addresses.retained.count`, `.pct` | Remaining IPs; same original-IP denominator |
| `prefixes.original.count` | Distinct /24s observed in the original comparison population |
| `prefixes.excluded.count`, `.pct` | Observed /24s intersecting the Census union; percent of observed original /24s |
| `prefixes.retained.count`, `.pct` | Remaining observed /24s; same original-prefix denominator |
| `unlocated.{original,excluded,retained}.count` | IPs with unknown continent, included in global address totals |
| `unlocated.original.pct` | Unknown-continent IPs / all original comparison IPs |

`unlocated.*` is global only. TCP Connection scalar keys retain **`.full` at the
very end**, e.g.
`results.eu.tcp.conn.rt-vs-fi-base.anycast.continent.eu.addresses.excluded.pct.full`.
The first `eu` is the source VP; `continent.eu` is the target continent. There is
no sum or pooled unique-IP estimate across the four protocol/mode populations.

Global prefix counts are calculated with a separate distinct aggregation:
the same /24 can appear in multiple target continents and must not be counted
twice globally. Address totals include `ZZ` (unknown continent); located plot
groups exclude it. Counts describe the union map before p99.9 clipping, not
KDE samples, packets, all ZMap targets or all campaign measurements. Quartile
valid-sample counts remain separately stored in the numerical aggregate.

The shared worldwide Census prefix count is exported as:

```text
results.anycast.census.prefixes.count
```

This value is 18,512 for this frozen selection, **not** the number of affected
measured prefixes. Percentages use unrounded counts. Zero original populations
do not acquire fabricated percentages. Related rows stay together in values.tex.

Dates, used/expected snapshot counts, period completeness, confidence, rule and
Census revision remain in the full JSON metadata only. No
`params.anycast.census.*` keys are written to values.tex or the public scalar
table in values.json. These metadata still participate in validation and
mixed-period checks.

Export reads only bounded PDFs, reports, receipts and sidecars; it does not open
per-IP files or rerun preparation. Legacy v1 PDFs keep their existing figure
keys without invented numeric impact values. New v2 receipts require matching
sidecars, source-report hashes, conserved integer populations, /24 capacity
bounds and exact complete High-Confidence periods. Mixed Census masks/periods
across the selected plots reject strict export before replacing local outputs.
Canonical run selection and pinned Full report validation are reused.

Nothing in this workflow edits manuscript sections or updates the local Paper
repository automatically. Copy the resulting values.tex and figure ZIP after
review, using the existing import workflow.
