# Counter/OS revision values

Ordinary `make paper-values` remains a report/cache reader. It does not prepare
OS groups, reclassify measurements, read raw IP-ID sequences, regenerate PDFs,
or edit manuscript sections. Existing `readers.py`, `paired.py`, `tcp_review.py`,
`full_review.py` and the numerical OS-preparation code are unchanged, preserving
their existing code identities and prepared caches.

## Fast additions

- OS percentages use the existing PR178 `__adjusted.json` preparation, with the
  same complete-group denominators as the adjusted heatmaps, including
  Unclassified and Not Enough Samples. No per-IP OS join is rebuilt. The cache's
  original report, inputs, taxonomy and conservation checks are reused.
- The requested EU scalar keys use `mikrotik_routeros`, `hpe_comware`,
  `tp_link`, `check_point` in their public names. These identifiers map to the
  stored hyphenated OS groups. The exact tag split remains RouterOS only for
  `OS_TAG=mikrotik-routeros` and Comware only for `OS_TAG=hpe-comware`.
- Existing `os.mikrotik.*` and `os.mikrotik-zyxel.*` recipes are replaced by
  `os.mikrotik_routeros.*` and `os.mikrotik_routeros-zyxel.*` across all six VPs.
  This changes their populations as well as their names. Historical source
  aggregates stay untouched. Manuscript references must be updated separately.
- EU TCP-Connection OS additions have `.full` suffixes. They are bound to the
  registered Full completion inventory and measurement ID; no historical
  Connection sample fallback is allowed. The global six-VP completion and the
  selected site's inventory must agree before any Full OS scalar is exported.
- `results.eu.tcp.no-conn.rt-fi.os.zte-aruba-mikrotik_routeros-zyxel-tp_link-pfsense-opnsense.constant.{min,max}-pct`
  compares the seven individual unrounded shares, rather than pooling counts.
- `results.{vp}.{icmp,tcp.no-conn,udp}.rt-fi.success-all.pct` is 100 times the
  existing once-per-IP `success-all.count`, divided by the original RT ZMap
  target count. Missing/zero denominators stay unavailable; Phase-2 IPs are
  never counted twice. For Connection use the existing
  `results.{vp}.tcp.conn.rt-base.success.pct.full`, not an invented RT/FI merge.
- `results.vp-comparison.{family}.{strategy}.mean[.full]` is the unweighted
  arithmetic mean of the six unrounded VP percentages. It is unavailable when
  any VP is missing. It is not a pooled/population-weighted percentage.
- VP ranges are displayed as `<0.01` in TeX whenever their unrounded value is
  below 0.01 percentage points, including exactly zero. Use `\getval`, not
  numeric `\getnumf`, for these display values. At exactly 0.01 the output is
  `0.01`. The exact raw scalars and definitions remain in
  `tmp/values.json` under `paper_values.values`; the existing derived-cache
  contexts are retained.
- The 54 non-applicable Connection RT-BASE Multi/Random/NES distribution and
  VP min/max/range keys (historical and Full) are no longer exported. New means
  likewise cover only the six Base rules and Unclassified. Valid observed
  zero shares in applicable categories remain zero. Coverage failure counts
  are unaffected; non-applicability must not be read as an empirical 0% finding.

For Phase-2 prose use `results.eu.icmp.fi-mass.all.*`, in particular
`results.eu.icmp.fi-mass.all.single-per-destination-per-bucket.pct`. This uses
all selected Phase-2 targets, including missing/insufficient results. The
historical sufficient-result metric is retained with its distinct denominator.

## Explicit one-off EU ICMP outlier preparation

```sh
# Small reports and Parquet footers only; no label/raw row scans or output writes.
make prepare-paper-outliers CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--dry-run"

# Explicit data read and calculation; a valid completed cache is reused.
make prepare-paper-outliers CAMPAIGN_ID=geo-full-20261107-r1

make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"
make paper-figures-zip
```

Use the existing VM storage environment and bounded RAM/DuckDB budgets, and
stop other analysis jobs before changing the repository checkout. This command
does not need `prepare-paper-full`, `--prepare-os-adjusted`, probing-interval
preparation or a plot render. Root overrides (`--reports-root`,
`--processed-root`, `--raw-root`, `--repository-root`) support isolated audits.

The preparation performs the following operations in order:

1. Select only EU/ICMP FI-MASS IPs classified as Single, Per-Destination or
   Per-Bucket, preserving each IP's FI class; validate these counts against
   the canonical FI report and require unique, non-null candidate addresses.
2. Read the corresponding RT labels and retain RT-Unclassified IPs. Record
   both the candidate and retained counts. Missing/duplicate RT labels fail.
3. Fetch only the retained IPs' RT sequences, projecting just `IP_ADDR` and
   `IPID_SEQUENCE`. No FI raw sequences or unrelated raw columns are used.
   Each selected raw sequence must occur exactly once and contain its complete
   snapshot-defined sample positions and valid 16-bit values. Missing,
   duplicate or malformed rows fail without silently reducing the denominator.
4. Compute the greedy score, vectorized across batches. The first IP-ID in
   each rule stream initializes its last-valid reference. Subsequent values
   are compared modulo 65536; invalid values add one and are ignored, without
   moving the reference. Single has one global stream; Per-Destination uses
   the two interleaved source streams and increment exactly one; Per-Bucket
   uses the original connection/probe streams and the production bounded
   positive increment rule. Skipping a value never renumbers probe positions
   or changes the required increment according to the number skipped. The
   initial reference is not retrospectively repaired. This is deliberately
   order-dependent, not a minimum-deletion or bad-adjacent-edge metric.
5. Publish the complete integer-score histogram, per-FI-class histograms,
   cohort counts, score-zero diagnostic, layout, stage timings and source
   fingerprints to the new immutable cache
   `data/processed/continental/<campaign>/eu/paper-review/eu-icmp-rt-greedy-outliers-v1.json`.

The public six keys are:

```text
results.eu.icmp.fi-mass.all.single-per-destination-per-bucket.rt-outlier-score.0.pct
results.eu.icmp.fi-mass.all.single-per-destination-per-bucket.rt-outlier-score.1.pct
results.eu.icmp.fi-mass.all.single-per-destination-per-bucket.rt-outlier-score.2.pct
results.eu.icmp.fi-mass.all.single-per-destination-per-bucket.rt-outlier-score.3.pct
results.eu.icmp.fi-mass.all.single-per-destination-per-bucket.rt-outlier-score.4.pct
results.eu.icmp.fi-mass.all.single-per-destination-per-bucket.rt-outlier-score.5.pct
```

Their denominator is the entire retained RT-Unclassified/FI-counter cohort,
not all Phase-2 targets and not just scores 0..5. Scores greater than five stay
in the prepared histogram and denominator, so the six displayed percentages
need not sum to 100. Score zero is exported and reported explicitly rather
than being removed to enforce the expected near-match interpretation.

Ordinary export reads this compact JSON and validates the current canonical
RT/FI report binding and source metadata. Missing preparation leaves these six
values unavailable; it never triggers an automatic raw-data scan. Stale,
foreign or inconsistent preparation fails strict export, retaining previous
exports. Existing preparation files are not overwritten automatically.

The dry run reports an upper-bound compressed size for the two projected RT
columns. Selecting around 300k IPs does not guarantee proportional physical
I/O: unsorted Parquet rowgroups may still require most projected column data
from the S3 mount. No unmeasured runtime/speedup is promised. Candidate/cohort
stages and scored-batch progress are logged separately so the VM run can be
measured before making any larger changes.
