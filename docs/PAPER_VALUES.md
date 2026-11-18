# Paper values

Run from the Analysis VM checkout:

```sh
make paper-values CAMPAIGN_ID=geo-full-20261107-r1
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"
```

Uppercase `ARGS` takes precedence over lowercase `args`. The command writes only
`tmp/values.tex` and `tmp/values.json`. It reads the existing campaign data; it
does not run a workflow, start measurements, import `ipid_analysis`, or rebuild
large raw-data joins. Existing analysis workers can continue running.

## Sources and keys

`paper_values/metrics.json` is the single definition of the selected paper scope:
200 numeric metric families and 29 PDF-path families. Expanding the VP-dependent
families gives 1,264 public keys: 207 per VP (`af`, `as`, `au`, `eu`, `na`, `sa`)
and 22 shared/global keys. All six VPs are exported, even when the text discusses EU.
There is no internal/public key translation or general diagnostic catalog.
`--profile full` has been removed.

The selection covers the currently mapped, methodologically compatible paper
values, including the two compatible synthetic-validation figures. It is not a
claim that every old statement has already been reconciled with the new method.
Unused, obsolete, manual and scientifically unresolved families are omitted
from the definitions, rather than filtered after their values are calculated.
Shared input reports may still be needed for retained fields, cohort checks and
canonical-run selection. A selected key is not evidence that its value is available.

### Naming

The public keys use five prefixes:

| Prefix | Meaning |
|---|---|
| `results` | Measured or derived campaign results, including distributions, ratios and paired comparisons |
| `validation` | Synthetic validation scenarios and classifier performance on those scenarios |
| `classifier` | Exact constants and thresholds of the classifier |
| `params` | Measurement settings |
| `fig` | PDF paths relative to the paper repository |

`claims`, `stats`, `probing`, `incs` and `tcp-transition` are folded into
`results`. The use of a number in a paper claim does not define a different kind
of measurement result. Increments retain an explicit `increments` component
after the measurement phase. The paired TCP transition uses
`results.{vp}.tcp.rt-no-conn-to-conn...`.

TCP retains `no-conn` or `conn`; ICMP and UDP omit the redundant `no-conn`
component. ITDK role components are `routers` and `all-others`. These are public
key names only: existing JSON selectors still read `transit-observed` and
`no-transit-evidence`, and source report/PDF paths are unchanged. Renaming a key
does not change its population, denominator or value.

Examples:

```text
results.eu.icmp.rt-fi.itdk-strategy.routers.single.pct
results.eu.tcp.conn.rt-base.itdk-strategy.all-others.single.pct
results.eu.tcp.conn.rt-base.increments.per-bucket.p99
fig.eu.udp.rt-fi.os-by-strategy
```

Only the new public names are exported; there are no legacy aliases or runtime
name translations. Update paper references and any `--additional-report` keys
together with the exporter. The derived-value cache depends on source data and
calculation code, not public key spelling, so the rename does not invalidate it.

Each group shares a source context and unit. Its `prefix` plus each suffix in
`values` is the **actual public key**, not an intermediate name. For example:

```json
{
  "prefix": "results.{vp}.tcp.no-conn.rt-fi",
  "unit": "percent",
  "sources": [{
    "reader": "field",
    "protocol": "tcp",
    "connection": "no-connection",
    "phase": "rt-fi-merged",
    "kind": "itdk-role-by-strategy"
  }],
  "values": {
    "itdk-strategy.transit-observed.single.pct": {
      "definition": "SINGLE / Transit-Observed population in the report",
      "read": {"field": ["plot-percentages", "transit-observed", "single"]}
    }
  }
}
```

The resulting key is `results.eu.tcp.no-conn.rt-fi.itdk-strategy.routers.single.pct`.
Field lookup normalizes case/underscores but rejects ambiguous field names.
Only declared fields/report families are read. Values are never discovered by
flattening every scalar in a report. Scientific readers in `readers.py` handle
the existing ratios, OS-family ranges and exact histogram statistics.

Default read-only inputs:

- `reports/continental/<campaign>`: existing JSON reports and selected PDF metadata;
- `data/processed/continental/<campaign>`: the existing small OS/strategy and
  positive-increment histogram aggregates, and per-IP strategy labels for paired metrics;
- `data/raw/continental/<campaign>`: measurement YAML snapshots and the persisted
  FI target samples with their JSON metadata;
- `reports/figures/classifier-validation/classifier-validation-summary.json` and
  its two selected PDFs. Override this directory with `--validation-root`.

Archive exports do not read implicit global validation inputs. Use
`--validation-root` explicitly when combining an archive with local validation.

The current paper selection reads reported scalars directly from JSON and computes
only the explicitly selected derived quantities from existing Parquet inputs. The large
repetition CSVs contain per-target observations, not additional selected paper
scalars, and are not scanned. A future CSV metric should name its exact file,
columns and reduction explicitly rather than introducing a generic directory-wide
CSV scan.

Canonical ZMap reports are preferred over streaming copies; repetition runs are
not substituted for baseline measurements. Conflicting copies are reported.
Streaming Mass baselines are selected only when existing provenance identifies
them: a canonical primary/merged report or the reproducibility evaluation's
explicit baseline ID. Repeat coverage reports are excluded by their directory ID
even though their JSON has no measurement ID. While this provenance is still
pending, unbound Mass values remain empty without blocking other VPs, including
in strict mode. Neither timestamps nor population sizes select a baseline.
Later publication of the required provenance makes the values available on the
next export. Conflicting explicit identities and conflicting canonical copies
still fail strict export.
Existing JSON completion receipts are checked against bytes. Campaign PDF checks validate
the signature, size and receipt metadata. The two global validation PDFs additionally
match the full hashes in their generating summary. Missing
receipts remain supported for legacy reports. The output contains paper-relative
PDF paths; copy `reports/` without changing its relative structure.

## Derived-value cache

`values.json` caches only derived scalar tables from existing processed inputs:
OS/strategy cells, exact increment-histogram statistics and paired-population counts. It does **not** cache
entire JSON reports, PDF inventories or raw probe sequences. Source reports are
read fresh on every export. One source file is reused within a run for all metrics
that depend on it.

Cache entries are scoped by campaign, calculation code, operation, full input path,
source file size/timestamps, completion receipt and accompanying report hash.
Changing a source or calculation invalidates its entry. Missing sources are
checked again next run. A damaged or old-format cache is disposable; it cannot
replace source data. The former metadata cache is rebuilt in the new format once.

```sh
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--strict --recompute --timings"
```

`--recompute` ignores cached derived values. It does not bypass a mounted S3
filesystem's own caches. `--io-workers` bounds both directory inventory and file
reads: the default is 8 (maximum 32); 1 selects serial operation. Inventory jobs
cover one existing VP directory in either the reports or processed campaign root
(at most 12 jobs). Only the six VP directories are searched. Within recognized
measurement directories, the metric definitions select connection modes, phases,
merge/comparison pairs and the random-reproducibility evaluation subdirectory
before their directory listings are requested. Unknown layouts inside a VP retain
recursive discovery. Results are sorted independently of worker completion order.

When raw inputs are configured, required measurement snapshots are prefetched
with the same worker limit. Each unique VP/protocol/connection/phase request is
loaded once per export through the existing snapshot reader (currently at most
42 requests). The scalar metric loop then uses those results in its original
order. Failed reads are retained and reported against the same metric keys;
strict export still preserves existing output files. Snapshot data is not added
to the persistent derived-value cache and is refreshed on the next invocation.

The inventory is rebuilt on every invocation; it is not stored in `values.json`.
New reports are rediscovered, and listing/read errors still fail strict export.
ZIP reads remain serial. Compare `inventory` and `total` in `--timings` on the
actual Analysis VM; local ZIP or filesystem timings do not predict S3 mount speed.

### Diagnosing slow mounted inputs

`--timings` additionally prints live derived-operation progress, operation counts,
summed elapsed seconds and the
longest individual call. These details are console-only: they do not enter either
export file and do not invalidate the derived-value cache. Without `--timings`,
operation timing is disabled. Existing phase timings remain wall-clock durations.

| Operation | What is measured |
| --- | --- |
| `json.check`, `pdf.check` | Source fingerprints and completion checks before/after reading, including receipt reads |
| `json.read` | Opening/reading JSON bytes from a directory or ZIP |
| `json.decode` | Decoding JSON report bytes |
| `pdf.read` | Opening a PDF and reading its signature |
| `cache.load` | Loading and initializing the derived-value cache |
| `cache.check` | Checking aggregate existence/fingerprints/receipts and cached scalar integrity; also checks missing sources |
| `cache.verify` | Validating an aggregate's completion receipt on a cache miss |
| `cache.compute` | Reading an aggregate and computing its scalar table on a cache miss |
| `snapshot.access` | Snapshot lookup, existence/receipt checks, reading and YAML parsing; one access per unique prefetched request when raw inputs are configured, otherwise memoized metric lookups |
| `snapshot.prefetch` | Wall-clock duration of the snapshot prefetch (one call), included in `derive`; worker access sums can exceed this duration |

Operation counts include failed attempts. A valid JSON/PDF normally has two
`check` calls, one before and one after reading. Sums across parallel workers can
exceed a phase's wall time, so do not add operation rows to obtain total runtime.
They are diagnostic subsets of the work, not a complete phase accounting.
On a warm derived-value cache, `cache.compute` and `cache.verify` disappear, while
`cache.check` and snapshot accesses can still take time on the mounted inputs.

## Partial archives and missing results

```sh
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--archive /path/to/eu-results.zip --strict"
```

An archive is read without extraction. In archive mode, default mount inputs are
disabled. Explicit `--reports-root`, `--processed-root` and `--raw-root` may
supplement it. Archive and directory copies must agree.

A reports-only ZIP lacks YAML snapshots and Parquet aggregates. Consequently it
can populate fewer keys than an export on the Analysis VM. Replacing a fuller
paper export with that partial export can make previously known values empty.
Values from previous exports or other campaigns are never used to fill gaps.

`--strict` rejects invalid or conflicting required inputs before replacing either
existing output. Missing results alone are allowed. Without strict mode, input
errors are printed and affected values stay empty. Conflicting shared settings
always reject the export. Observed zero stays zero; percentages with a zero
denominator stay empty. In particular, the full RT population is not used as a
replacement denominator for a matched RT/FI comparison.

Known missing scientific reports remain missing. Campaign dates, public
source-prefix properties, resource requirements and editorial settings are
manually maintained paper information. The following five key families (20
concrete keys across the six VPs) are deliberately excluded from the export:

- `campaign.{vp}.start-month`
- `campaign.{vp}.end-month`
- `params.{vp}.source-addresses.prefix-length`
- `artifacts.required-disk-gib`
- `artifacts.required-memory-gib`

The exporter neither infers these values nor emits empty placeholders for them.
The current scope also omits unused dataset-provenance keys, support metrics
without a confirmed paper occurrence, earlier validation diagnostics and the
unresolved RT-outlier claims. None of those keys is calculated or emitted as an
empty placeholder. Their absence does not mean the associated analysis reports
should be deleted. Refresh the generated export; do not delete its keys manually.

The previously absent
`results.{vp}.tcp.conn.rt-base.itdk-strategy.all-others.single.pct`
reads the existing report's `plot_percentages["No Transit Evidence"]["SINGLE"]`.
This is already a percentage, not a fraction, and is not multiplied by 100.

All 17 formerly unimplemented families (40 numeric keys plus two PDF keys) have
explicit readers. A missing input still leaves the corresponding value empty;
there are no `reader: "missing"` definitions in the selected catalog.

### Synthetic validation

After updating the checkout, regenerate the current production-profile validation:

```sh
IPID_ANALYSIS_SCOPE= make validate-classifier
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"
```

The first command additionally writes
`reports/figures/classifier-validation/classifier-validation-summary.json`.
It includes the actual Base/Mass sequence lengths and impairment settings,
the existing confusion metrics for all five needed scenarios, classifier/code
identity and hashes for the two selected PDFs. It records the same metrics used
by the figures, without rerunning classification for the exporter.

The ten validation keys and two PDF keys read this summary automatically.
Accuracy fractions are converted to percentages once. MULTI misclassification
uses `(support - correct) / support`; minimum recall is the minimum MULTI/RANDOM
class recall across the separate 20% Lossy and 20% Reordered scenarios. It does
not silently substitute the combined loss-and-reordering scenario.

Legacy/custom-null-table validation results and reports from different classifier
or generator code are rejected. Use the normal production defaults for the paper;
the documented small smoke run is not an exportable production-profile result.
A missing summary stays empty until validation is run. Existing NIST diagnostics
remain a separate command and are not substituted for older paper figures.

### Matched FI/RT and TCP populations

For each VP/protocol/mode, the FI/RT reader uses the canonical RT-BASE and FI-BASE
strategy Parquets. The target is
`data/raw/continental/<campaign>/<vp>/zmap/<zmap-id>/zmap-fixed-base-sample.pq`
with its `.json` metadata; TCP connection mode uses `zmap-connection-sample.pq`
instead. The persisted metadata must identify that discovery and target type.
The target size must match FI coverage, and all FI results must belong to it.

Let T be this target set, R the sufficiently answered RT rows in T, and F the
sufficiently answered FI rows. The exported value is `100 * |F| / |R|`.
`UNCLASSIFIED` still means sufficient replies and is included; only
`NOT_ENOUGH_SAMPLES` is excluded. This is a ratio of success counts in the same
target population, not `|F intersection R| / |R|`, and can exceed 100%.
An empty denominator stays unavailable.

The TCP transition joins the no-connection and connection RT-BASE labels by IP,
retaining only IPs with sufficient replies in both measurements. The denominator
is the number of PER_CONNECTION labels after connection establishment in this
common population. The numerator is the subset labelled CONSTANT before it.

Both readers require matching discovery/classifier identities and verify that
Parquet class counts agree with the selected JSON reports. Duplicate IPs in the
joined populations, invalid target membership or inconsistent identities reject
strict export. They never reclassify sequences or rebuild existing analyses.
The readers use the existing `IPID_ANALYSIS_DUCKDB_MEMORY` and
`IPID_ANALYSIS_THREADS` environment variables. Defaults remain 2 GB and at most
eight threads. For the 24-vCPU/256-GB analysis VM, with enough RAM currently free,
a larger budget can be selected for this invocation without changing workers:

```sh
IPID_ANALYSIS_DUCKDB_MEMORY=64GB IPID_ANALYSIS_THREADS=12 \
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"
```

Measurement inputs continue to be read through the existing S3 mounts. FI labels
and TCP connection labels are materialized once per comparison, using only
`IP_ADDR` and `IPID_SELECTION_STRATEGY`; subsequent checks and joins reuse the
RAM table. The much larger full RT population remains a projected view, so it
is not copied wholesale into RAM. All scientific checks and denominators remain
unchanged.

**File spilling is disabled**, with both an empty `temp_directory` and a zero
`max_temp_directory_size`. There is no `/tmp`, local-disk, or S3-file fallback for
intermediate query data. If the selected memory budget is insufficient, the
calculation fails with an explicit RAM-budget error. `memory_limit` is DuckDB's
buffer-manager budget, not a hard limit on total process RSS. Keep headroom for
other allocations and the running analysis workers.

`--timings` prints the selected resources immediately and reports each derived
operation's start, completion or cache hit while the export is running. Output
values and formatting are unaffected. A different environment budget can reuse
the same derived cache; the calculation-code update itself invalidates the old
cache once. Only the small resulting counts are cached in `values.json`.
The existing final output files remain `tmp/values.tex` and `tmp/values.json`;
the exporter creates no persistent bulk-data copies.

An already running process does not adopt the new settings. Finish or interrupt
that export before starting the updated version; completed derivations from an
interrupted run have not yet been written to the cache.
All contributing Parquets, target metadata and report hashes invalidate the cache
when changed. A warm export avoids repeating the joins.

For an existing supplemental metric report:

```sh
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--additional-report /path/to/validation-values.json --strict"
```

The explicit schema uses **public keys**, not the former internal names:

```json
{
  "schema_version": 1,
  "campaign_id": "geo-full-20261107-r1",
  "values": {
    "validation.fi-ideal.accuracy-pct": {
      "value": 99.3,
      "unit": "percent",
      "definition": "Accuracy of the specified current synthetic validation"
    }
  }
}
```

The key must be defined in `metrics.json`, and unit/campaign must match.
`--additional-report` reads existing results; it does not run missing analyses.
New scientific metrics and currently unexported paper keys can be added separately.

## LaTeX

```tex
\usepackage{todonotes}
\input{values}
\getval{results.eu.tcp.no-conn.rt-base.success.constant.pct}
\getval{classifier.single.max-inc}
```

The standalone `setval`/`getval` convention is compatible with ndpi. `getval`
returns stored values unchanged. Its generated fallback uses the existing
`\todo[inline]{...}` for both an undefined key and a defined empty value. Existing
consumer macros can override the provided fallback.

Percentages retain the existing two-decimal display and escaped percent sign;
time strings retain their recorded unit. Counts and exact thresholds are not
compacted. Other floats retain their raw Python precision rather than twelve
significant digits. Raw cached values are never rounded.

Explicit compact formatting belongs in the paper's planned `getvalcompact`
function. It should operate on plain real numbers and be invoked only where
`k`/`M`/`B` or scientific notation is desired. The exporter does not supply a
custom formatter or figure/TODO framework. Updating the paper's existing
`getvalf` integration is a separate step.

## Output and tests

The two output files are replaced atomically **individually**, not as one filesystem
transaction. Output symlinks, writes into input directories and S3/FUSE output
paths are rejected. Run one exporter at a time. Unrelated files in `tmp` are kept.
No Markdown, logs, per-input receipts or report-side caches are generated.

```sh
python -B -m unittest discover -s tests -p 'test_paper_value*.py'
IPID_PAPER_VALUES_ARCHIVE=/path/to/eu-results.zip \
  python -B -m unittest discover -s tests -p 'test_paper_value*.py'
```

Tests cover public keys, six-VP separation, denominators, canonical/repeated runs,
source receipts, incomplete data, processed-aggregate caching and invalidation,
exact thresholds, the TeX interface, input immutability and the CLI/Make wrapper.
The optional supplied EU archive test checks known populations and partial-VP
behavior. The Python diagnostic return now uses public keys directly; the former
internal-key `key_map` and `profile` API are removed together with the diagnostic
catalog.
