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

## ZIP the figures referenced by values.tex

```sh
make paper-values CAMPAIGN_ID=geo-full-20261107-r1
make paper-figures-zip
```

The second command writes `tmp/paper-figures.zip`. It reads the existing
`tmp/values.tex` and includes exactly the nonempty `fig.*` PDF paths recorded
there, deduplicating repeated references. It does not regenerate the values,
select other figure variants, render plots, or read measurement tables.
Consequently, a `__paper.pdf` is included precisely when the exported value
references it; otherwise the named original PDF is included.

Archive members start directly with `reports/continental/...` or
`reports/figures/...`, without an extra wrapping directory. Extract the ZIP at
the root of the paper repository; its paths then match the exported values.
The ZIP contains only those PDFs, not `values.tex`, reports, caches or receipts.
Copy `values.tex` to the manuscript as usual. No manuscript files are modified
by the packaging command.

Optional alternate inputs/output name:

```sh
make paper-figures-zip \
  ARGS="--values tmp/other-values.tex --output-name other-figures.zip"
```

`--reports-root /path/to/reports` maps the same paper-relative paths onto another
mounted or local reports directory. Output names must be plain ZIP filenames;
the archive always stays under the checkout's local `tmp/` directory.

Empty figure values are skipped and counted. Missing named PDFs, malformed
assignments, invalid PDF signatures or publication receipts, and inputs changing
during packaging fail clearly. PDFs are streamed in bounded chunks, with stored
checksums verified when a receipt exists. The ZIP is CRC-checked and committed
atomically only after all references have been copied successfully. On failure,
an existing `tmp/paper-figures.zip` is retained and temporary files are removed.
All source PDFs, source receipts, `values.tex` and the numeric cache remain unchanged.

## Sources and keys

### Completed full TCP-Connection follow-up

For `geo-full-20261107-r1`, the normal commands also include the completed
`tcp-connection-full-20261121-r1` follow-up:

```sh
make paper-values CAMPAIGN_ID=geo-full-20261107-r1
make paper-figures-zip
```

`paper_values/followups.json` pins the exact published analysis
`analysis-20261122T140145060416Z` and its six original TCP discoveries. No
directory-wide "latest run" search is performed. The exporter reads the existing
global/site `done__full.json` inventories under
`reports/followups/<measurement-run>/<analysis-id>/`, the selected small JSON
summaries, and the six existing `__paper__full.pdf` files per VP. It never starts
a measurement, classifier, plotting command, raw-data join or follow-up analysis.
All input files stay unchanged; repeated exports reuse the same pinned results.

The additional public keys end in `.full`; original keys retain their original
values and PDF paths. Examples:

```text
results.eu.tcp.conn.rt-base.targets.count.full
results.eu.tcp.conn.rt-base.success.count.full
results.eu.tcp.conn.rt-base.coverage.pct.full
results.eu.tcp.conn.rt-base.success.single.pct.full
results.eu.tcp.conn.rt-base.itdk-strategy.routers.single.pct.full
results.eu.tcp.conn.rt-vs-fi-base.paired-success.count.full
fig.eu.tcp.conn.rt-vs-fi-base.strategy-intersection.full
```

The JSON-backed RT-BASE strategy counts/shares and selected ITDK role shares
have `.full` counterparts. Coverage uses **all original SYN-ACK targets** as
its denominator. The three RT/FI comparison figures instead use **jointly
successful new RT and historical FI-BASE IPs**, as recorded in the completion
inventory; FI-BASE was not repeated. The remaining three paper PDFs show the
full RT ITDK roles, full RT OS groups, and the TCP measurement-type comparison
with the updated Connection bar. All 36 additional paper PDFs keep their
`__paper__full.pdf` filenames and `reports/followups/...` paths in the ZIP.

This registration adds only values already represented by the selected JSON
summaries. It does not fabricate full-population increment quantiles from the
paired histogram, reinterpret old FI statistics, or derive exact statistics
from the downsampled increment CDF. Historical increment keys remain unchanged.

If the registered run is unavailable, its keys are empty; another run is never
substituted. A completed but corrupt/inconsistent run is reported as an input
error, with no partial follow-up import. `--strict` retains the previous outputs
on such errors. Archive exports do not implicitly consult live follow-up mounts.
For an archive or an alternate layout, explicitly provide the existing directory:

```sh
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--followups-root /path/to/reports/followups --strict"
```

The ZIP command needs no special option: it already bundles exactly the `fig.*`
paths in `values.tex`, including the new `.full` entries. Regenerate `values.tex`
before creating a ZIP after deploying this change.

`paper_values/metrics.json` defines the original campaign paper scope (the
registered follow-up additions above use separate `.full` keys):
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

### Paper-ready figure paths

For each selected `fig.*` value, the exporter prefers the sibling
`<original-stem>__paper.pdf` when present; otherwise it exports the original PDF.
For the two merged stateless ITDK role figures it first prefers
`n-rt-b_fi-m_itdk-role-by-strategy__paper__final.pdf` and
`n-rt-b_fi-m_itdk-role-by-os__paper_final.pdf`, respectively. These Final
figures use all measured IPs minus Router for All Other (with resolved OS only
in the OS figure). Existing numeric ITDK scalar keys retain their original
report populations. See [Final ITDK figures](PAPER_PLOTS.md#final-itdk-complement-figures).
This works for mounted directories, ZIP archives and mixed inputs, without
changing public keys or selecting a different measurement run. Canonical-run
selection happens first; paper/original variants of the same figure are then
resolved. Ambiguous runs or conflicting copies of the preferred PDF still fail
strict export. PDF signatures and existing completion receipts remain checked;
a broken preferred variant is reported rather than silently accepted.

The global validation figure paths use the same preference, while the original
PDF must still match the full hash in its generating classifier summary.
No plots or source data are created, modified or renamed by this selection.
After `make paper-plots`, rerun `make paper-values` to refresh `tmp/values.tex`.
Figure availability is inventoried each run, so neither `--recompute` nor cache
deletion is needed. If a paper variant later disappears, the original path is
selected again. Existing numeric derived-value cache entries remain reusable.

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

Prepare the additional immutable validation inputs and render the approved
paper figures before exporting values:

```sh
make validate-classifier ARGS="--prepare-paper --workers 8"
make paper-plots ARGS="--validation --workers 4"
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"
make paper-figures-zip
```

The exporter reads only the small `__paper.json` sidecars and referenced PDFs.
It does not import classification/rendering code, open raw Parquet, generate
sequences or rerun statistical tests. All twelve validation figures enter the
same paper-relative `reports/figures/classifier-validation/...` paths, including
the NIST subdirectory. The ZIP command therefore bundles them without a second
selection mechanism.

New public key families include:

| Key | Meaning |
| --- | --- |
| `validation.e2e.<condition>.accuracy-pct` | Correct final labels / all original targets |
| `validation.e2e.<condition>.balanced-accuracy-pct` | Mean recall across all eight true classes |
| `validation.e2e.<condition>.total.count`, `.correct.count`, `.unclassified.count`, `.unclassified.pct` | Exact E2E population and final Unclassified outcomes |
| `validation.e2e.<condition>.class.<class>.recall-pct`, `.precision-pct` | Final per-class metrics |
| `validation.e2e.routing.<class>.support.count`, `.forwarded.count`, `.forwarded.pct` | Actual RT-Unclassified handoff to FI |
| `validation.random-score.<condition>.random.rejection-pct` | True RANDOM rejected by the isolated production compatibility score |
| `validation.nist-score.<condition>.random.rejection-pct` | Corresponding adapted NIST rejection rate |
| `validation.random.threshold`, `validation.nist.threshold` | Frozen score cutoffs |
| `validation.data.nontrivial-samples-per-class.count`, `.trivial-samples-per-class.count`, `.seed`, `.null-samples.count` | Numerical population and reproducibility settings |
| `fig.validation.e2e-ideal.confusion-matrix` | Two-stage ideal E2E paper figure |
| `fig.validation.fi-lossy-vs-lossy-reordered.confusion-matrix` | Additional paired Mass paper figure |
| `fig.validation.random-score-cdf.<condition>`, `fig.validation.nist-score-cdf.<condition>` | All eight CDFs |

Conditions are `ideal`, `lossy`, `reordered`, `lossy-reordered`; class names use
lowercase hyphens (e.g. `per-bucket`). Accuracy/recall/precision fractions are
converted to percentages exactly once. E2E denominators retain early RT errors
and final Unclassified targets. Isolated score rejection is not full-classifier
error or proof of a random generator. Precision follows the stored zero value
when a class has no predictions.

The existing ten validation numeric keys and two figure keys are retained.
They prefer verified new sidecars and otherwise retain the original
`classifier-validation-summary.json` reader. The historical public key
`fig.validation.fi-lossy-reordered.confusion-matrix` continues to mean the
**lossy-vs-reordered** figure. The new **lossy-vs-lossy-reordered** figure has its
own explicit key. Legacy RT-only accuracy still concerns the six supported RT
classes; actual forwarding of MULTI/RANDOM is not counted as an error in that
legacy key. With new sidecars it uses those six classes of the paired E2E RT
cohort, with source attribution pointing to the E2E sidecar.

The known pre-#168 summary remains readable after the CLI-only extension.
Compatibility requires its exact historical generator hash, an unchanged
generator AST excluding only `main`, and exact hashes for all four other
classifier modules. Unknown generators and numerical changes still fail;
the exporter never rewrites or re-stamps the old summary.

New sidecars are checked against current model/code fingerprints, production
sample sizes, unchanged cutoffs and exact confusion/routing counts. Small smoke
runs, altered code, invalid denominators or inconsistent accuracy fail strict
export and preserve the previous `values.tex`/`values.json`. A missing report
leaves its values empty. The full historical `make validate-classifier` exporter
without `--prepare-paper` retains its original behavior and is not required by
this additive workflow.

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

Percentages retain the existing two-decimal display and escaped percent sign.
Validation assignments are grouped with blank lines only between related blocks:
dataset settings, each E2E condition, routing, standalone FI results, NIST,
RANDOM, and standalone RT. Validation figure paths are grouped as E2E, FI,
NIST CDFs, and RANDOM CDFs. Key ordering and non-validation grouping are unchanged.
This affects only `values.tex` whitespace; numerical values and cache identities
are preserved.

Time strings retain their recorded unit, with a nonbreaking TeX space between
number and unit (`20ms` becomes `20~ms`, `2s` becomes `2~s`). This happens only
when rendering `values.tex`: raw values, calculations and the derived-cache
identity are unchanged. An existing valid `values.json` is reused without
recomputing unchanged inputs; source inventory and integrity checks still run.
Counts and exact thresholds are not
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

## Revision exports: coverage, VP spread, EU DF and interprotocol cohorts

The additional families are defined in `paper_values/review.py`. They extend the
existing keys; historical TCP transitions and registered `.full` follow-up values
retain their original populations and definitions.

### One-off preparation and ordinary exports

```sh
# Use the same mounted-storage environment as the existing paper commands.
# The background workflow worker can keep running. Run one preparation at a time.
IPID_ANALYSIS_THREADS=4 IPID_ANALYSIS_DUCKDB_MEMORY=16GB \
  make prepare-paper-review CAMPAIGN_ID=geo-full-20261107-r1

make paper-plots CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--dry-run"
make paper-plots CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--workers 4"
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"
make paper-figures-zip
```

Only explicit preparation scans the EU TCP IPID/DF sequences and the small local
reference label tables. It publishes new versioned JSON aggregates below
`data/processed/continental/<campaign>/<vp>/paper-review/`. Existing reports,
labels, snapshots and raw files are read only. Unchanged inputs reuse the prepared
aggregate without scanning sequences again. Source metadata/receipts are checked;
stale preparation is an error, not permission for an ordinary export to reread raw
data. `--recompute` on paper-values does not opt into TCP raw preparation.
Changed source data can be prepared again; no original analysis is overwritten.
The resource limits reuse the existing paired-reader environment, with disk
spilling disabled. Default small budgets may be insufficient for the EU merge.

Ordinary paper-values reads small coverage/strategy/outcome JSONs and prepared
summaries. Missing preparation leaves the seven EU TCP percentages empty. A zero
denominator is empty, never a measured 0%. Prepared audit JSONs retain exact counts,
unknown/mixed DF and input identities, even when these are not public TeX metrics.

### Populations and keys

* `results.<vp>.<icmp|tcp|udp>.zmap.count` stores the original protocol discovery
  population once, using canonical stateless RT coverage. It is not a later
  sample size. Protocol/VP populations must not be summed as globally unique IPs.
* Phase populations extend the existing key families:
  `results.<vp>.<icmp|udp>.<rt-base|fi-mass|fi-base>.*` and
  `results.<vp>.tcp.<no-conn|conn>.<phase>.*` (connection has RT/BASE and FI/BASE).
  The four suffixes are `targets.count`, `targets.pct`, `success.count`,
  `success.pct`. Existing `success.count` keys are reused and checked for agreement.
  `targets.count` comes from the phase's coverage metadata. `success.count` is the
  strategy population minus NOT_ENOUGH_SAMPLES, **including UNCLASSIFIED**.
  `success.pct = 100 * success.count / targets.count` for every phase.
* Final RT/FI-MASS populations also expose these four suffixes under
  `results.<vp>.<icmp|udp>.rt-fi.*` and `results.<vp>.tcp.no-conn.rt-fi.*`.
  The published merged strategy report supplies `success.count` (all rows except
  NOT_ENOUGH_SAMPLES, including UNCLASSIFIED). `targets.count` remains the original
  RT/BASE selected target count from coverage metadata; it is not the number of
  RT responders or RT plus FI targets. `targets.pct` uses the original protocol
  ZMap population, and `success.pct` uses these original selected RT targets.
  Original RT nonresponders and failed FI refinements therefore remain failures.
  No Connection RT/FI-MASS merge is invented; Full Connection RT stays separate.
  These values only read existing small JSON reports and do not require preparation.
* Selection percentages use the actual source population: stateless RT/BASE and FI/BASE
  `targets.pct` divide by `results.<vp>.<protocol>.zmap.count`; TCP Connection
  RT/BASE and FI/BASE divide by `results.<vp>.tcp.zmap.syn-ack.count`, read from
  `eligible_rows` in the frozen `zmap-connection-sample.json`. The original TCP
  ZMap count still includes the other TCP reply types; it is not overwritten.
  FI/MASS divides by
  the same VP/protocol's RT/BASE `success.count`. For example, selecting 20 FI/MASS
  targets from 200 RT successes gives 10%, even if original discovery had 1,000 IPs.
  Missing or zero denominators remain empty, not zero or an assumed 100%.
* Phase matching uses existing canonical artifact selection by VP/protocol/mode/
  phase, not physical directories: strategy reports may be under `reports/.../
  figures`, while coverage is under `data/processed`. Canonical discovery/streaming
  aliases and repetitions follow the existing selection rules. Dates and
  measurement IDs remain in JSON provenance, not public population keys. The
  date-heavy `results.<vp>.measurements.*` keys are no longer emitted.
* Interprotocol uses `results.<vp>.interprotocol.<protocol-group>.*` with the same
  four suffixes, combining the disjoint BASE/MASS strategy cohorts of that group.
  Success requires enough replies in **every** participating protocol. Version-2
  outcomes retain every generated target, including zero-reply targets, so the
  source population comes from the upstream `target-summary.json`, not from
  the measured outcome count (100% when the entire nonempty pool was selected).
  Pair cohorts exclude the same-strategy triple. This is not coverage
  of an original ZMap intersection; no such unmeasured denominator is inferred.
* Local controls use `results.<vp>.reference.<protocol>.<base|mass>.*`, with the
  same four suffixes. Their source population is the upstream union of
  participating protocol cohorts for this protocol/sample, read from the target
  summary. These exclusive pair/triple cohorts are disjoint for a given IP, so
  the union size is their sum after filtering BASE/MASS strategies.
  The classified reference table includes one row per frozen target, with missing
  replies classified as NOT_ENOUGH_SAMPLES. Existing v1 prepared reference JSONs
  are translated on read without any table/raw rescan or cache rewrite. If the
  upstream target summary is unavailable, only `targets.pct` remains empty;
  `success.pct` still uses the known selected-target denominator. An upstream
  population must not be invented from the measured target count.
* Pinned full TCP follow-ups retain their separate `.full` suffix and existing
  keys. `targets.pct.full` uses the original SYN-ACK eligible pool (100% for a
  completed full selection), independently checked against its selected targets.
  Historical Connection RT/BASE and FI/BASE select the frozen sample (10% for this
  campaign, subject to the sampler's minimum and integer rounding in other campaigns).
  `success.pct.full` uses full selected targets; the
  older `coverage.pct.full` alias is retained unchanged for compatibility.
* TeX output groups the same metric across `af`, `as`, `au`, `eu`, `na`, `sa`
  directly on consecutive lines, without blank lines between VPs. VP comparison
  min/max/range statistics for the same family and strategy also form one block,
  e.g. `results.vp-comparison.icmp.rt-fi.constant.*`. Blank lines separate different
  comparison families/strategies. Existing key ordering and values are unchanged.
  In addition, each `results.<vp>.interprotocol.<class>.*` family forms one
  uninterrupted block, as do TCP Connection RT Constant, its Per-Connection
  handshake family, TCP no-connection RT/FI Constant, and `fig.<vp>.interprotocol.*`.
  Related historical and `.full` values stay together; different classes/families
  retain their separating blank line. Existing same-metric VP ordering is preserved.
* Results and figure keys follow pipeline order within each protocol (ICMP, TCP,
  UDP): ZMap, RT/BASE, FI/MASS and repeats, FI/BASE, final RT/FI merge, then method
  comparisons. TCP discovery is followed by no-connection phases, Connection
  phases and their cross-mode transition. Local reference families follow the
  protocol families, followed by interprotocol results and VP comparisons.
  This is a semantic reading order; asynchronous jobs need not finish in this order.
  Identical metrics remain consecutive across the six VPs. Keys, numbers and the
  final `.full` suffix do not change; only the order/spacing of TeX lines changes.
* `results.<vp>.<family>.distribution.<strategy>.pct` uses the exact population of
  the existing strategy plot, including UNCLASSIFIED and NOT_ENOUGH_SAMPLES.
  Families are `icmp.rt-fi`, `udp.rt-fi`, `tcp.no-conn.rt-fi` and `tcp.conn.rt-base`.
  `results.vp-comparison.<family>.<strategy>.min.pct`, `.max.pct`, `.range.pp` use
  all six unrounded percentages. If any VP is unavailable, the range is empty.
* `results.eu.tcp.<no-conn.rt-fi|conn.rt-base>.constant.<df0|df1|ipid-zero>.pct`:
  one vote per Constant IP. DF0/DF1 means exclusively that flag among received
  samples used by the classifier. Both percentages divide by **all Constant IPs**;
  mixed samples are not removed or renormalized. Missing DF telemetry is never
  treated as DF0; incomplete telemetry leaves DF percentages unavailable.
  IPID-zero uses the constant value on these same classifier-used positions.
  The stateless merge routes retained RT Constant rows to RT raw and refined RT
  UNCLASSIFIED rows to FI-MASS raw. BASE follows production's first-round skip;
  MASS preserves missing positions. No rankings are exported.
* `results.eu.tcp.conn.rt-base.per-connection.handshake.constant.pct` counts
  PER_CONNECTION labels whose **first four SYN-ACK IPIDs in that same raw run are
  equal**, divided by all PER_CONNECTION IPs in that run. This is deliberately
  separate from `results.<vp>.tcp.rt-no-conn-to-conn.constant.per-connection.column-pct`,
  which joins two separate RT measurements. Neither existing definition nor value
  is replaced. The preparation checks four connections and valid handshake slots.

### Strategy shares aligned with the Phase 2 plot

Text describing the two measurement-type bars needs two explicit denominators:
RT-based shares use sufficiently sampled Phase 1 IPs (`success`, including
Unclassified). The Fixed-Interval bar includes **all selected Phase 2 IPs**,
including Unclassified and Not Enough Samples. Existing `.success.*` keys and
coverage definitions are retained; none is renamed or silently renormalized.

For every VP and family (`icmp`, `udp`, `tcp.no-conn`), ordinary `make paper-values`
additionally exports:

| Suffix after `results.<vp>.<family>` | Definition |
| --- | --- |
| `fi-mass.all.count` | Complete Phase 2 target cohort, including insufficient and missing replies |
| `fi-mass.all.<strategy>.count` | Exact class count in that cohort; missing result rows contribute to `not-enough-samples` |
| `fi-mass.all.<strategy>.pct` | Class count / complete Phase 2 cohort, for all ten classes |
| `fi-mass.all.single-per-destination-per-bucket.pct` | Sum of these three class counts / the same complete cohort |
| `rt-fi.success-all.count` | RT-based sufficiently sampled IPs with their Unclassified rows replaced by all Phase 2 outcomes, counted once |

The internal phase key is retained for compatibility; manuscript prose can use
**RT-based**, **Fixed-Interval**, and **Phase 1/2/3**. For example, use
`results.eu.icmp.fi-mass.all.random.pct` when discussing the Fixed-Interval bar.
The existing `...fi-mass.success.random.pct` continues to describe Random among
IPs with sufficient replies. A 56.18% success-normalized share at about 97.24%
coverage corresponds to about 54.6% of all Phase 2 targets, displayed as 55 in
the integer-labelled bar. The exact EU counts, 2,633,904 Random IPs / 4,820,880
targets, give 54.64%. Calculate from exact counts, not rounded TeX values.

`success-all.count` is **not** `success.count + Phase 2 targets`: the refined IPs
already occurred in Phase 1. It includes failed Phase 2 refinements but excludes
original Phase 1 failures. For current EU ICMP, final `rt-fi.success.count` is
318,972,823 while `rt-fi.success-all.count` is 319,105,777. The original selected
target count remains 375,963,342. Actual combined coverage still uses
`rt-fi.success.count / rt-fi.targets.count`, not the presentation count.
If a Phase 1 strategy report itself retains insufficient rows, those rows are
excluded from `success-all.count`; they are not retroactively called successes.

The exporter reuses the existing small measurement-type report's bar counts,
cross-checking them against canonical strategy reports when available. Legacy
campaigns without that plot report use the same counting rule as the existing
refinement plot: Phase 1 Unclassified defines the cohort, and missing Phase 2
rows are Not Enough Samples. Unbound legacy phase reports that do not establish
this relationship leave the new values empty without blocking old exports.
Conflicting published bar counts, percentages or target metadata fail strict
export and preserve previous outputs. Missing evidence leaves values empty;
a zero cohort has zero counts and undefined percentages.
The combined count requires both the original and merged strategy reports.

Existing merged `distribution.*.pct`, VP min/max/spread, conditional OS/ITDK
metrics and all original success/coverage keys are unchanged. This extension
only reads small reports, works with the existing archive input, and does not
open per-IP Parquet or raw measurement files, prepare new aggregates, alter
plots, or require `--recompute`. Refresh with:

```sh
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"
make paper-figures-zip
```

Figure paths and ZIP selection are unchanged. Switch the manuscript's Phase 2
references to the new `.all.*` keys; the paper repository is not edited by these
commands.

### Full TCP Connection RT/BASE (historical FI/BASE retained)

The old Connection RT sample values and figures remain available under their
existing keys. New RT results always carry the final `.full` suffix. Missing Full
inputs never fall back to the sample. This does **not** replace the historical
FI/BASE sample or request a new measurement, reclassification or plot rendering.

Small, pinned published reports immediately provide:

* `results.<vp>.tcp.conn.rt-base.distribution.<strategy>.pct.full` for all ten
  classes, with the complete strategy-plot denominator (including UNCLASSIFIED/NES).
* `results.vp-comparison.tcp.conn.rt-base.<strategy>.<min.pct|max.pct|range.pp>.full`
  across all six **unrounded** VP shares. Partial VP sets never produce a range.
* `results.<vp>.tcp.conn.rt-base.itdk.strict-agreement.pct.full`: agreement among
  ITDK nodes with at least two concretely classified interfaces, using the existing
  Full consistency summary. This is the existing node-level definition, not an
  IP-level agreement rate or an assumption that every matched node is a router.

Prepare the additional per-IP-derived values once on the analysis VM, using the
usual mounted-storage/RAM environment and paired-reader resource budgets:

```bash
make prepare-paper-full CAMPAIGN_ID=geo-full-20261107-r1
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"
make paper-figures-zip
```

`prepare-paper-full` reuses the pinned registration in `followups.json`, publication
inventories/receipts, original SYN-ACK sample metadata and canonical original SYN/FI
reports. It processes one VP at a time without file spilling. It writes only new
`data/processed/continental/<campaign>/<vp>/paper-review/tcp-full-review-v1.json`
summaries with exact audit counts, provenance and input/code fingerprints. Cache
hits avoid per-IP and raw reads; a failed later VP can be retried without repeating
finished VPs. Existing raw/processed/reports files and the old review summaries
are only read. An updated input refreshes this new cache, not the old sample values.

The prepared keys are:

* EU only: `results.eu.tcp.conn.rt-base.constant.<df0|df1|ipid-zero>.pct.full` and
  `results.eu.tcp.conn.rt-base.per-connection.handshake.constant.pct.full`. The
  existing Constant/DF and four-SYN-ACK preparation is reused with the **Full** raw
  measurement and labels. Unknown/mixed DF remains in the Constant denominator;
  missing DF cannot become zero. This is not the separate-run transition metric.
* Every VP: `results.<vp>.tcp.conn.rt-base.increments.<per-bucket.p99|single.below-2000.pct>.full`
  from all positive strategy-selected increments of the Full run. P99 is the
  nearest-rank discrete quantile; the threshold is strictly `<2000`, excluding zero
  increments in both numerator and denominator.
* Paired plot population, every VP:
  `results.<vp>.tcp.conn.rt-vs-fi-base.<rt-base|fi-base>.increments.<per-bucket.p99|single.below-2000.pct>.full`.
  These reuse the **published paired histogram** for jointly successful new RT /
  historical FI IPs. They retain its original full positive-increment counts,
  not the renormalized p99.9 display counts. When a clipped tail cannot determine
  the exact `<2000` fraction, this explicit preparation reads the already-published
  paired per-IP increments for that mode; the ordinary exporter never does so.
  These are the appropriate keys when describing the
  paired figures; neither the whole Full RT nor historical whole FI is substituted.
* `results.<vp>.tcp.conn.fi-to-rt-success.pct.full` reuses the existing matched-cohort
  calculation with Full RT labels: FI successes / Full RT successes **within the
  frozen FI target list**, including UNCLASSIFIED and excluding NES. This is a
  ratio and can exceed 100%; it is not FI successes / all Full RT successes.
* `results.<vp>.tcp.rt-no-conn-to-conn.constant.per-connection.column-pct.full`
  reuses the existing separate-run transition join: the fraction of Full
  PER_CONNECTION IPs with a sufficiently sampled original SYN-based observation
  that was CONSTANT. It never reads old Connection RT sample labels.

Ordinary `paper-values` only checks fingerprints and reads these small summaries;
it never performs Full preparation implicitly, even with `--recompute`. Missing
preparation yields empty `.full` values, while stale/corrupt evidence is an input
error (`--strict` preserves the previous exports). All prior value keys remain.
Only the incorrect Connection `targets.pct` denominators are corrected. The TeX
writer keeps historical and Full VP min/max/range blocks separate and compact.

### EU interprotocol values and three additional paper figures

One completed version-2 EU campaign summary is selected explicitly by its unique
`eu/interprotocol/*/runs/*/interprotocol-campaign-summary.json` path. Multiple runs
are an ambiguity error; a newer timestamp is not a selection policy.

For Per-Destination, Single and Per-Bucket, public families are
`results.eu.interprotocol.<strategy>.<protocol-group>.*`:

* `targets.count`, `outcome.<outcome>.count` and `.pct` preserve all UpSet outcome
  categories and their original whole-cohort denominators, including insufficient
  samples. `results.eu.interprotocol.<strategy>.set.<protocol>.count` exports the
  set sizes displayed by the plot. Counts refer to cohort IPs, not globally unique
  addresses across groups or strategies.
* `changed.<protocol>-<label>....pct` exports only observed label combinations
  **strictly above 1% of Changed cases** within that baseline/protocol group.
  Identical label combinations are pooled across hidden state-count fields first;
  the threshold uses exact counts before rounding. UNCLASSIFIED remains explicit.
  No additional whole-population percentage is exported for these detail rows.
* `partial.shared-<pair-set>.partitions-<partition-set>.pct` exports every observed
  Partial relationship, divided by all Partial cases in its baseline/group.
  Compatible pair sets are retained together: an ambiguous IP is not counted once
  per candidate pair. There is no additional whole-population detail percentage.
* Reflection, Constant, Multi and Random export `targets.count`, `consistent.count`
  and `consistent.pct` for every protocol group. Consistent means all individual
  protocol classes stayed unchanged, not equality of Constant values/Reflection
  offsets. Both joint Multi outcomes count as class-consistent. The denominator
  includes insufficient replies, as in the original console table.

`make paper-plots` additionally discovers the three EU counter UpSets, from the
existing summary only, and writes sibling `interprotocol-upset-<strategy>__paper.pdf`
files in the original reports figure directory. The originals are untouched.
The common 3.33-inch width, 7pt Linux Libertine and 4pt legend squares are reused.
Existing paper PDFs are preserved on repeated runs. The new figure keys are
`fig.eu.interprotocol.<per-destination|single|per-bucket>.upset`: paper PDFs are
preferred, with original-PDF fallback. They enter paper-figures.zip automatically
with the same reports-relative paths as every other figure key.
