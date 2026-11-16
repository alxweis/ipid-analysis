# Paper values (read-only export)

Run from the ipid-analysis checkout on the Analysis VM:

```sh
cd /mnt/data01/ipid-analysis
make paper-values CAMPAIGN_ID=geo-full-20261107-r1
```

Persistent outputs are limited to `/mnt/data01/ipid-analysis/tmp/values.tex`
and the reusable `/mnt/data01/ipid-analysis/tmp/values.json` cache.
Rerunning replaces these files atomically. No worker restart, environment changes,
analysis run or campaign change is required. `ARGS` and lowercase `args` are
supported for this target; for example:

```sh
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict"
```

`--strict` rejects corrupt, conflicting or ambiguous inputs before replacing an
existing values.tex or values.json. Missing results alone are allowed: their keys remain empty.
Without strict mode, invalid inputs are reported on stdout and affected values
remain empty. An empty value is different from an observed zero.

## Isolation and inputs

The standalone `paper_values` package never imports `ipid_analysis`, invokes
analysis functions, starts subprocesses or imports plotting libraries. Make runs
Python with `-B`, preventing bytecode-cache writes. No new dependencies or changes
to production analysis code are required.

Default input directories are read only:

- `reports/continental/<campaign>`: published JSON reports and PDF metadata;
- `data/processed/continental/<campaign>`: coverage and small existing aggregates;
- `data/raw/continental/<campaign>`: existing configuration snapshots only.

Existing small aggregate Parquet tables are read using PyArrow and YAML snapshots
using PyYAML (already analysis dependencies). Raw probe sequences are never scanned.
Classifier constants are read as restricted AST expressions from the existing source,
without importing or executing it, and used only when recorded versions match.
Reads through existing mounted S3 storage can cause network reads managed by that
mount; the exporter does not manage mounts, caches or S3 uploads.

Apart from values.json, it does not write status sidecars, Markdown, logs, receipts
or analysis artifacts. Atomic replacement briefly creates private staging files
under `tmp`, then replaces each output and removes staging on failure. Existing unrelated files under `tmp`
are retained. Output symlinks, paths resolving into input directories and writes
through S3/FUSE mounts are rejected. Do not run concurrently with changes to the
input directory layout.

To read a downloaded report ZIP without extracting or changing it:

```sh
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--archive /path/to/eu-results.zip --strict"
```

With `--archive`, default filesystem inputs are disabled. Explicit
`--reports-root`, `--processed-root`, `--raw-root` can supplement the archive.
Repeated `--additional-report` accepts compatible existing supplemental reports;
it never generates them. Output remains `tmp/values.tex` in this checkout even
when the working directory or input locations differ. There is no output override.

## Cache, performance and freshness

```sh
# Normal export: reuse unchanged metadata and derived scalar values.
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"

# Bypass every exporter cache entry and refresh computations/read checks.
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --recompute --timings"
```

The cache contains validated report metadata, PDF-signature checks, and derived
scalar values from small OS/strategy aggregates, increment histograms and YAML
snapshots. Aggregate Parquet rows and raw probe sequences are not cached. Cached
values supplement existing JSON values; they never supersede fresh metadata.

Cache contexts include campaign, all input locations, profile and the exporter
code/selection version. Each entry additionally records its VP, measurement/run,
operation and full source path. Entries are reused only while source size,
modification/change timestamps, completion receipt and (for derived values) the
report SHA256 still match. Metadata receipt hashes are checked on every run.
Archive entries also include the member CRC and archive fingerprint. Cache
payloads have integrity checksums; corrupt or incompatible caches are rebuilt.
Changes to code, source data or receipts invalidate the relevant entries.
Unusable cache contexts from older exporter versions are discarded; contexts
for other campaigns/VPs on the current version are preserved. A newly
appearing source is read automatically: missing files/results are not negatively
cached. New campaigns/VPs are never filled using values from another context.

Directory discovery remains fresh on every invocation, so the export can be used
while the campaign continues. Discovery now walks each input tree once, skips
hidden receipt directories, and inventories JSON and PDF together. Unsupported
JSON filenames are filtered before opening. The paper profile avoids unrelated
interprotocol detail reports, unselected PDFs and unused strategy-intersection
Parquet cells. Independent mounted-file reads use eight workers by default;
`--io-workers 1` selects serial reads and values must remain identical. ZIP
members are read serially for compatibility with shared-file implementations.

The expensive work is remote I/O and reading existing aggregate tables; no raw
per-IP join or new scientific analysis is triggered. Most counts/proportions are
already in JSON. OS/strategy cells need their small aggregate table, and exact
positive-increment statistics need the retained full-count histogram. Subsequent
exports reuse those derived scalar values instead of reading Parquet again.

`--timings` prints inventory, JSON, PDF, derivation, write and total wall times,
plus cache hit/miss counters. Output measurements describe this export, not the
measurement/analysis workers. Source identity checks and directory listing still
perform reads on warm runs. Mount-level metadata caching/freshness is controlled
by the existing S3 mount; `--recompute` bypasses the exporter cache, not that mount.
No VM-specific runtime promise can be inferred from local ZIP timings.

Only successful exports persist a refreshed cache. Strict input failures retain
both existing outputs. Unrelated files in tmp are preserved. Run one paper export
at a time; the analysis worker can continue in parallel. Concurrent exports can
replace each other's two final output files (each file replacement is atomic,
the pair is not a filesystem transaction).

## Keys and missing results

The default `paper` profile is the reviewed current-manuscript selection in
`paper_values/selection.json`: 301 templates expand to 1,631 keys, comprising
266 per-VP quantities for each of eu/na/sa/as/af/au and 35 global quantities.
Manuscript references and source-file hashes document the selection. The main
paper can use `vp-eu`, while the other VPs have independent values. No EU values
are copied into missing VPs. Synthetic validation and algorithm constants are global.

The standard export prepares only selected placeholders; it does not allocate the
large optional diagnostic catalog or compute unused cross-VP marginal summaries.
The full profile retains its complete key set. JSON/PDF cache and receipt checks
share one read path. Source validation and per-value source references remain,
without building the unused inventories for the former JSON/Markdown sidecars.

For maintenance, version-2 catalog JSON shares identical subtrees: a property such
as `"single per-bucket"` means the same child fields for each space-separated name.
`report_fields()` expands these into ordinary field names. The manuscript selection
similarly allows `"keys": [...]` to share a definition/unit/reference block; its
loader exposes the same individual entries. All 301 templates, units, reasons and
paper references are preserved. Regression fingerprints cover the complete original
catalog and selection, rather than checking only their sizes.

The selection includes current-paper result values and result figure paths.
Background and setup drawings are excluded. Missing constant-zero proportions,
paired TCP classifications, matched RT/FI cohort comparisons, synthetic validation
values and validation figures remain empty until suitable existing reports are
available and their readers are added. This command does not compute missing
analyses. `ARGS="--profile full"` additionally exposes the diagnostic catalog;
it still writes only the two export files and does not run additional analysis.

Counts and percentages retain explicit population definitions. For example, merged
successful counts replace the RT unclassified target subset by successful FI results;
a full-RT denominator is not substituted for a matched RT/FI cohort. Missing
percentages with a zero denominator stay empty even when the count is zero.
Definitions and missing-value reasons accompany each key as LaTeX comments.

## LaTeX and PDF paths

```tex
\input{tmp/values.tex} % adjust only this input path after copying to the paper repo
\getval{results.vp-eu.tcp.no-connection.rt-base.successful-results.constant.share-pct}
\includegraphics[width=\linewidth]{\getval{plots.vp-eu.tcp.no-connection.rt-fi-merged.operating-system-group-by-strategy}}
```

The file provides standalone `\setval` / expandable `\getval` macros compatible
with the value-store convention. No ndpi installation is required. Percent signs
and text are escaped; PDF filenames are represented with `\detokenize` so that
underscores and spaces work directly with `\includegraphics`.

PDF values begin with `reports/continental/...`, without `./` or absolute VM paths.
Copy the reports tree into the paper repository preserving its relative structure.
Duplicate unpinned plot candidates remain empty rather than selecting an arbitrary
file. A missing plot key expands to empty; include a figure only when it is available.

## Validation

```sh
.venv/bin/python -B -m unittest discover -s tests -p test_paper_values.py
```

Tests cover scientific denominators, missing/zero distinctions, receipts,
conflicting reports, VP separation, paper selection, plot paths and serialization.
Subprocess checks verify the Make target, input immutability, two-output behavior,
repeated execution, strict failure, bytecode suppression and analysis-import isolation.
The optional `IPID_PAPER_VALUES_ARCHIVE=/path/to/eu-results.zip` enables an additional
integration check against the supplied real EU report archive.
