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

`paper_values/metrics.json` is the single definition of the 1,497 existing public
keys: 242 quantities per VP (`af`, `as`, `au`, `eu`, `na`, `sa`) and 45 shared/global
quantities. All six VPs are exported, even when the current text discusses EU.
The short public names, populations and known missing values are preserved.
There is no internal/public key translation or general diagnostic catalog.
`--profile full` has been removed.

Each group shares a source context and unit. Its `prefix` plus each suffix in
`values` is the **actual public key**, not an intermediate name. For example:

```json
{
  "prefix": "stats.{vp}.tcp.no-conn.rt-fi",
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

The resulting key is `stats.eu.tcp.no-conn.rt-fi.itdk-strategy.transit-observed.single.pct`.
Field lookup normalizes case/underscores but rejects ambiguous field names.
Only declared fields/report families are read. Values are never discovered by
flattening every scalar in a report. Scientific readers in `readers.py` handle
the existing ratios, OS-family ranges and exact histogram statistics.

Default read-only inputs:

- `reports/continental/<campaign>`: existing JSON reports and selected PDF metadata;
- `data/processed/continental/<campaign>`: the existing small OS/strategy and
  positive-increment histogram aggregates needed by selected metrics;
- `data/raw/continental/<campaign>`: measurement YAML snapshots only.

The current paper selection obtains its reported values from JSON. The large
repetition CSVs contain per-target observations, not additional selected paper
scalars, and are not scanned. A future CSV metric should name its exact file,
columns and reduction explicitly rather than introducing a generic directory-wide
CSV scan.

Canonical ZMap reports are preferred over streaming copies; repetition runs are
not substituted for baseline measurements. Conflicting copies are reported.
Existing JSON completion receipts are checked against bytes. PDF checks validate
the signature, size and receipt metadata, not a full-file PDF hash. Missing
receipts remain supported for legacy reports. The output contains paper-relative
PDF paths; copy `reports/` without changing its relative structure.

## Derived-value cache

`values.json` caches only derived scalar tables from existing processed inputs:
OS/strategy cells and exact increment-histogram statistics. It does **not** cache
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
filesystem's own caches. `--io-workers 1` selects serial reads; the default is 8
(maximum 32). ZIP reads are serial. Local ZIP timings are not performance promises
for the S3 mounts.

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

Known missing scientific reports remain missing. Resource requirements, public
source-prefix properties and editorial settings are manually maintained paper
information; the exporter does not infer them from VM capacity, private GCP
addresses or analysis-generation timestamps. Their existing unfilled keys are
retained until the paper integration is revised.

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
