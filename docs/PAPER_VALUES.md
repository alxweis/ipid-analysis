# Paper values (read-only export)

Run from the ipid-analysis checkout on the Analysis VM:

```sh
cd /mnt/data01/ipid-analysis
make paper-values CAMPAIGN_ID=geo-full-20261107-r1
```

The **only persistent output** is `/mnt/data01/ipid-analysis/tmp/values.tex`.
Rerunning replaces this file atomically. No worker restart, environment changes,
analysis run or campaign change is required. `ARGS` and lowercase `args` are
supported for this target; for example:

```sh
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict"
```

`--strict` rejects corrupt, conflicting or ambiguous inputs before replacing an
existing values.tex. Missing results alone are allowed: their keys remain empty.
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

It does not write JSON/Markdown sidecars, logs, receipts or analysis artifacts. Atomic
replacement briefly creates a private staging file under `tmp`, then replaces
`values.tex` and removes staging on failure. Existing unrelated files under `tmp`
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

## Keys and missing results

The default `paper` profile is the reviewed current-manuscript selection in
`paper_values/selection.json`: 301 templates expand to 1,631 keys, comprising
266 per-VP quantities for each of eu/na/sa/as/af/au and 35 global quantities.
Manuscript references and source-file hashes document the selection. The main
paper can use `vp-eu`, while the other VPs have independent values. No EU values
are copied into missing VPs. Synthetic validation and algorithm constants are global.

The selection includes current-paper result values and result figure paths.
Background and setup drawings are excluded. Missing constant-zero proportions,
paired TCP classifications, matched RT/FI cohort comparisons, synthetic validation
values and validation figures remain empty until suitable existing reports are
available and their readers are added. This command does not compute missing
analyses. `ARGS="--profile full"` additionally exposes the diagnostic catalog;
it still writes only values.tex and does not run additional analysis.

Counts and percentages retain explicit population definitions. For example, merged
successful counts replace the RT unclassified target subset by successful FI results;
a full-RT denominator is not substituted for a matched RT/FI cohort. Missing
percentages with a zero denominator stay empty even when the count is zero.
Definitions and missing-value reasons accompany each key as LaTeX comments.

## LaTeX and PDF paths

```tex
\input{tmp/values.tex} % adjust only this input path after copying to the paper repo
\getval{coverage.vp-eu.tcp.no-connection.rt-fi-merged.successful.count}
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
Subprocess checks verify the Make target, input immutability, single-output behavior,
repeated execution, strict failure, bytecode suppression and analysis-import isolation.
The optional `IPID_PAPER_VALUES_ARCHIVE=/path/to/eu-results.zip` enables an additional
integration check against the supplied real EU report archive.
