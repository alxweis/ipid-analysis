# Additional paper-ready plots

The paper renderer reads existing small JSON reports and, for increment ECDFs,
their compact histogram Parquet. It does not run
classification, open per-IP Parquet files, perform CAIDA joins, reconstruct data
from PDFs, download reference datasets, or start analysis workflow tasks.

Supported input/output pairs, in the same directory:

| Existing report JSON | New output |
| --- | --- |
| `n-rt-b_fi-m_measurement-type-by-strategy.json` | `n-rt-b_fi-m_measurement-type-by-strategy__paper.pdf` |
| `n-rt-b_fi-m_itdk-role-by-strategy.json` | `n-rt-b_fi-m_itdk-role-by-strategy__paper.pdf` |
| `n-rt-b_fi-b_increment-distributions.json` plus its existing histogram | `n-rt-b_fi-b_increment-distributions__paper.pdf` |

The first adapter reads `bars[mode].percentages`; the second reads
`plot_percentages` and `plot_role_totals`. Percentages and populations are not
recomputed or renormalized. Unsupported schemas and invalid percentages fail
without changing their sources. Empty ITDK role populations remain empty rows.

The increment adapter reads the exact compact `.pq` histogram named by the
report's `aggregate` field, not the per-IP `n-rt-b_increments.pq` or
`n-fi-b_increments.pq`. It checks the bounded footer/schema, series summaries,
counts and stored cumulative percentages. Sorting bins and prepending a zero
baseline preserve the original step ECDF; no histogram, percentile, clipping,
or normalized distribution is recomputed. The existing p99.9 clipping and
retained-sample normalization remain unchanged. Columnar arrays avoid allocating
a dictionary for every histogram bin. Inputs are limited to 128 MiB and
8 * 65,535 rows, covering every positive 16-bit increment in each of eight series.
Missing/inconsistent aggregates fail without starting a measurement-data fallback.

## Run on the Analysis VM

Use the existing deployment and RAM/mounted-S3 environment. The normal worker
need not be restarted or configured to discover new measurement tasks.

```bash
cd /mnt/data01/ipid-analysis
source .venv/bin/activate
set -a
source /etc/default/ipid-geographic-analysis
set +a

# Inventory only; no output, scratch, lock or cache writes.
make paper-plots CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--dry-run"

# Only the requested campaign's supported plots.
make paper-plots CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--workers 4"

# Limit scanning before opening unselected VPs/protocol branches.
make paper-plots CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--sites eu na --protocols icmp tcp --workers 4"
```

For another mounted reports root, add `--reports-root /path/to/reports`.
When using an offline copy of a campaign, `--processed-root /path/to/processed`
remaps the histogram's recorded `continental/<campaign>/<site>/...` path to
that root. Without this option, the report's original absolute path is used.
For explicit inputs, use the module directly; an original PDF path selects its
sibling JSON rather than reading the PDF:

```bash
python -B -m ipid_analysis.paper_ready --workers 2 \
  /path/to/n-rt-b_fi-m_itdk-role-by-strategy.json \
  /path/to/n-rt-b_fi-m_measurement-type-by-strategy.pdf
```

Only the three exact stateless report names above are supported.
The two bar plots are discovered under `no-connection/merged/rt-based-base_fixed-interval-mass`;
the increment plot under `no-connection/comparison/rt-based-base_fixed-interval-base`.
Connection-oriented three-bar plots, individual-measurement variants and all
other plots retain their existing behavior. New plot types can be registered
with an adapter and a design without changing the original analysis renderers.

## Design

The opt-in `paper_plot_style` profile matches the approved drafts: exact PDF
width 3.33 inches, Linux Libertine at 7 pt, pastel colors (including the lighter
Constant blue and the chosen gray/brown), two-row/five-column legend with
4-by-4-point squares, and rounded integer labels for shares >=1%.
Python's nearest-even rounding at exact halves is retained. The free gap between
bars is 8.316 pt and each outer frame clearance is 1.980 pt. The refinement guide
retains its chosen light gray. The ITDK labels are `Router`, `All Other`, and
`Device Type`; these are display aliases for the original report's two roles.
The original role definitions, populations and totals in its JSON remain intact.

The `Device Type` title is centered on the frame. `Measurement Type` is centered
on the combined frame/x-label height. Outer padding targets approximately one
pixel in a 300-dpi preview; PDFs themselves have physical units, not pixels.
Very narrow >=1% segments keep the agreed font size and integer label.

The approved increment design shares the same width, font, pastel colors and
4-point strategy swatches. Its three-column/two-row legend is a compact centered
block with 10-point RT/FI line samples. The logarithmic x-axis uses `10⁰`, `10¹`,
etc.; the lower boundary is 0.85, showing exactly one minor tick (0.9) before 1.
The upper boundary follows the data with the original automatic logarithmic
padding, expanded only when needed to keep its last decade label inside the page.
The y-axis stays at 0–103%, in a reduced 0.80-inch-high frame. Curve
styles reuse the original RT/FI line styles; an isolated increment retains its
vertical zero-to-100% jump. Missing series are omitted and a genuinely empty
histogram remains an empty frame, rather than inventing a distribution.

Font discovery/registration reuses `paper_figures`. A CFF/OpenType regular face
is converted to a temporary TrueType-outline copy once per worker so the PDF
font type and embedded bytes agree. The installed source font is only read.

## Existing files are immutable

Original PDFs, JSONs, aggregates, manifests and their completion receipts are
only read. Existing `__paper.pdf` files are skipped, including legacy or stale
variants; there is deliberately no overwrite/force option. Repeating a completed
batch skips rendering and does not update output timestamps or receipts.
For increment figures, the new receipt tracks both the report and histogram;
both inputs are checked for changes across rendering.
If a previous receipt exists without its PDF, that job fails rather than
rewriting the receipt or guessing a recovery action.

Rendering uses private runtime scratch and per-worker Matplotlib caches (RAM in
mounted-S3 mode). `artifacts.publish_new_file` reuses the existing runtime,
fingerprints, locks, provenance and native publication machinery. Local outputs
are committed with atomic no-replace hard links; mounted-S3 uploads use the
discovered rclone remote with `copyto --immutable`. The only persistent additions
are each new PDF and its new `.ipid-complete/<pdf-name>.json` receipt.
An incomplete publication never deletes or repairs pre-existing artifacts.

## Throughput

The existing `paper_values.inputs.Inputs` inventory prunes unrelated phases and
supports VP selection before listing their directories. Worker processes are
bounded by the requested count and existing CPU budget. Native library threads
are limited to one per worker; fonts and imports are reused across that worker's
plots. No rendering threads or new workflow scheduler are introduced.

Start with four workers and compare with eight on the VM if needed. The command
reports per-plot and total timings. S3 directory/metadata latency and cold font
initialization can dominate these small jobs; the VM's 256 GB RAM is not needed
to reload measurement tables. Live-S3 performance is environment-dependent.

A local offline smoke benchmark of the two bar types rendered twelve fixture PDFs in 3.0 seconds
with four processes, compared with 5.7 seconds serially (wall time including
Python startup and inventory). The completed repeat took 0.13 seconds and
changed none of the 48 existing fixture/output/receipt files. The six fixture
directories repeat one approved EU dummy dataset; this is a rendering benchmark,
not a measurement of six VPs or of live-S3 latency.

An increment smoke run skipped twelve existing bar PDFs and added six synthetic
histogram PDFs in 2.5 seconds; its completed repeat took 0.18 seconds, preserving
all existing bytes and timestamps. A dense fixture with all 524,280 possible
series/bin cells (3.7 MB compressed) rendered and published locally in 1.4 seconds.
These are offline fixture timings, not live-S3 or scientific measurement results.

`make paper-values` continues to export the original figure paths. Selecting the
new variants in the manuscript is a separate action; this command does not edit
LaTeX files or existing path exports.
