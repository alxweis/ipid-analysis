# Additional paper-ready plots

The paper renderer reads existing small JSON reports and compact histogram or
quantile Parquet. By default it does not run
classification, open per-IP Parquet files, perform CAIDA joins, reconstruct data
from PDFs, download reference datasets, or start analysis workflow tasks.

Supported input/output pairs, in the same directory:

| Existing report JSON | New output |
| --- | --- |
| `n-rt-b_fi-m_measurement-type-by-strategy.json` | `n-rt-b_fi-m_measurement-type-by-strategy__paper.pdf` |
| `n-rt-b_fi-m_itdk-role-by-strategy.json` | `n-rt-b_fi-m_itdk-role-by-strategy__paper.pdf` |
| `n-rt-b_fi-m_itdk-role-by-os.json` | `n-rt-b_fi-m_itdk-role-by-os__paper.pdf` |
| `n-rt-b_fi-m_measurement-type-by-strategy-with-connection.json` | `n-rt-b_fi-m_measurement-type-by-strategy-with-connection__paper.pdf` |
| `n-rt-b_fi-m_tcp-flags-by-strategy.json` | `n-rt-b_fi-m_tcp-flags-by-strategy__paper.pdf` |
| `n-rt-b_fi-b_increment-distributions.json` plus its existing histogram | `n-rt-b_fi-b_increment-distributions__paper.pdf` |
| `n-rt-b_fi-b_probing-intervals-by-continent.json` plus its prepared p99.9/256 paper aggregate | `n-rt-b_fi-b_probing-intervals-by-continent__paper.pdf` |
| `n-rt-b_fi-b_strategy-intersection.json` plus its existing 49-cell aggregate | `n-rt-b_fi-b_strategy-intersection__paper.pdf` |

The first adapter reads `bars[mode].percentages`; the second reads
`plot_percentages` and `plot_role_totals`. Percentages and populations are not
recomputed or renormalized. Unsupported schemas and invalid percentages fail
without changing their sources. Empty ITDK role populations remain empty rows.

The TCP three-bar adapter also reads `bars["RT-based & Connection-oriented"].percentages`.
Each bar retains its independent normalization from the report. No per-IP data,
Parquet or original PDF is read for this variant. A missing or invalid third bar
fails without replacing any artifact; there is no fallback to the two-bar plot.

The TCP-flags adapter reads the existing `bars[flags].percentages` for
`SYN-ACK/RST`, `SYN-ACK`, and `RST` in that order. Each distribution retains its
existing normalization; the combined bar is not recomputed as an average of the
two subgroups. No ZMap join, per-IP strategy table, Parquet or source PDF is read.
Missing/invalid groups fail without triggering reanalysis.

The ITDK/OS adapter reads `plot_groups`, `plot_percentages` and `plot_counts`.
It preserves the existing selection/order of at most ten groups, including the
already-combined `Other`, and each role's independent normalization. Counts and
percentages are checked for consistency; they are never recomputed for plotting.
Zero-population roles remain empty. Unknown/duplicate groups, inconsistent counts,
missing rows and non-finite percentages fail before publication. No OS scan,
taxonomy remapping, ITDK join, ranking, Parquet or source PDF is needed.
Canonical display names and colors reuse the original OS renderer's helpers;
old JSON colors do not override the current style.

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

The strategy-intersection adapter reads the report's exact compact 49-cell
Parquet via the existing aggregate-path/remapping helper. It validates the
original schema, all seven-by-seven strategy pairs, nonnegative counts,
consistent RT-based row totals, finite percentages and report populations.
Stored percentages are used unchanged, without another join, count query or
renormalization. Empty rows remain zero rows. Exact zeros display `-`; positive
values retain the original one-decimal formatting, including small positive
shares displayed as `0.0`. Missing/invalid aggregates fail without a PDF or
per-IP-data fallback. Its receipt tracks the original JSON and compact table;
the input is bounded to 49 rows and 1 MiB. This variant needs no preparation flag.

The probing-interval violin uses the approved **p99.9 upper limit and 256 quantile
points**. The old p99.5 aggregate cannot recover that tail. An explicit one-off
preparation reads the already stored RT-base/FI-base probing intervals and
continent map, reusing `aggregate_probing_intervals` and the bounded runtime
connection. It does not probe IPs, read raw measurement files, fetch MaxMind, or
repeat continent lookup/classification. The original aggregator's default
p99.5 schema and output remain unchanged.

```bash
# Optional inventory first; dry-run never prepares or writes aggregates.
make paper-plots CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--prepare-probing-intervals --dry-run"

# One-off, potentially slow: query existing interval tables sequentially,
# publish separate compact aggregates, then render with four workers.
make paper-plots CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--prepare-probing-intervals --workers 4"

# Subsequent runs read only reports and compact aggregates.
make paper-plots CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--workers 4"
```

The preparation writes `n-rt-b_fi-b_probing-intervals-by-continent__paper.pq` and
`...__paper.json` beside the **processed** original aggregate, with completion
receipts for both. It uses create-only publication, records the original report
and interval/map inputs, checks for source changes, and reuses only a matching
complete preparation. Conflicting/orphan/stale files are never overwritten or
repaired automatically. Existing paper PDFs skip preparation as well as rendering.
The normal command fails that violin job with a preparation hint when p99.9 data
is missing; it never silently uses p99.5 or extrapolates the original PDF.

The prepared table contains at most fourteen continent/mode rows. Its 256
`QUANTILES_MS` points span probabilities 0 through 0.999. `Q1_MS`, `MEDIAN_MS` and
`Q3_MS` are separately calculated approximate population quantiles at .25, .5
and .75 from the per-IP interval medians, rather than inferred from the KDE or
local dummy contours. Original union populations and mode-specific counts are
retained; non-finite/negative medians and unmapped IPs are excluded. The reader
validates schema, populations, monotonicity, finite values and p99.9/256 metadata
before rendering, with a 1 MiB limit per compact input. Its PDF receipt tracks
the original report, new quantile table and preparation metadata.

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

Only the eight exact report names above are supported.
The bar plots are discovered under `no-connection/merged/rt-based-base_fixed-interval-mass`;
the increment, intersection and continent violin plots under `no-connection/comparison/rt-based-base_fixed-interval-base`.
The three-bar refinement and TCP-flags variants are selected only for TCP runs,
not under a `connection/` directory or for ICMP/UDP. Individual-measurement variants and all
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

The `Device Type` title is centered on the frame. In the two-bar variant,
`Measurement Type` is centered on the combined frame/x-label height. Outer padding
targets approximately one pixel in a 300-dpi preview; PDFs themselves have
physical units, not pixels.
Very narrow >=1% segments keep the agreed font size and integer label.

The approved continent violin design uses a 0.88-inch-high frame and 7 pt labels,
including single-line `N. America` / `S. America` with IP counts below. RT-based
is the left half in the approved pastel blue; Fixed-Interval is the right half
in pastel red. Width-normalized halves reuse Matplotlib's original 0.82-width
violin calculation and default Scott bandwidth. Q1/Q3 lines are dashed and the
median is solid, bounded by the corresponding half-violin contour. At nearly
constant intervals these lines can coincide visually. Major ticks adapt to the
data range, with exactly one minor tick between adjacent majors. No new tail
taper is fabricated to hide a nonzero density at the p99.9 cutoff.

The framed white legend keeps 4 pt square swatches and a 1.2 pt swatch-to-label
gap, independent of font-size units. It checks nine standard
positions plus fine horizontal positions along the upper edge against actual
violin contours with a small clearance. A nonintersecting position is preferred;
when none fits, sampled covered area is minimized. Thus its location can differ
across VPs and protocols. Font size, width and frame height remain fixed.

The approved intersection heatmap reuses the original strategy order,
`PERCENTAGE_CMAP`, percentage-colorbar helper and shared cached 7 pt font. The
0.05-inch-wide colorbar retains `Percentage [%]` and its 0-100 scale. Tick labels
are rotated 30 degrees. Axis titles use the full original wording: one-line
`Detected IP-ID Selection Strategy using Fixed-Interval probing` and three-line
`Detected IP-ID Selection` / `Strategy using RT-based` / `probing`.

All heatmap cells are measured around the rendered `100.0` text box with 5 px
padding on each side at 300 dpi (1.2 pt per side). A temporary text measurement
keeps that geometry even if the actual matrix has no `100.0` annotation. This
preserves 7 pt text without spreading cells over the entire page. The resulting
compact content is centered on the 3.33-inch canvas, with the long x-title kept
inside its bounds. The y-title moves up to 4 pt farther left into available
margin, while retaining a small clearance from the page edge. Only the design
is changed; source percentages and populations remain unchanged.

The approved TCP three-bar design reuses the same renderer, palette, text size,
4-point legend squares, 8.316-point bar thickness and free inter-bar gaps.
The frame is 0.6325 inches high; its left edge is at 1.01 inches to fit the
two-line `RT-based &` / `Connection-oriented` tick label. `Measurement Type` is
centered on that frame with a 2-point downward offset. The legend keeps the
original five-column/two-row distribution; the compact centered experiment is
not applied. The gray refinement guide connects only the top RT-based
Unclassified segment to the Fixed-Interval bar, never to the independent third bar.

The approved TCP-flags design uses the same three-bar frame, spacing and legend.
Its left edge is at 0.80 inches; the `TCP Flags` title is centered vertically on
the frame. The three rows have no refinement guide between them. Percentage
labels keep the shared >=1% threshold and integer rounding, including narrow
one-percent segments. It reuses the bar renderer rather than introducing a
second plotting or publication pipeline.

The approved ITDK/OS design reuses the same two-bar renderer: frame left at
0.59 inches, frame height 0.4015 inches, `Router` / `All Other`, and `Device Type`
centered on the frame. Its x-label is `OS Distribution [%]`; no refinement guide
is drawn. The source OS-group order also controls the legend and stacked segments.
The full ten-entry legend uses the approved five-column/two-row layout and
4-point squares; shorter selections use fewer entries without invented categories.

The centrally defined `PAPER_OS_COLOR_PALETTE` in `plot_os_group_strategy` now uses
the approved 20 pastel colors and matching companion shades. The stable group
order and 20-color cycling for all 94 OS groups are unchanged. `Other` uses
`#D0C8C4`. The new variant and future original OS renders use the same definitions;
the shared hues do not imply any OS-to-IP-ID-strategy association. This does not
rewrite existing original PDFs, reports, caches or paper variants. The paper
completion receipt includes the OS adapter and palette source in its style hash.

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
The explicit probing preparation is the sole opt-in step here that reads large
processed interval tables. It creates separate paper aggregates and does not
mutate any source artifact. Preparation time depends on the existing tables,
mounted-S3 throughput and available runtime resources; the small rendering
benchmarks below do not predict the preparation time of a full campaign.
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

A mixed four-type fixture run skipped eighteen existing paper PDFs and added
six three-bar TCP PDFs in 2.6 seconds; its completed repeat took 0.13 seconds.
All existing bytes/timestamps remained unchanged. The six inputs repeat one
existing EU/TCP report, so this is a local rendering check, not six-VP results.

A mixed five-type fixture run skipped twenty-four existing paper PDFs and added
six TCP-flags PDFs in 2.7 seconds; the completed repeat took 0.14 seconds. Existing
bytes/timestamps were unchanged, and fresh renders of the four prior plot types
were pixel-identical to their baseline previews. The six flags reports repeat
one EU/TCP input; these are offline checks, not live-S3 or six-VP measurements.

A mixed six-type run preserved/skipped thirty existing paper PDFs and added
eighteen ITDK/OS PDFs in 4.0 seconds with four workers; the completed repeat took
0.15 seconds. Every existing file's bytes and timestamp remained unchanged.
Fresh renders of all five prior paper types were pixel-identical to their
baseline 300-dpi previews. Each fixture site repeats the same three existing
EU/ICMP,TCP,UDP reports; these timings are not live-S3 or six-VP results.

A probing-interval smoke run rendered eighteen prepared synthetic p99.9/256
violin reports in 5.4 seconds with four workers; its completed repeat took
0.12 seconds. Every existing fixture/input timestamp and byte remained unchanged.
All six previous paper types rendered pixel-identically to their baseline
previews. The tiny synthetic interval tables were prepared separately; these
figures do not estimate full-campaign aggregation or live-S3 performance.

An intersection smoke run rendered eighteen synthetic 49-cell aggregates in
3.9 seconds with four workers; the repeat took 0.11 seconds. Existing input and
output bytes/timestamps were preserved. Fresh renders of all seven prior paper
types were pixel-identical to their 300-dpi baselines. These are local fixture
timings, not live-S3 performance or scientific results for eighteen VP/protocols.

`make paper-values` prefers an available sibling `__paper.pdf` for each selected
figure and falls back to its original PDF otherwise. The existing `fig.*` keys
stay unchanged. Rerun the export after generating paper plots to refresh the
paths; no `--recompute` is needed. Neither command edits manuscript files.
