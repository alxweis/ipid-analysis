# Additional paper-ready plots

The paper renderer reads the existing small JSON reports. It does not run
classification, open per-IP Parquet files, perform CAIDA joins, reconstruct data
from PDFs, download reference datasets, or start analysis workflow tasks.

Supported input/output pairs, in the same directory:

| Existing report JSON | New output |
| --- | --- |
| `n-rt-b_fi-m_measurement-type-by-strategy.json` | `n-rt-b_fi-m_measurement-type-by-strategy__paper.pdf` |
| `n-rt-b_fi-m_itdk-role-by-strategy.json` | `n-rt-b_fi-m_itdk-role-by-strategy__paper.pdf` |

The first adapter reads `bars[mode].percentages`; the second reads
`plot_percentages` and `plot_role_totals`. Percentages and populations are not
recomputed or renormalized. Unsupported schemas and invalid percentages fail
without changing their sources. Empty ITDK role populations remain empty rows.

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
For explicit inputs, use the module directly; an original PDF path selects its
sibling JSON rather than reading the PDF:

```bash
python -B -m ipid_analysis.paper_ready --workers 2 \
  /path/to/n-rt-b_fi-m_itdk-role-by-strategy.json \
  /path/to/n-rt-b_fi-m_measurement-type-by-strategy.pdf
```

Only the two exact stateless merged report names are supported initially.
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

Font discovery/registration reuses `paper_figures`. A CFF/OpenType regular face
is converted to a temporary TrueType-outline copy once per worker so the PDF
font type and embedded bytes agree. The installed source font is only read.

## Existing files are immutable

Original PDFs, JSONs, aggregates, manifests and their completion receipts are
only read. Existing `__paper.pdf` files are skipped, including legacy or stale
variants; there is deliberately no overwrite/force option. Repeating a completed
batch skips rendering and does not update output timestamps or receipts.
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

A local offline smoke benchmark rendered twelve fixture PDFs in 3.0 seconds
with four processes, compared with 5.7 seconds serially (wall time including
Python startup and inventory). The completed repeat took 0.13 seconds and
changed none of the 48 existing fixture/output/receipt files. The six fixture
directories repeat one approved EU dummy dataset; this is a rendering benchmark,
not a measurement of six VPs or of live-S3 latency.

`make paper-values` continues to export the original figure paths. Selecting the
new variants in the manuscript is a separate action; this command does not edit
LaTeX files or existing path exports.
