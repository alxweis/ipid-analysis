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
| `c-rt-b_itdk-role-by-strategy.json` | `c-rt-b_itdk-role-by-strategy__paper.pdf` |
| `n-rt-b_fi-m_itdk-role-by-os.json` | `n-rt-b_fi-m_itdk-role-by-os__paper.pdf` |
| `n-rt-b_fi-m_itdk-role-by-strategy.json` plus sibling `n-rt-b_fi-m_strategies.json` | `n-rt-b_fi-m_itdk-role-by-strategy__paper__final.pdf` |
| `n-rt-b_fi-m_itdk-role-by-os.json` plus sibling `n-rt-b_fi-m_operating-system-group-by-strategy.json` | `n-rt-b_fi-m_itdk-role-by-os__paper_final.pdf` |
| `n-rt-b_fi-m_measurement-type-by-strategy-with-connection.json` | `n-rt-b_fi-m_measurement-type-by-strategy-with-connection__paper.pdf` |
| `n-rt-b_fi-m_tcp-flags-by-strategy.json` | `n-rt-b_fi-m_tcp-flags-by-strategy__paper.pdf` |
| `n-rt-b_fi-b_increment-distributions.json` plus its existing histogram | `n-rt-b_fi-b_increment-distributions__paper.pdf` |
| `c-rt-b_fi-b_increment-distributions.json` plus its existing histogram | `c-rt-b_fi-b_increment-distributions__paper.pdf` |
| `n-rt-b_fi-b_probing-intervals-by-continent.json` plus its prepared p99.9/256 paper aggregate | `n-rt-b_fi-b_probing-intervals-by-continent__paper.pdf` |
| `c-rt-b_fi-b_probing-intervals-by-continent.json` plus its prepared p99.9/256 paper aggregate | `c-rt-b_fi-b_probing-intervals-by-continent__paper.pdf` |
| `n-rt-b_fi-b_strategy-intersection.json` plus its existing 49-cell aggregate | `n-rt-b_fi-b_strategy-intersection__paper.pdf` |
| `c-rt-b_fi-b_strategy-intersection.json` plus its existing 49-cell aggregate | `c-rt-b_fi-b_strategy-intersection__paper.pdf` |
| `n-rt-b_fi-m_operating-system-group-by-strategy.json` plus its existing OS/strategy aggregate | `n-rt-b_fi-m_operating-system-group-by-strategy__paper.pdf` |
| `c-rt-b_operating-system-group-by-strategy.json` plus its existing OS/strategy aggregate | `c-rt-b_operating-system-group-by-strategy__paper.pdf` |

The OS-group and strategy-intersection paper heatmaps share display-only decimal
rounding: ties round away from zero, rather than Python's ties-to-even rule.
Rounding starts from each stored numeric value or exact integer IP population,
never from an already abbreviated label. Percentages retain one decimal, except
the rounded 100% endpoint is shown as `100`. Exact-zero cells remain dashes;
small positive cells may still display `0.0`. Heatmap colors and input values are
unchanged.

IP labels below 100k keep tenths of a thousand, from 100k to below one million
they use whole thousands, and millions retain one decimal. Decimal trailing zeros
are removed (`57k`, `35k`, `2M`); a rounded `1000k` is promoted to `1M`.
For example, 156470 displays as `156k`, while an exact 156500 displays as `157k`.
The percentage-axis label pad is 0 pt. The heatmap/colorbar gap is capped at
4 pt, without expanding existing smaller preferences: stateless OS heatmaps
retain 1.44 pt, while the previous 5.04-pt connection OS and 7.2-pt intersection
gaps are reduced to 4 pt. These rules apply to both connection modes and are
included in the paper-output style fingerprint. Existing paper PDFs still skip
rendering and are never silently replaced.

Every cell in both paper heatmap families and connection modes has a fixed
10-pt physical height, independent of font metrics, data or label wrapping.
The seven-row intersection frame is therefore 70 pt high; the shared percentage
bar is also 70 pt high in every variant. Cell widths retain their existing
layout rules. Labels that cannot fit at 7 pt fail clearly instead of enlarging
all rows or overlapping neighboring labels.

The first adapter reads `bars[mode].percentages`; the second reads
`plot_percentages` and `plot_role_totals`. Percentages and populations are not
recomputed or renormalized. Unsupported schemas and invalid percentages fail
without changing their sources. Empty ITDK role populations remain empty rows.

### Final ITDK complement figures

Campaign discovery creates both the legacy paper figures and the two additional
Final figures above for each available VP/protocol. To create only the Final
additions, use:

```sh
make paper-plots CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--final-only --dry-run"
make paper-plots CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--final-only --workers 4"
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"
make paper-figures-zip
```

Explicit original JSON paths retain their legacy paper output unless
`--final-only` is supplied. An explicit Final PDF path also selects its Final
adapter. The two different suffix spellings are intentional.

Router retains the existing exact measured-IP ITDK `T=true` selection, independent
of `D`; there is no alias-node expansion. All Other is the **entire measured
population minus Router**, including both matched non-transit and ITDK-unmatched
IPs. The strategy population is the merged RT-BASE/FI-MASS strategy table,
including its existing Unclassified/Not Enough Samples rows. Insufficient samples
remain folded into Unclassified for display, as before. Each role normalizes
independently. The OS plot uses the measured, resolved OS intersection, not the
whole shared OS scan; unresolved OS IPs are excluded from both bars.

The adapters subtract exact integer Router category counts from the sibling
global category counts. They validate protocol, merged target, ZMap/base/mass
IDs, OS measurement ID, coverage, and subset/count consistency before publication.
Only bounded aggregate JSON reports are read; no new per-IP scan, classification,
ITDK/OS join or DuckDB query is performed. The existing OS group selection and
Other grouping are reused on the expanded population. Missing or incompatible
reports fail explicitly, without reverting to the old matched-only population.
Receipts record both report fingerprints and the resulting role populations.

Final design retains Linux Libertine 7 pt, 3.33-inch width, pastel palettes,
axes/bar geometry and integer segment labels at >=1%. Its compact five-column
legend is centered as one block, preserving vertically aligned column pairs,
with 4-pt square swatches and 1.2-pt label gaps. Column gaps are automatically
limited to 4 pt, like all shared paper bar legends. The OS legend
uses Router/Server instead of Generic router/Generic server.

All source JSONs, original PDFs and existing paper/Final PDFs remain untouched.
Existing outputs skip rendering; this is not an overwrite or refresh command.
`paper-values` prefers Final > paper > original **within the same canonical
figure identity/run**. The existing `fig.*.itdk-role-by-strategy` and
`fig.*.itdk-role-by-os` keys then reference the Final filenames, so the ZIP
includes them automatically with unchanged directory structure. Numerical ITDK
scalar keys retain their existing report definitions; this addition changes
figure populations, not historical scalar cohorts.

The connection-oriented TCP ITDK variant reads the same exact `plot_percentages`
and `plot_role_totals` fields. It is discovered only in TCP
`connection/rt-based-base` directories, independently of the stateless merged
variant. The report must identify TCP, connection mode, RT-based interval and
base scale. It uses the very same ITDK paper renderer, palette, font, geometry,
legend and annotations as the stateless variant; only its source population and
percentages differ. No new renderer, original PDF, Parquet, ITDK join or
measurement input is required. Create-only publication and existing-output
skipping apply unchanged.

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
values use the shared decimal rounding, including small positive shares
displayed as `0.0` and the endpoint displayed as `100`. Missing/invalid aggregates fail without a PDF or
per-IP-data fallback. Its receipt tracks the original JSON and compact table;
the input is bounded to 49 rows and 1 MiB. This variant needs no preparation flag.

The connection-oriented TCP increment and intersection variants reuse these same
loaders and renderers, including their fonts, physical geometry, colors, legends,
annotations and validation limits. They are discovered only in TCP
`connection/comparison/rt-based-base_fixed-interval-base` directories. Each report
must identify the expected figure, TCP, connection mode and the combined
`tcp.ipid.connection.rt-based.base+tcp.ipid.connection.fixed-interval.base` target;
its exact aggregate must belong to the matching comparison. `--processed-root`
retains the same mode and phase restrictions. Neither variant requires a
preparation flag: they read the already stored `c-rt-b_fi-b_...` compact table,
without interval queries, new histograms, strategy joins, source-PDF extraction
or renormalization. Isolated ECDF increments retain their vertical zero-to-100%
jump. Heatmap zeros remain dashes, and small positive shares rounding to `0.0`
remain numeric. Missing/invalid aggregates fail without an analysis fallback.
Existing original and paper artifacts remain immutable.

The OS-group adapter reads only its report and existing compact OS/strategy
aggregate (at most 4 MiB and ten cells per canonical OS group). It validates the
schema, unique/complete cells, taxonomy labels, counts, percentages and report
populations before selecting these groups in this fixed order:

- **General-Purpose OS:** Ubuntu, Debian, RHEL, CentOS, Fedora, FreeBSD, OpenBSD,
  Microsoft Windows.
- **Network OS:** MikroTik, F5, Cisco, SonicWall, ZTE, HPE Network OS, Check Point,
  OPNsense, Huawei, DrayTek, Zyxel, Aruba.

All ten strategy columns retain their original order and row-wise normalization.
There is no ranking, reclassification, per-IP join, PDF extraction or preparation
step. A selected OS absent from the validated complete aggregate remains a
zero-population `(0)` row with dashes; other OS groups are not displayed. Stored
percentages are used unchanged. Exact-zero cells show `-`; positive shares use
the common decimal formatting described above.

The approved 3.33-inch, Linux Libertine 7-pt design has two stacked heatmaps with
headings centered over their own frames, inline parenthesized IP counts using
the common compact k/M formatting, and `Operating-System Group (#IP Addr.)` as y title.
The upper heatmap has ten bottom x ticks without labels. Its frame is separated
from the lower frame by 0.20 inches, with room for the lower section heading.
The 0.05-inch percentage bar is centered vertically on the combined heatmap
frames and has exactly the same height as the seven-row intersection bar,
using shared font measurement rather than platform-specific hardcoded pixels.
The palette, canonical labels, population formatting, colorbar helper and cached
font are reused. Columns are fitted to the available width without reducing text
size. Rows remain fixed at 10 pt; labels that cannot fit fail clearly.
No labels or cells are silently dropped or clipped. Publication and repeats
use the existing create-only, bounded multiprocessing path.

The connection-oriented TCP OS-group variant reuses this adapter and renderer
in `connection/rt-based-base`. Its report must identify TCP, connection mode
and the `tcp.ipid.connection.rt-based.base` target. It keeps the same 20 selected
OS rows and order, but displays seven BASE columns: Reflection, Constant, Single,
Per-Connection, Per-Destination, Per-Bucket and Unclassified. The complete exact
aggregate is validated first; Multi, Random and Not Enough Samples must have
zero counts **and** zero percentages in every source OS group, including groups
not selected for display. Otherwise rendering fails without hiding observations.
The retained percentages are not renormalized, and an absent selected OS remains
a `(0)` row of dashes.

This variant uses a fixed physical cell size of 11.9145 by 10 pt,
the 3.6-by-70-pt percentage bar and a capped 4-pt horizontal bar gap. The heatmap
gap remains 0.20 inches; the y-title/OS-label layout gap remains 2 pt. No reference
PDF or existing paper output is read to determine geometry. Names and populations
remain on one line, with counts from 100,000 to below one million rounded to
whole thousands (for example 162197 -> `162k`). Both modes share the same
population formatter and omit decimal trailing zeros.
The 3.33-inch canvas, Linux Libertine 7 pt, palette, zero dashes, centered headings,
30-degree strategy labels and upper unlabeled ticks are shared. Ordinary numeric
annotations use decimal ties away from zero; the rounded `100.0` endpoint is shown as `100`
so its text fits the approved fixed-width cells. Source values remain exact.
The stateless OS renderer retains its existing sizing algorithm.

### Anycast-excluded probing-interval sensitivity plots

For EU TCP with a connection, explicitly select the registered full-RT follow-up
paired with historical Fixed-Interval successes (not the historical RT sample):

```sh
make paper-anycast-plots CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--census $census --sites eu --protocols tcp --connection-mode connection --tcp-full --dry-run"
make paper-anycast-plots CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--census $census --sites eu --protocols tcp --connection-mode connection --tcp-full --prepare --workers 1"
```

This uses the registered `reports/followups/.../eu/paired/` report and its stored
paired per-IP interval tables. The completion inventory binds the selected report;
missing or mismatching full inputs fail without falling back to the sample. The
additional PDF is `c-rt-b_fi-b_probing-intervals-by-continent__paper__full__anycast.pdf`
next to the full comparison PDF. The existing full PDFs and original reports remain
unchanged, and paper-value/ZIP selection remains unchanged.

Create an additional `__paper__anycast.pdf` beside an existing original
probing-interval report, preserving both original and ordinary paper PDFs.
Only this figure family is supported. The first comparison can be restricted
to EU ICMP, TCP SYN (no connection) and UDP:

```bash
make prepare-anycast-census \
  ARGS="--month 2026-11 --revision 841f1285ff6f563a40f629cfe4338329388f0ac3 --allow-partial"

census="$PWD/references/anycast-census/2026-11/841f1285ff6f563a40f629cfe4338329388f0ac3/dataset.json"
make paper-anycast-plots CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--census $census --sites eu --protocols icmp tcp udp --connection-mode no-connection --dry-run"
make paper-anycast-plots CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--census $census --sites eu --protocols icmp tcp udp --connection-mode no-connection --prepare --workers 3"
```

The pinned example uses **24 available daily snapshots, November 1--24, 2026**,
not a complete November. `--allow-partial` is mandatory for incomplete months;
the manifest lists every included/missing date and preserves `complete_month=false`.
Omit `--revision` only when intentionally selecting the then-current census commit.
All daily downloads within one run are pinned to one Git commit and checked
against their Git blob hashes. Missing dates never silently become unicast data.

The [LACeS recommendations](https://github.com/ut-dacs/anycast-census#data-recommendations)
define high confidence as `max(AB) > 3 OR max(GCD) > 1`, across IPv4 protocols.
Filter each snapshot first, then take the union over the included days. Use
the candidate IPv4 `/24` in `prefix`, **not** the broader `backing_prefix` or
the routed-prefix CSV. For this pinned November subset the union contains
18,720 /24s. Both per-protocol evidence columns and the published AB/GCD
summary schema are supported; IPv6 scores are not mixed into IPv4 detection.

Prefix exclusion occurs before per-continent population counts, population
quartiles and p99.9 quantiles. The query reads the existing processed per-IP
probing-interval tables and stored continent map; it does not send probes,
read raw measurement files, fetch MaxMind or repeat geolocation. Preparation
is sequential and uses the existing bounded DuckDB/RAM-scratch runtime.
PDF workers only read compact prepared aggregates and use the existing
3.33-inch, Linux Libertine 7-pt renderer. This retains the original definition:
**median probing interval per IP**, not pure propagation RTT. Each filtered
continent/mode computes its own p99.9, as in the unfiltered paper variant.

New aggregate/metadata files are stored under an `anycast/<identity>/`
subdirectory beside the comparison aggregates. The identity binds both the
census manifest and scientific aggregation code. Repeated rendering reuses
them; pure renderer/design changes do not invalidate the expensive preparation.
Metadata records the source hashes, included census dates/filter, and exact
original/excluded/retained IP counts for each continent (including unknown
geolocation separately). Existing PDFs are immutable. A different census or
rendering profile cannot silently reuse an existing `__paper__anycast.pdf`;
use `--output-root reports/anycast-comparison/<new-name>` to retain both
comparisons with their original relative hierarchy.

This is a sensitivity filter for **detected anycast prefixes**. A selected /24
can contain unicast and anycast addresses, and absence from the census does
not prove unicast. A monthly union also includes detections outside a particular
scan's exact interval. Report these limits when interpreting changes; exclusion
alone does not establish why a geolocation/interval observation was anomalous.

The census cache is local/generated and excluded from Git. Its manifest retains
source URLs/hashes and the dataset's CC BY-NC-SA 4.0 attribution. Cite the
[LACeS IMC 2025 paper](https://doi.org/10.1145/3730567.3764484) when using the results.
Normal `paper-plots`, figure keys, `values.tex` and `paper-figures.zip` selection
remain unchanged; no anycast variant is automatically substituted into the paper.

The probing-interval violin uses the approved **p99.9 upper limit and 256 quantile
points**. The old p99.5 aggregate cannot recover that tail. An explicit one-off
preparation reads the already stored RT-base/FI-base probing intervals and
continent map, reusing `aggregate_probing_intervals` and the bounded runtime
connection. It does not probe IPs, read raw measurement files, fetch MaxMind, or
repeat continent lookup/classification. The original aggregator's default
p99.5 schema and output remain unchanged.

The connection-oriented TCP violin uses this same preparation and renderer,
with the same p99.9/256 definition and population Q1/median/Q3. It is discovered
only under TCP `connection/comparison/rt-based-base_fixed-interval-base`.
Its original report must identify TCP, connection mode and the combined
`tcp.ipid.connection.rt-based.base+tcp.ipid.connection.fixed-interval.base`
target. Preparation reads that comparison's `c-rt-b_probing-intervals.pq`,
`c-fi-b_probing-intervals.pq` and `c-rt-b_fi-b_ip-continents.pq`, never the
stateless interval tables or a FI-mass comparison. Report fingerprints bind
each prepared aggregate to its own source. The same flag below prepares both
modes, sequentially, without changing existing artifacts. `--processed-root`
remaps both the seven-component interval paths and eight-component comparison
paths while preserving their connection mode and canonical comparison.

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
receipts for both (or the corresponding `c-rt-b_fi-b_...` names for connection
mode). It uses create-only publication, records the original report
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

Only the eleven exact report names above are supported.
The `c-rt-b` ITDK and OS-group plots are discovered under TCP `connection/rt-based-base`.
The other bar plots are discovered under `no-connection/merged/rt-based-base_fixed-interval-mass`;
the increment, intersection and continent violin plots under `no-connection/comparison/rt-based-base_fixed-interval-base`.
The three-bar refinement and TCP-flags variants are selected only for TCP runs,
not under a `connection/` directory or for ICMP/UDP. Other individual-measurement variants and all
other plots retain their existing behavior. New plot types can be registered
with an adapter and a design without changing the original analysis renderers.

## Design

The opt-in `paper_plot_style` profile matches the approved drafts: exact PDF
width 3.33 inches, Linux Libertine at 7 pt, pastel colors (including the lighter
Constant blue and the chosen gray/brown), two-row/five-column legend with
4-by-4-point squares, and rounded integer labels for shares >=1%.
All shared paper bar legends center their entire column block. The column gap
is `min(4 pt, available width after column widths and outer padding / gap count)`:
compact legends retain 4 pt, while the full ten-label strategy legend uses
approximately 2.8 pt depending on font metrics. Both rows retain aligned
column starts. Hidden ITDK Not Enough Samples entries reserve no label width.
The 1.2-pt swatch/label gap, 7-pt text and page width remain fixed; labels that
cannot fit fail rather than shrinking the font or clipping. This shared rule
also applies when the renderer is used for TCP-Connection Full follow-up plots.
Previously generated PDFs still skip rendering and remain unchanged.
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

Intersection cell widths are measured around the rendered `100.0` text box with
5 px horizontal padding on each side at 300 dpi (1.2 pt per side). Heights are
fixed at 10 pt, rather than derived from that font's text box. A temporary width
measurement keeps the same geometry even if the actual matrix has no `100.0`
annotation. This preserves 7 pt text without spreading cells over the entire page. The resulting
compact content is centered on the 3.33-inch canvas, with the long x-title kept
inside its bounds. The y-title moves up to 4 pt farther left into available
margin, while retaining a small clearance from the page edge. Only the design
is changed; source percentages and populations remain unchanged.

The approved TCP three-bar design reuses the same renderer, palette, text size,
4-point legend squares, 8.316-point bar thickness and free inter-bar gaps.
The frame is 0.6325 inches high; its left edge is at 1.01 inches to fit the
two-line `RT-based &` / `Connection-oriented` tick label. `Measurement Type` is
centered on that frame with a 2-point downward offset. The legend keeps the
original five-column/two-row order with the same centered block and automatic
column spacing capped at 4 pt. The gray refinement guide connects only the top RT-based
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

## Global classifier-validation paper figures

These figures are independent of the geographic campaign and are opt-in:

```sh
# Selection only; no cache preparation, calibration or artifact writes.
make paper-plots ARGS="--validation --dry-run"

# First run: prepare missing numerical aggregates in the validation workflow.
make validate-classifier ARGS="--prepare-paper --dry-run"
make validate-classifier ARGS="--prepare-paper --workers 8"

# Render the prepared results with four workers.
make paper-plots ARGS="--validation --workers 4"

# Later runs: render only from persisted aggregates; existing outputs skip.
make paper-plots ARGS="--validation --workers 4"
```

`--validation` can also be combined with `CAMPAIGN_ID=...` to select both families.
`make validate-classifier ARGS="--prepare-paper"` is a dedicated create-only
mode: it returns before the original validation exporter, and Make skips the
legacy Random-CDF exporter in this mode. It uses the populations, seed and
calibration in retained validation metadata; numerical generation overrides are
rejected. Its `--dry-run` inspects the input identity and destination without
computing or writing. `--reports-root` and `--processed-root` are available in
this mode for input/cache remapping. The historical invocation without
`--prepare-paper` retains its original behavior.

`make paper-plots ARGS="--validation --workers 4"` only renders prepared
validation inputs. There is no preparation flag for validation in the renderer.
Missing aggregates produce a clear error pointing to the validation command
rather than silently running expensive calculations. No separate workflow worker is
started. Rendering uses the existing bounded process pool and storage transport.

The new outputs, with matching `__paper.json` sidecars, are:

- `mass-4x25-classifier-confusion-ideal__paper.pdf`
- `mass-4x25-classifier-confusion-lossy-vs-lossy-reordered__paper.pdf`
- `mass-4x25-classifier-confusion-lossy-vs-reordered__paper.pdf`
- `mass-4x25-random-score-cdf-{ideal,lossy,reordered,lossy-reordered}__paper.pdf`
- `nist-baseline/mass-4x25-nist-score-cdf-{ideal,lossy,reordered,lossy-reordered}__paper.pdf`
- `e2e-classifier-confusion-ideal__paper.pdf`

All are under `reports/figures/classifier-validation/`, next to the corresponding
validation figure families. The sidecars record exact confusion counts/supports,
normalization, source fingerprints, numerical model/calibration, thresholds,
seeds and layout. E2E metrics for **all four** conditions are retained in the
E2E sidecar, although only the ideal two-stage view is plotted. CDF sidecars
reference exact score/count Parquet histograms, with per-class support and
threshold summaries. `make paper-values` exports all twelve global figure paths
plus E2E accuracy/routing and score metadata; the existing ZIP command includes
the referenced PDFs automatically. Existing validation keys retain their meaning
and prefer verified paper sidecars, falling back to the original summary when
new sidecars are absent.

### One-time preparation and scientific scope

Required existing inputs are:

- `data/processed/classifier-validation/synthetic-classifier-validation.pq`
- `reports/figures/classifier-validation/out-of-scope-classifier-rejection.json`

The latter supplies the original generator/classifier version, populations,
request pattern and production calibration. Mismatched versions, calibration,
or Parquet populations fail explicitly. Mass counts project only the retained
`DATASET`, `EXPECTED_STRATEGY` and `DETECTED_STRATEGY` columns; sequence columns
and old PDF pixels are not read. Original Mass results are reused exactly.

The existing Mass counts do not contain target-paired RT/FI decisions. Therefore
the additional E2E experiment is prepared once with the existing production
classifiers and generators. Every generating class enters RT; only actual
RT-`UNCLASSIFIED` targets enter FI. Early RT errors remain errors. A shared latent
target trace provides **16 RT observations followed by 100 new FI observations**;
FI does not reuse RT samples. Reflection's request pattern resets at the phase
boundary. Multi's counters are balanced over the complete 116-position trace,
not separately within each phase. RT observations are clean for all eight
classes. FI uses ideal, 20% loss, 20% reordering, or loss plus reordering.
All 20 lost positions are retained in the loss mask; reordering of the remaining
samples follows the existing impairment model. The lower heatmap normalizes
within each actually forwarded true class; overall E2E accuracy uses all original
targets. This is synthetic **classifier** accuracy, without a network RTT,
timeout, scheduler, fresh-connection-identity, middlebox or drift model.

For the CDF comparison, both methods use **identical newly generated Mass
observations, loss masks and reorder permutations**. This creates a paired
comparison rather than assuming older independently generated NIST/Random
exports used identical inputs. Both scoring implementations are reused. The
production RANDOM calibration and adapted short-sequence NIST calibration use
one million null samples; NIST is an adapted baseline, not NIST certification.
The first run may take minutes or longer depending on original population and
CPU/storage speed. No runtime guarantee is implied.

Preparation uses a bounded NumPy thread pool (`--workers`, default 8, maximum
32), capped by the CPU budget and a conservative available-memory estimate.
The four shared null references (80/100 IPIDs and 1280/1600 bits) are built in
bounded blocks and persisted once. Their arrays are read-only and shared by
all numerical jobs. Paired CDF scores and null-table blocks are evaluated in
parallel. E2E runs serially because its row-wise impairment loops are GIL-bound;
classes with no actual RT-Unclassified targets skip FI impairments entirely.
Parent-owned random streams and stable result ordering preserve the
serial observations, null distributions and decisions exactly; worker count
is not part of the numerical cache identity. Cached reruns do not rebuild them.

On an otherwise idle 24-vCPU / 256-GiB VM, for example:

```sh
export IPID_ANALYSIS_THREADS=24
make validate-classifier ARGS="--prepare-paper --workers 16"
make paper-plots ARGS="--validation --workers 8"
make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"
make paper-figures-zip
```

PDF rendering still uses the existing separate process pool. More workers do
not guarantee linear speedup; memory bandwidth and S3 I/O may become limiting.

Prepared data reside in a content-addressed
`data/processed/classifier-validation/paper-ready/<identity>/` directory. Its
manifest binds original source fingerprints, model code and all components.
Style changes do not invalidate numerical aggregates. Normal rendering reads
only that manifest and compact prepared inputs; it never regenerates sequences,
reclassifies original targets or recalibrates tests. Repeating preparation checks
and reuses the persisted components. Original inputs must remain available for
source identity checks. `--reports-root`/`--processed-root` can remap both roots.

### Layout and preservation

All figures use 3.33-inch width and Linux Libertine at 7 pt, shared strategy
colors, percentage colorbar and decimal rounding. Heatmap rows are 10 pt and the
colorbar is 70 pt high. The ideal Mass view has no heading; paired Mass views use
“Lossy Dataset”, “Reordered Dataset” and “Lossy & Reordered Dataset”. E2E retains
the approved RT-based/Fixed-Interval headings, 14.4-pt stage gap and shaded dashed
connector from RT-Unclassified to the FI frame. Its bottom x labels are shared.
Both CDF families use the x-axis label `Randomness Test Score`.
CDF curves are solid, circles have white edges, and the dashed dark-gray
threshold label is above all curves. Circle spacing is 36 pt along the displayed
curve. The Random CDF retains its categorical `<10⁻⁶` panel; only its 100% circles
are offset sideways, never the statistical curves.

All cache, PDF and JSON publications are **create-only** and use existing
completion receipts/native S3 transport. Existing ordinary validation PDFs,
JSONs and Parquet files are never changed or deleted. Existing complete paper
PDF/JSON pairs skip. An incomplete pair or orphan receipt fails and is preserved
for inspection; it is never automatically repaired or overwritten. Publishing a revised design requires an explicit separate workflow; normal
rendering never replaces an existing PDF/JSON pair.
