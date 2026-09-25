# ipid-analysis

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

A short description of the project.

## Paper plot font

All figures use the actual Linux Libertine OpenType family, including its
bold/italic variants and Matplotlib math text. Install it on Debian/Ubuntu
before running the analysis:

```bash
sudo apt install fonts-linuxlibertine
```

The renderer loads `LinLibertine_*.otf` or `LinLibertine_*.ttf` files directly
and fails with a clear message instead of silently substituting another family.
For a non-standard installation, set `IPID_LINUX_LIBERTINE_DIR` to the
directory containing `LinLibertine_R.otf`/`.ttf` and the other variants.

## S3 measurement handoff worker

The analysis VM can process stateless ICMP, TCP, and UDP-DNS RT measurements
produced by `ipid-measure` without any direct network connection between the
VMs. Both sides use the same S3 prefix and locally configured `s3cmd`
credentials:

```bash
export IPID_ANALYSIS_S3_PREFIX=s3://bucket/ipid-analysis-workflow/
make workflow-worker
```

For every `jobs/<measurement-id>/request.json`, the worker:

1. downloads `ipid.pq` and `ipid.snapshot.yaml` from the completed measurement upload,
2. runs the normal IPID selection-strategy classifier,
3. persists `strategies.pq` beside the RT measurement's `ipid.pq`,
4. writes `zmap_unclassified.pq` with the ZMap-compatible columns `IP_ADDR` and
   `REPLY_TYPE`, containing only `UNCLASSIFIED` addresses,
5. uploads that parquet beside the RT measurement's `ipid.pq`, and then
   publishes `jobs/<measurement-id>/done.json` with its canonical URI, row
   count, size, and SHA-256 digest.

If processing fails, `failed.json` is uploaded instead. Requests are idempotent:
jobs with either terminal marker are skipped. Use `--once` to process the current
queue and exit, which is useful for cron; the default process polls continuously.

The corresponding `ipid-measure` run waits for the terminal marker and will not
start that protocol's 25-request fixed-interval measurement until the result has
been downloaded and verified.

The same worker also starts the complete postprocessing automatically after a
successful `make run-all-*` sweep. It polls
`analysis-jobs/<zmap-id>/request.json`, downloads the manifest and its referenced
ZMap, OS, and IP-ID outputs into `data/raw`, then runs the same processing as
`make analyse <manifest>`. The persistent job files and log are kept under
`data/analysis-jobs/<zmap-id>/`; S3 receives `postprocess.log` followed by either
`done.json` or `failed.json`. A single worker processes the three protocol VMs'
jobs sequentially, so no additional scheduler or locking service is required.

For campaigns declaring `fixed_base_target_uri`, the worker also downloads
`zmap-fixed-base-sample.pq` and its JSON metadata from the ZMap measurement
prefix. Stateless fixed-base coverage uses this sample for ICMP, TCP, and
UDP-DNS. Historical campaigns without a fixed-base sample retain the full
ZMap denominator. RT Base and FI Mass keep their existing target populations.
Deploy this worker version before enabling ICMP/UDP-DNS fixed-base sampling
on the measurement VMs.

New TCP connection runs
declare `connection_target: zmap-connection-sample.pq` in the protocol manifest;
the worker downloads that SYN-ACK sample and its JSON metadata, and both
connection variants use it as their coverage denominator. A declared but missing
sample fails instead of silently using the full ZMap population. Historical
manifests without this declaration retain their previous coverage behavior.
Deploy this support before enabling SYN-ACK sampling on the measurement VMs.

## Strategy classification by measurement scale

Base measurements classify the position-dependent or cheaply identifiable
strategies `REFLECTION`, `CONSTANT`, `PER_DESTINATION`, `PER_CONNECTION`,
`SINGLE`, and `PER_BUCKET`. All other base sequences are `UNCLASSIFIED` and can
be passed to a mass measurement.

Complete mass-measurement sequences first use the same exact rules and priority
as base measurements: `REFLECTION`, `CONSTANT`, `PER_DESTINATION`,
`PER_CONNECTION`, `SINGLE`, and `PER_BUCKET`. The established
position-independent `CONSTANT` and `MULTI` rules then handle residual rows,
including rows with missing replies. Only the remaining sequences are tested
for `RANDOM` using the calibrated RANDOM-compatibility structure score described
below; all other sequences remain `UNCLASSIFIED`.

The exact rules require all 100 positions. Missing-reply markers are never
removed to create a shorter artificial sequence, so this does not introduce
loss-tolerant variants of the base rules. The 4 x 25 position layout and
missing-reply markers are retained for the RANDOM score's full, destination,
and connection views. Minimum reply-rate filtering is performed by
`ipid-measure` before fixed-interval rows are written, so analysis does not
duplicate that measurement-stage decision as an IPID strategy.

The classification produced by the measurement handoff is the authoritative
historical result. `make analyse data.json` first reuses an existing processed
`strategies.pq`, otherwise imports the persisted raw-measurement
`strategies.pq`, and only runs the classifier when neither exists. To
deliberately recompute all measurement classifications with the current code,
run:

```bash
make analyse data.json ARGS="--reclassify"
```

Reclassification does not replace the persisted historical input. Its result
may differ from the classification that selected the original fixed-interval
mass target when classifier rules have changed in the meantime.

To inspect why sampled mass-measurement rows retained a particular label, join
their processed strategy with the raw IP-ID sequence and step through ten
interactive plots:

```bash
make inspect-sequences ARGS="icmp.ipid.no-connection.fixed-interval.mass \
  --manifest data/analysis-jobs/<job-id>/manifest.json \
  --strategy UNCLASSIFIED --samples 10 --seed 42"
```

Each window shows the complete measurement order, both destination and all four
connection subsequences, missing replies, and the production RANDOM-score components.
Closing the window (or pressing Right/Space) advances to the next sample; Left
returns to the previous one and Q/Escape exits. On a headless SSH session, add
`--save-dir reports/sequence-inspection/<job-id>` to write the ten figures as
PNGs instead.

### Synthetic classifier validation

Generate reproducible, measurement-shaped IP-ID sequences and evaluate the
classifier independently of a campaign:

```bash
make validate-classifier
# Optional:
make validate-classifier ARGS="--samples-per-strategy 100000 --seed 42"
```

The paper figures use the selected validation-only RANDOM candidate by default:
the minimum of raw-IPID
uniformity, increment uniformity, and circular gap uniformity. The calibrated
threshold and empirical-null specification are recorded in each JSON sidecar.
This replaces only the final RANDOM decision inside the synthetic validation;
the production classifier remains unchanged. To reproduce the confusion
matrices with the current production RANDOM score instead, run:

```bash
python -m ipid_analysis.classifier_validation --production-random-score
```

By default, 100,000 sequences are generated per evaluated strategy.
`REFLECTION` and `CONSTANT` are deterministic, trivial cases and are fixed at
1,000 sequences each. `--samples-per-strategy` therefore controls the
nontrivial strategies.

Every synthetic generator samples uniformly across its complete valid
classifier domain: 16-bit starts and values use `0..65535`, `SINGLE` and
`PER_BUCKET` increments use `1..21845`, and `MULTI` uses a uniformly selected
cluster count from 2 through 16 with randomized cluster positions and offsets
through the inclusive 800 threshold. All cumulative counters wrap naturally
modulo 2^16. The validation generates only values within the strategy
definitions; it does not inject invalid boundary cases. The exact generator
parameters are also recorded in every validation JSON sidecar.

The Base validation uses the real 4 x 4 round/connection interleaving and
evaluates `REFLECTION`, `CONSTANT`, `SINGLE`, `PER_CONNECTION`,
`PER_DESTINATION`, and `PER_BUCKET`. Base measurements retain only complete
16-reply results, so no Base loss case is generated. Two paper alternatives
compare the same ideal matrix against exactly three reordered positions
(18.75%) and exactly four reordered positions (25%). `MULTI` and `RANDOM` are
evaluated separately as out-of-scope Base inputs and must remain
`UNCLASSIFIED`.

The Mass validation uses 4 x 25 sequences and evaluates all eight generating
strategies under four conditions: ideal, exactly 20 missing replies, exactly 20
reordered positions, and 20 missing replies plus 16 reordered positions among
the remaining 80. Under impairment, exact-label recovery and the
safety-critical structured-as-RANDOM behavior can therefore be inspected
separately. Out-of-scope rejection rates and detected-output counts are written to
`reports/figures/classifier-validation/out-of-scope-classifier-rejection.json`
and are not mixed into the supported-strategy accuracy, precision, recall, or
F1 scores.

The raw-format synthetic sequences and detected labels are written to
`data/processed/classifier-validation/synthetic-classifier-validation.pq`.
Three confusion-matrix PDFs and their metric JSON sidecars are written below
`reports/figures/classifier-validation/`. The JSON reports include accuracy,
balanced accuracy, per-class precision/recall/F1, macro and weighted averages,
Cohen's kappa, and multiclass Matthews correlation coefficient.

```text
base-4x4-classifier-confusion-reordered-3.pdf
base-4x4-classifier-confusion-reordered-4.pdf
mass-4x25-classifier-confusion.pdf
```

The four Mass datasets also evaluate the selected RANDOM-compatibility score:

```bash
make plot-mass-random-score-cdf
# Optional:
make plot-mass-random-score-cdf ARGS="--samples-per-strategy 100000 --seed 42"
```

The candidate score is
`S = min(raw_uniformity, increment_uniformity, gap_uniformity)`. Raw uniformity
uses the established 16-bin Pearson test. Increment uniformity is the empirical,
adaptive-bin test over the full, two destination, and four connection views; it
uses only originally adjacent present positions and is order-dependent. Gap uniformity
compares the complete circular spacing distribution of the present, sorted
16-bit IP-ID values against the same versioned discrete empirical RANDOM null;
it is order-independent. The fixed selected threshold is
`tau = 8.999991223392587e-06`, calibrated at a target 0.01% RANDOM
false-rejection rate on the independent held-out v2 evaluation. A sequence is
RANDOM-compatible when `S >= tau`.

By default, the plotted nontrivial strategies use 100,000 sequences;
`REFLECTION` and `CONSTANT` remain fixed at 1,000. The exact complete-sequence
rules and the established `CONSTANT` and `MULTI` fallbacks keep precedence over
the candidate score. This validation pipeline does not change the production
classifier. The PDFs and JSON metadata are written below
`reports/figures/classifier-validation/`; the underlying scores and decisions
are stored in
`data/processed/classifier-validation/mass-4x25-random-score-cdf.pq`. The four
PDFs are:

```text
mass-4x25-random-score-cdf-ideal.pdf
mass-4x25-random-score-cdf-lossy.pdf
mass-4x25-random-score-cdf-reordered.pdf
mass-4x25-random-score-cdf-lossy-reordered.pdf
```

The
logarithmic x-axis reserves one decade of space below the smallest score,
labels every second power of ten as a major tick, and uses the intervening
powers as minor ticks. Strategies whose complete CDF coincides at the numerical
score floor are additionally marked in their strategy colors on that shared
vertical line.

### RANDOM metric-selection experiment

Evaluate the four production score components together with candidate
increment-uniformity and circular gap-uniformity metrics without changing the
production classifier:

```bash
# Fast pipeline/plot smoke test (not statistically conclusive)
make evaluate-random-classifier ARGS="--samples-per-strategy 1000 --calibration-samples-per-condition 2000 --null-table-samples 5000 --batch-size 1000 --seed 42"

# Full comparison run
make evaluate-random-classifier ARGS="--samples-per-strategy 100000 --calibration-samples-per-condition 250000 --null-table-samples 500000 --target-random-frr 0.0001 --batch-size 10000 --seed 42"
```

The increment test preserves the original measurement positions: an increment
is formed only when both originally adjacent positions in the selected full,
destination, or connection view are present. It never bridges a missing reply.
The number of equal-width bins is selected from the observed transition count:
the largest power of two that retains at least five expected transitions per
bin, capped at 16. Views with fewer than ten transitions are uninformative.
Pearson statistics are converted to empirical multinomial null probabilities,
so short and long views produce comparable compatibility scores.

The gap test sorts the present 16-bit values, includes duplicate zero-gaps and
the circular wraparound gap, and calculates a Cramer-von-Mises discrepancy for
the complete spacing distribution. Its empirical discrete-uniform null table
is conditioned on the number of present values, making the statistic invariant
to packet order and calibrated for each tested loss level.

The experiment covers ideal data; random 5%, 10%, and 20% loss; 20% loss plus
20% reordering of present values; and 20% loss concentrated in one destination
or one connection. In addition to the eight standard strategy generators, it
includes constant-step SINGLE counters at low, medium, and high rates, a
jittered high-rate SINGLE counter, and non-clustered interleavings of two, four,
or eight independent counters. It calculates each metric once, calibrates all
63 non-empty metric subsets as complete minimum-score decision rules, evaluates
False-RANDOM and true-RANDOM false rejection, benchmarks standalone metric
cost, and reports the non-dominated subsets. Calibration and held-out test
generators use independent deterministic random streams.

Compact reports are written to
`data/processed/classifier-validation/random-classifier-evaluation/`:

```text
summary.json
subset-results.csv
subset-by-scenario.csv
pareto-frontier.csv
recommendations.txt
run.log
random-classifier-review-bundle.zip
metric-scores.pq                 # large, detailed per-sequence scores
```

Plots are written to
`reports/figures/classifier-validation/random-classifier-evaluation/`:

```text
metric-false-random-heatmap.pdf
subset-pareto-tradeoff.pdf
```

The ZIP review bundle contains all compact CSV, JSON, text, log, and PDF
artifacts, but deliberately excludes the potentially large Parquet score table.
The report contains both standalone metric timings and measured timings for
every complete subset. Occupancy and maximum-gap candidates share their sort
and feature pass in the subset benchmark. Gap-uniformity still has its own sort;
close finalists should be benchmarked again after the selected production code
has been optimized.

### Accuracy-first RANDOM metric-selection experiment (v2)

Version 2 removes classifier-threshold leakage from the structured generators.
Counter steps span the complete `1..65535` domain without importing the
production increment or clustering thresholds. It uses independent selection
and held-out generator profiles, adds burst-loss conditions, evaluates all 63
metric subsets, and compares minimum, Fisher, Cauchy, and selection-weighted
minimum combination rules. Every rule is calibrated against dependent
true-RANDOM metric scores rather than an asymptotic combined-p-value formula.

```bash
# Fast end-to-end smoke run (not statistically conclusive)
make evaluate-random-classifier-v2 ARGS="--selection-samples-per-strategy 100 --test-samples-per-strategy 100 --weight-training-samples-per-strategy 100 --calibration-samples-per-condition 1000 --null-table-samples 5000 --target-random-frr 0.01 --batch-size 100 --seed 42"

# Accuracy-first full run
make evaluate-random-classifier-v2 ARGS="--selection-samples-per-strategy 100000 --test-samples-per-strategy 500000 --weight-training-samples-per-strategy 50000 --calibration-samples-per-condition 1000000 --null-table-samples 1000000 --target-random-frr 0.0001 --batch-size 10000 --seed 20260925"
```

The selection profile learns only the weighted-minimum allocation and provides
development-set comparisons. The independently seeded held-out profile uses a
different step distribution, broader jitter and drift, and more imbalanced
multi-counter traffic. Accuracy reports prioritize worst-case, p95, and
production-residual-like False-RANDOM rates; runtime is reported but used only
after accuracy unless costs differ materially.

Compact reports are written to
`data/processed/classifier-validation/random-classifier-evaluation-v2/`:

```text
summary.json
combination-results.csv
combination-by-scenario.csv
step-sensitivity.csv
heldout-pareto-frontier.csv
recommendations.txt
run.log
random-classifier-review-bundle-v2.zip
heldout-metric-scores.pq         # large, excluded from the review ZIP
```

Plots are written to
`reports/figures/classifier-validation/random-classifier-evaluation-v2/`:

```text
metric-false-random-heatmap.pdf
combination-accuracy-tradeoff.pdf
metric-step-sensitivity.pdf
```

The ZIP is the preferred review artifact because it contains every compact
table, the recommendations, the run log, and all three plots.

## Merging base and mass strategies

The canonical no-connection RT-base and fixed-interval-mass results can be
merged after both measurements have been classified:

```bash
python ipid_analysis/strategy_merge.py \
  tcp.ipid.no-connection.rt-based.base \
  tcp.ipid.no-connection.fixed-interval.mass

python ipid_analysis/plot_strategies.py \
  tcp.ipid.no-connection.rt-based.base \
  tcp.ipid.no-connection.fixed-interval.mass
```

A classified base strategy is retained. `UNCLASSIFIED` base rows are replaced
by their mass strategy; if their intended mass probe produced no stored row,
they become `NOT_ENOUGH_SAMPLES`. Base probe failures do not appear in the
merged result because the base strategies file defines its population.

| Base | Mass | Merged |
|---|---|---|
| classified strategy | not targeted | base strategy |
| `UNCLASSIFIED` | classified strategy | mass strategy |
| `UNCLASSIFIED` | `UNCLASSIFIED` | `UNCLASSIFIED` |
| `UNCLASSIFIED` | probe failed | `NOT_ENOUGH_SAMPLES` |
| probe failed | not targeted | omitted |

The commands above write:

```text
data/processed/<zmap-id>/no-connection/merged/rt-based-base_fixed-interval-mass/n-rt-b_fi-m_strategies.pq
reports/figures/<zmap-id>/no-connection/merged/rt-based-base_fixed-interval-mass/n-rt-b_fi-m_strategies.pdf
reports/figures/<zmap-id>/no-connection/merged/rt-based-base_fixed-interval-mass/n-rt-b_fi-m_strategies.json
```

`postprocess.py` performs these canonical merges and plots automatically after
all individual measurements in the manifest have been processed.

### RT-based to fixed-interval strategy refinement

For every canonical no-connection RT-base and fixed-interval-mass pair,
`make analyse data.json` also creates a compact ACM-width stacked-bar figure.
The upper bar contains the complete RT-based strategy distribution. The lower
bar contains the strategy distribution of the fixed-interval mass result, whose
target population is exactly the addresses classified as `UNCLASSIFIED` by the
RT-based measurement. Light guide lines expand the RT `UNCLASSIFIED` segment to
the fixed-interval bar.

The renderer validates this population relationship and fails instead of
creating a misleading figure if a fixed-interval address was not RT
`UNCLASSIFIED`. Probe failures that make the stored fixed-interval result smaller
than the target population are shown as `NOT_ENOUGH_SAMPLES`, so the lower bar
still represents every intended follow-up target and sums to 100%.

```bash
python ipid_analysis/plot_strategy_refinement.py \
  tcp.ipid.no-connection.rt-based.base \
  tcp.ipid.no-connection.fixed-interval.mass \
  --manifest data.json
```

The generated artifacts are:

```text
data/processed/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_measurement-type-by-strategy.pq

reports/figures/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_measurement-type-by-strategy.pdf
    n-rt-b_fi-m_measurement-type-by-strategy.json
```

For TCP campaigns that also contain
`tcp.ipid.connection.rt-based.base`, the same analysis run creates a second
figure with a third `RT-based & Connection-oriented` bar. The original
two-bar figure remains unchanged. The three-bar variant can also be rendered
directly:

```bash
python ipid_analysis/plot_strategy_refinement.py \
  tcp.ipid.no-connection.rt-based.base \
  tcp.ipid.no-connection.fixed-interval.mass \
  tcp.ipid.connection.rt-based.base \
  --manifest data.json
```

Its additional artifacts are:

```text
data/processed/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_measurement-type-by-strategy-with-connection.pq

reports/figures/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_measurement-type-by-strategy-with-connection.pdf
    n-rt-b_fi-m_measurement-type-by-strategy-with-connection.json
```

### TCP flags by merged strategy

For TCP, `make analyse data.json` also joins the merged RT-based-base and
fixed-interval-mass strategy result to the original ZMap `REPLY_TYPE` by
`IP_ADDR`. It creates three independently normalized bars: all recognized TCP
replies (`SYN-ACK/RST`), only `SYN-ACK`, and only `RST`. The label is `RST`
rather than `RST-ACK` because ZMap persists the reply classification `rst`, not
the complete received TCP flag set.

```bash
python ipid_analysis/plot_tcp_flags_strategy.py \
  tcp.ipid.no-connection.rt-based.base \
  tcp.ipid.no-connection.fixed-interval.mass \
  --manifest data.json
```

The generated artifacts are:

```text
data/processed/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_tcp-flags-by-strategy.pq

reports/figures/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_tcp-flags-by-strategy.pdf
    n-rt-b_fi-m_tcp-flags-by-strategy.json
```

### Operating-system groups by strategy

For every ICMP, TCP, or UDP-DNS campaign with an `os` measurement,
`make analyse data.json` joins the protocol's OS fingerprints from
`data/raw/os/<os-id>/os.pq` to that protocol's merged RT-based-base and
fixed-interval-mass strategies by `IP_ADDR`. Postprocessing validates the
current raw schema and maps every resolved `OS_TAG` to exactly one `OS_GROUP`.
It stores the per-IP result in
`data/processed/os/<os-id>/os-groups.pq` and creates one
ACM-width heatmap per protocol, split into represented `General-Purpose OS`,
`Network / Appliance OS`, and `Embedded / RTOS` sections. Every group row is
normalized independently to 100%, while its matched IP-address count is shown
beside the row label. Within each section, groups are ordered by descending
IP-address count. Exact zero cells are
displayed as `-`.
All nine IP-ID selection strategies plus the `NOT_ENOUGH_SAMPLES` follow-up
outcome remain visible even when a complete column is zero. Each OS-group row
therefore represents its complete matched merged population and still sums
to 100%.

The S3 analysis worker downloads `os-coverage.json` beside raw `os.pq`, so the
target, classification, per-service response/evidence/tag, and conflict counts
recorded by the measurement remain available for coverage reporting.

When `tcp.ipid.connection.rt-based.base` is present, the same analysis also
creates an identical heatmap for that individual connection-oriented strategy
result. It uses the same OS groups, strategy columns, row normalization,
ordering, labels, and color scale as the merged TCP plot.

The taxonomy covers every canonical OS tag emitted by `ipid-measure`. Linux
distributions, FreeBSD, OpenBSD, NetBSD, SONiC, and SonicWall retain separate
groups where their IP-ID behavior can differ. Product variants such as Cisco
IOS, IOS XE, IOS XR, NX-OS, ASA, and FTD map surjectively to the `cisco` group.
Ambiguous and unclassified rows remain quantitative measurement outcomes in
raw `os.pq`; the group population contains resolved rows only.

DuckDB performs the tag-to-group projection and strategy join using streaming
Parquet scans and bounded memory. The processed group file is reused by the TCP
connection and merged analyses when the source file and taxonomy version match.
Python receives only the compact group-by-strategy aggregate used for plotting.

```bash
make analyse data.json
```

The generated artifacts are:

```text
data/processed/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_operating-system-group-by-strategy.pq

data/processed/os/<os-id>/
  os-groups.pq
  os-groups.meta.json

reports/figures/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_operating-system-group-by-strategy.pdf
    n-rt-b_fi-m_operating-system-group-by-strategy.json

data/processed/<zmap-id>/connection/rt-based-base/
  c-rt-b_operating-system-group-by-strategy.pq

reports/figures/<zmap-id>/connection/rt-based-base/
  c-rt-b_operating-system-group-by-strategy.pdf
  c-rt-b_operating-system-group-by-strategy.json
```

## ACM comparison figures

For every protocol and connection mode that has both an RT-based base run and a
fixed-interval base run, `make analyse data.json` additionally creates three
compact, title-free paper figures:

1. split violin plots of the per-IP median probing interval by MaxMind continent
   (limited to p99.5),
2. paired empirical increment CDFs for `SINGLE`, `PER_DESTINATION`,
   `PER_CONNECTION`, and `PER_BUCKET` (limited to p99.9), and
3. a row-normalized strategy-intersection heatmap.

The MaxMind figure needs a GeoLite2/GeoIP2 Country or City database. Put it at
`references/GeoLite2-Country.mmdb`, set `IPID_MAXMIND_DB`, or pass it directly:

```bash
python ipid_analysis/postprocess.py data.json \
  --maxmind-db /path/to/GeoLite2-Country.mmdb

python ipid_analysis/paper_figures.py \
  udp-dns.ipid.connection.rt-based.base \
  udp-dns.ipid.connection.fixed-interval.base \
  --manifest data.json \
  --maxmind-db /path/to/GeoLite2-Country.mmdb
```

If no MaxMind database is configured, the increment and intersection figures
are still produced and only the continent figure is skipped with a warning.
For a connection-mode comparison, the artifacts are written as:

```text
data/processed/<zmap-id>/<connection-mode>/comparison/
  rt-based-base_fixed-interval-base/
    <n|c>-rt-b_fi-b_ip-continents.pq
    <n|c>-rt-b_fi-b_probing-intervals-by-continent.pq
    <n|c>-rt-b_fi-b_increment-distributions.pq
    <n|c>-rt-b_fi-b_strategy-intersection.pq

reports/figures/<zmap-id>/<connection-mode>/comparison/
  rt-based-base_fixed-interval-base/
    <n|c>-rt-b_fi-b_probing-intervals-by-continent.{pdf,json}
    <n|c>-rt-b_fi-b_increment-distributions.{pdf,json}
    <n|c>-rt-b_fi-b_strategy-intersection.{pdf,json}
```

The aggregate Parquets make the plotted values independently inspectable. Each
JSON sidecar records the source measurements, aggregation/normalization method,
percentile handling, population sizes, and generation timestamp.

## Manifest and artifact naming

The campaign manifest uses descriptive keys for connection and interval modes:

```json
{
  "tcp": {
    "zmap": "tcp-80_<timestamp>",
    "os": "tcp-80_<timestamp>",
    "ipid": {
      "no-connection": {
        "rt-based": {"base": "tcp-80_<timestamp>"},
        "fixed-interval": {
          "base": "tcp-80_<timestamp>",
          "mass": "tcp-80_<timestamp>"
        }
      },
      "connection": {
        "rt-based": {"base": "tcp-80_<timestamp>"},
        "fixed-interval": {"base": "tcp-80_<timestamp>"}
      }
    }
  }
}
```

CLI targets use the same names, for example
`tcp.ipid.no-connection.fixed-interval.mass`.

Every generated campaign artifact uses one shared layout below its ZMap run:

```text
<zmap-id>/
└── <no-connection|connection>/
    └── <rt-based|fixed-interval>-<base|mass>/
        └── <n|c>-<rt|fi>-<b|m>_<kind>.<pq|pdf|json>
```

For example, the mass fixed-interval strategy artifacts without established
connections are written as:

```text
data/processed/<zmap-id>/no-connection/fixed-interval-mass/n-fi-m_strategies.pq
reports/figures/<zmap-id>/no-connection/fixed-interval-mass/n-fi-m_strategies.pdf
reports/figures/<zmap-id>/no-connection/fixed-interval-mass/n-fi-m_strategies.json
```

## Project Organization

```
├── LICENSE            <- Open-source license if one is chosen
├── Makefile           <- Makefile with convenience commands like `make data` or `make train`
├── README.md          <- The top-level README for developers using this project.
├── data
│   ├── external       <- Data from third party sources.
│   ├── interim        <- Intermediate data that has been transformed.
│   ├── processed      <- The final, canonical data sets for modeling.
│   └── raw            <- The original, immutable data dump.
│
├── docs               <- A default mkdocs project; see www.mkdocs.org for details
│
├── models             <- Trained and serialized models, model predictions, or model summaries
│
├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
│                         the creator's initials, and a short `-` delimited description, e.g.
│                         `1.0-jqp-initial-data-exploration`.
│
├── pyproject.toml     <- Project configuration file with package metadata for 
│                         ipid_analysis and configuration for tools like black
│
├── references         <- Data dictionaries, manuals, and all other explanatory materials.
│
├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
│   └── figures        <- Generated graphics and figures to be used in reporting
│
├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
│                         generated with `pip freeze > requirements.txt`
│
├── setup.cfg          <- Configuration file for flake8
│
└── ipid_analysis   <- Source code for use in this project.
    │
    ├── __init__.py             <- Makes ipid_analysis a Python module
    │
    ├── config.py               <- Store useful variables and configuration
    │
    ├── dataset.py              <- Scripts to download or generate data
    │
    ├── features.py             <- Code to create features for modeling
    │
    ├── modeling                
    │   ├── __init__.py 
    │   ├── predict.py          <- Code to run model inference with trained models          
    │   └── train.py            <- Code to train models
    │
    └── plots.py                <- Code to create visualizations
```

--------

