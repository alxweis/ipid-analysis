# ipid-analysis

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

A short description of the project.

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

## Strategy classification by measurement scale

Base measurements classify the position-dependent or cheaply identifiable
strategies `REFLECTION`, `CONSTANT`, `PER_DESTINATION`, `PER_CONNECTION`,
`SINGLE`, and `PER_BUCKET`. All other base sequences are `UNCLASSIFIED` and can
be passed to a mass measurement.

Mass measurements use only position-independent rules and classify `CONSTANT`,
`MULTI`, and `RANDOM`. Any sequence not matching those rules remains
`UNCLASSIFIED`. Minimum reply-rate filtering is performed by `ipid-measure`
before fixed-interval rows are written, so analysis does not duplicate that
measurement-stage decision as an IPID strategy.

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

### Operating systems by merged strategy

For every ICMP, TCP, or UDP-DNS campaign with an `os` measurement,
`make analyse data.json` joins the protocol's OS fingerprints from
`data/raw/os/<os-id>/os.pq` to that protocol's merged RT-based-base and
fixed-interval-mass strategies by `IP_ADDR`. It creates one ACM-width heatmap
per protocol, split into `General-Purpose OS` and `Network OS`. Every
operating-system row is normalized independently to 100%, while its matched
IP-address count is shown beside the row label. Exact zero cells are displayed
as `-`.
All nine IP-ID selection strategies plus the `NOT_ENOUGH_SAMPLES` follow-up
outcome remain visible even when a complete column is zero. Each operating-system
row therefore represents its complete matched merged population and still sums
to 100%.

The OS grouping explicitly covers every `OS_NAME` currently emitted by
`ipid-measure`. Its `rhel` fingerprint includes both RHEL and CentOS banners, so
the figure labels that row `RHEL / CentOS` instead of implying a distinction
that is not present in `os.pq`.

```bash
python ipid_analysis/plot_os_strategy.py \
  <protocol>.ipid.no-connection.rt-based.base \
  <protocol>.ipid.no-connection.fixed-interval.mass \
  --manifest data.json
```

The generated artifacts are:

```text
data/processed/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_operating-system-by-strategy.pq

reports/figures/<zmap-id>/no-connection/merged/
  rt-based-base_fixed-interval-mass/
    n-rt-b_fi-m_operating-system-by-strategy.pdf
    n-rt-b_fi-m_operating-system-by-strategy.json
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

