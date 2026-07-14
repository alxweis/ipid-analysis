# ipid-analysis

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

A short description of the project.

## S3 measurement handoff worker

The analysis VM can process the stateless TCP RT measurement produced by
`ipid-measure` without any direct network connection between the VMs. Both sides
use the same S3 prefix and locally configured `s3cmd` credentials:

```bash
export IPID_ANALYSIS_S3_PREFIX=s3://bucket/ipid-analysis-workflow/
make workflow-worker
```

For every `jobs/<measurement-id>/request.json`, the worker:

1. downloads `ipid.pq` and `ipid.snapshot.yaml` from the completed measurement upload,
2. runs the normal IPID selection-strategy classifier,
3. writes `zmap_unclassified.pq` with the ZMap-compatible columns `IP_ADDR` and
   `REPLY_TYPE`, containing only `UNCLASSIFIED` addresses,
4. uploads that parquet and then publishes `done.json` with its row count, size,
   and SHA-256 digest.

If processing fails, `failed.json` is uploaded instead. Requests are idempotent:
jobs with either terminal marker are skipped. Use `--once` to process the current
queue and exit, which is useful for cron; the default process polls continuously.

The corresponding `ipid-measure` run waits for the terminal marker and will not
start the 25-request fixed-interval measurement until the result has been
downloaded and verified.

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

