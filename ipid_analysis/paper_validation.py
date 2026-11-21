"""Opt-in validation jobs for the existing create-only paper renderer."""

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from time import perf_counter

from ipid_analysis.artifacts import (
    assert_complete,
    completion_path,
    fingerprint,
    publish_new_file,
)
from ipid_analysis.runtime import exclusive_lock, temporary_path

CONDITIONS = ("ideal", "lossy", "reordered", "lossy-reordered")
MASS_PANELS = {
    "ideal": (("", "ideal"),),
    "lossy-vs-lossy-reordered": (
        ("Lossy Dataset", "lossy"),
        ("Lossy & Reordered Dataset", "lossy-reordered"),
    ),
    "lossy-vs-reordered": (("Lossy Dataset", "lossy"), ("Reordered Dataset", "reordered")),
}
FIGURES = (
    *(f"mass-4x25-classifier-confusion-{key}" for key in MASS_PANELS),
    *(f"mass-4x25-random-score-cdf-{c}" for c in CONDITIONS),
    *(f"mass-4x25-nist-score-cdf-{c}" for c in CONDITIONS),
    "e2e-classifier-confusion-ideal",
)


@dataclass(frozen=True)
class ValidationJob:
    source: Path
    destination: Path
    figure: str
    kind: str = "classifier-validation"

    @property
    def metadata(self):
        return self.destination.with_suffix(".json")


def discover(reports_root, processed_root):
    from ipid_analysis.classifier_validation_preparation import cache_manifest

    root = Path(reports_root) / "figures/classifier-validation"
    source = cache_manifest(reports_root, processed_root)
    return [
        ValidationJob(
            source,
            root / ("nist-baseline" if "-nist-score-" in figure else "") / f"{figure}__paper.pdf",
            figure,
        )
        for figure in FIGURES
    ]


def existing_result(job):
    """Preserve every existing file/receipt, including incomplete pairs."""
    from ipid_analysis.paper_ready import Result

    pdf, metadata = os.path.lexists(job.destination), os.path.lexists(job.metadata)
    if pdf and metadata:
        return Result(str(job.destination), "skipped-existing")
    if (
        pdf
        or metadata
        or any(os.path.lexists(completion_path(p)) for p in (job.destination, job.metadata))
    ):
        return Result(
            str(job.destination),
            "failed",
            error="existing validation PDF/JSON pair or receipt is incomplete; left untouched",
        )
    return None


def load_scores(path):
    import numpy as np
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from ipid_analysis.classifier_validation_preparation import (
        CLASSES,
        MAX_TARGETS_PER_CLASS,
        SCORE_SCHEMA,
    )

    assert_complete(path)
    with pq.ParquetFile(path) as source:
        if (
            source.schema_arrow.remove_metadata() != SCORE_SCHEMA
            or source.metadata.num_rows > 8 * MAX_TARGETS_PER_CLASS
        ):
            raise ValueError("unexpected validation score histogram schema/size")
        table = source.read()
    if any(c.null_count for c in table.columns):
        raise ValueError("null validation score histogram")
    result = {}
    if set(pc.unique(table["STRATEGY"]).to_pylist()) != set(CLASSES):
        raise ValueError("validation score histogram is missing a class or has an unknown class")
    for k in CLASSES:
        selected = table.filter(pc.equal(table["STRATEGY"], k))
        values = selected["SCORE"].to_numpy()
        counts = selected["COUNT"].to_numpy()
        if (
            not np.isfinite(values).all()
            or np.any((values < 0) | (values > 1))
            or np.any(np.diff(values) <= 0)
            or np.any(counts <= 0)
            or sum(map(int, counts)) > max(1000, MAX_TARGETS_PER_CLASS)
        ):
            raise ValueError("invalid validation histogram values/counts")
        result[k] = np.repeat(values, counts)
    return result


def load_payload(job):
    from ipid_analysis.classifier_validation_preparation import (
        read_json,
        validate_e2e,
        validate_metrics,
    )

    if not job.source.is_file():
        raise ValueError(
            'validation cache missing; run make validate-classifier ARGS="--prepare-paper" once'
        )
    manifest = read_json(job.source)
    if manifest.get("version") != 1:
        raise ValueError("unsupported validation cache version")
    root = job.source.parent

    def component(name):
        if not isinstance(name, str) or Path(name).name != name or name in (".", ".."):
            raise ValueError("invalid validation cache component path")
        path = root / name
        expected = [item for item in manifest["components"] if Path(item["path"]).name == name]
        if len(expected) != 1 or fingerprint(path) != expected[0]:
            raise ValueError("validation component changed after preparation")
        return path

    if job.figure.startswith("mass-4x25-classifier-confusion-"):
        source = component(manifest["mass"])
        data = read_json(source)
        key = job.figure.removeprefix("mass-4x25-classifier-confusion-")
        payload = {
            "panels": [
                {"title": title, "condition": condition, "metrics": data[condition]}
                for title, condition in MASS_PANELS[key]
            ]
        }
        for panel in payload["panels"]:
            panel["metrics"] = validate_metrics(panel["metrics"])
    elif job.figure == "e2e-classifier-confusion-ideal":
        source = component(manifest["e2e"])
        payload = validate_e2e(read_json(source))
    else:
        method = "nist" if "-nist-score-" in job.figure else "random"
        condition = job.figure.removeprefix(f"mass-4x25-{method}-score-cdf-")
        if condition not in CONDITIONS:
            raise ValueError("unknown validation condition")
        source = component(manifest["scores"][f"{method}:{condition}"])
        payload = {"method": method, "condition": condition, "scores": load_scores(source)}
        expected = manifest["samples_per_nontrivial_class"]
        for k, values in payload["scores"].items():
            if len(values) != (1000 if k in ("REFLECTION", "CONSTANT") else expected):
                raise ValueError("CDF population disagrees with validation manifest")
            if method == "nist" and values.min() <= 0:
                raise ValueError("adapted NIST empirical p-values must be positive")
    return manifest, payload, [job.source, source]


def worker(job):
    from ipid_analysis.paper_ready import Result

    started = perf_counter()
    try:
        with exclusive_lock("paper-validation-pair:" + str(job.destination)):
            previous = existing_result(job)
            if previous:
                return previous
            manifest, payload, inputs = load_payload(job)
            from ipid_analysis.paper_validation_plot import draw

            before = [fingerprint(p) for p in inputs]
            profile = hashlib.sha256()
            for name in (
                "paper_validation_plot.py",
                "paper_plot_style.py",
                "paper_heatmap_style.py",
                "paper_figures.py",
                "paper_increment_plot.py",
            ):
                profile.update(Path(__file__).with_name(name).read_bytes())
            parameters = {
                "operation": "paper-validation-figure-v1",
                "figure": job.figure,
                "profile_sha256": profile.hexdigest(),
            }
            pdf_temp, json_temp = temporary_path(job.destination), temporary_path(job.metadata)
            try:
                layout = draw(job.figure, payload, pdf_temp)
                layout.pop("file", None)
                if "scores" in payload:
                    public = {k: v for k, v in payload.items() if k != "scores"}
                    threshold = (
                        manifest["nist_threshold"]
                        if payload["method"] == "nist"
                        else manifest["random_threshold"]
                    )
                    public["threshold"] = threshold
                    public["summary_by_strategy"] = {
                        k: {
                            "count": len(v),
                            "minimum": float(v.min()),
                            "maximum": float(v.max()),
                            "random_compatible_count": int((v >= threshold).sum()),
                        }
                        for k, v in payload["scores"].items()
                    }
                    public["aggregate"] = str(inputs[-1])
                else:
                    public = payload
                result = {
                    "version": 1,
                    "figure": job.figure,
                    "pdf": str(job.destination),
                    "paper_ready": True,
                    "layout": layout,
                    "parameters": parameters,
                    "validation": {
                        k: v
                        for k, v in manifest.items()
                        if k not in ("components", "scores", "mass", "e2e")
                    },
                    "sources": before,
                    **public,
                }
                result["normalization"] = (
                    {
                        "rt": "per original generating-class support",
                        "fi": "per actually forwarded generating-class support",
                        "e2e": "all original targets, including retained RT errors",
                    }
                    if job.figure.startswith("e2e-")
                    else "per generating class; all observations in the selected dataset"
                )
                json_temp.write_text(
                    json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
                )
                if [fingerprint(p) for p in inputs] != before:
                    raise ValueError("validation input changed during rendering")
                # No artifact/receipt is ever replaced, including partial pairs.
                publish_new_file(json_temp, job.metadata, inputs=inputs, parameters=parameters)
                publish_new_file(pdf_temp, job.destination, inputs=inputs, parameters=parameters)
            finally:
                pdf_temp.unlink(missing_ok=True)
                json_temp.unlink(missing_ok=True)
        return Result(str(job.destination), "created", perf_counter() - started)
    except Exception as exc:
        return Result(
            str(job.destination),
            "failed",
            perf_counter() - started,
            f"{type(exc).__name__}: {exc}",
        )
