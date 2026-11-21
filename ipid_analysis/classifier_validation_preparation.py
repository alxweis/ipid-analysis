"""Create-only, persistent inputs for paper validation figures.

Mass counts are projected from the original validation Parquet. The additional
paired E2E experiment and the two CDF methods are prepared once; redraws never
generate sequences, classify targets, calibrate tests or open original raw data.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import classifier_validation as cv
from ipid_analysis.artifacts import (
    assert_complete,
    cache_current,
    fingerprint,
    publish_new_file,
)
from ipid_analysis.classifier_validation_parallel import bounded_map, prepare_nulls, worker_count
from ipid_analysis.nist_short_sequence import (
    NIST_BASELINE_VERSION,
    ShortNistNullTables,
    ipids_to_bitstreams,
)
from ipid_analysis.plot_random_structure_score_cdf import calculate_scores
from ipid_analysis.random_classifier_candidate import (
    CANDIDATE_NULL_TABLE_SAMPLES,
    CANDIDATE_NULL_TABLE_SEED,
    CANDIDATE_RANDOM_MIN_SCORE,
    CANDIDATE_RANDOM_SCORE_VERSION,
    CANDIDATE_RANDOM_THRESHOLD_STATUS,
    create_candidate_null_tables,
)
from ipid_analysis.runtime import exclusive_lock, temporary_path
from ipid_analysis.strategies import IPIDStrategy, MeasurementConfig, classify_batch
from ipid_analysis.synthetic_mass_sequences import generate_mass_paper_sequences

VERSION = 1
CLASSES = cv.FIXED_STRATEGIES
OUTPUTS = (*CLASSES, "UNCLASSIFIED")
CONDITIONS = ("ideal", "lossy", "reordered", "lossy-reordered")
MASS_DATASETS = dict(
    zip(
        CONDITIONS,
        (
            cv.FIXED_IDEAL_DATASET,
            cv.FIXED_LOSSY_DATASET,
            cv.MASS_REORDERED_DATASET,
            cv.FIXED_REORDERED_DATASET,
        ),
    )
)
MAX_TARGETS_PER_CLASS = 1_000_000
MAX_JSON_BYTES = 4 << 20
SCORE_SCHEMA = pa.schema(
    [
        ("STRATEGY", pa.string()),
        ("SCORE", pa.float64()),
        ("COUNT", pa.int64()),
    ]
)


def read_json(path):
    path = Path(path)
    assert_complete(path)
    with path.open("rb") as stream:
        raw = stream.read(MAX_JSON_BYTES + 1)
    if len(raw) > MAX_JSON_BYTES:
        raise ValueError(f"validation metadata exceeds 4 MiB: {path}")
    return json.loads(raw)


def model_sources():
    names = (
        "classifier_validation_preparation.py",
        "classifier_validation_parallel.py",
        "classifier_validation.py",
        "strategies.py",
        "random_classifier_candidate.py",
        "empirical_random_uniformity.py",
        "increment_uniformity.py",
        "nist_short_sequence.py",
        "synthetic_mass_sequences.py",
        "plot_random_structure_score_cdf.py",
    )
    return {
        name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
        for name in names
    }


@lru_cache(maxsize=1)
def model_signature():
    """Style changes do not invalidate the numerical cache."""
    digest = hashlib.sha256()
    for name in (
        "classifier_validation_preparation.py",
        "classifier_validation_parallel.py",
        "classifier_validation.py",
        "strategies.py",
        "random_classifier_candidate.py",
        "empirical_random_uniformity.py",
        "increment_uniformity.py",
        "nist_short_sequence.py",
        "synthetic_mass_sequences.py",
        "plot_random_structure_score_cdf.py",
    ):
        digest.update(Path(__file__).with_name(name).read_bytes())
    return digest.hexdigest()


def source_paths(reports_root, processed_root):
    return (
        Path(processed_root) / "classifier-validation/synthetic-classifier-validation.pq",
        Path(reports_root)
        / "figures/classifier-validation/out-of-scope-classifier-rejection.json",
    )


def cache_manifest(reports_root, processed_root):
    sources = source_paths(reports_root, processed_root)
    identity = {
        "version": VERSION,
        "model": model_signature(),
        "sources": [fingerprint(path) for path in sources],
    }
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return Path(processed_root) / "classifier-validation/paper-ready" / key / "manifest.json"


def matrix_metrics(counts):
    a = np.asarray(counts)
    if a.shape != (8, 9) or a.dtype.kind not in "iu" or np.any(a < 0):
        raise ValueError("expected nonnegative integer validation counts, shape 8x9")
    totals = a.sum(axis=1)
    if not np.all(totals > 0):
        raise ValueError("all generating classes need nonzero support")
    correct = np.diag(a).astype(float)
    predicted = a[:, :8].sum(axis=0)
    precision = np.divide(correct, predicted, out=np.zeros(8), where=predicted > 0)
    recall = correct / totals
    return {
        "classes": list(CLASSES),
        "predicted_classes": list(OUTPUTS),
        "counts": a.tolist(),
        "supports": totals.tolist(),
        "percentages": (100 * a / totals[:, None]).tolist(),
        "accuracy": float(correct.sum() / totals.sum()),
        "balanced_accuracy": float(recall.mean()),
        "precision_by_class": dict(zip(CLASSES, precision.tolist())),
        "recall_by_class": dict(zip(CLASSES, recall.tolist())),
        "unclassified_count": int(a[:, -1].sum()),
    }


def validate_metrics(value):
    """Recompute display statistics from exact counts; reject inconsistent caches."""
    if value.get("classes") != list(CLASSES) or value.get("predicted_classes") != list(OUTPUTS):
        raise ValueError("unexpected validation class order")
    counts = np.asarray(value["counts"])
    if counts.dtype.kind not in "iu" or np.any(counts > MAX_TARGETS_PER_CLASS):
        raise ValueError("invalid validation population")
    expected = matrix_metrics(counts)
    if max(expected["supports"]) > MAX_TARGETS_PER_CLASS:
        raise ValueError("validation support exceeds sample budget")
    for key in ("supports", "percentages", "accuracy", "balanced_accuracy", "unclassified_count"):
        if not np.allclose(value[key], expected[key], rtol=1e-12, atol=1e-12):
            raise ValueError(f"validation counts disagree with {key}")
    return expected


def validate_e2e(value):
    rt = validate_metrics(value["e2e_rt"])
    rt_counts = np.asarray(rt["counts"])
    if rt_counts[:, 6:8].any():
        raise ValueError("RT cannot emit MULTI/RANDOM")
    retained = np.array(rt_counts, copy=True)
    retained[:, -1] = 0
    for i, k in enumerate(CLASSES):
        route = value["e2e_routing"][k]
        if (
            route["support"] != rt["supports"][i]
            or route["forwarded_to_fi"] != int(rt_counts[i, -1])
            or route["retained_rt_counts"] != retained[i].tolist()
        ):
            raise ValueError("E2E routing does not match actual RT UNCLASSIFIED decisions")
    if not rt_counts[:, -1].any():
        raise ValueError("no targets forwarded to FI")
    for condition in CONDITIONS:
        final = validate_metrics(value["e2e"][condition])
        fi = np.asarray(final["counts"]) - retained
        if (
            np.any(fi < 0)
            or final["supports"] != rt["supports"]
            or not np.array_equal(fi.sum(axis=1), rt_counts[:, -1])
        ):
            raise ValueError("E2E changed retained RT decisions or lost forwarded targets")
    return value


def _code_counts(codes):
    counts = np.bincount(codes, minlength=len(IPIDStrategy))
    if counts[int(IPIDStrategy.NOT_ENOUGH_SAMPLES)]:
        raise ValueError("unexpected insufficient-sample result in synthetic validation")
    return [int(counts[int(IPIDStrategy[k])]) for k in OUTPUTS]


def mass_metrics(path, config):
    """Project three columns only; never deserialize retained IPID sequences."""
    columns = ["DATASET", "EXPECTED_STRATEGY", "DETECTED_STRATEGY"]
    inverse = {name: condition for condition, name in MASS_DATASETS.items()}
    counts = {condition: np.zeros((8, 9), dtype=np.int64) for condition in CONDITIONS}
    with pq.ParquetFile(path) as source:
        for batch in source.iter_batches(columns=columns, batch_size=262_144):
            grouped = (
                pa.Table.from_batches([batch]).group_by(columns).aggregate([("DATASET", "count")])
            )
            for row in grouped.to_pylist():
                if row["DATASET"] not in inverse:
                    continue
                i, j = (
                    CLASSES.index(row["EXPECTED_STRATEGY"]),
                    OUTPUTS.index(row["DETECTED_STRATEGY"]),
                )
                counts[inverse[row["DATASET"]]][i, j] += row["DATASET_count"]
    result = {c: matrix_metrics(a) for c, a in counts.items()}
    expected = [
        1000 if k in cv.TRIVIAL_STRATEGIES else config["samples_per_strategy"] for k in CLASSES
    ]
    if any(m["supports"] != expected for m in result.values()):
        raise ValueError("Mass Parquet supports disagree with original validation metadata")
    return result


def condition_inputs(values, rng, reorder_rng):
    missing, lossy, mixed = cv.apply_fixed_interval_impairments(values, rng)
    return {
        "ideal": (values, None),
        "lossy": (lossy, missing),
        "reordered": (cv.apply_reordering(values, reorder_rng, reordered_count=20), None),
        "lossy-reordered": (mixed, missing),
    }


def paired_sequences(samples, seed):
    """Disjoint 16 RT + 100 FI observations of the same latent target."""
    rng = np.random.default_rng(np.random.SeedSequence([seed, 20_261_120]))
    _, traces = cv._generate_exact_sequences(max(samples, 1000), 29, rng)
    traces["MULTI"] = cv._generate_multi_sequences(samples, 116, rng)
    traces["RANDOM"] = rng.integers(0, 65536, size=(samples, 116), dtype=np.uint16)
    for k in CLASSES:
        trace = traces.pop(k)[: 1000 if k in cv.TRIVIAL_STRATEGIES else samples]
        rt, fi = trace[:, :16], trace[:, 16:].copy()
        if k == "REFLECTION":
            offset = (rt[:, 0].astype(np.int64) - int(cv.REQUEST_IP_IDS[0])) % 65536
            pattern = cv.REQUEST_IP_IDS[np.arange(100) % len(cv.REQUEST_IP_IDS)]
            fi = ((pattern[None, :] + offset[:, None]) % 65536).astype(np.uint16)
        yield k, rt, fi


def _mass_codes(values, missing, tables):
    return np.concatenate(
        [
            cv._classify_mass(
                values[i : i + 4096],
                None if missing is None else missing[i : i + 4096],
                candidate_null_tables=tables,
            )
            for i in range(0, len(values), 4096)
        ]
    )


def _e2e_class(job):
    index, k, rt, fi, seed, tables = job
    counts = {}
    config = MeasurementConfig(4, 4, cv.REQUEST_IP_IDS)
    print(f"validation prepare: E2E {k}", flush=True)
    codes = classify_batch(rt, config)
    gate = codes == int(IPIDStrategy.UNCLASSIFIED)
    rt_counts = _code_counts(codes)
    routing = {
        "support": len(rt),
        "forwarded_to_fi": int(gate.sum()),
        "retained_rt_counts": _code_counts(codes[~gate]),
    }
    if not gate.any():
        # This class never reaches FI. Do not generate four unused impairment
        # variants; class-local RNG streams keep all other outcomes unchanged.
        return k, rt_counts, routing, {c: rt_counts.copy() for c in CONDITIONS}
    rng = np.random.default_rng(np.random.SeedSequence([seed, 999, index]))
    rrng = np.random.default_rng(np.random.SeedSequence([seed, 1000, index]))
    for c, (values, missing) in condition_inputs(fi, rng, rrng).items():
        final = codes.copy()
        if gate.any():
            final[gate] = _mass_codes(
                values[gate], None if missing is None else missing[gate], tables
            )
        if not np.array_equal(final[~gate], codes[~gate]):
            raise ValueError("a retained RT decision changed")
        counts[c] = _code_counts(final)
    return k, rt_counts, routing, counts


def e2e_metrics(samples, seed, tables, *, pool=None, workers=1):
    counts = {c: [] for c in CONDITIONS}
    rt_counts, routing = [], {}
    jobs = (
        (i, k, rt, fi, seed, tables)
        for i, (k, rt, fi) in enumerate(paired_sequences(samples, seed))
    )
    for k, rt, route, final in bounded_map(pool, _e2e_class, jobs, workers):
        rt_counts.append(rt)
        routing[k] = route
        for condition in CONDITIONS:
            counts[condition].append(final[condition])
    return {
        "e2e": {c: matrix_metrics(np.array(a)) for c, a in counts.items()},
        "e2e_rt": matrix_metrics(np.array(rt_counts)),
        "e2e_routing": routing,
        "model": {
            "version": "paired-116-v1",
            "rt_observations": 16,
            "fi_new_observations": 100,
            "rt_impairments": "none for all eight generating classes",
            "routing": "FI only for actual RT UNCLASSIFIED; all other RT decisions retained",
            "cohort": "one 116-observation latent trace per target; reflection request IDs reset at the phase boundary",
            "multi": "balanced independent advancing counters across 116 positions, not separately within each phase",
            "limitations": "synthetic classifier validation; no real RTT, timeout, middlebox, fresh socket identity or temporal drift model",
        },
    }


def score_histogram(scores):
    arrays = {"STRATEGY": [], "SCORE": [], "COUNT": []}
    for k in CLASSES:
        values = np.asarray(scores[k])
        if (
            values.ndim != 1
            or not len(values)
            or not np.isfinite(values).all()
            or np.any((values < 0) | (values > 1))
        ):
            raise ValueError("invalid validation compatibility scores")
        unique, counts = np.unique(values, return_counts=True)
        arrays["STRATEGY"].extend([k] * len(unique))
        arrays["SCORE"].extend(unique.tolist())
        arrays["COUNT"].extend(counts.tolist())
    return pa.table(arrays, schema=SCORE_SCHEMA)


def _publish(path, value, inputs, parameters):
    if os.path.lexists(path):
        if not cache_current(path, inputs, parameters):
            raise ValueError(f"existing validation cache conflicts with its provenance: {path}")
        return
    temporary = temporary_path(path)
    try:
        if path.suffix == ".pq":
            pq.write_table(value, temporary, compression="zstd")
        else:
            temporary.write_text(
                json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
        publish_new_file(temporary, path, inputs=inputs, parameters=parameters)
    finally:
        temporary.unlink(missing_ok=True)


def _original_config(metadata):
    config = read_json(metadata)
    if (str(config.get("classifier_version")), str(config.get("synthetic_generator_version"))) != (
        str(cv.CLASSIFIER_VERSION),
        "4",
    ):
        raise ValueError("original validation metadata uses a different classifier/generator")
    random = config.get("random_structure_score", {})
    if (
        random.get("version") != CANDIDATE_RANDOM_SCORE_VERSION
        or random.get("threshold") != CANDIDATE_RANDOM_MIN_SCORE
    ):
        raise ValueError("original validation did not use the current production RANDOM score")
    null = random.get("null_tables", {})
    if (null.get("sample_count"), null.get("base_seed")) != (
        CANDIDATE_NULL_TABLE_SAMPLES,
        CANDIDATE_NULL_TABLE_SEED,
    ) or random.get("threshold_status") != CANDIDATE_RANDOM_THRESHOLD_STATUS:
        raise ValueError("original RANDOM calibration differs from the production calibration")
    n, seed = config.get("samples_per_strategy"), config.get("seed")
    if (
        type(n) is not int
        or not 1 <= n <= MAX_TARGETS_PER_CLASS
        or type(seed) is not int
        or seed < 0
    ):
        raise ValueError("invalid original synthetic sample budget/seed")
    if config.get("trivial_samples_per_strategy") != 1000 or config.get("connection_count") != 4:
        raise ValueError("unsupported original validation geometry/populations")
    if config.get("request_ip_ids") != cv.REQUEST_IP_IDS.tolist():
        raise ValueError("original validation used a different request IP-ID pattern")
    return config


def _cdf_job(job):
    k, condition, values, missing, tables, nist_tables, paths = job
    print(f"validation prepare: paired RANDOM/NIST scores {k}, {condition}", flush=True)
    present = np.ones_like(values, dtype=bool) if missing is None else ~missing
    result = {}
    if not paths["random", condition].exists():
        result["random"] = calculate_scores(values, ~present, tables)
    if not paths["nist", condition].exists():
        parts = []
        for i in range(0, len(values), 2000):
            bits = ipids_to_bitstreams(values[i : i + 2000], present[i : i + 2000])
            parts.append(nist_tables.combined_pvalues(bits)[0])
        result["nist"] = np.concatenate(parts)
    return k, condition, result


def prepare(reports_root, processed_root, *, workers=8):
    source, metadata = source_paths(reports_root, processed_root)
    manifest = cache_manifest(reports_root, processed_root)
    inputs = [source, metadata]
    before = [fingerprint(p) for p in inputs]
    parameters = {"operation": "paper-validation-data-v1", "model_sha256": model_signature()}
    with exclusive_lock("paper-validation:" + str(manifest)):
        if os.path.lexists(manifest):
            assert_complete(manifest)
            summary = read_json(manifest)
            if summary.get("original_inputs") != before or summary.get("parameters") != parameters:
                raise ValueError("existing validation manifest has different provenance")
            for component in summary["components"]:
                path = manifest.parent / Path(component["path"]).name
                if fingerprint(path) != component or not cache_current(path, inputs, parameters):
                    raise ValueError("existing validation component changed; left untouched")
            return manifest
        config = _original_config(metadata)
        samples, seed = config["samples_per_strategy"], config["seed"]
        root = manifest.parent
        mass_path, e2e_path = root / "mass.json", root / "e2e.json"
        if not mass_path.exists():
            print("validation prepare: projecting original Mass counts", flush=True)
            _publish(mass_path, mass_metrics(source, config), inputs, parameters)
        count = worker_count(workers, samples)
        print(f"validation prepare: {count} bounded compute workers", flush=True)
        tables = create_candidate_null_tables()
        nist_tables = ShortNistNullTables(1_000_000, 20_260_930, 2000)
        score_paths = {
            (method, c): root / f"{method}-{c}.pq"
            for method in ("random", "nist")
            for c in CONDITIONS
        }
        with ThreadPoolExecutor(max_workers=count) if count > 1 else nullcontext(None) as pool:
            null_paths = prepare_nulls(
                root, tables, nist_tables, inputs, parameters, _publish, pool, count
            )
            if not e2e_path.exists():
                _publish(
                    e2e_path,
                    # Row-wise impairment generation is GIL-bound. Threads
                    # slow this stage down; reserve the pool for NumPy/null/CDF
                    # work, and retain the same serial seeds and decisions.
                    e2e_metrics(samples, seed, tables),
                    inputs,
                    parameters,
                )
            if not all(p.exists() for p in score_paths.values()):
                rng, impair, reorder = [
                    np.random.default_rng(s) for s in np.random.SeedSequence(seed).spawn(3)
                ]
                populations = generate_mass_paper_sequences(samples, rng)
                scores = {(method, c): {} for method in ("random", "nist") for c in CONDITIONS}

                def jobs():
                    for k in CLASSES:
                        for c, (values, missing) in condition_inputs(
                            populations.pop(k), impair, reorder
                        ).items():
                            yield k, c, values, missing, tables, nist_tables, score_paths

                for k, c, results in bounded_map(pool, _cdf_job, jobs(), count):
                    for method, values in results.items():
                        scores[method, c][k] = values
                for key, path in score_paths.items():
                    if not path.exists():
                        _publish(path, score_histogram(scores[key]), inputs, parameters)
        components = [mass_path, e2e_path, *score_paths.values(), *null_paths]
        for p in components:
            if not cache_current(p, inputs, parameters):
                raise ValueError(f"invalid or unfinished validation cache component: {p}")
        if [fingerprint(p) for p in inputs] != before:
            raise ValueError("original validation input changed during preparation")
        summary = {
            "version": VERSION,
            "parameters": parameters,
            "model_sources": model_sources(),
            "original_inputs": before,
            "classifier_version": cv.CLASSIFIER_VERSION,
            "original_validation": config,
            "samples_per_nontrivial_class": samples,
            "trivial_samples_per_class": 1000,
            "seed": seed,
            "random_score_version": CANDIDATE_RANDOM_SCORE_VERSION,
            "random_threshold": CANDIDATE_RANDOM_MIN_SCORE,
            "nist_version": NIST_BASELINE_VERSION,
            "nist_threshold": 0.0005,
            "nist_null_seed": 20_260_930,
            "null_samples": 1_000_000,
            "random_null_seed": CANDIDATE_NULL_TABLE_SEED,
            "cdf_cohort": "identical values, loss masks and reorder permutations for RANDOM and adapted NIST",
            "mass": mass_path.name,
            "e2e": e2e_path.name,
            "scores": {f"{m}:{c}": p.name for (m, c), p in score_paths.items()},
            "components": [fingerprint(p) for p in components],
        }
        _publish(manifest, summary, inputs + components, parameters)
    return manifest
