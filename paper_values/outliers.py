"""Explicit EU/ICMP greedy outlier preparation; export only reads its small cache."""

import argparse
import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
from time import perf_counter

from paper_values.cache import fingerprint, verify_file
from paper_values.inputs import Inputs, select_artifacts
from paper_values.readers import counts
from paper_values.review import percentage, read_json, record

VERSION = "eu-icmp-rt-greedy-outliers-v1"
COUNTERS = ("SINGLE", "PER_DESTINATION", "PER_BUCKET")
PREFIX = "results.eu.icmp.fi-mass.all.single-per-destination-per-bucket.rt-outlier-score"


def cache_path(processed):
    return Path(processed) / "eu/paper-review" / (VERSION + ".json")


def code_identity():
    root = Path(__file__).resolve().parents[1]
    selected = {
        "paper_values/outliers.py": {"calculate", "canonical", "plan", "COUNTERS"},
        "ipid_analysis/deterministic_transition_analysis.py": {
            "greedy_outlier_scores",
            "counter_groups",
        },
        "ipid_analysis/strategies.py": {
            "_batch_to_matrix",
            "load_config",
            "MeasurementConfig",
            "MODULUS",
            "MIN_STEPS_BEFORE_WRAPAROUND",
            "MAX_INC",
            "CLASSIFIER_VERSION",
        },
    }
    definitions = []
    for relative, names in selected.items():
        found = set()
        for node in ast.parse((root / relative).read_text()).body:
            name = (
                node.name
                if isinstance(node, (ast.FunctionDef, ast.ClassDef))
                else next((t.id for t in node.targets if isinstance(t, ast.Name)), None)
                if isinstance(node, ast.Assign)
                else None
            )
            if name in names:
                found.add(name)
                definitions.append([relative, name, ast.dump(node, include_attributes=False)])
        if found != names:
            raise ValueError("missing outlier calculation identity component")
    # Export labels, formatting, comments and unrelated plotting changes must
    # not trigger another raw sequence scan. Numerical changes still invalidate.
    return hashlib.sha256(json.dumps(definitions, sort_keys=True).encode()).hexdigest()


def canonical(artifacts):
    index = {
        a.phase: a
        for a in artifacts
        if (a.site, a.protocol, a.connection, a.kind)
        == ("eu", "icmp", "no-connection", "strategies")
    }
    rt, fi = index.get("rt-base"), index.get("fi-mass")
    if rt is None or fi is None:
        raise ValueError("missing canonical EU ICMP RT-BASE/FI-MASS reports")
    if not rt.data.get("zmap_id") or rt.data["zmap_id"] != fi.data.get("zmap_id"):
        raise ValueError("outlier RT/FI reports belong to different discoveries")
    return rt, fi


def plan(artifacts, processed, raw):
    from paper_values.tcp_review import source

    rt, fi = canonical(artifacts)
    labels = [
        source(
            processed, Path("eu") / Path(a.relative.removeprefix("figures/")).with_suffix(".pq")
        )
        for a in (rt, fi)
    ]
    mid = rt.data.get("measurement_id")
    if not isinstance(mid, str) or not re.fullmatch(r"icmp_[A-Za-z0-9_-]+", mid):
        raise ValueError("invalid RT measurement identity for outlier raw lookup")
    ipid = source(raw, Path("eu/ipid") / mid / "ipid.pq")
    snapshot = ipid.with_name("ipid.snapshot.yaml")
    dependencies = [Path(rt.name), Path(fi.name), *labels, ipid, snapshot]
    from ipid_analysis.artifacts import assert_complete

    for path in dependencies:
        if not path.is_file():
            raise FileNotFoundError(path)
        if path in (Path(rt.name), Path(fi.name), snapshot):
            verify_file(path)
        else:
            assert_complete(path)  # Metadata only; no extra whole-table checksum scan.
    return dict(
        rt=rt,
        fi=fi,
        rt_labels=labels[0],
        fi_labels=labels[1],
        raw=ipid,
        snapshot=snapshot,
        inputs=dependencies,
    )


def footer(path):
    import pyarrow.parquet as pq

    with pq.ParquetFile(path) as p:
        if not {"IP_ADDR", "IPID_SEQUENCE"} <= set(p.schema_arrow.names):
            raise ValueError("RT file lacks IP_ADDR/IPID_SEQUENCE")
        compressed = sum(
            p.metadata.row_group(i).column(j).total_compressed_size
            for i in range(p.metadata.num_row_groups)
            for j in range(p.metadata.row_group(i).num_columns)
            if p.metadata.row_group(i).column(j).path_in_schema in ("IP_ADDR", "IPID_SEQUENCE")
        )
        return dict(
            rows=p.metadata.num_rows,
            row_groups=p.metadata.num_row_groups,
            projected_compressed_bytes=compressed,
            note="Unpruned IP_ADDR/IPID_SEQUENCE column bound, not a measured transfer or runtime",
        )


def read_prepared(processed, campaign, artifacts, *, archive=False):
    values = {
        f"{PREFIX}.{score}.pct": record(
            None,
            "percent",
            f"100 * greedy RT outlier-score={score} / EU RT-Unclassified -> FI-MASS Single/Per-Destination/Per-Bucket cohort; no renormalization to scores 0..5",
        )
        for score in range(6)
    }
    if archive or processed is None or not cache_path(processed).is_file():
        return values
    path = cache_path(processed)
    doc = read_json(path)
    rt, fi = canonical(artifacts)
    expected = {
        str(Path(a.name).resolve()): hashlib.sha256(Path(a.name).read_bytes()).hexdigest()
        for a in (rt, fi)
    }
    if (
        doc.get("version") != VERSION
        or doc.get("campaign") != campaign
        or doc.get("code_sha256") != code_identity()
        or doc.get("reports") != expected
        or not doc.get("inputs")
        or any(fingerprint(p["path"]) != p for p in doc["inputs"])
    ):
        raise ValueError(
            "outlier preparation is stale or bound to another RT/FI cohort; existing files retained"
        )
    histogram, total = doc["histogram"], doc["cohort_count"]
    if (
        type(total) is not int
        or total < 0
        or not isinstance(histogram, dict)
        or any(
            not re.fullmatch(r"0|[1-9][0-9]*", k) or type(n) is not int or n < 0
            for k, n in histogram.items()
        )
        or sum(histogram.values()) != total
    ):
        raise ValueError("outlier histogram and cohort denominator disagree")
    candidates = doc.get("fi_candidate_count")
    fi_counts, _ = counts(fi.data)
    if (
        type(candidates) is not int
        or not 0 <= total <= candidates
        or candidates != sum(fi_counts.get(s, 0) for s in COUNTERS)
        or doc.get("rt_not_unclassified") != candidates - total
        or doc.get("zero_score_ips") != histogram.get("0", 0)
    ):
        raise ValueError("outlier candidate/cohort diagnostics disagree")
    combined = Counter()
    if set(doc.get("by_strategy", {})) != set(COUNTERS):
        raise ValueError("outlier FI-strategy histogram is incomplete")
    for cells in doc["by_strategy"].values():
        if any(k not in histogram or type(n) is not int or n < 0 for k, n in cells.items()):
            raise ValueError("invalid per-strategy outlier histogram")
        combined.update(cells)
    if +combined != +Counter(histogram):
        raise ValueError("outlier strategy histograms disagree with the total")
    for score in range(6):
        key = f"{PREFIX}.{score}.pct"
        values[key] = record(
            percentage(histogram.get(str(score), 0), total),
            "percent",
            values[key]["definition"],
            (path, *expected),
        )
    return values


def calculate(work):
    import numpy as np
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from ipid_analysis.deterministic_transition_analysis import greedy_outlier_scores
    from ipid_analysis.strategies import CLASSIFIER_VERSION, _batch_to_matrix, load_config
    from paper_values.paired import connection, unique

    cfg = load_config(work["snapshot"])
    for name in ("rt_labels", "fi_labels"):
        with pq.ParquetFile(work[name]) as p:
            if (p.schema_arrow.metadata or {}).get(
                b"classifier_version", b""
            ).decode() != CLASSIFIER_VERSION:
                raise ValueError("outlier labels do not match the production classifier")
            report = work["rt"] if name == "rt_labels" else work["fi"]
            if p.metadata.num_rows != report.data["total_ips"]:
                raise ValueError("outlier labels disagree with the selected strategy report")
    expected, _ = counts(work["fi"].data)
    histogram, by_strategy, seen = Counter(), {s: Counter() for s in COUNTERS}, set()
    timings, start = {}, perf_counter()
    with connection() as con:
        # Materialize the small FI cohort first, before any RT sequence lookup.
        con.execute(
            "CREATE TEMP TABLE fi AS SELECT IP_ADDR, CAST(IPID_SELECTION_STRATEGY AS VARCHAR) AS strategy FROM read_parquet(?) WHERE IPID_SELECTION_STRATEGY IN ('SINGLE','PER_DESTINATION','PER_BUCKET')",
            [str(work["fi_labels"])],
        )
        candidate_count = unique(con, "fi")
        actual = dict(con.execute("SELECT strategy, count(*) FROM fi GROUP BY 1").fetchall())
        if actual != {s: expected[s] for s in COUNTERS if expected.get(s, 0)}:
            raise ValueError("FI candidate counts differ from their canonical report")
        timings["fi_candidates_s"] = perf_counter() - start
        print(f"FI candidate IPs: {candidate_count:,}; reading only their RT labels", flush=True)
        mark = perf_counter()
        con.execute(
            "CREATE TEMP TABLE selected_rt AS SELECT r.IP_ADDR, CAST(r.IPID_SELECTION_STRATEGY AS VARCHAR) AS rt_strategy FROM read_parquet(?) r SEMI JOIN fi USING (IP_ADDR)",
            [str(work["rt_labels"])],
        )
        if unique(con, "selected_rt") != candidate_count:
            raise ValueError("missing or duplicate RT labels for FI candidates")
        con.execute(
            "CREATE TEMP TABLE cohort AS SELECT f.* FROM fi f JOIN selected_rt r USING (IP_ADDR) WHERE r.rt_strategy='UNCLASSIFIED'"
        )
        total = unique(con, "cohort")
        timings["rt_cohort_s"] = perf_counter() - mark
        print(
            f"RT-Unclassified cohort: {total:,}; excluded other RT labels: {candidate_count - total:,}",
            flush=True,
        )
        mark = perf_counter()
        if total:
            query = """SELECT r.IP_ADDR, c.strategy, list_transform(string_split(r.IPID_SEQUENCE, ','), x -> try_cast(trim(x) AS INTEGER)) AS ipids
                       FROM read_parquet(?) r JOIN cohort c USING (IP_ADDR)"""
            reader = con.execute(query, [str(work["raw"])]).to_arrow_reader(25000)
            for batch in reader:
                addresses = batch.column(0).to_pylist()
                if len(addresses) != len(set(addresses)) or seen.intersection(addresses):
                    raise ValueError("duplicate RT raw row for selected IP")
                seen.update(addresses)
                ids = batch.column(2)
                if pc.any(pc.is_null(ids)).as_py() or pc.any(pc.is_null(ids.flatten())).as_py():
                    raise ValueError("selected RT sequence contains a missing/non-numeric IP-ID")
                valid, matrix = _batch_to_matrix(ids, cfg.sequence_length)
                if not valid.all():
                    raise ValueError(
                        "selected RT sequence has missing/extra sample positions; cohort not silently reduced"
                    )
                strategies = np.asarray(batch.column(1).to_pylist())
                scores = greedy_outlier_scores(matrix, strategies, cfg)
                histogram.update(map(int, scores))
                for strategy in COUNTERS:
                    by_strategy[strategy].update(map(int, scores[strategies == strategy]))
                print(
                    f"RT sequences scored: {len(seen):,}/{total:,}; raw lookup + scoring {perf_counter() - mark:.1f}s",
                    flush=True,
                )
        if len(seen) != total:
            raise ValueError(
                "missing RT raw sequences; cohort denominator retained and publication refused"
            )
        timings["raw_lookup_and_scores_s"] = perf_counter() - mark
    return dict(
        cohort_count=total,
        fi_candidate_count=candidate_count,
        rt_not_unclassified=candidate_count - total,
        histogram=dict(sorted(histogram.items())),
        by_strategy={s: dict(sorted(c.items())) for s, c in by_strategy.items()},
        zero_score_ips=histogram[0],
        layout=dict(connections=cfg.connection_count, requests=cfg.requests_per_connection),
        timings=timings,
    )


def prepare(campaign, artifacts, processed, raw, *, dry_run=False):
    work = plan(artifacts, processed, raw)
    destination = cache_path(processed)
    if destination.exists():
        read_prepared(processed, campaign, artifacts)
        print(f"Outlier preparation: cache hit: {destination}", flush=True)
        return destination
    projection = footer(work["raw"])
    if projection["rows"] != work["rt"].data["total_ips"]:
        raise ValueError("RT raw row count differs from its canonical strategy report")
    expected, _ = counts(work["fi"].data)
    print(
        json.dumps(
            dict(
                fi_candidate_count=sum(expected.get(s, 0) for s in COUNTERS),
                raw_projection=projection,
            ),
            indent=2,
        ),
        flush=True,
    )
    if dry_run:
        return None
    before = [fingerprint(p) for p in work["inputs"]]
    result = calculate(work)
    if before != [fingerprint(p) for p in work["inputs"]]:
        raise ValueError("outlier inputs changed during preparation")
    document = dict(
        version=VERSION,
        campaign=campaign,
        code_sha256=code_identity(),
        inputs=before,
        reports={
            str(p.resolve()): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in work["inputs"][:2]
        },
        raw_projection=projection,
        **result,
    )
    from ipid_analysis.artifacts import publish_new_file, temporary_path

    stage = temporary_path(destination)
    try:
        stage.write_text(json.dumps(document, sort_keys=True, indent=2, allow_nan=False) + "\n")
        publish_new_file(
            stage, destination, inputs=work["inputs"], parameters={"calculation": VERSION}
        )
    finally:
        stage.unlink(missing_ok=True)
    print(
        f"Outlier preparation complete: {destination}; score-zero IPs: {result['zero_score_ips']}",
        flush=True,
    )
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True)
    parser.add_argument(
        "--repository-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    for option in ("reports-root", "processed-root", "raw-root"):
        parser.add_argument("--" + option, type=Path)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="inspect reports and Parquet footers only; no label/raw data scan or writes",
    )
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", args.campaign):
        parser.error("invalid campaign")
    scope = Path("continental") / args.campaign
    reports = args.reports_root or args.repository_root / "reports" / scope
    processed = args.processed_root or args.repository_root / "data/processed" / scope
    raw = args.raw_root or args.repository_root / "data/raw" / scope
    contexts = {
        ("icmp", "no-connection", phase, "strategies")
        for phase in ("rt-base", "fi-mass", "rt-fi-merged")
    }
    inputs = Inputs((reports, processed), contexts=contexts, sites=("eu",))
    try:
        artifacts, conflicts = select_artifacts(list(inputs.artifacts(args.campaign)))
        if inputs.errors or conflicts:
            raise ValueError(f"invalid canonical outlier reports: {inputs.errors + conflicts}")
        prepare(args.campaign, artifacts, processed, raw, dry_run=args.dry_run)
    finally:
        inputs.close()


if __name__ == "__main__":
    main()
