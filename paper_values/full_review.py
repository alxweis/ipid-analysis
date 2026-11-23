"""Explicit preparation of pinned Full TCP scalars; ordinary export reads JSON only.

Historical Connection RT labels are never an input. Historical FI-BASE remains
the frozen SYN-ACK sample. Full-RT and paired-plot increment populations have
separate public keys.
"""

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
from types import SimpleNamespace

from paper_values import followups, paired, tcp_review
from paper_values.cache import fingerprint, verify_file
from paper_values.inputs import SITES
from paper_values.readers import classifier_constants, increment_histogram
from paper_values.review import percentage, read_json, record, tcp_defaults

VERSION = "tcp-full-review-v1"


def defaults():
    result = {
        key + ".full": record(None, "percent", value["definition"] + "; new Full RT run")
        for key, value in tcp_defaults().items()
        if ".tcp.conn." in key
    }
    for site in SITES:
        for family in ("rt-base", "rt-vs-fi-base.rt-base", "rt-vs-fi-base.fi-base"):
            for suffix, unit in (("per-bucket.p99", ""), ("single.below-2000.pct", "percent")):
                result[f"results.{site}.tcp.conn.{family}.increments.{suffix}.full"] = record(
                    None,
                    unit,
                    "Exact positive strategy-selected increments; "
                    + (
                        "complete Full RT population"
                        if family == "rt-base"
                        else "joint-success population of Full RT and historical FI-BASE, as in the paired plot"
                    ),
                )
        result[f"results.{site}.tcp.conn.fi-to-rt-success.pct.full"] = record(
            None,
            "percent",
            "Historical FI successes / Full RT successes within the same frozen FI target cohort",
        )
        result[
            f"results.{site}.tcp.rt-no-conn-to-conn.constant.per-connection.column-pct.full"
        ] = record(
            None,
            "percent",
            "SYN-based Constant / Full RT PER_CONNECTION IPs with sufficient samples in both separate runs",
        )
    return result


def code_identity():
    return hashlib.sha256(
        b"".join(
            Path(__file__).with_name(name).read_bytes()
            for name in ("full_review.py", "tcp_review.py", "paired.py", "readers.py")
        )
    ).hexdigest()


def cache_path(processed, site):
    return Path(processed) / site / "paper-review" / (VERSION + ".json")


def read_prepared(processed, campaign, *, sites=SITES):
    result = {}
    spec = followups.registration(campaign)
    if spec is None:
        return result
    expected = defaults()
    for site in sites:
        path = cache_path(processed, site)
        if not path.is_file():
            continue
        doc = read_json(path)
        keys = {k for k in expected if k.startswith(f"results.{site}.")}
        if (
            doc.get("version") != VERSION
            or doc.get("campaign") != campaign
            or doc.get("registration") != spec
            or doc.get("site") != site
            or doc.get("code_sha256") != code_identity()
            or set(doc.get("values", {})) != keys
        ):
            raise ValueError(
                "Full TCP review identity/code/keys changed; rerun prepare-paper-full"
            )
        for entry in doc["inputs"]:
            if fingerprint(entry["path"]) != entry:
                raise ValueError("Full TCP review is stale; rerun prepare-paper-full")
        staged = {}
        for key, value in doc["values"].items():
            raw, unit = value["raw"], expected[key]["unit"]
            if raw is not None and (
                type(raw) not in (int, float)
                or not math.isfinite(raw)
                or raw < 0
                or unit == "percent"
                and raw > 100
                and ".fi-to-rt-success." not in key
            ):
                raise ValueError("invalid prepared Full TCP scalar")
            staged[key] = record(raw, unit, expected[key]["definition"], (path, *value["sources"]))
        result.update(staged)
    return result


def load(processed, campaign, *, archive=False):
    if followups.registration(campaign) is None:
        return {}, []
    result = defaults()
    if archive or processed is None:
        return result, []
    try:
        result.update(read_prepared(processed, campaign))
        return result, []
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return defaults(), [dict(source="Full TCP review", error=str(exc))]


def published(root, done, spec, site, relative, area="reports"):
    """Map cross-machine inventory names into the caller's mounts, never trust absolute paths."""
    anchor = f"/{area}/followups/{spec['measurement_run']}/{spec['analysis_id']}/{site}/"
    matches = [
        a
        for a in done["artifacts"]
        if anchor in a["path"] and a["path"].split(anchor, 1)[1] == relative
    ]
    if len(matches) != 1:
        raise ValueError(f"missing/ambiguous pinned Full input: {area}/{relative}")
    path = followups.confined(Path(root), relative)
    entry = matches[0]
    if not re.fullmatch(r"[a-f0-9]{64}", str(entry.get("sha256", ""))):
        raise ValueError("invalid pinned Full input checksum")
    if path.stat().st_size != entry["size_bytes"]:
        raise ValueError(f"Full input differs from pinned inventory: {path}")
    verify_file(path)
    receipt = path.parent / ".ipid-complete" / (path.name + ".json")
    if not receipt.is_file() and path.stat().st_size > 32 << 20:
        raise ValueError(f"large published Full input lacks a completion receipt: {path}")
    if receipt.is_file() and read_json(receipt).get("sha256") != entry["sha256"]:
        raise ValueError(f"Full input receipt differs from pinned inventory: {path}")
    # Small reports/histograms can be checked fully. Do not reread multi-GB per-IP inputs.
    if (
        path.stat().st_size <= 32 << 20
        and hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]
    ):
        raise ValueError(f"Full input checksum differs from pinned inventory: {path}")
    return path


def full_increment_stats(path):
    """Exact bounded histogram: no quantile over a clipped tail or unbounded list sort."""
    with paired.connection() as con:
        rows = con.execute(
            """
            SELECT strategy, increment, count(*) FROM (
                SELECT CAST(IPID_SELECTION_STRATEGY AS VARCHAR) AS strategy,
                       unnest(INCREMENTS) AS increment FROM read_parquet(?)
                WHERE CAST(IPID_SELECTION_STRATEGY AS VARCHAR) IN ('SINGLE', 'PER_BUCKET')
            ) WHERE increment > 0 GROUP BY 1,2 ORDER BY 1,2
        """,
            [str(path)],
        ).fetchall()
    result = {}
    for strategy, metric in (
        ("PER_BUCKET", "per-bucket.p99"),
        ("SINGLE", "single.below-2000.pct"),
    ):
        histogram = [(v, n) for s, v, n in rows if s == strategy]
        if any(
            type(v) is not int or not 0 < v <= 65535 or type(n) is not int or n <= 0
            for v, n in histogram
        ):
            raise ValueError("invalid Full positive-increment histogram")
        total = sum(n for _, n in histogram)
        if strategy == "SINGLE":
            result[metric] = percentage(sum(n for v, n in histogram if v < 2000), total)
        else:
            cumulative, value = 0, None
            for increment, n in histogram:
                cumulative += n
                if cumulative >= (99 * total + 99) // 100:
                    value = increment
                    break
            result[metric] = value
    return result


def prepare_site(campaign, spec, site, done, report_root, processed_root, raw_root, artifacts):
    """Prepare one independent VP; an interrupted later VP does not invalidate earlier caches."""
    from ipid_analysis.artifacts import publish_file, temporary_path

    root = report_root / site
    if read_json(root / "done__full.json") != done:
        raise ValueError("global and site Full completion disagree")
    storage = Path(processed_root).parent.parent
    p_root = storage / "followups" / spec["measurement_run"] / spec["analysis_id"] / site
    zmap = spec["zmap_ids"][site]
    phase = f"{zmap}/connection/rt-based-base/c-rt-b_"
    dependencies = [report_root / "done__full.json", root / "done__full.json"]

    def pub(relative, area="processed"):
        path = published(p_root if area == "processed" else root, done, spec, site, relative, area)
        dependencies.append(path)
        return path

    report_path = pub("full/figures/" + phase + "strategies__full.json", "reports")
    rt = read_json(report_path)
    rt_labels = pub("full/" + phase + "strategies.pq")
    rt_increments = pub("full/" + phase + "increments.pq")
    pair_hist = pub(f"paired/{zmap}/{followups.COMPARISON}/c-rt-b_fi-b_increment-distributions.pq")
    histogram = increment_histogram(pair_hist)
    tail_inputs = {}
    for mode, directory, stem in (
        ("RT-based", "rt-based-base", "c-rt-b"),
        ("Fixed-Interval", "fixed-interval-base", "c-fi-b"),
    ):
        series = histogram.get(f"{mode}:SINGLE")
        if series is not None and "below-2000.share-pct" not in series:
            # The p99.9 display tail cannot establish this exact threshold.
            tail_inputs[mode] = pub(f"paired/{zmap}/connection/{directory}/{stem}_increments.pq")
    index = {
        (a.connection, a.phase, a.kind): a
        for a in artifacts
        if a.site == site and a.protocol == "tcp"
    }
    before = index["no-connection", "rt-base", "strategies"]
    fi = index["connection", "fi-base", "strategies"]
    coverage = index["connection", "fi-base", "coverage"]
    if any(a.data.get("zmap_id") != zmap for a in (before, fi)):
        raise ValueError("historical SYN/FI input differs from pinned Full discovery")

    def labels(a):
        path = tcp_review.source(
            processed_root,
            Path(site) / Path(a.relative.removeprefix("figures/")).with_suffix(".pq"),
        )
        dependencies.extend((Path(a.name), path))
        return path

    before_labels, fi_labels = labels(before), labels(fi)
    dependencies.append(Path(coverage.name))
    directory = Path(raw_root) / site / "zmap" / zmap
    target, meta = (directory / ("zmap-connection-sample" + ext) for ext in (".pq", ".json"))
    dependencies.extend((target, meta))
    from paper_values.selection import synack_metadata

    selection = synack_metadata(meta, zmap)
    if selection["eligible_rows"] != done["full_measurement"]["total_targets"]:
        raise ValueError("Full target count differs from original SYN-ACK eligibility")
    plan = None
    if site == "eu":
        import yaml

        mid = done["full_measurement"]["measurement_id"]
        if not isinstance(mid, str) or not re.fullmatch(r"tcp-80_[A-Za-z0-9_-]+", mid):
            raise ValueError("invalid pinned Full measurement identity")
        directory = (
            Path(raw_root).parent.parent
            / "followups"
            / spec["measurement_run"]
            / site
            / "full/ipid"
            / mid
        )
        raw, snapshot = directory / "ipid.pq", directory / "ipid.snapshot.yaml"
        completion_path = directory / "measurement-complete.json"
        completion = read_json(completion_path)
        if completion.get("version") != 1 or completion.get("measurement_id") != mid:
            raise ValueError("Full raw completion differs from pinned measurement")
        for path in (raw, snapshot):
            if path.stat().st_size != completion["files"][path.name]["size_bytes"]:
                raise ValueError("Full raw input size differs from completion")
        cfg = yaml.safe_load(snapshot.read_text())
        c, r = cfg["connection_count"], cfg["requests_per_connection"]
        if (
            type(c) is not int
            or c != 4
            or type(r) is not int
            or r < 2
            or cfg.get("tcp", {}).get("establish_connection") is not True
        ):
            raise ValueError("Full handshake requires four SYN-ACKs and TCP establishment")
        dependencies.extend((raw, snapshot, completion_path))
        plan = dict(
            mode="connection",
            report=SimpleNamespace(data=rt),
            labels=rt_labels,
            base=None,
            sources=[dict(raw=raw, snapshot=snapshot, width=c * r, skip=c, route=None)],
        )
    constants_path = Path(__file__).resolve().parents[1] / "ipid_analysis/strategies.py"
    dependencies.append(constants_path)
    dependencies = sorted(set(dependencies))
    for path in dependencies:
        verify_file(path)
    before_fingerprints = [fingerprint(path) for path in dependencies]
    destination = cache_path(processed_root, site)
    if destination.is_file():
        doc = read_json(destination)
        if (
            doc.get("inputs") == before_fingerprints
            and doc.get("code_sha256") == code_identity()
            and doc.get("registration") == spec
            and doc.get("campaign") == campaign
            and doc.get("site") == site
            and doc.get("version") == VERSION
        ):
            read_prepared(processed_root, campaign, sites=(site,))
            print(f"Full TCP review: cache hit: {destination}", flush=True)
            return destination
    expected = defaults()
    values, audit = {}, {}

    def put(key, value, sources):
        definition = expected[key]
        values[key] = record(value, definition["unit"], definition["definition"], sources)

    print(f"Full TCP review: preparing {site}", flush=True)
    for suffix, value in full_increment_stats(rt_increments).items():
        put(f"results.{site}.tcp.conn.rt-base.increments.{suffix}.full", value, (rt_increments,))
    for mode, family in (("RT-based", "rt-base"), ("Fixed-Interval", "fi-base")):
        tail = full_increment_stats(tail_inputs[mode]) if mode in tail_inputs else {}
        for suffix, strategy, stat in (
            ("per-bucket.p99", "PER_BUCKET", "p99"),
            ("single.below-2000.pct", "SINGLE", "below-2000.share-pct"),
        ):
            value = histogram.get(f"{mode}:{strategy}", {}).get(stat)
            fallback = value is None and suffix in tail
            put(
                f"results.{site}.tcp.conn.rt-vs-fi-base.{family}.increments.{suffix}.full",
                tail[suffix] if fallback else value,
                (pair_hist, tail_inputs[mode]) if fallback else (pair_hist,),
            )
    version = classifier_constants(constants_path)["CLASSIFIER_VERSION"]
    fi_rt = paired.fi_rt(
        [rt_labels, fi_labels, target, meta],
        rt,
        fi.data,
        coverage.data,
        version,
        connection_mode=True,
    )
    transition = paired.tcp_transition([before_labels, rt_labels], before.data, rt, version)
    put(
        f"results.{site}.tcp.conn.fi-to-rt-success.pct.full",
        fi_rt["pct"],
        (rt_labels, fi_labels, target, meta, coverage.name),
    )
    put(
        f"results.{site}.tcp.rt-no-conn-to-conn.constant.per-connection.column-pct.full",
        transition["pct"],
        (before_labels, rt_labels),
    )
    audit.update(fi_rt=fi_rt, transition=transition)
    if plan:
        found, audit["constant_handshake"] = tcp_review.calculate(plan)
        for key, value in found.items():
            put(key + ".full", value["raw"], value["sources"])
    if set(values) != {k for k in expected if k.startswith(f"results.{site}.")}:
        raise ValueError("incomplete prepared Full TCP key set")
    if [fingerprint(path) for path in dependencies] != before_fingerprints:
        raise ValueError("Full TCP inputs changed during calculation")
    document = dict(
        version=VERSION,
        campaign=campaign,
        registration=spec,
        site=site,
        code_sha256=code_identity(),
        inputs=before_fingerprints,
        values=values,
        audit=audit,
    )
    temporary = temporary_path(destination)
    try:
        temporary.write_text(json.dumps(document, indent=2, allow_nan=False) + "\n")
        publish_file(
            temporary, destination, inputs=dependencies, parameters={"calculation": VERSION}
        )
    finally:
        temporary.unlink(missing_ok=True)
    print(f"Full TCP review: prepared {destination}", flush=True)
    return destination


def prepare(campaign, reports_root, processed_root, raw_root):
    from paper_values.exporter import load_metrics, required_contexts
    from paper_values.inputs import Inputs, select_artifacts

    spec = followups.registration(campaign)
    if spec is None:
        raise ValueError("no pinned Full TCP follow-up registered for campaign")
    _, errors = followups.load(campaign, load_metrics()["metrics"], reports_root)
    root = (
        Path(reports_root).parent.parent
        / "followups"
        / spec["measurement_run"]
        / spec["analysis_id"]
    )
    if errors or not (root / "done__full.json").is_file():
        raise ValueError(f"pinned Full TCP completion unavailable/invalid: {errors}")
    contexts, plots = required_contexts(load_metrics()["metrics"])
    inputs = Inputs((reports_root, processed_root), contexts=contexts, plot_contexts=plots)
    try:
        artifacts, conflicts = select_artifacts(list(inputs.artifacts(campaign)))
        if inputs.errors or conflicts:
            raise ValueError(f"invalid canonical reports: {inputs.errors + conflicts}")
    finally:
        inputs.close()
    for done in read_json(root / "done__full.json")["sites"]:
        prepare_site(
            campaign,
            spec,
            done["site"],
            done,
            root,
            Path(processed_root),
            Path(raw_root),
            artifacts,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True)
    parser.add_argument(
        "--repository-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    for name in ("reports-root", "processed-root", "raw-root"):
        parser.add_argument("--" + name, type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", args.campaign):
        parser.error("invalid campaign")
    scope = Path("continental") / args.campaign
    prepare(
        args.campaign,
        args.reports_root or args.repository_root / "reports" / scope,
        args.processed_root or args.repository_root / "data/processed" / scope,
        args.raw_root or args.repository_root / "data/raw" / scope,
    )


if __name__ == "__main__":
    main()
