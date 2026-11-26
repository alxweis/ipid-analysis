"""Export the explicit paper metrics for all six VPs; cache only derived values."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import json
import math
import os
from pathlib import Path
import re
import tempfile
from time import perf_counter
import zipfile

from paper_values import followups, review
from paper_values.cache import ValueCache, verify_file
from paper_values.diagnostics import Diagnostics
from paper_values.inputs import Inputs, select_artifacts
from paper_values.itdk_consistency import read_router_agreement
from paper_values.readers import Readers, ValueConflict, counts, validate_report
from paper_values.tex import formatted, render


def load_metrics():
    data = json.loads(Path(__file__).with_name("metrics.json").read_text(encoding="utf-8"))
    if data.get("schema_version") != 1 or data.get("sites") != [
        "af",
        "as",
        "au",
        "eu",
        "na",
        "sa",
    ]:
        raise ValueError("unsupported metric definitions")
    # Each group shares only a source context/unit. Its suffix is the public key.
    data["metrics"] = [
        dict(
            key=".".join(part for part in (group["prefix"], suffix) if part),
            unit=group["unit"],
            definition=value["definition"],
            reason=value.get("reason", "required analysis artifact is not available"),
            sources=[dict(source, **value.get("read", {})) for source in group["sources"]],
        )
        for group in data["metrics"]
        for suffix, value in group["values"].items()
    ]
    keys = [
        m["key"].format(vp=site)
        for m in data["metrics"]
        for site in data["sites"]
        if "{vp}" in m["key"] or site == data["sites"][0]
    ]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate paper metric definition")
    return data


def required_contexts(metrics):
    contexts, plots = set(), set()
    for metric in metrics:
        for spec in metric["sources"]:
            if "kind" in spec:
                context = tuple(spec[k] for k in ("protocol", "connection", "phase", "kind"))
                if spec["reader"] == "plot":
                    plots.add(".".join(context))
                else:
                    contexts.add(context)
    # Small primary reports preserve cohort checks and canonical-run selection.
    for protocol in ("icmp", "tcp", "udp"):
        for phase in ("rt-base", "fi-mass", "fi-base", "rt-fi-merged"):
            contexts.add((protocol, "no-connection", phase, "strategies"))
            contexts.add((protocol, "no-connection", phase, "coverage"))
        contexts.add((protocol, "no-connection", "fi-mass", "missing-replies"))
        contexts.add((protocol, "no-connection", "rt-fi-merged", "measurement-type-by-strategy"))
        contexts.add((protocol, "no-connection", "fi-mass", "random-reproducibility-evaluation"))
        contexts.add(
            (
                protocol,
                "no-connection",
                "rt-base-vs-fi-base",
                "probing-intervals-by-continent",
            )
        )
    for phase in ("rt-base", "fi-base"):
        contexts.add(("tcp", "connection", phase, "strategies"))
        contexts.add(("tcp", "connection", phase, "coverage"))
    contexts.add(("tcp", "connection", "rt-base-vs-fi-base", "probing-intervals-by-continent"))
    return contexts, plots


def prefetch_snapshots(readers, metrics, sites, workers):
    """Read each required snapshot once; surface failures at its metric lookup."""
    if readers.raw_root is None:
        return
    requests = sorted(
        {
            (site, spec["protocol"], spec["connection"], spec["phase"])
            for metric in metrics
            for spec in metric["sources"]
            if spec["reader"] == "snapshot"
            for site in sites
        }
    )
    with ThreadPoolExecutor(max_workers=workers) as pool:
        snapshots = {request: pool.submit(readers.snapshot, *request) for request in requests}
    # Futures retain both values and failures. Resolving them in the existing
    # metric loop preserves deterministic error attribution and strict handling.
    readers.snapshot = lambda *request: snapshots[request].result()


def check_populations(artifacts):
    indexed = {(a.site, a.protocol, a.connection, a.phase, a.kind): a for a in artifacts}
    for a in artifacts:
        if a.kind != "strategies":
            continue
        c, total = counts(a.data)
        if a.phase == "rt-fi-merged":
            rt = indexed.get((a.site, a.protocol, a.connection, "rt-base", "strategies"))
            if rt is not None and total != rt.data["total_ips"]:
                raise ValueError("merged population differs from RT results")
        else:
            coverage = indexed.get((a.site, a.protocol, a.connection, a.phase, "coverage"))
            if (
                coverage is not None
                and total - c.get("NOT_ENOUGH_SAMPLES", 0) != coverage.data["ipid_ip_count"]
            ):
                raise ValueError("strategy population differs from coverage report")


def choose_plot(spec, site, plots, artifacts):
    scope = ".".join(spec[k] for k in ("protocol", "connection", "phase", "kind"))
    candidates = [p for p in plots if p.site == site and p.scope == scope]
    reports = [a for a in artifacts if a.site == site and a.protocol == spec["protocol"]]
    canonical = {
        a.data["zmap_id"] for a in reports if a.phase == "rt-fi-merged" and a.data.get("zmap_id")
    }
    preferred = {
        run
        for a in reports
        if (a.connection, a.phase, a.kind) == (spec["connection"], spec["phase"], spec["kind"])
        for run in (a.run, a.data.get("measurement_id"))
    }
    chosen = (
        [p for p in candidates if p.run in canonical]
        or [p for p in candidates if p.run in preferred]
        or candidates
    )
    if not chosen:
        return None, ()
    # Canonical run selection comes first. Prefer adjusted > Final > paper > original for
    # each identity, preserving ambiguity between unrelated runs/paths.
    ranks = {}
    for plot in chosen:
        ranks[plot.original_path] = max(ranks.get(plot.original_path, 0), plot.variant_rank)
    chosen = [p for p in chosen if p.variant_rank == ranks[p.original_path]]
    identities = {(p.paper_path, p.size_bytes) for p in chosen}
    hashes = {p.receipt_sha256 for p in chosen if p.receipt_sha256}
    if len(identities) != 1 or len(hashes) > 1:
        raise ValueError(f"ambiguous PDF for {site}/{scope}")
    if any(p.error for p in chosen):
        raise ValueError("; ".join(p.error for p in chosen if p.error))
    for plot in chosen:
        if plot.variant_rank == 3:
            expected = {
                a.sha256
                for a in reports
                if (a.connection, a.phase, a.kind, a.run)
                == (spec["connection"], spec["phase"], spec["kind"], plot.run)
            }
            if plot.source_report_sha256 not in expected:
                raise ValueError("adjusted OS PDF source differs from its canonical report")
    return chosen[0].paper_path, tuple(p.name for p in chosen)


def put(values, key, raw, sources):
    """Only canonical, explicitly requested keys enter the value store."""
    if raw is None:
        return
    if (
        not isinstance(raw, (str, int, float, bool))
        or isinstance(raw, float)
        and not math.isfinite(raw)
    ):
        raise ValueError(f"{key}: expected a finite scalar")
    previous = values[key]
    if previous["unit"] in ("percent", "percentage-points") and type(raw) not in (int, float):
        raise ValueError(f"{key}: expected a numeric percentage")
    if previous["raw"] is not None and previous["raw"] != raw:
        raise ValueConflict(f"conflicting paper value: {key}")
    previous.update(
        raw=raw,
        status="available",
        reason="",
        sources=sorted(set(previous["sources"]) | set(sources)),
    )


def atomic_text(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".paper-values-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def output_path(root, filename, input_roots, input_files):
    path = root / "tmp" / filename
    resolved = path.resolve()
    if path.is_symlink():
        raise ValueError(f"tmp/{filename} must not be a symbolic link")
    for directory in [
        root / "reports",
        root / "data",
        root / "references",
        *(Path(p) for p in input_roots if p is not None),
    ]:
        if resolved == directory.resolve() or directory.resolve() in resolved.parents:
            raise ValueError("output resolves inside analysis inputs")
    if any(resolved == Path(p).resolve() for p in input_files if p is not None):
        raise ValueError("output aliases an input file")
    mountinfo = Path("/proc/self/mountinfo")
    if mountinfo.is_file():
        for line in mountinfo.read_text(encoding="utf-8").splitlines():
            mount, filesystem = line.split(" - ", 1)
            point = Path(mount.split()[4].replace("\\040", " "))
            if filesystem.split()[0].startswith("fuse") and (
                resolved == point or point in resolved.parents
            ):
                raise ValueError("output must be local, outside S3 mounts")
    return path


def export(
    campaign,
    *,
    reports_root,
    processed_root=None,
    archive=None,
    raw_root=None,
    validation_root=None,
    additional_reports=(),
    followups_root=None,
    repository_root=None,
    strict=False,
    recompute=False,
    io_workers=8,
    use_cache=True,
    diagnostics=False,
):
    started = perf_counter()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", campaign):
        raise ValueError("invalid campaign ID")
    if not 1 <= io_workers <= 32:
        raise ValueError("io_workers must be between 1 and 32")
    root = Path(repository_root or Path(__file__).resolve().parents[1]).resolve()
    if validation_root is None and archive is None:
        validation_root = root / "reports/figures/classifier-validation"
    input_roots, input_files = (
        (reports_root, processed_root, raw_root, validation_root, followups_root),
        (archive, *additional_reports),
    )
    output = output_path(root, "values.tex", input_roots, input_files)
    cache_path = output_path(root, "values.json", input_roots, input_files)
    definitions = load_metrics()
    metrics, sites = definitions["metrics"], definitions["sites"]
    contexts, plot_contexts = required_contexts(metrics)
    details = Diagnostics(enabled=diagnostics)
    details.progress("scanning campaign reports and figure metadata")
    inputs = Inputs(
        (reports_root, processed_root),
        archive,
        workers=io_workers,
        contexts=contexts,
        plot_contexts=plot_contexts,
        diagnostics=details,
    )
    try:
        all_artifacts = list(inputs.artifacts(campaign))
        artifacts, conflicts = select_artifacts(all_artifacts)
        plots = list(inputs.plots(campaign))
    finally:
        inputs.close()
    details.progress(f"inventory complete: {len(artifacts)} reports, {len(plots)} figures")
    errors = inputs.errors + conflicts
    valid = []
    for a in artifacts:
        try:
            validate_report(a)
            valid.append(a)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append({"source": a.name, "error": str(exc)})
    check_populations(valid)
    cache = details.call(
        "cache.load",
        ValueCache,
        cache_path,
        campaign,
        recompute=recompute,
        enabled=use_cache,
        diagnostics=details,
    )
    readers = Readers(valid, processed_root, raw_root, cache, errors, validation_root)
    from paper_values.adjusted_os import AdjustedOSReader

    adjusted_os = AdjustedOSReader(
        valid, campaign, reports_root, followups_root=followups_root, archive=archive is not None
    )
    derive_start = perf_counter()
    if diagnostics:
        # Include snapshot I/O and parsing without changing calculation code or
        # its cache identity; prefetch times one access per distinct request.
        readers.snapshot = partial(details.call, "snapshot.access", readers.snapshot)
    details.progress("reading measurement snapshots")
    details.call("snapshot.prefetch", prefetch_snapshots, readers, metrics, sites, inputs.workers)
    details.progress("extracting paper values")
    values = {}
    for metric in metrics:
        for site in sites if "{vp}" in metric["key"] else (None,):
            key = metric["key"].format(vp=site)
            values[key] = {
                "raw": None,
                "unit": metric["unit"],
                "definition": metric["definition"],
                "sources": [],
                "status": "missing",
                "reason": metric.get("reason", "required analysis artifact is not available"),
            }
            proposals = []
            try:
                for spec in metric["sources"]:
                    scopes = (
                        (spec["site"],)
                        if "site" in spec
                        else (site,)
                        if site is not None or "protocol" not in spec
                        else sites
                    )
                    for scope in scopes:
                        raw, sources = (
                            choose_plot(spec, scope, plots, valid)
                            if spec["reader"] == "plot"
                            else adjusted_os.read(spec, scope)
                            if spec["reader"] == "adjusted-os"
                            else read_router_agreement(spec, scope, readers)
                            if spec["reader"] == "itdk-router"
                            else readers.read(spec, scope)
                        )
                        if raw is not None:
                            proposals.append((raw, sources))
                for raw, sources in proposals:
                    put(values, key, raw, sources)
            except ValueConflict:
                raise
            except (OSError, ValueError, TypeError, KeyError, ImportError) as exc:
                # A conflicting/invalid recipe must not leak a partly registered value.
                values[key].update(raw=None, status="missing", sources=[])
                errors.append({"source": key, "error": str(exc)})
    followup_values, followup_errors = followups.load(
        campaign,
        metrics,
        reports_root,
        archive=archive is not None,
        followups_root=followups_root,
    )
    if values.keys() & followup_values.keys():
        raise ValueError("follow-up keys conflict with original campaign keys")
    values.update(followup_values)
    errors.extend(followup_errors)
    from paper_values.anycast_figures import load as load_anycast_figures

    anycast_values, anycast_errors = load_anycast_figures(
        values, campaign=campaign, archive=archive is not None
    )
    if values.keys() & anycast_values.keys():
        raise ValueError("Anycast figure keys conflict with existing values")
    values.update(anycast_values)
    errors.extend(anycast_errors)
    revision_values, revision_errors = review.load(
        valid,
        processed_root,
        reports_root,
        campaign,
        archive=archive is not None,
        raw_root=raw_root,
    )
    for key, entry in revision_values.items():
        if key in values:
            # Population exports extend existing success.count families. Reuse
            # those keys, but reject disagreement instead of silently replacing.
            compatible_units = {values[key]["unit"], entry["unit"]} <= {"count", "ips"}
            if values[key]["unit"] != entry["unit"] and not compatible_units:
                raise ValueError(f"revision unit conflicts with existing public key: {key}")
            put(values, key, entry["raw"], entry["sources"])
        else:
            values[key] = entry
    values.update(review.full_coverage_values(values))
    errors.extend(revision_errors)
    from paper_values.full_review import load as load_full_review

    full_review_values, full_review_errors = load_full_review(
        processed_root, campaign, archive=archive is not None
    )
    if values.keys() & full_review_values.keys():
        raise ValueError("Full review keys conflict with existing values")
    values.update(full_review_values)
    errors.extend(full_review_errors)
    from paper_values.outliers import read_prepared as read_outliers

    try:
        values.update(read_outliers(processed_root, campaign, valid, archive=archive is not None))
    except (OSError, ValueError, KeyError, TypeError) as exc:
        errors.append({"source": "EU ICMP RT outlier scores", "error": str(exc)})
    for path in additional_reports:
        verify_file(path)
        document = json.loads(Path(path).read_text(encoding="utf-8-sig"))
        if (
            not isinstance(document, dict)
            or document.get("schema_version") != 1
            or document.get("campaign_id") != campaign
            or not isinstance(document.get("values"), dict)
        ):
            raise ValueError(f"invalid additional metric report: {path}")
        for key, record in document["values"].items():
            if key not in values or not isinstance(record, dict) or not record.get("definition"):
                raise ValueError(
                    f"additional report requires a defined public key and definition: {key}"
                )
            if record.get("unit", values[key]["unit"]) != values[key]["unit"]:
                raise ValueError(f"additional report unit differs for {key}")
            put(values, key, record.get("value"), (str(path),))
    if strict and errors:
        raise ValueError(
            f"{len(errors)} invalid or ambiguous inputs; existing outputs retained\n"
            + "\n".join(f"{e['source']}: {e['error']}" for e in errors)
        )
    timings = {**inputs.timings, "derive": perf_counter() - derive_start}
    write_start = perf_counter()
    tex = render(values, campaign)
    # Preserve unrounded public scalars, including threshold-formatted ranges.
    cache.document["paper_values"] = {
        "campaign": campaign,
        "values": {key: value for key, value in values.items() if value["unit"] != "pdf-path"},
    }
    serialized_cache = cache.serialize()
    atomic_text(output, tex)
    if use_cache:
        atomic_text(cache_path, serialized_cache)
    timings.update(write=perf_counter() - write_start, total=perf_counter() - started)
    for key, value in values.items():
        value["formatted"] = formatted(value["raw"], value["unit"], key)
    return {
        "available": sum(v["raw"] is not None for v in values.values()),
        "missing": sum(v["raw"] is None for v in values.values()),
        "values": values,
        "input_errors": errors,
        "cache": dict(cache.stats),
        "timings": timings,
        "operations": details.summary(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True)
    for name in ("reports-root", "processed-root", "raw-root", "validation-root", "archive"):
        parser.add_argument("--" + name, type=Path)
    parser.add_argument("--additional-report", type=Path, action="append", default=[])
    parser.add_argument(
        "--followups-root",
        type=Path,
        help="existing reports/followups directory; auto-detected beside mounted campaign reports",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="reject invalid inputs; missing results stay empty",
    )
    parser.add_argument(
        "--recompute",
        action="store_true",
        help="recompute derived values instead of using values.json",
    )
    parser.add_argument("--io-workers", type=int, default=8)
    parser.add_argument(
        "--timings",
        action="store_true",
        help="show resources, live derived-operation progress and final timings",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    roots = {name: getattr(args, name + "_root") for name in ("reports", "processed", "raw")}
    if args.archive is None:
        for name, directory in (
            ("reports", "reports"),
            ("processed", "data/processed"),
            ("raw", "data/raw"),
        ):
            roots[name] = roots[name] or root / directory / "continental" / args.campaign
    try:
        if args.timings:
            from paper_values.paired import connection_config

            config = connection_config()
            print(
                f"Paired resources: memory={config['memory_limit']}, "
                f"threads={config['threads']}, file spilling=disabled",
                flush=True,
            )
        result = export(
            args.campaign,
            reports_root=roots["reports"],
            processed_root=roots["processed"],
            raw_root=roots["raw"],
            validation_root=args.validation_root,
            archive=args.archive,
            additional_reports=args.additional_report,
            followups_root=args.followups_root,
            strict=args.strict,
            recompute=args.recompute,
            io_workers=args.io_workers,
            diagnostics=args.timings,
        )
    except (OSError, ValueError, zipfile.BadZipFile) as exc:
        parser.exit(2, f"paper-values: {exc}\n")
    print(
        f"Paper values: {result['available']} available, {result['missing']} empty; {len(result['input_errors'])} input errors; output: {root / 'tmp/values.tex'}"
    )
    if args.timings:
        print("Timings: " + ", ".join(f"{k}={v:.3f}s" for k, v in result["timings"].items()))
        print("Derived-value cache: " + json.dumps(result["cache"], sort_keys=True))
        print("Operations (elapsed sums can exceed wall time with parallel I/O):")
        for name, entry in result["operations"].items():
            print(
                f"  {name}: calls={entry['calls']}, sum={entry['seconds']:.3f}s, max={entry['maximum']:.3f}s"
            )
    for error in result["input_errors"]:
        print(f"Unavailable input: {error['source']}: {error['error']}")
