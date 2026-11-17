"""Export the explicit paper metrics for all six VPs; cache only derived values."""

import argparse
from functools import partial
import json
import math
import os
from pathlib import Path
import re
import tempfile
from time import perf_counter
import zipfile

from paper_values.cache import ValueCache, verify_file
from paper_values.diagnostics import Diagnostics
from paper_values.inputs import Inputs, select_artifacts
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
    identities = {(p.paper_path, p.size_bytes) for p in chosen}
    hashes = {p.receipt_sha256 for p in chosen if p.receipt_sha256}
    if len(identities) != 1 or len(hashes) > 1:
        raise ValueError(f"ambiguous PDF for {site}/{scope}")
    if any(p.error for p in chosen):
        raise ValueError("; ".join(p.error for p in chosen if p.error))
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
    additional_reports=(),
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
    input_roots, input_files = (
        (reports_root, processed_root, raw_root),
        (archive, *additional_reports),
    )
    output = output_path(root, "values.tex", input_roots, input_files)
    cache_path = output_path(root, "values.json", input_roots, input_files)
    definitions = load_metrics()
    metrics, sites = definitions["metrics"], definitions["sites"]
    contexts, plot_contexts = required_contexts(metrics)
    details = Diagnostics(enabled=diagnostics)
    inputs = Inputs(
        (reports_root, processed_root),
        archive,
        workers=io_workers,
        contexts=contexts,
        plot_contexts=plot_contexts,
        diagnostics=details,
    )
    try:
        artifacts, conflicts = select_artifacts(list(inputs.artifacts(campaign)))
        plots = list(inputs.plots(campaign))
    finally:
        inputs.close()
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
    readers = Readers(valid, processed_root, raw_root, cache, errors)
    if diagnostics:
        # Include snapshot I/O and parsing without changing calculation code or
        # its cache identity. Calls include the reader's memoized lookups.
        readers.snapshot = partial(details.call, "snapshot.access", readers.snapshot)
    values = {}
    derive_start = perf_counter()
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
                    scopes = (site,) if site is not None or "protocol" not in spec else sites
                    for scope in scopes:
                        raw, sources = (
                            choose_plot(spec, scope, plots, valid)
                            if spec["reader"] == "plot"
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
    serialized_cache = cache.serialize()
    atomic_text(output, tex)
    if use_cache:
        atomic_text(cache_path, serialized_cache)
    timings.update(write=perf_counter() - write_start, total=perf_counter() - started)
    for value in values.values():
        value["formatted"] = formatted(value["raw"], value["unit"])
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
    for name in ("reports-root", "processed-root", "raw-root", "archive"):
        parser.add_argument("--" + name, type=Path)
    parser.add_argument("--additional-report", type=Path, action="append", default=[])
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
        help="show wall-clock phases and operation counts/summed elapsed times",
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
        result = export(
            args.campaign,
            reports_root=roots["reports"],
            processed_root=roots["processed"],
            raw_root=roots["raw"],
            archive=args.archive,
            additional_reports=args.additional_report,
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
