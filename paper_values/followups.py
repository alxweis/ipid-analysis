"""Export pinned, already published follow-up summaries; never run an analysis."""

import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re

from paper_values.cache import verify_file
from paper_values.inputs import SITES, Artifact, identify, validate_plot_path
from paper_values.readers import Readers, counts, validate_report

COMPARISON = "connection/comparison/rt-based-base_fixed-interval-base"
MERGED = "no-connection/merged/rt-based-base_fixed-interval-mass"
# These are the six paper variants actually published by the one-off analysis.
FIGURES = {
    "tcp.conn.rt-base.itdk-role-by-strategy": (
        "full",
        "connection/rt-based-base/c-rt-b_itdk-role-by-strategy",
    ),
    "tcp.conn.rt-base.os-by-strategy": (
        "full",
        "connection/rt-based-base/c-rt-b_operating-system-group-by-strategy",
    ),
    "tcp.no-conn.rt-fi.phase-by-strategy-with-connection": (
        "full",
        MERGED + "/n-rt-b_fi-m_measurement-type-by-strategy-with-connection",
    ),
    "tcp.conn.rt-vs-fi-base.inc-distributions": (
        "paired",
        COMPARISON + "/c-rt-b_fi-b_increment-distributions",
    ),
    "tcp.conn.rt-vs-fi-base.intervals-by-continent": (
        "paired",
        COMPARISON + "/c-rt-b_fi-b_probing-intervals-by-continent",
    ),
    "tcp.conn.rt-vs-fi-base.strategy-intersection": (
        "paired",
        COMPARISON + "/c-rt-b_fi-b_strategy-intersection",
    ),
}


def registration(campaign):
    document = json.loads(Path(__file__).with_name("followups.json").read_text())
    if document.get("schema_version") != 1:
        raise ValueError("unsupported follow-up registrations")
    spec = document["campaigns"].get(campaign)
    if spec is not None:
        for field in ("measurement_run", "analysis_id"):
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", spec[field]):
                raise ValueError("invalid registered follow-up identity")
        if set(spec["zmap_ids"]) != set(SITES) or any(
            not re.fullmatch(r"tcp-80_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", run)
            for run in spec["zmap_ids"].values()
        ):
            raise ValueError("invalid registered follow-up discoveries")
    return spec


def definitions(metrics):
    # Only scalar JSON readers: no per-IP joins, histogram rebuilding or raw reads.
    return [
        metric
        for metric in metrics
        if metric["key"].startswith("results.{vp}.tcp.conn.rt-base.")
        and all(
            spec["reader"] in ("field", "strategy")
            and (spec.get("protocol"), spec.get("connection"), spec.get("phase"))
            == ("tcp", "connection", "rt-base")
            for spec in metric["sources"]
        )
    ]


def record(unit, definition, *, raw=None, sources=()):
    return dict(
        raw=raw,
        unit=unit,
        definition=definition,
        sources=list(sources),
        status="available" if raw is not None else "missing",
        reason="" if raw is not None else "registered full follow-up is not available",
    )


def empty_values(metrics):
    result = {}
    for site in SITES:
        for metric in definitions(metrics):
            result[metric["key"].format(vp=site) + ".full"] = record(
                metric["unit"], metric["definition"] + " (full SYN-ACK RT follow-up)"
            )
        for suffix, unit, definition in (
            ("rt-base.targets.count", "count", "All original SYN-ACK targets in the full RT run"),
            ("rt-base.coverage.pct", "percent", "Full RT successes / all full RT targets"),
            (
                "rt-vs-fi-base.paired-success.count",
                "count",
                "Jointly successful new RT / historical FI-BASE IPs",
            ),
        ):
            result[f"results.{site}.tcp.conn.{suffix}.full"] = record(unit, definition)
        for suffix in FIGURES:
            result[f"fig.{site}.{suffix}.full"] = record(
                "pdf-path", "Existing full follow-up PDF; paired comparisons use joint successes"
            )
    return result


def read_json(path, expected=None):
    if path.stat().st_size > 4 << 20:
        raise ValueError(f"expected a small follow-up report: {path}")
    payload = path.read_bytes()
    verify_file(path)
    if expected is not None and (
        len(payload) != expected["size_bytes"]
        or hashlib.sha256(payload).hexdigest() != expected["sha256"]
    ):
        raise ValueError(f"follow-up inventory differs from report: {path}")
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError(f"expected a follow-up JSON object: {path}")
    return data


def confined(root, relative):
    parts = PurePosixPath(relative).parts
    if not parts or PurePosixPath(relative).is_absolute() or any(p in (".", "..") for p in parts):
        raise ValueError("invalid follow-up artifact path")
    path = root.joinpath(*parts)
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("follow-up artifact resolves outside its registered directory")
    return path


def site_values(campaign, spec, site, done, root, metrics):
    """Read only the selected small JSON reports and PDF metadata, from one pinned run."""
    if any(
        done.get(k) != v
        for k, v in {
            "version": 1,
            "site": site,
            "analysis_id": spec["analysis_id"],
            "original_campaign": campaign,
            "measurement_run": spec["measurement_run"],
            "paired_population": "successful in both new RT and historical FI-BASE",
        }.items()
    ):
        raise ValueError(f"unexpected follow-up completion identity: {site}")
    measurement = done["full_measurement"]
    total, success, paired = (
        measurement["total_targets"],
        measurement["successful_ips"],
        done["paired_successful_ips"],
    )
    if (
        measurement.get("stage") != "full"
        or measurement.get("inflight") != 4000
        or not all(type(n) is int for n in (total, success, paired))
        or not 0 < paired <= success <= total
    ):
        raise ValueError(f"invalid full/paired population: {site}")
    anchor = f"/reports/followups/{spec['measurement_run']}/{spec['analysis_id']}/{site}/"
    inventory = {}
    for item in done["artifacts"]:
        name = item["path"]
        if not isinstance(name, str) or "\\" in name:
            raise ValueError("invalid follow-up inventory path")
        # Processed artifacts are deliberately never opened by this exporter.
        if anchor not in name:
            continue
        relative = name.split(anchor, 1)[1]
        confined(root, relative)
        if relative in inventory or not re.fullmatch(r"[a-f0-9]{64}", item["sha256"]):
            raise ValueError("duplicate or invalid follow-up inventory entry")
        inventory[relative] = item

    def report(relative):
        path = confined(root, relative)
        return read_json(path, inventory[relative]), str(path)

    zmap = spec["zmap_ids"][site]
    relative = f"full/figures/{zmap}/connection/rt-based-base/c-rt-b_strategies__full.json"
    data, source = report(relative)
    if data.get("measurement_id") != measurement["measurement_id"] or data.get("zmap_id") != zmap:
        raise ValueError("full strategy report differs from registered measurement")
    c, population = counts(data)
    if population - c.get("NOT_ENOUGH_SAMPLES", 0) != success:
        raise ValueError("full strategy population differs from completion")
    reports = []
    for kind in ("strategies", "itdk-role-by-strategy"):
        relative = f"full/figures/{zmap}/connection/rt-based-base/c-rt-b_{kind}__full.json"
        data, source = report(relative)
        canonical = f"figures/{zmap}/connection/rt-based-base/c-rt-b_{kind}.json"
        artifact = identify(Artifact(source, data, "", 0, "", site), canonical)
        validate_report(artifact)
        reports.append(artifact)
    readers = Readers(reports, None, None, None, [])
    result = {}
    for metric in definitions(metrics):
        proposals = [readers.read(recipe, site) for recipe in metric["sources"]]
        available = [(value, sources) for value, sources in proposals if value is not None]
        if available:
            if any(value != available[0][0] for value, _ in available):
                raise ValueError("conflicting full follow-up scalar")
            result[metric["key"].format(vp=site) + ".full"] = available[0]
    completion = str(root / "done__full.json")
    result.update(
        {
            f"results.{site}.tcp.conn.rt-base.targets.count.full": (total, (completion,)),
            f"results.{site}.tcp.conn.rt-base.coverage.pct.full": (
                100 * success / total,
                (completion,),
            ),
            f"results.{site}.tcp.conn.rt-vs-fi-base.paired-success.count.full": (
                paired,
                (completion,),
            ),
        }
    )
    for suffix, (population, stem) in FIGURES.items():
        relative = f"{population}/figures/{zmap}/{stem}__paper__full.pdf"
        entry = inventory[relative]
        path = confined(root, relative)
        if path.stat().st_size != entry["size_bytes"]:
            raise ValueError(f"follow-up PDF size differs from completion: {path}")
        verify_file(path)
        receipt = path.parent / ".ipid-complete" / (path.name + ".json")
        if receipt.is_file():
            if read_json(receipt).get("sha256") != entry["sha256"]:
                raise ValueError(f"follow-up PDF receipt differs from inventory: {path}")
        else:
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1 << 20), b""):
                    digest.update(block)
            if digest.hexdigest() != entry["sha256"]:
                raise ValueError(f"follow-up PDF differs from inventory: {path}")
        with path.open("rb") as stream:
            if stream.read(5) != b"%PDF-":
                raise ValueError(f"invalid follow-up PDF: {path}")
        member = (
            f"reports/followups/{spec['measurement_run']}/{spec['analysis_id']}/{site}/{relative}"
        )
        result[f"fig.{site}.{suffix}.full"] = (validate_plot_path(member), (str(path),))
    return result


def load(campaign, metrics, reports_root, *, archive=False, followups_root=None):
    spec = registration(campaign)
    if spec is None:
        return {}, []
    values = empty_values(metrics)
    if followups_root is None:
        # Do not implicitly mix an archive or alternate campaign root with live mounts.
        if archive or reports_root is None:
            return values, []
        campaign_root = Path(reports_root)
        if campaign_root.parts[-2:] != ("continental", campaign):
            return values, []
        followups_root = campaign_root.parent.parent / "followups"
    root = Path(followups_root) / spec["measurement_run"] / spec["analysis_id"]
    marker = root / "done__full.json"
    if not marker.is_file():
        return values, []  # Unmounted/unavailable follow-up: never guess another run.
    try:
        done = read_json(marker)
        if done.get("version") != 1 or done.get("analysis_id") != spec["analysis_id"]:
            raise ValueError("unexpected global follow-up completion identity")
        sites = done["sites"]
        if len(sites) != len(SITES) or {site["site"] for site in sites} != set(SITES):
            raise ValueError("follow-up completion does not contain all six sites")
        # Stage everything: a broken completion must not leak a partly exported run.
        staged = {}
        for site_done in sites:
            site = site_done["site"]
            site_root = confined(root, site)
            if read_json(site_root / "done__full.json") != site_done:
                raise ValueError("global and site follow-up completion disagree")
            staged.update(site_values(campaign, spec, site, site_done, site_root, metrics))
        for key, (raw, _) in staged.items():
            if values[key]["unit"] == "pdf-path":
                validate_plot_path(raw)
            elif (
                type(raw) not in (int, float)
                or isinstance(raw, float)
                and not math.isfinite(raw)
                or values[key]["unit"] == "percent"
                and not 0 <= raw <= 100
                or values[key]["unit"] == "count"
                and (type(raw) is not int or raw < 0)
            ):
                raise ValueError(f"invalid full follow-up scalar: {key}")
        for key, (raw, sources) in staged.items():
            values[key].update(raw=raw, sources=list(sources), status="available", reason="")
        return values, []
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return values, [{"source": str(marker), "error": str(exc)}]
