"""Export existing EU anycast-excluded PDFs; never prepare data or render plots."""

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re

from paper_values.cache import fingerprint, verify_file
from paper_values.inputs import validate_plot_path

KIND = "probing-intervals-by-continent"
KEYS = (
    "fig.eu.icmp.rt-vs-fi-base.intervals-by-continent",
    "fig.eu.tcp.no-conn.rt-vs-fi-base.intervals-by-continent",
    "fig.eu.udp.rt-vs-fi-base.intervals-by-continent",
    "fig.eu.tcp.conn.rt-vs-fi-base.intervals-by-continent.full",
)


def _variant(base, full):
    path = PurePosixPath(validate_plot_path(base))
    stem = f"{'c' if full else 'n'}-rt-b_fi-b_{KIND}"
    if full:
        if path.stem != stem + "__paper__full":
            raise ValueError("expected the registered Full TCP comparison PDF")
        return path.with_name(stem + "__paper__full__anycast.pdf"), stem + "__full.json"
    if path.stem not in (stem, stem + "__paper"):
        raise ValueError("expected the canonical stateless interval comparison PDF")
    return path.with_name(stem + "__paper__anycast.pdf"), stem + ".json"


def _validate(pdf, report, source_member):
    marker = pdf.parent / ".ipid-complete" / (pdf.name + ".json")
    if not marker.is_file():
        raise ValueError("anycast PDF is missing its publication receipt")
    for path, limit in ((pdf, 16 << 20), (report, 4 << 20), (marker, 1 << 20)):
        if path.stat().st_size > limit:
            raise ValueError("expected a small anycast PDF/report/receipt")
    if pdf.resolve().parent != report.resolve().parent:
        raise ValueError("anycast PDF resolves outside its source-report directory")
    before = [fingerprint(path) for path in (pdf, report, marker)]
    verify_file(pdf)
    verify_file(report)
    receipt = json.loads(marker.read_text(encoding="utf-8-sig"))
    parameters = receipt.get("parameters", {})
    if parameters.get("operation") != "paper-anycast-plot-v1" or any(
        not re.fullmatch(r"[a-f0-9]{64}", str(parameters.get(key, "")))
        for key in ("census_sha256", "profile_sha256")
    ):
        raise ValueError("PDF is not a completed anycast sensitivity plot")
    sources = receipt.get("sources", [])
    bound = [s for s in sources if str(s.get("path", "")).endswith("/" + source_member)]
    if len(bound) != 1:
        raise ValueError("anycast PDF belongs to another source report")
    source = bound[0]
    stat = report.stat()
    if source.get("size") != stat.st_size or (
        source.get("sha256") != hashlib.sha256(report.read_bytes()).hexdigest()
        if "sha256" in source
        else source.get("mtime_ns") != stat.st_mtime_ns
    ):
        raise ValueError("anycast PDF source report changed since rendering")
    data = json.loads(report.read_text(encoding="utf-8-sig"))
    if data.get("figure") != KIND:
        raise ValueError("anycast PDF source is not an interval comparison report")
    with pdf.open("rb") as stream:
        if stream.read(5) != b"%PDF-":
            raise ValueError("anycast figure has no PDF signature")
    if before != [fingerprint(path) for path in (pdf, report, marker)]:
        raise ValueError("anycast figure/source changed while exporting")
    return [str(pdf), str(report), str(marker)]


def load(values, *, campaign=None, archive=False):
    """Follow the already validated canonical/Full figure selection, add separate keys."""
    result, errors = {}, []
    if archive:
        return result, errors  # Never mix an archived campaign with live optional PDFs.
    for base_key in KEYS:
        entry = values.get(base_key, {})
        if not entry.get("raw"):
            continue
        full = base_key.endswith(".full")
        key = base_key.removesuffix(".full") + ".anycast" + (".full" if full else "")
        try:
            member, report_name = _variant(entry["raw"], full)
            # Sources come from the ordinary canonical selection or the validated
            # six-VP Full completion; do not scan other runs, VPs or the old sample.
            selected = [
                Path(p)
                for p in entry["sources"]
                if Path(p).name == PurePosixPath(entry["raw"]).name
            ]
            if not selected:
                continue
            original = selected[0]
            pdf = original.with_name(member.name)
            marker = pdf.parent / ".ipid-complete" / (pdf.name + ".json")
            if not os.path.lexists(pdf):
                if os.path.lexists(marker):
                    raise ValueError("anycast PDF receipt exists but its PDF is missing")
                continue
            report = original.with_name(report_name)
            source_member = str(member.with_name(report_name))
            sources = _validate(pdf, report, source_member)
            if full:
                from paper_values.followups import COMPARISON, read_json, registration
                from paper_values.full_review import published

                spec = registration(campaign)
                if spec is None:
                    raise ValueError("Anycast Full figure requires a registered follow-up")
                # The ordinary Full key already passed global/site completion checks.
                # Reuse the pinned inventory checker for this extra source report too.
                site_root = report.parents[6]
                complete = site_root / "done__full.json"
                relative = f"paired/figures/{spec['zmap_ids']['eu']}/{COMPARISON}/{report_name}"
                if published(site_root, read_json(complete), spec, "eu", relative) != report:
                    raise ValueError("Anycast Full report differs from registered paired source")
                sources.append(str(complete))
            result[key] = dict(
                raw=validate_plot_path(str(member)),
                unit="pdf-path",
                status="available",
                reason="",
                definition="EU probing-interval comparison with detected anycast IPv4 /24 prefixes excluded"
                + ("; paired Full RT and historical Fixed-Interval successes" if full else ""),
                sources=sources,
            )
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            errors.append({"source": key, "error": str(exc)})
    return result, errors
