"""Read compact published Anycast impact statistics without reopening per-IP data."""

from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import re

from paper_values.cache import fingerprint, verify_file

RULE = "max(AB across IPv4 protocols) > 3 OR max(GCD across IPv4 protocols) > 1"
CONTINENTS = {"NA": "na", "AS": "as", "EU": "eu", "SA": "sa", "AF": "af", "OC": "oc", "AN": "an"}


def record(raw, unit, definition, sources):
    return dict(
        raw=raw,
        unit=unit,
        definition=definition,
        sources=sorted(set(map(str, sources))),
        status="available",
        reason="",
    )


def validate(doc, receipt):
    census = doc["anycast_census"]
    start, end = (
        date.fromisoformat(census["period_start"]),
        date.fromisoformat(census["period_end"]),
    )
    if end < start or (end - start).days > 30:
        raise ValueError("invalid Anycast metadata period")
    expected = [(start + timedelta(days=n)).isoformat() for n in range((end - start).days + 1)]
    if (
        census.get("dates") != expected
        or census.get("missing_dates") != []
        or census.get("complete_period") is not True
        or census.get("expected_snapshots") != len(expected)
        or census.get("confidence") != "high-confidence"
        or census.get("filter") != RULE
        or not re.fullmatch(r"[a-f0-9]{40}", str(census.get("revision", "")))
        or [s.get("date") for s in census.get("snapshots", [])] != expected
        or census.get("manifest_sha256") != receipt["parameters"]["census_sha256"]
    ):
        raise ValueError("Anycast metadata requires the exact complete High-Confidence period")
    prefix_count = census["prefix_count"]
    if type(prefix_count) is not int or prefix_count <= 0:
        raise ValueError("invalid Census prefix count")
    stats = doc["anycast_filtering"]
    if stats.get("schema_version") != 2 or stats.get("prefix_length") != 24:
        raise ValueError("missing Anycast address/prefix impact metadata")
    groups = stats["continents"]
    if not isinstance(groups, dict) or not groups or set(groups) - {*CONTINENTS, "ZZ"}:
        raise ValueError("invalid Anycast continent groups")
    for row in (stats["total"], *groups.values()):
        for kind in ("addresses", "prefixes"):
            counts = row[kind]
            if set(counts) != {"original", "excluded", "retained"} or any(
                type(n) is not int or n < 0 for n in counts.values()
            ):
                raise ValueError("invalid Anycast impact counts")
            if counts["original"] != counts["excluded"] + counts["retained"]:
                raise ValueError("Anycast impact counts do not conserve the population")
        for state in ("original", "excluded", "retained"):
            if (
                not row["prefixes"][state]
                <= row["addresses"][state]
                <= 256 * row["prefixes"][state]
            ):
                raise ValueError("Anycast /24 count contradicts its address population")
        if row["prefixes"]["excluded"] > prefix_count:
            raise ValueError("affected prefixes exceed the Census prefix union")
    for state in ("original", "excluded", "retained"):
        if stats["total"]["addresses"][state] != sum(
            row["addresses"][state] for row in groups.values()
        ):
            raise ValueError("continent address counts do not sum to the overall population")
        prefixes = [row["prefixes"][state] for row in groups.values()]
        if not max(prefixes) <= stats["total"]["prefixes"][state] <= sum(prefixes):
            raise ValueError("global distinct prefixes contradict continent prefix counts")
    if stats["total"]["addresses"]["original"] <= 0:
        raise ValueError("empty original Anycast comparison population")
    return census, stats


def read(pdf, report, base_key):
    pdf, report = Path(pdf), Path(report)
    marker = pdf.parent / ".ipid-complete" / (pdf.name + ".json")
    receipt = json.loads(marker.read_text())
    if receipt["parameters"].get("impact_metadata_version") != 2:
        return {}, None  # Existing v1 PDFs retain their figure keys, without invented counts.
    metadata = pdf.with_suffix(".json")
    if not metadata.is_file() or metadata.stat().st_size > 1 << 20:
        raise ValueError(
            "published Anycast impact metadata is missing or too large; finish --refresh"
        )
    before = fingerprint(metadata)
    verify_file(metadata)
    payload = metadata.read_bytes()
    expected_name = report.stem.removesuffix("__full") + "__paper__anycast.json"
    inputs = [
        s
        for s in receipt.get("sources", [])
        if Path(s.get("path", "")).name == expected_name and "/anycast/" in s["path"]
    ]
    if (
        len(inputs) != 1
        or inputs[0].get("sha256") != hashlib.sha256(payload).hexdigest()
        or inputs[0].get("size") != len(payload)
    ):
        raise ValueError("Anycast impact metadata differs from the PDF's numerical input")
    doc = json.loads(payload)
    if doc.get("source_report_sha256") != hashlib.sha256(report.read_bytes()).hexdigest():
        raise ValueError("Anycast impact metadata belongs to another original report")
    census, stats = validate(doc, receipt)
    if fingerprint(metadata) != before:
        raise ValueError("Anycast impact metadata changed during export")
    full = base_key.endswith(".full")
    prefix = (
        "results."
        + base_key.removeprefix("fig.")
        .removesuffix(".full")
        .removesuffix(".intervals-by-continent")
        + ".anycast"
    )
    sources = [pdf, report, marker, metadata]
    values = {}
    for scope, row in [
        ("", stats["total"]),
        *(
            ("continent." + CONTINENTS[code] + ".", row)
            for code, row in stats["continents"].items()
            if code != "ZZ"
        ),
    ]:
        for kind in ("addresses", "prefixes"):
            denominator = row[kind]["original"]
            for state in ("original", "excluded", "retained"):
                key = f"{prefix}.{scope}{kind}.{state}.count" + (".full" if full else "")
                values[key] = record(
                    row[kind][state],
                    "count",
                    f"{state} unique {kind} in the stored comparison union; {scope or 'all continents including unlocated'}",
                    sources,
                )
                if state != "original" and denominator:
                    key = f"{prefix}.{scope}{kind}.{state}.pct" + (".full" if full else "")
                    values[key] = record(
                        100 * row[kind][state] / denominator,
                        "percent",
                        f"{state} {kind} / original {kind} in the same comparison/continent",
                        sources,
                    )
    unknown = (
        stats["continents"]
        .get("ZZ", {})
        .get("addresses", dict(original=0, excluded=0, retained=0))
    )
    for state, count in unknown.items():
        values[f"{prefix}.unlocated.{state}.count" + (".full" if full else "")] = record(
            count,
            "count",
            "comparison IPs without a continent assignment; included in global totals",
            sources,
        )
    values[f"{prefix}.unlocated.original.pct" + (".full" if full else "")] = record(
        100 * unknown["original"] / stats["total"]["addresses"]["original"],
        "percent",
        "original IPs without a continent assignment / all original comparison IPs",
        sources,
    )
    shared = dict(
        manifest_sha256=census["manifest_sha256"],
        period_start=census["period_start"],
        period_end=census["period_end"],
        snapshots=len(census["dates"]),
        expected_snapshots=census["expected_snapshots"],
        revision=census["revision"],
        prefix_count=census["prefix_count"],
    )
    return values, (shared, sources)


def global_values(shared, sources):
    values = {}
    for suffix, raw, unit in (
        ("start-date", shared["period_start"], "date"),
        ("end-date", shared["period_end"], "date"),
        ("snapshots.count", shared["snapshots"], "count"),
        ("expected-snapshots.count", shared["expected_snapshots"], "count"),
        ("complete-period", True, "boolean"),
        ("confidence", "high-confidence", "string"),
        ("revision", shared["revision"], "string"),
        ("rule", "AB > 3 OR GCD > 1", "string"),
    ):
        values["params.anycast.census." + suffix] = record(
            raw, unit, "Census metadata shared by the selected Anycast comparisons", sources
        )
    values["results.anycast.census.prefixes.count"] = record(
        shared["prefix_count"],
        "count",
        "distinct high-confidence /24s in the selected-period global Census union; not measured affected prefixes",
        sources,
    )
    return values
