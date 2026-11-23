"""Cache coverage of local controls from existing small classified tables."""

import json
from pathlib import Path

from paper_values.cache import fingerprint
from paper_values.paired import connection
from paper_values.review import percentage, read_json, record

VERSION = "local-reference-coverage-v1"


def path_for(processed_root, site):
    return Path(processed_root) / site / "paper-review" / (VERSION + ".json")


def read_prepared(processed_root, campaign):
    from paper_values.inputs import SITES

    values = {}
    for site in SITES:
        path = path_for(processed_root, site)
        if not path.is_file():
            continue
        data = read_json(path)
        if (data.get("version"), data.get("campaign"), data.get("site")) != (
            VERSION,
            campaign,
            site,
        ):
            raise ValueError("invalid prepared local-reference identity")
        if any(fingerprint(p["path"]) != p for p in data["inputs"]):
            raise ValueError("local-reference coverage is stale; rerun prepare-paper-review")
        for key, entry in data["values"].items():
            if not key.startswith(f"results.{site}.measurements.reference."):
                raise ValueError("invalid local-reference coverage key")
            raw = entry["raw"]
            if raw is not None and (type(raw) not in (int, float) or raw < 0):
                raise ValueError("invalid local-reference coverage value")
            values[key] = record(
                raw, entry["unit"], entry["definition"], (path, *entry["sources"])
            )
    return values


def prepare(campaign, processed_root, artifacts):
    from ipid_analysis.artifacts import publish_file, temporary_path
    from paper_values.inputs import SITES

    original = {
        (a.site, a.protocol): (a.data["zmap_ip_count"], Path(a.name))
        for a in artifacts
        if a.kind == "coverage" and a.phase == "rt-base" and a.connection == "no-connection"
    }
    for site in SITES:
        tables = []
        for protocol in ("icmp", "tcp", "udp"):
            for sample in ("base", "mass"):
                path = (
                    Path(processed_root)
                    / site
                    / "interprotocol/local-references"
                    / f"reference-{protocol}-{sample}/strategies.pq"
                )
                if path.is_file():
                    tables.append((protocol, sample, path))
        if not tables:
            continue
        dependencies = sorted(
            {p for _, _, p in tables}
            | {original[site, p][1] for p, _, _ in tables if (site, p) in original}
        )
        before = [fingerprint(path) for path in dependencies]
        destination = path_for(processed_root, site)
        if destination.is_file():
            data = read_json(destination)
            if (
                data.get("version") == VERSION
                and data.get("campaign") == campaign
                and data.get("inputs") == before
            ):
                print(f"Reference coverage: cache hit: {site}", flush=True)
                continue
        values = {}
        with connection() as con:
            for protocol, sample, path in tables:
                n, distinct, success, invalid = con.execute(
                    """
                    SELECT count(*), count(DISTINCT nullif(trim(IP_ADDR), '')),
                           count(*) FILTER (WHERE LOCAL_STRATEGY IS NOT NULL AND LOCAL_STRATEGY <> 'NOT_ENOUGH_SAMPLES'),
                           count(*) FILTER (WHERE LOCAL_STRATEGY IS NULL OR LOCAL_STRATEGY NOT IN
                             ('REFLECTION','CONSTANT','PER_DESTINATION','PER_CONNECTION','SINGLE','PER_BUCKET','MULTI','RANDOM','UNCLASSIFIED','NOT_ENOUGH_SAMPLES'))
                    FROM read_parquet(?)
                """,
                    [str(path)],
                ).fetchone()
                if n != distinct or invalid:
                    raise ValueError("duplicate/null local-reference targets or invalid strategy")
                zmap, source = original.get((site, protocol), (None, None))
                prefix = f"results.{site}.measurements.reference.{protocol}.{sample}"
                for key, raw, unit in (
                    ("targets.count", n, "count"),
                    ("success.count", success, "count"),
                    ("coverage.pct", percentage(success, n), "percent"),
                    ("zmap-total.count", zmap, "count"),
                ):
                    values[prefix + "." + key] = record(
                        raw,
                        unit,
                        "Frozen local-reference targets; enough samples includes UNCLASSIFIED; original ZMap count is protocol-specific",
                        (path, *([source] if source else [])),
                    )
        if [fingerprint(path) for path in dependencies] != before:
            raise ValueError("reference inputs changed during preparation")
        temporary = temporary_path(destination)
        try:
            temporary.write_text(
                json.dumps(
                    dict(
                        version=VERSION, campaign=campaign, site=site, inputs=before, values=values
                    ),
                    allow_nan=False,
                )
                + "\n"
            )
            publish_file(
                temporary, destination, inputs=dependencies, parameters={"calculation": VERSION}
            )
        finally:
            temporary.unlink(missing_ok=True)
        print(f"Reference coverage: prepared {site}", flush=True)
