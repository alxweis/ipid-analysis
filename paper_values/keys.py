"""Public paper key names and layout; scientific input/cache identities stay intact."""

from dataclasses import replace
import re

VP = re.compile(r"(?:vp-)?(?:af|as|au|eu|na|sa)\Z")
SHARED = (
    (r"references\.vp-[a-z]{2}\.(itdk\.release|maxmind\.build-month)", r"refs.\1"),
    (
        r"classifier\.vp-[a-z]{2}\.(?:icmp|tcp|udp)\.random\.threshold",
        "classifier.random.threshold",
    ),
    # These campaign settings are shared by the three no-connection probes.
    # Keep phase: RT and FI settings have distinct meanings, even when equal.
    (
        r"parameters\.vp-[a-z]{2}\.(?:icmp|tcp|udp)\.no-connection\.(rt-base|fi-mass)\.(configured-sequence-length\.count|maximum-tolerated-rtt|minimum-required-replies\.count|fixed-interval\.request-interval)",
        r"params.\1.\2",
    ),
    (
        r"parameters\.vp-[a-z]{2}\.tcp\.connection\.rt-base\.connection-count",
        "params.tcp.conn.rt-base.connections.count",
    ),
)
SEGMENTS = {
    "references": "refs",
    "parameters": "params",
    "increments": "incs",
    "reports": "stats",
    "probing-comparison": "probing",
    "no-connection": "no-conn",
    "connection": "conn",
    "rt-fi-merged": "rt-fi",
    "rt-base-vs-fi-base": "rt-vs-fi-base",
    "rt-no-connection-to-rt-connection": "rt-no-conn-to-conn",
    "share-pct": "pct",
    "column-share-pct": "column-pct",
    "min-share-pct": "min-pct",
    "max-share-pct": "max-pct",
    "maximum-increment": "max-inc",
    "maximum-clusters": "max-clusters",
    "configured-sequence-length": "sequence-length",
    "maximum-tolerated-rtt": "reply-timeout",
    "minimum-required-replies": "min-replies",
    "common-prefix-length": "prefix-length",
    "successful-results": "success",
    "rt-results": "rt-pop",
    "increment-distributions": "inc-distributions",
    "probing-intervals-by-continent": "intervals-by-continent",
    "measurement-type-by-strategy": "phase-by-strategy",
    "measurement-type-by-strategy-with-connection": "phase-by-strategy-with-conn",
    "operating-system-group-by-strategy": "os-by-strategy",
}


def is_shared(key):
    return any(re.fullmatch(pattern, key) for pattern, _ in SHARED)


def public_key(key):
    """Shorten known redundant structures, never arbitrary identifiers or paths."""
    for pattern, replacement in SHARED:
        if re.fullmatch(pattern, key):
            key = re.sub(pattern, replacement, key)
            break
    key = key.replace("operating-system-group-by-strategy.groups.", "os.")
    if ".os." in key:
        key = key.replace(".strategies.", ".")
    key = key.replace("itdk-role-by-strategy.plot-percentages.", "itdk-strategy.")
    if ".itdk-strategy." in key and not key.endswith(".pct"):
        key += ".pct"
    key = key.replace(
        "itdk-consistency-summary.strict-agreement-percentage", "itdk.strict-agreement.pct"
    )
    key = key.replace(
        "itdk-role-by-os.coverage.transit-observed-os-resolved",
        "itdk-os.transit-observed.os-resolved.count",
    )
    key = key.replace(".fixed-interval.request-interval", ".request-interval")
    key = key.replace("validation.pipeline.", "validation.")
    key = key.replace("validation.datasets.", "validation.data.")
    parts = [
        SEGMENTS.get(p, p.removeprefix("vp-") if VP.fullmatch(p) else p) for p in key.split(".")
    ]
    key = ".".join(parts)
    for population in ("success", "rt-pop"):
        key = key.replace(f".{population}.total.count", f".{population}.count")
    return key


def export_values(values):
    """Coalesce declared shared settings; reject conflicting values/collisions."""
    grouped = {}
    for old, value in values.items():
        grouped.setdefault(public_key(old), []).append((old, value))
    result = {}
    for key, entries in grouped.items():
        if len(entries) > 1 and not all(is_shared(old) for old, _ in entries):
            raise ValueError(f"paper key collision: {key}")
        known = [v for _, v in entries if v.status == "available"]
        chosen = known[0] if known else entries[0][1]
        if any(v.raw != chosen.raw or v.unit != chosen.unit for v in known):
            names = ", ".join(old for old, v in entries if v.status == "available")
            raise ValueError(f"conflicting shared paper setting {key}: {names}")
        sources = tuple(sorted({s for _, v in entries for s in v.sources}))
        result[key] = replace(chosen, sources=sources)
    return result


THEMES = "campaign refs params classifier artifacts validation results claims stats constant incs probing tcp-transition plots".split()
MEANINGS = {
    "campaign": "measurement dates/identity",
    "refs": "reference datasets",
    "params": "probe configuration",
    "classifier": "classification settings",
    "artifacts": "artifact resource requirements",
    "validation": "validation datasets/results",
    "results": "result populations/class shares",
    "claims": "derived paper summaries",
    "stats": "statistics extracted from analysis reports",
    "constant": "constant IP-ID values",
    "incs": "positive IP-ID increment statistics",
    "probing": "matched-cohort RT/FI comparison",
    "tcp-transition": "paired TCP connection-state transitions",
    "plots": "relative PDF paths",
    "coverage": "probing coverage",
    "os": "OS detection",
    "interprotocol": "cross-protocol results",
    "comparison": "cross-VP comparisons",
    "applications": "application-specific metrics",
    "ambiguity": "ambiguous classifications",
    "meta": "export identity",
}


def key_order(key):
    parts = key.split(".")
    # Only the scope slots identify VPs. A geographic statistic may also have
    # an af/as/... token later in its metric path, referring to target locations.
    slots = (1, 2) if parts[0] == "comparison" else (1,)
    site_slots = {i for i in slots if i < len(parts) and VP.fullmatch(parts[i])}
    metric = tuple(p for i, p in enumerate(parts) if i not in site_slots)
    sites = tuple(p for i, p in enumerate(parts) if i in site_slots)
    theme = THEMES.index(parts[0]) if parts[0] in THEMES else len(THEMES)
    return theme, metric, sites


def tex_header(keys):
    prefixes = sorted({k.split(".")[0] for k in keys}, key=lambda p: key_order(p))
    lines = [
        "% Keys: topic.[VP].[protocol].[no-conn|conn].[phase].metric; optional dimensions only where needed.",
        "% VP: af/as/au/eu/na/sa; each metric lists VPs alphabetically. Blank values are unavailable, not zero.",
        "% rt-fi: RT/FI merge; rt-vs-fi-base: RT-BASE/FI-BASE comparison; success: enough samples, not necessarily classified.",
        "% rt-pop: original retained RT population; pct: percent (denominator remains metric-specific); count: count; p99: 99th percentile.",
    ]
    for i in range(0, len(prefixes), 2):
        lines.append(
            "% "
            + "; ".join(
                p + ": " + MEANINGS.get(p, "additional export metrics")
                for p in prefixes[i : i + 2]
            )
            + "."
        )
    return lines
