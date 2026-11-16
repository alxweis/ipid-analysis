"""Reviewed current-manuscript selection, independent of any local paper checkout."""

import ast
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def classifier_constants(path):
    """Read a small allowlist of constant expressions; never import or execute source."""
    values = {}
    allowed = {
        "MODULUS",
        "CLASSIFIER_VERSION",
        "MIN_STEPS_BEFORE_WRAPAROUND",
        "MAX_INC",
        "MULTI_MAX_CLUSTERS",
        "MULTI_MAX_INC",
    }

    def read(node):
        if isinstance(node, ast.Constant) and type(node.value) in (str, int, float):
            return node.value
        if isinstance(node, ast.Name):
            return values[node.id]
        if isinstance(node, ast.BinOp):
            left, right = read(node.left), read(node.right)
            if isinstance(node.op, ast.LShift) and 0 <= right <= 64:
                return left << right
            if isinstance(node.op, ast.Div):
                return left / right
            if isinstance(node.op, ast.Sub):
                return left - right
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "math"
            and node.func.attr == "ceil"
            and len(node.args) == 1
            and not node.keywords
        ):
            return math.ceil(read(node.args[0]))
        raise ValueError("unsupported constant expression")

    try:
        source = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, UnicodeError):
        return values
    for node in source.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id in allowed
        ):
            try:
                values[node.targets[0].id] = read(node.value)
            except (KeyError, ValueError, TypeError, ArithmeticError):
                values.pop(node.targets[0].id, None)
    return values


def load_selection():
    data = json.loads(
        Path(__file__).with_name("selection.json").read_text(encoding="utf-8")
    )
    if data.get("schema_version") != 1:
        raise ValueError("unsupported current-paper selection")
    return data


def manuscript_metrics(registry, artifacts, processed_root, errors):
    """Only derived quantities cited in the current paper; no raw measurement rescan."""
    selection = load_selection()

    def read(key):
        value = registry.values.get(key)
        return value if value is not None and value.status == "available" else None

    def put(key, value, unit="percent", sources=(), definition="", reason=None):
        if not read(key):
            registry.put(
                key,
                value,
                unit=unit,
                sources=sources,
                definition=definition,
                reason=reason,
            )

    def ratio(key, numerator, denominator, sources, definition):
        put(
            key,
            100 * numerator / denominator if denominator else None,
            sources=sources,
            definition=definition,
            reason="denominator is zero",
        )

    sites = ("eu", "na", "sa", "as", "af", "au")
    for site in sites:
        site_artifacts = [a for a in artifacts if a.site == site]
        releases = {
            a.data["itdk_release"]
            for a in site_artifacts
            if a.kind == "itdk-consistency-summary" and a.data.get("itdk_release")
        }
        builds = {
            a.data["maxmind_provenance"]["build_epoch"]
            for a in site_artifacts
            if isinstance(a.data.get("maxmind_provenance"), dict)
            and type(a.data["maxmind_provenance"].get("build_epoch")) is int
        }
        if len(releases) == 1:
            put(
                f"references.vp-{site}.itdk.release",
                next(iter(releases)),
                "",
                tuple(
                    a.name
                    for a in site_artifacts
                    if a.kind == "itdk-consistency-summary"
                ),
                "Recorded, consistent ITDK release across available protocol reports",
            )
        if len(builds) == 1:
            month = datetime.fromtimestamp(
                next(iter(builds)), tz=timezone.utc
            ).strftime("%Y-%m")
            put(
                f"references.vp-{site}.maxmind.build-month",
                month,
                "",
                tuple(
                    a.name for a in site_artifacts if a.data.get("maxmind_provenance")
                ),
                "UTC build month from recorded MaxMind metadata",
            )
        starts = {}
        for a in site_artifacts:
            if a.phase == "rt-fi-merged" and a.data.get("zmap_id"):
                try:
                    start = datetime.strptime(
                        a.data["zmap_id"].rsplit("_", 2)[1], "%Y-%m-%d"
                    )
                except (ValueError, IndexError):
                    continue
                starts.setdefault(a.protocol, set()).add(start)
        if all(len(starts.get(p, set())) == 1 for p in ("icmp", "tcp", "udp")):
            month = min(next(iter(starts[p])) for p in ("icmp", "tcp", "udp")).strftime(
                "%Y-%m"
            )
            put(
                f"campaign.vp-{site}.start-month",
                month,
                "",
                tuple(a.name for a in site_artifacts if a.phase == "rt-fi-merged"),
                "Month of earliest original ZMap identifier; not an analysis-generation timestamp",
            )
        fractions = []
        for protocol in ("icmp", "tcp", "udp"):
            base = f"results.vp-{site}.{protocol}.no-connection"
            total = read(base + ".rt-fi-merged.rt-results.total.count")
            classified = read(base + ".rt-fi-merged.classified-results.total.count")
            if total and classified and total.raw:
                fractions.append(
                    (
                        100 * classified.raw / total.raw,
                        (*total.sources, *classified.sources),
                    )
                )
            fi = base + ".fi-mass.successful-results"
            denominator = read(fi + ".total.count")
            counts = [
                read(fi + "." + s + ".count")
                for s in ("single", "per-destination", "per-bucket")
            ]
            if denominator and all(counts):
                ratio(
                    f"claims.vp-{site}.{protocol}.no-connection.fi-mass."
                    "single-per-destination-per-bucket.share-pct",
                    sum(c.raw for c in counts),
                    denominator.raw,
                    tuple(s for c in [denominator, *counts] for s in c.sources),
                    "Recovered deterministic classes / successful FI-MASS IPs",
                )
            for mode in (
                ("no-connection", "connection")
                if protocol == "tcp"
                else ("no-connection",)
            ):
                for phase in ("rt-base", "fi-mass", "fi-base"):
                    prefix = f"parameters.vp-{site}.{protocol}.{mode}.{phase}"
                    connections, requests = (
                        read(prefix + ".connection-count"),
                        read(prefix + ".requests-per-connection"),
                    )
                    if (
                        connections
                        and requests
                        and type(connections.raw) is int
                        and type(requests.raw) is int
                    ):
                        length = connections.raw * requests.raw
                        put(
                            prefix + ".configured-sequence-length.count",
                            length,
                            "probes",
                            (*connections.sources, *requests.sources),
                            "Configured connections Ã— requests per connection",
                        )
                        rate = read(prefix + ".fixed-interval.minimum-reply-rate")
                        if (
                            phase == "fi-mass"
                            and rate
                            and isinstance(rate.raw, (int, float))
                            and 0 <= rate.raw <= 1
                        ):
                            put(
                                prefix + ".minimum-required-replies.count",
                                math.ceil(length * rate.raw),
                                "replies",
                                (
                                    *connections.sources,
                                    *requests.sources,
                                    *rate.sources,
                                ),
                                "Ceiling of configured length Ã— minimum reply rate",
                            )
                comparison = f"probing-comparison.vp-{site}.{protocol}.{mode}"
                rt_count = read(comparison + ".rt-successful.count")
                fi_count = read(
                    f"results.vp-{site}.{protocol}.{mode}.fi-base.successful-results.total.count"
                )
                if rt_count and fi_count:
                    ratio(
                        comparison + ".fi-to-rt-success.share-pct",
                        fi_count.raw,
                        rt_count.raw,
                        (*rt_count.sources, *fi_count.sources),
                        "Same selected FI-BASE target population in both probes",
                    )
        if len(fractions) == 3:
            put(
                f"claims.vp-{site}.all-protocols.classified-min.share-pct",
                min(n for n, _ in fractions),
                sources=tuple(s for _, sources in fractions for s in sources),
                definition="Minimum classified fraction across ICMP/TCP/UDP; denominator: retained RT results",
            )

    for a in artifacts:
        prefix = f"claims.vp-{a.site}.{a.protocol}.{a.connection}.{a.phase}"
        source = (a.name,)
        try:
            if a.kind == "operating-system-group-by-strategy":
                for family in ("rpm", "deb-rpm", "mikrotik-zyxel"):
                    for strategy in ("constant", "per-bucket", "per-destination"):
                        members = [
                            read(
                                f"reports.vp-{a.site}.{a.protocol}.{a.connection}.{a.phase}."
                                f"operating-system-group-by-strategy.groups.{group}.strategies.{strategy}.share-pct"
                            )
                            for group in selection["os_families"][family]
                        ]
                        if all(members):
                            for statistic, function in (("min", min), ("max", max)):
                                put(
                                    prefix
                                    + f".os.{family}.{strategy}.{statistic}-share-pct",
                                    function(v.raw for v in members),
                                    sources=tuple(
                                        s for v in members for s in v.sources
                                    ),
                                    definition=f"{statistic} among named OS groups: {', '.join(selection['os_families'][family])}",
                                )
            elif a.kind == "tcp-flags-by-strategy":
                for label in ("SYN-ACK", "RST"):
                    bar = a.data["bars"][label]
                    ratio(
                        prefix
                        + f".flags.{label.lower()}.single-per-bucket-random.share-pct",
                        sum(
                            bar["counts"].get(s, 0)
                            for s in ("SINGLE", "PER_BUCKET", "RANDOM")
                        ),
                        bar["total"],
                        source,
                        "SINGLE + PER_BUCKET + RANDOM / response-class bar total",
                    )
            elif a.kind == "itdk-role-by-os":
                rows = a.data.get("raw_distribution", [])
                totals = defaultdict(int)
                counts = defaultdict(int)
                for row in rows:
                    if type(row["count"]) is not int or row["count"] < 0:
                        raise ValueError("invalid ITDK OS composition counts")
                    totals[row["role"]] += row["count"]
                    counts[row["role"], row["os_group"]] += row["count"]
                for role in ("Transit-Observed", "No Transit Evidence"):
                    if role not in totals:
                        continue
                    for group in (
                        "deb",
                        "cisco",
                        "windows",
                        "huawei",
                        "rhel",
                        "centos",
                    ):
                        members = (
                            selection["os_families"]["deb"]
                            if group == "deb"
                            else [group]
                        )
                        ratio(
                            prefix
                            + f".itdk-os.{role.lower().replace(' ', '-')}.{group}.share-pct",
                            sum(counts[role, g] for g in members),
                            totals[role],
                            source,
                            "OS composition over the full resolved role population, before top-N grouping",
                        )
            elif (
                a.kind == "random-reproducibility-evaluation" and "threshold" in a.data
            ):
                put(
                    f"classifier.vp-{a.site}.{a.protocol}.random.threshold",
                    a.data["threshold"],
                    "",
                    source,
                    "Recorded production RANDOM score threshold, not the retired chi-square threshold",
                )
            elif a.kind == "missing-replies":
                for row in a.data.get("measurements", []):
                    if (
                        row.get("target")
                        == f"{a.protocol}.ipid.{a.connection}.fixed-interval.mass"
                    ):
                        base = f"parameters.vp-{a.site}.{a.protocol}.{a.connection}.fi-mass"
                        for source_field, target, unit in (
                            (
                                "sequence_length",
                                "configured-sequence-length.count",
                                "probes",
                            ),
                            (
                                "configured_minimum_replies",
                                "minimum-required-replies.count",
                                "replies",
                            ),
                        ):
                            if source_field in row:
                                put(
                                    base + "." + target,
                                    row[source_field],
                                    unit,
                                    source,
                                    "Persisted fixed-interval configuration",
                                )
            elif a.kind == "increment-distributions" and processed_root:
                _increment_histogram(registry, a, processed_root, put, ratio)
        except (OSError, ValueError, TypeError, KeyError, ImportError) as exc:
            errors.append(dict(source=a.name, error=f"current-paper derivation: {exc}"))

    # Constants belong to the recorded classifier version, not to the VP population.
    versions = [
        a
        for a in artifacts
        if a.kind == "random-reproducibility-evaluation"
        and a.data.get("classifier_version")
    ]
    if versions:
        path = Path(__file__).resolve().parents[1] / "ipid_analysis" / "strategies.py"
        constants = classifier_constants(path)
        version = constants.get("CLASSIFIER_VERSION")
        if version and all(
            str(a.data["classifier_version"]) == version for a in versions
        ):
            registry.record_file(path)
            sources = (str(path), *(a.name for a in versions))
            for metric, value in (
                ("single.maximum-increment", constants.get("MAX_INC")),
                ("per-bucket.maximum-increment", constants.get("MAX_INC")),
                ("multi.maximum-increment", constants.get("MULTI_MAX_INC")),
                ("multi.maximum-clusters", constants.get("MULTI_MAX_CLUSTERS")),
            ):
                put(
                    "classifier." + metric,
                    value,
                    "",
                    sources,
                    "Current implementation parameter; recorded classifier versions match "
                    + version,
                )
    truth = "validation.pipeline.rt-ideal.truth.multi"
    numerator, denominator = (
        read(truth + ".predicted.per-bucket.count"),
        read(truth + ".total.count"),
    )
    if numerator and denominator:
        ratio(
            truth + ".predicted.per-bucket.share-pct",
            numerator.raw,
            denominator.raw,
            (*numerator.sources, *denominator.sources),
            "MULTIâ†’PER_BUCKET / true synthetic MULTI sequences",
        )
    recall = read("validation.pipeline.fi-ideal.truth.multi.correct.share-pct")
    if recall:
        put(
            "validation.pipeline.fi-ideal.truth.multi.misclassified.share-pct",
            100 - recall.raw,
            sources=recall.sources,
            definition="100 minus synthetic MULTI recall",
        )
    recalls = [
        read(f"validation.pipeline.{scenario}.truth.{strategy}.correct.share-pct")
        for scenario in ("fi-lossy", "fi-reordered")
        for strategy in ("multi", "random")
    ]
    if all(recalls):
        put(
            "validation.pipeline.lossy-reordered.multi-random.minimum-recall.share-pct",
            min(v.raw for v in recalls),
            sources=tuple(s for v in recalls for s in v.sources),
            definition="Minimum over the four synthetic robustness recalls",
        )


def _increment_histogram(registry, artifact, processed_root, put, ratio):
    """Exact statistics where the retained full-count histogram still determines them."""
    relative = artifact.relative.removeprefix("figures/")
    path = Path(processed_root) / artifact.site / Path(relative).with_suffix(".pq")
    if not path.is_file():
        return
    registry.record_file(path)
    import pyarrow.parquet as pq

    with pq.ParquetFile(path) as source:
        if source.metadata.num_rows > 1_000_000:
            raise ValueError("expected a small increment plot histogram")
        rows = source.read(
            columns=[
                "MODE",
                "IPID_SELECTION_STRATEGY",
                "INCREMENT",
                "COUNT",
                "CUMULATIVE_COUNT",
                "TOTAL_COUNT",
            ]
        ).to_pylist()
    if any(
        r["INCREMENT"] <= 0 or r["COUNT"] < 0 or r["TOTAL_COUNT"] <= 0 for r in rows
    ):
        raise ValueError("expected positive-increment histogram counts")
    groups = defaultdict(list)
    for row in rows:
        groups[row["MODE"], row["IPID_SELECTION_STRATEGY"]].append(row)
    for (mode, strategy), group in groups.items():
        phase = {"RT-based": "rt-base", "Fixed-Interval": "fi-base"}.get(mode)
        if phase is None:
            raise ValueError("unknown histogram probing mode")
        group.sort(key=lambda r: r["INCREMENT"])
        totals = {r["TOTAL_COUNT"] for r in group}
        if len(totals) != 1:
            raise ValueError("increment histogram denominators disagree")
        total = totals.pop()
        key = f"increments.vp-{artifact.site}.{artifact.protocol}.{artifact.connection}.{phase}.{strategy.lower().replace('_', '-')}"
        cutoff = max(r["INCREMENT"] for r in group)
        retained = sum(r["COUNT"] for r in group)
        cumulative = 0
        for row in group:
            cumulative += row["COUNT"]
            if cumulative != row["CUMULATIVE_COUNT"]:
                raise ValueError("invalid cumulative increment histogram")
        sources = (artifact.name, str(path))
        p99 = next(
            (
                r["INCREMENT"]
                for r in group
                if r["CUMULATIVE_COUNT"] >= (99 * total + 99) // 100
            ),
            None,
        )
        if p99 is not None:
            put(
                key + ".p99",
                p99,
                "increments",
                sources,
                "Discrete 99th percentile of all positive strategy-selected increments",
            )
        for threshold in (1000, 2000):
            if retained == total or threshold <= cutoff + 1:
                ratio(
                    key + f".below-{threshold}.share-pct",
                    sum(r["COUNT"] for r in group if r["INCREMENT"] < threshold),
                    total,
                    sources,
                    f"Exact positive increments < {threshold}; full-count denominator",
                )


def select_manuscript_values(registry, sites):
    """No glob or automatic scalar discovery is allowed in the default output."""
    selection = load_selection()
    output = type(registry)()
    references = {}
    for entry in selection["entries"]:
        for site in sites if "{vp}" in entry["key"] else (None,):
            key = entry["key"].format(vp=site)
            if key in references:
                raise ValueError("duplicate manuscript selection: " + key)
            references[key] = entry["paper_references"]
            value = registry.values.get(key)
            if value and value.status == "available":
                output.put(
                    key,
                    value.raw,
                    unit=entry["unit"],
                    definition=value.definition or entry["definition"],
                    sources=value.sources,
                )
            else:
                output.reserve(
                    key,
                    unit=entry["unit"],
                    definition=entry["definition"],
                    reason=entry.get("missing_reason")
                    or (
                        value.reason
                        if value
                        else "required current-paper analysis report is unavailable"
                    ),
                )
    output.evidence = registry.evidence
    output.profile = "paper"
    output.paper_selection = {
        key: value for key, value in selection.items() if key != "entries"
    }
    output.paper_references = references
    return output
