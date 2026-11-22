"""Small, read-only revision exports; raw TCP preparation is explicitly opt-in."""

from collections import Counter, defaultdict
import json
from pathlib import Path

from paper_values.cache import verify_file
from paper_values.inputs import SITES, component, validate_plot_path
from paper_values.readers import counts

STRATEGIES = (
    "REFLECTION",
    "CONSTANT",
    "PER_DESTINATION",
    "PER_CONNECTION",
    "SINGLE",
    "PER_BUCKET",
    "MULTI",
    "RANDOM",
    "UNCLASSIFIED",
    "NOT_ENOUGH_SAMPLES",
)
COUNTERS = ("PER_DESTINATION", "SINGLE", "PER_BUCKET")
GROUPS = ("icmp-tcp", "icmp-udp", "tcp-udp", "icmp-tcp-udp")
UNCHANGED = {
    "REFLECTION": ("SAME_PARAMETER", "PARTIALLY_MATCHING_PARAMETERS", "DIFFERENT_PARAMETERS"),
    "CONSTANT": ("SAME_PARAMETER", "PARTIALLY_MATCHING_PARAMETERS", "DIFFERENT_PARAMETERS"),
    "MULTI": ("SAME_INTER_STRATEGY", "DIFFERENT_INTER_STRATEGY"),
    "RANDOM": ("UNCHANGED",),
}
POPULATIONS = (
    ("icmp", "no-connection", "rt-fi-merged", "icmp.rt-fi"),
    ("udp", "no-connection", "rt-fi-merged", "udp.rt-fi"),
    ("tcp", "no-connection", "rt-fi-merged", "tcp.no-conn.rt-fi"),
    ("tcp", "connection", "rt-base", "tcp.conn.rt-base"),
)
TCP_VERSION = "tcp-constant-handshake-v1"


def record(raw, unit, definition, sources=()):
    return dict(
        raw=raw,
        unit=unit,
        definition=definition,
        sources=sorted(set(map(str, sources))),
        status="available" if raw is not None else "missing",
        reason="" if raw is not None else "required revision aggregate is unavailable",
    )


def percentage(n, d):
    return 100.0 * n / d if d else None


def read_json(path):
    if path.stat().st_size > 16 << 20:
        raise ValueError(f"expected a small revision report: {path}")
    verify_file(path)
    return json.loads(path.read_text(encoding="utf-8-sig"))


def tcp_defaults():
    result = {}
    for mode, phase in (("no-conn", "rt-fi"), ("conn", "rt-base")):
        prefix = f"results.eu.tcp.{mode}.{phase}.constant"
        for suffix, definition in (
            (
                "df0.pct",
                "Constant IPs whose classifier-used replies all have DF=0 / all Constant IPs",
            ),
            (
                "df1.pct",
                "Constant IPs whose classifier-used replies all have DF=1 / all Constant IPs",
            ),
            ("ipid-zero.pct", "Constant IPs with constant IPID=0 / all Constant IPs"),
        ):
            result[f"{prefix}.{suffix}"] = record(None, "percent", definition)
    result["results.eu.tcp.conn.rt-base.per-connection.handshake.constant.pct"] = record(
        None,
        "percent",
        "PER_CONNECTION IPs with four equal SYN-ACK IPIDs in this same connection RT run / all PER_CONNECTION IPs in that run",
    )
    return result


def vp_values(artifacts):
    index = {
        (a.site, a.protocol, a.connection, a.phase): a for a in artifacts if a.kind == "strategies"
    }
    result = {}
    for protocol, mode, phase, family in POPULATIONS:
        for strategy in STRATEGIES:
            shares, sources = [], []
            for site in SITES:
                artifact = index.get((site, protocol, mode, phase))
                value = None
                if artifact:
                    population, total = counts(artifact.data)
                    value = percentage(population.get(strategy, 0), total)
                    sources.append(artifact.name)
                result[f"results.{site}.{family}.distribution.{component(strategy)}.pct"] = record(
                    value,
                    "percent",
                    "Strategy share in the existing plot population, including insufficient/unclassified categories",
                    (artifact.name,) if artifact else (),
                )
                shares.append(value)
            complete = all(value is not None for value in shares)
            for stat in ("min", "max", "range"):
                raw = (
                    (
                        max(shares) - min(shares)
                        if stat == "range"
                        else min(shares)
                        if stat == "min"
                        else max(shares)
                    )
                    if complete
                    else None
                )
                result[
                    f"results.vp-comparison.{family}.{component(strategy)}.{stat}.{'pp' if stat == 'range' else 'pct'}"
                ] = record(
                    raw,
                    "percentage-points" if stat == "range" else "percent",
                    f"{stat} of unrounded shares across all six VPs; never use a partial VP set",
                    sources,
                )
    return result


def coverage_values(artifacts, canonical):
    """All published measurement IDs, not only canonical paper runs. No bulk reads."""
    result = {}
    original = {}
    for a in canonical:
        if a.kind == "coverage" and a.phase == "rt-base" and a.connection == "no-connection":
            original[a.site, a.protocol] = (a.data["zmap_ip_count"], a.name)
    by_directory = defaultdict(dict)
    for a in artifacts:
        if a.kind in ("strategies", "coverage"):
            key = (a.site, str(Path(a.relative).parent), Path(a.relative).stem.rsplit("_", 1)[0])
            previous = by_directory[key].get(a.kind)
            if previous and previous.data != a.data:
                # Identical metadata can be published in reports and processed.
                if a.kind == "coverage" or counts(previous.data) != counts(a.data):
                    raise ValueError(f"conflicting per-measurement population: {a.name}")
            by_directory[key][a.kind] = a
    seen = {}
    for group in by_directory.values():
        a = group.get("strategies")
        if not a or a.phase == "rt-fi-merged" or not a.data.get("measurement_id"):
            continue
        mid = a.data["measurement_id"]
        if not isinstance(mid, str) or Path(mid).name != mid:
            raise ValueError("invalid coverage measurement identity")
        population, total = counts(a.data)
        success = total - population.get("NOT_ENOUGH_SAMPLES", 0)
        coverage = group.get("coverage")
        targets = coverage.data["zmap_ip_count"] if coverage else None
        if targets is not None and (type(targets) is not int or targets < 0):
            raise ValueError(f"invalid selected-target count: {mid}")
        if targets is not None and not 0 <= success <= targets:
            raise ValueError(f"successful population exceeds selected targets: {mid}")
        zmap, source = original.get((a.site, a.protocol), (None, None))
        values = dict(
            zmap=zmap,
            targets=targets,
            successful=success,
            coverage=percentage(success, targets) if targets is not None else None,
        )
        identity = (a.site, a.protocol, a.run, a.connection, a.phase, mid)
        if identity in seen and seen[identity] != values:
            raise ValueError(f"conflicting coverage aliases for {mid}")
        seen[identity] = values
        sources = [a.name] + ([coverage.name] if coverage else []) + ([source] if source else [])
        mode = "conn" if a.connection == "connection" else "no-conn"
        prefix = f"results.{a.site}.measurements.{a.protocol}.{mode}.{a.phase}.{component(a.run or mid)}.{component(mid)}"
        for suffix, value in values.items():
            key = {
                "zmap": "zmap-total.count",
                "targets": "targets.count",
                "successful": "success.count",
                "coverage": "coverage.pct",
            }[suffix]
            result[f"{prefix}.{key}"] = record(
                value,
                "percent" if suffix == "coverage" else "count",
                {
                    "zmap": "Original protocol discovery population at this VP, not this phase's selected targets",
                    "targets": "Frozen selected targets for this measurement",
                    "successful": "Enough samples under this phase's eligibility rules; UNCLASSIFIED included",
                    "coverage": "100 * sufficiently sampled IPs / selected targets",
                }[suffix],
                sources,
            )
    return result


def find_summary(processed_root, site="eu"):
    if processed_root is None:
        return None
    paths = sorted(
        (Path(processed_root) / site / "interprotocol").glob(
            "*/runs/*/interprotocol-campaign-summary.json"
        )
    )
    if len(paths) > 1:
        raise ValueError(
            "ambiguous EU interprotocol runs; select one campaign analysis before exporting"
        )
    return paths[0] if paths else None


def interprotocol_coverage(processed_root, canonical):
    """Version-2 runs retain every selected target, including zero-reply rows."""
    original = {
        (a.site, a.protocol): (a.data["zmap_ip_count"], a.name)
        for a in canonical
        if a.kind == "coverage" and a.phase == "rt-base" and a.connection == "no-connection"
    }
    values = {}
    for site in SITES:
        path = find_summary(processed_root, site)
        if path is None:
            continue
        summary = validate_summary(read_json(path))
        for group, measurements in summary.get("measurements", {}).items():
            if not isinstance(measurements, dict):
                raise ValueError("expected separate base/mass interprotocol measurement IDs")
            for sample, mid in measurements.items():
                if (
                    sample not in ("base", "mass")
                    or not isinstance(mid, str)
                    or Path(mid).name != mid
                ):
                    raise ValueError("invalid interprotocol measurement identity")
                entries = summary["groups"][group]["counts"]
                selected = {
                    k: v
                    for k, v in entries.items()
                    if (k.split(":")[0] in ("MULTI", "RANDOM")) == (sample == "mass")
                }
                total = sum(selected.values())
                success = total - sum(
                    v for k, v in selected.items() if k.endswith(":NOT_ENOUGH_SAMPLES")
                )
                prefix = f"results.{site}.measurements.interprotocol.{component(mid)}"
                for suffix, raw, unit in (
                    ("targets.count", total, "count"),
                    ("success.count", success, "count"),
                    ("coverage.pct", percentage(success, total), "percent"),
                ):
                    values[prefix + "." + suffix] = record(
                        raw,
                        unit,
                        "Interprotocol measurement: enough samples in every participating protocol / all selected IPs",
                        (path,),
                    )
                for protocol in group.split("-"):
                    n, source = original.get((site, protocol), (None, None))
                    values[f"{prefix}.zmap.{protocol}.count"] = record(
                        n,
                        "count",
                        "Original discovery for this participating protocol; no sum across protocols",
                        (source,) if source else (),
                    )
    return values


def validate_summary(data):
    if data.get("version") != 2 or set(data.get("groups", {})) - set(GROUPS):
        raise ValueError("expected version-2 interprotocol outcomes")
    total = 0
    for group, row in data["groups"].items():
        for key, n in row["counts"].items():
            strategy, outcome = key.split(":")
            allowed = (
                ("PROTOCOL_SHARED", "PARTIAL_PROTOCOL_SHARED", "PROTOCOL_ISOLATED")
                if strategy in COUNTERS
                else UNCHANGED.get(strategy, ())
            )
            if strategy not in (*COUNTERS, *UNCHANGED) or outcome not in (
                *allowed,
                "STRATEGY_CHANGED",
                "NOT_ENOUGH_SAMPLES",
            ):
                raise ValueError("unknown interprotocol strategy/outcome")
            if (
                type(n) is not int
                or n < 0
                or (outcome == "PARTIAL_PROTOCOL_SHARED" and group != "icmp-tcp-udp")
            ):
                raise ValueError("invalid interprotocol count")
        n = sum(row["counts"].values())
        if row.get("rows", n) != n:
            raise ValueError("interprotocol group total differs from outcomes")
        total += n
    if data.get("rows", total) != total:
        raise ValueError("interprotocol campaign total differs from groups")
    return data


def interprotocol_values(path):
    summary = validate_summary(read_json(path))
    result = {}
    for strategy in (*COUNTERS, *UNCHANGED):
        family = f"results.eu.interprotocol.{component(strategy)}"
        totals = {}
        for group in GROUPS:
            entries = {
                k.split(":")[1]: v
                for k, v in summary["groups"].get(group, {}).get("counts", {}).items()
                if k.startswith(strategy + ":")
            }
            total = totals[group] = sum(entries.values())
            prefix = f"{family}.{group}"
            result[prefix + ".targets.count"] = record(
                total,
                "count",
                "IPs within this baseline/protocol cohort; groups are not global unique IPs",
                (path,),
            )
            if strategy in COUNTERS:
                for outcome in (
                    "PROTOCOL_SHARED",
                    "PARTIAL_PROTOCOL_SHARED",
                    "PROTOCOL_ISOLATED",
                    "STRATEGY_CHANGED",
                    "NOT_ENOUGH_SAMPLES",
                ):
                    for stat, value, unit in (
                        ("count", entries.get(outcome, 0), "count"),
                        ("pct", percentage(entries.get(outcome, 0), total), "percent"),
                    ):
                        result[f"{prefix}.outcome.{component(outcome)}.{stat}"] = record(
                            value,
                            unit,
                            "UpSet outcome; percentage denominator is this entire baseline/protocol cohort",
                            (path,),
                        )
            else:
                unchanged = sum(entries.get(k, 0) for k in UNCHANGED[strategy])
                for stat, value, unit in (
                    ("count", unchanged, "count"),
                    ("pct", percentage(unchanged, total), "percent"),
                ):
                    result[f"{prefix}.consistent.{stat}"] = record(
                        value,
                        unit,
                        "All individual protocol classes unchanged; parameters/joint Multi fit may differ; denominator includes insufficient replies",
                        (path,),
                    )
        if strategy not in COUNTERS:
            continue
        for protocol in ("icmp", "tcp", "udp"):
            result[f"{family}.set.{protocol}.count"] = record(
                sum(n for g, n in totals.items() if protocol in g.split("-")),
                "count",
                "Protocol-set size displayed by the UpSet cohort plot",
                (path,),
            )
        alt_path = path.parent / (
            "interprotocol-campaign-" + component(strategy) + "-alternatives.json"
        )
        alternative = read_json(alt_path)
        if alternative.get("baseline_strategy") != strategy or not isinstance(
            alternative.get("alternatives"), list
        ):
            raise ValueError("unexpected Changed detail report")
        changed = Counter()
        for row in alternative["alternatives"]:
            group, observed, n = row["protocol_group"], row["observed"], row["count"]
            if (
                group not in GROUPS
                or set(observed) != set(group.split("-"))
                or set(observed.values()) - set(STRATEGIES)
                or type(n) is not int
                or n < 0
            ):
                raise ValueError("invalid Changed combination")
            changed[group, tuple(observed[p] for p in group.split("-"))] += n
        for group in GROUPS:
            denominator = (
                summary["groups"]
                .get(group, {})
                .get("counts", {})
                .get(strategy + ":STRATEGY_CHANGED", 0)
            )
            if sum(n for (g, _), n in changed.items() if g == group) != denominator:
                raise ValueError("Changed details disagree with main outcome")
            for (g, labels), n in sorted(changed.items()):
                if (
                    g == group and 100 * n > denominator
                ):  # strictly >1%, after pooling hidden state fields
                    combination = ".".join(
                        f"{p}-{component(label)}" for p, label in zip(g.split("-"), labels)
                    )
                    result[f"{family}.{g}.changed.{combination}.pct"] = record(
                        percentage(n, denominator),
                        "percent",
                        "Share of Changed cases in this baseline/group; only pooled combinations strictly above 1%; UNCLASSIFIED retained",
                        (path, alt_path),
                    )
        relations = Counter()
        for row in summary.get("partial_relationships", []):
            if row["baseline"] != strategy or row["outcome"] != "PARTIAL_PROTOCOL_SHARED":
                continue
            group, pairs, partitions, n = (
                row["protocol_group"],
                tuple(sorted(row["shared_pairs"])),
                tuple(sorted(row["compatible_partitions"])),
                row["count"],
            )
            allowed = {"icmp-tcp", "icmp-udp", "tcp-udp"}
            if (
                group != "icmp-tcp-udp"
                or not pairs
                or not partitions
                or set(pairs) - allowed
                or set(partitions) - set(pairs)
                or len(pairs) != len(set(pairs))
                or type(n) is not int
                or n < 0
            ):
                raise ValueError("invalid Partial relationship")
            relations[group, pairs, partitions] += n
        denominator = (
            summary["groups"]
            .get("icmp-tcp-udp", {})
            .get("counts", {})
            .get(strategy + ":PARTIAL_PROTOCOL_SHARED", 0)
        )
        if sum(relations.values()) != denominator:
            raise ValueError("Partial details disagree with main outcome")
        for (group, pairs, partitions), n in sorted(relations.items()):
            identity = "shared-" + "-and-".join(pairs) + ".partitions-" + "-or-".join(partitions)
            result[f"{family}.{group}.partial.{identity}.pct"] = record(
                percentage(n, denominator),
                "percent",
                "Share of Partial cases; complete compatible-pair sets are exclusive rows, not independent pair counts",
                (path,),
            )
    return result


def figure_destination(summary, processed_root, reports_root, strategy, paper=True):
    relative = summary.parent.relative_to(Path(processed_root) / "eu")
    return (
        Path(reports_root)
        / "eu/figures"
        / relative
        / ("interprotocol-upset-" + component(strategy) + ("__paper" if paper else "") + ".pdf")
    )


def load(artifacts, all_artifacts, processed_root, reports_root, campaign, *, archive=False):
    values = {
        **tcp_defaults(),
        **vp_values(artifacts),
        **coverage_values(all_artifacts, artifacts),
    }
    errors = []
    for strategy in COUNTERS:
        values[f"fig.eu.interprotocol.{component(strategy)}.upset"] = record(
            None, "pdf-path", "EU interprotocol UpSet; paper PDF preferred"
        )
    if not archive and processed_root is not None:
        try:
            values.update(interprotocol_coverage(processed_root, artifacts))
            from paper_values.reference_review import read_prepared as read_references

            values.update(read_references(Path(processed_root), campaign))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(dict(source="measurement review coverage", error=str(exc)))
        try:
            path = find_summary(processed_root)
            if path:
                values.update(interprotocol_values(path))
                if reports_root is not None:
                    for strategy in COUNTERS:
                        choices = [
                            figure_destination(path, processed_root, reports_root, strategy, p)
                            for p in (True, False)
                        ]
                        selected = next((p for p in choices if p.is_file()), None)
                        if selected:
                            verify_file(selected)
                            with selected.open("rb") as stream:
                                if stream.read(5) != b"%PDF-":
                                    raise ValueError("invalid interprotocol PDF")
                            relative = (
                                "reports/continental/"
                                + campaign
                                + "/"
                                + selected.relative_to(reports_root).as_posix()
                            )
                            values[f"fig.eu.interprotocol.{component(strategy)}.upset"] = record(
                                validate_plot_path(relative),
                                "pdf-path",
                                "EU interprotocol UpSet; paper PDF preferred",
                                (selected, path),
                            )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(dict(source="EU interprotocol review", error=str(exc)))
        try:
            from paper_values.tcp_review import read_prepared

            values.update(read_prepared(Path(processed_root), campaign))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(dict(source="EU TCP review", error=str(exc)))
    return values, errors
