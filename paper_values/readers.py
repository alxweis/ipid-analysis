"""Explicit readers and existing paper calculations; no workflow or plotting imports."""

import ast
from collections import defaultdict
from functools import lru_cache
import math
from pathlib import Path
import re

from paper_values.cache import digest, verify_file
from paper_values.inputs import component
from paper_values.validation import Validation

STRATEGIES = {
    "REFLECTION",
    "CONSTANT",
    "SINGLE",
    "PER_DESTINATION",
    "PER_CONNECTION",
    "PER_BUCKET",
    "MULTI",
    "RANDOM",
    "UNCLASSIFIED",
    "NOT_ENOUGH_SAMPLES",
}


class ValueConflict(ValueError):
    """Conflicting shared settings must never replace an existing export."""


def percentage(numerator, denominator):
    return 100 * numerator / denominator if denominator else None


def field(data, path):
    """Read one named JSON field, tolerating underscore/case presentation differences."""
    for part in path:
        if not isinstance(data, dict):
            return None
        matches = [key for key in data if component(key) == part]
        if len(matches) > 1:
            raise ValueError(f"ambiguous report field: {part}")
        if not matches:
            return None
        data = data[matches[0]]
    return data


def counts(data):
    values, total = data.get("counts"), data.get("total_ips")
    if (
        not isinstance(values, dict)
        or type(total) is not int
        or total < 0
        or any(k not in STRATEGIES or type(v) is not int or v < 0 for k, v in values.items())
        or sum(values.values()) != total
    ):
        raise ValueError("invalid strategy counts or denominator")
    return values, total


def validate_report(a):
    """Validate a selected scientific report before exposing any of its values."""
    data = a.data
    if a.kind == "strategies":
        counts(data)
    elif a.kind == "coverage":
        total, success = data.get("zmap_ip_count"), data.get("ipid_ip_count")
        if not all(type(n) is int and n >= 0 for n in (total, success)) or success > total:
            raise ValueError("invalid coverage numerator or denominator")
    elif a.kind == "operating-system-group-by-strategy":
        total, matched = (
            data.get("strategy_ip_count"),
            data.get("matched_grouped_ip_count"),
        )
        if total is not None or matched is not None:
            if not all(type(n) is int and n >= 0 for n in (total, matched)) or matched > total:
                raise ValueError("invalid OS-matched denominator")
            if sum(g["ip_count"] for g in data.get("groups", {}).values()) != matched:
                raise ValueError("OS group composition differs from the joined population")
    elif a.kind == "itdk-role-by-strategy":
        n = data.get("plot_role_totals", {}).get("Transit-Observed")
        total = data.get("coverage", {}).get("total")
        if (
            n is not None
            and total is not None
            and (type(n) is not int or type(total) is not int or not 0 <= n <= total)
        ):
            raise ValueError("invalid transit-observed denominator")
    elif a.kind == "tcp-flags-by-strategy":
        for label in ("SYN-ACK", "RST"):
            bar = data["bars"][label]
            total = bar["total"]
            if (
                type(total) is not int
                or total < 0
                or any(type(n) is not int or not 0 <= n <= total for n in bar["counts"].values())
            ):
                raise ValueError("invalid TCP-response denominator")
    elif a.kind == "itdk-role-by-os":
        if any(
            type(r["count"]) is not int or r["count"] < 0 for r in data.get("raw_distribution", [])
        ):
            raise ValueError("invalid ITDK OS composition counts")


def os_aggregate(path):
    """Reuse the persisted OS/strategy aggregate; do not rebuild its raw-data join."""
    import pyarrow.parquet as pq

    with pq.ParquetFile(path) as source:
        if source.metadata.num_rows > 100_000:
            raise ValueError("expected a small OS/strategy aggregate")
        rows = source.read(
            columns=["OS_GROUP", "IPID_SELECTION_STRATEGY", "COUNT", "PERCENTAGE"]
        ).to_pylist()
    result = {}
    for row in rows:
        key = f"{component(row['OS_GROUP'])}:{row['IPID_SELECTION_STRATEGY']}"
        n, pct = row["COUNT"], row["PERCENTAGE"]
        if type(n) is not int or n < 0 or not isinstance(pct, (int, float)) or not 0 <= pct <= 100:
            raise ValueError("invalid OS/strategy aggregate")
        value = {"count": n, "share-pct": pct}
        if key in result and result[key] != value:
            raise ValueError("duplicate OS/strategy aggregate cell")
        result[key] = value
    return result


def increment_histogram(path):
    """Existing exact positive-increment statistics, retaining full-count denominators."""
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
    groups = defaultdict(list)
    for row in rows:
        if (
            row["MODE"] not in ("RT-based", "Fixed-Interval")
            or row["INCREMENT"] <= 0
            or row["COUNT"] < 0
            or row["TOTAL_COUNT"] <= 0
        ):
            raise ValueError("invalid positive-increment histogram")
        groups[row["MODE"], row["IPID_SELECTION_STRATEGY"]].append(row)
    result = {}
    for (mode, strategy), group in groups.items():
        group.sort(key=lambda r: r["INCREMENT"])
        totals = {r["TOTAL_COUNT"] for r in group}
        if len(totals) != 1:
            raise ValueError("increment histogram denominators disagree")
        total, cumulative = totals.pop(), 0
        for row in group:
            cumulative += row["COUNT"]
            if cumulative != row["CUMULATIVE_COUNT"] or cumulative > total:
                raise ValueError("invalid cumulative increment histogram")
        stats = {}
        p99 = next(
            (r["INCREMENT"] for r in group if r["CUMULATIVE_COUNT"] >= (99 * total + 99) // 100),
            None,
        )
        if p99 is not None:
            stats["p99"] = p99
        for threshold in (1000, 2000):
            # A clipped tail cannot determine fractions above the retained cutoff.
            if cumulative == total or threshold <= group[-1]["INCREMENT"] + 1:
                stats[f"below-{threshold}.share-pct"] = percentage(
                    sum(r["COUNT"] for r in group if r["INCREMENT"] < threshold), total
                )
        result[f"{mode}:{strategy}"] = stats
    return result


def classifier_constants(path):
    """Read only the existing small constant allowlist; do not import analysis code."""
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
        source = ast.parse(Path(path).read_text(encoding="utf-8"))
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


class Readers:
    def __init__(self, artifacts, processed_root, raw_root, cache, errors, validation_root=None):
        self.reports = {(a.site, a.protocol, a.connection, a.phase, a.kind): a for a in artifacts}
        self.processed_root, self.raw_root = processed_root, raw_root
        self.cache, self.errors = cache, errors
        self.validation = Validation(validation_root)
        # Per-export memoization: repeated API calls do not retain other Readers instances.
        self.aggregate = lru_cache(maxsize=None)(self.aggregate)
        self.snapshot = lru_cache(maxsize=None)(self.snapshot)
        self.constants = lru_cache(maxsize=1)(classifier_constants)

    def report(self, spec, site, *, kind=None, phase=None):
        return self.reports.get(
            (
                site,
                spec["protocol"],
                spec["connection"],
                phase or spec["phase"],
                kind or spec["kind"],
            )
        )

    def aggregate(self, site, protocol, connection, phase, kind):
        a = self.reports.get((site, protocol, connection, phase, kind))
        if a is None or self.processed_root is None:
            return {}, ()
        path = (
            Path(self.processed_root)
            / site
            / Path(a.relative.removeprefix("figures/")).with_suffix(".pq")
        )
        operation, compute = (
            ("histogram", increment_histogram)
            if kind == "increment-distributions"
            else ("os-strategy", os_aggregate)
        )
        try:
            values = self.cache.derive(operation, path, a.sha256, lambda: compute(path))
            return values, (a.name, str(path)) if values else ()
        except (OSError, ValueError, TypeError, KeyError, ImportError) as exc:
            self.errors.append({"source": str(path), "error": str(exc)})
            return {}, ()

    def snapshot(self, site, protocol, connection, phase):
        a = self.reports.get((site, protocol, connection, phase, "strategies"))
        if a is None or self.raw_root is None:
            return {}, ()
        mid = a.data.get("measurement_id")
        if not mid:
            return {}, ()
        if not isinstance(mid, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", mid):
            raise ValueError("invalid measurement identity")
        path = Path(self.raw_root) / site / "ipid" / mid / "ipid.snapshot.yaml"
        if not path.is_file():
            return {}, ()
        import yaml

        verify_file(path)
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
        except yaml.YAMLError as exc:
            raise ValueError(f"invalid snapshot: {path}: {exc}") from exc
        if not isinstance(data, dict):
            raise ValueError(f"invalid snapshot: {path}")
        return data, (str(path),)

    def read(self, spec, site):
        """Return a raw scalar and its sources for this explicit metric recipe."""
        reader = spec["reader"]
        if reader == "validation":
            return self.validation.read(spec)
        if reader in ("fi-rt", "tcp-transition"):
            return self.paired(spec, site)
        if reader in ("missing", "plot"):
            return None, ()
        if reader == "classifier":
            return self.summary(spec, site)
        a = self.report(spec, site)
        if reader == "snapshot":
            return self.parameter(spec, site)
        if a is None:
            return None, ()
        data, sources = a.data, (a.name,)
        if reader == "field":
            return field(data, spec["field"]), sources
        if reader in ("strategy", "recovered"):
            values, total = counts(data)
            if reader == "recovered":
                return percentage(
                    sum(values.get(s, 0) for s in ("SINGLE", "PER_DESTINATION", "PER_BUCKET")),
                    total - values.get("NOT_ENOUGH_SAMPLES", 0),
                ), sources
            excluded = (
                {"NOT_ENOUGH_SAMPLES"} if spec["population"] == "successful-results" else set()
            )
            if spec["population"] == "classified-results":
                excluded = {"NOT_ENOUGH_SAMPLES", "UNCLASSIFIED"}
            denominator = total - sum(values.get(s, 0) for s in excluded)
            strategy = spec["strategy"]
            if strategy in excluded:
                return None, sources
            value = denominator if strategy == "TOTAL" else values.get(strategy, 0)
            return (
                percentage(value, denominator) if spec["stat"] == "share-pct" else value
            ), sources
        if reader == "os-coverage":
            n, total = (
                data.get("matched_grouped_ip_count"),
                data.get("strategy_ip_count"),
            )
            if n is None or total is None:
                return None, sources
            return (
                percentage(n, total)
                if spec["stat"].endswith("share-pct") and n is not None and total is not None
                else n
            ), sources
        if reader in ("os-strategy", "os-range", "histogram"):
            table, sources = self.aggregate(
                site, spec["protocol"], spec["connection"], spec["phase"], spec["kind"]
            )
            if reader == "histogram":
                return table.get(f"{spec['mode']}:{spec['strategy']}", {}).get(
                    spec["stat"]
                ), sources
            if reader == "os-strategy":
                return table.get(f"{spec['group']}:{spec['strategy']}", {}).get(
                    spec["stat"]
                ), sources
            values = [
                table.get(f"{group}:{spec['strategy']}", {}).get("share-pct")
                for group in spec["groups"]
            ]
            if any(value is None for value in values):
                return None, sources
            return (min(values) if spec["stat"].startswith("min-") else max(values)), sources
        if reader == "role-os":
            rows = [
                r for r in data.get("raw_distribution", []) if component(r["role"]) == spec["role"]
            ]
            return percentage(
                sum(r["count"] for r in rows if r["os_group"] in spec["groups"]),
                sum(r["count"] for r in rows),
            ), sources
        if reader == "transit":
            n, total = (
                data.get("plot_role_totals", {}).get("Transit-Observed"),
                data.get("coverage", {}).get("total"),
            )
            if n is None or total is None:
                return None, sources
            return (
                percentage(n, total)
                if spec["stat"] == "share-pct" and n is not None and total is not None
                else n
            ), sources
        if reader == "flags":
            if spec["stat"].endswith("single-per-bucket-random.share-pct"):
                label = "SYN-ACK" if ".syn-ack." in spec["stat"] else "RST"
                bar = data["bars"][label]
                return percentage(
                    sum(bar["counts"].get(s, 0) for s in ("SINGLE", "PER_BUCKET", "RANDOM")),
                    bar["total"],
                ), sources
            shares = {
                label: percentage(bar["counts"].get("CONSTANT", 0), bar["total"])
                for label, bar in data["bars"].items()
                if label in ("SYN-ACK", "RST")
            }
            syn, rst = shares["SYN-ACK"], shares["RST"]
            return (
                percentage(syn - rst, rst) if syn is not None and rst is not None else None
            ), sources
        if reader == "threshold":
            return data.get("threshold"), sources
        raise ValueError(f"unknown metric reader: {reader}")

    def parameter(self, spec, site):
        data, sources = self.snapshot(site, spec["protocol"], spec["connection"], spec["phase"])
        name = spec["field"]
        length = None
        if (
            type(data.get("connection_count")) is int
            and type(data.get("requests_per_connection")) is int
        ):
            length = data["connection_count"] * data["requests_per_connection"]
        if name == "configured-sequence-length.count":
            value = length
        elif name == "minimum-required-replies.count":
            rate = data.get("fixed_interval", {}).get("minimum_reply_rate")
            value = (
                math.ceil(length * rate)
                if length is not None and isinstance(rate, (int, float)) and 0 <= rate <= 1
                else None
            )
        else:
            value = field(data, name.split("."))
        if (
            value is None
            and spec["phase"] == "fi-mass"
            and name in ("configured-sequence-length.count", "minimum-required-replies.count")
        ):
            report = self.report(spec, site, kind="missing-replies")
            if report:
                target = f"{spec['protocol']}.ipid.{spec['connection']}.fixed-interval.mass"
                source_field = (
                    "sequence_length"
                    if name.startswith("configured-")
                    else "configured_minimum_replies"
                )
                values = {
                    row[source_field]
                    for row in report.data.get("measurements", [])
                    if row.get("target") == target and source_field in row
                }
                if len(values) > 1:
                    raise ValueError("conflicting persisted fixed-interval configuration")
                if values:
                    value, sources = values.pop(), (report.name,)
        return value, sources

    def summary(self, spec, site):
        reader = spec["reader"]
        reports = [a for a in self.reports.values() if site is None or a.site == site]
        if reader == "classifier":
            versions = [
                a
                for a in reports
                if a.kind == "random-reproducibility-evaluation"
                and a.data.get("classifier_version")
            ]
            path = Path(__file__).resolve().parents[1] / "ipid_analysis/strategies.py"
            if not versions:
                return None, ()
            constants = self.constants(path)
            if versions and all(
                str(a.data["classifier_version"]) == constants.get("CLASSIFIER_VERSION")
                for a in versions
            ):
                names = {
                    "single.maximum-increment": "MAX_INC",
                    "per-bucket.maximum-increment": "MAX_INC",
                    "multi.maximum-increment": "MULTI_MAX_INC",
                    "multi.maximum-clusters": "MULTI_MAX_CLUSTERS",
                }
                verify_file(path)
                return constants.get(names[spec["field"]]), (
                    str(path),
                    *(a.name for a in versions),
                )
        return None, ()

    def paired(self, spec, site):
        from paper_values import paired

        if self.processed_root is None:
            return None, ()
        proto, mode = spec["protocol"], spec["connection"]
        first = self.reports.get(
            (
                site,
                proto,
                "no-connection" if spec["reader"] == "tcp-transition" else mode,
                "rt-base",
                "strategies",
            )
        )
        second = self.reports.get(
            (
                site,
                proto,
                mode,
                "rt-base" if spec["reader"] == "tcp-transition" else "fi-base",
                "strategies",
            )
        )
        if first is None or second is None:
            return None, ()
        zid = first.data.get("zmap_id")
        if not zid or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", zid):
            raise ValueError("paired measurements need a safe discovery identity")
        if second.data.get("zmap_id") != zid:
            raise ValueError("paired measurements have different discovery identities")
        paths = [
            Path(self.processed_root)
            / site
            / Path(a.relative.removeprefix("figures/")).with_suffix(".pq")
            for a in (first, second)
        ]
        reports = [first, second]
        if spec["reader"] == "fi-rt":
            coverage = self.reports.get((site, proto, mode, "fi-base", "coverage"))
            if self.raw_root is None or coverage is None:
                return None, ()
            stem = "zmap-connection-sample" if mode == "connection" else "zmap-fixed-base-sample"
            directory = Path(self.raw_root) / site / "zmap" / zid
            paths += [directory / (stem + ext) for ext in (".pq", ".json")]
            reports.append(coverage)
        version = self.constants(
            Path(__file__).resolve().parents[1] / "ipid_analysis/strategies.py"
        )
        report_sha = digest([a.sha256 for a in reports])

        def compute():
            if spec["reader"] == "tcp-transition":
                return paired.tcp_transition(
                    paths, first.data, second.data, version["CLASSIFIER_VERSION"]
                )
            return paired.fi_rt(
                paths,
                first.data,
                second.data,
                coverage.data,
                version["CLASSIFIER_VERSION"],
                connection_mode=mode == "connection",
            )

        result = self.cache.derive(spec["reader"], paths, report_sha, compute)
        if not result:
            return None, ()
        return result["pct"], tuple([a.name for a in reports] + [str(p) for p in paths])
