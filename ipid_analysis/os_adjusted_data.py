"""One-off tag regrouping into new compact caches; all existing inputs are read-only."""

from collections import Counter
import hashlib
import json
from pathlib import Path
import tempfile

import pyarrow.parquet as pq

from ipid_analysis.artifacts import cache_current, fingerprint, publish_new_file
from ipid_analysis.paper_os_taxonomy import GROUPS, TAGS, VERSION, parent
from ipid_analysis.plot_os_group_strategy import (
    aggregate_os_groups,
    write_os_groups,
)
from ipid_analysis.runtime import connect, scratch_root

KIND = "operating-system-group-by-strategy"
MAX_BYTES = 4 << 20


def read_json(path):
    from ipid_analysis.artifacts import assert_complete

    assert_complete(path)
    if path.stat().st_size > MAX_BYTES:
        raise ValueError("expected a small OS report/cache (at most 4 MiB)")
    return json.loads(path.read_text())


def identity():
    return {
        "version": VERSION,
        "code_sha256": hashlib.sha256(
            json.dumps(TAGS, sort_keys=True).encode()
            + b"".join(
                Path(__file__).with_name(name).read_bytes()
                for name in (
                    "os_adjusted_data.py",
                    "plot_os_group_strategy.py",
                )
            )
        ).hexdigest(),
    }


def paths(source):
    """Use the bound report's exact input paths, including Full follow-up paths."""
    report = read_json(source)
    if report.get("figure") != KIND:
        raise ValueError("expected the original OS-group strategy report")
    target = report.get("target", "")
    mode = ".connection." in target
    proto = report.get("protocol")
    expected = (
        "tcp.ipid.connection.rt-based.base"
        if mode
        else (
            f"{proto}.ipid.no-connection.rt-based.base+{proto}.ipid.no-connection.fixed-interval.mass"
        )
    )
    if target != expected or proto not in ("icmp", "tcp", "udp-dns"):
        raise ValueError("unexpected adjusted OS measurement population")
    if not report.get("zmap_id") or not report.get("os_measurement_id"):
        raise ValueError("OS report lacks measurement identity")
    inputs = report.get("sources", {})
    strategies, os_source, aggregate = [
        Path(value)
        for value in (
            inputs.get("strategies", ""),
            inputs.get("os", ""),
            report.get("aggregate", ""),
        )
    ]
    if not all(p.is_absolute() for p in (strategies, os_source, aggregate)):
        raise ValueError("OS report inputs must be explicit absolute paths")
    prefix = "c-rt-b_" if mode else "n-rt-b_fi-m_"
    if strategies.name != prefix + "strategies.pq" or aggregate.name != prefix + KIND + ".pq":
        raise ValueError("OS aggregate/labels do not match their measurement phase")
    if aggregate.parent != strategies.parent:
        raise ValueError("OS aggregate/labels do not share a measurement directory")
    full = "__full" if source.stem.endswith("__full") else ""
    role = source.with_name(prefix + "itdk-role-by-os" + full + ".json")
    joined = aggregate.with_name(prefix + "itdk-os-role-join.pq")
    cache = aggregate.with_name(aggregate.stem + "__adjusted.json")
    return report, strategies, os_source, role, joined, cache


def group_cache(raw, root):
    key = hashlib.sha256(
        (str(raw.resolve()) + json.dumps(TAGS, sort_keys=True)).encode()
    ).hexdigest()[:20]
    return root / (VERSION + "-" + key + ".pq")


def new_groups(raw, destination):
    params = {
        "taxonomy_version": VERSION,
        "mapping_sha256": hashlib.sha256(json.dumps(TAGS, sort_keys=True).encode()).hexdigest(),
    }
    if destination.exists():
        if not cache_current(destination, [raw], params):
            raise ValueError("adjusted OS tag cache is stale; existing files were left untouched")
        return
    before = fingerprint(raw)
    with tempfile.TemporaryDirectory(dir=scratch_root(), prefix="os-tags-adjusted-") as directory:
        stage = Path(directory) / "groups.pq"
        write_os_groups(raw, stage, tag_mapping=TAGS, taxonomy_version=VERSION)
        with connect() as con:
            n, present, distinct = (
                con.read_parquet(str(stage))
                .aggregate("count(*), count(IP_ADDR), count(DISTINCT IP_ADDR)")
                .fetchone()
            )
            if n != present or n != distinct:
                raise ValueError("duplicate/null IP in the adjusted resolved OS groups")
        if fingerprint(raw) != before:
            raise ValueError("OS source changed during adjusted preparation")
        publish_new_file(stage, destination, inputs=[raw], parameters=params)


def collapse(counts):
    result = Counter()
    for group, count in counts.items():
        result[parent(group)] += count
    return +result


def prepare(source, *, need_roles=False, group_root=None):
    report, strategies, raw, role_path, joined, destination = paths(source)
    if need_roles and not role_path.is_file():
        raise FileNotFoundError(role_path)
    inputs = [source, strategies, raw]
    if role_path.is_file() and joined.is_file():
        inputs.extend((role_path, joined))
    elif need_roles:
        raise FileNotFoundError(joined)
    params = identity()
    if destination.exists():
        doc = read_json(destination)
        all_inputs = [Path(p["path"]) for p in doc["inputs"]]
        if (
            doc.get("identity") != params
            or doc.get("source") != str(source.resolve())
            or not cache_current(destination, all_inputs, params)
            or need_roles
            and not doc.get("roles")
        ):
            raise ValueError(
                "adjusted OS aggregate is stale; existing outputs were left untouched"
            )
        return "cached"
    # Store one shared tag map per original OS source; design-only renders never touch it.
    if group_root is None:
        from ipid_analysis.config import PROCESSED_DATA_DIR

        group_root = PROCESSED_DATA_DIR / "paper-adjusted-tags"
    groups = group_cache(raw, Path(group_root))
    new_groups(raw, groups)
    inputs.append(groups)
    before = [fingerprint(p) for p in inputs]
    with tempfile.TemporaryDirectory(dir=scratch_root(), prefix="os-adjusted-") as directory:
        stage = Path(directory) / "aggregate.pq"
        stats = aggregate_os_groups(strategies, groups, stage, group_info=GROUPS)
        original_totals = {g: row["ip_count"] for g, row in report["groups"].items()}
        if (
            stats["strategy_ip_count"] != report["strategy_ip_count"]
            or stats["matched_grouped_ip_count"] != report["matched_grouped_ip_count"]
            or collapse({g: row["ip_count"] for g, row in stats["groups"].items()})
            != +Counter(original_totals)
        ):
            raise ValueError("adjusted OS groups do not conserve the original measured population")
        rows = pq.read_table(stage).to_pylist()
        # Where available, prove every original per-strategy cell is conserved.
        original_aggregate = Path(report["aggregate"])
        if original_aggregate.is_file():
            if original_aggregate.stat().st_size > MAX_BYTES:
                raise ValueError("original OS aggregate is unexpectedly large")
            old_cells = {
                (r["OS_GROUP"], r["IPID_SELECTION_STRATEGY"]): r["COUNT"]
                for r in pq.read_table(original_aggregate).to_pylist()
            }
            cells = Counter()
            for row in rows:
                cells[parent(row["OS_GROUP"]), row["IPID_SELECTION_STRATEGY"]] += row["COUNT"]
            if +cells != +Counter(old_cells):
                raise ValueError("adjusted OS/strategy cells differ from the original aggregate")
        roles = {}
        if role_path in inputs:
            role_report = read_json(role_path)
            for key in ("protocol", "target", "zmap_id"):
                if role_report.get(key) != report.get(key):
                    raise ValueError("adjusted role/heatmap report identities disagree")
            from ipid_analysis.paper_itdk_final import same_population

            if ".connection." not in report["target"]:
                same_population(role_report, report, os=True)
            elif (
                role_report.get("measurement_id")
                != report.get("measurements", {}).get("rt_based_base")
                or role_report.get("os", {}).get("measurement_id") != report["os_measurement_id"]
            ):
                raise ValueError("adjusted Full role/heatmap measurement IDs disagree")

            def sql(path):
                return "'" + str(path).replace("'", "''") + "'"

            with connect() as con:
                n, distinct, present, known_t = con.execute(
                    f"SELECT count(*),count(DISTINCT IP_ADDR),count(IP_ADDR),count(T) FROM read_parquet({sql(joined)})"
                ).fetchone()
                if not n == distinct == present == known_t:
                    raise ValueError("invalid/duplicate IP or missing T in original ITDK/OS join")
                counted = con.execute(f"""
                    SELECT j.T,g.OS_GROUP,count(*)::BIGINT
                    FROM read_parquet({sql(joined)}) j
                    JOIN read_parquet({sql(groups)}) g USING (IP_ADDR)
                    GROUP BY j.T,g.OS_GROUP
                """).fetchall()
            for flag, group, count in counted:
                roles.setdefault("Transit-Observed" if flag else "No Transit Evidence", {})[
                    group
                ] = count
            for name in ("Transit-Observed", "No Transit Evidence"):
                old_counts = {
                    r["os_group"]: r["count"]
                    for r in role_report["raw_distribution"]
                    if r["role"] == name
                }
                if collapse(roles.get(name, {})) != +Counter(old_counts):
                    raise ValueError(
                        "adjusted ITDK/OS counts differ from the original Router/non-transit population"
                    )
        adjusted_report = {
            **report,
            **stats,
            "methodology": {
                **report.get("methodology", {}),
                "taxonomy_version": VERSION,
                "os_input": "resolved OS_TAG split without changing the measured population",
            },
        }
        doc = dict(
            identity=params,
            source=str(source.resolve()),
            inputs=before,
            original_report_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
            report=adjusted_report,
            rows=rows,
            roles=roles,
        )
        if before != [fingerprint(p) for p in inputs]:
            raise ValueError("original inputs changed during adjusted preparation")
        output = Path(directory) / "adjusted.json"
        output.write_text(json.dumps(doc, sort_keys=True) + "\n")
        publish_new_file(output, destination, inputs=inputs, parameters=params)
    return "prepared"


def load(source, *, need_roles=False):
    _, _, _, _, _, destination = paths(source)
    if not destination.is_file():
        raise ValueError("adjusted OS aggregate missing; run --prepare-os-adjusted once")
    doc = read_json(destination)
    inputs = [Path(p["path"]) for p in doc.get("inputs", [])]
    if (
        doc.get("identity") != identity()
        or doc.get("source") != str(source.resolve())
        or not inputs
        or not cache_current(destination, inputs, identity())
        or doc.get("original_report_sha256") != hashlib.sha256(source.read_bytes()).hexdigest()
        or need_roles
        and not doc.get("roles")
    ):
        raise ValueError("adjusted OS aggregate is stale or incomplete; no raw-data fallback")
    return doc, destination, inputs
