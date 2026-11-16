"""Export a partial geographic campaign to a named, traceable LaTeX value store.

The default path reads published JSON, small aggregates and PDF metadata, never raw probe Parquet.
The standalone writer emits ndpi-compatible LaTeX without importing analysis workflows.
Missing observations stay missing, including observed
populations of size zero whose percentages have no defined denominator.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import os
import re
import tempfile
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path, PurePosixPath
from time import perf_counter
from typing import Any

from paper_values.cache import ExportCache, stamp

SITES = ("eu", "na", "sa", "as", "af", "au")
PROTOCOLS = ("icmp", "tcp", "udp")
STRATEGIES = (
    "REFLECTION",
    "CONSTANT",
    "SINGLE",
    "PER_CONNECTION",
    "PER_DESTINATION",
    "PER_BUCKET",
    "MULTI",
    "RANDOM",
    "UNCLASSIFIED",
    "NOT_ENOUGH_SAMPLES",
)
PHASES = ("rt-base", "fi-mass", "fi-base", "rt-fi-merged", "rt-base-vs-fi-base")
PROTOCOL_GROUPS = ("icmp-tcp", "icmp-udp", "tcp-udp", "icmp-tcp-udp")
INTERPROTOCOL_OUTCOMES = (
    "PROTOCOL_SHARED",
    "PARTIAL_PROTOCOL_SHARED",
    "PROTOCOL_ISOLATED",
    "SAME_PARAMETER",
    "PARTIALLY_MATCHING_PARAMETERS",
    "DIFFERENT_PARAMETERS",
    "SAME_INTER_STRATEGY",
    "DIFFERENT_INTER_STRATEGY",
    "UNCHANGED",
    "STRATEGY_CHANGED",
    "NOT_ENOUGH_SAMPLES",
)
KINDS = (
    "strategies",
    "coverage",
    "measurement-type-by-strategy",
    "measurement-type-by-strategy-with-connection",
    "increments",
    "probing-intervals",
    "strategy-intersection",
    "increment-distributions",
    "probing-intervals-by-continent",
    "operating-system-group-by-strategy",
    "itdk-role-by-strategy",
    "itdk-role-by-os",
    "itdk-consistency-summary",
    "ripe-atlas-role-by-strategy",
    "ripe-atlas-role-by-os",
    "caida-ripe-intersection-role-by-strategy",
    "tcp-flags-by-strategy",
    "missing-replies",
    "random-reproducibility-evaluation",
    "os-coverage",
)
SKIP_FIELDS = {
    "source",
    "sources",
    "outputs",
    "aggregate",
    "artifacts",
    "storage_roots",
    "software",
    "generated_at",
    "methodology",
    "target",
    "zmap_id",
    "measurement_id",
    "measurements",
    "base_target",
    "mass_target",
    "base_measurement_id",
    "mass_measurement_id",
    "protocol",
    "connection_mode",
    "interval",
    "scale",
    "figure",
    "manifest",
    "processed_os_groups",
    "threshold_calibration_report",
    "histogram",
    "increment",
    "cumulative_pct",
    "quantile_points",
    "shared_os",
    "maxmind_provenance",
    "itdk",
    "ripe",
    "os",
    "input",
    "snapshot",
    "path",
    "sha256",
    "mtime_ns",
    "code_revision",
    "python",
    "os_measurement_id",
    "artifact_semantics",
    "role_definition",
    "reused_group_file",
    "analysis_cache_version",
    "provenance",
}
PREFIXES = (
    "n-rt-b_fi-m_",
    "n-rt-b_fi-b_",
    "c-rt-b_fi-b_",
    "n-rt-b_",
    "n-fi-m_",
    "n-fi-b_",
    "c-rt-b_",
    "c-fi-b_",
)
TEX_PREAMBLE = r"""% Generated campaign values. Edit the analysis inputs, not this file.
\providecommand{\setval}[2]{\expandafter\gdef\csname texval@#1\endcsname{#2}}
\providecommand{\getval}[1]{%
  \ifcsname texval@#1\endcsname\csname texval@#1\endcsname%
  \else\errmessage{Unknown paper value: #1}\fi}
"""
PLOT_KINDS = set(KINDS) - {
    "coverage",
    "os-coverage",
    "itdk-consistency-summary",
    "random-reproducibility-evaluation",
}


def component(value: Any) -> str:
    value = str(value).lower().replace("_", "-")
    value = re.sub(r"[^a-z0-9-]+", "-", value).strip("-")
    if not value:
        raise ValueError("empty key component")
    return value


def percentage(numerator, denominator):
    return 100 * numerator / denominator if denominator else None


@dataclass
class Value:
    raw: Any = None
    unit: str = ""
    definition: str = ""
    sources: tuple[str, ...] = ()
    status: str = "missing"
    reason: str = "required analysis artifact is not available"


class Registry:
    def __init__(self):
        self.values: dict[str, Value] = {}
        self.evidence = {}
        self.cache = None

    def record_file(self, path):
        path = Path(path)
        stat = path.stat()
        marker_path = path.parent / ".ipid-complete" / (path.name + ".json")
        marker = (
            json.loads(marker_path.read_text(encoding="utf-8-sig"))
            if marker_path.is_file()
            else None
        )
        if marker is not None and not isinstance(marker, dict):
            raise ValueError(f"invalid completion receipt: {path}")
        if marker is not None and (
            marker.get("status") != "complete"
            or marker.get("size_bytes") != stat.st_size
        ):
            raise ValueError(f"completion receipt does not match source size: {path}")
        digest = (
            hashlib.sha256(path.read_bytes()).hexdigest()
            if stat.st_size <= 32 * 1024**2
            else None
        )
        if digest and marker is not None and digest != marker.get("sha256"):
            raise ValueError(f"completion receipt does not match source bytes: {path}")
        self.evidence[str(path)] = {
            "path": str(path),
            "size_bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "sha256": digest,
            "receipt_sha256": marker.get("sha256") if marker is not None else None,
            "completion": ("verified" if digest else "size-checked")
            if marker is not None
            else "legacy-no-receipt",
        }

    def reserve(self, key, *, unit="", definition="", reason=None):
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", key):
            raise ValueError(f"invalid value key: {key}")
        if key not in self.values:
            self.values[key] = Value(unit=unit, definition=definition)
        if reason:
            self.values[key].reason = reason

    def put(self, key, raw, *, unit="", definition="", sources=(), reason=None):
        self.reserve(key, unit=unit, definition=definition)
        if raw is None:
            previous = self.values[key]
            if previous.status != "available":
                self.values[key] = Value(
                    None,
                    unit or previous.unit,
                    definition or previous.definition,
                    tuple(sources),
                    "missing",
                    reason or previous.reason,
                )
            return
        if not isinstance(raw, (str, int, float, bool)):
            raise ValueError(f"{key}: expected a scalar")
        if isinstance(raw, float) and not math.isfinite(raw):
            raise ValueError(f"{key}: non-finite value")
        value = Value(raw, unit, definition, tuple(sources), "available", "")
        previous = self.values[key]
        if previous.status == "available" and previous != value:
            raise ValueError(f"ambiguous or duplicate value: {key}")
        self.values[key] = value

    def mirror_missing_vps(self):
        """Every discovered VP metric has a key at all six VPs, without copying data."""
        for key, value in list(self.values.items()):
            match = re.search(r"\.vp-(eu|na|sa|as|af|au)\.", key)
            if match and not key.startswith("comparison."):
                for site in SITES:
                    sibling = key[: match.start()] + f".vp-{site}." + key[match.end() :]
                    self.reserve(sibling, unit=value.unit, definition=value.definition)

    def merge(self, other):
        for key, value in other.values.items():
            previous = self.values.get(key)
            if previous and previous.status == "available" and previous != value:
                raise ValueError(f"ambiguous or duplicate value: {key}")
        self.values.update(other.values)
        self.evidence.update(other.evidence)


@dataclass
class Artifact:
    name: str
    data: dict
    sha256: str
    size_bytes: int
    completion: str
    site: str
    protocol: str = ""
    connection: str = ""
    phase: str = ""
    kind: str = ""
    run: str = ""
    origin: str = ""
    relative: str = ""


@dataclass
class Plot:
    name: str
    site: str
    relative: str
    paper_path: str
    protocol: str
    run: str
    scope: str
    size_bytes: int = 0
    receipt_sha256: str | None = None
    completion: str = "legacy-no-receipt"
    error: str | None = None


class Inputs:
    def __init__(
        self, paths=(), archive=None, *, cache=None, workers=8, profile="full"
    ):
        self.reports_root = Path(paths[0]) if paths and paths[0] is not None else None
        self.paths = [Path(p) for p in paths if p is not None and Path(p).is_dir()]
        self.archive = zipfile.ZipFile(archive) if archive else None
        self.errors = []
        self.cache = cache
        self.workers = 1 if archive else workers
        self.profile = profile
        self.inventory = {}
        self.timings = {}
        self.archive_stamp = stamp(archive) if archive else None

    def close(self):
        if self.archive:
            self.archive.close()

    def scan(self):
        start = perf_counter()
        for root in self.paths:
            files = []

            def failed(error):
                self.errors.append({"source": str(root), "error": str(error)})

            for directory, directories, names in os.walk(root, onerror=failed):
                directories[:] = [d for d in directories if not d.startswith(".")]
                files.extend(
                    Path(directory) / name
                    for name in names
                    if not name.startswith(".") and name.endswith((".json", ".pdf"))
                )
            self.inventory[root] = sorted(files)
        self.timings["inventory"] = perf_counter() - start

    def files(self):
        if self.archive:
            for info in self.archive.infolist():
                if not info.is_dir() and info.filename.endswith(".json"):
                    yield (
                        info.filename,
                        lambda n=info.filename: self.archive.read(n),
                        "archive",
                    )
        for root in self.paths:
            for path in self.inventory[root]:
                if path.suffix == ".json":
                    yield path.as_posix(), path.read_bytes, str(root)

    def read_marker(self, name, origin):
        path = PurePosixPath(name)
        marker = str(path.parent / ".ipid-complete" / (path.name + ".json"))
        if origin == "archive":
            try:
                return json.loads(self.archive.read(marker))
            except KeyError:
                return None
        marker_path = Path(marker)
        return (
            json.loads(marker_path.read_text(encoding="utf-8-sig"))
            if marker_path.is_file()
            else None
        )

    def fingerprint(self, name, origin, marker):
        if origin == "archive":
            info = self.archive.getinfo(name)
            source = [*self.archive_stamp, info.CRC, info.file_size]
        else:
            source = stamp(name)
        return dict(source=source, receipt=marker)

    def load_artifact(self, entry):
        name, read, origin, site, relative = entry
        try:
            marker = self.read_marker(name, origin)
            before = self.fingerprint(name, origin, marker)
            identity = (
                self.cache.identity("metadata", site, relative, name)
                if self.cache
                else None
            )
            payload = self.cache.get(identity, before) if self.cache else None
            if payload is None:
                content = read()
                payload = dict(
                    data=json.loads(content),
                    sha256=hashlib.sha256(content).hexdigest(),
                    size_bytes=len(content),
                )
                if self.fingerprint(name, origin, marker) != before:
                    raise ValueError("JSON artifact changed while reading")
            if marker is not None and (
                not isinstance(marker, dict)
                or marker.get("status") != "complete"
                or marker.get("size_bytes") != payload["size_bytes"]
                or marker.get("sha256") != payload["sha256"]
            ):
                raise ValueError("completion receipt does not match artifact bytes")
            if not isinstance(payload["data"], dict):
                return None, None
            artifact = identify(
                Artifact(
                    name,
                    payload["data"],
                    payload["sha256"],
                    payload["size_bytes"],
                    "verified" if marker is not None else "legacy-no-receipt",
                    site,
                    origin=origin,
                    relative=relative,
                ),
                relative,
            )
            if self.cache and artifact:
                self.cache.put(identity, before, payload)
            return artifact, None
        except (OSError, ValueError, KeyError, zipfile.BadZipFile) as exc:
            return None, {"source": name, "error": str(exc)}

    def artifacts(self, campaign):
        self.scan()
        start = perf_counter()
        entries = []
        for name, read, origin in self.files():
            match = re.search(
                rf"/continental/{re.escape(campaign)}/(eu|na|sa|as|af|au)/(.+)$",
                "/" + name.lstrip("/"),
            )
            if not match or "/paper-values/" in name:
                continue
            site, relative = match.groups()
            if any(p.startswith(".") for p in PurePosixPath(relative).parts):
                continue
            stem = PurePosixPath(relative).stem
            kind = next((stem[len(p) :] for p in PREFIXES if stem.startswith(p)), stem)
            if kind.endswith("random-reproducibility-evaluation"):
                kind = "random-reproducibility-evaluation"
            if kind not in (
                *KINDS,
                "shared-os-coverage",
                "interprotocol-campaign-summary",
            ):
                continue  # Never open metadata which cannot contribute an export value.
            if self.profile == "paper" and kind == "interprotocol-campaign-summary":
                continue
            entries.append((name, read, origin, site, relative))
        seen, conflicts = {}, set()
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for artifact, error in pool.map(self.load_artifact, entries):
                if error:
                    self.errors.append(error)
                if artifact:
                    identity = (artifact.site, artifact.relative)
                    if identity in seen:
                        if seen[identity].sha256 != artifact.sha256:
                            conflicts.add(identity)
                            self.errors.append(
                                {
                                    "source": artifact.name,
                                    "error": "archive and mounted artifact disagree",
                                }
                            )
                    else:
                        seen[identity] = artifact
        self.timings["json"] = perf_counter() - start
        yield from (a for identity, a in seen.items() if identity not in conflicts)

    def load_plot(self, entry):
        name, site, relative, origin, size = entry
        plot = identify_plot(name, site, relative, self.campaign)
        if plot is None:
            return None
        if (
            self.plot_keys is not None
            and f"plots.vp-{site}.{plot.scope}" not in self.plot_keys
        ):
            return None
        try:
            validate_plot_path(plot.paper_path)
            marker = self.read_marker(name, origin)
            before = self.fingerprint(name, origin, marker)
            identity = (
                self.cache.identity("pdf", site, relative, name) if self.cache else None
            )
            payload = self.cache.get(identity, before) if self.cache else None
            if payload is None:
                if origin == "archive":
                    with self.archive.open(name) as source:
                        header = source.read(5)
                else:
                    size = before["source"][0]
                    with Path(name).open("rb") as source:
                        header = source.read(5)
                if header != b"%PDF-":
                    raise ValueError("report file has no PDF signature")
                payload = dict(size_bytes=size)
                if self.fingerprint(name, origin, marker) != before:
                    raise ValueError("PDF changed while reading")
            size = payload["size_bytes"]
            plot.size_bytes = size
            if marker is not None:
                if (
                    not isinstance(marker, dict)
                    or marker.get("status") != "complete"
                    or marker.get("size_bytes") != size
                    or not re.fullmatch(
                        r"[a-fA-F0-9]{64}", str(marker.get("sha256", ""))
                    )
                ):
                    raise ValueError(
                        "PDF completion receipt does not match file metadata"
                    )
                plot.receipt_sha256 = marker["sha256"]
                plot.completion = "size-checked"
            if self.cache:
                self.cache.put(identity, before, payload)
        except (OSError, ValueError, KeyError, zipfile.BadZipFile) as exc:
            plot.error = str(exc)
        return plot

    def plots(self, campaign):
        start = perf_counter()
        self.campaign = campaign
        self.plot_keys = None
        if self.profile == "paper":
            from paper_values.profile import load_selection

            self.plot_keys = {
                entry["key"].format(vp=site)
                for entry in load_selection()["entries"]
                if entry["key"].startswith("plots.")
                for site in SITES
            }
        entries = []
        if self.archive:
            for info in self.archive.infolist():
                match = re.search(
                    rf"(?:^|/)reports/continental/{re.escape(campaign)}/"
                    r"(eu|na|sa|as|af|au)/(.+\.pdf)$",
                    info.filename,
                )
                if match and not info.is_dir():
                    site, relative = match.groups()
                    entries.append(
                        (info.filename, site, relative, "archive", info.file_size)
                    )
        if self.reports_root and self.reports_root.is_dir():
            for path in self.inventory[self.reports_root]:
                if path.suffix != ".pdf":
                    continue
                parts = path.relative_to(self.reports_root).parts
                if len(parts) >= 2 and parts[0] in SITES:
                    entries.append(
                        (
                            path.as_posix(),
                            parts[0],
                            PurePosixPath(*parts[1:]).as_posix(),
                            "directory",
                            None,
                        )
                    )
        entries = [
            e
            for e in entries
            if not any(p.startswith(".") for p in PurePosixPath(e[2]).parts)
        ]
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            yield from (
                plot for plot in pool.map(self.load_plot, entries) if plot is not None
            )
        self.timings["pdf"] = perf_counter() - start


def identify(artifact, relative, *, allow_unknown_kind=False):
    data = artifact.data
    protocol = str(data.get("protocol", "")).replace("udp-dns", "udp")
    parts = PurePosixPath(relative).parts
    if protocol not in PROTOCOLS and parts:
        token = (
            parts[0] if parts[0] != "figures" else parts[1] if len(parts) > 1 else ""
        )
        protocol = (
            "icmp"
            if token.startswith("icmp_")
            else "tcp"
            if token.startswith("tcp-")
            else "udp"
            if token.startswith("udp-dns-")
            else ""
        )
    stem = PurePosixPath(relative).stem
    if stem in ("shared-os-coverage", "os-coverage") and protocol in PROTOCOLS:
        artifact.protocol, artifact.connection = protocol, "no-connection"
        artifact.phase, artifact.kind = "os-scan", "os-coverage"
        artifact.run = parts[1] if parts[0] == "figures" else parts[0]
        return artifact
    kind = next((stem[len(p) :] for p in PREFIXES if stem.startswith(p)), stem)
    if kind.endswith("random-reproducibility-evaluation"):
        kind = "random-reproducibility-evaluation"
    elif kind.endswith("random-reproducibility-summary"):
        kind = "random-reproducibility-summary"
    if protocol in PROTOCOLS and (kind in KINDS or allow_unknown_kind):
        target = str(data.get("target", ""))
        connection = "no-connection" if "/no-connection/" in relative else "connection"
        if ".no-connection." in target:
            connection = "no-connection"
        elif ".connection." in target:
            connection = "connection"
        if "/merged/" in relative:
            phase = "rt-fi-merged"
        elif "/comparison/" in relative:
            phase = "rt-base-vs-fi-base"
        elif "random-reproducibility" in relative:
            phase = "fi-mass"
        else:
            phase = next(
                (
                    name
                    for source, name in (
                        ("rt-based-base", "rt-base"),
                        ("fixed-interval-mass", "fi-mass"),
                        ("fixed-interval-base", "fi-base"),
                    )
                    if source in relative
                ),
                "",
            )
        if not phase:
            return None
        run = parts[1] if parts[0] == "figures" else parts[0]
        artifact.protocol, artifact.connection = protocol, connection
        artifact.phase, artifact.kind, artifact.run = phase, kind, run
        return artifact
    if stem == "interprotocol-campaign-summary":
        artifact.protocol = "all-protocols"
        artifact.connection, artifact.phase, artifact.kind = (
            "mixed",
            "interprotocol",
            stem,
        )
        return artifact
    return None


def validate_plot_path(path):
    if (
        not isinstance(path, str)
        or not path.startswith("reports/")
        or not path.endswith(".pdf")
        or not re.fullmatch(r"[A-Za-z0-9_./ -]+", path)
        or ".." in PurePosixPath(path).parts
    ):
        raise ValueError("expected a TeX-safe, paper-relative reports/.../*.pdf path")
    return path


def identify_plot(name, site, relative, campaign):
    stem = PurePosixPath(relative).stem
    if stem.startswith(("interprotocol-campaign-", "interprotocol-upset-")):
        scope = "interprotocol." + stem.removeprefix("interprotocol-").replace(
            "-", ".", 1
        )
        # Keep multi-word strategy names as a single component (e.g. per-bucket).
        protocol, run = "all-protocols", "interprotocol"
    elif (
        stem == "target-population" and "interprotocol" in PurePosixPath(relative).parts
    ):
        scope, protocol, run = (
            "interprotocol.target-population",
            "all-protocols",
            "interprotocol",
        )
    else:
        a = identify(
            Artifact(name, {}, "", 0, "", site), relative, allow_unknown_kind=True
        )
        if a is None:
            return None
        scope = f"{a.protocol}.{a.connection}.{a.phase}.{component(a.kind)}"
        protocol, run = a.protocol, a.run
    return Plot(
        name,
        site,
        relative,
        f"reports/continental/{campaign}/{site}/{relative}",
        protocol,
        run,
        scope,
    )


def plot_values(registry, plots, artifacts, errors):
    canonical, preferred, repeats = defaultdict(set), defaultdict(set), {}
    for a in artifacts:
        if a.phase == "rt-fi-merged" and a.data.get("zmap_id"):
            canonical[a.site, a.protocol].add(a.data["zmap_id"])
        scope = f"{a.protocol}.{a.connection}.{a.phase}.{component(a.kind)}"
        preferred[a.site, scope].update((a.run, a.data.get("measurement_id")))
        if a.kind == "random-reproducibility-evaluation":
            for row in a.data.get("repeats", []):
                index = row.get("repeat_index")
                if type(index) is int and index > 0:
                    repeats[a.site, row["measurement_id"]] = index
                    for kind in ("strategies", "increments", "probing-intervals"):
                        key = (
                            f"plots.vp-{a.site}.{a.protocol}.{a.connection}.{a.phase}."
                            f"repetitions.repeat-{index}.{kind}"
                        )
                        registry.reserve(
                            key,
                            unit="pdf-path",
                            reason="repetition PDF is not available",
                            definition="PDF path relative to the paper root",
                        )
    grouped = defaultdict(list)
    for plot in plots:
        index = repeats.get((plot.site, plot.run))
        if index is not None:
            phase, kind = plot.scope.rsplit(".", 1)
            plot.scope = f"{phase}.repetitions.repeat-{index}.{kind}"
        key = f"plots.vp-{plot.site}.{plot.scope}"
        registry.reserve(
            key,
            unit="pdf-path",
            definition="PDF path relative to the paper root",
            reason="PDF is not available",
        )
        if plot.error:
            errors.append({"source": plot.name, "error": plot.error})
        else:
            grouped[key].append(plot)
    for key, candidates in sorted(grouped.items()):
        first = candidates[0]
        pinned = canonical[first.site, first.protocol]
        chosen = [p for p in candidates if p.run in pinned]
        if not chosen:
            chosen = [
                p for p in candidates if p.run in preferred[first.site, first.scope]
            ]
        candidates = chosen or candidates
        identities = {(p.paper_path, p.size_bytes) for p in candidates}
        hashes = {p.receipt_sha256 for p in candidates if p.receipt_sha256}
        if len(identities) != 1 or len(hashes) > 1:
            errors.append(
                {
                    "source": [p.name for p in candidates],
                    "error": f"ambiguous PDF for {key}; no path selected",
                }
            )
            registry.reserve(key, reason="multiple PDF candidates disagree")
            continue
        plot = candidates[0]
        registry.put(
            key,
            plot.paper_path,
            unit="pdf-path",
            sources=(plot.name,),
            definition="existing PDF; copy reports/ unchanged into the paper root",
        )
        registry.evidence[plot.name] = {
            "path": plot.name,
            "paper_path": plot.paper_path,
            "size_bytes": plot.size_bytes,
            "sha256": None,
            "receipt_sha256": plot.receipt_sha256,
            "completion": plot.completion,
        }


def select_artifacts(artifacts):
    """Prefer canonical ZMap reports; keep repeated Mass probes out of baseline metrics."""
    canonical = defaultdict(set)
    repeat_ids = set()
    for a in artifacts:
        if a.phase == "rt-fi-merged" and a.data.get("zmap_id"):
            canonical[a.site, a.protocol].add(a.data["zmap_id"])
        if a.kind == "random-reproducibility-evaluation":
            repeat_ids.update(
                (a.site, row["measurement_id"])
                for row in a.data.get("repeats", [])
                if isinstance(row, dict) and "measurement_id" in row
            )
    groups = defaultdict(list)
    for a in artifacts:
        if (a.site, a.data.get("measurement_id")) in repeat_ids:
            continue
        groups[a.site, a.protocol, a.connection, a.phase, a.kind].append(a)
    selected, errors = [], []
    for group, candidates in sorted(groups.items()):
        pinned = canonical[group[:2]]
        preferred = [a for a in candidates if a.run in pinned]
        if preferred:
            candidates = preferred
        ids = {a.data.get("measurement_id") for a in candidates}
        if len(candidates) > 1 and len(ids) > 1:
            errors.append(
                {
                    "source": [a.name for a in candidates],
                    "error": "ambiguous measurement identity; no baseline guessed",
                }
            )
            continue
        summaries = {
            json.dumps(list(flatten(report_payload(a))), sort_keys=True)
            for a in candidates
        }
        if len(summaries) > 1:
            errors.append(
                {
                    "source": [a.name for a in candidates],
                    "error": "copies of the same report disagree; no value selected",
                }
            )
            continue
        # The same canonical figure can also be present in processed storage.
        candidates.sort(key=lambda a: ("/figures/" not in a.name, a.name))
        selected.append(candidates[0])
    return selected, errors


def flatten(data, path=()):
    """Export named scalar summaries, not sampled CDF points or per-IP observations."""
    if isinstance(data, dict):
        for key, value in sorted(data.items()):
            if key not in SKIP_FIELDS:
                yield from flatten(value, path + (component(key),))
    elif isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            identifiers = [
                (k, row[k])
                for k in (
                    "strategy",
                    "baseline",
                    "protocol_group",
                    "role",
                    "source",
                    "group",
                    "os_group",
                    "continent",
                    "protocol",
                    "sample",
                    "repeat_index",
                    "name",
                    "label",
                    "outcome",
                    "target",
                )
                if k in row and isinstance(row[k], (str, int))
            ]
            # Relationship rows can share baseline/group/outcome while differing
            # in these lists. Preserve array boundaries and punctuation losslessly.
            for field in ("shared_pairs", "compatible_partitions", "matching_pairs"):
                if field not in row:
                    continue
                value = row[field]
                if not isinstance(value, list) or any(
                    type(v) is not str for v in value
                ):
                    raise ValueError(f"invalid relationship identifier: {field}")
                encoded = json.dumps(value, ensure_ascii=True, separators=(",", ":"))
                identifiers.append((field, "json-" + encoded.encode("ascii").hex()))
            if identifiers:
                label = "-".join(
                    f"{component(k)}-{component(v)}" for k, v in identifiers
                )
                rest = {k: v for k, v in row.items() if k not in dict(identifiers)}
                yield from flatten(rest, path + (label,))
    elif isinstance(data, (str, int, float, bool)) or data is None:
        yield path, data


def base_key(domain, site, protocol, connection, phase):
    return f"{domain}.vp-{site}.{protocol}.{connection}.{phase}"


def scalar_unit(pointer):
    if (
        pointer[-1] in ("percent", "pct", "percentage")
        or pointer[-1].startswith(("percent-", "percentage-"))
        or any(p in ("percentages", "plot-percentages") for p in pointer)
        or pointer[-1].endswith(("-pct", "-percent", "-percentage"))
    ):
        return "percent"
    return ""


def reserve_catalog(registry):
    """Fixed primary metrics also exist for an entirely empty campaign."""
    catalog_path = Path(__file__).with_name("catalog.json")
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    if catalog.get("schema_version") != 1:
        raise ValueError("unsupported paper value catalog")
    for namespace in catalog["contexts"]:
        kind = namespace.rsplit(".", 1)[-1]
        plot_names = [kind] if kind in PLOT_KINDS else []
        if kind == "itdk-consistency-summary":
            plot_names.append("itdk-discordant-strategy-combinations")
        if kind == "caida-ripe-intersection-role-by-strategy":
            plot_names.append("caida-ripe-role-agreement")
        if kind == "random-reproducibility-evaluation":
            plot_names.append("random-reproducibility-summary")
        for plot_name in plot_names:
            for site in SITES:
                registry.reserve(
                    f"plots.vp-{site}.{namespace.rsplit('.', 1)[0]}.{plot_name}",
                    unit="pdf-path",
                    reason="PDF is not available",
                    definition="PDF path relative to the paper root",
                )
        for field, unit in catalog["fields"][kind].items():
            for site in SITES:
                registry.reserve(
                    f"reports.vp-{site}.{namespace}.{field}",
                    unit=unit,
                    definition="published report scalar; see source metadata",
                )
        claims = {}
        if kind in ("itdk-role-by-strategy", "ripe-atlas-role-by-strategy"):
            dataset = "itdk" if kind.startswith("itdk") else "ripe-atlas"
            claims = {
                f"{dataset}.transit-observed.count": "ips",
                f"{dataset}.transit-observed.share-pct": "percent",
            }
        elif kind == "operating-system-group-by-strategy":
            claims = {
                "os.matched.count": "ips",
                "os.matched-of-results.share-pct": "percent",
            }
            for section in (
                "general-purpose-os",
                "network-appliance-os",
                "embedded-rtos",
            ):
                claims[f"os.sections.{section}.count"] = "ips"
                claims[f"os.sections.{section}.share-pct"] = "percent"
            for field in catalog["fields"][kind]:
                if field.startswith("groups."):
                    group = field.split(".")[1]
                    claims[f"os.groups.{group}.within-section.share-pct"] = "percent"
        elif kind == "tcp-flags-by-strategy":
            claims = {
                "flags.constant.syn-ack-minus-rst.percentage-points": "percentage-points",
                "flags.constant.syn-ack-relative-to-rst.gain-pct": "percent",
            }
        for suffix, unit in claims.items():
            for site in SITES:
                registry.reserve(
                    f"claims.vp-{site}.{namespace.rsplit('.', 1)[0]}.{suffix}",
                    unit=unit,
                    definition="derived prose metric; source denominator required",
                )
    for site in SITES:
        registry.put(
            f"meta.vp-{site}.site", site, definition="measurement vantage point"
        )
        registry.reserve(
            f"plots.vp-{site}.interprotocol.target-population",
            unit="pdf-path",
            reason="PDF is not available",
        )
        for strategy in (
            "reflection",
            "constant",
            "single",
            "per-destination",
            "per-bucket",
            "multi",
            "random",
        ):
            for family in ("campaign", "upset"):
                registry.reserve(
                    f"plots.vp-{site}.interprotocol.{family}.{strategy}",
                    unit="pdf-path",
                    reason="PDF is not available",
                )
        for group in PROTOCOL_GROUPS:
            for strategy in (
                "REFLECTION",
                "CONSTANT",
                "SINGLE",
                "PER_DESTINATION",
                "PER_BUCKET",
                "MULTI",
                "RANDOM",
            ):
                prefix = f"interprotocol.vp-{site}.{group}.{component(strategy)}"
                registry.reserve(prefix + ".targets.count", unit="ips")
                for outcome in INTERPROTOCOL_OUTCOMES:
                    for metric, unit in (("count", "ips"), ("share-pct", "percent")):
                        registry.reserve(
                            prefix + "." + component(outcome) + "." + metric,
                            unit=unit,
                            definition="within selected baseline/protocol-group cohort",
                        )
        for protocol in PROTOCOLS:
            for metric in (
                "scan-targets.count",
                "resolved.count",
                "resolved.share-pct",
                "ambiguous.count",
                "ambiguous.share-pct",
                "unclassified.count",
                "unclassified.share-pct",
                "with-evidence.count",
            ):
                registry.reserve(
                    f"os.vp-{site}.{protocol}.{metric}",
                    unit="percent" if metric.endswith("share-pct") else "ips",
                    definition="OS status over this VP/protocol's unique discovery targets",
                    reason="requires regional shared-os-coverage.json",
                )
            modes = (
                ("no-connection", "connection")
                if protocol == "tcp"
                else ("no-connection",)
            )
            for mode in modes:
                for phase in PHASES:
                    result = base_key("results", site, protocol, mode, phase)
                    for population in (
                        "rt-results",
                        "successful-results",
                        "classified-results",
                    ):
                        registry.reserve(
                            f"{result}.{population}.total.count",
                            unit="ips",
                            definition=f"unique IPs in {population}",
                        )
                        for strategy in STRATEGIES:
                            for metric, unit in (
                                ("count", "ips"),
                                ("share-pct", "percent"),
                            ):
                                registry.reserve(
                                    f"{result}.{population}.{component(strategy)}.{metric}",
                                    unit=unit,
                                    definition=f"{strategy} among {population}; phase={phase}",
                                )
                    coverage = base_key("coverage", site, protocol, mode, phase)
                    for metric, unit in (
                        ("all-targets.count", "ips"),
                        ("successful.count", "ips"),
                        ("successful.share-pct", "percent"),
                        ("classified.count", "ips"),
                        ("classified.share-pct", "percent"),
                    ):
                        registry.reserve(
                            f"{coverage}.{metric}",
                            unit=unit,
                            definition="coverage denominator: all intended targets",
                        )
                    for family, metrics in {
                        "constant": (
                            "zero.count",
                            "nonzero.count",
                            "zero.share-pct",
                            "nonzero.share-pct",
                            "zero-df0-only.count",
                            "zero-df1-only.count",
                            "zero-df-mixed.count",
                            "zero-df-unknown.count",
                            "zero-df0-only.share-pct",
                            "zero-df1-only.share-pct",
                            "zero-df-mixed.share-pct",
                            "zero-df-unknown.share-pct",
                        ),
                        "ambiguity": (
                            "multiple-rule-hits.count",
                            "multiple-rule-hits.share-pct",
                        ),
                    }.items():
                        for metric in metrics:
                            registry.reserve(
                                f"{family}.vp-{site}.{protocol}.{mode}.{phase}.{metric}",
                                unit="percent"
                                if metric.endswith("share-pct")
                                else "ips",
                                reason="requires an explicit DF/constant or nonexclusive-rule report",
                            )
                    for strategy in (
                        "SINGLE",
                        "PER_DESTINATION",
                        "PER_CONNECTION",
                        "PER_BUCKET",
                    ):
                        prefix = f"increments.vp-{site}.{protocol}.{mode}.{phase}.{component(strategy)}"
                        registry.reserve(prefix + ".total.count", unit="increments")
                        for threshold in (1000, 2000):
                            registry.reserve(
                                prefix + f".below-{threshold}.count", unit="increments"
                            )
                            registry.reserve(
                                f"increments.vp-{site}.{protocol}.{mode}.{phase}."
                                f"{component(strategy)}.below-{threshold}.share-pct",
                                unit="percent",
                                definition=f"fraction of strategy-specific increments < {threshold}",
                                reason="exact increment counts required; sampled CDF is insufficient",
                            )
                for phase in ("rt-base", "fi-mass", "fi-base"):
                    for metric in (
                        "connection-count",
                        "requests-per-connection",
                        "maximum-tolerated-rtt",
                        "fixed-interval.request-interval",
                        "fixed-interval.minimum-reply-rate",
                        "number-of-inflight-probes",
                    ):
                        registry.reserve(
                            base_key("parameters", site, protocol, mode, phase)
                            + "."
                            + metric,
                            reason="requires the actual measurement snapshot",
                        )
            for technique in (
                "alias-resolution",
                "traffic-inference",
                "idle-scan",
                "sav",
                "rov",
            ):
                for metric in ("eligible.count", "eligible.share-pct", "targets.count"):
                    registry.reserve(
                        f"applications.vp-{site}.{protocol}.{technique}.{metric}",
                        unit="percent" if metric.endswith("share-pct") else "ips",
                        reason="requires technique-specific eligibility criteria and analysis",
                    )
    for left, right in itertools.combinations(SITES, 2):
        for protocol in PROTOCOLS:
            prefix = f"comparison.vp-{left}.vp-{right}.{protocol}.rt-fi-merged"
            for metric in (
                "overlap.count",
                "agreement.count",
                "agreement.share-pct",
                "time-separation-seconds",
            ):
                registry.reserve(
                    prefix + "." + metric,
                    unit="percent"
                    if metric.endswith("share-pct")
                    else ("seconds" if metric == "time-separation-seconds" else "ips"),
                    reason="requires a paired cross-VP analysis; marginals insufficient",
                )
            for source, target in itertools.product(STRATEGIES, repeat=2):
                registry.reserve(
                    f"{prefix}.transition.{component(source)}.{component(target)}.count"
                )
    for scenario in ("rt-ideal", "fi-ideal", "fi-lossy", "fi-reordered", "end-to-end"):
        for metric in ("accuracy-pct", "correct.count", "total.count"):
            registry.reserve(
                f"validation.pipeline.{scenario}.{metric}",
                unit="percent" if metric == "accuracy-pct" else "sequences",
                reason="requires a synthetic ground-truth validation report",
            )
        for truth in STRATEGIES:
            prefix = f"validation.pipeline.{scenario}.truth.{component(truth)}"
            registry.reserve(prefix + ".total.count", unit="sequences")
            registry.reserve(prefix + ".correct.share-pct", unit="percent")
            for predicted in STRATEGIES:
                registry.reserve(
                    prefix + f".predicted.{component(predicted)}.count",
                    unit="sequences",
                )
    for site in SITES:
        prefix = f"tcp-transition.vp-{site}.rt-no-connection-to-rt-connection"
        registry.reserve(prefix + ".overlap.count", unit="ips")
        for source, target in itertools.product(STRATEGIES, repeat=2):
            for metric in ("count", "row-share-pct"):
                registry.reserve(
                    prefix + f".{component(source)}.{component(target)}.{metric}",
                    unit="percent" if metric.endswith("pct") else "ips",
                    reason="requires paired TCP observations of the same addresses",
                )


def strategy_values(registry, artifact):
    data = artifact.data
    counts, total = data.get("counts"), data.get("total_ips")
    if not isinstance(counts, dict) or type(total) is not int or total < 0:
        raise ValueError("strategy report lacks nonnegative counts and total_ips")
    if any(
        k not in STRATEGIES or type(v) is not int or v < 0 for k, v in counts.items()
    ):
        raise ValueError("invalid strategy count")
    if sum(counts.values()) != total:
        raise ValueError("strategy counts do not sum to total_ips")
    prefix = base_key(
        "results", artifact.site, artifact.protocol, artifact.connection, artifact.phase
    )
    populations = {
        "successful-results": (
            total - counts.get("NOT_ENOUGH_SAMPLES", 0),
            {"NOT_ENOUGH_SAMPLES"},
        ),
        "classified-results": (
            total - counts.get("NOT_ENOUGH_SAMPLES", 0) - counts.get("UNCLASSIFIED", 0),
            {"NOT_ENOUGH_SAMPLES", "UNCLASSIFIED"},
        ),
    }
    # Only the merge retains all RT-result rows; never call a FI target set "RT results".
    if artifact.phase == "rt-fi-merged":
        populations["rt-results"] = (total, set())
    for population, (denominator, excluded) in populations.items():
        registry.put(
            f"{prefix}.{population}.total.count",
            denominator,
            unit="ips",
            definition=f"denominator: {population}",
            sources=(artifact.name,),
        )
        for strategy in STRATEGIES:
            if strategy in excluded:
                for metric in ("count", "share-pct"):
                    registry.reserve(
                        f"{prefix}.{population}.{component(strategy)}.{metric}",
                        reason="strategy is excluded from this population",
                    )
                continue
            count = counts.get(strategy, 0)
            for metric, raw, unit in (
                ("count", count, "ips"),
                ("share-pct", percentage(count, denominator), "percent"),
            ):
                registry.put(
                    f"{prefix}.{population}.{component(strategy)}.{metric}",
                    raw,
                    unit=unit,
                    definition=f"{strategy} among {population}",
                    sources=(artifact.name,),
                    reason="denominator is zero",
                )


def coverage_values(registry, artifact):
    data = artifact.data
    total, success = data.get("zmap_ip_count"), data.get("ipid_ip_count")
    if not all(type(n) is int and n >= 0 for n in (total, success)) or success > total:
        raise ValueError("invalid coverage numerator or denominator")
    prefix = base_key(
        "coverage",
        artifact.site,
        artifact.protocol,
        artifact.connection,
        artifact.phase,
    )
    for metric, raw, unit in (
        ("all-targets.count", total, "ips"),
        ("successful.count", success, "ips"),
        ("successful.share-pct", percentage(success, total), "percent"),
    ):
        registry.put(
            prefix + "." + metric,
            raw,
            unit=unit,
            definition="successful = enough samples; denominator = intended targets",
            sources=(artifact.name,),
            reason="denominator is zero",
        )


def report_payload(artifact):
    if artifact.kind == "missing-replies":
        rows = artifact.data.get("measurements", [])
        return {"sequences": [dict(row, name=row.get("target", "")) for row in rows]}
    return artifact.data


def report_claims(registry, artifact):
    """Derived prose quantities, with a named denominator rather than manual paper arithmetic."""
    data = artifact.data
    prefix = base_key(
        "claims", artifact.site, artifact.protocol, artifact.connection, artifact.phase
    )
    if artifact.kind in ("itdk-role-by-strategy", "ripe-atlas-role-by-strategy"):
        dataset = "itdk" if artifact.kind.startswith("itdk") else "ripe-atlas"
        count = data.get("plot_role_totals", {}).get("Transit-Observed")
        total = data.get("coverage", {}).get("total")
        if count is None or total is None:
            return
        if type(count) is not int or type(total) is not int or not 0 <= count <= total:
            raise ValueError("invalid transit-observed prose denominator")
        registry.put(
            prefix + f".{dataset}.transit-observed.count",
            count,
            unit="ips",
            sources=(artifact.name,),
            definition="ITDK/RIPE transit-observed report population",
        )
        registry.put(
            prefix + f".{dataset}.transit-observed.share-pct",
            percentage(count, total),
            unit="percent",
            sources=(artifact.name,),
            reason="empty report population",
            definition="transit-observed / all retained result IPs, including unmatched IPs",
        )
    elif artifact.kind == "operating-system-group-by-strategy":
        count, total = (
            data.get("matched_grouped_ip_count"),
            data.get("strategy_ip_count"),
        )
        if count is None or total is None:
            return
        if type(count) is not int or type(total) is not int or not 0 <= count <= total:
            raise ValueError("invalid OS-matched prose denominator")
        registry.put(
            prefix + ".os.matched.count",
            count,
            unit="ips",
            sources=(artifact.name,),
            definition="resolved OS groups joined to sufficient-sample strategy rows",
        )
        registry.put(
            prefix + ".os.matched-of-results.share-pct",
            percentage(count, total),
            unit="percent",
            sources=(artifact.name,),
            reason="empty strategy population",
            definition="matched grouped OS IPs / all rows in source strategy result",
        )
        sections = defaultdict(int)
        for group in data.get("groups", {}).values():
            sections[group["section"]] += group["ip_count"]
        if sum(sections.values()) != count:
            raise ValueError("OS group composition differs from the joined population")
        for section, n in sections.items():
            key = prefix + ".os.sections." + component(section)
            registry.put(
                key + ".count",
                n,
                unit="ips",
                sources=(artifact.name,),
                definition="resolved OS group IPs in this section",
            )
            registry.put(
                key + ".share-pct",
                percentage(n, count),
                unit="percent",
                sources=(artifact.name,),
                reason="no grouped OS IPs",
                definition="OS section / all matched grouped OS IPs",
            )
        for group, entry in data.get("groups", {}).items():
            key = prefix + ".os.groups." + component(group)
            registry.put(
                key + ".within-section.share-pct",
                percentage(entry["ip_count"], sections[entry["section"]]),
                unit="percent",
                sources=(artifact.name,),
                reason="empty OS section",
                definition="OS group / its general-purpose, network/appliance or embedded section",
            )
    elif artifact.kind == "tcp-flags-by-strategy":
        bars = data["bars"]
        shares = {}
        for label in ("SYN-ACK", "RST"):
            bar = bars[label]
            total, count = bar["total"], bar["counts"].get("CONSTANT", 0)
            if (
                type(total) is not int
                or type(count) is not int
                or not 0 <= count <= total
            ):
                raise ValueError("invalid TCP-response prose denominator")
            shares[label] = percentage(count, total)
        synack, rst = shares["SYN-ACK"], shares["RST"]
        difference = synack - rst if synack is not None and rst is not None else None
        registry.put(
            prefix + ".flags.constant.syn-ack-minus-rst.percentage-points",
            difference,
            unit="percentage-points",
            sources=(artifact.name,),
            reason="empty response class",
            definition="CONSTANT share in SYN-ACK minus share in RST; unpaired populations",
        )
        registry.put(
            prefix + ".flags.constant.syn-ack-relative-to-rst.gain-pct",
            percentage(difference, rst) if difference is not None else None,
            unit="percent",
            sources=(artifact.name,),
            reason="empty/zero RST reference share",
            definition="relative increase: (SYN-ACK CONSTANT share − RST share) / RST share",
        )


def os_coverage_values(registry, artifact):
    detail = artifact.data["detail"]
    total = detail["targets"]["total"]
    if type(total) is not int or total < 0:
        raise ValueError("invalid OS target population")
    prefix = f"os.vp-{artifact.site}.{artifact.protocol}"
    registry.put(
        prefix + ".scan-targets.count",
        total,
        unit="ips",
        sources=(artifact.name,),
        definition="unique discovery addresses scanned for OS from the shared OS VP",
    )
    for state in ("resolved", "ambiguous", "unclassified"):
        count = detail["classification"][state]
        if type(count) is not int or not 0 <= count <= total:
            raise ValueError("invalid OS status count")
        for metric, value, unit in (
            ("count", count, "ips"),
            ("share-pct", percentage(count, total), "percent"),
        ):
            registry.put(
                prefix + "." + state + "." + metric,
                value,
                unit=unit,
                sources=(artifact.name,),
                reason="no scanned targets",
                definition="OS status / all unique discovery targets; one status per IP",
            )
    evidence = detail["targets"]["with_evidence"]
    if type(evidence) is not int or not 0 <= evidence <= total:
        raise ValueError("invalid OS evidence count")
    registry.put(
        prefix + ".with-evidence.count",
        evidence,
        unit="ips",
        sources=(artifact.name,),
        definition="addresses with OS evidence, including ambiguous/unclassified",
    )


def interprotocol_values(registry, artifact):
    # v2 summary schema, not a call to the producer/classifier/plotting module.
    baselines = (
        "REFLECTION",
        "CONSTANT",
        "PER_DESTINATION",
        "SINGLE",
        "PER_BUCKET",
        "MULTI",
        "RANDOM",
    )

    def outcomes(strategy):
        if strategy in ("PER_DESTINATION", "SINGLE", "PER_BUCKET"):
            result = ["PROTOCOL_SHARED", "PARTIAL_PROTOCOL_SHARED", "PROTOCOL_ISOLATED"]
        elif strategy in ("REFLECTION", "CONSTANT"):
            result = [
                "SAME_PARAMETER",
                "PARTIALLY_MATCHING_PARAMETERS",
                "DIFFERENT_PARAMETERS",
            ]
        elif strategy == "MULTI":
            result = ["SAME_INTER_STRATEGY", "DIFFERENT_INTER_STRATEGY"]
        else:
            result = ["UNCHANGED"]
        return result + ["STRATEGY_CHANGED", "NOT_ENOUGH_SAMPLES"]

    data = artifact.data
    if data.get("version") != 2:
        raise ValueError("expected a v2 interprotocol campaign summary")
    all_rows = 0
    for group, summary in data["groups"].items():
        if group not in PROTOCOL_GROUPS:
            raise ValueError(f"unknown interprotocol group: {group}")
        counts = summary["counts"]
        allowed = {
            f"{strategy}:{outcome}"
            for strategy in baselines
            for outcome in outcomes(strategy)
        }
        if any(
            key not in allowed or type(v) is not int or v < 0
            for key, v in counts.items()
        ):
            raise ValueError("invalid interprotocol counts")
        if sum(counts.values()) != summary["rows"]:
            raise ValueError("interprotocol group counts do not sum to its row count")
        all_rows += summary["rows"]
        for strategy in baselines:
            categories = outcomes(strategy)
            total = sum(
                counts.get(f"{strategy}:{outcome}", 0) for outcome in categories
            )
            prefix = f"interprotocol.vp-{artifact.site}.{group}.{component(strategy)}"
            registry.put(
                prefix + ".targets.count",
                total,
                unit="ips",
                sources=(artifact.name,),
                definition="selected same-baseline cohort within this protocol group",
            )
            for outcome in categories:
                count = counts.get(f"{strategy}:{outcome}", 0)
                for metric, value, unit in (
                    ("count", count, "ips"),
                    ("share-pct", percentage(count, total), "percent"),
                ):
                    registry.put(
                        prefix + "." + component(outcome) + "." + metric,
                        value,
                        unit=unit,
                        sources=(artifact.name,),
                        reason="empty baseline cohort",
                        definition="outcome / selected cohort including insufficient replies",
                    )
    if all_rows != data["rows"]:
        raise ValueError("interprotocol campaign count differs from the group counts")


def aggregate_values(registry, artifact, processed_root, *, cached=False):
    """Read only small, existing plot aggregates; never rebuild joins from raw scans."""
    if not processed_root or artifact.kind not in (
        "operating-system-group-by-strategy",
        "strategy-intersection",
    ):
        return
    relative = artifact.relative
    if relative.startswith("figures/"):
        relative = relative[len("figures/") :]
    path = Path(processed_root) / artifact.site / Path(relative).with_suffix(".pq")
    if not path.is_file():
        return
    if registry.cache is not None and not cached:
        registry.cache.derive(
            registry,
            artifact,
            path,
            "aggregate",
            lambda: aggregate_values(registry, artifact, processed_root, cached=True),
        )
        return
    registry.record_file(path)
    import pyarrow.parquet as pq

    with pq.ParquetFile(path) as source:
        if source.metadata.num_rows > 100_000:
            raise ValueError(
                f"expected a small plot aggregate, not per-IP data: {path}"
            )
        columns = (
            ["OS_GROUP", "IPID_SELECTION_STRATEGY", "COUNT", "PERCENTAGE"]
            if artifact.kind == "operating-system-group-by-strategy"
            else None
        )
        rows = source.read(columns=columns).to_pylist()
    prefix = (
        base_key(
            "reports",
            artifact.site,
            artifact.protocol,
            artifact.connection,
            artifact.phase,
        )
        + "."
        + artifact.kind
    )
    if artifact.kind == "operating-system-group-by-strategy":
        for row in rows:
            key = (
                prefix
                + f".groups.{component(row['OS_GROUP'])}.strategies."
                + component(row["IPID_SELECTION_STRATEGY"])
            )
            for field, metric, unit in (
                ("COUNT", "count", "ips"),
                ("PERCENTAGE", "share-pct", "percent"),
            ):
                registry.put(
                    key + "." + metric,
                    row[field],
                    unit=unit,
                    definition="strategy within this resolved OS group; row denominator",
                    sources=(artifact.name, str(path)),
                )
    else:
        # Named row fields vary across plot generations. Do not silently guess a schema.
        for row in rows:
            identities = {
                key: value
                for key, value in row.items()
                if isinstance(value, str) and "STRATEGY" in key
            }
            if len(identities) != 2:
                raise ValueError(f"unsupported strategy-intersection aggregate: {path}")
            suffix = ".".join(
                f"{component(k)}-{component(v)}" for k, v in sorted(identities.items())
            )
            for field, value in row.items():
                if isinstance(value, (float, int)):
                    registry.put(
                        prefix + ".cells." + suffix + "." + component(field),
                        value,
                        unit="percent" if field == "PERCENTAGE" else "",
                        definition="paired RT/FI intersection cell; report row normalization",
                        sources=(artifact.name, str(path)),
                    )


def supplementary_coverage(registry, artifacts):
    """Some figure reports retain the original target count, even in a reports-only ZIP."""
    for a in artifacts:
        if a.kind not in ("tcp-flags-by-strategy", "measurement-type-by-strategy"):
            continue
        if a.kind == "tcp-flags-by-strategy":
            total, success, phase = (
                a.data.get("zmap_ip_count"),
                a.data.get("merged_ip_count"),
                "rt-base",
            )
        else:
            total = a.data.get("fixed_interval_target_ip_count")
            success = a.data.get("fixed_interval_result_ip_count")
            phase = "fi-mass"
        if total is None or success is None:
            continue  # Legacy reports may not retain these exact denominators.
        if (
            not all(type(n) is int and n >= 0 for n in (total, success))
            or success > total
        ):
            raise ValueError(f"invalid coverage retained by {a.name}")
        prefix = base_key("coverage", a.site, a.protocol, a.connection, phase)
        for metric, value, unit in (
            ("all-targets.count", total, "ips"),
            ("successful.count", success, "ips"),
            ("successful.share-pct", percentage(success, total), "percent"),
        ):
            key = prefix + "." + metric
            previous = registry.values.get(key)
            if previous and previous.status == "available":
                if previous.raw != value:
                    raise ValueError(f"coverage reports disagree for {key}")
                continue
            registry.put(
                key,
                value,
                unit=unit,
                sources=(a.name,),
                definition="successful = enough samples; original target denominator",
            )


def marginal_comparison(registry):
    """Compare VP distributions without treating VP marginals as paired observations."""
    for protocol in PROTOCOLS:
        for strategy in STRATEGIES:
            for population in (
                "rt-results",
                "successful-results",
                "classified-results",
            ):
                suffix = f"{protocol}.no-connection.rt-fi-merged.{population}.{component(strategy)}"
                observations = [
                    registry.values[f"results.vp-{site}.{suffix}.share-pct"]
                    for site in SITES
                ]
                available = [v for v in observations if v.status == "available"]
                prefix = f"comparison.all-vps.{suffix}"
                registry.put(
                    prefix + ".available-vps.count",
                    len(available),
                    unit="vps",
                    definition="number of VPs with this marginal proportion available",
                )
                for metric, unit in (
                    ("min-share-pct", "percent"),
                    ("max-share-pct", "percent"),
                    ("range-percentage-points", "percentage-points"),
                ):
                    registry.reserve(
                        prefix + "." + metric,
                        unit=unit,
                        reason="all six VP marginals are required",
                    )
                if len(available) == len(SITES):
                    low, high = (
                        min(v.raw for v in available),
                        max(v.raw for v in available),
                    )
                    for metric, value, unit in (
                        ("min-share-pct", low, "percent"),
                        ("max-share-pct", high, "percent"),
                        ("range-percentage-points", high - low, "percentage-points"),
                    ):
                        registry.put(
                            prefix + "." + metric,
                            value,
                            unit=unit,
                            definition="six marginal VP shares; not paired IP agreement",
                            sources=tuple(s for v in available for s in v.sources),
                        )


def derived_coverage(registry, artifacts):
    for artifact in artifacts:
        if artifact.kind != "strategies":
            continue
        counts = artifact.data["counts"]
        success = artifact.data["total_ips"] - counts.get("NOT_ENOUGH_SAMPLES", 0)
        classified = success - counts.get("UNCLASSIFIED", 0)
        prefix = base_key(
            "coverage",
            artifact.site,
            artifact.protocol,
            artifact.connection,
            artifact.phase,
        )
        source_prefix = (
            base_key(
                "coverage",
                artifact.site,
                artifact.protocol,
                artifact.connection,
                "rt-base",
            )
            if artifact.phase == "rt-fi-merged"
            else prefix
        )
        target = registry.values[source_prefix + ".all-targets.count"]
        if artifact.phase == "rt-fi-merged":
            rt_result = registry.values[source_prefix + ".successful.count"]
            if (
                rt_result.status == "available"
                and artifact.data["total_ips"] != rt_result.raw
            ):
                raise ValueError("merged population differs from RT coverage")
        metrics = [
            ("successful.count", success, "ips"),
            ("classified.count", classified, "ips"),
        ]
        if target.status == "available":
            total = target.raw
            if success > total:
                raise ValueError("coverage success count exceeds target count")
            metrics.extend(
                (
                    ("all-targets.count", total, "ips"),
                    ("successful.share-pct", percentage(success, total), "percent"),
                    ("classified.share-pct", percentage(classified, total), "percent"),
                )
            )
        for metric, raw, unit in metrics:
            previous = registry.values[prefix + "." + metric]
            if previous.status == "available":
                if previous.raw != raw:
                    raise ValueError("strategy population differs from coverage report")
                continue
            registry.put(
                prefix + "." + metric,
                raw,
                unit=unit,
                definition="enough-sample/classified results; denominator: original targets",
                sources=(artifact.name, *target.sources),
                reason="denominator is zero",
            )


def snapshot_values(registry, artifacts, raw_root, *, cached=False):
    if not raw_root:
        return
    for a in artifacts:
        mid = a.data.get("measurement_id")
        if (
            a.kind != "strategies"
            or not mid
            or a.phase not in ("rt-base", "fi-base", "fi-mass")
        ):
            continue
        if not re.fullmatch(r"[A-Za-z0-9_-]+", mid):
            raise ValueError(f"invalid measurement identity: {mid}")
        path = Path(raw_root) / a.site / "ipid" / mid / "ipid.snapshot.yaml"
        if not path.is_file():
            continue
        if registry.cache is not None and not cached:
            registry.cache.derive(
                registry,
                a,
                path,
                "snapshot",
                lambda: snapshot_values(registry, [a], raw_root, cached=True),
            )
            continue
        import yaml

        registry.record_file(path)
        data = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
        if not isinstance(data, dict):
            raise ValueError(f"invalid snapshot: {path}")
        prefix = base_key("parameters", a.site, a.protocol, a.connection, a.phase)
        for field in (
            "connection_count",
            "requests_per_connection",
            "maximum_tolerated_rtt",
            "number_of_inflight_probes",
            "fixed_interval.request_interval",
            "fixed_interval.minimum_reply_rate",
        ):
            value = data
            for part in field.split("."):
                value = value.get(part) if isinstance(value, dict) else None
            registry.put(
                prefix + "." + field.replace("_", "-"),
                value,
                sources=(str(path),),
                definition="actual measured configuration; not a software default",
            )


def extra_reports(registry, paths, campaign):
    """A small explicit schema for future DF, overlap, validation and paired-VP analyses."""
    for path in paths:
        document = json.loads(Path(path).read_text(encoding="utf-8-sig"))
        if (
            document.get("schema_version") != 1
            or document.get("campaign_id") != campaign
            or not isinstance(document.get("values"), dict)
        ):
            raise ValueError(f"invalid additional metric report: {path}")
        registry.record_file(path)
        for key, record in document["values"].items():
            if not isinstance(record, dict) or "definition" not in record:
                raise ValueError(f"{path}: each metric requires a definition")
            registry.put(
                key,
                record.get("value"),
                unit=record.get("unit", ""),
                definition=record["definition"],
                sources=(str(path),),
                reason=record.get("reason"),
            )


def formatted(value):
    if value.status != "available":
        return ""
    raw = value.raw
    if value.unit in ("percent", "percentage-points"):
        return f"{Decimal(str(raw)):.2f}" + ("%" if value.unit == "percent" else "")
    if isinstance(raw, bool):
        return "true" if raw else "false"
    if isinstance(raw, float):
        return format(raw, ".12g")
    return str(raw)


def tex_escape(text):
    mapping = {
        "\\": r"\textbackslash{}",
        "{": r"\{",
        "}": r"\}",
        "%": r"\%",
        "&": r"\&",
        "#": r"\#",
        "_": r"\_",
        "$": r"\$",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(mapping.get(c, c) for c in text)


def latex_value(value):
    if value.unit == "pdf-path" and value.status == "available":
        # A filename is not prose: preserve underscores and slashes as filename characters.
        return r"\detokenize{" + validate_plot_path(value.raw) + "}"
    return tex_escape(formatted(value))


def atomic_text(path, text):
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


def emit(registry, output_file, campaign, artifacts, errors):
    """Serialize only LaTeX here; export() separately persists its local cache."""
    values = dict(sorted(registry.values.items()))
    lines = [
        TEX_PREAMBLE.rstrip(),
        f"% Campaign: {campaign}; profile: {getattr(registry, 'profile', 'full')}",
    ]
    for key, value in values.items():
        note = value.reason if value.status != "available" else value.definition
        note = " ".join(note.split())
        lines.append(f"\\setval{{{key}}}{{{latex_value(value)}}} % {note}")
    status = {
        "schema_version": 1,
        "profile": getattr(registry, "profile", "full"),
        "paper_selection": getattr(registry, "paper_selection", None),
        "paper_references": getattr(registry, "paper_references", {}),
        "campaign_id": campaign,
        "main_vp": "eu",
        "sites": list(SITES),
        "available": sum(v.status == "available" for v in values.values()),
        "missing": sum(v.status == "missing" for v in values.values()),
        "input_errors": errors,
        "sources": [
            {
                "path": a.name,
                "sha256": a.sha256,
                "size_bytes": a.size_bytes,
                "completion": a.completion,
                "generated_at": a.data.get("generated_at"),
            }
            for a in artifacts
        ]
        + list(registry.evidence.values()),
        "values": {
            key: dict(asdict(value), formatted=formatted(value))
            for key, value in values.items()
        },
    }
    if status["profile"] == "paper":
        referenced = {source for value in values.values() for source in value.sources}
        status["sources"] = [
            source for source in status["sources"] if source["path"] in referenced
        ]
    atomic_text(output_file, "\n".join(lines) + "\n")
    return status


def fixed_output_path(root, input_roots, input_files, filename="values.tex"):
    """Only root/tmp/values.tex is writable; never follow a link into input data."""
    if filename not in ("values.tex", "values.json"):
        raise ValueError("unsupported export output")
    output = root / "tmp" / filename
    if output.is_symlink():
        raise ValueError(f"tmp/{filename} must not be a symbolic link")
    resolved = output.resolve()
    protected = [root / "reports", root / "data", root / "references"]
    protected.extend(Path(path) for path in input_roots if path is not None)
    for path in protected:
        directory = path.resolve()
        if resolved == directory or directory in resolved.parents:
            raise ValueError(
                "tmp/values.tex resolves inside analysis inputs; nothing written"
            )
    if any(
        resolved == Path(path).resolve() for path in input_files if path is not None
    ):
        raise ValueError("tmp/values.tex aliases an input file; nothing written")
    return output


def export(
    campaign,
    *,
    reports_root,
    processed_root=None,
    archive=None,
    raw_root=None,
    additional_reports=(),
    profile="paper",
    repository_root=None,
    strict=False,
    recompute=False,
    io_workers=8,
    use_cache=True,
):
    started = perf_counter()
    if not 1 <= io_workers <= 32:
        raise ValueError("io_workers must be between 1 and 32")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", campaign):
        raise ValueError("invalid campaign ID")
    if profile not in ("paper", "full"):
        raise ValueError("invalid value export profile")
    root = Path(repository_root or Path(__file__).resolve().parents[1]).resolve()
    output = fixed_output_path(
        root, (reports_root, processed_root, raw_root), (archive, *additional_reports)
    )
    # Local export: no FUSE writes, worker leases, deployment or network subprocesses.
    if Path("/proc/self/mountinfo").is_file():
        for line in (
            Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
        ):
            mount, filesystem = line.split(" - ", 1)
            point = Path(mount.split()[4].replace("\\040", " "))
            if filesystem.split()[0].startswith("fuse") and (
                output.resolve() == point or point in output.resolve().parents
            ):
                raise ValueError(
                    "choose a local output directory outside the S3 mounts"
                )
    cache_path = fixed_output_path(
        root,
        (reports_root, processed_root, raw_root),
        (archive, *additional_reports),
        "values.json",
    )
    cache = (
        ExportCache(
            cache_path,
            campaign,
            [reports_root, processed_root, raw_root, archive, *additional_reports],
            profile,
            recompute=recompute,
        )
        if use_cache
        else None
    )
    registry = Registry()
    registry.cache = cache
    reserve_catalog(registry)
    registry.put("meta.campaign-id", campaign, definition="immutable campaign identity")
    inputs = Inputs(
        (reports_root, processed_root),
        archive,
        cache=cache,
        workers=io_workers,
        profile=profile,
    )
    try:
        artifacts, conflicts = select_artifacts(list(inputs.artifacts(campaign)))
        plots = list(inputs.plots(campaign))
    finally:
        inputs.close()
    errors = inputs.errors + conflicts
    valid = []
    derive_start = perf_counter()
    for a in artifacts:
        transaction = Registry()
        transaction.cache = cache
        try:
            # Validate scientific primary reports before exposing their scalar fields.
            if a.kind == "strategies":
                strategy_values(transaction, a)
            elif a.kind == "coverage":
                coverage_values(transaction, a)
            elif a.kind == "os-coverage":
                os_coverage_values(transaction, a)
            elif a.kind == "interprotocol-campaign-summary":
                interprotocol_values(transaction, a)
            report_claims(transaction, a)
            prefix = (
                base_key("reports", a.site, a.protocol, a.connection, a.phase)
                + "."
                + a.kind
            )
            for pointer, value in flatten(report_payload(a)):
                if pointer:
                    transaction.put(
                        prefix + "." + ".".join(pointer),
                        value,
                        unit=scalar_unit(pointer),
                        definition=f"reported scalar: {a.kind}/{'/'.join(pointer)}",
                        sources=(a.name,),
                    )
            if profile != "paper" or a.kind != "strategy-intersection":
                aggregate_values(transaction, a, processed_root)
            registry.merge(transaction)
            valid.append(a)
        except (OSError, ValueError, TypeError, KeyError, ImportError) as exc:
            errors.append({"source": a.name, "error": str(exc)})
    supplementary_coverage(registry, valid)
    derived_coverage(registry, valid)
    try:
        snapshot_values(registry, valid, raw_root)
    except (OSError, ValueError, ImportError) as exc:
        errors.append({"source": str(raw_root), "error": str(exc)})
    marginal_comparison(registry)
    extra_reports(registry, additional_reports, campaign)
    plot_values(registry, plots, valid, errors)
    registry.mirror_missing_vps()
    from paper_values.profile import manuscript_metrics, select_manuscript_values

    manuscript_metrics(registry, valid, processed_root, errors)
    selected = select_manuscript_values(registry, SITES)
    if profile == "paper":
        registry = selected
    else:
        for key, value in selected.values.items():
            registry.values.setdefault(key, value)
        registry.paper_selection = selected.paper_selection
        registry.paper_references = selected.paper_references
    if strict and errors:
        details = "\n".join(f"{error['source']}: {error['error']}" for error in errors)
        raise ValueError(
            f"{len(errors)} invalid or ambiguous inputs; existing tmp/values.tex retained\n"
            + details
        )
    timings = {**inputs.timings, "derive": perf_counter() - derive_start}
    write_start = perf_counter()
    status = emit(registry, output, campaign, valid, errors)
    if cache is not None:
        cache.save(atomic_text)
    timings["write"] = perf_counter() - write_start
    timings["total"] = perf_counter() - started
    status["timings"] = timings
    status["cache"] = dict(cache.stats) if cache is not None else {}
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--reports-root", type=Path)
    parser.add_argument("--processed-root", type=Path)
    parser.add_argument("--raw-root", type=Path)
    parser.add_argument(
        "--archive", type=Path, help="read a reports ZIP without extraction"
    )
    parser.add_argument("--profile", choices=("paper", "full"), default="paper")
    parser.add_argument("--additional-report", type=Path, action="append", default=[])
    parser.add_argument(
        "--strict",
        action="store_true",
        help="reject invalid inputs before replacing values.tex; missing results are allowed",
    )
    parser.add_argument(
        "--recompute",
        action="store_true",
        help="ignore cache and refresh export-derived values",
    )
    parser.add_argument(
        "--io-workers",
        type=int,
        default=8,
        help="parallel independent reads (1-32; default 8)",
    )
    parser.add_argument(
        "--timings",
        action="store_true",
        help="print per-stage timings and cache hit counters",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    reports = args.reports_root or root / "reports" / "continental" / args.campaign
    processed = (
        args.processed_root or root / "data/processed/continental" / args.campaign
    )
    raw = args.raw_root or root / "data/raw/continental" / args.campaign
    if args.archive:
        reports, processed, raw = args.reports_root, args.processed_root, args.raw_root
    try:
        status = export(
            args.campaign,
            reports_root=reports,
            processed_root=processed,
            archive=args.archive,
            raw_root=raw,
            profile=args.profile,
            additional_reports=args.additional_report,
            strict=args.strict,
            recompute=args.recompute,
            io_workers=args.io_workers,
        )
    except (OSError, ValueError, zipfile.BadZipFile) as exc:
        parser.exit(2, f"paper-values: {exc}\n")
    print(
        f"Paper values: {status['available']} available, {status['missing']} empty; "
        f"{len(status['input_errors'])} input errors; output: {root / 'tmp/values.tex'}"
    )
    if args.timings:
        print(
            "Timings: "
            + ", ".join(
                f"{key}={value:.3f}s" for key, value in status["timings"].items()
            )
        )
        print("Cache: " + json.dumps(status["cache"], sort_keys=True))
    for error in status["input_errors"]:
        print(f"Unavailable input: {error['source']}: {error['error']}")


if __name__ == "__main__":
    main()
