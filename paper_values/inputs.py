"""Read only required reports/PDFs, retaining canonical-run and receipt checks."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
from time import perf_counter
from typing import Any
import zipfile

SITES = ("af", "as", "au", "eu", "na", "sa")


def stamp(path):
    stat = Path(path).stat()
    return [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


PROTOCOLS = ("icmp", "tcp", "udp")
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


def component(value: Any) -> str:
    value = str(value).lower().replace("_", "-")
    value = re.sub(r"[^a-z0-9-]+", "-", value).strip("-")
    if not value:
        raise ValueError("empty key component")
    return value


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
    error: str | None = None


class Inputs:
    def __init__(self, paths=(), archive=None, *, workers=8, contexts=(), plot_contexts=()):
        self.reports_root = Path(paths[0]) if paths and paths[0] is not None else None
        self.paths = [Path(p) for p in paths if p is not None and Path(p).is_dir()]
        self.archive = zipfile.ZipFile(archive) if archive else None
        self.errors = []
        self.contexts = set(contexts)
        self.plot_contexts = set(plot_contexts)
        self.workers = 1 if archive else workers
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

    def read_payload(self, operation, name, origin, site, relative, read):
        """Check source freshness and optional receipts while reading once."""
        marker = self.read_marker(name, origin)
        before = self.fingerprint(name, origin, marker)
        size = before["source"][-1 if origin == "archive" else 0]
        payload = read(size)
        if self.fingerprint(name, origin, self.read_marker(name, origin)) != before:
            raise ValueError(f"{operation} changed while reading")
        if marker is not None:
            valid = (
                isinstance(marker, dict)
                and marker.get("status") == "complete"
                and marker.get("size_bytes") == payload["size_bytes"]
            )
            if valid:
                valid = (
                    marker.get("sha256") == payload["sha256"]
                    if operation == "metadata"
                    else re.fullmatch(r"[a-fA-F0-9]{64}", str(marker.get("sha256", "")))
                )
            if not valid:
                raise ValueError(
                    "completion receipt does not match artifact bytes"
                    if operation == "metadata"
                    else "PDF completion receipt does not match file metadata"
                )
        return payload, marker

    def load_artifact(self, entry):
        name, read, origin, site, relative = entry
        try:

            def metadata(size):
                content = read()
                return dict(
                    data=json.loads(content),
                    sha256=hashlib.sha256(content).hexdigest(),
                    size_bytes=len(content),
                )

            payload, marker = self.read_payload("metadata", name, origin, site, relative, metadata)
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
                    relative=relative,
                ),
                relative,
            )
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
            candidate = identify(Artifact(name, {}, "", 0, "", site), relative)
            if (
                candidate is None
                or (
                    candidate.protocol,
                    candidate.connection,
                    candidate.phase,
                    candidate.kind,
                )
                not in self.contexts
            ):
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
        name, site, relative, origin, _ = entry
        plot = identify_plot(name, site, relative, self.campaign)
        if plot is None or plot.scope not in self.plot_contexts:
            return None
        try:
            validate_plot_path(plot.paper_path)

            def signature(size):
                source = self.archive.open(name) if origin == "archive" else Path(name).open("rb")
                with source:
                    if source.read(5) != b"%PDF-":
                        raise ValueError("report file has no PDF signature")
                return dict(size_bytes=size)

            payload, marker = self.read_payload("pdf", name, origin, site, relative, signature)
            plot.size_bytes = payload["size_bytes"]
            plot.receipt_sha256 = marker["sha256"] if marker is not None else None
        except (OSError, ValueError, KeyError, zipfile.BadZipFile) as exc:
            plot.error = str(exc)
        return plot

    def plots(self, campaign):
        start = perf_counter()
        self.campaign = campaign
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
                    entries.append((info.filename, site, relative, "archive", info.file_size))
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
            e for e in entries if not any(p.startswith(".") for p in PurePosixPath(e[2]).parts)
        ]
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            yield from (plot for plot in pool.map(self.load_plot, entries) if plot is not None)
        self.timings["pdf"] = perf_counter() - start


def identify(artifact, relative, *, allow_unknown_kind=False):
    data = artifact.data
    protocol = str(data.get("protocol", "")).replace("udp-dns", "udp")
    parts = PurePosixPath(relative).parts
    if protocol not in PROTOCOLS and parts:
        token = parts[0] if parts[0] != "figures" else parts[1] if len(parts) > 1 else ""
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
        scope = "interprotocol." + stem.removeprefix("interprotocol-").replace("-", ".", 1)
        # Keep multi-word strategy names as a single component (e.g. per-bucket).
        protocol, run = "all-protocols", "interprotocol"
    elif stem == "target-population" and "interprotocol" in PurePosixPath(relative).parts:
        scope, protocol, run = (
            "interprotocol.target-population",
            "all-protocols",
            "interprotocol",
        )
    else:
        a = identify(Artifact(name, {}, "", 0, "", site), relative, allow_unknown_kind=True)
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
        if len(candidates) == 1:
            selected.append(candidates[0])
            continue
        summaries = {
            json.dumps(list(flatten(report_payload(a))), sort_keys=True) for a in candidates
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
                if not isinstance(value, list) or any(type(v) is not str for v in value):
                    raise ValueError(f"invalid relationship identifier: {field}")
                encoded = json.dumps(value, ensure_ascii=True, separators=(",", ":"))
                identifiers.append((field, "json-" + encoded.encode("ascii").hex()))
            if identifiers:
                label = "-".join(f"{component(k)}-{component(v)}" for k, v in identifiers)
                rest = {k: v for k, v in row.items() if k not in dict(identifiers)}
                yield from flatten(rest, path + (label,))
    elif isinstance(data, (str, int, float, bool)) or data is None:
        yield path, data


def report_payload(artifact):
    if artifact.kind == "missing-replies":
        rows = artifact.data.get("measurements", [])
        return {"sequences": [dict(row, name=row.get("target", "")) for row in rows]}
    return artifact.data
