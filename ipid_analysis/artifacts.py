"""Publish completed files without relying on FUSE rename or file existence."""

from __future__ import annotations

import atexit
from contextlib import contextmanager
from contextvars import ContextVar
from functools import lru_cache
import hashlib
from importlib.metadata import version
import json
import logging
import os
from pathlib import Path
import platform
import shutil
import sys

import pyarrow.parquet as pq

from ipid_analysis.runtime import (
    exclusive_lock,
    mounted_storage,
    persistent_roots,
    scratch_root,
    temporary_path,
    validate_storage,
)
from ipid_analysis.transport import native_put

_publications = ContextVar("ipid_publications", default=None)


@contextmanager
def publication_inventory():
    paths = []
    token = _publications.set(paths)
    try:
        yield paths
    finally:
        _publications.reset(token)


def completion_path(path: Path) -> Path:
    return path.parent / ".ipid-complete" / (path.name + ".json")


def _write_json(path: Path, value: dict, *, remote: bool = False) -> None:
    data = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    if remote:
        temporary = temporary_path(path)
        try:
            temporary.write_bytes(data)
            native_put(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".part")
        temporary.write_bytes(data)
        temporary.replace(path)


def assert_complete(path: Path) -> None:
    """Reject interrupted new publications; legacy artifacts remain supported."""
    marker = completion_path(path)
    if marker.is_file():
        try:
            metadata = json.loads(marker.read_text(encoding="utf-8"))
            stat = path.stat()
            if (
                metadata["status"] != "complete"
                or stat.st_size != metadata["size_bytes"]
                or ("mtime_ns" in metadata and stat.st_mtime_ns != metadata["mtime_ns"])
            ):
                raise ValueError("unfinished publication or changed size")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise ValueError(f"incomplete artifact {path}: {exc}") from exc


def fingerprint(path: Path) -> dict:
    assert_complete(path)
    stat = path.stat()
    result = {"path": str(path.resolve()), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
    marker = completion_path(path)
    if marker.is_file():
        result["sha256"] = json.loads(marker.read_text())["sha256"]
    return result


def fingerprints(paths) -> list[dict]:
    return [fingerprint(Path(path)) for path in paths]


@lru_cache(maxsize=64)
def _digest_at(path: str, size: int, mtime_ns: int) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            digest.update(block)
    stat = Path(path).stat()
    if (stat.st_size, stat.st_mtime_ns) != (size, mtime_ns):
        raise ValueError(f"file changed while hashing: {path}")
    return digest.hexdigest()


def file_digest(path: Path) -> str:
    assert_complete(path)
    marker = completion_path(path)
    if marker.is_file():
        return json.loads(marker.read_text())["sha256"]
    stat = path.stat()
    return _digest_at(str(path.resolve()), stat.st_size, stat.st_mtime_ns)


@lru_cache(maxsize=1)
def _revision() -> str:
    configured = os.environ.get("IPID_ANALYSIS_CODE_REVISION")
    if configured:
        return configured
    package = Path(__file__).resolve().parent
    digest = hashlib.sha256()
    for path in sorted(package.rglob("*.py")):
        digest.update(path.relative_to(package).as_posix().encode())
        digest.update(path.read_bytes())
    # The source digest also identifies editable or unpackaged installations.
    return "source-sha256:" + digest.hexdigest()


def provenance() -> dict:
    return {
        "software": {name: version(name) for name in ("duckdb", "pyarrow", "numpy", "scipy")},
        "code_revision": _revision(),
        "python": platform.python_version(),
        "storage_roots": json.loads(os.environ.get("IPID_ANALYSIS_S3_ROOTS", "{}")),
    }


def validate_ip_population(path: Path) -> int:
    """Validate the one-row-per-IP invariant at an external processing boundary."""
    from ipid_analysis.runtime import connect

    assert_complete(path)
    with connect() as con:
        count, distinct = con.execute(
            "SELECT count(*), count(DISTINCT nullif(trim(CAST(IP_ADDR AS VARCHAR)), '')) "
            "FROM read_parquet(?)",
            [str(path)],
        ).fetchone()
    if count != distinct:
        raise ValueError(f"{path}: duplicate, null or empty IP addresses")
    return count


def cache_current(path: Path, inputs, parameters: dict) -> bool:
    try:
        assert_complete(path)
        info = json.loads(completion_path(path).read_text())
        return (
            info.get("status") == "complete"
            and info.get("sources") == fingerprints(inputs)
            and info.get("parameters") == parameters
        )
    except (OSError, ValueError, TypeError, KeyError):
        return False


def publish_file(
    source: Path, destination: Path, *, inputs=(), parameters=None, expected_rows=None
) -> Path:
    """Validate first; mark remote writes in progress; commit metadata last.

    Local files retain atomic replace. Remote files use a known-size native PUT,
    never copy+delete rename or an unknown-length FUSE write stream.
    No raw or previously successful artifact is deleted by this function.
    """
    source, destination = Path(source), Path(destination)
    persistent = any(
        root.absolute() in destination.absolute().parents for root in persistent_roots()
    )
    remote = mounted_storage() and persistent
    if persistent:
        validate_storage()
    else:
        scratch_root()  # local error/control artifacts remain writable after a mount failure
    rows = None
    if destination.suffix in {".pq", ".parquet"}:
        rows = pq.ParquetFile(source).metadata.num_rows
        if expected_rows is not None and rows != expected_rows:
            raise ValueError(f"incomplete output: expected {expected_rows} rows, got {rows}")
    sources = fingerprints(inputs)
    if not remote:
        destination.parent.mkdir(parents=True, exist_ok=True)
    with exclusive_lock(str(destination.resolve())):
        checksum = file_digest(source)
        info = {
            "version": 1,
            "status": "complete",
            "size_bytes": source.stat().st_size,
            "sha256": checksum,
            "rows": rows,
            "sources": sources,
            "operation": "publication",
            "parameters": parameters or {},
            **provenance(),
        }
        marker = completion_path(destination)
        # Invalidate even a previous commit before modifying any canonical bytes.
        previous = None
        if marker.is_file():
            try:
                previous = json.loads(marker.read_text())
                if previous.get("status") == "writing":
                    previous = previous.get("previous")
            except ValueError:
                pass
        _write_json(
            marker, {"version": 1, "status": "writing", "previous": previous}, remote=remote
        )
        if remote:
            native_put(source, destination)
        else:
            temporary = destination.with_name(destination.name + ".publish.part")
            try:
                owned = source.parent == scratch_root() / "artifacts" and source.name.startswith(
                    f"artifact-{os.getpid()}-"
                )
                if owned and source.stat().st_dev == destination.parent.stat().st_dev:
                    source.replace(temporary)
                else:
                    shutil.copyfile(source, temporary)
                temporary.replace(destination)
            finally:
                temporary.unlink(missing_ok=True)
        info["mtime_ns"] = destination.stat().st_mtime_ns
        _write_json(marker, info, remote=remote)
        inventory = _publications.get()
        if inventory is not None:
            inventory.append(destination)
    return destination


@contextmanager
def atomic_output(destination: Path, *, inputs=(), parameters=None, expected_rows=None):
    """All computation happens on local scratch before touching final storage."""
    if any(root.absolute() in destination.absolute().parents for root in persistent_roots()):
        validate_storage()
    temporary = temporary_path(destination)
    try:
        yield temporary
        publish_file(
            temporary,
            destination,
            inputs=inputs,
            parameters=parameters,
            expected_rows=expected_rows,
        )
    finally:
        temporary.unlink(missing_ok=True)


def write_table(table, destination, **kwargs):
    with atomic_output(Path(destination), expected_rows=table.num_rows) as temporary:
        pq.write_table(table, temporary, **kwargs)


def write_csv(table, destination, **kwargs):
    import pyarrow.csv as csv

    with atomic_output(Path(destination)) as temporary:
        csv.write_csv(table, temporary, **kwargs)


def write_text(destination, text, **kwargs):
    with atomic_output(Path(destination)) as temporary:
        return temporary.write_text(text, **kwargs)


def save_figure(figure, destination, **kwargs):
    with atomic_output(Path(destination)) as temporary:
        figure.savefig(temporary, **kwargs)


def bind_dependencies(path: Path, inputs, parameters: dict) -> None:
    """Bind a completed aggregate/report to its sources without rewriting data."""
    assert_complete(path)
    marker = completion_path(path)
    info = json.loads(marker.read_text())
    info.update(sources=fingerprints(inputs), parameters=parameters)
    remote = mounted_storage() and any(
        root.resolve() in path.resolve().parents for root in persistent_roots()
    )
    _write_json(marker, info, remote=remote)


def adopt_existing(path: Path, *, inputs=(), parameters=None) -> None:
    """Record a separately validated legacy artifact without altering its bytes."""
    assert_complete(path)
    marker = completion_path(path)
    if marker.is_file():
        return
    with exclusive_lock(str(path.resolve())):
        stat = path.stat()
        info = {
            "version": 1,
            "status": "complete",
            "sha256": file_digest(path),
            "size_bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "rows": pq.read_metadata(path).num_rows
            if path.suffix in {".pq", ".parquet"}
            else None,
            "sources": fingerprints(inputs),
            "parameters": parameters or {},
            "operation": "validated-legacy-adoption",
            **provenance(),
        }
        remote = mounted_storage() and any(
            root.absolute() in path.absolute().parents for root in persistent_roots()
        )
        if remote:
            validate_storage()
        _write_json(marker, info, remote=remote)


_log_exit_registered = False


def local_log_handler(destination: Path) -> logging.FileHandler:
    """Keep live logs on POSIX scratch; publish flushed snapshots explicitly."""
    global _log_exit_registered
    handler = logging.FileHandler(temporary_path(destination), mode="w", encoding="utf-8")
    handler.ipid_destination = destination
    if not _log_exit_registered:
        atexit.register(publish_logs, strict=False)
        _log_exit_registered = True
    return handler


def publish_logs(*, strict=True):
    for handler in logging.getLogger().handlers:
        destination = getattr(handler, "ipid_destination", None)
        if destination is None:
            continue
        if not strict and not destination.parent.exists():
            continue
        try:
            handler.acquire()
            try:
                handler.flush()
                publish_file(Path(handler.baseFilename), destination)
            finally:
                handler.release()
        except Exception as exc:
            if strict:
                raise
            sys.stderr.write(
                f"log publication failed; local log retained at {handler.baseFilename}: {exc}\n"
            )
