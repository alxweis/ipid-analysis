"""Pinned MaxMind inputs: durable S3 source, bounded per-job RAM working copy."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import tempfile

from loguru import logger

from ipid_analysis.config import REFERENCES_DIR
from ipid_analysis.runtime import (
    check_scratch_budget,
    exclusive_lock,
    mounted_storage,
    ram_scratch,
    reserve_download,
    scratch_free,
    scratch_root,
    validate_storage,
)

_CHUNK_BYTES = 1 << 20
_staged: dict[Path, dict] = {}


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(_CHUNK_BYTES), b""):
            digest.update(block)
    return digest.hexdigest()


def _metadata(path: Path) -> dict:
    import maxminddb

    try:
        with maxminddb.open_database(str(path)) as reader:
            metadata = reader.metadata()
            if metadata.database_type not in {
                "GeoLite2-Country",
                "GeoLite2-City",
                "GeoIP2-Country",
                "GeoIP2-City",
            }:
                raise ValueError("MaxMind continent plots need a Country or City database")
            return {
                "database_type": metadata.database_type,
                "build_epoch": metadata.build_epoch,
            }
    except maxminddb.InvalidDatabaseError as exc:
        raise ValueError(f"invalid MaxMind database: {path}") from exc


def _prepare_s3(uri: str) -> Path:
    # Requiring a digest prevents an overwritten S3 key from silently mixing
    # database versions between continents or between retries.
    expected = os.environ.get("IPID_MAXMIND_DB_SHA256", "").lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise ValueError("IPID_MAXMIND_DB_S3 requires IPID_MAXMIND_DB_SHA256 (64 hex digits)")
    version = os.environ.get("IPID_MAXMIND_DB_VERSION", "").strip()
    if not version or len(version) > 128:
        raise ValueError("IPID_MAXMIND_DB_S3 requires IPID_MAXMIND_DB_VERSION (1-128 characters)")
    limit = int(os.environ.get("IPID_MAXMIND_DB_MAX_BYTES", 256 << 20))
    if limit <= 0:
        raise ValueError("IPID_MAXMIND_DB_MAX_BYTES must be positive")
    if not uri.startswith("s3://") or not uri.endswith(".mmdb"):
        raise ValueError("IPID_MAXMIND_DB_S3 must identify an s3:// object ending in .mmdb")
    if not mounted_storage() or not ram_scratch():
        raise ValueError("MaxMind S3 staging requires mounted-s3 storage and RAM scratch")
    validate_storage()
    from ipid_analysis.s3_workflow import S3Client

    source = S3Client().mapped_path(uri)
    if source is None:
        raise ValueError("MaxMind S3 object must be under IPID_ANALYSIS_S3_ROOTS")
    root = scratch_root() / "maxmind"
    root.mkdir(exist_ok=True)
    target = root / f"{expected}.mmdb"
    # The lock covers concurrent callers using the same job directory. Separate
    # jobs deliberately own separate copies, charged to their own scratch limits.
    with exclusive_lock("maxmind:" + str(target)):
        if target.exists():
            if target.stat().st_size > limit or _digest(target) != expected:
                raise ValueError("cached MaxMind database differs from the pinned checksum/size")
            details = _metadata(target)
        else:
            size = source.stat().st_size
            if not 0 < size <= limit:
                raise ValueError("MaxMind database exceeds its configured size limit or is empty")
            with reserve_download(size):
                descriptor, name = tempfile.mkstemp(dir=root, prefix="download-", suffix=".part")
                partial = Path(name)
                try:
                    digest = hashlib.sha256()
                    copied = 0
                    with (
                        os.fdopen(descriptor, "wb", buffering=0) as output,
                        source.open("rb") as stream,
                    ):
                        for block in iter(lambda: stream.read(_CHUNK_BYTES), b""):
                            if copied + len(block) > size:
                                raise ValueError("MaxMind source grew during staging")
                            if scratch_free() < len(block):
                                raise OSError("insufficient RAM scratch for MaxMind database")
                            output.write(block)
                            digest.update(block)
                            copied += len(block)
                            check_scratch_budget()
                    if copied != size or digest.hexdigest() != expected:
                        raise ValueError("MaxMind database size or SHA-256 mismatch")
                    details = _metadata(partial)
                    check_scratch_budget()
                    partial.replace(target)
                finally:
                    partial.unlink(missing_ok=True)
        _staged[target.resolve()] = {
            "source_uri": uri,
            "version": version,
            "sha256": expected,
            "size_bytes": target.stat().st_size,
            **details,
        }
    logger.info(f"MaxMind RAM input ready: version={version}, sha256={expected}, source={uri}")
    return target


def default_database() -> Path | None:
    """Keep local overrides compatible; explicitly configured S3 fails closed."""
    configured = os.environ.get("IPID_MAXMIND_DB")
    if configured:
        path = Path(configured).expanduser()
        if path.is_file():
            return path
    uri = os.environ.get("IPID_MAXMIND_DB_S3")
    if uri:
        return _prepare_s3(uri)
    return next(
        (
            path
            for path in (
                REFERENCES_DIR / "GeoLite2-Country.mmdb",
                REFERENCES_DIR / "GeoLite2-City.mmdb",
                REFERENCES_DIR / "GeoIP2-Country.mmdb",
                REFERENCES_DIR / "GeoIP2-City.mmdb",
            )
            if path.is_file()
        ),
        None,
    )


def database_provenance(path: Path) -> dict:
    """Record content identity, including for legacy local or CLI overrides."""
    path = Path(path)
    staged = _staged.get(path.resolve())
    digest = _digest(path)
    if staged is not None:
        if digest != staged["sha256"]:
            raise ValueError("staged MaxMind database changed after preparation")
        return dict(staged)
    return {
        "source_path": str(path),
        "sha256": digest,
        "size_bytes": path.stat().st_size,
        **_metadata(path),
    }
