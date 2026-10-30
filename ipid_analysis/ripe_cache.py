"""Manifest-last S3 cache using the workflow's existing s3cmd credentials."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis.artifacts import assert_complete, completion_path, publish_file
from ipid_analysis.runtime import scratch_root


def digest(path: Path) -> str:
    # file_digest() intentionally trusts existing completion-marker hashes.
    # Cache verification must compare those declarations with the actual bytes.
    before = path.stat()
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 << 20), b""):
            result.update(block)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError(f"RIPE cache changed while hashing: {path}")
    return result.hexdigest()


def read_metadata(path: Path) -> dict:
    assert_complete(path)
    marker = completion_path(path)
    if marker.is_file() and digest(path) != json.loads(marker.read_text(encoding="utf-8")).get(
        "sha256"
    ):
        raise ValueError("RIPE cache metadata checksum mismatch")
    info = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(info, dict):
        raise ValueError("RIPE cache metadata must be an object")
    return info


def validate_roles(
    parquet: Path, info: dict, version: str, *, identity: str | None = None
) -> None:
    """Validate old and new caches without changing their keys or requiring raw inputs."""
    if version not in {"2", "4"}:
        raise ValueError("RIPE cache version mismatch")
    if info.get("cache_version") != version:
        raise ValueError("RIPE cache version mismatch")
    if identity is not None and info.get("cache_identity") != identity:
        raise ValueError("RIPE cache identity mismatch")
    assert_complete(parquet)
    checksums = [info["parquet_sha256"]] if "parquet_sha256" in info else []
    marker = completion_path(parquet)
    if marker.is_file():
        checksums.append(json.loads(marker.read_text(encoding="utf-8")).get("sha256"))
    if checksums:
        actual = digest(parquet)
        if any(
            not isinstance(value, str)
            or not re.fullmatch("[a-f0-9]{64}", value)
            or actual != value
            for value in checksums
        ):
            raise ValueError("RIPE cache checksum mismatch")
    expected = [("IP_V4", pa.uint32()), ("T", pa.bool_()), ("D", pa.bool_())]
    if version != "2":
        expected.insert(1, ("IP_ADDR", pa.string()))
    data = pq.ParquetFile(parquet)
    if not data.schema_arrow.equals(pa.schema(expected), check_metadata=False):
        raise ValueError("RIPE cache schema mismatch")
    if "addresses" in info and data.metadata.num_rows != info["addresses"]:
        raise ValueError("RIPE cache row count mismatch")
    if "parquet_size_bytes" in info and parquet.stat().st_size != info["parquet_size_bytes"]:
        raise ValueError("RIPE cache size mismatch")
    if not checksums:
        # Markerless legacy files have no checksum to compare. Decode their
        # pages instead of accepting only a readable footer or rebuilding them.
        if sum(batch.num_rows for batch in data.iter_batches()) != data.metadata.num_rows:
            raise ValueError("RIPE cache decoded row count mismatch")


class RoleCache:
    def __init__(self):
        prefix = os.environ.get("IPID_ANALYSIS_RIPE_CACHE_S3")
        if prefix is None:
            workflow = os.environ.get(
                "IPID_ANALYSIS_WORKFLOW_ROOT", os.environ.get("IPID_ANALYSIS_S3_PREFIX", "")
            )
            prefix = workflow.rstrip("/") + "/ripe-atlas-cache" if workflow else ""
        if prefix and not prefix.startswith("s3://"):
            raise ValueError("IPID_ANALYSIS_RIPE_CACHE_S3 must be an s3:// URI")
        self.prefix = prefix.rstrip("/") + "/v4" if prefix else ""

    def _get(self, uri: str, path: Path) -> bool:
        result = subprocess.run(
            ["s3cmd", "get", "--force", "--no-progress", uri, str(path)],
            capture_output=True,
            check=False,
            text=True,
            timeout=1800,
        )
        if result.returncode:
            # s3cmd can report a missing key as a parameter error. Match only
            # the exact requested object, not arbitrary parameter failures.
            if (
                re.fullmatch(
                    r"ERROR: (?:S3 error: 404\b[^\r\n]*|NoSuchKey)", result.stderr.strip()
                )
                or result.stderr.strip()
                == f"ERROR: Parameter problem: Source object '{uri}' does not exist."
            ):
                return False
            raise RuntimeError(f"RIPE cache download failed: {result.stderr.strip()}")
        return True

    def _put(self, path: Path, uri: str) -> None:
        subprocess.run(
            ["s3cmd", "put", "--no-progress", str(path), uri],
            check=True,
            capture_output=True,
            text=True,
            timeout=1800,
        )

    def restore(self, key: str, parquet: Path, metadata: Path, version: str) -> bool:
        if not self.prefix:
            return False
        parquet.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch_root(), prefix=".ripe-cache-") as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            if not self._get(f"{self.prefix}/{key}/manifest.json", manifest):
                return False
            info = read_metadata(manifest)
            checksum = info.get("parquet_sha256", "")
            if info.get("cache_version") != version:
                return False
            if not isinstance(checksum, str) or not re.fullmatch("[a-f0-9]{64}", checksum):
                raise ValueError("RIPE cache manifest checksum is invalid")
            identity = (
                key.removeprefix("windows/")
                if version == "4" and key.startswith("windows/")
                else None
            )
            if identity is not None and info.get("cache_identity") != identity:
                raise ValueError("RIPE cache identity mismatch")
            data = root / "roles.pq"
            if not self._get(f"{self.prefix}/objects/{checksum}.pq", data):
                return False
            validate_roles(data, info, version, identity=identity)
            publish_file(data, parquet)
            publish_file(manifest, metadata)
        return True

    def publish(self, key: str, parquet: Path, metadata: Path) -> None:
        info = read_metadata(metadata)
        validate_roles(
            parquet,
            info,
            info.get("cache_version"),
            identity=(key.removeprefix("windows/") if key.startswith("windows/") else None),
        )
        if not self.prefix:
            return
        publication = f"{self.prefix}/{key}"
        if info.get("published_to") == publication:
            return
        checksum = digest(parquet)
        info["parquet_sha256"] = checksum
        info["parquet_size_bytes"] = parquet.stat().st_size
        info["published_to"] = publication
        with tempfile.TemporaryDirectory(dir=scratch_root(), prefix=".ripe-upload-") as directory:
            manifest = Path(directory) / "manifest.json"
            manifest.write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")
            self._put(parquet, f"{self.prefix}/objects/{checksum}.pq")
            # Content-addressed objects are immutable. The manifest is the commit marker.
            self._put(manifest, f"{self.prefix}/{key}/manifest.json")
            publish_file(manifest, metadata)
