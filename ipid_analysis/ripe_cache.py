"""Manifest-last S3 cache using the workflow's existing s3cmd credentials."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

import pyarrow.parquet as pq

from ipid_analysis.artifacts import assert_complete, file_digest, publish_file
from ipid_analysis.runtime import scratch_root


def digest(path: Path) -> str:
    return file_digest(path)


class RoleCache:
    def __init__(self):
        prefix = os.environ.get("IPID_ANALYSIS_RIPE_CACHE_S3")
        if prefix is None:
            workflow = os.environ.get("IPID_ANALYSIS_S3_PREFIX", "")
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
                "NoSuchKey" in result.stderr
                or "404" in result.stderr
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
            info = json.loads(manifest.read_text(encoding="utf-8"))
            checksum = info.get("parquet_sha256", "")
            if info.get("cache_version") != version or not re.fullmatch("[a-f0-9]{64}", checksum):
                return False
            data = root / "roles.pq"
            if not self._get(f"{self.prefix}/objects/{checksum}.pq", data):
                return False
            if digest(data) != checksum:
                raise ValueError("RIPE S3 cache checksum mismatch")
            # Reject truncated/non-Parquet data even when its manifest matches.
            names = pq.ParquetFile(data).schema_arrow.names
            expected = ["IP_V4", "T", "D"] if version == "2" else ["IP_V4", "IP_ADDR", "T", "D"]
            if names != expected:
                raise ValueError("RIPE S3 cache schema mismatch")
            publish_file(data, parquet)
            publish_file(manifest, metadata)
        return True

    def publish(self, key: str, parquet: Path, metadata: Path) -> None:
        if not self.prefix:
            return
        assert_complete(parquet)
        info = json.loads(metadata.read_text(encoding="utf-8"))
        publication = f"{self.prefix}/{key}"
        if info.get("published_to") == publication:
            return
        checksum = digest(parquet)
        info["parquet_sha256"] = checksum
        info["published_to"] = publication
        with tempfile.TemporaryDirectory(dir=scratch_root(), prefix=".ripe-upload-") as directory:
            manifest = Path(directory) / "manifest.json"
            manifest.write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")
            self._put(parquet, f"{self.prefix}/objects/{checksum}.pq")
            # Content-addressed objects are immutable. The manifest is the commit marker.
            self._put(manifest, f"{self.prefix}/{key}/manifest.json")
            publish_file(manifest, metadata)
