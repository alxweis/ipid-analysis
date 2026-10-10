"""Shared helpers for scalable, cacheable network-role analyses."""

from __future__ import annotations

from collections.abc import Iterable
import json
from pathlib import Path

from loguru import logger

NETWORK_ROLE_ANALYSIS_VERSION = "2"


def sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def output_cache_is_current(
    *,
    metadata_path: Path,
    outputs: Iterable[Path | None],
    inputs: Iterable[Path],
    version: str = NETWORK_ROLE_ANALYSIS_VERSION,
) -> bool:
    """Return true when all outputs belong to this version and postdate inputs."""
    output_paths = [Path(path) for path in outputs if path is not None]
    input_paths = [Path(path) for path in inputs]
    if not output_paths or not all(path.is_file() for path in output_paths):
        return False
    if not all(path.is_file() for path in input_paths):
        return False
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return False
    if metadata.get("analysis_cache_version") != version:
        return False
    newest_input = max(path.stat().st_mtime_ns for path in input_paths)
    oldest_output = min(path.stat().st_mtime_ns for path in output_paths)
    return oldest_output >= newest_input


def log_cache_reuse(target: str, metadata_path: Path) -> None:
    logger.info(f"[{target}] reusing current network-role artifacts: {metadata_path}")


def role_count_label(label: str, count: int) -> str:
    return f"{label}\n(n={count:,})"
