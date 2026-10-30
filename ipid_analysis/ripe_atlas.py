"""Prepare a reproducible RIPE Atlas IPv4 traceroute role lookup.

The lookup covers the 28 complete UTC days preceding an IP-ID campaign.  Two
input modes are supported:

* sampled hourly files from the public RIPE Atlas daily dumps; and
* public result streams for explicitly configured measurement IDs.

Both inputs are persisted below ``data/raw/ripe-atlas`` and converted to the
same compact Parquet schema below ``data/processed/ripe-atlas``.
"""

from __future__ import annotations

import bz2
from collections.abc import Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from datetime import time as datetime_time
import hashlib
import ipaddress
import json
import multiprocessing
import os
from pathlib import Path
import re
import shutil
import socket
import threading
import time
from typing import TextIO
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import uuid
from zoneinfo import ZoneInfo

from loguru import logger
import pyarrow.parquet as pq
import typer

from ipid_analysis.artifacts import assert_complete, file_digest, publish_file
from ipid_analysis.downloads import download as download_http
from ipid_analysis.runtime import (
    available_memory,
    cleanup_work,
    connect,
    cpu_budget,
    directory_bytes,
    reserve_download,
    ripe_transfer_limit,
    ripe_transfer_root,
    scratch_root,
    start_run,
    temporary_path,
    work_path,
)

try:
    import orjson
except ImportError:  # pragma: no cover - compatibility for existing environments
    orjson = None

from ipid_analysis.config import PROCESSED_STORAGE_DIR as PROCESSED_DATA_DIR
from ipid_analysis.config import RAW_STORAGE_DIR as RAW_DATA_DIR
from ipid_analysis.ripe_bitmaps import ROLE_SCHEMA, RoleBitmaps, trace_roles
from ipid_analysis.ripe_cache import RoleCache, read_metadata, validate_roles

app = typer.Typer()

DEFAULT_SOURCE = "daily-dumps"
DEFAULT_LOOKBACK_DAYS = 28
# Twelve evenly distributed hours improve coverage while bounded parallel
# preprocessing keeps the one-time cache construction practical.
DEFAULT_DUMP_SAMPLES = 12
DEFAULT_PREPROCESS_WORKERS = 4
DEFAULT_DOWNLOAD_ATTEMPTS = 5
DEFAULT_DUMP_BASE_URL = "https://data-store.ripe.net/datasets/atlas-daily-dumps"
DEFAULT_API_BASE_URL = "https://atlas.ripe.net/api/v2"
DEFAULT_LOCK_TIMEOUT_SECONDS = 6 * 3600.0
DEFAULT_LOCK_POLL_SECONDS = 1.0
MALFORMED_LOCK_GRACE_SECONDS = 300.0
RIPE_CACHE_VERSION = "4"
RIPE_PART_CACHE_VERSION = "2"
RIPE_LOCK_VERSION = 1
_MEASUREMENT_TIMESTAMP_RE = re.compile(
    r"_(?P<stamp>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})(?:$|[^0-9])"
)
_DUMP_DAY_RE = re.compile(r'href=["\'](?P<day>\d{4}-\d{2}-\d{2})/["\']')


@dataclass(frozen=True)
class RipeWindow:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("RIPE window timestamps must be timezone-aware")
        if self.start >= self.end:
            raise ValueError("RIPE window start must precede end")

    @property
    def label(self) -> str:
        return f"{self.start.date().isoformat()}_{self.end.date().isoformat()}"


@dataclass(frozen=True)
class RipeAtlasDataset:
    source: str
    window: RipeWindow
    directory: Path
    roles_path: Path
    metadata_path: Path


def _parse_measurement_timestamp(value: str) -> datetime | None:
    match = _MEASUREMENT_TIMESTAMP_RE.search(value)
    if match is None:
        return None
    zone = ZoneInfo(os.environ.get("IPID_ANALYSIS_CAMPAIGN_TIMEZONE", "UTC"))
    local = datetime.strptime(match.group("stamp"), "%Y-%m-%d_%H-%M-%S").replace(tzinfo=zone)
    if local.utcoffset() != local.replace(fold=1).utcoffset():
        raise ValueError("ambiguous campaign timestamp; provide --ripe-campaign-start explicitly")
    return local.astimezone(timezone.utc)


def infer_campaign_start(manifest: dict) -> datetime:
    """Infer the campaign start from measurement IDs, preferring ZMap IDs."""
    zmap_starts: list[datetime] = []
    all_starts: list[datetime] = []

    def visit(value: object, *, zmap: bool = False) -> None:
        if isinstance(value, str):
            parsed = _parse_measurement_timestamp(value)
            if parsed is not None:
                all_starts.append(parsed)
                if zmap:
                    zmap_starts.append(parsed)
        elif isinstance(value, dict):
            for key, child in value.items():
                visit(child, zmap=key == "zmap")
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(manifest)
    candidates = zmap_starts or all_starts
    if not candidates:
        raise ValueError("could not infer campaign start from timestamped manifest IDs")
    return min(candidates)


def campaign_window(
    campaign_start: datetime,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> RipeWindow:
    """Return complete UTC days immediately preceding *campaign_start*."""
    if lookback_days < 1:
        raise ValueError("RIPE lookback must be at least one day")
    if campaign_start.tzinfo is None:
        campaign_start = campaign_start.replace(tzinfo=timezone.utc)
    utc_start = campaign_start.astimezone(timezone.utc)
    end = datetime.combine(utc_start.date(), datetime_time(), tzinfo=timezone.utc)
    return RipeWindow(start=end - timedelta(days=lookback_days), end=end)


def sampled_dump_slots(window: RipeWindow, samples: int) -> tuple[datetime, ...]:
    """Select evenly spaced days and rotating UTC hours within *window*."""
    days = (window.end.date() - window.start.date()).days
    if not 1 <= samples <= days * 24:
        raise ValueError(f"RIPE dump samples must be in [1, {days * 24}]")
    if samples == days * 24:
        return tuple(window.start + timedelta(hours=offset) for offset in range(samples))

    slots: list[datetime] = []
    used: set[datetime] = set()
    for index in range(samples):
        day_offset = min(int((index + 0.5) * days / samples), days - 1)
        hour = (index * 7) % 24
        slot = window.start + timedelta(days=day_offset, hours=hour)
        while slot in used:
            slot += timedelta(hours=1)
        if slot >= window.end:
            slot = window.end - timedelta(hours=1)
        used.add(slot)
        slots.append(slot)
    return tuple(slots)


def sampled_available_dump_slots(
    window: RipeWindow,
    samples: int,
    available_days: Iterable[date],
) -> tuple[datetime, ...]:
    """Select reproducible slots from available dump days inside *window*."""
    days = tuple(
        day
        for day in sorted(set(available_days))
        if window.start.date() <= day < window.end.date()
    )
    if not days:
        raise FileNotFoundError(
            "no RIPE Atlas daily dumps overlap the requested window "
            f"[{window.start.date()}, {window.end.date()}); use --source api with "
            "--measurement-ids-file, or provide --input-dir"
        )
    if not 1 <= samples <= len(days) * 24:
        raise ValueError(f"RIPE dump samples must be in [1, {len(days) * 24}]")

    slots: list[datetime] = []
    used: set[datetime] = set()
    for index in range(samples):
        day_index = min(int((index + 0.5) * len(days) / samples), len(days) - 1)
        hour = (index * 7) % 24
        attempt = 0
        while True:
            candidate_day = days[(day_index + attempt // 24) % len(days)]
            candidate_hour = (hour + attempt) % 24
            slot = datetime.combine(
                candidate_day,
                datetime_time(candidate_hour),
                tzinfo=timezone.utc,
            )
            if slot not in used:
                break
            attempt += 1
        used.add(slot)
        slots.append(slot)
    return tuple(slots)


def _dataset_paths(
    window: RipeWindow,
    source_key: str,
    *,
    processed_root: Path,
) -> RipeAtlasDataset:
    directory = Path(processed_root) / "ripe-atlas" / window.label / source_key
    return RipeAtlasDataset(
        source=source_key,
        window=window,
        directory=directory,
        roles_path=directory / "ipv4-roles.pq",
        metadata_path=directory / "source.json",
    )


def _boot_id() -> str | None:
    """Return the Linux boot ID used to reject locks left by an earlier boot."""
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    except OSError:
        return None


def _process_start_token(pid: int) -> str | None:
    """Return Linux process start ticks so a reused PID is not mistaken for the owner."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except OSError:
        return None
    # The command in field 2 is parenthesized and may contain spaces or ')'.
    closing_parenthesis = stat.rfind(")")
    if closing_parenthesis < 0:
        return None
    fields_from_state = stat[closing_parenthesis + 2 :].split()
    # starttime is field 22; the list above starts at field 3.
    return fields_from_state[19] if len(fields_from_state) > 19 else None


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        # Unlike POSIX, Windows implements os.kill(pid, 0) through
        # TerminateProcess.  Query the process handle instead so the liveness
        # check can never stop the lock owner.
        import ctypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.restype = ctypes.c_void_p
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        process_query_limited_information = 0x1000
        handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
        if handle:
            kernel32.CloseHandle(handle)
            return True
        # ERROR_INVALID_PARAMETER means that the PID does not exist.  Access
        # denied and unknown errors are treated conservatively as active.
        return ctypes.get_last_error() != 87
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        # Unknown platform-specific errors must not make us steal a possibly
        # active lock.
        return True
    return True


def _new_lock_owner() -> dict[str, object]:
    pid = os.getpid()
    return {
        "version": RIPE_LOCK_VERSION,
        "token": uuid.uuid4().hex,
        "pid": pid,
        "hostname": socket.gethostname(),
        "boot_id": _boot_id(),
        "process_start": _process_start_token(pid),
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def _read_lock_owner(path: Path) -> tuple[dict[str, object] | None, str]:
    try:
        content = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        return None, f"unreadable ({exc})"
    try:
        owner = json.loads(content)
    except (json.JSONDecodeError, TypeError):
        # PR #97 wrote this legacy format. Keep it recoverable after upgrade.
        match = re.fullmatch(r"pid=(\d+)", content)
        if match is None:
            return None, "malformed"
        owner = {"version": 0, "pid": int(match.group(1))}
    if not isinstance(owner, dict) or not isinstance(owner.get("pid"), int):
        return None, "malformed"
    return owner, "parsed"


def _lock_owner_state(owner: dict[str, object] | None) -> tuple[str, str]:
    """Return active, stale, foreign, or malformed plus a diagnostic owner string."""
    if owner is None:
        return "malformed", "unknown owner"
    pid = int(owner["pid"])
    hostname = owner.get("hostname")
    description = f"pid={pid}"
    if hostname:
        description += f", host={hostname}"
        if hostname != socket.gethostname():
            return "foreign", description

    current_boot_id = _boot_id()
    owner_boot_id = owner.get("boot_id")
    if owner_boot_id and current_boot_id and owner_boot_id != current_boot_id:
        return "stale", f"{description}, previous boot"
    if not _pid_is_alive(pid):
        return "stale", f"{description}, process absent"

    owner_start = owner.get("process_start")
    current_start = _process_start_token(pid)
    if owner_start and current_start and owner_start != current_start:
        return "stale", f"{description}, PID reused"
    return "active", description


def _unlink_same_file(path: Path, expected_stat: os.stat_result) -> bool:
    """Unlink only if *path* still names the inode that was inspected/opened."""
    try:
        current_stat = path.stat()
        if not os.path.samestat(expected_stat, current_stat):
            return False
        path.unlink()
    except FileNotFoundError:
        return False
    return True


def _unlink_owned_lock(
    path: Path,
    expected_stat: os.stat_result,
    expected_token: str,
) -> bool:
    """Release only the lock file created by this context manager."""
    owner, _ = _read_lock_owner(path)
    if owner is None or owner.get("token") != expected_token:
        return False
    return _unlink_same_file(path, expected_stat)


@contextmanager
def _exclusive_lock(
    path: Path,
    timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
    *,
    poll_seconds: float = DEFAULT_LOCK_POLL_SECONDS,
    malformed_grace_seconds: float = MALFORMED_LOCK_GRACE_SECONDS,
):
    """Acquire a portable exclusive lock and recover owners that no longer exist."""
    if timeout_seconds < 0 or poll_seconds <= 0 or malformed_grace_seconds < 0:
        raise ValueError("invalid RIPE lock timing configuration")
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    acquired_stat: os.stat_result | None = None
    owner = _new_lock_owner()
    owner_token = str(owner["token"])
    payload = (json.dumps(owner, sort_keys=True) + "\n").encode()
    last_description = "unknown owner"
    next_progress_log = time.monotonic() + 60.0
    while True:
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            try:
                existing_stat = path.stat()
            except FileNotFoundError:
                continue
            existing_owner, parse_status = _read_lock_owner(path)
            state, description = _lock_owner_state(existing_owner)
            age_seconds = max(0.0, time.time() - existing_stat.st_mtime)
            last_description = f"{description}, state={state}, age={age_seconds:.0f}s"
            recover = state == "stale" or (
                state == "malformed" and age_seconds >= malformed_grace_seconds
            )
            if recover and _unlink_same_file(path, existing_stat):
                logger.warning(
                    f"recovered stale RIPE preparation lock {path} ({last_description})"
                )
                continue

            now = time.monotonic()
            if now >= deadline:
                raise TimeoutError(
                    f"timed out waiting for RIPE preparation lock: {path} "
                    f"({last_description}; parse={parse_status})"
                )
            if now >= next_progress_log:
                logger.info(f"waiting for RIPE preparation lock {path} ({last_description})")
                next_progress_log = now + 60.0
            time.sleep(min(poll_seconds, max(0.0, deadline - now)))
        else:
            acquired_stat = os.fstat(descriptor)
            try:
                os.write(descriptor, payload)
                os.fsync(descriptor)
            except Exception:
                os.close(descriptor)
                _unlink_same_file(path, acquired_stat)
                raise
            os.close(descriptor)
            # The lock is represented by the path, not by an open handle.
            # Closing here also permits safe replacement tests and recovery on
            # Windows, where an open file cannot be unlinked.
            break
    try:
        yield
    finally:
        if acquired_stat is not None:
            _unlink_owned_lock(path, acquired_stat, owner_token)


def _remote_file_size(url: str) -> int:
    request = Request(
        url,
        method="HEAD",
        headers={"User-Agent": "ipid-analysis/ripe-atlas"},
    )
    with urlopen(request, timeout=120) as response:
        value = response.headers.get("Content-Length")
    try:
        size = int(value)
    except (TypeError, ValueError) as exc:
        raise OSError(f"RIPE source did not report a valid Content-Length: {url}") from exc
    if size <= 0:
        raise OSError(f"RIPE source reported an invalid Content-Length ({size}): {url}")
    return size


def _download(
    url: str,
    destination: Path,
    *,
    resume=True,
    expected_size=None,
    max_attempts=DEFAULT_DOWNLOAD_ATTEMPTS,
) -> Path:
    # Persistent paths are never opened for append. Preserve API raw artifacts.
    local = destination
    if RAW_DATA_DIR.resolve() in destination.resolve().parents:
        local = work_path(url) / destination.name
    result = download_http(
        url,
        local,
        opener=urlopen,
        resume=resume,
        expected_size=expected_size,
        max_attempts=max_attempts,
        finalize=_finalize_download,
    )
    if local != destination:
        publish_file(result, destination, parameters={"source_url": url})
    return result


def _finalize_download(
    work_path: Path,
    destination: Path,
    expected_size: int | None,
) -> Path:
    if work_path == destination:
        return destination
    try:
        work_path.replace(destination)
        return destination
    except OSError:
        # Some object-storage FUSE mounts cannot rename objects reliably. A
        # complete, size-validated .part object is safe for immediate parsing
        # and can be reused by the next run without downloading it again.
        if work_path.is_file() and (
            expected_size is None or work_path.stat().st_size == expected_size
        ):
            logger.warning(
                f"RIPE download is complete but the storage mount could not rename "
                f"{work_path} to {destination}; using the validated partial object"
            )
            return work_path
        if destination.is_file() and (
            expected_size is None or destination.stat().st_size == expected_size
        ):
            return destination
        raise


def _available_dump_days(base_url: str) -> tuple[date, ...]:
    request = Request(
        f"{base_url.rstrip('/')}/",
        headers={"User-Agent": "ipid-analysis/ripe-atlas"},
    )
    with urlopen(request, timeout=120) as response:
        listing = response.read().decode("utf-8", errors="replace")
    days = tuple(
        sorted(
            {
                datetime.strptime(match.group("day"), "%Y-%m-%d").date()
                for match in _DUMP_DAY_RE.finditer(listing)
            }
        )
    )
    if not days:
        raise ValueError(f"no dated RIPE Atlas daily-dump directories found at {base_url}")
    return days


def _copy_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and source.resolve() == destination.resolve():
        return
    partial = temporary_path(destination)
    with source.open("rb") as input_file, partial.open("wb") as output_file:
        shutil.copyfileobj(input_file, output_file, length=8 * 1024 * 1024)
    publish_file(partial, destination)
    partial.unlink(missing_ok=True)


def _sha256(path: Path) -> str:
    return file_digest(path)


def _is_public_ipv4(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    return isinstance(address, ipaddress.IPv4Address) and address.is_global


def _iter_json_lines(stream: TextIO) -> Iterator[dict]:
    for number, line in enumerate(stream, start=1):
        if not line.strip():
            continue
        try:
            value = orjson.loads(line) if orjson is not None else json.loads(line)
        except (json.JSONDecodeError, ValueError) as exc:
            raise ValueError(f"invalid RIPE JSON on line {number}: {exc}") from exc
        if isinstance(value, dict):
            yield value


def _open_json_lines(path: Path) -> AbstractContextManager[TextIO]:
    if ".bz2" in path.suffixes:
        return bz2.open(path, "rt", encoding="utf-8", errors="strict")
    return path.open("rt", encoding="utf-8", errors="strict")


_PART_SCHEMA = ROLE_SCHEMA


def _parse_role_file(path: Path, *, window: RipeWindow, apply_window: bool = True):
    bitmaps = RoleBitmaps()
    traces = skipped_outside_window = skipped_non_ipv4 = 0
    start, end = int(window.start.timestamp()), int(window.end.timestamp())
    started = last_log = time.monotonic()
    logger.info(f"parsing RIPE Atlas traceroutes: {path}")
    try:
        with _open_json_lines(path) as stream:
            for number, record in enumerate(_iter_json_lines(stream), 1):
                if number % 100000 == 0 and time.monotonic() - last_log >= 30:
                    logger.info(
                        f"RIPE {path.name}: {number:,} records, {time.monotonic() - started:.0f}s"
                    )
                    last_log = time.monotonic()
                if record.get("af") != 4 or record.get("type", "traceroute") != "traceroute":
                    skipped_non_ipv4 += 1
                    continue
                timestamp = record.get("timestamp", record.get("endtime"))
                if not isinstance(timestamp, (int, float)):
                    continue
                if apply_window and not start <= timestamp < end:
                    skipped_outside_window += 1
                    continue
                addresses = trace_roles(record)
                if addresses:
                    traces += 1
                for address, (transit, destination) in addresses.items():
                    bitmaps.add(address, transit, destination)
    except BaseException:
        bitmaps.close()
        raise
    return bitmaps, {
        "traces": traces,
        "skipped_outside_window": skipped_outside_window,
        "skipped_non_ipv4": skipped_non_ipv4,
        "parse_seconds": time.monotonic() - started,
    }


def _part_cache_key(path: Path, window: RipeWindow, *, apply_window: bool) -> str:
    identity = [RIPE_PART_CACHE_VERSION, _sha256(path)]
    if apply_window:
        identity.extend((window.start.isoformat(), window.end.isoformat()))
    return hashlib.sha256("|".join(identity).encode()).hexdigest()


def _build_role_part(
    path: Path,
    part: Path,
    metadata: Path,
    window: RipeWindow,
    *,
    apply_window: bool,
) -> dict:
    started = time.monotonic()
    bitmaps, counters = _parse_role_file(path, window=window, apply_window=apply_window)
    part.parent.mkdir(parents=True, exist_ok=True)
    temporary = temporary_path(part)
    metadata_temporary = temporary_path(metadata)
    rows = 0
    try:
        with pq.ParquetWriter(temporary, _PART_SCHEMA, compression="zstd") as writer:
            for table in bitmaps.batches():
                writer.write_table(table, row_group_size=262144)
                rows += table.num_rows
        info = {
            "cache_version": RIPE_PART_CACHE_VERSION,
            "source": str(path),
            "source_size": path.stat().st_size,
            "source_sha256": _sha256(path),
            "window_filter_applied": apply_window,
            "addresses": rows,
            **counters,
            "prepare_seconds": time.monotonic() - started,
        }
        metadata_temporary.write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")
        publish_file(
            temporary,
            part,
            parameters={"cache_version": RIPE_PART_CACHE_VERSION},
            expected_rows=rows,
        )
        publish_file(metadata_temporary, metadata)
    finally:
        bitmaps.close()
        temporary.unlink(missing_ok=True)
        metadata_temporary.unlink(missing_ok=True)
    logger.info(
        f"prepared RIPE part {path.name}: {rows:,} addresses in {info['prepare_seconds']:.1f}s"
    )
    return info


def _cached_role_part(part: Path, metadata: Path) -> dict | None:
    """Return validated metadata for a complete reusable part."""
    if not part.is_file() or not metadata.is_file():
        return None
    try:
        info = read_metadata(metadata)
        validate_roles(part, info, RIPE_PART_CACHE_VERSION)
    except (FileNotFoundError, ValueError, TypeError):
        return None
    return info


def _cache_lock_path(identity: str, name: str) -> Path:
    # Unlike work_path(), this does not follow the scheduled job's TMP_DIR.
    key = hashlib.sha256(identity.encode()).hexdigest()
    return ripe_transfer_root() / "locks" / f"{key}-{name}"


def _prune_stale_cache_locks() -> None:
    """The preparation owner reclaims dead owners from older window/part keys."""
    for path in (ripe_transfer_root() / "locks").glob("*.lock"):
        if path.is_symlink() or not re.fullmatch(
            "[a-f0-9]{64}-(part|prepare|resources)\\.lock", path.name
        ):
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        owner, _ = _read_lock_owner(path)
        if owner and owner.get("pid") == os.getpid():
            continue
        state, _ = _lock_owner_state(owner)
        if state == "stale" or (
            state == "malformed" and time.time() - stat.st_mtime >= MALFORMED_LOCK_GRACE_SECONDS
        ):
            _unlink_same_file(path, stat)


def _release_transfer(source: Path) -> None:
    """Only reclaim a newly owned transfer after its processed part is committed."""
    root = ripe_transfer_root() / "downloads"
    if root not in source.resolve().parents:
        source.unlink(missing_ok=True)
        return
    final = source.with_suffix("") if source.suffix == ".part" else source
    final.unlink(missing_ok=True)
    final.with_suffix(final.suffix + ".part").unlink(missing_ok=True)
    final.with_suffix(final.suffix + ".part.http.json").unlink(missing_ok=True)
    try:
        source.parent.rmdir()
    except OSError:
        pass


def _prepare_role_part(arguments: tuple[str, str, str, str, str, bool]) -> tuple[str, dict]:
    source, part, metadata, start, end, apply_window, *options = arguments
    part_path = Path(part)
    metadata_path = Path(metadata)
    with _exclusive_lock(_cache_lock_path(str(part_path), "part.lock")):
        info = _cached_role_part(part_path, metadata_path)
        cache = RoleCache()
        remote_key = f"parts/{part_path.stem}"
        if info is None and cache.restore(
            remote_key, part_path, metadata_path, RIPE_PART_CACHE_VERSION
        ):
            info = _cached_role_part(part_path, metadata_path)
        if info is None:
            info = _build_role_part(
                Path(source),
                part_path,
                metadata_path,
                RipeWindow(datetime.fromisoformat(start), datetime.fromisoformat(end)),
                apply_window=apply_window,
            )
        cache.publish(remote_key, part_path, metadata_path)
    if options and options[0]:
        _release_transfer(Path(source))
    return part, info


def _sql_paths(paths: Iterable[Path]) -> str:
    values = ", ".join(f"'{str(path).replace(chr(39), chr(39) * 2)}'" for path in paths)
    return f"[{values}]"


def build_role_lookup(
    files: Iterable[Path],
    destination: Path,
    *,
    window: RipeWindow,
    source: str,
    workers: int = DEFAULT_PREPROCESS_WORKERS,
) -> dict[str, int]:
    """Aggregate RIPE dumps through reusable per-file Parquet parts."""
    if workers < 1:
        raise ValueError("RIPE preprocessing workers must be positive")
    # Keep CPUs available to the measurement workflow and cap all active prepares
    # on this VM with the outer preparation lock.
    budget_gib = min(
        int(os.environ.get("IPID_ANALYSIS_RIPE_MEMORY_GIB", "16")),
        max(1, available_memory() // (1 << 30)),
    )
    workers = min(workers, cpu_budget(), max(1, available_memory() // (2 << 30)))
    if budget_gib < 4:
        raise ValueError("RIPE memory budget must be at least 4 GiB")
    workers = min(workers, max(1, (os.cpu_count() or 2) - 1), (budget_gib - 2) // 2)
    apply_window = not source.startswith("daily-dumps")
    parts_root = destination.parents[2] / "file-parts"
    parts_root.mkdir(parents=True, exist_ok=True)
    prepared, metadata_rows = [], []
    started = time.monotonic()

    def collect(result):
        part, info = result
        prepared.append(Path(part))
        metadata_rows.append(info)

    with (
        ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn"))
        if workers > 1
        else _inline_executor()
    ) as executor:
        pending = set()
        for path in files:
            identity = None
            cleanup = False
            if isinstance(path, tuple):
                path, identity, cleanup = path
            path = Path(path)
            if path.suffix == ".pq":
                info = _cached_role_part(path, path.with_suffix(".json"))
                if info is None:
                    raise ValueError(f"invalid prepared RIPE part: {path}")
                collect((str(path), info))
                continue
            key = identity or _part_cache_key(path, window, apply_window=apply_window)
            arguments = (
                str(path),
                str(parts_root / f"{key}.pq"),
                str(parts_root / f"{key}.json"),
                window.start.isoformat(),
                window.end.isoformat(),
                apply_window,
                cleanup,
            )
            if workers == 1:
                collect(_prepare_role_part(arguments))
            else:
                pending.add(executor.submit(_prepare_role_part, arguments))
                if len(pending) >= workers:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        collect(future.result())
        for future in pending:
            collect(future.result())
    if not prepared:
        raise ValueError("RIPE Atlas input list is empty")
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = temporary_path(destination)
    partial.unlink(missing_ok=True)
    threads = min(
        max(1, (os.cpu_count() or 2) - 1),
        max(1, int(os.environ.get("IPID_ANALYSIS_RIPE_DUCKDB_THREADS", "4"))),
    )
    memory = os.environ.get("IPID_ANALYSIS_RIPE_DUCKDB_MEMORY", "8GB")
    temp = scratch_root() / "duckdb-ripe"
    logger.info(f"merging {len(prepared)} RIPE parts (threads={threads}, memory={memory})")
    with connect(
        config={"threads": threads, "memory_limit": memory, "temp_directory": str(temp)}
    ) as con:
        con.execute(f"""
            COPY (
                SELECT IP_V4,
                    concat((IP_V4::UBIGINT >> 24) & 255, '.', (IP_V4::UBIGINT >> 16) & 255,
                           '.', (IP_V4::UBIGINT >> 8) & 255, '.', IP_V4::UBIGINT & 255) AS IP_ADDR,
                    bool_or(T) AS T, bool_or(D) AS D
                FROM read_parquet({_sql_paths(prepared)}) GROUP BY IP_V4 ORDER BY IP_V4
            ) TO '{str(partial).replace(chr(39), chr(39) * 2)}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 262144)
        """)
        addresses, transit, dest = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE T), count(*) FILTER (WHERE D) "
            "FROM read_parquet(?)",
            [str(partial)],
        ).fetchone()
    publish_file(partial, destination)
    partial.unlink(missing_ok=True)
    return {
        "traces": sum(int(row["traces"]) for row in metadata_rows),
        "addresses": addresses,
        "transit_addresses": transit,
        "destination_addresses": dest,
        "skipped_outside_window": sum(int(row["skipped_outside_window"]) for row in metadata_rows),
        "skipped_non_ipv4": sum(int(row["skipped_non_ipv4"]) for row in metadata_rows),
        "preprocess_workers": min(workers, len(prepared)),
        "reusable_file_parts": len(prepared),
        "lookup_seconds": time.monotonic() - started,
        "files": [
            {
                "path": row["source"],
                "size": row["source_size"],
                "sha256": row["source_sha256"],
                "part": str(part),
            }
            for part, row in zip(prepared, metadata_rows)
        ],
    }


@contextmanager
def _inline_executor():
    yield None


def _parse_measurement_ids(path: Path | None) -> tuple[int, ...]:
    if path is None:
        raise ValueError("RIPE API source requires --measurement-ids-file")
    values = []
    for token in re.split(r"[\s,]+", Path(path).read_text(encoding="utf-8")):
        if not token:
            continue
        try:
            value = int(token)
        except ValueError as exc:
            raise ValueError(f"invalid RIPE measurement ID: {token!r}") from exc
        if value <= 0:
            raise ValueError(f"invalid RIPE measurement ID: {value}")
        values.append(value)
    if not values:
        raise ValueError("RIPE measurement ID file is empty")
    return tuple(sorted(set(values)))


def _source_key(source: str, *, samples: int, measurement_ids: tuple[int, ...]) -> str:
    if source == "daily-dumps":
        return f"daily-dumps-s{samples}"
    digest = hashlib.sha256(",".join(map(str, measurement_ids)).encode()).hexdigest()[:12]
    return f"api-{digest}"


class _TransferBudget:
    """Bound the shared raw staging area; the outer RIPE lock owns its writers."""

    def __init__(self, protected):
        self.root = ripe_transfer_root()
        self.limit = ripe_transfer_limit()
        self.protected = set(protected)
        self.reserved = {}
        self.lock = threading.Lock()
        self.failed = threading.Event()

    @contextmanager
    def reserve(self, destination, expected_size):
        # Leave room for HTTP metadata and lock files as well as dump bytes.
        overhead = 1 << 20
        if expected_size + overhead > self.limit:
            raise OSError(
                f"RIPE dump exceeds shared transfer budget: {destination.name} ({expected_size} bytes, limit={self.limit})"
            )
        deadline = time.monotonic() + DEFAULT_LOCK_TIMEOUT_SECONDS

        def present(path):
            partial = path.with_suffix(path.suffix + ".part")
            sizes = []
            for candidate in (path, partial):
                try:
                    sizes.append(candidate.stat().st_size)
                except FileNotFoundError:
                    pass
            return max(sizes, default=0)

        def additional(path, size):
            current = present(path)
            return max(0, size - current) + (overhead if current != size else 0)

        while True:
            with self.lock:
                needed = additional(destination, expected_size)
                if needed and self.failed.is_set():
                    raise OSError("RIPE prefetch stopped after another transfer failed")
                pending = sum(additional(path, size) for path, size in self.reserved.items())
                used = directory_bytes(self.root)
                if used + pending + needed > self.limit:
                    # Evict only inactive transfers owned by this new staging
                    # area. Current-window inputs and persistent RIPE caches
                    # are never evicted. A successful part frees its raw input.
                    downloads = self.root / "downloads"
                    candidates = sorted(
                        (
                            p
                            for p in downloads.glob("*")
                            if p.is_dir()
                            and not p.is_symlink()
                            and re.fullmatch("[a-f0-9]{64}", p.name)
                            and p.name not in self.protected
                        ),
                        key=lambda p: p.stat().st_mtime_ns,
                    )
                    for candidate in candidates:
                        cleanup_work(candidate, root=downloads)
                        used = directory_bytes(self.root)
                        if used + pending + needed <= self.limit:
                            break
                if used + pending + needed <= self.limit:
                    self.reserved[destination] = expected_size
                    break
                if not self.reserved and not any(
                    path.is_file()
                    for identity in self.protected
                    for path in (self.root / "downloads" / identity).glob("*.bz2")
                ):
                    # Only retained partials remain, with no downloader/parser
                    # able to free bytes. Keep them and report the budget issue
                    # rather than silently waiting for hours with no progress.
                    raise OSError(
                        "insufficient shared RIPE transfer budget; protected partials retained"
                    )
            if time.monotonic() >= deadline:
                raise TimeoutError("timed out waiting for bounded RIPE transfer space")
            time.sleep(0.1)
        try:
            yield
        finally:
            with self.lock:
                del self.reserved[destination]


def _daily_dump_files(
    window: RipeWindow,
    *,
    samples: int,
    raw_root: Path,
    base_url: str,
    parts_root: Path,
) -> tuple[Iterable, tuple[datetime, ...], tuple[date, ...]]:
    available_days = _available_dump_days(base_url)
    days_in_window = tuple(
        day for day in available_days if window.start.date() <= day < window.end.date()
    )
    slots = sampled_available_dump_slots(window, samples, available_days)
    urls = {
        slot: f"{base_url.rstrip('/')}/{slot:%Y-%m-%d}/traceroute-{slot:%Y-%m-%dT%H00}.bz2"
        for slot in slots
    }
    identities = {
        slot: hashlib.sha256(f"{RIPE_PART_CACHE_VERSION}|{url}".encode()).hexdigest()
        for slot, url in urls.items()
    }
    transfers = _TransferBudget(identities.values())
    requested_days = (window.end.date() - window.start.date()).days
    if len(days_in_window) < requested_days:
        logger.warning(
            "RIPE Atlas daily-dump retention covers only "
            f"{days_in_window[0]} through {days_in_window[-1]} "
            f"({len(days_in_window)}/{requested_days} requested UTC days); "
            f"sampling {len(slots)} available slots inside the requested window"
        )

    def fetch(slot):
        day = slot.date().isoformat()
        filename = f"traceroute-{slot:%Y-%m-%dT%H00}.bz2"
        url = urls[slot]
        identity = identities[slot]
        part, metadata = parts_root / f"{identity}.pq", parts_root / f"{identity}.json"
        with _exclusive_lock(_cache_lock_path(str(part), "part.lock")):
            info = _cached_role_part(part, metadata)
            cache = RoleCache()
            if info is None and cache.restore(
                f"parts/{identity}", part, metadata, RIPE_PART_CACHE_VERSION
            ):
                info = _cached_role_part(part, metadata)
            if info is not None:
                cache.publish(f"parts/{identity}", part, metadata)
                _release_transfer(transfers.root / "downloads" / identity / filename)
                logger.info(f"reusing processed RIPE hour: {filename}")
                return part
        existing = Path(raw_root) / "ripe-atlas" / "daily-dumps" / day / filename
        # Keep the explicitly configured legacy download directory usable, but
        # new transfers always use the bounded, job-independent local area.
        legacy = (
            Path(
                os.environ.get(
                    "IPID_ANALYSIS_RIPE_WORK_DIR", str(scratch_root() / "ripe-downloads")
                )
            )
            / day
            / filename
        )
        if not existing.is_file() and legacy.is_file():
            existing = legacy
        destination = (
            existing if existing.is_file() else transfers.root / "downloads" / identity / filename
        )
        state_path = destination.with_suffix(destination.suffix + ".part.http.json")
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError):
            state = {}
        if not isinstance(state, dict):
            state = {}
        complete_size = state.get("expected_size") if state.get("url") == url else None
        expected_size = (
            complete_size
            if isinstance(complete_size, int)
            and complete_size > 0
            and destination.is_file()
            and destination.stat().st_size == complete_size
            else _remote_file_size(url)
        )
        if existing.is_file() and existing.stat().st_size == expected_size:
            assert_complete(existing)
            return existing, identity, False
        # An invalid retained raw file is not a destination for new downloads.
        # Leave it untouched and stage the replacement on local storage only.
        destination = transfers.root / "downloads" / identity / filename
        started = time.monotonic()
        if destination.is_file() and destination.stat().st_size == expected_size:
            logger.info(f"reusing complete RIPE download: {destination}")
        else:
            logger.info(f"downloading RIPE Atlas daily dump: {url}")
        partial = destination.with_suffix(destination.suffix + ".part")
        present = max((p.stat().st_size for p in (destination, partial) if p.is_file()), default=0)
        with (
            transfers.reserve(destination, expected_size),
            reserve_download(max(0, expected_size - present) + (1 << 20)),
        ):
            destination.parent.mkdir(parents=True, exist_ok=True)
            source_path = _download(url, destination, expected_size=expected_size)
        logger.info(f"RIPE download ready: {filename} ({time.monotonic() - started:.1f}s)")
        return source_path, identity, True

    def inputs():
        # At most two downloaded inputs ahead of the bounded parser queue.
        concurrency = max(1, min(2, int(os.environ.get("IPID_ANALYSIS_RIPE_DOWNLOADS", "2"))))
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            remaining = iter(slots)
            pending = {pool.submit(fetch, slot) for slot in list(slots)[:concurrency]}
            error = None
            for _ in range(min(concurrency, len(slots))):
                next(remaining)
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    try:
                        result = future.result()
                    except Exception as exc:
                        error = error or exc
                        transfers.failed.set()
                    else:
                        yield result
                    slot = next(remaining, None) if error is None else None
                    if slot is not None:
                        pending.add(pool.submit(fetch, slot))
            if error is not None:
                raise error

    return inputs(), slots, days_in_window


def _api_files(
    window: RipeWindow,
    measurement_ids: tuple[int, ...],
    *,
    raw_root: Path,
    base_url: str,
) -> list[Path]:
    files = []
    day = window.start
    while day < window.end:
        next_day = day + timedelta(days=1)
        for measurement_id in measurement_ids:
            destination = (
                Path(raw_root)
                / "ripe-atlas"
                / "api"
                / str(measurement_id)
                / f"{day.date().isoformat()}.jsonl"
            )
            if not destination.is_file():
                query = urlencode(
                    {
                        "start": int(day.timestamp()),
                        "stop": int(next_day.timestamp()) - 1,
                        "format": "txt",
                    }
                )
                url = f"{base_url.rstrip('/')}/measurements/{measurement_id}/results/?{query}"
                logger.info(f"downloading RIPE Atlas measurement {measurement_id}: {day.date()}")
                source_path = _download(url, destination, resume=False)
            else:
                source_path = destination
            files.append(source_path)
        day = next_day
    return files


def _local_files(input_dir: Path, raw_root: Path, window: RipeWindow) -> list[Path]:
    source_files = sorted(
        path
        for path in Path(input_dir).rglob("*")
        if path.is_file() and (path.suffix == ".bz2" or path.suffix in {".jsonl", ".txt"})
    )
    if not source_files:
        raise FileNotFoundError(f"no RIPE .bz2/.jsonl/.txt files below {input_dir}")
    destination_dir = Path(raw_root) / "ripe-atlas" / "local" / window.label
    copied = []
    for index, source in enumerate(source_files):
        destination = destination_dir / f"{index:04d}-{_sha256(source)[:16]}-{source.name}"
        if not destination.is_file():
            _copy_atomic(source, destination)
        copied.append(destination)
    return copied


def prepare_ripe_atlas(
    *,
    campaign_start: datetime,
    source: str = DEFAULT_SOURCE,
    measurement_ids_file: Path | None = None,
    input_dir: Path | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    dump_samples: int = DEFAULT_DUMP_SAMPLES,
    preprocess_workers: int = DEFAULT_PREPROCESS_WORKERS,
    raw_root: Path = RAW_DATA_DIR,
    processed_root: Path = PROCESSED_DATA_DIR,
    dump_base_url: str = DEFAULT_DUMP_BASE_URL,
    api_base_url: str = DEFAULT_API_BASE_URL,
    force: bool = False,
) -> RipeAtlasDataset:
    """Download/import, aggregate, and return one RIPE role data set."""
    if source not in {"daily-dumps", "api"}:
        raise ValueError("RIPE source must be 'daily-dumps' or 'api'")
    window = campaign_window(campaign_start, lookback_days=lookback_days)
    measurement_ids = (
        _parse_measurement_ids(measurement_ids_file)
        if source == "api" and input_dir is None
        else ()
    )
    if input_dir is not None:
        local_sources = sorted(
            path
            for path in Path(input_dir).rglob("*")
            if path.is_file() and path.suffix in {".bz2", ".jsonl", ".txt"}
        )
        local_digest = hashlib.sha256(
            "\n".join(
                str(path.relative_to(input_dir)) + ":" + _sha256(path) for path in local_sources
            ).encode()
        ).hexdigest()[:12]
        key = f"local-{local_digest}"
    else:
        key = _source_key(source, samples=dump_samples, measurement_ids=measurement_ids)
    dataset = _dataset_paths(window, key, processed_root=processed_root)

    cache = RoleCache()
    identity = hashlib.sha256(
        f"{window.label}|{key}|{dump_base_url}|{api_base_url}".encode()
    ).hexdigest()
    remote_key = f"windows/{identity}"
    budget_lock = _cache_lock_path("ripe-resources", "resources.lock")
    with (
        _exclusive_lock(budget_lock),
        _exclusive_lock(_cache_lock_path(str(dataset.directory), "prepare.lock")),
    ):
        _prune_stale_cache_locks()
        dataset.directory.mkdir(parents=True, exist_ok=True)
        if not force and dataset.roles_path.is_file() and dataset.metadata_path.is_file():
            try:
                cached_info = read_metadata(dataset.metadata_path)
            except (FileNotFoundError, ValueError, TypeError):
                cached_info = {}
            if (
                cached_info.get("cache_version") == RIPE_CACHE_VERSION
                and cached_info.get("cache_identity") == identity
            ):
                try:
                    validate_roles(
                        dataset.roles_path, cached_info, RIPE_CACHE_VERSION, identity=identity
                    )
                except ValueError as exc:
                    logger.warning(f"invalid local RIPE window cache: {dataset.roles_path}: {exc}")
                else:
                    cache.publish(remote_key, dataset.roles_path, dataset.metadata_path)
                    logger.info(f"reusing RIPE Atlas role cache: {dataset.roles_path}")
                    return dataset
            logger.info(f"rebuilding outdated RIPE Atlas role cache: {dataset.roles_path}")

        if not force and cache.restore(
            remote_key, dataset.roles_path, dataset.metadata_path, RIPE_CACHE_VERSION
        ):
            logger.info(f"restored RIPE role cache from S3: {dataset.roles_path}")
            return dataset
        dump_slots: tuple[datetime, ...] = ()
        dump_days: tuple[date, ...] = ()
        if input_dir is not None:
            files = _local_files(input_dir, raw_root, window)
            source_kind = "local-files"
        elif source == "daily-dumps":
            files, dump_slots, dump_days = _daily_dump_files(
                window,
                samples=dump_samples,
                raw_root=raw_root,
                base_url=dump_base_url,
                parts_root=dataset.roles_path.parents[2] / "file-parts",
            )
            source_kind = "public-daily-dumps"
        else:
            files = _api_files(
                window,
                measurement_ids,
                raw_root=raw_root,
                base_url=api_base_url,
            )
            source_kind = "public-api"

        stats = build_role_lookup(
            files,
            dataset.roles_path,
            window=window,
            source=key,
            workers=preprocess_workers,
        )
        info = {
            "cache_version": RIPE_CACHE_VERSION,
            "cache_identity": identity,
            "source": source,
            "source_kind": source_kind,
            "campaign_start": campaign_start.astimezone(timezone.utc).isoformat(),
            "window_start": window.start.isoformat(),
            "window_end": window.end.isoformat(),
            "lookback_days": lookback_days,
            "dump_samples": dump_samples if source == "daily-dumps" else None,
            "preprocess_workers": preprocess_workers,
            "dump_base_url": dump_base_url if source == "daily-dumps" else None,
            "api_base_url": api_base_url if source == "api" else None,
            "sampled_dump_slots": (
                [slot.isoformat() for slot in dump_slots]
                if source == "daily-dumps" and input_dir is None
                else []
            ),
            "daily_dump_days_in_window": [day.isoformat() for day in dump_days],
            "daily_dump_available_day_count": len(dump_days),
            "daily_dump_requested_day_count": (
                (window.end.date() - window.start.date()).days
                if source == "daily-dumps" and input_dir is None
                else None
            ),
            "measurement_ids": list(measurement_ids),
            **stats,
            "prepared_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "measurement_id_timezone": os.environ.get("IPID_ANALYSIS_CAMPAIGN_TIMEZONE", "UTC"),
        }
        partial = temporary_path(dataset.metadata_path)
        partial.write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")
        publish_file(partial, dataset.metadata_path)
        partial.unlink(missing_ok=True)
        cache.publish(remote_key, dataset.roles_path, dataset.metadata_path)
        logger.success(
            f"prepared RIPE Atlas {window.label}: {stats['addresses']:,} public IPv4 "
            f"addresses from {stats['traces']:,} traceroutes"
        )
    return dataset


def parse_datetime(value: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


@app.command()
def main(
    campaign_start: str = typer.Option(..., help="IP-ID campaign start (ISO-8601, UTC)"),
    source: str = typer.Option(
        DEFAULT_SOURCE,
        envvar="IPID_ANALYSIS_RIPE_SOURCE",
        help="daily-dumps or api",
    ),
    measurement_ids_file: Path | None = typer.Option(
        None,
        envvar="IPID_ANALYSIS_RIPE_MEASUREMENT_IDS",
        help="text file with public traceroute measurement IDs (API source)",
    ),
    input_dir: Path | None = typer.Option(
        None,
        envvar="IPID_ANALYSIS_RIPE_INPUT_DIR",
        help="local directory with RIPE JSONL/BZip2 files instead of downloading",
    ),
    dump_samples: int = typer.Option(
        DEFAULT_DUMP_SAMPLES,
        min=1,
        help="evenly distributed hourly daily-dump files within the window",
    ),
    preprocess_workers: int = typer.Option(
        DEFAULT_PREPROCESS_WORKERS,
        min=1,
        envvar="IPID_ANALYSIS_RIPE_WORKERS",
        help="parallel RIPE dump preprocessing processes",
    ),
    raw_root: Path = typer.Option(RAW_DATA_DIR),
    processed_root: Path = typer.Option(PROCESSED_DATA_DIR),
    force: bool = typer.Option(False, help="rebuild an existing cache"),
) -> None:
    start_run()
    try:
        dataset = prepare_ripe_atlas(
            campaign_start=parse_datetime(campaign_start),
            source=source,
            measurement_ids_file=measurement_ids_file,
            input_dir=input_dir,
            dump_samples=dump_samples,
            preprocess_workers=preprocess_workers,
            raw_root=raw_root,
            processed_root=processed_root,
            force=force,
        )
    except (FileNotFoundError, HTTPError, OSError, TimeoutError, ValueError) as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1) from exc
    logger.success(f"RIPE Atlas lookup ready: {dataset.roles_path}")


if __name__ == "__main__":
    app()
