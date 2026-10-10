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
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from datetime import time as datetime_time
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import time
from typing import TextIO
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import typer

from ipid_analysis.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

app = typer.Typer()

DEFAULT_SOURCE = "daily-dumps"
DEFAULT_LOOKBACK_DAYS = 28
# Four evenly distributed hours keep automatic preparation practical: a full
# 28-day archive currently exceeds 1.5 TB of compressed traceroute data.
DEFAULT_DUMP_SAMPLES = 4
DEFAULT_DUMP_BASE_URL = "https://data-store.ripe.net/datasets/atlas-daily-dumps"
DEFAULT_API_BASE_URL = "https://atlas.ripe.net/api/v2"
RIPE_CACHE_VERSION = "2"
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


@dataclass
class _AddressStats:
    transit: bool = False
    destination: bool = False
    trace_count: int = 0
    probes: set[int] = field(default_factory=set)
    measurements: set[int] = field(default_factory=set)
    protocols: set[str] = field(default_factory=set)
    first_seen: int | None = None
    last_seen: int | None = None


def _parse_measurement_timestamp(value: str) -> datetime | None:
    match = _MEASUREMENT_TIMESTAMP_RE.search(value)
    if match is None:
        return None
    return datetime.strptime(match.group("stamp") + "+0000", "%Y-%m-%d_%H-%M-%S%z")


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


@contextmanager
def _exclusive_lock(path: Path, timeout_seconds: float = 3600.0):
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    descriptor: int | None = None
    while descriptor is None:
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(descriptor, f"pid={os.getpid()}\n".encode())
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for RIPE preparation lock: {path}")
            time.sleep(1.0)
    try:
        yield
    finally:
        os.close(descriptor)
        path.unlink(missing_ok=True)


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
    resume: bool = True,
    expected_size: int | None = None,
) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    work_path = partial
    if destination.is_file():
        actual_size = destination.stat().st_size
        if expected_size is None or actual_size == expected_size:
            return destination
        logger.warning(
            f"incomplete RIPE download {destination}: {actual_size:,}/{expected_size:,} bytes; "
            "resuming"
        )
        if partial.is_file() and partial.stat().st_size >= actual_size:
            work_path = partial
        else:
            partial.unlink(missing_ok=True)
            # Renaming an existing object may fail on object-storage FUSE mounts.
            # Resume the validated-size final path in place instead. It is never
            # returned to callers until it has the expected remote size.
            work_path = destination

    offset = work_path.stat().st_size if resume and work_path.is_file() else 0
    if expected_size is not None and offset > expected_size:
        logger.warning(f"discarding oversized RIPE partial download: {work_path}")
        work_path.unlink()
        offset = 0
    if expected_size is not None and offset == expected_size:
        return _finalize_download(work_path, destination, expected_size)
    headers = {"User-Agent": "ipid-analysis/ripe-atlas"}
    if offset:
        headers["Range"] = f"bytes={offset}-"
    request = Request(url, headers=headers)
    try:
        response = urlopen(request, timeout=120)
    except HTTPError as exc:
        if exc.code == 416 and expected_size is not None and offset == expected_size:
            return _finalize_download(work_path, destination, expected_size)
        raise
    with response:
        resumed = offset > 0 and getattr(response, "status", None) == 206
        mode = "ab" if resumed else "wb"
        if offset and not resumed:
            logger.warning(f"server did not honor Range for {url}; restarting download")
        with work_path.open(mode) as output:
            shutil.copyfileobj(response, output, length=8 * 1024 * 1024)
    if expected_size is not None:
        actual_size = work_path.stat().st_size
        if actual_size != expected_size:
            raise OSError(
                f"incomplete RIPE download retained at {work_path}: "
                f"{actual_size:,}/{expected_size:,} bytes"
            )
    return _finalize_download(work_path, destination, expected_size)


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
    partial = destination.with_suffix(destination.suffix + ".part")
    with source.open("rb") as input_file, partial.open("wb") as output_file:
        shutil.copyfileobj(input_file, output_file, length=8 * 1024 * 1024)
    partial.replace(destination)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as input_file:
        for block in iter(lambda: input_file.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


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
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid RIPE JSON on line {number}: {exc}") from exc
        if isinstance(value, dict):
            yield value


def _open_json_lines(path: Path) -> AbstractContextManager[TextIO]:
    if path.suffix == ".bz2":
        return bz2.open(path, "rt", encoding="utf-8", errors="strict")
    return path.open("rt", encoding="utf-8", errors="strict")


def _trace_addresses(record: dict) -> dict[str, tuple[bool, bool]]:
    destination = record.get("dst_addr")
    valid_destination = destination if _is_public_ipv4(destination) else None
    observed: dict[str, list[bool]] = {}

    for hop in record.get("result", []):
        if not isinstance(hop, dict):
            continue
        for reply in hop.get("result", []):
            if not isinstance(reply, dict):
                continue
            address = reply.get("from")
            if not _is_public_ipv4(address):
                continue
            flags = observed.setdefault(address, [False, False])
            if valid_destination is not None and address == valid_destination:
                flags[1] = True
            else:
                flags[0] = True

    if valid_destination is not None and record.get("destination_ip_responded") is True:
        observed.setdefault(valid_destination, [False, False])[1] = True
    return {address: (flags[0], flags[1]) for address, flags in observed.items()}


def build_role_lookup(
    files: Iterable[Path],
    destination: Path,
    *,
    window: RipeWindow,
    source: str,
) -> dict[str, int]:
    """Aggregate RIPE JSONL/BZip2 files into one address-level Parquet lookup."""
    stats: dict[str, _AddressStats] = {}
    traces = 0
    skipped_outside_window = 0
    skipped_non_ipv4 = 0
    start_timestamp = int(window.start.timestamp())
    end_timestamp = int(window.end.timestamp())

    for path in files:
        logger.info(f"parsing RIPE Atlas traceroutes: {path}")
        with _open_json_lines(path) as input_file:
            for record in _iter_json_lines(input_file):
                if record.get("af") != 4 or record.get("type", "traceroute") != "traceroute":
                    skipped_non_ipv4 += 1
                    continue
                timestamp = record.get("timestamp", record.get("endtime"))
                if not isinstance(timestamp, (int, float)):
                    continue
                timestamp = int(timestamp)
                if not start_timestamp <= timestamp < end_timestamp:
                    skipped_outside_window += 1
                    continue
                addresses = _trace_addresses(record)
                if not addresses:
                    continue
                traces += 1
                probe = record.get("prb_id")
                measurement = record.get("msm_id")
                protocol = str(record.get("proto", "UNKNOWN")).upper()
                for address, (transit, destination_observed) in addresses.items():
                    item = stats.setdefault(address, _AddressStats())
                    item.transit |= transit
                    item.destination |= destination_observed
                    item.trace_count += 1
                    if isinstance(probe, int):
                        item.probes.add(probe)
                    if isinstance(measurement, int):
                        item.measurements.add(measurement)
                    item.protocols.add(protocol)
                    item.first_seen = (
                        timestamp if item.first_seen is None else min(item.first_seen, timestamp)
                    )
                    item.last_seen = (
                        timestamp if item.last_seen is None else max(item.last_seen, timestamp)
                    )

    if not stats:
        raise ValueError("RIPE Atlas inputs contain no public IPv4 traceroute replies")

    schema = pa.schema(
        [
            ("IP_ADDR", pa.string()),
            ("T", pa.bool_()),
            ("D", pa.bool_()),
            ("TRACE_COUNT", pa.int64()),
            ("PROBE_COUNT", pa.int64()),
            ("MEASUREMENT_COUNT", pa.int64()),
            ("FIRST_SEEN", pa.int64()),
            ("LAST_SEEN", pa.int64()),
            ("PROTOCOLS", pa.string()),
        ],
        metadata={
            b"ripe_cache_version": RIPE_CACHE_VERSION.encode(),
            b"ripe_source": source.encode(),
            b"window_start": window.start.isoformat().encode(),
            b"window_end": window.end.isoformat().encode(),
        },
    )
    rows = []
    for address in sorted(stats):
        item = stats[address]
        rows.append(
            {
                "IP_ADDR": address,
                "T": item.transit,
                "D": item.destination,
                "TRACE_COUNT": item.trace_count,
                "PROBE_COUNT": len(item.probes),
                "MEASUREMENT_COUNT": len(item.measurements),
                "FIRST_SEEN": item.first_seen or 0,
                "LAST_SEEN": item.last_seen or 0,
                "PROTOCOLS": ",".join(sorted(item.protocols)),
            }
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), partial, compression="zstd")
    partial.replace(destination)
    return {
        "traces": traces,
        "addresses": len(stats),
        "transit_addresses": sum(item.transit for item in stats.values()),
        "destination_addresses": sum(item.destination for item in stats.values()),
        "skipped_outside_window": skipped_outside_window,
        "skipped_non_ipv4": skipped_non_ipv4,
    }


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


def _daily_dump_files(
    window: RipeWindow,
    *,
    samples: int,
    raw_root: Path,
    base_url: str,
) -> tuple[list[Path], tuple[datetime, ...], tuple[date, ...]]:
    available_days = _available_dump_days(base_url)
    days_in_window = tuple(
        day for day in available_days if window.start.date() <= day < window.end.date()
    )
    slots = sampled_available_dump_slots(window, samples, available_days)
    requested_days = (window.end.date() - window.start.date()).days
    if len(days_in_window) < requested_days:
        logger.warning(
            "RIPE Atlas daily-dump retention covers only "
            f"{days_in_window[0]} through {days_in_window[-1]} "
            f"({len(days_in_window)}/{requested_days} requested UTC days); "
            f"sampling {len(slots)} available slots inside the requested window"
        )
    files = []
    for slot in slots:
        day = slot.date().isoformat()
        filename = f"traceroute-{slot:%Y-%m-%dT%H00}.bz2"
        destination = Path(raw_root) / "ripe-atlas" / "daily-dumps" / day / filename
        url = f"{base_url.rstrip('/')}/{day}/{filename}"
        expected_size = _remote_file_size(url)
        if not destination.is_file() or destination.stat().st_size != expected_size:
            logger.info(f"downloading RIPE Atlas daily dump: {url}")
            source_path = _download(url, destination, expected_size=expected_size)
        else:
            source_path = destination
        files.append(source_path)
    return files, slots, days_in_window


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
        destination = destination_dir / f"{index:04d}-{source.name}"
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
        local_digest = hashlib.sha256(str(Path(input_dir).resolve()).encode()).hexdigest()[:12]
        key = f"local-{local_digest}"
    else:
        key = _source_key(source, samples=dump_samples, measurement_ids=measurement_ids)
    dataset = _dataset_paths(window, key, processed_root=processed_root)
    dataset.directory.mkdir(parents=True, exist_ok=True)

    with _exclusive_lock(dataset.directory / ".prepare.lock"):
        if not force and dataset.roles_path.is_file() and dataset.metadata_path.is_file():
            logger.info(f"reusing RIPE Atlas role cache: {dataset.roles_path}")
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

        stats = build_role_lookup(files, dataset.roles_path, window=window, source=key)
        file_metadata = [
            {
                "path": str(path),
                "size": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for path in files
        ]
        info = {
            "cache_version": RIPE_CACHE_VERSION,
            "source": source,
            "source_kind": source_kind,
            "campaign_start": campaign_start.astimezone(timezone.utc).isoformat(),
            "window_start": window.start.isoformat(),
            "window_end": window.end.isoformat(),
            "lookback_days": lookback_days,
            "dump_samples": dump_samples if source == "daily-dumps" else None,
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
            "files": file_metadata,
            **stats,
            "prepared_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        partial = dataset.metadata_path.with_suffix(".json.part")
        partial.write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")
        partial.replace(dataset.metadata_path)
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
    raw_root: Path = typer.Option(RAW_DATA_DIR),
    processed_root: Path = typer.Option(PROCESSED_DATA_DIR),
    force: bool = typer.Option(False, help="rebuild an existing cache"),
) -> None:
    try:
        dataset = prepare_ripe_atlas(
            campaign_start=parse_datetime(campaign_start),
            source=source,
            measurement_ids_file=measurement_ids_file,
            input_dir=input_dir,
            dump_samples=dump_samples,
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
