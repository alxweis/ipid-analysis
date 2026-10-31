"""Local scratch, bounded DuckDB connections and single-host coordination."""

from __future__ import annotations

import atexit
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import re
import shutil
import tempfile
import threading
import time

import duckdb

from ipid_analysis.config import (
    PROCESSED_DATA_DIR,
    PROCESSED_STORAGE_DIR,
    PROJ_ROOT,
    RAW_DATA_DIR,
    RAW_STORAGE_DIR,
    REPORTS_DIR,
    REPORTS_STORAGE_DIR,
    SCOPE,
)


def mounted_storage() -> bool:
    mode = os.environ.get("IPID_ANALYSIS_STORAGE_MODE", "local")
    if mode not in {"local", "mounted-s3"}:
        raise ValueError("IPID_ANALYSIS_STORAGE_MODE must be local or mounted-s3")
    return mode == "mounted-s3"


def persistent_roots() -> tuple[Path, ...]:
    return (
        (RAW_STORAGE_DIR, PROCESSED_STORAGE_DIR, REPORTS_STORAGE_DIR)
        if SCOPE
        else (RAW_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR)
    )


def validate_storage() -> None:
    """Fail before creating directories underneath missing production mounts."""
    if mounted_storage():
        for root in persistent_roots():
            if not root.is_mount():
                raise OSError(f"required S3 mount is missing: {root}")
    elif Path("/proc/self/mountinfo").is_file():
        expected = {str(path.resolve()) for path in persistent_roots()}
        for line in Path("/proc/self/mountinfo").read_text().splitlines():
            fields = line.split()
            if (
                len(fields) > 6
                and fields[4].replace("\\040", " ") in expected
                and " - fuse.rclone " in line
            ):
                raise ValueError(
                    "rclone data mounts require IPID_ANALYSIS_STORAGE_MODE=mounted-s3"
                )
    scratch_root()


def ram_scratch() -> bool:
    mode = os.environ.get("IPID_ANALYSIS_SCRATCH_STORAGE", "ram" if mounted_storage() else "disk")
    if mode not in {"ram", "disk"}:
        raise ValueError("IPID_ANALYSIS_SCRATCH_STORAGE must be ram or disk")
    return mode == "ram"


def control_root() -> Path:
    """Small durable control files and locks; never spill or bulk downloads."""
    return _scratch_path(
        Path(
            os.environ.get(
                "IPID_ANALYSIS_CONTROL_ROOT",
                os.environ.get("IPID_ANALYSIS_TMP_DIR", str(PROJ_ROOT / "tmp")),
            )
        )
    )


def filesystem_type(path: Path) -> str:
    """Find the most specific Linux mount, including paths below a mount point."""
    matches = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        point = Path(fields[4].replace("\\040", " ").replace("\\134", "\\"))
        if path == point or point in path.parents:
            matches.append((len(point.parts), line.split(" - ", 1)[1].split()[0]))
    return max(matches)[1] if matches else ""


def ram_root() -> Path:
    if os.name != "posix":
        raise OSError("RAM scratch requires an existing Linux tmpfs filesystem")
    identity = hashlib.sha256(str(PROJ_ROOT.resolve()).encode()).hexdigest()[:16]
    default = Path("/dev/shm") / f"ipid-analysis-{getattr(os, 'getuid', lambda: 0)()}-{identity}"
    root = Path(os.environ.get("IPID_ANALYSIS_RAM_DIR", str(default))).absolute()
    if root.is_symlink() or root.is_mount():
        raise ValueError(
            "RAM work directory must be a dedicated directory, not a mount root or symlink"
        )
    if filesystem_type(root.resolve()) != "tmpfs":
        raise OSError(f"RAM work directory must use an existing tmpfs filesystem: {root}")
    root = _scratch_path(root)
    if root.stat().st_uid != os.getuid():
        raise PermissionError(f"RAM work directory must belong to the current user: {root}")
    # A private directory on the existing tmpfs needs no mount/configuration change.
    root.chmod(0o700)
    return root


def ram_limit() -> int:
    limit = int(os.environ.get("IPID_ANALYSIS_RAM_BYTES", min(32 << 30, available_memory() // 4)))
    if limit <= 0:
        raise ValueError("RAM scratch budget must be positive")
    return limit


def scratch_root(root: Path | None = None) -> Path:
    if ram_scratch():
        base = ram_root()
        result = (
            Path(root)
            if root is not None
            else Path(os.environ.get("IPID_ANALYSIS_RAM_JOB_DIR", str(base)))
        ).resolve()
        if result != base and base not in result.parents:
            raise ValueError("RAM scratch must be inside the configured RAM work directory")
        if filesystem_type(result) != "tmpfs":
            raise OSError("RAM scratch must not cross into a disk filesystem")
        return _scratch_path(result)
    return _scratch_path(
        Path(root or os.environ.get("IPID_ANALYSIS_TMP_DIR", str(PROJ_ROOT / "tmp")))
    )


def _scratch_path(root: Path) -> Path:
    root = root.resolve()
    if root == Path(root.anchor) or root == Path.home().resolve():
        raise ValueError("scratch must be a dedicated application directory")
    for persistent in persistent_roots():
        if (
            root == persistent.resolve()
            or persistent.resolve() in root.parents
            or root in persistent.resolve().parents
        ):
            raise ValueError(f"scratch must not be inside persistent storage: {root}")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    # An accidental scratch mount must not turn spill/locks into remote operations.
    if mounted_storage():
        ancestor = root
        while not ancestor.is_mount() and ancestor != ancestor.parent:
            ancestor = ancestor.parent
        if os.name == "posix":
            for line in Path("/proc/self/mountinfo").read_text().splitlines():
                fields = line.split()
                if len(fields) > 6 and fields[4].replace("\\040", " ") == str(ancestor):
                    filesystem = line.split(" - ", 1)[1].split()[0]
                    if filesystem.startswith("fuse"):
                        raise OSError(f"scratch must use a local filesystem: {root}")
    return root


def ripe_transfer_root() -> Path:
    """Shared local transfers, independent of a scheduled child's scratch root."""
    legacy = os.environ.get("IPID_ANALYSIS_RIPE_WORK_DIR")
    default = (
        ram_root() / "ripe-transfers"
        if ram_scratch()
        else Path(legacy) / "shared-transfers"
        if legacy
        else PROJ_ROOT / "tmp" / "ripe-transfers"
    )
    root = Path(os.environ.get("IPID_ANALYSIS_RIPE_TRANSFER_DIR", str(default))).resolve()
    if root == PROJ_ROOT.resolve() or (
        mounted_storage() and not ram_scratch() and PROJ_ROOT.resolve() not in root.parents
    ):
        raise ValueError(
            "RIPE transfers must be a dedicated directory inside the analysis checkout"
        )
    return scratch_root(root)


def ripe_transfer_limit() -> int:
    limit = int(os.environ.get("IPID_ANALYSIS_RIPE_TRANSFER_BYTES", 8 << 30))
    if limit <= 0:
        raise ValueError("RIPE transfer budget must be positive")
    return limit


def directory_bytes(directory: Path) -> int:
    total = 0
    for root, dirs, files in os.walk(directory, followlinks=False):
        dirs[:] = [d for d in dirs if not (Path(root) / d).is_symlink()]
        for name in files:
            path = Path(root) / name
            try:
                if not path.is_symlink():
                    total += path.stat().st_size
            except FileNotFoundError:
                pass
    return total


def scratch_free() -> int:
    root = scratch_root()
    if not ram_scratch():
        return shutil.disk_usage(root).free
    reserve = int(os.environ.get("IPID_ANALYSIS_RAM_RESERVE_BYTES", 64 << 20))
    global_free = ram_limit() - directory_bytes(ram_root())
    job_free = int(
        os.environ.get("IPID_ANALYSIS_JOB_SCRATCH_BYTES", ram_limit())
    ) - directory_bytes(root)
    return max(0, min(global_free, job_free, shutil.disk_usage(root).free - reserve))


def check_scratch_budget() -> None:
    if ram_scratch() and (
        directory_bytes(ram_root()) > ram_limit()
        or directory_bytes(scratch_root())
        > int(os.environ.get("IPID_ANALYSIS_JOB_SCRATCH_BYTES", ram_limit()))
    ):
        raise OSError("RAM scratch budget exceeded; refusing publication")


def temporary_path(destination: Path) -> Path:
    root = scratch_root() / "artifacts"
    if ram_scratch() and scratch_free() <= 0:
        raise OSError("RAM scratch budget exhausted")
    root.mkdir(exist_ok=True)
    descriptor, name = tempfile.mkstemp(
        prefix=f"artifact-{os.getpid()}-", suffix=destination.suffix, dir=root
    )
    os.close(descriptor)
    return Path(name)


def work_path(identity: str, *, category: str = "downloads") -> Path:
    """Stable local location for resumable work; identity need not be a pathname."""
    key = hashlib.sha256(str(identity).encode()).hexdigest()
    base = control_root() if category in {"locks", "terminal", "retry"} else scratch_root()
    root = base / category / key
    root.mkdir(parents=True, exist_ok=True)
    return root


def cleanup_work(directory: Path, *, root: Path | None = None) -> None:
    """Only remove a strict descendant of an explicitly owned work root."""
    allowed = (root or scratch_root()).resolve()
    target = directory.resolve()
    if target == allowed or allowed not in target.parents or directory.is_symlink():
        raise ValueError(f"refusing cleanup outside owned work root: {directory}")
    if target.exists():
        shutil.rmtree(target)


def cleanup_current_artifacts() -> None:
    """Called between synchronous jobs, never while a producer is still running."""
    parent = scratch_root() / "artifacts"
    for path in parent.glob(f"artifact-{os.getpid()}-*"):
        if path.is_symlink() or path.suffix == ".log":
            continue
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass  # Keep a locked file for the age-bounded maintenance command.


def _cleanup_on_exit():
    try:
        cleanup_current_artifacts()
    except (OSError, ValueError):
        pass


atexit.register(_cleanup_on_exit)


def available_memory() -> int:
    """Conservative current availability, including a Linux cgroup v2 limit."""
    available = 2 << 30
    if os.name == "nt":
        import ctypes

        class MemoryStatus(ctypes.Structure):
            _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong)] + [
                (name, ctypes.c_ulonglong)
                for name in (
                    "total",
                    "available",
                    "page_total",
                    "page_available",
                    "virtual_total",
                    "virtual_available",
                    "extended",
                )
            ]

        status = MemoryStatus()
        status.length = ctypes.sizeof(status)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            available = status.available
    if Path("/proc/meminfo").is_file():
        values = dict(
            line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines()
        )
        available = int(values["MemAvailable"].split()[0]) * 1024
        limit = _cgroup_root() / "memory.max"
        used = _cgroup_root() / "memory.current"
        if limit.is_file() and used.is_file() and limit.read_text().strip() != "max":
            available = min(available, max(1, int(limit.read_text()) - int(used.read_text())))
    return min(available, int(os.environ.get("IPID_ANALYSIS_JOB_MEMORY_BYTES", available)))


def _cgroup_root() -> Path:
    root = Path("/sys/fs/cgroup")
    membership = Path("/proc/self/cgroup")
    if membership.is_file():
        for line in membership.read_text().splitlines():
            if line.startswith("0::"):
                candidate = root / line[3:].lstrip("/")
                if candidate.is_dir():
                    return candidate
    return root


def cpu_budget() -> int:
    count = (
        len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count() or 1
    )
    quota = _cgroup_root() / "cpu.max"
    if quota.is_file():
        limit, period = quota.read_text().split()
        if limit != "max":
            count = min(count, max(1, int(limit) // int(period)))
    return max(1, min(count, int(os.environ.get("IPID_ANALYSIS_THREADS", count))))


def batch_budget(requested: int, *, width: int = 100) -> int:
    if requested < 1:
        raise ValueError("batch size must be positive")
    # Reserve for several simultaneous NumPy views, Arrow buffers and the engine.
    return max(1, min(requested, available_memory() // (32 * max(1, width) * 8)))


@contextmanager
def connect(*, config: dict | None = None):
    """One connection owns and cleans its local spill directory, also on errors."""
    validate_storage()
    root = scratch_root()
    with tempfile.TemporaryDirectory(dir=root, prefix="duckdb-") as directory:
        options = dict(config or {})
        options["threads"] = min(int(options.get("threads") or cpu_budget()), cpu_budget())
        options.setdefault(
            "memory_limit",
            os.environ.get(
                "IPID_ANALYSIS_DUCKDB_MEMORY", f"{max(64, available_memory() // (2 << 20))}MB"
            ),
        )
        match = re.fullmatch(r"([0-9.]+)\s*(K|M|G|T)?(i)?B", str(options["memory_limit"]), re.I)
        if not match:
            raise ValueError("memory limit must be an explicit byte quantity")
        power = {None: 0, "K": 1, "M": 2, "G": 3, "T": 4}[match[2].upper() if match[2] else None]
        requested = float(match[1]) * (1024 if match[3] else 1000) ** power
        options["memory_limit"] = f"{max(1, int(min(requested, available_memory() // 2)))}B"
        options["temp_directory"] = directory
        free = scratch_free()
        reserve = (
            0
            if ram_scratch()
            else int(os.environ.get("IPID_ANALYSIS_SCRATCH_RESERVE_BYTES", 1 << 30))
        )
        if free <= reserve:
            raise OSError("insufficient local scratch space")
        spill = min(
            free - reserve, int(os.environ.get("IPID_ANALYSIS_JOB_SCRATCH_BYTES", free - reserve))
        )
        options["max_temp_directory_size"] = f"{max(1, spill // (1 << 20))}MB"
        connection = duckdb.connect(config=options)
        try:
            yield connection
        finally:
            connection.close()


@contextmanager
def exclusive_lock(identity: str, timeout_seconds: float = 3600):
    """Kernel-owned local lock; crashes release ownership without stale PID files."""
    lock_root = os.environ.get("IPID_ANALYSIS_LOCK_DIR")
    if lock_root:
        directory = Path(lock_root) / hashlib.sha256(identity.encode()).hexdigest()
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "owner.lock"
    else:
        path = work_path(identity, category="locks") / "owner.lock"
    with path.open("a+b") as stream:
        stream.seek(0)
        if os.fstat(stream.fileno()).st_size == 0:
            stream.write(b"0")
            stream.flush()
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                stream.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out acquiring local lock: {identity}") from None
                time.sleep(0.1)
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


_run_lease = None


def start_run() -> None:
    """All production CLI writers share a lease; worker children inherit ownership."""
    global _run_lease
    validate_storage()
    if not mounted_storage() or _run_lease is not None:
        return
    owner = os.environ.get("IPID_ANALYSIS_RUN_OWNER")
    if owner:
        try:
            os.kill(int(owner), 0)
        except (OSError, ValueError):
            raise RuntimeError("inherited analysis owner is no longer running") from None
        return
    lease = exclusive_lock("analysis-writer", timeout_seconds=0)
    lease.__enter__()
    _run_lease = lease
    os.environ["IPID_ANALYSIS_RUN_OWNER"] = str(os.getpid())
    atexit.register(lease.__exit__, None, None, None)


_reservation_lock = threading.Lock()
_reserved = 0


@contextmanager
def reserve_download(bytes_needed: int):
    """Account for concurrent downloads in the single CLI owner process."""
    global _reserved
    needed = max(0, bytes_needed)
    with _reservation_lock:
        reserve = (
            0
            if ram_scratch()
            else int(os.environ.get("IPID_ANALYSIS_SCRATCH_RESERVE_BYTES", 1 << 30))
        )
        if scratch_free() < reserve + _reserved + needed:
            raise OSError("insufficient unreserved local scratch space")
        _reserved += needed
    try:
        yield
    finally:
        with _reservation_lock:
            _reserved -= needed
