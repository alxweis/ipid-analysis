"""Opt-in Linux benchmark of the real RIPE parser on ext4 versus existing tmpfs.

No production caches are prepared or published. See docs/RIPE_PIPELINE_BENCHMARK.md.
"""

import argparse
import cProfile
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import pstats
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit

GIB = 1 << 30
PROJECT = Path(__file__).resolve().parents[1]


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def filesystem(path):
    """Find the longest matching Linux mount, including bind mounts."""
    path = Path(path).resolve()
    matches = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        mount, details = line.split(" - ", 1)
        encoded = mount.split()[4]
        decoded = re_decode_mount(encoded)
        root = Path(decoded)
        if root == path or root in path.parents:
            matches.append((len(root.parts), details.split()[0]))
    return max(matches)[1]


def re_decode_mount(value):
    # Linux mountinfo escapes whitespace and backslashes with octal sequences.
    import re

    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), value)


def isolated_environment(scratch):
    env = {key: value for key, value in os.environ.items() if not key.startswith("IPID_ANALYSIS_")}
    env.update(
        PYTHON_DOTENV_DISABLED="1",
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONPATH=str(PROJECT),
        IPID_ANALYSIS_STORAGE_MODE="local",
        IPID_ANALYSIS_RIPE_CACHE_S3="",
        IPID_ANALYSIS_S3_ROOTS="{}",
        IPID_ANALYSIS_TMP_DIR=str(scratch),
        TMPDIR=str(scratch),
        TMP=str(scratch),
        TEMP=str(scratch),
        OPENBLAS_NUM_THREADS="1",
        OMP_NUM_THREADS="1",
        MKL_NUM_THREADS="1",
        NUMEXPR_NUM_THREADS="1",
        ARROW_NUM_THREADS="1",
    )
    return env


def tree_bytes(path):
    total = 0
    for item in path.rglob("*"):
        try:
            if item.is_file():
                total += item.stat().st_size
        except FileNotFoundError:
            pass  # The producer can commit/remove a temporary artifact between checks.
    return total


def proc_io(pid):
    try:
        return {
            key: int(value)
            for key, value in (
                line.split(":") for line in Path(f"/proc/{pid}/io").read_text().splitlines()
            )
        }
    except (FileNotFoundError, ProcessLookupError):
        return {}


def proc_rss(pid):
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except (FileNotFoundError, ProcessLookupError):
        pass
    return 0


def swap_pages():
    return {
        key: int(value)
        for key, value in (line.split() for line in Path("/proc/vmstat").read_text().splitlines())
        if key in ("pswpin", "pswpout")
    }


def roles_digest(path):
    """Canonical row values, independent of Parquet encoding and batch boundaries."""
    import numpy as np
    import pyarrow.parquet as pq

    digest = hashlib.sha256()
    previous = -1
    for batch in pq.ParquetFile(path).iter_batches(batch_size=262144, use_threads=False):
        if batch.schema.names != ["IP_V4", "T", "D"] or any(
            column.null_count for column in batch.columns
        ):
            raise ValueError("unexpected RIPE output schema/nulls")
        rows = np.empty(batch.num_rows, dtype=[("ip", ">u4"), ("t", "u1"), ("d", "u1")])
        rows["ip"] = batch.column(0).to_numpy(zero_copy_only=False)
        rows["t"] = batch.column(1).to_numpy(zero_copy_only=False)
        rows["d"] = batch.column(2).to_numpy(zero_copy_only=False)
        if len(rows) and (
            int(rows["ip"][0]) <= previous or np.any(rows["ip"][1:] <= rows["ip"][:-1])
        ):
            raise ValueError("RIPE output must contain ordered unique IPv4 addresses")
        if len(rows):
            previous = int(rows["ip"][-1])
        digest.update(rows.tobytes())
    return digest.hexdigest()


def profile_report(profile):
    stats = pstats.Stats(profile)
    groups = {
        "bz2_decompression": lambda file, name: "decompress" in name and "BZ2" in name,
        "json_decode": lambda file, name: (
            ("orjson" in name and "loads" in name)
            or (file.endswith("json/decoder.py") and name in ("decode", "raw_decode"))
        ),
        "role_derivation_inclusive": lambda file, name: (
            file.endswith("ripe_bitmaps.py") and name == "trace_roles"
        ),
        "bitmap_add_inclusive": lambda file, name: (
            file.endswith("ripe_bitmaps.py") and name == "add"
        ),
    }
    result = {}
    for group, predicate in groups.items():
        rows = []
        for (file, line, name), (primitive, calls, own, cumulative, _) in stats.stats.items():
            if predicate(file.replace("\\", "/"), name):
                rows.append(
                    dict(
                        function=f"{file}:{line}:{name}",
                        calls=calls,
                        self_seconds=own,
                        cumulative_seconds=cumulative,
                    )
                )
        result[group] = rows
    with profile.with_suffix(".txt").open("w", encoding="utf-8") as stream:
        pstats.Stats(profile, stream=stream).strip_dirs().sort_stats("cumulative").print_stats(60)
        pstats.Stats(profile, stream=stream).strip_dirs().sort_stats("tottime").print_stats(60)
    return result


def worker(spec):
    """Fresh process, unchanged production algorithm, local-only destinations."""
    import resource

    os.sched_setaffinity(0, {spec["cpu"]})
    from ipid_analysis import ripe_atlas as ripe
    from ipid_analysis.artifacts import completion_path, provenance

    # These paths are private benchmark files, never processed/raw cache paths.
    root = Path(spec["workspace"])
    part, metadata = root / "roles.pq", root / "roles.json"
    window = ripe.RipeWindow(
        datetime.fromisoformat(spec["window_start"]), datetime.fromisoformat(spec["window_end"])
    )
    before_io = proc_io(os.getpid())
    before_cpu = time.process_time()
    profile = cProfile.Profile() if spec["profile"] else None
    started = time.perf_counter()
    if profile:
        profile.enable()
    info = ripe._build_role_part(root / "input.bz2", part, metadata, window, apply_window=True)
    if profile:
        profile.disable()
    elapsed = time.perf_counter() - started
    cpu = time.process_time() - before_cpu
    after_io = proc_io(os.getpid())
    # Read actual ordered values, not Parquet bytes/metadata or trusted markers.
    canonical = roles_digest(part)
    result = {
        "profiled": bool(profile),
        "wall_seconds": elapsed,
        "cpu_seconds": cpu,
        "parse_seconds": info["parse_seconds"],
        "prepare_seconds": info["prepare_seconds"],
        "after_parse_seconds": elapsed - info["parse_seconds"],
        "addresses": info["addresses"],
        "traces": info["traces"],
        "skipped_non_ipv4": info["skipped_non_ipv4"],
        "skipped_outside_window": info["skipped_outside_window"],
        "roles_sha256": canonical,
        "parquet_bytes": part.stat().st_size,
        "source_sha256": info["source_sha256"],
        "kernel_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "io_delta": {key: value - before_io.get(key, 0) for key, value in after_io.items()},
        "software": provenance(),
    }
    # Validate both completion declarations against actual output bytes.
    for path in (part, metadata):
        marker = json.loads(completion_path(path).read_text())
        if marker["sha256"] != sha256(path) or marker["size_bytes"] != path.stat().st_size:
            raise ValueError("benchmark output completion integrity mismatch")
    if profile:
        location = Path(spec["result"]).with_suffix(".pstats")
        profile.dump_stats(location)
        result["profile_functions"] = profile_report(location)
    write_json(Path(spec["result"]), result)


def copy_input(source, destination, cold, limit):
    started = time.perf_counter()
    written = 0
    with source.open("rb") as stream, destination.open("xb") as target:
        while block := stream.read(min(8 << 20, limit - written + 1)):
            written += len(block)
            if written > limit:
                raise ValueError("staged input exceeds workspace cap")
            target.write(block)
        target.flush()
        os.fsync(target.fileno())
    checksum = sha256(destination)
    advice = "tmpfs: no eviction requested"
    if cold:
        if not hasattr(os, "posix_fadvise"):
            raise RuntimeError("this Linux/Python build lacks per-file posix_fadvise")
        with destination.open("rb") as stream:
            os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        advice = "fsync + per-file DONTNEED requested; cold block storage not guaranteed"
    return dict(
        copy_and_verify_seconds=time.perf_counter() - started,
        sha256=checksum,
        page_cache_policy=advice,
    )


def stop(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def run_trial(spec, available, memory_limit, workspace_limit, reserve, timeout):
    root = Path(spec["workspace"])
    result = Path(spec["result"])
    spec_file = result.with_suffix(".spec.json")
    write_json(spec_file, spec)
    peak_rss = peak_workspace = 0
    started = time.monotonic()
    with result.with_suffix(".log").open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "_worker", str(spec_file)],
            env=isolated_environment(root / "scratch"),
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            while process.poll() is None:
                peak_rss = max(peak_rss, proc_rss(process.pid))
                peak_workspace = max(peak_workspace, tree_bytes(root))
                if peak_rss > memory_limit or peak_workspace > workspace_limit:
                    raise RuntimeError("benchmark memory/workspace limit exceeded")
                if available() < reserve:
                    raise RuntimeError("benchmark stopped to preserve free-memory reserve")
                if time.monotonic() - started > timeout:
                    raise TimeoutError("benchmark trial timeout")
                time.sleep(0.1)
            if process.returncode:
                raise RuntimeError(f"parser exited {process.returncode}; see {log.name}")
        finally:
            stop(process)
    data = json.loads(result.read_text())
    data.update(sampled_peak_rss_bytes=peak_rss, sampled_peak_workspace_bytes=peak_workspace)
    if max(peak_rss, data["kernel_peak_rss_bytes"]) > memory_limit:
        raise RuntimeError("benchmark peak RSS exceeded memory limit")
    write_json(result, data)
    return data


def validate_results(results):
    signatures = {
        (
            row["roles_sha256"],
            row["addresses"],
            row["traces"],
            row["skipped_non_ipv4"],
            row["skipped_outside_window"],
            row["source_sha256"],
        )
        for row in results
    }
    if len(signatures) != 1:
        raise ValueError("ext4/RAM/profile outputs differ")
    return {
        backend: statistics.median(
            row["wall_seconds"]
            for row in results
            if row["backend"] == backend and not row["profiled"]
        )
        for backend in ("ext4", "ram")
    }


def parser():
    command = argparse.ArgumentParser(description=__doc__)
    sources = command.add_mutually_exclusive_group(required=True)
    sources.add_argument("--input", type=Path, help="existing .bz2 dump, read-only")
    sources.add_argument("--url", help="one public HTTPS RIPE daily-dump .bz2 URL; download once")
    command.add_argument(
        "--window-start", required=True, help="inclusive UTC date, e.g. 2026-10-02"
    )
    command.add_argument("--window-end", required=True, help="exclusive UTC date, e.g. 2026-10-30")
    command.add_argument("--work-dir", type=Path, default=PROJECT / "tmp" / "benchmarks")
    command.add_argument(
        "--ram-dir", type=Path, default=Path("/dev/shm"), help="existing tmpfs mount"
    )
    command.add_argument(
        "--repeats", type=int, default=2, help="pairs, alternating AB/BA (default 2)"
    )
    command.add_argument("--memory-gib", type=float, default=4, help="parser RSS limit")
    command.add_argument(
        "--workspace-gib", type=float, default=4, help="input + intermediate file cap"
    )
    command.add_argument("--reserve-gib", type=float, default=2, help="free RAM and disk reserve")
    command.add_argument("--timeout", type=float, default=7200, help="seconds per parser trial")
    command.add_argument(
        "--profile", action="store_true", help="extra RAM cProfile run, excluded from medians"
    )
    return command


def main(argv=None):
    args = parser().parse_args(argv)
    if not sys.platform.startswith("linux"):
        raise ValueError("ext4/tmpfs comparison requires Linux; use the Analysis VM")
    if args.repeats < 1 or any(
        not 0 < value < float("inf")
        for value in (args.memory_gib, args.workspace_gib, args.reserve_gib, args.timeout)
    ):
        raise ValueError("repeats and finite resource limits must be positive")
    start, end = (
        datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        for value in (args.window_start, args.window_end)
    )
    if start >= end or any(value.time().isoformat() != "00:00:00" for value in (start, end)):
        raise ValueError("window must contain complete days with start < end")
    work, ram = args.work_dir.resolve(), args.ram_dir.resolve()
    scratch = (PROJECT / "tmp").resolve()
    if scratch not in work.parents:
        raise ValueError("work-dir must be a dedicated descendant of this checkout's tmp/")
    work.mkdir(parents=True, exist_ok=True)
    if filesystem(work) != "ext4" or filesystem(ram) != "tmpfs":
        raise ValueError("comparison requires ext4 work-dir and an existing tmpfs ram-dir")
    memory, workspace, reserve = (
        int(value * GIB) for value in (args.memory_gib, args.workspace_gib, args.reserve_gib)
    )
    root = Path(tempfile.mkdtemp(prefix="ripe-pipeline-", dir=work))
    print(f"Benchmark directory: {root}", flush=True)
    # Never load the deployment .env or inherit production scope/cache settings.
    # This process owns its environment; the live worker is unaffected.
    env = isolated_environment(root / "control")
    for key in list(os.environ):
        if key.startswith("IPID_ANALYSIS_"):
            del os.environ[key]
    os.environ.update(env)
    from ipid_analysis.runtime import available_memory

    if available_memory() < memory + workspace + reserve:
        raise ValueError("insufficient available RAM for parser + tmpfs cap + reserve")
    if shutil.disk_usage(ram).free < workspace + reserve:
        raise ValueError(
            "existing tmpfs has insufficient capacity; do not remount it for this test"
        )
    if shutil.disk_usage(work).free < 2 * workspace + reserve:
        raise ValueError("insufficient ext4 space for retained source + trial + reserve")
    report = {
        "version": 1,
        "status": "running",
        "results": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "limits": vars(args).copy(),
    }
    report["limits"] = {
        key: str(value) if isinstance(value, Path) else value
        for key, value in report["limits"].items()
    }
    report_path = root / "results.json"
    before_swap = swap_pages()
    try:
        source = args.input.resolve() if args.input else root / "source.bz2"
        if args.url:
            uri = urlsplit(args.url)
            if (
                uri.scheme != "https"
                or uri.netloc != "data-store.ripe.net"
                or not uri.path.startswith("/datasets/atlas-daily-dumps/")
                or not uri.path.endswith(".bz2")
                or uri.query
                or uri.fragment
            ):
                raise ValueError("URL must be a public HTTPS RIPE daily-dump .bz2 URL")
            from urllib.request import Request, urlopen

            from ipid_analysis.downloads import download

            with urlopen(Request(args.url, method="HEAD"), timeout=120) as response:
                size = int(response.headers["Content-Length"])
            if not 0 < size < workspace:
                raise ValueError("dump exceeds workspace cap")
            started = time.perf_counter()
            download(args.url, source, expected_size=size)
            report["download_seconds"] = time.perf_counter() - started
        if (
            not source.is_file()
            or not source.name.endswith(".bz2")
            or not 0 < source.stat().st_size < workspace
        ):
            raise ValueError("input must be a nonempty .bz2 file smaller than workspace cap")
        checksum = sha256(source)
        report["input"] = {
            "path": str(source),
            "size_bytes": source.stat().st_size,
            "sha256": checksum,
            "url": args.url,
        }
        cpu = min(os.sched_getaffinity(0))
        report["cpu"] = cpu
        report["filesystems"] = {"work": filesystem(work), "ram": filesystem(ram)}
        modes = [
            mode
            for pair in range(args.repeats)
            for mode in (("ext4", "ram") if pair % 2 == 0 else ("ram", "ext4"))
        ]
        if args.profile:
            modes.append("ram-profile")
        for index, mode in enumerate(modes):
            backend = "ext4" if mode == "ext4" else "ram"
            if available_memory() < memory + workspace + reserve:
                raise RuntimeError("insufficient free RAM before next trial")
            parent = root if backend == "ext4" else ram
            if shutil.disk_usage(parent).free < workspace + reserve:
                raise RuntimeError("insufficient workspace capacity before next trial")
            with tempfile.TemporaryDirectory(prefix="ripe-trial-", dir=parent) as temporary:
                trial = Path(temporary)
                (trial / "scratch").mkdir()
                staging = copy_input(source, trial / "input.bz2", backend == "ext4", workspace)
                if staging["sha256"] != checksum:
                    raise ValueError("staged input differs from original dump")
                spec = dict(
                    workspace=str(trial),
                    result=str(root / f"trial-{index:02d}-{mode}.json"),
                    window_start=start.isoformat(),
                    window_end=end.isoformat(),
                    cpu=cpu,
                    profile=mode == "ram-profile",
                )
                print(f"Trial {index + 1}/{len(modes)}: {mode}", flush=True)
                row = run_trial(spec, available_memory, memory, workspace, reserve, args.timeout)
                if row["source_sha256"] != checksum:
                    raise ValueError("parser source checksum differs")
                row.update(backend=backend, staging=staging)
                write_json(Path(spec["result"]), row)
                report["results"].append(row)
                write_json(report_path, report)
        report["median_wall_seconds"] = validate_results(report["results"])
        report["status"] = "passed"
    except BaseException as exc:
        report.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        report["system_swap_pages_delta"] = {
            key: value - before_swap.get(key, 0) for key, value in swap_pages().items()
        }
        report["warnings"] = (
            [
                "System swap activity observed: tmpfs was not proven fully RAM-resident; "
                "repeat in a quiet maintenance window."
            ]
            if any(report["system_swap_pages_delta"].values())
            else []
        )
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_json(report_path, report)
        print(f"Results: {report_path}", flush=True)
    print(json.dumps(report["median_wall_seconds"], indent=2))


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "_worker":
        worker(json.loads(Path(sys.argv[2]).read_text()))
    else:
        main()
