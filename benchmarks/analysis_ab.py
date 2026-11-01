"""Opt-in warm-cache analysis comparison; never submit jobs or rebuild caches.

Run on Linux with the worker stopped. See docs/ANALYSIS_AB_BENCHMARK.md.
Only fresh .ipid-analysis-tests namespaces may receive S3 publications.
"""

import argparse
from contextlib import contextmanager
import csv
import functools
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
from benchmarks.ripe_pipeline import (  # noqa: E402
    filesystem,
    proc_io,
    proc_rss,
    sha256,
    stop,
    tree_bytes,
    write_json,
)

PINS = {
    "A": "b6e142c7de41d9c6c4a5c8f4515e82abeccc97ed",  # merge #113, INCLUDES #114
    "B": "68edad13e3b0f5a4e765f8c071dea8dc2e65a0a0",  # merge #117, actual v4 source
}
WORKFLOW = "s3://netds3o:aweis/ipid-2/analysis-workflow"
REQUEST = (
    WORKFLOW
    + "/continental/geo-test-v4/sites/as/analysis-jobs/tcp-80_2026-10-31_17-19-14/request.json"
)
GIB = 1 << 30


def below(path, root):
    return Path(path).resolve().is_relative_to(Path(root).resolve())


class WriteGuard:
    """Fail closed on Python file mutations and DuckDB COPY destinations.

    Not an OS security sandbox. The pinned application is trusted; the guard
    catches accidental production writes. Native output writers are checked too.
    """

    def __init__(self, roots):
        self.roots = tuple(Path(p).resolve() for p in roots)

    def check(self, path, directory_fd=-1):
        if isinstance(path, int):
            if path in {0, 1, 2}:
                return
            path = Path(f"/proc/self/fd/{path}").resolve()
        elif directory_fd is not None and directory_fd != -1:
            path = Path(f"/proc/self/fd/{directory_fd}").resolve() / os.fsdecode(path)
        if not any(below(os.fsdecode(path), root) for root in self.roots):
            raise PermissionError(f"benchmark refuses write outside owned paths: {path}")

    def audit(self, event, args):
        if event == "open":
            path, mode, flags = args
            if (mode and any(c in mode for c in "wax+")) or flags & (
                os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
            ):
                self.check(path)
        elif event == "os.mkdir":
            path = (
                Path(args[0])
                if args[2] == -1
                else Path(f"/proc/self/fd/{args[2]}").resolve() / args[0]
            )
            if not path.is_dir():
                self.check(path)
        elif event in {"os.remove", "os.rmdir"}:
            self.check(args[0], args[1])
        elif event in {"os.chmod", "os.utime"}:
            self.check(args[0], args[-1])
        elif event == "os.truncate":
            self.check(args[0])
        elif event in {"os.rename", "os.link", "os.symlink"}:
            if event == "os.rename":
                self.check(args[0], args[2])
                self.check(args[1], args[3])
            else:
                raise PermissionError("benchmark does not create links")
        elif event in {"socket.connect", "urllib.Request"}:
            raise PermissionError("warm-cache benchmark forbids Python network downloads")

    def sql(self, query, parameters=None):
        # Application COPY statements use quoted literal destinations.
        if re.search(r"\bCOPY\b", query, re.I):
            match = re.search(
                r"\bTO\s+(?:'((?:[^']|'')+)'|\$([A-Za-z_][A-Za-z0-9_]*)|(\?))", query, re.I
            )
            if match is None:
                raise PermissionError("benchmark cannot validate COPY destination")
            destination = (
                match[1].replace("''", "'")
                if match[1] is not None
                else parameters[match[2]]
                if match[2] is not None
                else parameters[query[: match.end()].count("?") - 1]
            )
            self.check(destination)


class Timings:
    def __init__(self, path):
        self.stream = Path(path).open("w", encoding="utf8")
        self.sequence = 0

    @contextmanager
    def span(self, kind, name):
        started, cpu = time.perf_counter(), time.process_time()
        success = False
        try:
            yield
            success = True
        finally:
            self.sequence += 1
            if self.sequence > 50_000:
                raise RuntimeError("benchmark trace exceeds bounded event count")
            self.stream.write(
                json.dumps(
                    dict(
                        kind=kind,
                        name=str(name)[:1500],
                        success=success,
                        seconds=time.perf_counter() - started,
                        cpu_seconds=time.process_time() - cpu,
                    )
                )
                + "\n"
            )
            self.stream.flush()

    def wrap(self, original, kind, name=None):
        @functools.wraps(original)
        def wrapped(*args, **kwargs):
            with self.span(kind, name or original.__qualname__):
                return original(*args, **kwargs)

        return wrapped


class Connection:
    def __init__(self, inner, guard, timings):
        self.inner, self.guard, self.timings = inner, guard, timings

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def execute(self, query, *args, **kwargs):
        self.guard.sql(query, args[0] if args else kwargs.get("parameters"))
        with self.timings.span("sql", query):
            result = self.inner.execute(query, *args, **kwargs)
        return self if result is self.inner else result

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self.inner.__exit__(*args)


def checked_publication(original, guard, timings):
    def publish(source, destination):
        guard.check(destination)
        with timings.span("publication", destination):
            return original(source, destination)

    return publish


def instrument(guard, timings, remote_prefixes):
    import duckdb
    import matplotlib.figure
    import pyarrow.csv as csv
    import pyarrow.parquet as pq

    from ipid_analysis import transport

    connect = duckdb.connect
    duckdb.connect = lambda *a, **kw: Connection(connect(*a, **kw), guard, timings)
    transport.native_put = checked_publication(transport.native_put, guard, timings)
    run = subprocess.run

    def checked_run(command, *args, **kwargs):
        # All publication subprocesses are rclone copyto. No s3cmd, rm,
        # shells, HTTP tools or arbitrary subprocesses run inside a trial.
        if (
            not isinstance(command, (list, tuple))
            or not command
            or Path(command[0]).name != "rclone"
            or "copyto" not in command
            or not any(command[-1].startswith(prefix + "/") for prefix in remote_prefixes)
            or kwargs.get("shell")
        ):
            raise PermissionError("benchmark refuses subprocess outside its test S3 namespace")
        with timings.span("rclone", "copyto"):
            return run(command, *args, **kwargs)

    subprocess.run = checked_run
    matplotlib.figure.Figure.savefig = timings.wrap(matplotlib.figure.Figure.savefig, "figure")
    for module, name, position in (
        (pq, "write_table", 1),
        (pq, "ParquetWriter", 0),
        (csv, "write_csv", 1),
    ):
        original = getattr(module, name)

        def checked_writer(*args, _original=original, _position=position, **kwargs):
            destination = (
                args[_position]
                if len(args) > _position
                else kwargs.get("where", kwargs.get("output_file"))
            )
            guard.check(destination)
            return _original(*args, **kwargs)

        setattr(module, name, checked_writer)


def instrument_science(postprocess, timings):
    from ipid_analysis.empirical_random_uniformity import EmpiricalNullTables
    from ipid_analysis.multiscale_increment_uniformity import MultiscaleIncrementNullTables

    record = postprocess.record_stage

    @contextmanager
    def stage(stages, name, **kwargs):
        with timings.span("stage", name + ":" + str(kwargs.get("target"))):
            with record(stages, name, **kwargs):
                yield

    postprocess.record_stage = stage
    for name in (
        "prepare_itdk",
        "prepare_ripe_atlas",
        "render_itdk_analysis",
        "render_ripe_atlas_analysis",
        "evaluate_random_reproducibility",
        "render_strategy_refinement_plot",
    ):
        setattr(postprocess, name, timings.wrap(getattr(postprocess, name), "function", name))
    for cls, method, cache in (
        (EmpiricalNullTables, "spacing", "_spacing"),
        (MultiscaleIncrementNullTables, "single", "_single"),
        (MultiscaleIncrementNullTables, "joint", "_joint"),
    ):
        original = getattr(cls, method)

        names = tuple(inspect.signature(original).parameters)[1:]

        def null_call(self, *args, _original=original, _cache=cache, _names=names, **kwargs):
            # These methods are called positionally by production. Trace only
            # new tables; per-IP diagnostic lookups must not flood the trace.
            values = args + tuple(kwargs[name] for name in _names[len(args) :])
            key = values[0] if len(values) == 1 else values
            if key in getattr(self, _cache):
                return _original(self, *args, **kwargs)
            with timings.span("null_table", _original.__qualname__ + str(key)):
                return _original(self, *args, **kwargs)

        setattr(cls, method, null_call)


def uri_path(uri, spec):
    for name, prefix in spec["s3_roots"].items():
        if uri.startswith(prefix.rstrip("/") + "/"):
            path = Path(spec["repo"]) / ("reports" if name == "reports" else f"data/{name}")
            candidate = path / uri[len(prefix.rstrip("/")) + 1 :]
            if not below(candidate, path):
                raise ValueError("source URI escapes configured root")
            return candidate
    raise ValueError("input URI is outside configured mounted S3 roots")


def verify_input(path):
    import pyarrow.parquet as pq

    before = path.stat()
    checksum = sha256(path)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError(f"input changed while hashing: {path}")
    marker = path.parent / ".ipid-complete" / (path.name + ".json")
    if marker.is_file():
        info = json.loads(marker.read_text())
        if (
            info.get("status") != "complete"
            or info.get("sha256") != checksum
            or info.get("size_bytes") != before.st_size
        ):
            raise ValueError(f"input completion integrity mismatch: {path}")
    if path.suffix in {".pq", ".parquet"}:
        pq.ParquetFile(path).metadata
    return dict(path=str(path), size=before.st_size, sha256=checksum)


def scientific_json(value, replacements):
    """Only volatile provenance is excluded; scientific values stay intact."""
    if isinstance(value, dict):
        volatile = {
            "generated_at",
            "code_revision",
            "software",
            "python",
            "storage_roots",
            "mtime_ns",
        }
        return {k: scientific_json(v, replacements) for k, v in value.items() if k not in volatile}
    if isinstance(value, list):
        return [scientific_json(v, replacements) for v in value]
    if isinstance(value, str):
        for path, token in replacements:
            value = value.replace(str(path), token)
    return value


def logical_digest(rows):
    # The selected 20k-IP diagnostic is deliberately small. Keep validation
    # bounded; this is not an in-memory validator for a 300M-address campaign.
    if len(rows) > 10_000_000:
        raise ValueError("diagnostic validation exceeds ten million rows")
    lines = sorted(
        json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) for row in rows
    )
    digest = hashlib.sha256()
    for line in lines:
        encoded = line.encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def output_inventory(spec):
    import pyarrow.parquet as pq

    from ipid_analysis.artifacts import assert_complete, completion_path

    output = {}
    replacements = [
        (spec[k], "<" + k + ">")
        for k in ("processed", "reports", "control", "scratch", "ram", "tree")
    ]
    for category in ("processed", "reports"):
        root = Path(spec[category])
        for path in sorted(root.rglob("*")):
            if not path.is_file() or ".ipid-complete" in path.parts:
                continue
            assert_complete(path)
            if not completion_path(path).is_file():
                raise ValueError(f"uncommitted diagnostic output: {path}")
            checked = verify_input(path)
            key = category + "/" + path.relative_to(root).as_posix()
            if path.suffix == ".pq":
                table = pq.read_table(path)
                checked.update(
                    kind="parquet",
                    rows=table.num_rows,
                    schema=[(f.name, str(f.type), f.nullable) for f in table.schema],
                    logical_sha256=logical_digest(table.to_pylist()),
                )
            elif path.suffix == ".json":
                canonical = scientific_json(json.loads(path.read_text()), replacements)
                checked.update(kind="json", logical_sha256=logical_digest([canonical]))
            elif path.suffix == ".csv":
                with path.open(newline="", encoding="utf8") as stream:
                    rows = list(csv.reader(stream))
                checked.update(
                    kind="csv",
                    header=rows[0],
                    rows=len(rows) - 1,
                    logical_sha256=logical_digest(rows[1:]),
                )
            elif path.suffix == ".pdf":
                data = path.read_bytes()
                if not data.startswith(b"%PDF-") or not data.rstrip().endswith(b"%%EOF"):
                    raise ValueError(f"invalid PDF envelope: {path}")
                checked.update(kind="pdf", pages=len(re.findall(rb"/Type\s*/Page\b", data)))
            else:
                raise ValueError(f"unexpected diagnostic output type: {path}")
            output[key] = checked
    if not output:
        raise ValueError("analysis produced no outputs")
    return output


def equivalence(inventories):
    reference = inventories[0]
    for candidate in inventories[1:]:
        if set(candidate) != set(reference):
            raise ValueError("analysis output file sets differ")
        for key, original in reference.items():
            fields = ("kind", "logical_sha256", "schema", "rows", "header", "pages")
            if any(original.get(field) != candidate[key].get(field) for field in fields):
                raise ValueError(f"analysis scientific outputs differ: {key}")
    return True


def worker(spec):
    sys.path.insert(0, spec["tree"])
    import ipid_analysis.config as config

    if not below(config.__file__, spec["tree"]):
        raise RuntimeError("wrong Python source imported")
    repo = Path(spec["repo"])
    config.PROJ_ROOT = repo
    config.RAW_STORAGE_DIR = repo / "data/raw"
    config.PROCESSED_STORAGE_DIR = repo / "data/processed"
    config.REPORTS_STORAGE_DIR = repo / "reports"
    config.RAW_DATA_DIR = config.RAW_STORAGE_DIR / spec["scope"]
    config.PROCESSED_DATA_DIR = Path(spec["processed"])
    config.REPORTS_DIR = Path(spec["reports"])
    config.FIGURES_DIR = config.REPORTS_DIR / "figures"
    control = Path(spec["control"])
    guard = WriteGuard([control, spec["scratch"], spec["ram"], spec["processed"], spec["reports"]])
    sys.addaudithook(guard.audit)
    timings = Timings(control / "timings.jsonl")
    instrument(guard, timings, spec["remote_prefixes"])
    from ipid_analysis import postprocess, s3_workflow
    from ipid_analysis.manifest import load_manifest
    from ipid_analysis.ripe_atlas import infer_campaign_start

    instrument_science(postprocess, timings)
    request_data = json.loads(Path(spec["request"]).read_text())
    request = s3_workflow.AnalysisRequest(**request_data)
    manifest = load_manifest(Path(spec["manifest"]))
    s3_workflow.validate_analysis_manifest(manifest, request)
    paths = set()

    class ReadOnlyInputs:
        def download(self, uri, destination):
            path = uri_path(uri, spec)
            if path.resolve() != Path(destination).resolve() or not path.is_file():
                raise ValueError(f"missing or misbound mounted input: {path}")
            paths.add(path)

        def exists(self, uri):
            path = uri_path(uri, spec)
            if path.is_file():
                paths.add(path)
                return True
            return False

    s3_workflow.download_analysis_inputs(ReadOnlyInputs(), request, manifest, config.RAW_DATA_DIR)
    # Actual version-specific cache checks run, but all external downloads and
    # writes to canonical caches are forbidden by the guard. Empty remote cache
    # transport disables restore/publication, not the existing local validation.
    itdk = postprocess.prepare_itdk()
    ripe = postprocess.prepare_ripe_atlas(campaign_start=infer_campaign_start(manifest))
    paths.update((itdk.interfaces_path, itdk.metadata_path, ripe.roles_path, ripe.metadata_path))
    fingerprints = [verify_input(p) for p in sorted(paths)]
    write_json(control / "inputs.json", fingerprints)
    if spec["mode"] == "preflight":
        return
    if fingerprints != json.loads(Path(spec["inputs"]).read_text()):
        raise ValueError("benchmark input snapshot changed")
    # Verification warms input bytes equally before both revisions; exclude it
    # from science timing. Timings are inclusive and must not be summed blindly.
    timings.stream.seek(0)
    timings.stream.truncate()
    timings.sequence = 0
    parameters = inspect.signature(postprocess.main).parameters
    values = {}
    import resource

    import typer.models

    for name, parameter in parameters.items():
        default = parameter.default
        values[name] = (
            default.default if isinstance(default, typer.models.ParameterInfo) else default
        )
    status_path = control / "postprocess-status.json"
    values.update(
        manifest_path=Path(spec["manifest"]),
        threads=len(spec["cpu_ids"]),
        batch_size=1_000_000,
        strict=True,
        status_file=status_path,
    )
    before = resource.getrusage(resource.RUSAGE_SELF)
    children_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    io_before = proc_io(os.getpid())
    started = time.perf_counter()
    postprocess.main(**values)
    seconds = time.perf_counter() - started
    after = resource.getrusage(resource.RUSAGE_SELF)
    children_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    io_after = proc_io(os.getpid())
    status = json.loads(status_path.read_text())
    if (
        not status.get("required_complete")
        or status["missing"]
        or any(stage["status"] not in {"completed", "disabled"} for stage in status["stages"])
        or status["optional"]["ripe"] != "prepared"
        or status["optional"]["itdk"] != "prepared"
    ):
        raise ValueError("analysis incomplete, including required diagnostic external stages")
    if [verify_input(p) for p in sorted(paths)] != fingerprints:
        raise ValueError("source inputs changed during analysis")
    inventory = output_inventory(spec)
    write_json(control / "output-inventory.json", inventory)
    cpu = after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime
    child_cpu = (
        children_after.ru_utime
        + children_after.ru_stime
        - children_before.ru_utime
        - children_before.ru_stime
    )
    write_json(
        control / "result.json",
        dict(
            revision=spec["revision"],
            label=spec["label"],
            analysis_seconds=seconds,
            cpu_seconds=cpu,
            child_cpu_seconds=child_cpu,
            average_cpu_cores=(cpu + child_cpu) / seconds,
            kernel_peak_rss_bytes=after.ru_maxrss * 1024,
            io_delta={
                key: io_after[key] - value for key, value in io_before.items() if key in io_after
            },
            warnings=status["warnings"],
            code_revision=status["code_revision"],
            completed_stages=len(status["stages"]),
        ),
    )


def environment(spec):
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("IPID_ANALYSIS_") and k != "IPID_MAXMIND_DB"
    }
    env.update(
        PYTHON_DOTENV_DISABLED="1",
        PYTHONDONTWRITEBYTECODE="1",
        MPLBACKEND="Agg",
        PYTHONPATH=spec["tree"],
        MPLCONFIGDIR=str(Path(spec["control"]) / "matplotlib"),
        IPID_ANALYSIS_SCOPE=spec["scope"],
        IPID_ANALYSIS_STORAGE_MODE="local" if spec.get("test_local") else "mounted-s3",
        IPID_ANALYSIS_TMP_DIR=spec["scratch"],
        IPID_ANALYSIS_CONTROL_ROOT=spec["control"],
        IPID_ANALYSIS_LOCK_DIR=str(Path(spec["control"]) / "locks"),
        IPID_ANALYSIS_RIPE_TRANSFER_DIR=str(
            Path(spec["ram"] if spec["label"] == "B" else spec["scratch"]) / "transfers"
        ),
        IPID_ANALYSIS_RIPE_CACHE_S3="",
        IPID_ANALYSIS_S3_ROOTS=json.dumps(spec["s3_roots"]),
        IPID_ANALYSIS_SCRATCH_STORAGE="ram" if spec["label"] == "B" else "disk",
        IPID_ANALYSIS_RAM_DIR=spec["ram"],
        IPID_ANALYSIS_RAM_JOB_DIR=spec["ram"],
        IPID_ANALYSIS_RAM_BYTES=str(32 * GIB),
        IPID_ANALYSIS_JOB_SCRATCH_BYTES=str(spec["scratch_limit"]),
        IPID_ANALYSIS_JOB_MEMORY_BYTES=str(spec["memory_limit"]),
        IPID_ANALYSIS_DUCKDB_MEMORY=str(spec["memory_limit"] // 2) + "B",
        IPID_ANALYSIS_THREADS=str(len(spec["cpu_ids"])),
        IPID_ANALYSIS_RUN_OWNER=str(os.getpid()),
        OMP_NUM_THREADS="1",
        OPENBLAS_NUM_THREADS="1",
        MKL_NUM_THREADS="1",
        NUMEXPR_NUM_THREADS="1",
        TMPDIR=spec["scratch"],
        TMP=spec["scratch"],
        TEMP=spec["scratch"],
    )
    return env


def export_tree(repo, revision, destination):
    destination.mkdir()
    process = subprocess.Popen(
        ["git", "-C", str(repo), "archive", revision, "ipid_analysis"], stdout=subprocess.PIPE
    )
    try:
        with tarfile.open(fileobj=process.stdout, mode="r|") as archive:
            for member in archive:
                if not below(destination / member.name, destination) or not (
                    member.isfile() or member.isdir()
                ):
                    raise ValueError("unsafe Git archive member")
                if member.isdir():
                    (destination / member.name).mkdir(parents=True, exist_ok=True)
                else:
                    path = destination / member.name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(archive.extractfile(member).read())
        if process.wait():
            raise RuntimeError(f"git archive failed for pinned revision {revision}")
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait()


@contextmanager
def writer_lease(repo):
    import fcntl

    control = Path(
        os.environ.get(
            "IPID_ANALYSIS_CONTROL_ROOT",
            os.environ.get("IPID_ANALYSIS_TMP_DIR", str(repo / "tmp")),
        )
    )
    if not below(control, repo) or filesystem(control) != "ext4":
        raise ValueError("production control root must be an ext4 directory under this checkout")
    key = hashlib.sha256(b"analysis-writer").hexdigest()
    lock_roots = {control / "locks", control / "global-locks"}
    if os.environ.get("IPID_ANALYSIS_LOCK_DIR"):
        lock_roots.add(Path(os.environ["IPID_ANALYSIS_LOCK_DIR"]))
    opened = []
    try:
        for root in sorted(lock_roots):
            if not below(root, repo):
                raise ValueError("writer lock root is outside the checkout")
            path = root / key / "owner.lock"
            path.parent.mkdir(parents=True, exist_ok=True)
            stream = path.open("a+b")
            opened.append(stream)
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError(
                    "analysis writer is active; finish jobs and stop worker first"
                ) from exc
        yield
    finally:
        for stream in opened:
            stream.close()


def run_child(spec, timeout):
    control = Path(spec["control"])
    for path in (control, Path(spec["scratch"]), Path(spec["ram"])):
        path.mkdir(parents=True, exist_ok=True)
    write_json(control / "spec.json", spec)
    peak_rss = peak_files = 0
    started = time.monotonic()
    with (control / "run.log").open("w", encoding="utf8") as log:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "_worker", str(control / "spec.json")],
            env=environment(spec),
            cwd=spec["tree"],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            while process.poll() is None:
                rss = 0
                for entry in Path("/proc").iterdir():
                    if not entry.name.isdigit():
                        continue
                    try:
                        stat = (entry / "stat").read_text().rsplit(")", 1)[1].split()
                        if int(stat[2]) == process.pid:
                            rss += proc_rss(int(entry.name))
                    except (FileNotFoundError, ProcessLookupError):
                        pass
                peak_rss = max(peak_rss, rss)
                peak_files = max(
                    peak_files, tree_bytes(Path(spec["ram"])) + tree_bytes(Path(spec["scratch"]))
                )
                if peak_rss > spec["memory_limit"] or peak_files > spec["scratch_limit"]:
                    raise RuntimeError("benchmark resource budget exceeded")
                if time.monotonic() - started > timeout:
                    raise TimeoutError("benchmark trial timeout")
                time.sleep(0.5)
            if process.returncode:
                raise RuntimeError(f"benchmark child failed: {control / 'run.log'}")
        finally:
            stop(process)
    result = dict(
        sampled_peak_rss_bytes=peak_rss,
        sampled_peak_scratch_bytes=peak_files,
        end_to_end_seconds=time.monotonic() - started,
    )
    write_json(control / "supervision.json", result)
    return result


def assert_headroom(memory, scratch):
    values = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    if int(values["MemAvailable"].split()[0]) * 1024 < memory + scratch + 4 * GIB:
        raise RuntimeError("insufficient free RAM for compute, tmpfs and 4 GiB reserve")
    if shutil.disk_usage("/dev/shm").free < scratch + GIB:
        raise RuntimeError("insufficient existing tmpfs capacity")


def swap_counters():
    return {
        name: int(value)
        for name, value in (line.split() for line in Path("/proc/vmstat").read_text().splitlines())
        if name in {"pswpin", "pswpout"}
    }


def remove_owned_ram(path, namespace):
    if (
        path.is_symlink()
        or not below(path, "/dev/shm")
        or not path.name.startswith(namespace + "-")
    ):
        raise ValueError("refusing unsafe benchmark RAM cleanup")
    if path.stat().st_uid != os.getuid() or filesystem(path) != "tmpfs":
        raise ValueError("RAM workspace ownership/filesystem changed")
    shutil.rmtree(path)


def mount_remote(root):
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if entry.stat().st_uid != os.geteuid():
                continue
            args = [v.decode() for v in (entry / "cmdline").read_bytes().split(b"\0") if v]
            if args and Path(args[0]).name == "rclone" and "mount" in args:
                index = args.index("mount")
                if Path(args[index + 2]).resolve() == root.resolve():
                    return args[index + 1].rstrip("/")
        except (FileNotFoundError, ProcessLookupError, PermissionError, IndexError):
            continue
    raise RuntimeError(f"cannot identify same-user rclone mount: {root}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute", action="store_true", help="allow new isolated S3 test outputs"
    )
    parser.add_argument("--repo", type=Path, default=PROJECT)
    parser.add_argument("--request-uri", default=REQUEST)
    parser.add_argument("--order", choices=("AB", "BA", "ABBA"), default="AB")
    parser.add_argument("--cpus", type=int, default=16)
    parser.add_argument("--memory-gib", type=int, default=125)
    parser.add_argument("--scratch-gib", type=int, default=21)
    parser.add_argument("--timeout", type=int, default=7200)
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error(
            "requires --execute; creates isolated test objects, never overwrites production outputs"
        )
    if sys.platform != "linux":
        parser.error("requires Linux, existing ext4 control storage and existing tmpfs")
    repo = args.repo.resolve()
    if (
        not (1 <= args.cpus <= len(os.sched_getaffinity(0)))
        or args.memory_gib < 4
        or not 1 <= args.scratch_gib <= 32
    ):
        parser.error("invalid CPU/RAM/scratch budget")
    match = re.fullmatch(
        r"(s3://[^\s]+?)/continental/([A-Za-z0-9][A-Za-z0-9_-]{0,79})/sites/(eu|as|af|na|sa|au)/analysis-jobs/(tcp-80_[A-Za-z0-9_.-]+)/request.json",
        args.request_uri,
    )
    if not match:
        parser.error("requires a Geographic TCP analysis request")
    workflow, campaign, site, job = match.groups()
    roots = json.loads(os.environ.get("IPID_ANALYSIS_S3_ROOTS", "{}"))
    if set(roots) != {"raw", "processed", "reports"}:
        parser.error("set IPID_ANALYSIS_S3_ROOTS to the existing three mounted S3 roots")
    scratch_parent = repo / "tmp/benchmarks"
    scratch_parent.mkdir(parents=True, exist_ok=True)
    if filesystem(scratch_parent) != "ext4" or filesystem(Path("/dev/shm")) != "tmpfs":
        parser.error("requires ext4 diagnostic directory and existing /dev/shm tmpfs")
    with writer_lease(repo):
        result = Path(tempfile.mkdtemp(prefix="analysis-ab-", dir=scratch_parent))
        namespace = "analysis-ab-" + uuid.uuid4().hex
        print(f"Diagnostics: {result}", flush=True)
        write_json(
            result / "benchmark.json",
            dict(
                pins=PINS,
                order=args.order,
                note="A includes PR #114; comparison excludes cold downloads and scheduling",
                request_uri=args.request_uri,
                namespace=namespace,
            ),
        )
        trees = {}
        for label, revision in PINS.items():
            trees[label] = result / ("source-" + label)
            export_tree(repo, revision, trees[label])
        request_path, manifest_path = result / "request.json", result / "manifest.json"
        subprocess.run(
            ["s3cmd", "get", "--no-progress", args.request_uri, str(request_path)], check=True
        )
        request = json.loads(request_path.read_text())
        expected = args.request_uri.removesuffix("request.json") + "manifest.json"
        if request.get("manifest_uri") != expected or request.get("os_status") != "not_requested":
            raise ValueError("benchmark requires the bound No-OS request/manifest pair")
        subprocess.run(["s3cmd", "get", "--no-progress", expected, str(manifest_path)], check=True)
        cpus = sorted(os.sched_getaffinity(0))[: args.cpus]
        common = dict(
            repo=str(repo),
            scope=f"continental/{campaign}/{site}",
            request=str(request_path),
            manifest=str(manifest_path),
            s3_roots=roots,
            cpu_ids=cpus,
            scratch_limit=args.scratch_gib * GIB,
            memory_limit=args.memory_gib * GIB,
        )
        mounts = {
            name: repo / path
            for name, path in (
                ("raw", "data/raw"),
                ("processed", "data/processed"),
                ("reports", "reports"),
            )
        }
        remotes = {name: mount_remote(path) for name, path in mounts.items()}
        if any(
            remotes[name].partition(":")[2].rstrip("/")
            != roots[name].removeprefix("s3://").rstrip("/")
            for name in mounts
        ):
            raise ValueError("configured S3 root does not match actual rclone mount")
        for root in mounts.values():
            if not root.is_mount():
                raise RuntimeError(f"required data mount missing: {root}")
        trials = []
        for index, label in enumerate("B" + args.order):
            mode = "preflight" if index == 0 else "trial"
            trial_name = f"{index:02}-{mode}-{label}"
            trial = result / trial_name
            ram = Path(tempfile.mkdtemp(prefix=namespace + "-" + trial_name + "-", dir="/dev/shm"))
            relative = f".ipid-analysis-tests/{namespace}/{trial_name}"
            processed, reports = mounts["processed"] / relative, mounts["reports"] / relative
            if processed.exists() or reports.exists():
                raise FileExistsError("benchmark namespace already exists")
            spec = dict(
                common,
                tree=str(trees[label]),
                label=label,
                revision=PINS[label],
                mode=mode,
                control=str(trial / "control"),
                scratch=str(trial / "scratch"),
                ram=str(ram),
                processed=str(processed),
                reports=str(reports),
                remote_prefixes=[remotes[k] + "/" + relative for k in ("processed", "reports")],
                inputs=str(result / "00-preflight-B/control/inputs.json"),
            )
            print(f"Starting {trial_name}", flush=True)
            assert_headroom(spec["memory_limit"], spec["scratch_limit"])
            if shutil.disk_usage(scratch_parent).free < spec["scratch_limit"] + 4 * GIB:
                raise RuntimeError("insufficient ext4 workspace capacity")
            swap_before = swap_counters()
            try:
                run_child(spec, args.timeout)
            finally:
                remove_owned_ram(ram, namespace)
            write_json(
                trial / "control/swap.json", dict(before=swap_before, after=swap_counters())
            )
            if mode == "trial":
                trials.append(spec)
        write_json(result / "trials.json", trials)
        equivalent = equivalence(
            [
                json.loads((Path(s["control"]) / "output-inventory.json").read_text())
                for s in trials
            ]
        )
        results = [json.loads((Path(s["control"]) / "result.json").read_text()) for s in trials]
        write_json(
            result / "summary.json",
            dict(
                status="passed",
                scientific_equivalence=equivalent,
                results=results,
                limitations=[
                    "PDF envelope/page counts checked, not visual equality",
                    "Includes original version-specific publication, excludes scheduler and cold cache builds",
                    "SQL execution/reading can be lazy; timings are nested/inclusive",
                    "AB is preliminary; repeat in reverse order before attributing small differences",
                ],
            ),
        )
        print(
            f"Completed trials; diagnostics and isolated S3 results retained: {result}", flush=True
        )


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "_worker":
        spec = json.loads(Path(sys.argv[2]).read_text())
        os.sched_setaffinity(0, spec["cpu_ids"])
        worker(spec)
    else:
        main()
