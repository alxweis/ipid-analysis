"""S3-only worker for measurement handoff and complete postprocessing.

The worker polls for ``jobs/<id>/request.json`` objects. For each request it
downloads the completed stateless RT measurement (ICMP, TCP, or UDP-DNS), runs
the normal IPID strategy classifier, uploads a ZMap-compatible UNCLASSIFIED
target parquet beside the measurement's ``ipid.pq``, and only then publishes
``done.json`` under the job prefix. A measurement VM can therefore treat the
done marker as an atomic readiness signal.

After a full protocol sweep, ``analysis-jobs/<zmap-id>/request.json`` starts the
normal manifest-driven postprocessing. The supervisor reserves critical handoff
capacity while background children retain their manifest, log and terminal marker.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import typer

from ipid_analysis.artifacts import (
    assert_complete,
    atomic_output,
    completion_path,
    file_digest,
    fingerprints,
    publication_inventory,
    publish_file,
)
from ipid_analysis.config import (
    FIGURES_DIR,
    PROCESSED_DATA_DIR,
    PROCESSED_STORAGE_DIR,
    PROJ_ROOT,
    RAW_DATA_DIR,
    RAW_STORAGE_DIR,
    REPORTS_STORAGE_DIR,
    SCOPE,
    os_source_path,
)
from ipid_analysis.interprotocol import (
    INTERPROTOCOL_FIGURES_DIR,
    build_target_files,
    classify_campaign,
    load_run_manifest,
)
from ipid_analysis.manifest import (
    iter_ipid_measurements,
    iter_random_reproducibility,
    load_manifest,
)
from ipid_analysis.random_reproducibility_analysis import (
    COHORT_FILE_NAME,
    DEFAULT_MAXIMUM_TARGETS,
    PREPARE_METADATA_FILE_NAME,
    TARGET_FILE_NAME,
    prepare_random_reproducibility_paths,
)
from ipid_analysis.runtime import (
    cleanup_current_artifacts,
    cleanup_work,
    connect,
    exclusive_lock,
    mounted_storage,
    scratch_root,
    start_run,
    temporary_path,
    validate_storage,
    work_path,
)
from ipid_analysis.strategies import classify_paths

PROTOCOL_VERSION = 2
ANALYSIS_JOB_VERSION = 1
INTERPROTOCOL_JOB_VERSION = 1
INTERPROTOCOL_TARGET_JOB_VERSION = 1
JOB_ID_RE = re.compile(r"^(?!\.{1,2}$)[A-Za-z0-9._-]+$")
TARGET_NAME = "zmap_unclassified.pq"
PURPOSE_RT_UNCLASSIFIED = "rt-unclassified"
PURPOSE_RANDOM_REPRODUCIBILITY = "random-reproducibility"
CONNECTION_TARGET_NAME = "zmap-connection-sample.pq"
CONNECTION_TARGET_METADATA_NAME = "zmap-connection-sample.json"
FIXED_BASE_TARGET_NAME = "zmap-fixed-base-sample.pq"
FIXED_BASE_TARGET_METADATA_NAME = "zmap-fixed-base-sample.json"
SUPPORTED_PROTOCOLS = frozenset({"icmp", "tcp", "udp-dns"})

app = typer.Typer()


def join_s3(prefix: str, *parts: str) -> str:
    value = prefix.rstrip("/")
    for part in parts:
        value += "/" + part.strip("/")
    return value


def measurement_protocol(measurement_id: str) -> str:
    """Derive the workflow protocol from an ipid-measure measurement ID."""
    prefix = measurement_id.split("_", 1)[0]
    if prefix == "icmp":
        return "icmp"
    if prefix == "tcp" or prefix.startswith("tcp-"):
        return "tcp"
    if prefix == "udp-dns" or prefix.startswith("udp-dns-"):
        return "udp-dns"
    raise ValueError(f"unsupported measurement id protocol in {measurement_id!r}")


@dataclass(frozen=True)
class Request:
    version: int
    job_id: str
    protocol: str
    measurement_id: str
    ipid_uri: str
    snapshot_uri: str
    result_uri: str
    done_uri: str
    failed_uri: str
    created_at: str
    purpose: str = PURPOSE_RT_UNCLASSIFIED
    maximum_targets: int = DEFAULT_MAXIMUM_TARGETS
    selection_seed: int = 42

    @classmethod
    def parse(cls, data: dict, s3_prefix: str) -> Request:
        try:
            request = cls(**data)
        except TypeError as exc:
            raise ValueError(f"invalid request fields: {exc}") from exc
        if request.version != PROTOCOL_VERSION:
            raise ValueError(f"unsupported request version {request.version}")
        if not JOB_ID_RE.fullmatch(request.job_id) or request.job_id != request.measurement_id:
            raise ValueError("invalid job_id or measurement_id")
        if request.protocol not in SUPPORTED_PROTOCOLS:
            raise ValueError(f"unsupported protocol {request.protocol!r}")
        expected_protocol = measurement_protocol(request.measurement_id)
        if request.protocol != expected_protocol:
            raise ValueError(
                f"protocol {request.protocol!r} does not match measurement id "
                f"({expected_protocol!r})"
            )
        if not request.ipid_uri.startswith("s3://") or not request.ipid_uri.endswith("/ipid.pq"):
            raise ValueError("invalid ipid_uri")
        measurement_prefix = request.ipid_uri.rsplit("/", 1)[0]
        raw = json.loads(os.environ.get("IPID_ANALYSIS_S3_ROOTS", "{}")).get("raw")
        if SCOPE and raw and measurement_prefix != join_s3(raw,SCOPE,"ipid",request.measurement_id):
            raise ValueError("handoff crosses campaign/site boundaries")
        expected_snapshot_uri = join_s3(measurement_prefix, "ipid.snapshot.yaml")
        if request.snapshot_uri != expected_snapshot_uri:
            raise ValueError("snapshot_uri does not match the IPID measurement prefix")

        if request.purpose not in {
            PURPOSE_RT_UNCLASSIFIED,
            PURPOSE_RANDOM_REPRODUCIBILITY,
        }:
            raise ValueError(f"unsupported analysis purpose {request.purpose!r}")
        if not 0 <= request.maximum_targets <= DEFAULT_MAXIMUM_TARGETS:
            raise ValueError(f"maximum_targets must be in [0, {DEFAULT_MAXIMUM_TARGETS}]")

        job_prefix = join_s3(s3_prefix, "jobs", request.job_id)
        result_name = (
            TARGET_NAME if request.purpose == PURPOSE_RT_UNCLASSIFIED else TARGET_FILE_NAME
        )
        expected = {
            "result_uri": join_s3(measurement_prefix, result_name),
            "done_uri": join_s3(job_prefix, "done.json"),
            "failed_uri": join_s3(job_prefix, "failed.json"),
        }
        for field, value in expected.items():
            if getattr(request, field) != value:
                raise ValueError(f"{field} does not match its canonical S3 location")
        return request


@dataclass(frozen=True)
class Done:
    version: int
    job_id: str
    result_uri: str
    rows: int
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class AnalysisRequest:
    version: int
    job_id: str
    protocol: str
    manifest_uri: str
    zmap_prefix: str
    os_prefix: str
    ipid_prefix: str
    done_uri: str
    failed_uri: str
    created_at: str
    fixed_base_target_uri: str | None = None
    connection_target_uri: str | None = None

    @classmethod
    def parse(cls, data: dict, s3_prefix: str) -> AnalysisRequest:
        try:
            request = cls(**data)
        except TypeError as exc:
            raise ValueError(f"invalid analysis request fields: {exc}") from exc
        if request.version != ANALYSIS_JOB_VERSION:
            raise ValueError(f"unsupported analysis request version {request.version}")
        if not JOB_ID_RE.fullmatch(request.job_id):
            raise ValueError("invalid analysis job_id")
        if request.protocol not in SUPPORTED_PROTOCOLS:
            raise ValueError(f"unsupported protocol {request.protocol!r}")
        expected_protocol = measurement_protocol(request.job_id)
        if request.protocol != expected_protocol:
            raise ValueError(
                f"protocol {request.protocol!r} does not match job id ({expected_protocol!r})"
            )
        for field in ("zmap_prefix", "os_prefix", "ipid_prefix"):
            value = getattr(request, field)
            if not value.startswith("s3://") or not value.removeprefix("s3://").strip("/"):
                raise ValueError(f"invalid {field}")

        job_prefix = join_s3(s3_prefix, "analysis-jobs", request.job_id)
        expected = {
            "manifest_uri": join_s3(job_prefix, "manifest.json"),
            "done_uri": join_s3(job_prefix, "done.json"),
            "failed_uri": join_s3(job_prefix, "failed.json"),
        }
        for field, value in expected.items():
            if getattr(request, field) != value:
                raise ValueError(f"{field} does not match its canonical S3 location")

        if request.connection_target_uri is not None:
            if request.protocol != "tcp" or request.connection_target_uri != join_s3(
                request.zmap_prefix, request.job_id, CONNECTION_TARGET_NAME
            ):
                raise ValueError("invalid connection_target_uri")
        if request.fixed_base_target_uri is not None:
            expected_target_uri = join_s3(
                request.zmap_prefix,
                request.job_id,
                FIXED_BASE_TARGET_NAME,
            )
            if request.fixed_base_target_uri != expected_target_uri:
                raise ValueError("fixed_base_target_uri does not match its canonical S3 location")
        return request


@dataclass(frozen=True)
class AnalysisDone:
    version: int
    job_id: str
    manifest_uri: str
    log_uri: str
    completed_at: str


@dataclass(frozen=True)
class InterprotocolRequest:
    version: int
    job_id: str
    campaign_id: str
    manifest_uri: str
    ipid_prefix: str
    result_prefix: str
    done_uri: str
    failed_uri: str
    created_at: str

    @classmethod
    def parse(cls, data: dict, s3_prefix: str) -> InterprotocolRequest:
        try:
            request = cls(**data)
        except TypeError as exc:
            raise ValueError(f"invalid inter-protocol request fields: {exc}") from exc
        if request.version != INTERPROTOCOL_JOB_VERSION:
            raise ValueError(f"unsupported inter-protocol request version {request.version}")
        if not JOB_ID_RE.fullmatch(request.job_id):
            raise ValueError("invalid inter-protocol job_id")
        if not JOB_ID_RE.fullmatch(request.campaign_id):
            raise ValueError("invalid inter-protocol campaign_id")
        if not request.ipid_prefix.startswith("s3://"):
            raise ValueError("invalid inter-protocol ipid_prefix")
        raw = json.loads(os.environ.get("IPID_ANALYSIS_S3_ROOTS", "{}")).get("raw")
        if SCOPE and raw and request.ipid_prefix.rstrip("/") != join_s3(raw,SCOPE,"ipid"):
            raise ValueError("interprotocol request crosses campaign/site boundaries")
        job_prefix = join_s3(s3_prefix, "interprotocol-jobs", request.job_id)
        expected = {
            "manifest_uri": join_s3(job_prefix, "manifest.json"),
            "result_prefix": join_s3(job_prefix, "results"),
            "done_uri": join_s3(job_prefix, "done.json"),
            "failed_uri": join_s3(job_prefix, "failed.json"),
        }
        for field, value in expected.items():
            if getattr(request, field) != value:
                raise ValueError(f"{field} does not match its canonical S3 location")
        return request


@dataclass(frozen=True)
class InterprotocolDone:
    version: int
    job_id: str
    campaign_id: str
    result_prefix: str
    rows: int
    completed_at: str


@dataclass(frozen=True)
class InterprotocolTargetRequest:
    version: int
    job_id: str
    campaign_id: str
    manifests: dict[str, str]
    target_prefix: str
    done_uri: str
    failed_uri: str
    created_at: str

    @classmethod
    def parse(cls, data: dict, s3_prefix: str) -> InterprotocolTargetRequest:
        try:
            request = cls(**data)
        except TypeError as exc:
            raise ValueError(f"invalid inter-protocol target request fields: {exc}") from exc
        if request.version != INTERPROTOCOL_TARGET_JOB_VERSION:
            raise ValueError(
                f"unsupported inter-protocol target request version {request.version}"
            )
        if not JOB_ID_RE.fullmatch(request.job_id) or not JOB_ID_RE.fullmatch(request.campaign_id):
            raise ValueError("invalid inter-protocol target job or campaign id")
        if set(request.manifests) != {"icmp", "tcp", "udp"}:
            raise ValueError("inter-protocol target request requires icmp, tcp, and udp manifests")
        for protocol, uri in request.manifests.items():
            if SCOPE and not uri.startswith(join_s3(s3_prefix,"analysis-jobs")+"/"):
                raise ValueError("target request crosses campaign/site boundaries")
            parts = uri.rstrip("/").split("/")
            if (
                not uri.startswith("s3://")
                or len(parts) < 3
                or parts[-1] != "manifest.json"
                or parts[-3] != "analysis-jobs"
                or not JOB_ID_RE.fullmatch(parts[-2])
            ):
                raise ValueError(f"invalid {protocol} analysis manifest URI")
        job_prefix = join_s3(s3_prefix, "interprotocol-target-jobs", request.job_id)
        expected = {
            "target_prefix": join_s3(job_prefix, "targets"),
            "done_uri": join_s3(job_prefix, "done.json"),
            "failed_uri": join_s3(job_prefix, "failed.json"),
        }
        for field, value in expected.items():
            if getattr(request, field) != value:
                raise ValueError(f"{field} does not match its canonical S3 location")
        return request


@dataclass(frozen=True)
class InterprotocolTargetDone:
    version: int
    job_id: str
    campaign_id: str
    target_prefix: str
    rows: dict[str, int]
    completed_at: str


class S3Client:
    def __init__(self, run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run, *, timeout: int | None = None):
        self._run = run
        self._timeout = timeout

    def _command(self, *args: str) -> str:
        for attempt in range(3):
            try:
                result = self._run(
                    ["s3cmd", *args],
                    check=True,
                    text=True,
                    capture_output=True,
                    timeout=self._timeout or int(os.environ.get("IPID_ANALYSIS_S3_TIMEOUT", "3600")),
                )
                return result.stdout
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as exc:
                transient = isinstance(exc, subprocess.TimeoutExpired) or any(
                    text in str(getattr(exc, "stderr", "")).lower()
                    for text in (
                        "timeout",
                        "timed out",
                        "connection",
                        "slowdown",
                        "503",
                        "500",
                        "502",
                        "504",
                    )
                )
                if not transient or attempt == 2:
                    raise
                time.sleep(2**attempt)
        raise AssertionError("unreachable")

    def mapped_path(self, uri: str) -> Path | None:
        """Explicit URI-to-mount identity; never infer a provider's bucket syntax."""
        roots = json.loads(os.environ.get("IPID_ANALYSIS_S3_ROOTS", "{}"))
        for name, root in (
            ("raw", RAW_STORAGE_DIR),
            ("processed", PROCESSED_STORAGE_DIR),
            ("reports", REPORTS_STORAGE_DIR),
        ):
            prefix = roots.get(name, "").rstrip("/")
            if prefix and uri.startswith(prefix + "/"):
                relative = uri[len(prefix) + 1 :]
                if ".." in relative.split("/") or "\\" in relative:
                    raise ValueError("invalid object key")
                candidate = root / relative
                if root.resolve() not in candidate.resolve().parents:
                    raise ValueError("object resolves outside configured mount")
                return candidate
        return None

    def input_path(self, uri: str, fallback: Path) -> Path:
        mapped = self.mapped_path(uri) if mounted_storage() else None
        if mapped is not None:
            self._check_mounted_input(uri, mapped)
            return mapped
        self.download(uri, fallback)
        return fallback

    def _check_mounted_input(self, uri: str, path: Path) -> None:
        validate_storage()
        info = self._command("info", uri)
        size = re.search(r"(?:File size|Content-Length):\s*(\d+)", info, re.I)
        if size is None:
            raise ValueError(f"S3 did not report a size for {uri}")
        deadline = time.monotonic() + int(
            os.environ.get("IPID_ANALYSIS_VISIBILITY_TIMEOUT", "330")
        )
        while not path.is_file() or path.stat().st_size != int(size[1]):
            if time.monotonic() >= deadline:
                raise TimeoutError(f"S3 object is not yet visible through FUSE: {path}")
            time.sleep(1)
        assert_complete(path)

    def exists(self, uri: str) -> bool:
        terminal = uri.endswith(("/done.json", "/failed.json"))
        cached = work_path(uri, category="terminal") / "seen" if terminal else None
        if cached is not None and cached.is_file():
            # Done is immutable; externally removed failures are rechecked shortly.
            if uri.endswith("/done.json") or time.time() - cached.stat().st_mtime < 30:
                return True
        output = self._command("ls", uri)
        found = any(line.split() and line.split()[-1] == uri for line in output.splitlines())
        if found and cached is not None:
            cached.write_text(uri, encoding="utf-8")
        return found

    def objects(self, prefix: str):
        # Spool listings locally rather than retaining all historical object names.
        import tempfile

        with tempfile.TemporaryFile(mode="w+", encoding="utf-8", dir=scratch_root()) as listing:
            self._run(
                ["s3cmd", "ls", "--recursive", prefix],
                check=True,
                text=True,
                stdout=listing,
                stderr=subprocess.PIPE,
                timeout=int(os.environ.get("IPID_ANALYSIS_LIST_TIMEOUT", "30")),
            )
            listing.seek(0)
            for line in listing:
                fields = line.split()
                if fields and fields[-1].startswith("s3://"):
                    yield fields[-1]

    def _requests(self, prefix: str):
        return (uri for uri in self.objects(prefix) if uri.endswith("/request.json"))

    def list_requests(self, s3_prefix: str):
        return self._requests(join_s3(s3_prefix, "jobs"))

    def list_analysis_requests(self, s3_prefix: str):
        return self._requests(join_s3(s3_prefix, "analysis-jobs"))

    def list_interprotocol_requests(self, s3_prefix: str):
        return self._requests(join_s3(s3_prefix, "interprotocol-jobs"))

    def list_interprotocol_target_requests(self, s3_prefix: str):
        return self._requests(join_s3(s3_prefix, "interprotocol-target-jobs"))

    def download(self, uri: str, path: Path) -> None:
        if mounted_storage() and RAW_DATA_DIR.resolve() in path.resolve().parents:
            if self.mapped_path(uri) != path:
                raise ValueError(
                    "refusing to hydrate raw S3 storage; configure IPID_ANALYSIS_S3_ROOTS"
                )
            self._check_mounted_input(uri, path)
            return
        with atomic_output(path) as temporary:
            self._command("get", "--force", "--no-progress", uri, str(temporary))

    def upload(self, path: Path, uri: str) -> None:
        assert_complete(path)
        if mounted_storage() and self.mapped_path(uri) == path:
            self._check_mounted_input(uri, path)
            return
        self._command("put", "--no-progress", str(path), uri)

    def delete_failure(self, uri: str) -> None:
        if not uri.endswith("/failed.json"):
            raise ValueError("only an explicitly retried failure marker may be removed")
        self._command("del", uri)
        (work_path(uri, category="terminal") / "seen").unlink(missing_ok=True)


def build_unclassified_targets(strategies_path: Path, output_path: Path) -> int:
    """Stream UNCLASSIFIED addresses into the schema consumed by ipid-measure."""
    with connect() as con, atomic_output(output_path, inputs=[strategies_path]) as temporary:
        reader = con.execute(
            "SELECT CAST(IP_ADDR AS VARCHAR) AS IP_ADDR, "
            "       CAST('' AS VARCHAR) AS REPLY_TYPE "
            "FROM read_parquet($path) "
            "WHERE CAST(IPID_SELECTION_STRATEGY AS VARCHAR) = 'UNCLASSIFIED'",
            {"path": str(strategies_path)},
        ).to_arrow_reader(1_000_000)
        schema = pa.schema([("IP_ADDR", pa.string()), ("REPLY_TYPE", pa.string())])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        writer = pq.ParquetWriter(temporary, schema, compression="zstd")
        rows = 0
        try:
            for batch in reader:
                batch = batch.cast(schema)
                writer.write_batch(batch)
                rows += batch.num_rows
        finally:
            writer.close()
            con.close()
        return rows


def sha256_file(path: Path) -> str:
    return file_digest(path)


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = temporary_path(path)
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    publish_file(temporary, path)
    temporary.unlink(missing_ok=True)


def cleanup_completed_job(work_dir: Path) -> None:
    try:
        if any(
            root.resolve() == work_dir.resolve() or root.resolve() in work_dir.resolve().parents
            for root in (RAW_DATA_DIR, PROCESSED_DATA_DIR, PROJ_ROOT / "reports")
        ):
            raise ValueError("refusing to clean persistent storage")
        cleanup_work(work_dir, root=work_dir.parent)
    except (OSError, ValueError):
        # Completion is already durable. A locked local file must never turn a
        # successful job into a second, contradictory terminal failure marker.
        logger.exception(f"completed job retained for local cleanup: {work_dir}")


def load_request(client: S3Client, request_uri: str, s3_prefix: str, work_dir: Path) -> Request:
    path = work_dir / "request.json"
    client.download(request_uri, path)
    return Request.parse(json.loads(path.read_text()), s3_prefix)


def discard_failed_bulk(work_dir: Path) -> None:
    if any(
        root.resolve() == work_dir.resolve() or root.resolve() in work_dir.resolve().parents
        for root in (RAW_DATA_DIR, PROCESSED_DATA_DIR, PROJ_ROOT / "reports")
    ):
        raise ValueError("refusing to discard data from persistent storage")
    if not work_dir.is_dir() or work_dir.is_symlink():
        return
    for path in work_dir.rglob("*.pq"):
        if path.is_symlink() or work_dir.resolve() not in path.resolve().parents:
            continue
        try:
            path.unlink(missing_ok=True)
        except OSError:
            logger.warning(f"failed-job bulk file retained for local maintenance: {path}")


def process_request(
    client: S3Client,
    request_uri: str,
    s3_prefix: str,
    work_root: Path,
    batch_size: int,
    threads: int,
) -> bool:
    job_id = request_uri.rstrip("/").split("/")[-2]
    if not JOB_ID_RE.fullmatch(job_id):
        raise ValueError(f"invalid job id in request URI: {request_uri}")
    work_dir = work_root / job_id
    job_prefix = join_s3(s3_prefix, "jobs", job_id)
    done_uri = join_s3(job_prefix, "done.json")
    failed_uri = join_s3(job_prefix, "failed.json")

    if client.exists(done_uri):
        cleanup_completed_job(work_dir)
        return False
    if client.exists(failed_uri):
        return False

    request = None
    try:
        request = load_request(client, request_uri, s3_prefix, work_dir)
        if request.job_id != job_id:
            raise ValueError("request job_id does not match request URI")
        input_path = work_dir / "ipid.pq"
        snapshot_path = work_dir / "ipid.snapshot.yaml"
        strategies_path = work_dir / "strategies.pq"
        result_path = work_dir / TARGET_NAME

        if hasattr(client, "input_path"):
            input_path = client.input_path(request.ipid_uri, input_path)
            snapshot_path = client.input_path(request.snapshot_uri, snapshot_path)
        else:
            client.download(request.ipid_uri, input_path)
            client.download(request.snapshot_uri, snapshot_path)
        classify_paths(
            input_path,
            snapshot_path,
            strategies_path,
            protocol=request.protocol,
            mass=request.purpose == PURPOSE_RANDOM_REPRODUCIBILITY,
            batch_size=batch_size,
            compression="zstd",
            threads=threads,
        )
        measurement_prefix = request.ipid_uri.rsplit("/", 1)[0]
        strategies_uri = join_s3(measurement_prefix, "strategies.pq")
        client.upload(strategies_path, strategies_uri)
        # Keep portable generation evidence when successful local work is removed.
        # This additive companion does not change the measurement/Done schemas.
        provenance_path = work_dir / "classification-provenance.json"
        evidence = json.loads(completion_path(strategies_path).read_text())
        write_json(
            provenance_path,
            {"version": 1, "strategies_uri": strategies_uri, "generation": evidence},
        )
        client.upload(provenance_path, join_s3(measurement_prefix, provenance_path.name))
        if request.purpose == PURPOSE_RT_UNCLASSIFIED:
            rows = build_unclassified_targets(strategies_path, result_path)
        else:
            result_path = work_dir / TARGET_FILE_NAME
            cohort_path = work_dir / COHORT_FILE_NAME
            metadata_path = work_dir / PREPARE_METADATA_FILE_NAME
            outputs = prepare_random_reproducibility_paths(
                input_path,
                snapshot_path,
                strategies_path,
                measurement_id=request.measurement_id,
                target=f"{request.protocol}.ipid.no-connection.fixed-interval.mass",
                cohort_path=cohort_path,
                target_path=result_path,
                json_path=metadata_path,
                maximum_targets=request.maximum_targets,
                seed=request.selection_seed,
            )
            rows = pq.read_metadata(outputs["targets"]).num_rows
            client.upload(
                cohort_path,
                join_s3(measurement_prefix, COHORT_FILE_NAME),
            )
            client.upload(
                metadata_path,
                join_s3(measurement_prefix, PREPARE_METADATA_FILE_NAME),
            )
        client.upload(result_path, request.result_uri)

        done = Done(
            version=PROTOCOL_VERSION,
            job_id=request.job_id,
            result_uri=request.result_uri,
            rows=rows,
            size_bytes=result_path.stat().st_size,
            sha256=sha256_file(result_path),
        )
        done_path = work_dir / "done.json"
        write_json(done_path, asdict(done))
        client.upload(done_path, request.done_uri)
        logger.success(
            f"[{request.job_id}] persisted strategies and uploaded {rows:,} "
            f"{request.purpose} targets"
        )
        cleanup_completed_job(work_dir)
        return True
    except Exception as exc:
        cleanup_current_artifacts()
        discard_failed_bulk(work_dir)
        failed_path = work_dir / "failed.json"
        failed_job_id = request.job_id if request is not None else job_id
        write_json(
            failed_path,
            {"version": PROTOCOL_VERSION, "job_id": failed_job_id, "error": str(exc)},
        )
        try:
            client.upload(failed_path, failed_uri)
            (work_dir / ".failure-published").write_text(failed_uri, encoding="utf-8")
        except Exception:
            logger.exception(f"[{failed_job_id}] could not upload failure marker")
        raise


def run_pending(
    client: S3Client,
    s3_prefix: str,
    work_root: Path,
    batch_size: int,
    threads: int,
) -> int:
    processed = 0
    for request_uri in client.list_requests(s3_prefix):
        try:
            processed += int(
                process_request(client, request_uri, s3_prefix, work_root, batch_size, threads)
            )
        except Exception:
            logger.exception(f"failed processing {request_uri}")
    return processed


def load_analysis_request(
    client: S3Client, request_uri: str, s3_prefix: str, work_dir: Path
) -> AnalysisRequest:
    path = work_dir / "request.json"
    client.download(request_uri, path)
    return AnalysisRequest.parse(json.loads(path.read_text()), s3_prefix)


def validate_analysis_manifest(manifest: dict, request: AnalysisRequest) -> None:
    if set(manifest) != {request.protocol}:
        raise ValueError("analysis manifest must contain exactly the requested protocol")
    section = manifest[request.protocol]
    if not isinstance(section, dict):
        raise ValueError("invalid protocol section in analysis manifest")
    if section.get("zmap") != request.job_id:
        raise ValueError("manifest zmap id must equal the analysis job id")
    os_id = section.get("os")
    if not isinstance(os_id, str) or not JOB_ID_RE.fullmatch(os_id):
        raise ValueError("manifest is missing a valid OS measurement id")

    if section.get("connection_target") != (
        CONNECTION_TARGET_NAME if request.connection_target_uri is not None else None
    ):
        raise ValueError("manifest connection target does not match request")
    measurements = iter_ipid_measurements(manifest)
    if not measurements:
        raise ValueError("analysis manifest contains no IPID measurements")
    for measurement_id in [os_id, *(m.measurement_id for m in measurements)]:
        if not JOB_ID_RE.fullmatch(measurement_id):
            raise ValueError(f"invalid measurement id {measurement_id!r}")
        if measurement_id == "shared-os" and SCOPE:
            continue
        if measurement_protocol(measurement_id) != request.protocol:
            raise ValueError(f"measurement id {measurement_id!r} has the wrong protocol")
    for specification in iter_random_reproducibility(manifest):
        for measurement_id in specification.repeat_ids:
            if not JOB_ID_RE.fullmatch(measurement_id):
                raise ValueError(f"invalid reproducibility measurement id {measurement_id!r}")
            if measurement_protocol(measurement_id) != request.protocol:
                raise ValueError(
                    f"reproducibility measurement id {measurement_id!r} has the wrong protocol"
                )


def download_analysis_inputs(
    client: S3Client,
    request: AnalysisRequest,
    manifest: dict,
    raw_root: Path = RAW_DATA_DIR,
) -> None:
    section = manifest[request.protocol]
    zmap_id = section["zmap"]
    os_id = section["os"]
    client.download(
        join_s3(request.zmap_prefix, zmap_id, "zmap.pq"),
        raw_root / "zmap" / zmap_id / "zmap.pq",
    )
    if request.connection_target_uri is not None:
        client.download(
            request.connection_target_uri,
            raw_root / "zmap" / zmap_id / CONNECTION_TARGET_NAME,
        )
        client.download(
            join_s3(request.zmap_prefix, zmap_id, CONNECTION_TARGET_METADATA_NAME),
            raw_root / "zmap" / zmap_id / CONNECTION_TARGET_METADATA_NAME,
        )
    if request.fixed_base_target_uri is not None:
        client.download(
            request.fixed_base_target_uri,
            raw_root / "zmap" / zmap_id / FIXED_BASE_TARGET_NAME,
        )
        client.download(
            join_s3(
                request.zmap_prefix,
                zmap_id,
                FIXED_BASE_TARGET_METADATA_NAME,
            ),
            raw_root / "zmap" / zmap_id / FIXED_BASE_TARGET_METADATA_NAME,
        )
    if os_id == "shared-os" and SCOPE:
        for name in ("os.pq", "os-coverage.json"):
            path = os_source_path(raw_root, os_id, name)
            if not path.is_file():
                raise FileNotFoundError(path)
    else:
        for name in ("os.pq", "os-coverage.json"):
            client.download(join_s3(request.os_prefix, os_id, name), raw_root / "os" / os_id / name)

    measurements = iter_ipid_measurements(manifest)
    for measurement in measurements:
        remote = join_s3(request.ipid_prefix, measurement.measurement_id)
        local = raw_root / "ipid" / measurement.measurement_id
        client.download(join_s3(remote, "ipid.pq"), local / "ipid.pq")
        client.download(join_s3(remote, "ipid.snapshot.yaml"), local / "ipid.snapshot.yaml")
        strategies_uri = join_s3(remote, "strategies.pq")
        if client.exists(strategies_uri):
            client.download(strategies_uri, local / "strategies.pq")

    rt_base = next(
        (
            measurement
            for measurement in measurements
            if (
                measurement.connection_mode,
                measurement.interval,
                measurement.scale,
            )
            == ("no-connection", "rt-based", "base")
        ),
        None,
    )
    if rt_base is None:
        raise ValueError("analysis manifest is missing the stateless RT-based base measurement")
    remote = join_s3(request.ipid_prefix, rt_base.measurement_id, TARGET_NAME)
    client.download(
        remote,
        raw_root / "ipid" / rt_base.measurement_id / TARGET_NAME,
    )

    for specification in iter_random_reproducibility(manifest):
        baseline_remote = join_s3(
            request.ipid_prefix,
            specification.baseline_measurement_id,
        )
        baseline_local = raw_root / "ipid" / specification.baseline_measurement_id
        for file_name in (
            specification.target_file,
            specification.cohort_file,
            specification.prepare_metadata_file,
        ):
            client.download(
                join_s3(baseline_remote, file_name),
                baseline_local / file_name,
            )
        for repeat_id in specification.repeat_ids:
            repeat_remote = join_s3(request.ipid_prefix, repeat_id)
            repeat_local = raw_root / "ipid" / repeat_id
            client.download(join_s3(repeat_remote, "ipid.pq"), repeat_local / "ipid.pq")
            client.download(
                join_s3(repeat_remote, "ipid.snapshot.yaml"),
                repeat_local / "ipid.snapshot.yaml",
            )


def run_postprocess(manifest_path: Path, log_path: Path, batch_size: int, threads: int) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log_file:
        subprocess.run(
            [
                sys.executable,
                str(PROJ_ROOT / "ipid_analysis" / "postprocess.py"),
                str(manifest_path),
                "--batch-size",
                str(batch_size),
                "--threads",
                str(threads),
                "--strict",
                "--status-file",
                str(log_path.with_name("postprocess-status.json")),
            ],
            cwd=PROJ_ROOT,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            check=True,
            timeout=int(os.environ.get("IPID_ANALYSIS_POSTPROCESS_TIMEOUT", "172800")),
        )


def process_analysis_request(
    client: S3Client,
    request_uri: str,
    s3_prefix: str,
    work_root: Path,
    batch_size: int,
    threads: int,
    postprocess: Callable[[Path, Path, int, int], None] = run_postprocess,
    raw_root: Path = RAW_DATA_DIR,
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = FIGURES_DIR,
) -> bool:
    job_id = request_uri.rstrip("/").split("/")[-2]
    if not JOB_ID_RE.fullmatch(job_id):
        raise ValueError(f"invalid analysis job id in request URI: {request_uri}")
    work_dir = work_root / job_id
    job_prefix = join_s3(s3_prefix, "analysis-jobs", job_id)
    done_uri = join_s3(job_prefix, "done.json")
    failed_uri = join_s3(job_prefix, "failed.json")
    log_uri = join_s3(job_prefix, "postprocess.log")

    if client.exists(done_uri) or client.exists(failed_uri):
        return False

    request = None
    log_path = work_dir / "postprocess.log"
    try:
        request = load_analysis_request(client, request_uri, s3_prefix, work_dir)
        if request.job_id != job_id:
            raise ValueError("analysis request job_id does not match request URI")
        manifest_path = work_dir / "manifest.json"
        client.download(request.manifest_uri, manifest_path)
        manifest = load_manifest(manifest_path)
        validate_analysis_manifest(manifest, request)
        download_analysis_inputs(client, request, manifest, raw_root)
        postprocess(manifest_path, log_path, batch_size, threads)
        status_path = log_path.with_name("postprocess-status.json")
        status = json.loads(status_path.read_text())
        if (
            status.get("version") != 1
            or status.get("required_complete") is not True
            or status.get("manifest") != fingerprints([manifest_path])
        ):
            raise ValueError("postprocessing did not complete its mandatory stages")
        from ipid_analysis.postprocess import required_core_artifacts

        required = {
            path.resolve()
            for path in required_core_artifacts(
                manifest, processed_root=processed_root, figures_root=figures_root
            )
        }
        declared = {Path(name).resolve() for name in status["artifacts"]}
        if not required or not required.issubset(declared):
            raise ValueError(
                "postprocessing omitted required artifacts from its completion report"
            )
        for name in status["artifacts"]:
            artifact = Path(name)
            if not artifact.is_file():
                raise FileNotFoundError(artifact)
            assert_complete(artifact)
            if not completion_path(artifact).is_file():
                raise ValueError(f"postprocessing did not commit artifact: {artifact}")
        client.upload(status_path, join_s3(job_prefix, "postprocess-status.json"))
        if log_path.is_file():
            client.upload(log_path, log_uri)

        done = AnalysisDone(
            version=ANALYSIS_JOB_VERSION,
            job_id=request.job_id,
            manifest_uri=request.manifest_uri,
            log_uri=log_uri,
            completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        )
        done_path = work_dir / "done.json"
        write_json(done_path, asdict(done))
        client.upload(done_path, request.done_uri)
        logger.success(f"[{request.job_id}] automatic postprocessing completed")
        cleanup_completed_job(work_dir)
        return True
    except Exception as exc:
        cleanup_current_artifacts()
        discard_failed_bulk(work_dir)
        failed_job_id = request.job_id if request is not None else job_id
        if log_path.is_file():
            try:
                client.upload(log_path, log_uri)
            except Exception:
                logger.exception(f"[{failed_job_id}] could not upload postprocessing log")
        failed_path = work_dir / "failed.json"
        write_json(
            failed_path,
            {
                "version": ANALYSIS_JOB_VERSION,
                "job_id": failed_job_id,
                "error": str(exc),
                "log_uri": log_uri,
            },
        )
        try:
            client.upload(failed_path, failed_uri)
            (work_dir / ".failure-published").write_text(failed_uri, encoding="utf-8")
        except Exception:
            logger.exception(f"[{failed_job_id}] could not upload analysis failure marker")
        raise


def run_analysis_pending(
    client: S3Client,
    s3_prefix: str,
    work_root: Path,
    batch_size: int,
    threads: int,
) -> int:
    processed = 0
    for request_uri in client.list_analysis_requests(s3_prefix):
        try:
            processed += int(
                process_analysis_request(
                    client,
                    request_uri,
                    s3_prefix,
                    work_root,
                    batch_size,
                    threads,
                )
            )
        except Exception:
            logger.exception(f"failed processing {request_uri}")
    return processed


def process_interprotocol_target_request(
    client: S3Client,
    request_uri: str,
    s3_prefix: str,
    work_root: Path,
    output_root: Path = PROCESSED_DATA_DIR / "interprotocol",
    processed_root: Path = PROCESSED_DATA_DIR,
    figures_root: Path = INTERPROTOCOL_FIGURES_DIR,
) -> bool:
    job_id = request_uri.rstrip("/").split("/")[-2]
    if not JOB_ID_RE.fullmatch(job_id):
        raise ValueError(f"invalid inter-protocol target job id: {request_uri}")
    work_dir = work_root / job_id
    job_prefix = join_s3(s3_prefix, "interprotocol-target-jobs", job_id)
    done_uri = join_s3(job_prefix, "done.json")
    failed_uri = join_s3(job_prefix, "failed.json")
    if client.exists(done_uri) or client.exists(failed_uri):
        return False
    request = None
    try:
        request_path = work_dir / "request.json"
        client.download(request_uri, request_path)
        request = InterprotocolTargetRequest.parse(json.loads(request_path.read_text()), s3_prefix)
        if request.job_id != job_id:
            raise ValueError("inter-protocol target request job_id does not match request URI")
        manifests = {}
        for protocol, uri in request.manifests.items():
            path = work_dir / "manifests" / f"{protocol}.json"
            client.download(uri, path)
            if SCOPE:
                from ipid_analysis.continental import validate_strategy_ready
                validate_strategy_ready(client,uri,json.loads(path.read_text()),processed_root,work_dir)
            manifests[protocol] = str(path)
        campaign_path = work_dir / "campaign.json"
        write_json(
            campaign_path,
            {"campaign_id": request.campaign_id, "manifests": manifests},
        )
        with publication_inventory() as published:
            output_dir = build_target_files(
                campaign_path,
                processed_root=processed_root,
                output_root=output_root,
                figures_root=figures_root,
            )

        figure_dir = figures_root / request.campaign_id
        inventory = []
        for path in dict.fromkeys(published):
            if output_dir in path.parents:
                uri = join_s3(request.target_prefix, path.relative_to(output_dir).as_posix())
            elif figure_dir in path.parents:
                uri = join_s3(
                    job_prefix, "reports", "figures", path.relative_to(figure_dir).as_posix()
                )
            else:
                continue
            assert_complete(path)
            client.upload(path, uri)
            info = json.loads(completion_path(path).read_text())
            inventory.append(
                {"uri": uri, "sha256": info["sha256"], "size_bytes": info["size_bytes"]}
            )
        if not inventory:
            raise ValueError("target producer published no artifacts")
        inventory_path = work_dir / "artifacts.json"
        write_json(inventory_path, {"version": 1, "artifacts": inventory})
        client.upload(inventory_path, join_s3(job_prefix, "artifacts.json"))
        summary = json.loads((output_dir / "target-summary.json").read_text())
        rows = {
            group: sum(int(value) for value in counts.values())
            for group, counts in summary["targets"].items()
        }
        done = InterprotocolTargetDone(
            version=INTERPROTOCOL_TARGET_JOB_VERSION,
            job_id=request.job_id,
            campaign_id=request.campaign_id,
            target_prefix=request.target_prefix,
            rows=rows,
            completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        )
        done_path = work_dir / "done.json"
        write_json(done_path, asdict(done))
        client.upload(done_path, request.done_uri)
        logger.success(f"[{request.job_id}] inter-protocol targets completed")
        cleanup_completed_job(work_dir)
        return True
    except Exception as exc:
        cleanup_current_artifacts()
        discard_failed_bulk(work_dir)
        failed_path = work_dir / "failed.json"
        write_json(
            failed_path,
            {
                "version": INTERPROTOCOL_TARGET_JOB_VERSION,
                "job_id": request.job_id if request is not None else job_id,
                "error": str(exc),
            },
        )
        try:
            client.upload(failed_path, failed_uri)
            (work_dir / ".failure-published").write_text(failed_uri, encoding="utf-8")
        except Exception:
            logger.exception(f"[{job_id}] could not upload target-build failure marker")
        raise


def run_interprotocol_targets_pending(
    client: S3Client,
    s3_prefix: str,
    work_root: Path,
) -> int:
    processed = 0
    for request_uri in client.list_interprotocol_target_requests(s3_prefix):
        try:
            processed += int(
                process_interprotocol_target_request(client, request_uri, s3_prefix, work_root)
            )
        except Exception:
            logger.exception(f"failed processing {request_uri}")
    return processed


def load_interprotocol_request(
    client: S3Client, request_uri: str, s3_prefix: str, work_dir: Path
) -> InterprotocolRequest:
    path = work_dir / "request.json"
    client.download(request_uri, path)
    return InterprotocolRequest.parse(json.loads(path.read_text()), s3_prefix)


def process_interprotocol_request(
    client: S3Client,
    request_uri: str,
    s3_prefix: str,
    work_root: Path,
    raw_root: Path = RAW_DATA_DIR / "ipid",
    output_root: Path = PROCESSED_DATA_DIR / "interprotocol",
    figures_root: Path = INTERPROTOCOL_FIGURES_DIR,
) -> bool:
    job_id = request_uri.rstrip("/").split("/")[-2]
    if not JOB_ID_RE.fullmatch(job_id):
        raise ValueError(f"invalid inter-protocol job id in request URI: {request_uri}")
    work_dir = work_root / job_id
    job_prefix = join_s3(s3_prefix, "interprotocol-jobs", job_id)
    done_uri = join_s3(job_prefix, "done.json")
    failed_uri = join_s3(job_prefix, "failed.json")
    if client.exists(done_uri) or client.exists(failed_uri):
        return False

    request = None
    try:
        request = load_interprotocol_request(client, request_uri, s3_prefix, work_dir)
        if request.job_id != job_id:
            raise ValueError("inter-protocol request job_id does not match request URI")
        manifest_path = work_dir / "manifest.json"
        client.download(request.manifest_uri, manifest_path)
        manifest = load_run_manifest(manifest_path)
        if manifest["run_id"] != request.job_id:
            raise ValueError("inter-protocol manifest run_id does not match job_id")
        if manifest["campaign_id"] != request.campaign_id:
            raise ValueError("inter-protocol manifest campaign_id does not match request")
        from ipid_analysis.interprotocol import measurement_items

        for specification in manifest["groups"].values():
            for sample, measurement_id in measurement_items(specification):
                remote = join_s3(request.ipid_prefix, measurement_id)
                local = raw_root / measurement_id
                client.download(join_s3(remote, "interprotocol.pq"), local / "interprotocol.pq")
                client.download(
                    join_s3(remote, "interprotocol.snapshot.yaml"),
                    local / "interprotocol.snapshot.yaml",
                )
        with publication_inventory() as published:
            output_dir = classify_campaign(
                manifest_path,
                raw_root=raw_root,
                output_root=output_root,
                figures_root=figures_root,
            )

        figure_dir = figures_root / manifest["campaign_id"] / "runs" / manifest["run_id"]
        inventory = []
        for path in dict.fromkeys(published):
            if output_dir in path.parents:
                uri = join_s3(request.result_prefix, path.relative_to(output_dir).as_posix())
            elif figure_dir in path.parents:
                uri = join_s3(
                    request.result_prefix,
                    "reports",
                    "figures",
                    path.relative_to(figure_dir).as_posix(),
                )
            else:
                continue
            assert_complete(path)
            client.upload(path, uri)
            info = json.loads(completion_path(path).read_text())
            inventory.append(
                {"uri": uri, "sha256": info["sha256"], "size_bytes": info["size_bytes"]}
            )
        if not inventory:
            raise ValueError("interprotocol producer published no artifacts")
        inventory_path = work_dir / "artifacts.json"
        write_json(inventory_path, {"version": 1, "artifacts": inventory})
        client.upload(inventory_path, join_s3(request.result_prefix, "artifacts.json"))
        summary = json.loads((output_dir / "interprotocol-campaign-summary.json").read_text())
        done = InterprotocolDone(
            version=INTERPROTOCOL_JOB_VERSION,
            job_id=request.job_id,
            campaign_id=request.campaign_id,
            result_prefix=request.result_prefix,
            rows=int(summary["rows"]),
            completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        )
        done_path = work_dir / "done.json"
        write_json(done_path, asdict(done))
        client.upload(done_path, request.done_uri)
        logger.success(f"[{request.job_id}] automatic inter-protocol analysis completed")
        cleanup_completed_job(work_dir)
        return True
    except Exception as exc:
        cleanup_current_artifacts()
        discard_failed_bulk(work_dir)
        failed_job_id = request.job_id if request is not None else job_id
        failed_path = work_dir / "failed.json"
        write_json(
            failed_path,
            {
                "version": INTERPROTOCOL_JOB_VERSION,
                "job_id": failed_job_id,
                "error": str(exc),
            },
        )
        try:
            client.upload(failed_path, failed_uri)
            (work_dir / ".failure-published").write_text(failed_uri, encoding="utf-8")
        except Exception:
            logger.exception(f"[{failed_job_id}] could not upload inter-protocol failure marker")
        raise


def run_interprotocol_pending(
    client: S3Client,
    s3_prefix: str,
    work_root: Path,
) -> int:
    processed = 0
    for request_uri in client.list_interprotocol_requests(s3_prefix):
        try:
            processed += int(
                process_interprotocol_request(client, request_uri, s3_prefix, work_root)
            )
        except Exception:
            logger.exception(f"failed processing {request_uri}")
    return processed


def retry_failed_request(client: S3Client, request_uri: str, s3_prefix: str) -> None:
    """Explicit recovery only: archive the terminal failure before removing it."""
    from ipid_analysis.continental import campaign_parts
    continental = campaign_parts(request_uri, s3_prefix)
    if continental is not None:
        campaign, site, remainder = continental
        campaign_prefix = join_s3(s3_prefix,"continental",campaign)
        if site is not None and remainder[:1] != ["final"]:
            return retry_failed_request(client, request_uri, join_s3(campaign_prefix,"sites",site))
        if (site is None and remainder in (["os","request.json"],["union","request.json"])) or (site is not None and remainder == ["final","request.json"]):
            from ipid_analysis.continental import descriptor, read_json
            root = work_path(request_uri, category="retry")
            descriptor(client, campaign_prefix, root)
            job = request_uri.rsplit("/",1)[0]
            if client.exists(job+"/done.json") or not client.exists(job+"/failed.json"):
                raise ValueError("retry requires a terminal failure without success")
            if remainder != ["union","request.json"]:
                read_json(client, request_uri, root)
            failure = root/"failed.json"
            client.download(job+"/failed.json", failure)
            client.upload(failure, job+"/attempts/"+str(time.time_ns())+"/failed.json")
            client.delete_failure(job+"/failed.json")
            cleanup_work(root)
            return
    prefix = s3_prefix.rstrip("/") + "/"
    if not request_uri.startswith(prefix):
        raise ValueError("retry request is outside workflow prefix")
    parts = request_uri[len(prefix) :].split("/")
    parsers = {
        "jobs": Request,
        "analysis-jobs": AnalysisRequest,
        "strategy-jobs": AnalysisRequest,
        "interprotocol-jobs": InterprotocolRequest,
        "interprotocol-target-jobs": InterprotocolTargetRequest,
    }
    if (
        len(parts) != 3
        or parts[0] not in parsers
        or parts[2] != "request.json"
        or not JOB_ID_RE.fullmatch(parts[1])
    ):
        raise ValueError("retry needs a canonical request URI")
    job_prefix = request_uri.rsplit("/", 1)[0]
    if client.exists(job_prefix + "/done.json"):
        raise ValueError("refusing to retry an already completed job")
    if not client.exists(job_prefix + "/failed.json"):
        raise ValueError("job has no terminal failure to retry")
    root = work_path(request_uri, category="retry")
    request_path = root / "request.json"
    client.download(request_uri, request_path)
    request = parsers[parts[0]].parse(json.loads(request_path.read_text()), s3_prefix)
    if request.job_id != parts[1]:
        raise ValueError("retry request job_id mismatch")
    failure = root / "failed.json"
    client.download(job_prefix + "/failed.json", failure)
    attempt = str(time.time_ns())
    client.upload(failure, job_prefix + "/attempts/" + attempt + "/failed.json")
    client.delete_failure(job_prefix + "/failed.json")
    cleanup_work(root)


@app.command()
def main(
    s3_prefix: str = typer.Option(
        ..., envvar="IPID_ANALYSIS_S3_PREFIX", help="shared S3 workflow prefix"
    ),
    work_root: Path | None = typer.Option(
        None, help="local worker directory; defaults to tmp/workflow"
    ),
    poll_interval: int = typer.Option(30, min=1, help="poll interval in seconds"),
    batch_size: int = typer.Option(1_000_000, min=1),
    threads: int = typer.Option(0, min=0, help="DuckDB threads; 0 uses all cores"),
    once: bool = typer.Option(False, help="process pending jobs once and exit"),
    retry_request_uri: str | None = typer.Option(
        None, help="archive and requeue this failed request URI"
    ),
    scheduling: str = typer.Option("scheduled", help="scheduled (reserved critical/background lanes) or legacy sequential"),
    storage_mode: str = typer.Option(
        "mounted-s3",
        envvar="IPID_ANALYSIS_STORAGE_MODE",
        help="mounted-s3 production or explicit local compatibility mode",
    ),
) -> None:
    os.environ["IPID_ANALYSIS_STORAGE_MODE"] = storage_mode
    start_run()
    if not s3_prefix.startswith("s3://") or not s3_prefix.removeprefix("s3://").strip("/"):
        raise typer.BadParameter("s3_prefix must be a non-empty s3:// URI")
    # Propagate the resolved CLI/env prefix to the postprocessing subprocess.
    os.environ["IPID_ANALYSIS_S3_PREFIX"] = s3_prefix
    validate_storage()
    if mounted_storage():
        roots = json.loads(os.environ.get("IPID_ANALYSIS_S3_ROOTS", "{}"))
        if not isinstance(roots, dict) or not str(roots.get("raw", "")).startswith("s3://"):
            raise typer.BadParameter(
                "mounted-s3 requires an explicit raw URI in IPID_ANALYSIS_S3_ROOTS before accepting jobs"
            )
    work_root = work_root or scratch_root() / "workflow"
    if mounted_storage() and scratch_root() not in work_root.resolve().parents:
        raise ValueError("mounted-s3 worker work-root must be under local scratch")
    client = S3Client()
    with exclusive_lock("workflow-worker", timeout_seconds=0):
        if retry_request_uri is not None:
            retry_failed_request(client, retry_request_uri, s3_prefix)
        if scheduling == "scheduled":
            from ipid_analysis.scheduler import Scheduler
            Scheduler(client, s3_prefix, work_root, batch_size=batch_size, threads=threads).run(poll_interval=poll_interval, once=once)
            return
        if scheduling != "sequential":
            raise ValueError("scheduling must be scheduled or sequential")
        while True:
            listing_errors = []
            for handler, category, options in (
                (run_pending, "handoff", (batch_size, threads)),
                (run_analysis_pending, "analysis", (batch_size, threads)),
                (run_interprotocol_targets_pending, "interprotocol-targets", ()),
                (run_interprotocol_pending, "interprotocol", ()),
            ):
                try:
                    handler(client, s3_prefix, work_root / category, *options)
                except Exception as exc:
                    logger.exception(f"workflow listing/dispatch failed: {category}")
                    listing_errors.append(str(exc))
            if once:
                if listing_errors:
                    raise RuntimeError("workflow dispatch failed: " + "; ".join(listing_errors))
                return
            time.sleep(poll_interval)


if __name__ == "__main__":
    app()
