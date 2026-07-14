"""S3-only worker for the measurement-to-analysis handoff.

The worker polls for ``jobs/<id>/request.json`` objects. For each request it
downloads the completed stateless TCP RT measurement, runs the normal IPID
strategy classifier, uploads a ZMap-compatible UNCLASSIFIED target parquet, and
only then publishes ``done.json``. A measurement VM can therefore treat the done
marker as an atomic readiness signal.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import time
from typing import Callable

import duckdb
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import typer

from ipid_analysis.config import INTERIM_DATA_DIR
from ipid_analysis.strategies import classify_paths

PROTOCOL_VERSION = 1
JOB_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
TARGET_NAME = "zmap_unclassified.pq"

app = typer.Typer()


def join_s3(prefix: str, *parts: str) -> str:
    value = prefix.rstrip("/")
    for part in parts:
        value += "/" + part.strip("/")
    return value


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

    @classmethod
    def parse(cls, data: dict, s3_prefix: str) -> "Request":
        try:
            request = cls(**data)
        except TypeError as exc:
            raise ValueError(f"invalid request fields: {exc}") from exc
        if request.version != PROTOCOL_VERSION:
            raise ValueError(f"unsupported request version {request.version}")
        if not JOB_ID_RE.fullmatch(request.job_id) or request.job_id != request.measurement_id:
            raise ValueError("invalid job_id or measurement_id")
        if request.protocol != "tcp":
            raise ValueError(f"unsupported protocol {request.protocol!r}")
        if not request.ipid_uri.startswith("s3://") or not request.ipid_uri.endswith("/ipid.pq"):
            raise ValueError("invalid ipid_uri")
        if not request.snapshot_uri.startswith("s3://") or not request.snapshot_uri.endswith(
            "/ipid.snapshot.yaml"
        ):
            raise ValueError("invalid snapshot_uri")

        job_prefix = join_s3(s3_prefix, "jobs", request.job_id)
        expected = {
            "result_uri": join_s3(job_prefix, TARGET_NAME),
            "done_uri": join_s3(job_prefix, "done.json"),
            "failed_uri": join_s3(job_prefix, "failed.json"),
        }
        for field, value in expected.items():
            if getattr(request, field) != value:
                raise ValueError(f"{field} does not match the configured workflow prefix")
        return request


@dataclass(frozen=True)
class Done:
    version: int
    job_id: str
    result_uri: str
    rows: int
    size_bytes: int
    sha256: str


class S3Client:
    def __init__(self, run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run):
        self._run = run

    def _command(self, *args: str) -> str:
        result = self._run(["s3cmd", *args], check=True, text=True, capture_output=True)
        return result.stdout

    def exists(self, uri: str) -> bool:
        output = self._command("ls", uri)
        return any(line.split() and line.split()[-1] == uri for line in output.splitlines())

    def list_requests(self, s3_prefix: str) -> list[str]:
        output = self._command("ls", "--recursive", join_s3(s3_prefix, "jobs"))
        uris = [line.split()[-1] for line in output.splitlines() if line.split()]
        return sorted(uri for uri in uris if uri.endswith("/request.json"))

    def download(self, uri: str, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".part")
        self._command("get", "--force", "--no-progress", uri, str(temporary))
        temporary.replace(path)

    def upload(self, path: Path, uri: str) -> None:
        self._command("put", "--no-progress", str(path), uri)


def build_unclassified_targets(strategies_path: Path, output_path: Path) -> int:
    """Stream UNCLASSIFIED addresses into the schema consumed by ipid-measure."""
    con = duckdb.connect()
    reader = con.execute(
        "SELECT CAST(IP_ADDR AS VARCHAR) AS IP_ADDR, "
        "       CAST('' AS VARCHAR) AS REPLY_TYPE "
        "FROM read_parquet($path) "
        "WHERE CAST(IPID_SELECTION_STRATEGY AS VARCHAR) = 'UNCLASSIFIED'",
        {"path": str(strategies_path)},
    ).to_arrow_reader(1_000_000)
    schema = pa.schema([("IP_ADDR", pa.string()), ("REPLY_TYPE", pa.string())])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(output_path, schema, compression="zstd")
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
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def cleanup_completed_job(work_dir: Path) -> None:
    try:
        shutil.rmtree(work_dir)
    except FileNotFoundError:
        pass
    except OSError as exc:
        logger.warning(f"could not remove completed job directory {work_dir}: {exc}")


def load_request(client: S3Client, request_uri: str, s3_prefix: str, work_dir: Path) -> Request:
    path = work_dir / "request.json"
    client.download(request_uri, path)
    return Request.parse(json.loads(path.read_text()), s3_prefix)


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

    try:
        request = load_request(client, request_uri, s3_prefix, work_dir)
        input_path = work_dir / "ipid.pq"
        snapshot_path = work_dir / "ipid.snapshot.yaml"
        strategies_path = work_dir / "strategies.pq"
        result_path = work_dir / TARGET_NAME

        client.download(request.ipid_uri, input_path)
        client.download(request.snapshot_uri, snapshot_path)
        classify_paths(
            input_path,
            snapshot_path,
            strategies_path,
            protocol=request.protocol,
            batch_size=batch_size,
            compression="zstd",
            threads=threads,
        )
        rows = build_unclassified_targets(strategies_path, result_path)
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
        logger.success(f"[{request.job_id}] {rows:,} unclassified targets uploaded")
        cleanup_completed_job(work_dir)
        return True
    except Exception as exc:
        failed_path = work_dir / "failed.json"
        write_json(
            failed_path,
            {"version": PROTOCOL_VERSION, "job_id": request.job_id, "error": str(exc)},
        )
        try:
            client.upload(failed_path, failed_uri)
        except Exception:
            logger.exception(f"[{request.job_id}] could not upload failure marker")
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


@app.command()
def main(
    s3_prefix: str = typer.Option(
        ..., envvar="IPID_ANALYSIS_S3_PREFIX", help="shared S3 workflow prefix"
    ),
    work_root: Path = typer.Option(INTERIM_DATA_DIR / "s3-workflow"),
    poll_interval: int = typer.Option(30, min=1, help="poll interval in seconds"),
    batch_size: int = typer.Option(1_000_000, min=1),
    threads: int = typer.Option(0, min=0, help="DuckDB threads; 0 uses all cores"),
    once: bool = typer.Option(False, help="process pending jobs once and exit"),
) -> None:
    if not s3_prefix.startswith("s3://") or not s3_prefix.removeprefix("s3://").strip("/"):
        raise typer.BadParameter("s3_prefix must be a non-empty s3:// URI")
    client = S3Client()
    while True:
        run_pending(client, s3_prefix, work_root, batch_size, threads)
        if once:
            return
        time.sleep(poll_interval)


if __name__ == "__main__":
    app()
