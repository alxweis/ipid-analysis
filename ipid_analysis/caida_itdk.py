"""Download, validate, and cache a CAIDA IPv4 ITDK interface data set.

Only the topology ``.ifaces.bz2`` file is required for the analyses in this
repository: it contains the interface address, optional alias-set node ID, and
the traceroute transit/destination flags.  Public releases are downloaded from
CAIDA; private releases can be imported from a local directory or file.
"""

from __future__ import annotations

import bz2
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil
from urllib.error import HTTPError
from urllib.request import urlopen

from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import typer

from ipid_analysis.artifacts import assert_complete, file_digest, publish_file
from ipid_analysis.config import RAW_STORAGE_DIR as RAW_DATA_DIR
from ipid_analysis.downloads import download as download_http
from ipid_analysis.runtime import connect, exclusive_lock, start_run, temporary_path, work_path

app = typer.Typer()

DEFAULT_RELEASE = "2025-08"
DEFAULT_TOPOLOGY = "midar-iff-snmp-tnt"
DEFAULT_BASE_URL = "https://publicdata.caida.org/datasets/topology/ark/ipv4/itdk"
ITDK_CACHE_VERSION = "1"
_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


@dataclass(frozen=True)
class ITDKDataset:
    release: str
    topology: str
    directory: Path
    ifaces_path: Path
    interfaces_path: Path
    metadata_path: Path
    readme_path: Path


def _safe_component(value: str, field: str) -> str:
    if not _COMPONENT_RE.fullmatch(value):
        raise ValueError(f"invalid ITDK {field}: {value!r}")
    return value


def dataset_paths(
    release: str,
    topology: str,
    *,
    raw_root: Path = RAW_DATA_DIR,
) -> ITDKDataset:
    release = _safe_component(release, "release")
    topology = _safe_component(topology, "topology")
    directory = Path(raw_root) / "caida-itdk" / release / topology
    return ITDKDataset(
        release=release,
        topology=topology,
        directory=directory,
        ifaces_path=directory / f"{topology}.ifaces.bz2",
        interfaces_path=directory / "interfaces.pq",
        metadata_path=directory / "source.json",
        readme_path=directory / "README.txt",
    )


@contextmanager
def _exclusive_lock(path: Path, timeout_seconds: float = 3600.0):
    with exclusive_lock(str(path), timeout_seconds):
        yield


def _download(url: str, destination: Path) -> Path:
    local = work_path(url) / destination.name
    result = download_http(url, local, opener=urlopen)
    publish_file(result, destination, parameters={"source_url": url})
    return result


def _copy_atomic(source: Path, destination: Path) -> None:
    source = source.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and source == destination.resolve():
        return
    partial = temporary_path(destination)
    with source.open("rb") as input_file, partial.open("wb") as output_file:
        shutil.copyfileobj(input_file, output_file, length=8 * 1024 * 1024)
    publish_file(partial, destination)
    partial.unlink(missing_ok=True)


def _sha256(path: Path) -> str:
    return file_digest(path)


def _parse_ipv4(value: str) -> tuple[bool, bool]:
    """Return ``(valid, placeholder)`` without expensive per-row objects."""
    parts = value.split(".")
    if len(parts) != 4:
        return False, False
    try:
        octets = [int(part) for part in parts]
    except ValueError:
        return False, False
    if any(not 0 <= octet <= 255 for octet in octets):
        return False, False
    return True, octets[0] >= 224


def parse_ifaces(
    source: Path,
    destination: Path,
    *,
    release: str,
    topology: str,
    source_sha256: str,
    batch_size: int = 250_000,
) -> dict[str, int]:
    """Stream an ITDK ``.ifaces.bz2`` file into a compact Parquet lookup."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = temporary_path(destination)
    partial.unlink(missing_ok=True)
    metadata = {
        b"itdk_cache_version": ITDK_CACHE_VERSION.encode(),
        b"itdk_release": release.encode(),
        b"itdk_topology": topology.encode(),
        b"source_sha256": source_sha256.encode(),
    }
    schema = pa.schema(
        [
            ("IP_ADDR", pa.string()),
            ("NODE_ID", pa.string()),
            ("T", pa.bool_()),
            ("D", pa.bool_()),
        ],
        metadata=metadata,
    )
    writer = pq.ParquetWriter(partial, schema, compression="zstd")
    columns: dict[str, list] = {"IP_ADDR": [], "NODE_ID": [], "T": [], "D": []}
    rows = 0
    skipped_invalid = 0
    skipped_placeholder = 0

    def flush() -> None:
        nonlocal columns
        if not columns["IP_ADDR"]:
            return
        writer.write_table(pa.table(columns, schema=schema))
        columns = {"IP_ADDR": [], "NODE_ID": [], "T": [], "D": []}

    try:
        with bz2.open(source, "rt", encoding="utf-8", errors="strict") as input_file:
            for line in input_file:
                tokens = line.split()
                if not tokens or tokens[0].startswith("#"):
                    continue
                address = tokens[0]
                valid, placeholder = _parse_ipv4(address)
                if not valid:
                    skipped_invalid += 1
                    continue
                if placeholder:
                    skipped_placeholder += 1
                    continue
                node_id = next((token for token in tokens[1:] if token.startswith("N")), None)
                columns["IP_ADDR"].append(address)
                columns["NODE_ID"].append(node_id)
                columns["T"].append("T" in tokens[1:])
                columns["D"].append("D" in tokens[1:])
                rows += 1
                if len(columns["IP_ADDR"]) >= batch_size:
                    flush()
        flush()
    except Exception:
        writer.close()
        partial.unlink(missing_ok=True)
        raise
    else:
        writer.close()

    if rows == 0:
        partial.unlink(missing_ok=True)
        raise ValueError(f"ITDK interface file contains no real IPv4 addresses: {source}")

    with connect() as con:
        try:
            total, distinct = con.execute(
                "SELECT count(*), count(DISTINCT IP_ADDR) FROM read_parquet($path)",
                {"path": str(partial)},
            ).fetchone()
        finally:
            con.close()
        if total != distinct:
            partial.unlink(missing_ok=True)
            raise ValueError(
                f"ITDK interface file contains duplicate IPv4 addresses: {total - distinct:,}"
            )
        publish_file(partial, destination, expected_rows=rows)
        partial.unlink(missing_ok=True)
        return {
            "interfaces": rows,
            "skipped_invalid": skipped_invalid,
            "skipped_placeholder": skipped_placeholder,
        }


def prepare_itdk(
    *,
    release: str = DEFAULT_RELEASE,
    topology: str = DEFAULT_TOPOLOGY,
    source_dir: Path | None = None,
    ifaces: Path | None = None,
    raw_root: Path = RAW_DATA_DIR,
    base_url: str = DEFAULT_BASE_URL,
    force: bool = False,
) -> ITDKDataset:
    """Prepare and return one reusable ITDK interface lookup."""
    if source_dir is not None and ifaces is not None:
        raise ValueError("provide either source_dir or ifaces, not both")
    dataset = dataset_paths(release, topology, raw_root=raw_root)
    dataset.directory.mkdir(parents=True, exist_ok=True)
    lock_path = dataset.directory / ".prepare.lock"
    with _exclusive_lock(lock_path):
        cached_valid = False
        if (
            not force
            and dataset.ifaces_path.is_file()
            and dataset.interfaces_path.is_file()
            and dataset.metadata_path.is_file()
        ):
            cached = json.loads(dataset.metadata_path.read_text())
            assert_complete(dataset.interfaces_path)
            source_path = (
                Path(ifaces)
                if ifaces is not None
                else Path(source_dir) / f"{topology}.ifaces.bz2"
                if source_dir is not None
                else None
            )
            valid = (
                cached.get("cache_version") == ITDK_CACHE_VERSION
                and cached.get("ifaces_size") == dataset.ifaces_path.stat().st_size
            )
            if source_path is not None:
                valid = valid and cached.get("ifaces_sha256") == _sha256(source_path)
            cached_valid = valid
            if cached_valid:
                logger.info(
                    f"reusing CAIDA ITDK {release}/{topology} cache: {dataset.interfaces_path}"
                )
                return dataset

        filename = f"{topology}.ifaces.bz2"
        source_kind: str
        source_reference: str
        if ifaces is not None:
            source_path = Path(ifaces)
            if not source_path.is_file():
                raise FileNotFoundError(source_path)
            _copy_atomic(source_path, dataset.ifaces_path)
            source_kind = "local-file"
            source_reference = str(source_path.resolve())
        elif source_dir is not None:
            directory = Path(source_dir)
            source_path = directory / filename
            if not source_path.is_file():
                raise FileNotFoundError(source_path)
            _copy_atomic(source_path, dataset.ifaces_path)
            local_readme = directory / "README.txt"
            if local_readme.is_file():
                _copy_atomic(local_readme, dataset.readme_path)
            source_kind = "local-directory"
            source_reference = str(directory.resolve())
        else:
            release_url = f"{base_url.rstrip('/')}/{release}"
            ifaces_url = f"{release_url}/{filename}"
            logger.info(f"downloading CAIDA ITDK interfaces: {ifaces_url}")
            _download(ifaces_url, dataset.ifaces_path)
            _download(f"{release_url}/README.txt", dataset.readme_path)
            source_kind = "public-url"
            source_reference = ifaces_url

        digest = _sha256(dataset.ifaces_path)
        stats = parse_ifaces(
            dataset.ifaces_path,
            dataset.interfaces_path,
            release=release,
            topology=topology,
            source_sha256=digest,
        )
        info = {
            "cache_version": ITDK_CACHE_VERSION,
            "release": release,
            "topology": topology,
            "source_kind": source_kind,
            "source": source_reference,
            "ifaces_file": dataset.ifaces_path.name,
            "ifaces_size": dataset.ifaces_path.stat().st_size,
            "ifaces_sha256": digest,
            **stats,
            "prepared_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        metadata_partial = temporary_path(dataset.metadata_path)
        metadata_partial.write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")
        publish_file(metadata_partial, dataset.metadata_path)
        metadata_partial.unlink(missing_ok=True)
        logger.success(
            f"prepared CAIDA ITDK {release}/{topology}: "
            f"{stats['interfaces']:,} real IPv4 interfaces"
        )
    return dataset


@app.command()
def main(
    release: str = typer.Option(
        DEFAULT_RELEASE,
        envvar="IPID_ANALYSIS_ITDK_RELEASE",
        help="pinned CAIDA ITDK release",
    ),
    topology: str = typer.Option(
        DEFAULT_TOPOLOGY,
        envvar="IPID_ANALYSIS_ITDK_TOPOLOGY",
        help="IPv4 topology prefix",
    ),
    source_dir: Path | None = typer.Option(
        None,
        envvar="IPID_ANALYSIS_ITDK_SOURCE_DIR",
        help="local release directory instead of public download",
    ),
    ifaces: Path | None = typer.Option(
        None,
        envvar="IPID_ANALYSIS_ITDK_IFACES",
        help="local .ifaces.bz2 file instead of public download",
    ),
    raw_root: Path = typer.Option(RAW_DATA_DIR, help="raw data root"),
    force: bool = typer.Option(False, help="replace and rebuild an existing cache"),
) -> None:
    start_run()
    try:
        dataset = prepare_itdk(
            release=release,
            topology=topology,
            source_dir=source_dir,
            ifaces=ifaces,
            raw_root=raw_root,
            force=force,
        )
    except (FileNotFoundError, HTTPError, OSError, TimeoutError, ValueError) as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1) from exc
    logger.success(f"ITDK lookup ready: {dataset.interfaces_path}")


if __name__ == "__main__":
    app()
