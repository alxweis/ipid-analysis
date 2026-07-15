"""Merge base and mass IPID strategy classifications.

The base result defines the output population. Classified base rows keep their
strategy. Base rows that remain UNCLASSIFIED are refined by the mass result; a
missing mass row means the intended follow-up probe failed and becomes
NOT_ENOUGH_SAMPLES.

Example::

    python ipid_analysis/strategy_merge.py \
        tcp.ipid.no-connection.rt-based.base \
        tcp.ipid.no-connection.fixed-interval.mass
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
from loguru import logger
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import typer

from ipid_analysis.config import PROCESSED_DATA_DIR
from ipid_analysis.manifest import (
    CONNECTION_ABBREVIATIONS,
    INTERVAL_ABBREVIATIONS,
    SCALE_ABBREVIATIONS,
    IpidMeasurement,
    load_manifest,
    resolve,
)
from ipid_analysis.strategies import (
    DEFAULT_MANIFEST,
    OUTPUT_SCHEMA,
    STRATEGY_DICT,
)

app = typer.Typer()

MERGE_SQL = """
SELECT
    b.IP_ADDR,
    CASE
        WHEN CAST(b.IPID_SELECTION_STRATEGY AS VARCHAR) <> 'UNCLASSIFIED'
            THEN CAST(b.IPID_SELECTION_STRATEGY AS VARCHAR)
        WHEN m.IP_ADDR IS NULL
            THEN 'NOT_ENOUGH_SAMPLES'
        ELSE CAST(m.IPID_SELECTION_STRATEGY AS VARCHAR)
    END AS strategy
FROM read_parquet($base) AS b
LEFT JOIN read_parquet($mass) AS m USING (IP_ADDR)
"""


@dataclass(frozen=True)
class StrategyMerge:
    base: IpidMeasurement
    mass: IpidMeasurement

    def __post_init__(self) -> None:
        if self.base.scale != "base" or self.mass.scale != "mass":
            raise ValueError("strategy merge requires a base target followed by a mass target")
        if self.base.protocol != self.mass.protocol:
            raise ValueError("base and mass targets must use the same protocol")
        if self.base.connection_mode != self.mass.connection_mode:
            raise ValueError("base and mass targets must use the same connection mode")
        if not self.base.zmap_id or self.base.zmap_id != self.mass.zmap_id:
            raise ValueError("base and mass targets must belong to the same zmap campaign")

    @property
    def protocol(self) -> str:
        return self.base.protocol

    @property
    def connection_mode(self) -> str:
        return self.base.connection_mode

    @property
    def zmap_id(self) -> str:
        assert self.base.zmap_id is not None
        return self.base.zmap_id

    @property
    def target(self) -> str:
        return f"{self.base.target}+{self.mass.target}"

    @property
    def stem(self) -> str:
        connection = CONNECTION_ABBREVIATIONS[self.connection_mode]
        base = (
            f"{INTERVAL_ABBREVIATIONS[self.base.interval]}-{SCALE_ABBREVIATIONS[self.base.scale]}"
        )
        mass = (
            f"{INTERVAL_ABBREVIATIONS[self.mass.interval]}-{SCALE_ABBREVIATIONS[self.mass.scale]}"
        )
        return f"{connection}-{base}_{mass}"

    @property
    def artifact_directory(self) -> Path:
        variants = f"{self.base.interval}-{self.base.scale}_{self.mass.interval}-{self.mass.scale}"
        return Path(self.connection_mode) / "merged" / variants

    def artifact_name(self, kind: str, ext: str = "pq") -> str:
        return f"{self.stem}_{kind}.{ext}"

    def artifact_path(self, root: Path, kind: str, ext: str = "pq") -> Path:
        return root / self.zmap_id / self.artifact_directory / self.artifact_name(kind, ext)


@dataclass(frozen=True)
class MergeStats:
    rows: int
    not_enough_samples: int


def resolve_strategy_merge(manifest: dict, base_target: str, mass_target: str) -> StrategyMerge:
    base = resolve(manifest, base_target)
    mass = resolve(manifest, mass_target)
    if base is None:
        raise ValueError(f"{base_target}: not present in manifest")
    if mass is None:
        raise ValueError(f"{mass_target}: not present in manifest")
    return StrategyMerge(base, mass)


def iter_strategy_merges(manifest: dict) -> list[StrategyMerge]:
    """Return the canonical RT-base + fixed-interval-mass pairs in a manifest."""
    merges = []
    for protocol in manifest:
        base_target = f"{protocol}.ipid.no-connection.rt-based.base"
        mass_target = f"{protocol}.ipid.no-connection.fixed-interval.mass"
        base = resolve(manifest, base_target)
        mass = resolve(manifest, mass_target)
        if base is not None and mass is not None:
            merges.append(StrategyMerge(base, mass))
    return merges


def merge_paths(
    base_path: Path,
    mass_path: Path,
    output_path: Path,
    *,
    batch_size: int = 1_000_000,
    compression: str | None = "zstd",
    threads: int = 0,
) -> MergeStats:
    """Stream the merge into a canonical dictionary-encoded strategies parquet."""
    for path in (base_path, mass_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".part")
    temporary.unlink(missing_ok=True)

    con = duckdb.connect(config={"threads": threads} if threads else {})
    reader = con.execute(
        MERGE_SQL,
        {"base": str(base_path), "mass": str(mass_path)},
    ).to_arrow_reader(batch_size)
    writer = pq.ParquetWriter(temporary, OUTPUT_SCHEMA, compression=compression)
    rows = 0
    not_enough = 0
    try:
        for batch in reader:
            names = batch.column("strategy").cast(pa.string())
            codes = pc.index_in(names, value_set=STRATEGY_DICT)
            if pc.any(pc.is_null(codes)).as_py():
                invalid = pc.unique(pc.filter(names, pc.is_null(codes))).to_pylist()
                raise ValueError(f"unknown IPID strategies in merge input: {invalid}")
            strategy = pa.DictionaryArray.from_arrays(codes.cast(pa.int8()), STRATEGY_DICT)
            ip_addr = batch.column("IP_ADDR").cast(pa.string())
            writer.write_batch(pa.record_batch([ip_addr, strategy], schema=OUTPUT_SCHEMA))
            rows += batch.num_rows
            not_enough += int(pc.sum(pc.equal(names, "NOT_ENOUGH_SAMPLES")).as_py() or 0)
    except Exception:
        writer.close()
        temporary.unlink(missing_ok=True)
        raise
    else:
        writer.close()
        temporary.replace(output_path)
    finally:
        con.close()

    return MergeStats(rows=rows, not_enough_samples=not_enough)


def merge_strategies(
    merge: StrategyMerge,
    *,
    processed_root: Path = PROCESSED_DATA_DIR,
    batch_size: int = 1_000_000,
    compression: str | None = "zstd",
    threads: int = 0,
) -> tuple[Path, MergeStats]:
    base_path = merge.base.artifact_path(processed_root, "strategies")
    mass_path = merge.mass.artifact_path(processed_root, "strategies")
    output_path = merge.artifact_path(processed_root, "strategies")
    stats = merge_paths(
        base_path,
        mass_path,
        output_path,
        batch_size=batch_size,
        compression=compression,
        threads=threads,
    )
    return output_path, stats


@app.command()
def main(
    base_target: str = typer.Argument(..., help="dotted base measurement target"),
    mass_target: str = typer.Argument(..., help="dotted mass measurement target"),
    manifest: Path = typer.Option(DEFAULT_MANIFEST, help="measurement manifest JSON"),
    batch_size: int = typer.Option(1_000_000, min=1, help="rows per output batch"),
    compression: str = typer.Option("zstd", help="zstd|snappy|gzip|lz4|none"),
    threads: int = typer.Option(0, min=0, help="DuckDB threads; 0 uses all cores"),
) -> None:
    try:
        merge = resolve_strategy_merge(load_manifest(manifest), base_target, mass_target)
        output, stats = merge_strategies(
            merge,
            batch_size=batch_size,
            compression=None if compression == "none" else compression,
            threads=threads,
        )
    except (FileNotFoundError, ValueError) as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1) from exc
    logger.success(
        f"[{merge.target}] {stats.rows:,} IPs, "
        f"{stats.not_enough_samples:,} not enough samples -> {output}"
    )


if __name__ == "__main__":
    app()
