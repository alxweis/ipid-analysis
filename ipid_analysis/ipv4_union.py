"""Exact, bounded-memory union of normalized IPv4 discovery partitions."""

from __future__ import annotations

import mmap

from loguru import logger
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SCHEMA = pa.schema([("IP_ADDR", pa.string()), ("REPLY_TYPE", pa.string())])
BATCH_ROWS = 1 << 18
BLOCK_BYTES = BATCH_ROWS // 8
OCTET = r"(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])"
ADDRESS = "^" + r"\.".join(f"(?P<o{i}>{OCTET})" for i in range(4)) + "$"


def addresses(values):
    """Vectorized strict parsing; never silently drop malformed discovery rows."""
    if not pa.types.is_string(values.type):
        raise ValueError("discovery IP_ADDR must contain canonical IPv4 strings")
    parsed = pc.extract_regex(values, ADDRESS)
    if parsed.null_count:
        raise ValueError("discovery IP_ADDR contains null or invalid IPv4 addresses")
    result = np.zeros(len(values), dtype=np.uint32)
    for i in range(4):
        octets = parsed.field(i).cast(pa.uint32()).to_numpy()
        result |= octets << np.uint32(24 - 8 * i)
    return result


def write_union(inputs, output):
    """Write the same two-column OS population without a global hash/sort spill.

    One anonymous 512-MiB bitmap covers all IPv4 addresses. Input and output
    batches contain at most 262144 rows, independent of input/union cardinality.
    Anonymous memory is charged as process RSS, never as a local spill file.
    The caller owns atomic publication and input provenance checks.
    """
    rows = 0
    with mmap.mmap(-1, 1 << 29) as bitmap:
        packed = np.frombuffer(bitmap, dtype=np.uint8)
        try:
            for path in inputs:
                source_rows = 0
                with pq.ParquetFile(path) as source:
                    for batch in source.iter_batches(
                        batch_size=BATCH_ROWS,
                        columns=["IP_ADDR", "REPLY_TYPE"],
                        use_threads=False,
                    ):
                        if batch.schema.names != SCHEMA.names:
                            raise ValueError("discovery partition is missing normalized columns")
                        replies = batch.column("REPLY_TYPE")
                        if replies.null_count or pc.any(pc.not_equal(replies, "")).as_py():
                            raise ValueError("discovery REPLY_TYPE must be empty")
                        numbers = addresses(batch.column("IP_ADDR"))
                        # Repeated byte indices require ufunc.at: ordinary indexed
                        # |= can lose distinct bits when several IPs share a byte.
                        masks = np.left_shift(np.uint8(1), (numbers & 7).astype(np.uint8))
                        np.bitwise_or.at(packed, numbers >> 3, masks)
                        source_rows += batch.num_rows
                logger.info(f"discovery union read: {path}; rows={source_rows}")
            with pq.ParquetWriter(
                output, SCHEMA, compression="zstd", use_dictionary=["REPLY_TYPE"]
            ) as writer:
                for offset in range(0, len(packed), BLOCK_BYTES):
                    block = packed[offset : offset + BLOCK_BYTES].copy()
                    if not block.any():
                        continue
                    indices = np.flatnonzero(np.unpackbits(block, bitorder="little"))
                    numbers = indices.astype(np.uint32) + np.uint32(offset * 8)
                    octets = [
                        pa.array((numbers >> shift) & 255, type=pa.uint32()).cast(pa.string())
                        for shift in (24, 16, 8, 0)
                    ]
                    strings = pc.binary_join_element_wise(*octets, ".")
                    table = pa.Table.from_arrays(
                        [strings, pa.repeat("", len(numbers))], schema=SCHEMA
                    )
                    writer.write_table(table, row_group_size=BATCH_ROWS)
                    rows += len(numbers)
                # Release the view before closing the anonymous mapping.
                del block
        finally:
            del packed
    if rows == 0:
        raise ValueError("empty discovery union")
    logger.info(f"discovery union complete: rows={rows}; output={output}")
    return rows
