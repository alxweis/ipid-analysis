# Input path: ipid/icmp_DD-MM-YYYY
# Build: zmap and os path
# Create IP->IPID_SELECTION_STRATEGY

from __future__ import annotations

import argparse
import math
import sys
import time
from collections.abc import Callable
from enum import Enum, auto
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

INPUT_COLUMNS = [
    "IP_ADDR",
    "IPID_SEQUENCE",
    "SEND_TIMESTAMP_SEQUENCE",
    "RECEIVE_TIMESTAMP_SEQUENCE",
]

OUTPUT_SCHEMA = pa.schema(
    [
        ("IP_ADDR", pa.string()),
        ("IPID_SELECTION_STRATEGY", pa.string()),
    ]
)

MIN_STEPS_BEFORE_WRAPAROUND = 3
MAX_INC = math.ceil(65536 / MIN_STEPS_BEFORE_WRAPAROUND) - 1

CONNECTION_COUNT = 4
REQUESTS_PER_CONNECTION = 4
SEQUENCE_LENGTH = CONNECTION_COUNT * REQUESTS_PER_CONNECTION
REQUEST_IP_IDS = [18933, 18932, 3717, 3718, 3719]

MULTI_MAX_INC = 800
MULTI_MAX_CLUSTERS = 16
RANDOM_MIN_CHI2_VALUE = 1e-9


class IPIDSubSequence:
    __slots__ = ("raw", "inc")

    def __init__(self, ipid_subseq: np.ndarray):
        self.raw = ipid_subseq
        self.inc = np.diff(ipid_subseq) % 65536

    def is_increasing(self, min_inc: int, max_inc: int) -> bool:
        return self.inc.size > 0 and bool(
            np.all((self.inc >= min_inc) & (self.inc <= max_inc))
        )


class IPIDSequence:
    __slots__ = ("all", "src1", "src2", "con")

    def __init__(self, ipid_seq: np.ndarray):
        if ipid_seq.shape != (SEQUENCE_LENGTH,):
            raise ValueError(f"expected {SEQUENCE_LENGTH} IPIDs, got {ipid_seq.shape}")
        if ipid_seq.dtype != np.uint16:
            ipid_seq = ipid_seq.astype(np.uint16)

        self.all = IPIDSubSequence(ipid_seq)

        self.src1 = IPIDSubSequence(ipid_seq[0::2])
        self.src2 = IPIDSubSequence(ipid_seq[1::2])

        per_connection = ipid_seq.reshape(REQUESTS_PER_CONNECTION, CONNECTION_COUNT).T
        self.con = [IPIDSubSequence(per_connection[i][1:]) for i in range(CONNECTION_COUNT)]


class IPIDStrategy(Enum):
    REFLECTION = auto()
    CONSTANT = auto()
    SINGLE = auto()
    PER_DESTINATION = auto()
    PER_CONNECTION = auto()
    PER_BUCKET = auto()
    MULTI = auto()
    RANDOM = auto()
    UNCLASSIFIED = auto()


def is_reflection(ipid_seq: IPIDSequence) -> bool:
    first_offset = (ipid_seq.all.raw[0] - REQUEST_IP_IDS[0]) % 65536

    for i, ip_id in enumerate(ipid_seq.all.raw):
        expected = (REQUEST_IP_IDS[i % len(REQUEST_IP_IDS)] + first_offset) % 65536
        if ip_id != expected:
            return False

    return True

def is_constant(ipid_seq: IPIDSequence) -> bool:
    return np.all(ipid_seq.all.inc == 0)

def is_per_destination(ipid_seq: IPIDSequence) -> bool:
    return (ipid_seq.src1.is_increasing(min_inc=1, max_inc=1) and
            ipid_seq.src2.is_increasing(min_inc=1, max_inc=1))

def is_per_connection(ipid_seq: IPIDSequence) -> bool:
    for con in ipid_seq.con:
        if not con.is_increasing(min_inc=1, max_inc=1):
            return False
    return True

def is_per_bucket(ipid_seq: IPIDSequence) -> bool:
    for con in ipid_seq.con:
        if not con.is_increasing(min_inc=1, max_inc=MAX_INC):
            return False
    return True

def is_single(ipid_seq: IPIDSequence) -> bool:
    return ipid_seq.all.is_increasing(min_inc=1, max_inc=MAX_INC)

def is_multi(ipid_seq: IPIDSequence) -> bool:
    clusters: list[dict[int, np.int32]] = get_clusters(ipid_seq.all.raw, max_diff=MULTI_MAX_INC)
    return 1 < len(clusters) <= MULTI_MAX_CLUSTERS

def is_random(ipid_seq: IPIDSequence) -> bool:
    chi2_values = [chi2_test(ipid_seq.all.inc), chi2_test(ipid_seq.src1.inc), chi2_test(ipid_seq.src2.inc)]
    for con in ipid_seq.con:
        chi2_values.append(con.inc)

    if min(chi2_values) < RANDOM_MIN_CHI2_VALUE:
        return False
    return True

def is_unclassified(ipid_seq: IPIDSequence) -> bool:
    return len([_ for predicate, _ in _CLASSIFIERS[:-1] if predicate(ipid_seq)]) == 0

_CLASSIFIERS: tuple[tuple[Callable[[np.ndarray], bool], IPIDStrategy], ...] = (
    (is_reflection, IPIDStrategy.REFLECTION),
    (is_constant, IPIDStrategy.CONSTANT),
    (is_per_destination, IPIDStrategy.PER_DESTINATION),
    (is_per_connection, IPIDStrategy.PER_CONNECTION),
    (is_single, IPIDStrategy.SINGLE),
    (is_per_bucket, IPIDStrategy.PER_BUCKET),
    (is_multi, IPIDStrategy.MULTI),
    (is_random, IPIDStrategy.RANDOM),
    (is_unclassified, IPIDStrategy.UNCLASSIFIED),
)


def classify(ipid_seq: np.ndarray) -> IPIDStrategy:
    for predicate, strategy in _CLASSIFIERS:
        if predicate(ipid_seq):
            return strategy
    raise NotImplementedError


def classify_all(ipid_seq: np.ndarray) -> list[IPIDStrategy]:
    matches = [s for predicate, s in _CLASSIFIERS if predicate(ipid_seq)]
    if not matches:
        raise NotImplementedError
    return matches


# --------------------------------------------------------------------------
def _parse_seq(raw: str | None) -> np.ndarray:
    """Comma-separierter String -> int64-Array. Leer/None -> leeres Array."""
    if not raw:
        return np.empty(0, dtype=np.int64)
    return np.fromstring(raw, sep=",", dtype=np.int64)


def process(
        input_path: Path,
        output_path: Path,
        batch_size: int,
        compression: str | None,
        log_every: int,
) -> None:
    reader = pq.ParquetFile(input_path)
    writer = pq.ParquetWriter(output_path, OUTPUT_SCHEMA, compression=compression)

    processed = 0
    last_log = 0
    start = time.monotonic()
    try:
        # iter_batches streamt Row-Group fuer Row-Group, ohne das ganze
        # File zu materialisieren.
        for batch in reader.iter_batches(batch_size=batch_size, columns=INPUT_COLUMNS):
            ip_addr = batch.column("IP_ADDR").to_pylist()
            ipid = batch.column("IPID_SEQUENCE").to_pylist()
            send = batch.column("SEND_TIMESTAMP_SEQUENCE").to_pylist()
            recv = batch.column("RECEIVE_TIMESTAMP_SEQUENCE").to_pylist()

            strategies = [
                classify(
                    ip_addr[i],
                    _parse_seq(ipid[i]),
                    _parse_seq(send[i]),
                    _parse_seq(recv[i]),
                )
                for i in range(len(ip_addr))
            ]

            writer.write_batch(
                pa.record_batch(
                    [
                        pa.array(ip_addr, type=pa.string()),
                        pa.array(strategies, type=pa.string()),
                    ],
                    schema=OUTPUT_SCHEMA,
                )
            )

            processed += len(ip_addr)
            if log_every and processed - last_log >= log_every:
                rate = processed / (time.monotonic() - start)
                print(f"{processed:,} IPs verarbeitet ({rate:,.0f}/s)", file=sys.stderr)
                last_log = processed
    finally:
        writer.close()

    elapsed = time.monotonic() - start
    print(f"Fertig: {processed:,} IPs in {elapsed:.1f}s -> {output_path}", file=sys.stderr)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("input", type=Path, help="Input-Parquet (z.B. ipid.pq)")
    p.add_argument("output", type=Path, help="Output-Parquet")
    p.add_argument(
        "--batch-size",
        type=int,
        default=50_000,
        help="Zeilen pro Batch; begrenzt den Speicherbedarf (default: 50000)",
    )
    p.add_argument(
        "--compression",
        default="zstd",
        choices=["zstd", "snappy", "gzip", "lz4", "none"],
        help="Output-Kompression (default: zstd)",
    )
    p.add_argument(
        "--log-every",
        type=int,
        default=500_000,
        help="Fortschritt alle N IPs auf stderr loggen (0 = aus)",
    )
    args = p.parse_args()

    process(
        args.input,
        args.output,
        batch_size=args.batch_size,
        compression=None if args.compression == "none" else args.compression,
        log_every=args.log_every,
    )


if __name__ == "__main__":
    main()
