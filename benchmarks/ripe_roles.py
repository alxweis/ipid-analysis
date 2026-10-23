"""Accumulator-only comparison; intentionally excludes I/O, JSON and BZip2.

Run as python -m benchmarks.ripe_roles; requires optional psutil.
"""

import argparse
from dataclasses import dataclass, field
import hashlib
import json
import multiprocessing
import struct
import threading
import time

import psutil

from ipid_analysis.ripe_bitmaps import RoleBitmaps


@dataclass
class PreviousStats:
    transit: bool = False
    destination: bool = False
    trace_count: int = 0
    probes: set = field(default_factory=set)
    measurements: set = field(default_factory=set)
    protocols: set = field(default_factory=set)
    first_seen: int | None = None
    last_seen: int | None = None


def run(mode, addresses, repeats, connection):
    process = psutil.Process()
    peak = [process.memory_info().rss]
    stop = threading.Event()

    def sample():
        while not stop.wait(0.01):
            peak[0] = max(peak[0], process.memory_info().rss)

    monitor = threading.Thread(target=sample)
    monitor.start()
    started = time.perf_counter()
    bitmap = RoleBitmaps() if mode == "bitmaps" else None
    previous = {}
    for repeat in range(repeats):
        for index in range(addresses):
            address = 0x08000000 + index
            t, d = index % 3 == 0, index % 5 == 0
            if bitmap is not None:
                bitmap.add(address, t, d)
            else:
                item = previous.setdefault(address, PreviousStats())
                item.transit |= t
                item.destination |= d
                item.trace_count += 1
                item.probes.add(repeat)
                item.measurements.add(repeat)
                item.protocols.add("ICMP")
                item.first_seen = (
                    repeat if item.first_seen is None else min(item.first_seen, repeat)
                )
                item.last_seen = repeat
    accumulation = time.perf_counter() - started
    checksum = hashlib.sha256()
    if bitmap is not None:
        for table in bitmap.batches():
            for row in table.to_pylist():
                checksum.update(struct.pack("!I??", row["IP_V4"], row["T"], row["D"]))
        bitmap.close()
    else:
        for ip, item in sorted(previous.items()):
            checksum.update(struct.pack("!I??", ip, item.transit, item.destination))
    stop.set()
    monitor.join()
    connection.send(
        {
            "mode": mode,
            "addresses": addresses,
            "observations": addresses * repeats,
            "accumulation_seconds": accumulation,
            "total_seconds": time.perf_counter() - started,
            "sampled_peak_rss_mib": peak[0] / 1024**2,
            "roles_sha256": checksum.hexdigest(),
        }
    )
    connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--addresses", type=int, default=200000)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if not 1 <= args.addresses <= 2**24 or args.repeats < 1:
        parser.error("addresses must be 1..16777216 and repeats positive")
    context = multiprocessing.get_context("spawn")
    results = []
    for mode in ("previous-metadata", "bitmaps"):
        reader, writer = context.Pipe(duplex=False)
        child = context.Process(target=run, args=(mode, args.addresses, args.repeats, writer))
        child.start()
        writer.close()
        results.append(reader.recv())
        child.join()
        if child.exitcode:
            raise RuntimeError(f"benchmark failed: {mode}")
    if results[0]["roles_sha256"] != results[1]["roles_sha256"]:
        raise AssertionError("role outputs differ")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
