"""Bounded IPv4 role accumulation; one writer per bitmap set."""

from __future__ import annotations

from functools import lru_cache
import ipaddress
import mmap

import numpy as np
import pyarrow as pa

ROLE_SCHEMA = pa.schema([("IP_V4", pa.uint32()), ("T", pa.bool_()), ("D", pa.bool_())])


@lru_cache(maxsize=65536)
def ipv4(value: str) -> tuple[int, bool, bool] | None:
    """Return numeric address, public flag and usable predecessor flag."""
    try:
        address = ipaddress.IPv4Address(value)
    except ipaddress.AddressValueError:
        return None
    public = address.is_global and not address.is_multicast
    number = int(address)
    private = number >> 24 == 10 or number >> 20 == 0xAC1 or number >> 16 == 0xC0A8
    return number, public, public or private


def trace_roles(record: dict) -> dict[int, tuple[bool, bool]]:
    """CAIDA-style evidence at distinct TTLs, never list/reply ordering."""

    def parse(value):
        return ipv4(value) if isinstance(value, str) else None

    destination = parse(record.get("dst_addr"))
    source = parse(record.get("src_addr")) or parse(record.get("from"))
    hops: dict[int, list[tuple[int, bool, bool]]] = {}
    for hop in record.get("result", []):
        if not isinstance(hop, dict):
            continue
        ttl = hop.get("hop")
        if not isinstance(ttl, int) or isinstance(ttl, bool) or ttl < 1:
            continue
        for reply in hop.get("result", []):
            address = parse(reply.get("from")) if isinstance(reply, dict) else None
            if address is not None:
                hops.setdefault(ttl, []).append(address)
    last_public = max((ttl for ttl, rows in hops.items() if any(a[1] for a in rows)), default=0)
    predecessor = bool(source and source[2])
    observed: dict[int, tuple[bool, bool]] = {}
    for ttl, rows in sorted(hops.items()):
        for number, public, _ in rows:
            if not public:
                continue
            is_destination = bool(destination and number == destination[0])
            is_source = bool(source and number == source[0])
            transit = predecessor and ttl < last_public and not is_destination and not is_source
            old_t, old_d = observed.get(number, (False, False))
            observed[number] = (old_t or transit, old_d or is_destination)
        predecessor |= any(row[2] for row in rows)
    if destination and destination[1] and record.get("destination_ip_responded") is True:
        # This proves D, but supplies no TTL for a transit successor check.
        observed[destination[0]] = (False, True)
    return observed


class RoleBitmaps:
    """Three anonymous 512-MiB maps; physical pages are committed on demand."""

    def __init__(self):
        self.maps = []
        self.blocks: set[int] = set()
        try:
            for _ in range(3):
                self.maps.append(mmap.mmap(-1, 1 << 29))
        except BaseException:
            self.close()
            raise

    def add(self, address: int, transit: bool, destination: bool) -> None:
        byte, bit = address >> 3, 1 << (address & 7)
        self.blocks.add(byte >> 15)
        for bitmap, enabled in zip(self.maps, (True, transit, destination)):
            if enabled:
                bitmap[byte] = bitmap[byte] | bit

    def batches(self):
        # Visit only touched 256K-address blocks; at most 262144 output rows.
        for block in sorted(self.blocks):
            offset = block << 15
            seen = np.frombuffer(self.maps[0][offset : offset + (1 << 15)], dtype=np.uint8)
            indices = np.flatnonzero(np.unpackbits(seen, bitorder="little")).astype(np.uint32)
            addresses = indices + np.uint32(offset * 8)
            columns = [pa.array(addresses, type=pa.uint32())]
            for bitmap in self.maps[1:]:
                packed = np.frombuffer(bitmap[offset : offset + (1 << 15)], dtype=np.uint8)
                flags = ((packed[indices >> 3] >> (indices & 7)) & 1).astype(bool)
                columns.append(pa.array(flags))
            yield pa.Table.from_arrays(columns, schema=ROLE_SCHEMA)

    def close(self):
        for bitmap in self.maps:
            bitmap.close()
        self.maps.clear()
