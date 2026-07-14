"""Parse the measurement manifest JSON and resolve dotted targets.

Structure (per protocol icmp/tcp/udp)::

    {proto}.zmap                              = <measurement_id>   # attempted IPs
    {proto}.os                                = <measurement_id>
    {proto}.ipid.{nec,ec}.{rt,fi}.{base,mass} = <measurement_id>   # ipid runs

A dotted target like ``tcp.ipid.nec.rt.base`` selects one ipid run. Every id is
a measurement directory name (e.g. ``tcp-80_2026-07-14_02-16-24``) under
``data/raw/<category>/<id>/`` where category is the top-level key (ipid/zmap/os).
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

CONN_MODES = ("nec", "ec")
INTERVALS = ("rt", "fi")
SCALES = ("base", "mass")


@dataclass(frozen=True)
class IpidMeasurement:
    protocol: str  # icmp | tcp | udp
    conn_mode: str  # nec (no established conn) | ec (established conn)
    interval: str  # rt (rt-based) | fi (fixed-interval)
    scale: str  # base | mass
    measurement_id: str  # e.g. tcp-80_2026-07-14_02-16-24
    zmap_id: str | None  # the protocol's zmap run (output dir + coverage)

    @property
    def target(self) -> str:
        """Dotted manifest path, e.g. 'tcp.ipid.nec.rt.base'."""
        return f"{self.protocol}.ipid.{self.conn_mode}.{self.interval}.{self.scale}"

    @property
    def input_key(self) -> str:
        """Key relative to data/raw, e.g. 'ipid/tcp-80_...'."""
        return f"ipid/{self.measurement_id}"

    @property
    def stem(self) -> str:
        """Output filename stem, e.g. 'tcp-ipid-nec-rt-base'."""
        return f"{self.protocol}-ipid-{self.conn_mode}-{self.interval}-{self.scale}"

    def output_name(self, kind: str) -> str:
        """e.g. output_name('strategies') -> 'tcp-ipid-nec-rt-base_strategies.pq'."""
        return f"{self.stem}_{kind}.pq"


def load_manifest(path: Path) -> dict:
    return json.loads(Path(path).read_text())


def resolve(manifest: dict, target: str) -> IpidMeasurement | None:
    """Resolve a dotted target (e.g. 'tcp.ipid.nec.rt.base') against the manifest.
    Returns None if the combination is absent."""
    parts = target.split(".")
    if len(parts) != 5 or parts[1] != "ipid":
        raise ValueError(f"expected <proto>.ipid.<nec|ec>.<rt|fi>.<base|mass>, got {target!r}")
    protocol, _, conn_mode, interval, scale = parts

    section = manifest.get(protocol)
    if not isinstance(section, dict):
        return None
    try:
        measurement_id = section["ipid"][conn_mode][interval][scale]
    except (KeyError, TypeError):
        return None
    return IpidMeasurement(
        protocol, conn_mode, interval, scale, measurement_id, section.get("zmap")
    )


def iter_ipid_measurements(manifest: dict) -> list[IpidMeasurement]:
    """All ipid runs present in the manifest, in a stable protocol/variant order."""
    out: list[IpidMeasurement] = []
    for protocol, section in manifest.items():
        if not isinstance(section, dict):
            continue
        for conn_mode in CONN_MODES:
            for interval in INTERVALS:
                for scale in SCALES:
                    m = resolve(manifest, f"{protocol}.ipid.{conn_mode}.{interval}.{scale}")
                    if m is not None:
                        out.append(m)
    return out
