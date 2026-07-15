"""Parse the measurement manifest JSON and resolve dotted targets.

Structure (per protocol icmp/tcp/udp)::

    {proto}.zmap = <measurement_id>   # attempted IPs
    {proto}.os   = <measurement_id>
    {proto}.ipid.{no-connection,connection}.{rt-based,fixed-interval}.{base,mass}
        = <measurement_id>            # ipid runs

A dotted target like ``tcp.ipid.no-connection.rt-based.base`` selects one ipid
run. Every id is a measurement directory name (e.g.
``tcp-80_2026-07-14_02-16-24``) under ``data/raw/<category>/<id>/`` where
category is the top-level key (ipid/zmap/os).

Generated artifacts use this layout below their zmap campaign directory::

    <connection-mode>/<interval>-<scale>/<mode>-<interval>-<scale>_<kind>.<ext>

For example, ``no-connection/fixed-interval-mass/n-fi-m_strategies.pdf``.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

CONNECTION_MODES = ("no-connection", "connection")
INTERVALS = ("rt-based", "fixed-interval")
SCALES = ("base", "mass")

CONNECTION_ABBREVIATIONS = {"no-connection": "n", "connection": "c"}
INTERVAL_ABBREVIATIONS = {"rt-based": "rt", "fixed-interval": "fi"}
SCALE_ABBREVIATIONS = {"base": "b", "mass": "m"}


@dataclass(frozen=True)
class IpidMeasurement:
    protocol: str  # icmp | tcp | udp
    connection_mode: str  # no-connection | connection
    interval: str  # rt-based | fixed-interval
    scale: str  # base | mass
    measurement_id: str  # e.g. tcp-80_2026-07-14_02-16-24
    zmap_id: str | None  # the protocol's zmap run (output dir + coverage)

    @property
    def target(self) -> str:
        """Dotted manifest path, e.g. 'tcp.ipid.no-connection.rt-based.base'."""
        return f"{self.protocol}.ipid.{self.connection_mode}.{self.interval}.{self.scale}"

    @property
    def input_key(self) -> str:
        """Key relative to data/raw, e.g. 'ipid/tcp-80_...'."""
        return f"ipid/{self.measurement_id}"

    @property
    def stem(self) -> str:
        """Compact artifact stem, e.g. ``n-rt-b`` or ``c-fi-m``."""
        return "-".join(
            (
                CONNECTION_ABBREVIATIONS[self.connection_mode],
                INTERVAL_ABBREVIATIONS[self.interval],
                SCALE_ABBREVIATIONS[self.scale],
            )
        )

    @property
    def artifact_directory(self) -> Path:
        """Variant directory below a zmap campaign, e.g. ``no-connection/rt-based-base``."""
        return Path(self.connection_mode) / f"{self.interval}-{self.scale}"

    def artifact_name(self, kind: str, ext: str = "pq") -> str:
        """e.g. ``artifact_name('strategies', 'pdf')`` -> ``n-rt-b_strategies.pdf``."""
        return f"{self.stem}_{kind}.{ext}"

    def artifact_path(self, root: Path, kind: str, ext: str = "pq") -> Path:
        """Absolute artifact path below ``root/<zmap_id>/``."""
        if not self.zmap_id:
            raise ValueError(f"{self.target}: no zmap id in manifest (needed for artifact path)")
        return root / self.zmap_id / self.artifact_directory / self.artifact_name(kind, ext)


def load_manifest(path: Path) -> dict:
    return json.loads(Path(path).read_text())


def resolve(manifest: dict, target: str) -> IpidMeasurement | None:
    """Resolve a dotted target against the manifest.

    Example: ``tcp.ipid.no-connection.rt-based.base``. Returns ``None`` if the
    combination is absent.
    """
    parts = target.split(".")
    if len(parts) != 5 or parts[1] != "ipid":
        raise ValueError(
            "expected <proto>.ipid.<no-connection|connection>."
            f"<rt-based|fixed-interval>.<base|mass>, got {target!r}"
        )
    protocol, _, connection_mode, interval, scale = parts

    if connection_mode not in CONNECTION_MODES:
        raise ValueError(f"invalid connection mode in target {target!r}")
    if interval not in INTERVALS:
        raise ValueError(f"invalid interval in target {target!r}")
    if scale not in SCALES:
        raise ValueError(f"invalid scale in target {target!r}")

    section = manifest.get(protocol)
    if not isinstance(section, dict):
        return None
    try:
        measurement_id = section["ipid"][connection_mode][interval][scale]
    except (KeyError, TypeError):
        return None
    return IpidMeasurement(
        protocol, connection_mode, interval, scale, measurement_id, section.get("zmap")
    )


def iter_ipid_measurements(manifest: dict) -> list[IpidMeasurement]:
    """All ipid runs present in the manifest, in a stable protocol/variant order."""
    out: list[IpidMeasurement] = []
    for protocol, section in manifest.items():
        if not isinstance(section, dict):
            continue
        for connection_mode in CONNECTION_MODES:
            for interval in INTERVALS:
                for scale in SCALES:
                    m = resolve(
                        manifest,
                        f"{protocol}.ipid.{connection_mode}.{interval}.{scale}",
                    )
                    if m is not None:
                        out.append(m)
    return out
