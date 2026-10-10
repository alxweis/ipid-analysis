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
RANDOM_REPRODUCIBILITY_REPEAT_COUNT = 5


@dataclass(frozen=True)
class RandomReproducibility:
    protocol: str
    baseline_target: str
    baseline_measurement_id: str
    repeat_ids: tuple[str, ...]
    target_file: str
    cohort_file: str
    prepare_metadata_file: str
    selection_seed: int
    maximum_targets: int


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


def resolve_random_reproducibility(
    manifest: dict,
    protocol: str,
) -> RandomReproducibility | None:
    """Resolve optional bounded Mass reproducibility provenance for a protocol."""
    section = manifest.get(protocol)
    if not isinstance(section, dict):
        return None
    value = section.get("random_reproducibility")
    if value is None:
        return None
    if not isinstance(value, dict):
        raise TypeError(f"{protocol}.random_reproducibility must be an object")
    baseline_target = f"{protocol}.ipid.no-connection.fixed-interval.mass"
    baseline = resolve(manifest, baseline_target)
    if baseline is None:
        raise ValueError(f"{protocol}.random_reproducibility requires {baseline_target}")
    baseline_id = value.get("baseline")
    if baseline_id != baseline.measurement_id:
        raise ValueError(
            f"{protocol}.random_reproducibility.baseline does not match the Mass measurement"
        )
    repeats = value.get("repeats")
    if not isinstance(repeats, list) or not all(
        isinstance(item, str) and item for item in repeats
    ):
        raise ValueError(f"{protocol}.random_reproducibility.repeats must be a string list")
    if len(repeats) != RANDOM_REPRODUCIBILITY_REPEAT_COUNT:
        raise ValueError(
            f"{protocol}.random_reproducibility.repeats must contain exactly "
            f"{RANDOM_REPRODUCIBILITY_REPEAT_COUNT} measurements"
        )
    if len(set(repeats)) != len(repeats):
        raise ValueError(f"{protocol}.random_reproducibility.repeats contains duplicates")
    maximum_targets = value.get("maximum_targets")
    if not isinstance(maximum_targets, int) or not 0 <= maximum_targets <= 10_000:
        raise ValueError(
            f"{protocol}.random_reproducibility.maximum_targets must be in [0, 10000]"
        )
    selection_seed = value.get("selection_seed")
    if not isinstance(selection_seed, int):
        raise TypeError(f"{protocol}.random_reproducibility.selection_seed must be an integer")

    def required_name(field: str) -> str:
        result = value.get(field)
        if not isinstance(result, str) or not result or Path(result).name != result:
            raise ValueError(f"{protocol}.random_reproducibility.{field} must be a file name")
        return result

    return RandomReproducibility(
        protocol=protocol,
        baseline_target=baseline_target,
        baseline_measurement_id=baseline.measurement_id,
        repeat_ids=tuple(repeats),
        target_file=required_name("target_file"),
        cohort_file=required_name("cohort_file"),
        prepare_metadata_file=required_name("prepare_metadata_file"),
        selection_seed=selection_seed,
        maximum_targets=maximum_targets,
    )


def iter_random_reproducibility(manifest: dict) -> list[RandomReproducibility]:
    return [
        spec
        for protocol in manifest
        if (spec := resolve_random_reproducibility(manifest, protocol)) is not None
    ]
