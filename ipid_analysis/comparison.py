"""Manifest model for comparing RT-based and fixed-interval base runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ipid_analysis.manifest import (
    CONNECTION_ABBREVIATIONS,
    INTERVAL_ABBREVIATIONS,
    SCALE_ABBREVIATIONS,
    IpidMeasurement,
    resolve,
)


@dataclass(frozen=True)
class BaseComparison:
    """One comparable RT-based-base/fixed-interval-base measurement pair."""

    rt_based: IpidMeasurement
    fixed_interval: IpidMeasurement

    def __post_init__(self) -> None:
        if (
            self.rt_based.interval != "rt-based"
            or self.fixed_interval.interval != "fixed-interval"
        ):
            raise ValueError("comparison requires RT-based followed by fixed-interval probing")
        if self.rt_based.scale != "base" or self.fixed_interval.scale != "base":
            raise ValueError("only base measurements can be compared")
        if self.rt_based.protocol != self.fixed_interval.protocol:
            raise ValueError("comparison targets must use the same protocol")
        if self.rt_based.connection_mode != self.fixed_interval.connection_mode:
            raise ValueError("comparison targets must use the same connection mode")
        if not self.rt_based.zmap_id or self.rt_based.zmap_id != self.fixed_interval.zmap_id:
            raise ValueError("comparison targets must belong to the same zmap campaign")

    @property
    def protocol(self) -> str:
        return self.rt_based.protocol

    @property
    def connection_mode(self) -> str:
        return self.rt_based.connection_mode

    @property
    def zmap_id(self) -> str:
        assert self.rt_based.zmap_id is not None
        return self.rt_based.zmap_id

    @property
    def target(self) -> str:
        return f"{self.rt_based.target}+{self.fixed_interval.target}"

    @property
    def stem(self) -> str:
        connection = CONNECTION_ABBREVIATIONS[self.connection_mode]
        rt = (
            f"{INTERVAL_ABBREVIATIONS[self.rt_based.interval]}-"
            f"{SCALE_ABBREVIATIONS[self.rt_based.scale]}"
        )
        fixed = (
            f"{INTERVAL_ABBREVIATIONS[self.fixed_interval.interval]}-"
            f"{SCALE_ABBREVIATIONS[self.fixed_interval.scale]}"
        )
        return f"{connection}-{rt}_{fixed}"

    @property
    def artifact_directory(self) -> Path:
        variants = (
            f"{self.rt_based.interval}-{self.rt_based.scale}_"
            f"{self.fixed_interval.interval}-{self.fixed_interval.scale}"
        )
        return Path(self.connection_mode) / "comparison" / variants

    def artifact_name(self, kind: str, ext: str = "pq") -> str:
        return f"{self.stem}_{kind}.{ext}"

    def artifact_path(self, root: Path, kind: str, ext: str = "pq") -> Path:
        return root / self.zmap_id / self.artifact_directory / self.artifact_name(kind, ext)


def resolve_base_comparison(manifest: dict, rt_target: str, fixed_target: str) -> BaseComparison:
    rt_based = resolve(manifest, rt_target)
    fixed_interval = resolve(manifest, fixed_target)
    if rt_based is None:
        raise ValueError(f"{rt_target}: not present in manifest")
    if fixed_interval is None:
        raise ValueError(f"{fixed_target}: not present in manifest")
    return BaseComparison(rt_based, fixed_interval)


def iter_base_comparisons(manifest: dict) -> list[BaseComparison]:
    """Return every available RT-base/fixed-interval-base pair in stable order."""
    comparisons = []
    for protocol, section in manifest.items():
        if not isinstance(section, dict):
            continue
        for connection_mode in ("no-connection", "connection"):
            rt_based = resolve(manifest, f"{protocol}.ipid.{connection_mode}.rt-based.base")
            fixed_interval = resolve(
                manifest, f"{protocol}.ipid.{connection_mode}.fixed-interval.base"
            )
            if rt_based is not None and fixed_interval is not None:
                comparisons.append(BaseComparison(rt_based, fixed_interval))
    return comparisons
