"""Frozen SYN-ACK selection evidence; never infer eligibility from RT outcomes."""

from pathlib import Path
import re

from paper_values.inputs import SITES
from paper_values.review import percentage, read_json, record


def synack_metadata(path, zmap, *, allow_missing=False):
    data = read_json(path)
    if (
        data.get("version") != 1
        or data.get("zmap_id") != zmap
        or data.get("reply_type") != "synack"
        or type(data.get("sample_rows")) is not int
        or data["sample_rows"] < 0
    ):
        raise ValueError(f"invalid frozen TCP SYN-ACK sample identity: {path}")
    if allow_missing and any(k not in data for k in ("source_rows", "eligible_rows")):
        return None  # Legacy metadata can prove the sample, but not its selection fraction.
    if (
        data.get("version") != 1
        or data.get("zmap_id") != zmap
        or data.get("reply_type") != "synack"
        or any(
            type(data.get(k)) is not int for k in ("source_rows", "eligible_rows", "sample_rows")
        )
        or not 0 <= data["sample_rows"] <= data["eligible_rows"] <= data["source_rows"]
    ):
        raise ValueError(f"invalid frozen TCP SYN-ACK selection metadata: {path}")
    return data


def values(artifacts, populations, raw_root):
    result = {}
    for site in SITES:
        matches = [
            a
            for a in artifacts
            if a.site == site
            and a.protocol == "tcp"
            and a.kind == "strategies"
            and a.phase in ("rt-base", "fi-base")
        ]
        discoveries = {a.data.get("zmap_id") for a in matches}
        if len(discoveries) > 1:
            raise ValueError("TCP measurements disagree on original discovery identity")
        zmap = next(iter(discoveries), None)
        if zmap is not None and not re.fullmatch(r"tcp-80_[A-Za-z0-9_-]+", zmap):
            raise ValueError("invalid TCP discovery identity for SYN-ACK selection")
        evidence = (
            Path(raw_root) / site / "zmap" / zmap / "zmap-connection-sample.json"
            if raw_root is not None and zmap
            else None
        )
        meta = (
            synack_metadata(evidence, zmap, allow_missing=True)
            if evidence and evidence.is_file()
            else None
        )
        denominator = meta["eligible_rows"] if meta else None
        sources = [evidence] if meta else []
        if meta:
            original = populations.get(f"results.{site}.tcp.zmap.count", {}).get("raw")
            if original is not None and original != meta["source_rows"]:
                raise ValueError("SYN-ACK selection source differs from original TCP ZMap count")
        result[f"results.{site}.tcp.zmap.syn-ack.count"] = record(
            denominator, "count", "Original TCP ZMap IPs eligible under reply_type=synack", sources
        )
        for phase in ("rt-base", "fi-base"):
            prefix = f"results.{site}.tcp.conn.{phase}"
            targets = populations.get(prefix + ".targets.count")
            if targets is None:
                continue
            n = targets["raw"]
            if meta and n is not None and n != meta["sample_rows"]:
                raise ValueError("historical Connection target count differs from frozen sample")
            result[prefix + ".targets.pct"] = record(
                percentage(n, denominator) if n is not None and denominator is not None else None,
                "percent",
                "100 * selected targets / original TCP ZMap SYN-ACK eligible IPs",
                (*targets["sources"], *sources),
            )
    return result
