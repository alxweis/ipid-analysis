"""Cache derived scalar tables, keyed by campaign, inputs and calculation code."""

from collections import Counter
import hashlib
import json
from pathlib import Path

from paper_values.diagnostics import Diagnostics


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def fingerprint(path):
    path = Path(path)
    stat = path.stat()
    receipt = path.parent / ".ipid-complete" / (path.name + ".json")
    return {
        "path": str(path.resolve()),
        "stat": [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns],
        "receipt": hashlib.sha256(receipt.read_bytes()).hexdigest() if receipt.is_file() else None,
    }


def verify_file(path):
    """Check existing publication receipts; legacy files without receipts remain readable."""
    path = Path(path)
    receipt = path.parent / ".ipid-complete" / (path.name + ".json")
    if not receipt.is_file():
        return
    data = json.loads(receipt.read_text(encoding="utf-8-sig"))
    size = path.stat().st_size
    if (
        not isinstance(data, dict)
        or data.get("status") != "complete"
        or data.get("size_bytes") != size
    ):
        raise ValueError(f"completion receipt does not match source: {path}")
    if size <= 32 * 1024**2 and hashlib.sha256(path.read_bytes()).hexdigest() != data.get(
        "sha256"
    ):
        raise ValueError(f"completion receipt does not match source bytes: {path}")


class ValueCache:
    def __init__(self, path, campaign, *, recompute=False, enabled=True, diagnostics=None):
        self.path, self.recompute, self.enabled = Path(path), recompute, enabled
        self.diagnostics = diagnostics or Diagnostics()
        self.stats = Counter()
        self.document = {"schema_version": 2, "contexts": {}}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if data.get("schema_version") == 2 and isinstance(data.get("contexts"), dict):
                self.document = data
        except (OSError, ValueError, AttributeError):
            pass
        code = Path(__file__).with_name("readers.py").read_bytes()
        identity = {
            "campaign": campaign,
            "calculation_sha256": hashlib.sha256(code).hexdigest(),
        }
        # Entries produced by older calculation code cannot be reused again.
        self.document["contexts"] = {
            key: entry
            for key, entry in self.document["contexts"].items()
            if isinstance(entry, dict)
            and isinstance(entry.get("identity"), dict)
            and entry["identity"].get("calculation_sha256") == identity["calculation_sha256"]
        }
        context = self.document["contexts"].get(digest(identity), {})
        if (
            not isinstance(context, dict)
            or context.get("identity") != identity
            or not isinstance(context.get("entries"), dict)
        ):
            context = {"identity": identity, "entries": {}}
        self.entries = context["entries"]
        self.document["contexts"][digest(identity)] = context

    def derive(self, operation, path, report_sha256, compute):
        """Return cached raw scalars when inputs are unchanged; never cache a missing file."""
        path = Path(path)
        with self.diagnostics.measure("cache.check"):
            if not path.is_file():
                return {}
            before = fingerprint(path)
            key = digest([operation, before["path"], report_sha256])
            entry = self.entries.get(key)
            try:
                if (
                    self.enabled
                    and not self.recompute
                    and entry["input"] == before
                    and entry["sha256"] == digest(entry["values"])
                ):
                    self.stats[operation + "_hits"] += 1
                    return entry["values"]
            except (KeyError, TypeError, ValueError):
                pass
        self.stats[operation + "_misses"] += 1
        self.diagnostics.call("cache.verify", verify_file, path)
        values = self.diagnostics.call("cache.compute", compute)
        with self.diagnostics.measure("cache.check"):
            if fingerprint(path) != before:
                raise ValueError(f"aggregate changed while reading: {path}")
        if self.enabled:
            self.entries[key] = {
                "input": before,
                "values": values,
                "sha256": digest(values),
            }
        return values

    def serialize(self):
        return (
            json.dumps(self.document, sort_keys=True, separators=(",", ":"), allow_nan=False)
            + "\n"
        )
