"""Local, versioned cache for paper export; never a writer of analysis artifacts."""

import hashlib
import json
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from threading import Lock


def digest(value):
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def stamp(path):
    """A changed/missing source invalidates its cache entry on every invocation."""
    stat = Path(path).stat()
    return [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


class ExportCache:
    def __init__(self, path, campaign, inputs, profile, *, recompute=False):
        self.path = path
        self.campaign = campaign
        self.recompute = recompute
        self.lock = Lock()
        self.stats = Counter()
        package = Path(__file__).parent
        version = digest(
            {
                p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(package.iterdir())
                if p.suffix in (".py", ".json")
            }
        )
        self.context = dict(
            campaign_id=campaign,
            profile=profile,
            exporter_version=version,
            inputs=[str(Path(p).resolve()) if p is not None else None for p in inputs],
        )
        self.document = {"schema_version": 1, "contexts": {}}
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
            if document.get("schema_version") == 1 and isinstance(
                document.get("contexts"), dict
            ):
                # Entries from other code versions can never be reused here.
                document["contexts"] = {
                    key: entry
                    for key, entry in document["contexts"].items()
                    if entry.get("identity", {}).get("exporter_version") == version
                }
                self.document = document
        except (OSError, ValueError, AttributeError):
            pass  # A cache is expendable; corrupt/old caches never replace source data.
        key = digest(self.context)
        existing = self.document["contexts"].get(key, {})
        if not isinstance(existing, dict) or existing.get("identity") != self.context:
            existing = {}
        entries = existing.get("entries", {})
        self.entries = entries if isinstance(entries, dict) else {}
        self.document["contexts"][key] = dict(
            identity=self.context, entries=self.entries
        )

    def identity(self, operation, site, measurement, path):
        return dict(
            operation=operation,
            campaign_id=self.campaign,
            vp=site,
            measurement=measurement,
            path=str(path),
        )

    def get(self, identity, fingerprint):
        operation = identity["operation"]
        with self.lock:
            entry = self.entries.get(digest(identity))
            try:
                if (
                    not self.recompute
                    and entry["identity"] == identity
                    and entry["fingerprint"] == fingerprint
                    and entry["payload_sha256"] == digest(entry["payload"])
                ):
                    self.stats[operation + "_hits"] += 1
                    return entry["payload"]
            except (KeyError, TypeError, ValueError):
                pass
            self.stats[operation + "_misses"] += 1
        return None

    def put(self, identity, fingerprint, payload):
        entry = dict(
            identity=identity,
            fingerprint=fingerprint,
            payload=payload,
            payload_sha256=digest(payload),
        )
        with self.lock:
            self.entries[digest(identity)] = entry

    def derive(self, registry, artifact, path, operation, compute):
        """Cache only derived scalar values, not aggregate/raw Parquet rows."""
        path = Path(path)
        if not path.is_file():
            return  # Missing files are checked again next run, never negatively cached.
        marker_path = path.parent / ".ipid-complete" / (path.name + ".json")

        def fingerprint():
            marker = marker_path.read_bytes() if marker_path.is_file() else None
            return dict(
                source=stamp(path),
                receipt=hashlib.sha256(marker).hexdigest()
                if marker is not None
                else None,
                report_sha256=artifact.sha256,
            )

        measurement = dict(
            run=artifact.run,
            measurement_id=artifact.data.get("measurement_id"),
            measurements=artifact.data.get("measurements"),
            zmap_id=artifact.data.get("zmap_id"),
        )
        identity = self.identity(operation, artifact.site, measurement, path)
        before = fingerprint()
        payload = self.get(identity, before)
        if payload is not None:
            # Existing JSON metadata always has priority over a cached derived value.
            for key, value in payload["values"].items():
                previous = registry.values.get(key)
                if previous is None or previous.status != "available":
                    registry.put(
                        key,
                        value["raw"],
                        unit=value["unit"],
                        definition=value["definition"],
                        sources=value["sources"],
                    )
            return
        old = dict(registry.values)
        compute()
        if fingerprint() != before:
            raise ValueError(f"aggregate changed while reading: {path}")
        payload = dict(
            values={
                key: asdict(value)
                for key, value in registry.values.items()
                if value.status == "available" and old.get(key) != value
            },
        )
        self.put(identity, before, payload)

    def save(self, atomic_text):
        atomic_text(
            self.path,
            json.dumps(
                self.document,
                sort_keys=True,
                ensure_ascii=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n",
        )
