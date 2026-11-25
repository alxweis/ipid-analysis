"""Scalar reads from PR178's compact OS caches; never rebuild a per-IP join."""

import hashlib
from pathlib import Path

from paper_values import followups
from paper_values.review import read_json


class AdjustedOSReader:
    def __init__(self, artifacts, campaign, reports_root, *, followups_root=None, archive=False):
        self.index = {
            (a.site, a.protocol, a.connection, a.phase): a
            for a in artifacts
            if a.kind == "operating-system-group-by-strategy"
        }
        self.campaign, self.reports_root = campaign, reports_root
        self.followups_root, self.archive = followups_root, archive
        self.tables = {}

    def source(self, spec, site):
        if self.archive:
            return None
        if not spec.get("full"):
            artifact = self.index.get((site, spec["protocol"], spec["connection"], spec["phase"]))
            return Path(artifact.name) if artifact and Path(artifact.name).is_file() else None
        registration = followups.registration(self.campaign)
        if registration is None or self.reports_root is None and self.followups_root is None:
            return None
        parent = (
            Path(self.followups_root)
            if self.followups_root
            else Path(self.reports_root).parent.parent / "followups"
        )
        root = parent / registration["measurement_run"] / registration["analysis_id"]
        marker = root / site / "done__full.json"
        if not marker.is_file():
            return None
        done = read_json(marker)
        expected = dict(
            version=1,
            site=site,
            original_campaign=self.campaign,
            measurement_run=registration["measurement_run"],
            analysis_id=registration["analysis_id"],
        )
        if (
            any(done.get(k) != v for k, v in expected.items())
            or done.get("full_measurement", {}).get("stage") != "full"
        ):
            raise ValueError("adjusted OS scalar completion is not the registered Full cohort")
        from paper_values.full_review import published

        zid = registration["zmap_ids"][site]
        relative = f"full/figures/{zid}/connection/rt-based-base/c-rt-b_operating-system-group-by-strategy__full.json"
        anchor = f"/reports/followups/{registration['measurement_run']}/{registration['analysis_id']}/{site}/"
        if not any(
            item.get("path", "").endswith(anchor + relative) for item in done.get("artifacts", [])
        ):
            return None  # Older Full inventories need not contain this new scalar's report.
        source = published(root / site, done, registration, site, relative)
        report = read_json(source)
        if (
            report.get("measurements", {}).get("rt_based_base")
            != done["full_measurement"]["measurement_id"]
            or report.get("zmap_id") != zid
            or report.get("target") != "tcp.ipid.connection.rt-based.base"
        ):
            raise ValueError("adjusted OS scalar report belongs to another measurement")
        return source

    def read(self, spec, site):
        context = (
            site,
            spec["protocol"],
            spec["connection"],
            spec["phase"],
            bool(spec.get("full")),
        )
        if context not in self.tables:
            source = self.source(spec, site)
            if source is None:
                self.tables[context] = ({}, ())
            else:
                report = read_json(source)
                aggregate = Path(report.get("aggregate") or ".")
                cache = (
                    aggregate.with_name(aggregate.stem + "__adjusted.json")
                    if aggregate.is_absolute()
                    else None
                )
                if cache is None or not cache.is_file():
                    self.tables[context] = ({}, (str(source),))
                else:
                    from ipid_analysis.os_adjusted_data import load
                    from ipid_analysis.paper_os_group_plot import data_from_rows
                    from ipid_analysis.paper_os_taxonomy import GROUPS

                    doc, cache, _ = load(source)
                    if (
                        doc["original_report_sha256"]
                        != hashlib.sha256(source.read_bytes()).hexdigest()
                    ):
                        raise ValueError("adjusted OS scalar is not bound to the original report")
                    # Reuse all count/percentage/population checks used by the heatmaps.
                    data_from_rows(
                        doc["rows"],
                        doc["report"],
                        group_info=GROUPS,
                        connection_base=bool(spec.get("full")),
                    )
                    table = {(r["OS_GROUP"], r["IPID_SELECTION_STRATEGY"]): r for r in doc["rows"]}
                    self.tables[context] = (table, (str(source), str(cache)))
        table, sources = self.tables[context]
        groups = spec.get("groups", [spec.get("group")])
        cells = [table.get((g, spec["strategy"])) for g in groups]
        if any(c is None for c in cells):
            return None, sources
        values = [c["PERCENTAGE"] for c in cells]
        value = (
            min(values)
            if spec["stat"].startswith("min-")
            else max(values)
            if spec["stat"].startswith("max-")
            else values[0]
        )
        return value, sources
