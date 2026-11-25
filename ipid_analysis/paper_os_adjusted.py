"""Additional OS paper figures from separately cached tag-aware populations."""

from dataclasses import replace
from pathlib import Path

from ipid_analysis import paper_os_group_plot as heatmap
from ipid_analysis.os_adjusted_data import load, read_json
from ipid_analysis.paper_os_taxonomy import (
    BAR_X_LABEL,
    COLORS,
    GROUPS,
    HEATMAP_Y_LABEL,
    ORDER,
    selection,
)
from ipid_analysis.plot_itdk_os import _role_plot_data

HEATMAP = "operating-system-group-by-strategy-adjusted"
FULL_HEATMAP = "operating-system-group-by-strategy-connection-adjusted"
BAR = "itdk-role-by-os-final-adjusted"
KINDS = (HEATMAP, FULL_HEATMAP, BAR)


def global_source(job):
    if job.kind != BAR:
        return job.source
    return job.source.with_name(
        job.source.name.replace("itdk-role-by-os", "operating-system-group-by-strategy")
    )


def canonical_jobs(original, reports_root, campaign, sites):
    """Reuse value-export run selection without requiring existing source PDFs."""
    from paper_values.exporter import choose_plot, load_metrics, required_contexts
    from paper_values.inputs import Inputs, identify_plot, select_artifacts

    if not campaign:
        return original
    root = (Path(reports_root) / "continental" / campaign).resolve()
    if not root.is_dir():
        return original
    contexts, _ = required_contexts(load_metrics()["metrics"])
    inventory = Inputs([root], contexts=contexts, sites=sites)
    try:
        artifacts, conflicts = select_artifacts(list(inventory.artifacts(campaign)))
        if conflicts:
            raise ValueError(f"ambiguous OS source report inventory: {conflicts}")
        if inventory.errors:
            raise ValueError(f"OS source report inventory failed: {inventory.errors}")
    finally:
        inventory.close()
    candidates, explicit = [], []
    for job in original:
        try:
            relative = job.source.relative_to(root)
        except ValueError:
            explicit.append(job)
            continue
        plot = identify_plot(
            str(job.source),
            relative.parts[0],
            relative.relative_to(relative.parts[0]).with_suffix(".pdf").as_posix(),
            campaign,
        )
        if plot:
            candidates.append(plot)
    selected = set()
    for site in sites:
        for protocol in ("icmp", "tcp", "udp"):
            for kind in ("itdk-role-by-os", "operating-system-group-by-strategy"):
                _, names = choose_plot(
                    dict(
                        protocol=protocol,
                        connection="no-connection",
                        phase="rt-fi-merged",
                        kind=kind,
                    ),
                    site,
                    candidates,
                    artifacts,
                )
                selected.update(names)
    return [*explicit, *[job for job in original if str(job.source) in selected]]


def jobs(original, reports_root, campaign, *, sites, protocols):
    from ipid_analysis.paper_ready import Job
    from paper_values import followups
    from paper_values.full_review import published

    result = []
    original = [
        job
        for job in original
        if job.kind
        in ("operating-system-group-by-strategy", "itdk-role-by-os-final", "itdk-role-by-os")
    ]
    for job in canonical_jobs(original, reports_root, campaign, sites):
        kind = {
            "operating-system-group-by-strategy": HEATMAP,
            "itdk-role-by-os-final": BAR,
            "itdk-role-by-os": BAR,
        }.get(job.kind)
        if kind:
            output = (
                job.source.with_name(job.source.stem + "__paper_final__adjusted.pdf")
                if kind == BAR
                else job.destination.with_name(job.destination.stem + "__adjusted.pdf")
            )
            result.append(replace(job, kind=kind, output=output))
    # Keep historical connection-sample reports untouched. Adjust only the pinned Full cohort.
    spec = followups.registration(campaign) if campaign and "tcp" in protocols else None
    if spec:
        root = Path(reports_root) / "followups" / spec["measurement_run"] / spec["analysis_id"]
        for site in sites:
            marker = root / site / "done__full.json"
            if not marker.is_file():
                continue
            done = read_json(marker)
            if any(
                done.get(k) != v
                for k, v in {
                    "version": 1,
                    "site": site,
                    "analysis_id": spec["analysis_id"],
                    "original_campaign": campaign,
                    "measurement_run": spec["measurement_run"],
                }.items()
            ):
                raise ValueError("adjusted OS Full completion identity mismatch")
            if done.get("full_measurement", {}).get("stage") != "full":
                raise ValueError("adjusted connection OS figures require the Full cohort")
            zid = spec["zmap_ids"][site]
            relative = f"full/figures/{zid}/connection/rt-based-base/c-rt-b_operating-system-group-by-strategy__full.json"
            source = published(root / site, done, spec, site, relative)
            report = read_json(source)
            if (
                report.get("measurements", {}).get("rt_based_base")
                != done["full_measurement"]["measurement_id"]
                or report.get("zmap_id") != zid
                or report.get("protocol") != "tcp"
                or report.get("target") != "tcp.ipid.connection.rt-based.base"
            ):
                raise ValueError(
                    "adjusted OS heatmap does not belong to the registered Full measurement"
                )
            output = source.with_name(
                source.stem.removesuffix("__full") + "__paper__full__adjusted.pdf"
            )
            result.append(Job(source.absolute(), FULL_HEATMAP, output=output))
    return result


def load_data(job):
    doc, cache, inputs = load(global_source(job), need_roles=job.kind == BAR)
    connection = job.kind == FULL_HEATMAP
    # Presentation labels/colors can change without repeating numerical preparation.
    report = {
        **doc["report"],
        "groups": {
            g: {**r, "section": GROUPS[g][0], "label": GROUPS[g][1]}
            for g, r in doc["report"]["groups"].items()
        },
    }
    rows = [
        {**r, "OS_SECTION": GROUPS[r["OS_GROUP"]][0], "OS_GROUP_LABEL": GROUPS[r["OS_GROUP"]][1]}
        for r in doc["rows"]
    ]
    data = heatmap.data_from_rows(
        rows,
        report,
        connection_base=connection,
        group_info=GROUPS,
        selection=selection(heatmap.SELECTION),
        y_label=HEATMAP_Y_LABEL,
    )
    data["subject"] = (
        "3.33 inches; Linux Libertine 7 pt; adjusted OS_TAG groups and recomputed row percentages"
    )
    if job.kind == BAR:
        all_counts = {g: r["ip_count"] for g, r in doc["report"]["groups"].items()}
        router = doc["roles"].get("Transit-Observed", {})
        if any(
            g not in GROUPS or type(n) is not int or n < 0
            for group in doc["roles"].values()
            for g, n in group.items()
        ):
            raise ValueError("invalid adjusted OS role category/count")
        if any(n > all_counts.get(g, 0) for g, n in router.items()):
            raise ValueError("adjusted Router counts exceed the measured OS population")
        other = {g: n - router.get(g, 0) for g, n in all_counts.items()}
        if any(
            n > other.get(g, 0) for g, n in doc["roles"].get("No Transit Evidence", {}).items()
        ):
            raise ValueError("adjusted non-transit subset exceeds All Other")
        rows = [
            (role, g, n)
            for role, counts in (("Transit-Observed", router), ("No Transit Evidence", other))
            for g, n in counts.items()
        ]
        data = _role_plot_data(rows, group_info=GROUPS, group_order=ORDER, top_n=10)
    return data, cache, inputs, doc["original_report_sha256"]


def draw(kind, data, output):
    if kind == BAR:
        from ipid_analysis.paper_plot_style import draw as draw_bar

        return draw_bar(
            "itdk-role-by-os",
            data,
            output,
            final=True,
            os_group_info=GROUPS,
            os_colors=COLORS,
            os_xlabel=BAR_X_LABEL,
            os_legend_columns=4,
            os_legend_rows=3,
        )
    return heatmap.draw("operating-system-group-by-strategy", data, output)
