"""Read plot-aligned phase populations without rescanning measurement data."""

import math

from paper_values.inputs import SITES, component
from paper_values.readers import counts
from paper_values.review import STRATEGIES, percentage, record

FAMILIES = (("icmp", "icmp"), ("tcp", "tcp.no-conn"), ("udp", "udp"))
COUNTER_SUM = ("SINGLE", "PER_DESTINATION", "PER_BUCKET")


def _bar(report, mode):
    bar = report.data["bars"][mode]
    population, total = counts(dict(counts=bar["counts"], total_ips=bar["total"]))
    if set(bar["percentages"]) - set(STRATEGIES):
        raise ValueError(f"unknown strategy in {mode} plot percentages")
    for strategy in STRATEGIES:
        value = bar["percentages"].get(strategy, 0)
        expected = percentage(population.get(strategy, 0), total) if total else 0
        if (
            type(value) not in (int, float)
            or not math.isfinite(value)
            or not math.isclose(value, expected, abs_tol=1e-7, rel_tol=1e-9)
        ):
            raise ValueError(f"{mode} plot percentage disagrees with counts: {strategy}")
    return {s: population.get(s, 0) for s in STRATEGIES}, total


def _refinement(rt, fixed, plot, coverage):
    """Mirror the existing refinement plot, including missing result rows."""
    population = total = None
    sources = []
    if rt is not None and fixed is not None:
        rt_counts, _ = counts(rt.data)
        fixed_counts, fixed_total = counts(fixed.data)
        total = rt_counts.get("UNCLASSIFIED", 0)
        if fixed_total > total:
            if plot is not None:
                raise ValueError("Fixed-Interval results exceed the RT Unclassified cohort")
            # Standalone legacy phase reports do not establish a refinement
            # relationship. Do not invent its denominator or block old metrics.
            return None, None, []
        population = {s: fixed_counts.get(s, 0) for s in STRATEGIES}
        population["NOT_ENOUGH_SAMPLES"] += total - fixed_total
        sources.extend((rt.name, fixed.name))
    if plot is not None:
        plotted, plotted_total = _bar(plot, "Fixed-Interval")
        plotted_rt, _ = _bar(plot, "RT-based")
        if plotted_total != plotted_rt.get("UNCLASSIFIED", 0):
            raise ValueError("refinement plot denominator differs from RT Unclassified")
        if rt is not None:
            actual, actual_total = counts(rt.data)
            if _bar(plot, "RT-based") != ({s: actual.get(s, 0) for s in STRATEGIES}, actual_total):
                raise ValueError("refinement plot disagrees with the RT strategy report")
        if population is not None and (population, total) != (plotted, plotted_total):
            raise ValueError("refinement plot disagrees with the Fixed-Interval strategy report")
        population, total = plotted, plotted_total
        sources.append(plot.name)
    if total is not None and coverage is not None:
        if coverage.data["zmap_ip_count"] != total:
            if plot is not None:
                raise ValueError("refinement plot cohort differs from selected Phase 2 targets")
            return None, None, []
        sources.append(coverage.name)
    return population, total, sources


def load(artifacts):
    index = {
        (a.site, a.protocol, a.phase, a.kind): a
        for a in artifacts
        if a.connection == "no-connection"
    }
    values, errors = {}, []
    for site in SITES:
        for protocol, family in FAMILIES:
            prefix = f"results.{site}.{family}"

            def get(phase, kind="strategies"):
                return index.get((site, protocol, phase, kind))

            rt, fixed, merged = get("rt-base"), get("fi-mass"), get("rt-fi-merged")
            plot = get("rt-fi-merged", "measurement-type-by-strategy")
            population, total, sources = None, None, []
            try:
                population, total, sources = _refinement(
                    rt, fixed, plot, get("fi-mass", "coverage")
                )
            except (KeyError, TypeError, ValueError) as exc:
                errors.append(dict(source=prefix + ".fi-mass.all", error=str(exc)))
            definition = (
                "All selected Phase 2 IPs (RT Unclassified cohort), including UNCLASSIFIED, "
                "NOT_ENOUGH_SAMPLES and missing result rows; same denominator as the Fixed-Interval bar"
            )
            values[prefix + ".fi-mass.all.count"] = record(total, "count", definition, sources)
            for strategy in STRATEGIES:
                n = population[strategy] if population is not None else None
                base = prefix + ".fi-mass.all." + component(strategy)
                values[base + ".count"] = record(
                    n, "count", strategy + " IPs in " + definition, sources
                )
                values[base + ".pct"] = record(
                    percentage(n, total) if n is not None else None,
                    "percent",
                    "100 * " + strategy + " IPs / " + definition,
                    sources,
                )
            values[prefix + ".fi-mass.all.single-per-destination-per-bucket.pct"] = record(
                percentage(sum(population[s] for s in COUNTER_SUM), total)
                if population is not None
                else None,
                "percent",
                "100 * (SINGLE + PER_DESTINATION + PER_BUCKET) / " + definition,
                sources,
            )

            combined, combined_sources = None, []
            if rt is not None and merged is not None:
                try:
                    rt_counts, rt_total = counts(rt.data)
                    merged_counts, merged_total = counts(merged.data)
                    rt_insufficient = rt_counts.get("NOT_ENOUGH_SAMPLES", 0)
                    if (
                        merged_total != rt_total
                        or merged_counts.get("NOT_ENOUGH_SAMPLES", 0) < rt_insufficient
                    ):
                        raise ValueError("merged population does not retain the original RT rows")
                    combined = merged_total - rt_insufficient
                    combined_sources = [rt.name, merged.name]
                except ValueError as exc:
                    errors.append(dict(source=prefix + ".rt-fi.success-all.count", error=str(exc)))
            values[prefix + ".rt-fi.success-all.count"] = record(
                combined,
                "count",
                "RT-based sufficiently sampled population with its Unclassified rows replaced "
                "by all Phase 2 outcomes, including insufficient/missing replies; IPs counted once. "
                "Excludes original RT failures; not the ZMap target count or final success count",
                combined_sources,
            )
    return values, errors
