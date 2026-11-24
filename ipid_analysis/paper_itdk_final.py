"""Final ITDK figures: unchanged transit IPs versus every other measured IP.

The population is an exact difference of existing integer aggregate counts.
No per-IP/raw data, OS lookup, ITDK lookup or classification is recomputed.
"""

from collections import Counter
from dataclasses import dataclass

from ipid_analysis.plot_itdk_os import _role_plot_data as os_plot_data
from ipid_analysis.plot_itdk_strategy import (
    NO_TRANSIT_ROLE,
    TRANSIT_ROLE,
)
from ipid_analysis.plot_itdk_strategy import (
    _role_plot_data as strategy_plot_data,
)
from ipid_analysis.plot_os_group_strategy import GROUP_INFO
from paper_values.readers import STRATEGIES, counts

KINDS = {
    "itdk-role-by-strategy-final": "itdk-role-by-strategy",
    "itdk-role-by-os-final": "itdk-role-by-os",
}
COMPANIONS = {
    "itdk-role-by-strategy-final": "strategies",
    "itdk-role-by-os-final": "operating-system-group-by-strategy",
}


@dataclass(frozen=True)
class FinalData:
    plot: object
    populations: dict


def integer(value):
    if type(value) is not int or value < 0:
        raise ValueError("Final ITDK counts must be nonnegative integers")
    return value


def same_population(role, total, *, os=False):
    """Do not subtract an unrelated run, connection mode, protocol or OS population."""
    for field in ("protocol", "target", "zmap_id"):
        if (
            not isinstance(role.get(field), str)
            or not role[field]
            or total.get(field) != role[field]
        ):
            raise ValueError(f"Final ITDK reports disagree on {field}")
    if role.get("connection_mode") != "no-connection":
        raise ValueError("Final ITDK figures require a no-connection RT/FI-MASS merge")
    protocol = role["protocol"]
    if (
        role["target"]
        != f"{protocol}.ipid.no-connection.rt-based.base+{protocol}.ipid.no-connection.fixed-interval.mass"
    ):
        raise ValueError("Final ITDK figures require the merged RT/FI-MASS target")
    if role["protocol"] not in ("icmp", "tcp", "udp", "udp-dns"):
        raise ValueError("unsupported Final ITDK protocol")
    for field, mode in (
        ("base_measurement_id", "rt_based_base"),
        ("mass_measurement_id", "fixed_interval_mass"),
    ):
        expected = role.get(field)
        actual = total.get("measurements", {}).get(mode) if os else total.get(field)
        if not isinstance(expected, str) or not expected or expected != actual:
            raise ValueError(f"Final ITDK reports disagree on {field}")
    if os:
        identity = role.get("os", {}).get("measurement_id")
        if (
            not isinstance(identity, str)
            or not identity
            or identity != total.get("os_measurement_id")
        ):
            raise ValueError("Final ITDK reports disagree on OS measurement")


def distribution(report, field, allowed):
    """Validate the complete old matched population before retaining its Router rows."""
    rows = report.get("raw_distribution")
    if not isinstance(rows, list):
        raise ValueError(
            "Final ITDK figures need raw_distribution counts, not rounded percentages"
        )
    result = {TRANSIT_ROLE: Counter(), NO_TRANSIT_ROLE: Counter()}
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid Final ITDK distribution row")
        role, key = row.get("role"), row.get(field)
        if role not in result or key not in allowed or (role, key) in seen:
            raise ValueError("duplicate/unknown Final ITDK distribution cell")
        seen.add((role, key))
        result[role][key] = integer(row.get("count"))
    return result


def subtract(all_counts, router):
    if any(n > all_counts.get(key, 0) for key, n in router.items()):
        raise ValueError("Router counts exceed the measured population")
    return Counter({key: n - router[key] for key, n in all_counts.items()})


def load_data(kind, role, total):
    os = kind == "itdk-role-by-os-final"
    if kind not in KINDS:
        raise ValueError("unsupported Final ITDK figure")
    same_population(role, total, os=os)
    coverage = role["coverage"]
    if os:
        if not isinstance(total.get("groups"), dict) or set(total["groups"]) - set(GROUP_INFO):
            raise ValueError("invalid measured OS-group totals")
        all_counts = Counter({g: integer(v.get("ip_count")) for g, v in total["groups"].items()})
        groups = distribution(role, "os_group", GROUP_INFO)
        total_count = integer(total.get("matched_grouped_ip_count"))
        if (
            sum(all_counts.values()) != total_count
            or integer(coverage.get("os_resolved")) != total_count
        ):
            raise ValueError("resolved OS totals disagree with the measured OS population")
        measured = integer(total.get("strategy_ip_count"))
        if measured != integer(coverage.get("total_measured")) or total_count > measured:
            raise ValueError("OS and ITDK measured populations disagree")
        if sum(groups[TRANSIT_ROLE].values()) != integer(
            coverage.get("transit_observed_os_resolved")
        ) or sum(groups[NO_TRANSIT_ROLE].values()) != integer(
            coverage.get("no_transit_evidence_os_resolved")
        ):
            raise ValueError("ITDK OS role totals disagree with their distributions")
        if sum(sum(g.values()) for g in groups.values()) != integer(
            coverage.get("itdk_os_resolved")
        ):
            raise ValueError("ITDK/OS intersection count disagrees")
    else:
        all_counts, total_count = counts(total)
        groups = distribution(role, "strategy", STRATEGIES)
        if integer(coverage.get("total")) != total_count:
            raise ValueError("ITDK and merged strategy populations disagree")
        for name in (TRANSIT_ROLE, NO_TRANSIT_ROLE):
            if sum(groups[name].values()) != integer(role["plot_role_totals"].get(name)):
                raise ValueError("ITDK strategy role total disagrees with distribution")
        matched = sum(sum(g.values()) for g in groups.values())
        if (
            matched != integer(coverage.get("matched"))
            or matched + integer(coverage.get("unmatched")) != total_count
        ):
            raise ValueError("ITDK matched/unmatched counts disagree")
    router = groups[TRANSIT_ROLE]
    # The original non-transit subset must itself be contained in the complement.
    other = subtract(all_counts, router)
    if any(n > other.get(key, 0) for key, n in groups[NO_TRANSIT_ROLE].items()):
        raise ValueError("non-transit subset exceeds the Final All Other population")
    router_count, other_count = sum(router.values()), sum(other.values())
    if router_count + other_count != total_count:
        raise ValueError("Final ITDK populations do not partition the measured population")
    rows = [
        (r, key, n)
        for r, group in ((TRANSIT_ROLE, router), (NO_TRANSIT_ROLE, other))
        for key, n in group.items()
    ]
    plot = os_plot_data(rows) if os else strategy_plot_data(rows)
    return FinalData(
        plot,
        dict(
            measured=total_count,
            router=router_count,
            all_other=other_count,
            itdk_unmatched_included=other_count - sum(groups[NO_TRANSIT_ROLE].values()),
            definition="All Other = measured population minus ITDK T=true Router IPs",
            os_resolved_only=os,
        ),
    )


def draw(kind, data, output):
    from ipid_analysis.paper_plot_style import draw as draw_paper

    draw_paper(KINDS[kind], data.plot, output, final=True)
