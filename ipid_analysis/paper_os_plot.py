"""Adapt already selected, role-normalized OS groups from the small JSON report."""

import math

from ipid_analysis.plot_itdk_os import MAX_LEGEND_GROUPS, OTHER_GROUP, OSRolePlotData
from ipid_analysis.plot_itdk_strategy import ROLE_ORDER
from ipid_analysis.plot_os_group_strategy import GROUP_INFO


def load_data(report: dict) -> OSRolePlotData:
    """Validate source values without grouping, ranking, joins or renormalization."""
    selected = report["plot_groups"]
    if not isinstance(selected, list) or len(selected) > MAX_LEGEND_GROUPS:
        raise ValueError("expected at most ten selected OS groups")
    groups = tuple(entry["group"] for entry in selected)
    if any(not isinstance(group, str) for group in groups):
        raise ValueError("invalid OS group identifier")
    if len(set(groups)) != len(groups) or set(groups) - (set(GROUP_INFO) | {OTHER_GROUP}):
        raise ValueError("duplicate or unknown OS groups")
    counts, percentages, totals = {}, {}, {}
    for role in ROLE_ORDER:
        stored = report["plot_counts"][role]
        shares = report["plot_percentages"][role]
        if not isinstance(stored, dict) or not isinstance(shares, dict):
            raise ValueError(f"missing OS counts/percentages: {role}")
        if set(stored) != set(groups) or set(shares) != set(groups):
            raise ValueError(f"OS distribution does not match selected groups: {role}")
        if any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in stored.values()):
            raise ValueError(f"invalid OS counts: {role}")
        if any(
            isinstance(v, bool)
            or not isinstance(v, (int, float))
            or not math.isfinite(v)
            or not 0 <= v <= 100
            for v in shares.values()
        ):
            raise ValueError(f"invalid OS percentages: {role}")
        total = sum(stored.values())
        if not math.isclose(sum(shares.values()), 100 if total else 0, abs_tol=1e-5):
            raise ValueError(f"OS percentages must sum to 100 (or zero for an empty role): {role}")
        if any(
            not math.isclose(
                shares[g], 100 * stored[g] / total if total else 0, rel_tol=1e-9, abs_tol=1e-7
            )
            for g in groups
        ):
            raise ValueError(f"OS counts and percentages disagree: {role}")
        counts[role], percentages[role], totals[role] = dict(stored), dict(shares), total
    return OSRolePlotData(groups, percentages, counts, totals)
